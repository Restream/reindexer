#include "selecter.h"
#include "mergerimpl.h"
#include "tools/objects_pool.h"
#include "tools/scope_guard.h"
#include "tools/serializer.h"

namespace reindexer {
// Minimal relevant length of the stemmer's term
constexpr int kMinStemRelevantLen = 3;
// Max length of the stemming result, which will be skipped
constexpr int kMaxStemSkipLen = 1;
constexpr bool kVariantsWithDifLength = (kMinStemRelevantLen - kMaxStemSkipLen) > 2;

template <typename IdCont>
void Selector<IdCont>::applyStemmers(const std::string& pattern, int proc, const FtDslOpts& termOpts, bool keepSuff,
									 std::vector<FtVariantEntry>& variants, h_vector<FtBoundVariantEntry, 4>* lowRelVariants,
									 std::string& buffer) {
	const int stemProc = std::max(proc - holder_.cfg_->rankingConfig.stemmerPenalty, BaseFTConfig::BaseRankingConfig::kMinProcAfterPenalty);
	std::string& stemstr = buffer;

	if (termOpts.op == OpNot && termOpts.suff) {
		// More strict match for negative (excluding) suffix terms
		if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
			logFmt(LogInfo, "Skipping stemming for '{}{}{}'", termOpts.suff ? "*" : "", pattern, termOpts.pref ? "*" : "");
		}
		return;
	}

	for (auto& lang : holder_.cfg_->stemmers) {
		auto stemIt = holder_.stemmers_.find(lang);
		if (stemIt == holder_.stemmers_.end()) {
			throw Error(errParams, "Stemmer for language {} is not available", lang);
		}
		stemstr = "";
		stemIt->second.stem(pattern, stemstr);
		if (pattern == stemstr || stemstr.empty()) {
			continue;
		}

		const int stemLen = getUTF8StringCharactersCount(stemstr);
		if (stemLen <= kMaxStemSkipLen) {
			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Skipping too short stemmer's term '{}{}*'", termOpts.suff && keepSuff ? "*" : "", stemstr);
			}
			continue;
		}

		const auto charCount = getUTF8StringCharactersCount(stemstr);
		if (charCount >= kMinStemRelevantLen) {
			variants.emplace_back(std::move(stemstr), std::move(termOpts.GetStemOpts(keepSuff)), stemProc, stemLen);
		} else if (lowRelVariants != nullptr) {
			lowRelVariants->emplace_back(std::move(stemstr), std::move(termOpts.GetStemOpts(keepSuff)), stemProc, stemLen);
		}
	}
}

template <typename IdCont>
void Selector<IdCont>::prepareSynonymVariants(const std::wstring& pattern, const FtDslOpts& opts, std::vector<FtVariantEntry>& variants,
											  std::string& patternBuf, std::string& stemmerBuf) {
	if (pattern.empty()) {
		return;
	}
	const int proc = holder_.cfg_->rankingConfig.synonyms;

	variants.resize(0);
	utf16_to_utf8(pattern, patternBuf);
	variants.emplace_back(patternBuf, opts, proc, -1);
	if (!opts.exact) {
		applyStemmers(patternBuf, proc, opts, true, variants, nullptr, stemmerBuf);
	}

	if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
		WrSerializer wrSer;
		size_t idx = 0;
		for (auto& variant : variants) {
			if (idx != 0) {
				wrSer << ", ";
			}
			++idx;
			wrSer << variant.pattern;
		}
		logFmt(LogInfo, "Multiword synonyms variants: [{}]", wrSer.Slice());
	}
}

template <typename IdCont>
void Selector<IdCont>::prepareVariants(const FtDSLEntry& term, int termProc, unsigned termIdx, std::vector<FtVariantEntry>& variants,
									   h_vector<FtBoundVariantEntry, 4>* lowRelVariants, std::vector<MultiWord>* synonyms,
									   h_vector<std::wstring, 8>* variantsForTypos) {
	const BaseFTConfig::BaseRankingConfig& rankingConfig = holder_.cfg_->rankingConfig;
	const FtDslOpts& opts = term.Opts();
	const std::wstring& pattern = term.Pattern();
	const StopWordsSetT& stopWords = holder_.cfg_->stopWords;

	if (pattern.empty()) {
		return;
	}

	thread_local ObjectsPool<std::string, 4> stringsPool;
	thread_local ObjectsPool<fast_hash_map<std::wstring, size_t>, 4> mapsPool;

	variants.emplace_back(std::string(), opts, termProc, -1);
	utf16_to_utf8(pattern, variants.back().pattern);
	if (!opts.exact) {
		auto buffer = stringsPool.Get();
		applyStemmers(variants.back().pattern, termProc, opts, true, variants, lowRelVariants, buffer.Data());
	}

	if (holder_.cfg_->enableNumbersSearch && opts.number) {
		return;
	}

	ITokenFilter::ResultsStorage additionalVariants;
	auto patternsUsedGrd = mapsPool.Get();
	auto& patternsUsed = patternsUsedGrd.Data();

	if (holder_.cfg_->enableTranslit && !opts.exact) {
		int translitProc =
			rankingConfig.fullMatch > 0 ? ((rankingConfig.translit * termProc) / rankingConfig.fullMatch) : rankingConfig.translit;
		holder_.translit_->GetVariants(pattern, additionalVariants, translitProc, patternsUsed);
	}

	if (holder_.cfg_->enableKbLayout && !opts.exact) {
		int kblayoutProc =
			rankingConfig.fullMatch > 0 ? ((rankingConfig.kblayout * termProc) / rankingConfig.fullMatch) : rankingConfig.kblayout;
		holder_.kbLayout_->GetVariants(pattern, additionalVariants, kblayoutProc, patternsUsed);
	}

	int synonymsProc =
		rankingConfig.fullMatch > 0 ? ((rankingConfig.synonyms * termProc) / rankingConfig.fullMatch) : rankingConfig.synonyms;
	if (opts.op != OpNot) {
		holder_.synonyms_->GetVariants(pattern, additionalVariants, synonymsProc, patternsUsed);
		holder_.synonyms_->AddOneToManySynonyms(pattern, opts, termIdx, synonymsProc, *synonyms);
	}

	if (!opts.exact) {
		int delimitedProc =
			rankingConfig.fullMatch > 0 ? ((rankingConfig.delimited * termProc) / rankingConfig.fullMatch) : rankingConfig.delimited;
		ITokenFilter::ResultsStorage variantsDelimited;
		auto variantPatternsUsedGrd = mapsPool.Get();
		auto& variantPatternsUsed = variantPatternsUsedGrd.Data();

		holder_.compositeWordsSplitter_->GetVariants(pattern, variantsDelimited, delimitedProc, variantPatternsUsed);
		for (auto& v : variantsDelimited) {
			if (opts.typos && variantsForTypos) {
				variantsForTypos->emplace_back(v.pattern);
			}

			holder_.synonyms_->GetVariants(v.pattern, additionalVariants, std::min(synonymsProc, delimitedProc), patternsUsed);
			holder_.synonyms_->AddOneToManySynonyms(v.pattern, opts, termIdx, std::min(synonymsProc, delimitedProc), *synonyms);

			AddOrUpdateVariant(additionalVariants, patternsUsed, std::move(v));
		}
	}

	auto variantPatternGrd = stringsPool.Get();
	auto& variantPattern = variantPatternGrd.Data();
	for (auto& v : additionalVariants) {
		FtDslOpts varOpts = opts;
		utf16_to_utf8(v.pattern, variantPattern);
		const bool prefSaved = opts.pref;
		if (v.prefAndStemmersForbidden) {
			varOpts.pref = false;
		}
		auto prefGuard = reindexer::MakeScopeGuard([&]() { varOpts.pref = prefSaved; });

		// stop words doesn't present in index, so we need to check it only in case of prefix or suffix search
		if (varOpts.pref || varOpts.suff) {
			if (variantPattern.empty()) {
				continue;
			}

			if (auto it = stopWords.find(variantPattern); it != stopWords.end() && it->type == StopWord::Type::Stop) {
				continue;
			}

			auto patternWithoutDelimsGrd = stringsPool.Get();
			auto& patternWithoutDelims = patternWithoutDelimsGrd.Data();
			if (holder_.cfg_->splitOptions.ContainsDelims(variantPattern)) {
				holder_.cfg_->splitOptions.RemoveDelims(variantPattern, patternWithoutDelims);
				if (auto it = stopWords.find(patternWithoutDelims); it != stopWords.end() && it->type == StopWord::Type::Stop) {
					continue;
				}
			}
		}

		variants.emplace_back(std::move(variantPattern), std::move(varOpts), v.proc, -1);
		if (!opts.exact && !v.prefAndStemmersForbidden) {
			auto& last = variants.back();
			auto buffer = stringsPool.Get();
			applyStemmers(last.pattern, v.proc, last.opts, false, variants, lowRelVariants, buffer.Data());
		}
	}
}

constexpr static size_t kVariantsReserveSize = 64;

template <typename IdCont>
template <FtUseExternStatuses useExternSt, typename MergedDataType>
MergedDataType Selector<IdCont>::Process(FtDSLQuery&& query, bool inTransaction, RankSortType rankSortType,
										 FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx) {
	std::vector<MultiWord> synonyms;
	const BaseFTConfig::BaseRankingConfig& rankingConfig = holder_.cfg_->rankingConfig;
	holder_.synonyms_->AddManyToManySynonyms(query, rankingConfig.synonyms, synonyms);
	if (!inTransaction) {
		ThrowOnCancel(rdxCtx);
	}

	size_t totalORVids = 0;
	std::vector<FtVariantEntry> variants;
	variants.reserve(kVariantsReserveSize);
	h_vector<FtBoundVariantEntry, 4> lowRelVariants;
	h_vector<std::wstring, 8> variantsForTypos;

	std::vector<ft::TermResults<IdCont>> rawResults;
	h_vector<std::optional<ft::FoundWordsProcsType>, 4> termsWordsProcs;
	thread_local ObjectsPool<ft::FoundWordsProcsType, 1> wordsProcsPool;
	auto wordsProcsGrd = wordsProcsPool.Get();
	auto& wordsProcs = wordsProcsGrd.Data();

	termsWordsProcs.resize(query.NumTerms());
	rawResults.reserve(query.NumTerms());
	std::wstring typosPattern;

	for (size_t i = 0; i < query.NumTerms(); ++i) {
		const FtDSLEntry& term = query.GetTerm(i);

		wordsProcs.clear();
		variants.resize(0);
		variantsForTypos.resize(0);
		typosPattern.resize(0);

		if (term.Opts().typos) {
			typosPattern = term.Pattern();
		}

		const auto irrVariantsCount = lowRelVariants.size();
		prepareVariants(term, rankingConfig.fullMatch, i, variants, &lowRelVariants, &synonyms, &variantsForTypos);
		if (i > 0 && holder_.cfg_->enableTermsConcat && term.CanBeJoinedWith(query.GetTerm(i - 1))) {
			const FtDSLEntry joinedTerm = term.JoinWithPrevTerm(query.GetTerm(i - 1));
			prepareVariants(joinedTerm, rankingConfig.concat, i, variants, &lowRelVariants, &synonyms, &variantsForTypos);
			if (joinedTerm.Opts().typos) {
				if (typosPattern.empty()) {
					typosPattern = joinedTerm.Pattern();
				} else {
					variantsForTypos.push_back(joinedTerm.Pattern());
				}
			}
		}

		const bool termHasLowRelVariants = (lowRelVariants.size() > irrVariantsCount);
		ft::FoundWordsProcsType* termWordsProcs = &wordsProcs;
		if (termHasLowRelVariants) {
			termWordsProcs = &(termsWordsProcs[i].emplace());
		}

		rawResults.emplace_back(term);
		auto& termRes = rawResults.back();

		for (unsigned j = irrVariantsCount; j < lowRelVariants.size(); ++j) {
			lowRelVariants[j].rawResultIdx = rawResults.size() - 1;
		}

		if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
			printVariants(variants, lowRelVariants, termRes);
		}

		for (const FtVariantEntry& variant : variants) {
			for (auto& step : holder_.steps) {
				totalORVids += processStepVariants<useExternSt>(step, variant, mergeStatuses, std::numeric_limits<int>::max(), termRes,
																*termWordsProcs);
			}
		}

		if (!typosPattern.empty()) {
			// Lookup typos from typos_ map and fill results
			TyposHandler h(*holder_.cfg_);
			size_t vidsCount = h.Process(typosPattern, variantsForTypos, holder_, termRes, *termWordsProcs);
			if (termRes.term.Opts().op == OpOr) {
				totalORVids += vidsCount;
			}
		}

		rawResults.back().UpdateProcs(*termWordsProcs);
	}

	std::vector<ft::TermResults<IdCont>> results;
	size_t reserveSize = rawResults.size();
	for (const MultiWord& syn : synonyms) {
		reserveSize += syn.words.size();
	}
	results.reserve(reserveSize);
	std::vector<size_t> synonymsBounds;
	synonymsBounds.reserve(synonyms.size());
	if (!inTransaction) {
		ThrowOnCancel(rdxCtx);
	}

	std::vector<FtVariantEntry> synVariants;
	if (!synonyms.empty()) {
		synVariants.reserve(kVariantsReserveSize);
	}
	std::vector<ft::TermResults<IdCont>> synResults;
	std::string patternBuf, stemmerBuf;

	for (MultiWord& syn : synonyms) {
		synResults.resize(0);
		synResults.reserve(syn.words.size());

		for (std::wstring& word : syn.words) {
			synVariants.resize(0);
			wordsProcs.clear();

			prepareSynonymVariants(word, syn.opts, synVariants, patternBuf, stemmerBuf);
			FtDSLEntry entry{std::move(word), syn.opts};
			synResults.emplace_back(std::move(entry));

			for (const FtVariantEntry& variant : synVariants) {
				for (auto& step : holder_.steps) {
					totalORVids += processStepVariants<useExternSt>(step, variant, mergeStatuses, std::numeric_limits<int>::max(),
																	synResults.back(), wordsProcs);
				}
			}

			synResults.back().UpdateProcs(wordsProcs);
		}
		for (size_t idx : syn.termsIdx) {
			assertrx_throw(idx < rawResults.size());
			rawResults[idx].synonymsGroups.push_back(synonymsBounds.size());
		}
		for (auto& res : synResults) {
			results.emplace_back(std::move(res));
		}
		synonymsBounds.push_back(results.size());
	}

	// Typos for terms with low relevancy will not be processed
	processLowRelVariants<useExternSt>(totalORVids, lowRelVariants, mergeStatuses, rawResults, termsWordsProcs);

	for (auto& res : rawResults) {
		results.emplace_back(std::move(res));
	}

	const auto maxMergedSize = std::min(size_t(holder_.cfg_->mergeLimit), totalORVids);
	if (maxMergedSize < 0xFFFF) {
		return mergeResults<uint16_t, MergedDataType>(results, totalORVids, synonymsBounds, inTransaction, rankSortType, mergeStatuses,
													  rdxCtx);
	} else if (maxMergedSize < 0xFFFFFFFF) {
		return mergeResults<uint32_t, MergedDataType>(results, totalORVids, synonymsBounds, inTransaction, rankSortType, mergeStatuses,
													  rdxCtx);
	} else {
		assertrx_throw(false);
	}
	return MergedDataType();
}

template <typename IdCont>
template <typename MergedOffsetT, typename MergedDataType>
MergedDataType Selector<IdCont>::mergeResults(std::vector<ft::TermResults<IdCont>>& results, size_t totalORVids,
											  const std::vector<size_t>& synonymsBounds, bool inTransaction, RankSortType rankSortType,
											  FtMergeStatuses::Statuses& mergeStatuses, const RdxContext& rdxCtx) {
	ft::Merger<IdCont, MergedDataType, MergedOffsetT> merger(holder_, mergeStatuses, fieldSize_, maxAreasInDoc_, inTransaction, rdxCtx);
	switch (holder_.cfg_->bm25Config.bm25Type) {
		case FtFastConfig::Bm25Config::Bm25Type::rx:
			return merger.template Merge<Bm25Rx>(results, totalORVids, synonymsBounds, rankSortType);
		case FtFastConfig::Bm25Config::Bm25Type::classic:
			return merger.template Merge<Bm25Classic>(results, totalORVids, synonymsBounds, rankSortType);
		case FtFastConfig::Bm25Config::Bm25Type::wordCount:
			return merger.template Merge<TermCount>(results, totalORVids, synonymsBounds, rankSortType);
	}
	assertrx_throw(false);
	return MergedDataType();
}

template <class VidsContainer>
static bool allVidsExcluded(const FtMergeStatuses::Statuses& mergeStatuses, const VidsContainer& wordVids) {
	for (const auto& id : wordVids) {
		if (mergeStatuses[id.Id()] != FtMergeStatuses::kExcluded) {
			return false;
		}
	}

	return true;
}

template <typename IdCont>
template <FtUseExternStatuses useExternSt>
size_t Selector<IdCont>::processStepVariants(typename DataHolder<IdCont>::CommitStep& step, const FtVariantEntry& variant,
											 const FtMergeStatuses::Statuses& mergeStatuses, int vidsLimit, ft::TermResults<IdCont>& result,
											 ft::FoundWordsProcsType& wordsProcs) {
	size_t totalORVids = 0;
	auto& pattern = variant.pattern;
	auto& suffixes = step.suffixes_;
	int matched = 0, skipped = 0, vids = 0, excludedCnt = 0;

	// Walk current variant in suffixes array and fill results

	bool needStop = false;
	for (auto wordIt = suffixes.lower_bound(pattern); wordIt != suffixes.end() && vids < vidsLimit && !needStop; ++wordIt) {
		needStop = wordIt.lcp() < int(pattern.length());

		const WordIdType wordId = wordIt->second;
		const auto& word = holder_.GetWordById(wordId);

		// ToDo This seems really bad for short suffix search...
		if (useExternSt == FtUseExternStatuses::Yes && allVidsExcluded(mergeStatuses, word.vids)) {
			++excludedCnt;
			continue;
		}

		const uint32_t suffixWordId = holder_.GetSuffixWordId(wordId, step);
		const std::string::value_type* suffixWord = suffixes.word_at(suffixWordId);
		const size_t suffixWordLength = suffixes.word_len_at(suffixWordId);
		const ptrdiff_t suffixLen = wordIt->first - suffixWord;

		if (!variant.opts.suff && suffixLen != 0) {
			continue;
		}
		if (!variant.opts.pref && suffixWordLength != pattern.length() + suffixLen) {
			break;
		}

		const int matchDif = std::abs(long(suffixWordLength - pattern.length() + suffixLen));
		const float proc =
			std::max<float>(variant.proc - holder_.cfg_->partialMatchDecrease * matchDif / std::max<float>(pattern.length(), 3),
							suffixLen ? holder_.cfg_->rankingConfig.suffixMin : holder_.cfg_->rankingConfig.prefixMin);

		auto [it, emplaced] = wordsProcs.try_emplace(wordId, proc);
		if (!emplaced) {
			it->second = std::max(it->second, proc);
			skipped++;
			continue;
		}

		result.subtermsResults.emplace_back(word.vids, wordIt->first, wordId, proc, typename ft::SubtermResults<IdCont>::NoHoldT{});
		result.idsCnt += word.vids.size();
		matched++;
		vids += word.vids.size();
		if (variant.opts.op == OpOr) {
			totalORVids += word.vids.size();
		}

		if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
			logFmt(LogInfo, " matched {} '{}' of word '{}' (variant '{}'), {} vids, {}%", suffixLen ? "suffix" : "prefix", wordIt->first,
				   suffixWord, variant.pattern, holder_.GetWordById(wordId).vids.size(), proc);
		}
	}

	if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
		std::string limitString;
		if (vids >= vidsLimit) {
			logFmt(LogInfo, "Terminating suffix loop on limit ({}). Current variant is '{}{}{}'", vidsLimit, variant.opts.suff ? "*" : "",
				   pattern, variant.opts.pref ? "*" : "");

			limitString = fmt::format(". Lookup terminated by VIDs limit({})", vidsLimit);
		}
		logFmt(LogInfo, "Lookup variant '{}' ({}%), matched {} suffixes, with {} vids, skipped {}, excluded {}{}", pattern, variant.proc,
			   matched, vids, skipped, excludedCnt, limitString);
	}

	return totalORVids;
}

template <typename IdCont>
template <FtUseExternStatuses useExternSt>
void Selector<IdCont>::processLowRelVariants(size_t& totalORVids, h_vector<FtBoundVariantEntry, 4>& lowRelVariants,
											 const FtMergeStatuses::Statuses& mergeStatuses,
											 std::vector<ft::TermResults<IdCont>>& rawResults,
											 h_vector<std::optional<ft::FoundWordsProcsType>, 4>& termsWordsProcs) {
	if (lowRelVariants.empty()) {
		return;
	}

	// Add words from low relevancy variants, ordered by length & proc
	if constexpr (kVariantsWithDifLength) {
		boost::sort::pdqsort(lowRelVariants.begin(), lowRelVariants.end(), [](FtBoundVariantEntry& l, FtBoundVariantEntry& r) noexcept {
			const auto lenL = l.GetLenCached();
			const auto lenR = r.GetLenCached();
			return lenL == lenR ? l.proc > r.proc : lenL > lenR;
		});
	} else {
		boost::sort::pdqsort_branchless(lowRelVariants.begin(), lowRelVariants.end(),
										[](FtBoundVariantEntry& l, FtBoundVariantEntry& r) noexcept { return l.proc > r.proc; });
	}

	// Those number were taken from nowhere and probably will require some calibration later on
	const unsigned targetORLimit = 4 * holder_.cfg_->mergeLimit;
	const unsigned targetANDLimit = 2 * holder_.cfg_->mergeLimit;

	auto lastVariantLen = lowRelVariants[0].GetLenCached();
	for (auto& variant : lowRelVariants) {
		if constexpr (kVariantsWithDifLength) {
			if (variant.GetLenCached() != lastVariantLen) {
				if (totalORVids >= targetORLimit) {
					if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
						logFmt(LogInfo, "Terminating on target OR limit. Current variant is '{}{}{}'", variant.opts.suff ? "*" : "",
							   variant.pattern, variant.opts.pref ? "*" : "");
					}
					break;
				}
				lastVariantLen = variant.GetLenCached();
			}
		}

		if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
			logFmt(LogInfo, "Handling '{}{}{}' as variant with low relevancy", variant.opts.suff ? "*" : "", variant.pattern,
				   variant.opts.pref ? "*" : "");
		}

		if (variant.opts.op == OpOr) {
			if (totalORVids < targetORLimit) {
				assertrx_dbg(termsWordsProcs[variant.rawResultIdx].has_value());
				// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
				auto& termWordsProcs = termsWordsProcs[variant.rawResultIdx].value();
				int remainingLimit = targetORLimit - totalORVids;
				for (auto& step : holder_.steps) {
					totalORVids += processStepVariants<useExternSt>(step, variant, mergeStatuses, remainingLimit,
																	rawResults[variant.rawResultIdx], termWordsProcs);
				}
			}
		} else {
			auto& res = rawResults[variant.rawResultIdx];
			if (res.idsCnt < targetANDLimit) {
				int remainingLimit = targetANDLimit - res.idsCnt;
				assertrx_dbg(termsWordsProcs[variant.rawResultIdx].has_value());
				// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
				auto& termWordsProcs = termsWordsProcs[variant.rawResultIdx].value();
				for (auto& step : holder_.steps) {
					totalORVids += processStepVariants<useExternSt>(step, variant, mergeStatuses, remainingLimit,
																	rawResults[variant.rawResultIdx], termWordsProcs);
				}
			}
		}
	}
}

template <typename IdCont>
size_t Selector<IdCont>::TyposHandler::Process(const std::wstring& pattern, const h_vector<std::wstring, 8>& variantsForTypos,
											   const DataHolder<IdCont>& holder, ft::TermResults<IdCont>& res,
											   ft::FoundWordsProcsType& wordsProcs) {
	size_t totalVids = 0;
	std::wstring buf;
	for (auto& step : holder.steps) {
		struct {
			ft::TermResults<IdCont>& res;
			ft::FoundWordsProcsType& wordsProcs;
			const DataHolder<IdCont>& holder;
			const DataHolder<IdCont>::CommitStep& step;
			size_t& totalVids;
			int matched, skipped, vids;
		} ctx{res, wordsProcs, holder, step, totalVids, 0, 0, 0};

		auto callback = [&ctx, this](std::wstring_view typo, const TyposVec& positions, std::wstring_view typoPattern) {
			size_t maxTypos = ctx.holder.cfg_->maxTypos;

			const auto typoRng = ctx.step.typos_.TyposRange(TyposMap::CalcHash(typo));
			for (auto typoIt = typoRng.first; typoIt != typoRng.second; ++typoIt) {
				const WordTypo wordTypo = *typoIt;

				if (wordTypo.positions.size() + positions.size() > maxTypos) {
					continue;
				}

				const auto& step = ctx.holder.GetStep(wordTypo.word);
				auto wordIdSfx = ctx.holder.GetSuffixWordId(wordTypo.word, step);
				std::string_view word(step.suffixes_.word_at(wordIdSfx));

				if (!wordTypo.CheckMatch(word, typo)) {
					continue;
				}

				if (positions.size() > wordTypo.positions.size() && (positions.size() - wordTypo.positions.size()) > int(maxExtraLetts_)) {
					if (logLevel_ >= LogTrace) [[unlikely]] {
						logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}': to many extra letters ({})"), utf16_to_utf8(typo),
							   word, positions.size() - wordTypo.positions.size());
					}
					++ctx.skipped;
					continue;
				}
				if (wordTypo.positions.size() > positions.size() &&
					(wordTypo.positions.size() - positions.size()) > int(maxMissingLetts_)) {
					if (logLevel_ >= LogTrace) [[unlikely]] {
						logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}': to many missing letters ({})"), utf16_to_utf8(typo),
							   word, wordTypo.positions.size() - positions.size());
					}
					++ctx.skipped;
					continue;
				}
				if (!checkMaxTyposDist(wordTypo, positions)) {
					const bool needMaxLettPermCheck = useMaxTypoDist_ && (!useMaxLettPermDist_ || maxLettPermDist_ > maxTypoDist_);
					if (!needMaxLettPermCheck || !checkMaxLettPermDist(word, wordTypo, typoPattern, positions)) {
						if (logLevel_ >= LogTrace) [[unlikely]] {
							logFmt(LogInfo, fmt::runtime(" skipping typo '{}' of word '{}' due to max_typos_distance settings"),
								   utf16_to_utf8(typo), word);
						}
						++ctx.skipped;
						continue;
					}
				}

				const uint8_t wordLength = step.suffixes_.word_len_at(wordIdSfx);
				const int tcount = std::max(positions.size(), wordTypo.positions.size());  // Each letter switch equals to 1 typo
				const auto& rankingConfig = ctx.holder.cfg_->rankingConfig;
				const float proc =
					std::max<float>(rankingConfig.typo - tcount * rankingConfig.typoPenalty /
															 std::max<float>((wordLength - tcount) / 3.f,
																			 BaseFTConfig::BaseRankingConfig::kMinProcAfterPenalty),
									1.f);

				const auto [it, emplaced] = ctx.wordsProcs.try_emplace(wordTypo.word, proc);
				if (emplaced) {
					const auto& hword = ctx.holder.GetWordById(wordTypo.word);
					std::string typoUTF8 = utf16_to_utf8(typo);
					if (logLevel_ >= LogTrace) [[unlikely]] {
						logFmt(LogInfo, fmt::runtime(" matched typo '{}' of word '{}', {} ids, {}%"), typoUTF8, word, hword.vids.size(),
							   proc);
					}
					ctx.res.subtermsResults.emplace_back(hword.vids, std::move(typoUTF8), wordTypo.word, proc,
														 typename ft::SubtermResults<IdCont>::HoldT{});
					ctx.res.idsCnt += hword.vids.size();

					++ctx.matched;
					ctx.vids += hword.vids.size();
					ctx.totalVids += hword.vids.size();
				} else {
					++ctx.skipped;
					it->second = std::max(it->second, proc);
				}
			}
		};

		mktypos(pattern, holder.cfg_->MaxTyposInWord(), holder.cfg_->maxTypoLen, callback, buf);
		for (const auto& varForTypo : variantsForTypos) {
			mktypos(varForTypo, holder.cfg_->MaxTyposInWord(), holder.cfg_->maxTypoLen, callback, buf);
		}

		if (holder.cfg_->logLevel >= LogInfo) [[unlikely]] {
			logFmt(LogInfo, "Lookup typos, matched {} typos, with {} vids, skipped {}", ctx.matched, ctx.vids, ctx.skipped);
		}
	}
	return totalVids;
}

RX_ALWAYS_INLINE unsigned uabs(int a) { return unsigned(std::abs(a)); }

template <typename IdCont>
bool Selector<IdCont>::TyposHandler::checkMaxTyposDist(const WordTypo& found, const TyposVec& current) {
	static_assert(kMaxTyposInWord <= 2, "Code in this function is expecting specific size of the typos positions arrays");
	if (!useMaxTypoDist_ || found.positions.size() == 0) {
		return true;
	}
	switch (current.size()) {
		case 0:
			return true;
		case 1: {
			const auto curP0 = current[0];
			const auto foundP0 = found.positions[0];

			if (found.positions.size() == 1) {
				// current.len == 1 && found.len == 1. I.e. exactly one letter must be changed and moved up to maxTypoDist_ value
				return uabs(curP0 - foundP0) <= maxTypoDist_;
			}
			// current.len == 1 && found.len == 2. I.e. exactly one letter must be changed and moved up to maxTypoDist_ value and the
			// other letter is missing in 'current'
			auto foundLeft = foundP0;
			auto foundRight = found.positions[1];
			if (foundLeft > foundRight) {
				std::swap(foundLeft, foundRight);
			}
			return uabs((foundRight - 1) - curP0) <= maxTypoDist_ || uabs(foundLeft - curP0) <= maxTypoDist_;
		}
		case 2: {
			const auto foundP0 = found.positions[0];
			const auto curP0 = current[0];
			const auto curP1 = current[1];

			if (found.positions.size() == 1) {
				// current.len == 2 && found.len == 1. I.e. exactly one letter must be changed and moved up to maxTypoDist_ value and
				// 'current' also has one extra letter
				auto curLeft = curP0;
				auto curRight = curP1;
				if (curLeft > curRight) {
					std::swap(curLeft, curRight);
				}

				return uabs((curRight - 1) - foundP0) <= maxTypoDist_ || uabs(curLeft - foundP0) <= maxTypoDist_;
			}

			// current.len == 2 && found.len == 2. I.e. exactly two letters must be changed and moved up to maxTypoDist_ value
			const auto foundP1 = found.positions[1];
			return ((uabs(curP0 - foundP0) <= maxTypoDist_) && (uabs(curP1 - foundP1) <= maxTypoDist_)) ||
				   ((uabs(curP0 - foundP1) <= maxTypoDist_) && (uabs(curP1 - foundP0) <= maxTypoDist_));
		}
		default:
			throw Error(errLogic, "Unexpected typos count: {}", current.size());
	}
}

template <typename IdCont>
bool Selector<IdCont>::TyposHandler::checkMaxLettPermDist(std::string_view foundWord, const WordTypo& found, std::wstring_view currentWord,
														  const TyposVec& current) {
	if (found.positions.size() == 0) {
		return true;
	}
	static_assert(kMaxTyposInWord <= 2, "Code in this function is expecting specific size of the typos positions arrays");
	utf8_to_utf16(foundWord, foundWordUTF16_);
	switch (current.size()) {
		case 0:
			throw Error(errLogic, "Internal logic error. Unable to handle max_typos_distance or max_symbol_permutation_distance settings");
		case 1: {
			const auto foundP0 = found.positions[0];
			const auto curP0 = current[0];
			if (foundWordUTF16_[foundP0] == currentWord[curP0] && (!useMaxLettPermDist_ || uabs(curP0 - foundP0) <= maxLettPermDist_)) {
				return true;
			}
			const auto foundP1 = found.positions[1];
			return (found.positions.size() == 2 && foundWordUTF16_[foundP1] == currentWord[curP0] &&
					(!useMaxLettPermDist_ || uabs(curP0 - foundP1) <= maxLettPermDist_));

			if (found.positions.size() == 1) {
				// current.len == 1 && found.len == 1. I.e. exactly one letter must be moved up to maxLettPermDist_ value
				return (foundWordUTF16_[foundP0] == currentWord[curP0]) &&
					   (!useMaxLettPermDist_ || uabs(curP0 - foundP0) <= maxLettPermDist_);
			}
			// current.len == 1 && found.len == 2. I.e. exactly one letter must be moved up to maxLettPermDist_ value and the other
			// letter is missing in 'current'
			auto foundLeft = foundP0;
			auto foundRight = found.positions[1];
			if (foundLeft > foundRight) {
				std::swap(foundLeft, foundRight);
			}

			// Right letter position requires correction for the comparison with distance, but not for the letter itself
			const auto foundRightLetter = foundWordUTF16_[foundRight--];
			const auto foundLeftLetter = foundWordUTF16_[foundLeft];
			const auto curP0Letter = currentWord[curP0];
			return (foundRightLetter == curP0Letter && (!useMaxLettPermDist_ || uabs(foundRight - curP0) <= maxLettPermDist_)) ||
				   (foundLeftLetter == curP0Letter && (!useMaxLettPermDist_ || uabs(foundLeft - curP0) <= maxLettPermDist_));
		}
		case 2: {
			const auto foundP0 = found.positions[0];
			const auto curP0 = current[0];
			const auto curP1 = current[1];

			if (found.positions.size() == 1) {
				// current.len == 2 && found.len == 1. I.e. exactly one letter must be moved up to maxLettPermDist_ value and 'current'
				// also has one extra letter
				auto curLeft = curP0;
				auto curRight = curP1;
				if (curLeft > curRight) {
					std::swap(curLeft, curRight);
				}
				// Right letter position requires correction for the comparison with distance, but not for the letter itself
				const auto curRightLetter = currentWord[curRight--];
				const auto curLeftLetter = currentWord[curLeft];
				const auto foundP0Letter = foundWordUTF16_[foundP0];
				return (foundP0Letter == curRightLetter && (!useMaxLettPermDist_ || uabs((curRight - 1) - foundP0) <= maxLettPermDist_)) ||
					   (foundP0Letter == curLeftLetter && (!useMaxLettPermDist_ || uabs(curLeft - foundP0) <= maxLettPermDist_));
			}

			// current.len == 2 && found.len == 2. I.e. two letters must be moved up to maxLettPermDist_ value
			const auto foundP1 = found.positions[1];
			const auto foundP0Letter = foundWordUTF16_[foundP0];
			const auto foundP1Letter = foundWordUTF16_[foundP1];
			const auto curP0Letter = currentWord[curP0];
			const auto curP1Letter = currentWord[curP1];
			const bool permutationOn00 =
				(foundP0Letter == curP0Letter && (!useMaxLettPermDist_ || uabs(curP0 - foundP0) <= maxLettPermDist_));
			const bool permutationOn11 =
				(foundP1Letter == curP1Letter && (!useMaxLettPermDist_ || uabs(curP1 - foundP1) <= maxLettPermDist_));
			if (permutationOn00 && permutationOn11) {
				return true;
			}
			const bool permutationOn01 =
				(foundP0Letter == curP1Letter && (!useMaxLettPermDist_ || uabs(curP1 - foundP0) <= maxLettPermDist_));
			const bool permutationOn10 =
				(foundP1Letter == curP0Letter && (!useMaxLettPermDist_ || uabs(curP0 - foundP1) <= maxLettPermDist_));
			if (permutationOn01 && permutationOn10) {
				return true;
			}
			const bool switchOn00 = (uabs(curP0 - foundP0) <= maxTypoDist_);
			if (permutationOn11 && switchOn00) {
				return true;
			}
			const bool switchOn11 = (uabs(curP1 - foundP1) <= maxTypoDist_);
			if (permutationOn00 && switchOn11) {
				return true;
			}
			const bool switchOn10 = (uabs(curP0 - foundP1) <= maxTypoDist_);
			if (permutationOn01 && switchOn10) {
				return true;
			}
			const bool switchOn01 = (uabs(curP1 - foundP0) <= maxTypoDist_);
			return permutationOn10 && switchOn01;
		}
		default:
			throw Error(errLogic, "Unexpected typos count: {}", current.size());
	}
}

template <typename IdCont>
void Selector<IdCont>::printVariants(const std::vector<FtVariantEntry>& variants, const h_vector<FtBoundVariantEntry, 4>& lowRelVariants,
									 const ft::TermResults<IdCont>& res) {
	WrSerializer wrSer;
	wrSer << "variants: [";
	for (auto& variant : variants) {
		if (&variant != &*variants.begin()) {
			wrSer << ", ";
		}
		wrSer << variant.pattern;
	}
	wrSer << "], variants_with_low_relevancy: [";
	for (auto& variant : lowRelVariants) {
		if (&variant != &*lowRelVariants.begin()) {
			wrSer << ", ";
		}
		wrSer << variant.pattern;
	}
	wrSer << "], typos: [";
	if (res.term.Opts().typos) {
		std::wstring buf;
		mktypos(
			res.term.Pattern(), holder_.cfg_->MaxTyposInWord(), holder_.cfg_->maxTypoLen,
			[&wrSer](std::wstring_view typo, const TyposVec& positions, std::wstring_view) {
				wrSer << utf16_to_utf8(typo);
				wrSer << ":(";
				for (unsigned j = 0, sz = positions.size(); j < sz; ++j) {
					if (j) {
						wrSer << ',';
					}
					wrSer << positions[j];
				}
				wrSer << "), ";
			},
			buf);
	}
	logFmt(LogInfo, "Variants: [{}]", wrSer.Slice());
}

template class Selector<PackedIdRelVec>;
template ft::MergeData Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, ft::MergeData>(FtDSLQuery&&, bool, RankSortType,
																								 FtMergeStatuses::Statuses&&,
																								 const RdxContext&);
template ft::MergeDataAreas<Area> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, ft::MergeDataAreas<Area>>(
	FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&, const RdxContext&);
template ft::MergeDataAreas<AreaDebug> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, ft::MergeDataAreas<AreaDebug>>(
	FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&, const RdxContext&);

template ft::MergeData Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																				   FtMergeStatuses::Statuses&&, const RdxContext&);
template ft::MergeDataAreas<Area> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																							  FtMergeStatuses::Statuses&&,
																							  const RdxContext&);
template ft::MergeDataAreas<AreaDebug> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																								   FtMergeStatuses::Statuses&&,
																								   const RdxContext&);

template class Selector<IdRelVec>;
template ft::MergeData Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&,
																			const RdxContext&);
template ft::MergeDataAreas<Area> Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType,
																					   FtMergeStatuses::Statuses&&, const RdxContext&);
template ft::MergeDataAreas<AreaDebug> Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType,
																							FtMergeStatuses::Statuses&&, const RdxContext&);

template ft::MergeData Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&,
																			 const RdxContext&);
template ft::MergeDataAreas<Area> Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																						FtMergeStatuses::Statuses&&, const RdxContext&);
template ft::MergeDataAreas<AreaDebug> Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																							 FtMergeStatuses::Statuses&&,
																							 const RdxContext&);

}  // namespace reindexer
