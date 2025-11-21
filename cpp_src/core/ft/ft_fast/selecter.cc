#include "selecter.h"
#include "mergerimpl.h"
#include "tools/scope_guard.h"
#include "tools/serializer.h"

namespace reindexer {
// Minimal relevant length of the stemmer's term
constexpr int kMinStemRellevantLen = 3;
// Max length of the stemming result, which will be skipped
constexpr int kMaxStemSkipLen = 1;
constexpr bool kVariantsWithDifLength = (kMinStemRellevantLen - kMaxStemSkipLen) > 2;

template <typename IdCont>
void Selector<IdCont>::applyStemmers(const std::string& pattern, int proc, const std::vector<std::string>& langs, const FtDslOpts& termOpts,
									 bool keepSuffForStemmedVars, std::vector<FtVariantEntry>& variants,
									 h_vector<FtBoundVariantEntry, 4>* lowRelVariants, std::string& buffer) {
	std::string& stemstr = buffer;

	if (termOpts.op == OpNot && termOpts.suff) {
		// More strict match for negative (excluding) suffix terms
		if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
			logFmt(LogInfo, "Skipping stemming for '{}{}{}'", termOpts.suff ? "*" : "", pattern, termOpts.pref ? "*" : "");
		}
		return;
	}

	for (auto& lang : langs) {
		auto stemIt = holder_.stemmers_.find(lang);
		if (stemIt == holder_.stemmers_.end()) {
			throw Error(errParams, "Stemmer for language {} is not available", lang);
		}
		stemstr = "";
		stemIt->second.stem(pattern, stemstr);
		if (pattern == stemstr || stemstr.empty()) {
			continue;
		}

		const auto charsCount = getUTF8StringCharactersCount(stemstr);
		if (charsCount <= kMaxStemSkipLen) {
			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Skipping too short stemmer's term '{}{}*'", termOpts.suff && keepSuffForStemmedVars ? "*" : "", stemstr);
			}
			continue;
		}
		FtDslOpts opts = termOpts;
		opts.pref = true;

		if (!keepSuffForStemmedVars) {
			opts.suff = false;
		}

		const auto charCount = getUTF8StringCharactersCount(stemstr);
		if (charCount >= kMinStemRellevantLen || !lowRelVariants) {
			variants.emplace_back(
				std::move(stemstr), std::move(opts),
				std::max(proc - holder_.cfg_->rankingConfig.stemmerPenalty, BaseFTConfig::BaseRankingConfig::kMinProcAfterPenalty),
				charsCount);
		} else {
			lowRelVariants->emplace_back(
				std::move(stemstr), std::move(opts),
				std::max(proc - holder_.cfg_->rankingConfig.stemmerPenalty, BaseFTConfig::BaseRankingConfig::kMinProcAfterPenalty),
				charsCount);
		}
	}
}

template <typename IdCont>
void Selector<IdCont>::prepareVariants(std::vector<FtVariantEntry>& variants, h_vector<FtBoundVariantEntry, 4>* lowRelVariants,
									   size_t termIdx, const std::vector<std::string>& langs, FtDSLQuery& dsl,
									   std::vector<SynonymsDsl>* synonymsDsl, std::vector<std::wstring>* variantsForTypos) {
	const BaseFTConfig::BaseRankingConfig& rankingConfig = holder_.cfg_->rankingConfig;
	const std::wstring& pattern = dsl.GetTerm(termIdx).pattern;
	FtDslOpts& opts = dsl.GetTerm(termIdx).opts;
	const StopWordsSetT& stopWords = holder_.cfg_->stopWords;

	if (pattern.empty()) {
		return;
	}

	variants.resize(0);
	std::string variantPattern, buffer;
	utf16_to_utf8(pattern, variantPattern);
	variants.emplace_back(variantPattern, opts, rankingConfig.fullMatch, -1);
	if (!opts.exact) {
		applyStemmers(variantPattern, rankingConfig.fullMatch, langs, opts, true, variants, lowRelVariants, buffer);
	}

	if (holder_.cfg_->enableNumbersSearch && opts.number) {
		return;
	}

	if (!synonymsDsl) {
		return;
	}

	ITokenFilter::ResultsStorage allVariants;
	fast_hash_map<std::wstring, size_t> patternsUsed;

	if (holder_.cfg_->enableTranslit && !opts.exact) {
		holder_.translit_->GetVariants(pattern, allVariants, rankingConfig.translit, patternsUsed);
	}

	if (holder_.cfg_->enableKbLayout && !opts.exact) {
		holder_.kbLayout_->GetVariants(pattern, allVariants, rankingConfig.kblayout, patternsUsed);
	}

	if (opts.op != OpNot) {
		holder_.synonyms_->GetVariants(pattern, allVariants, rankingConfig.synonyms, patternsUsed);
		holder_.synonyms_->AddOneToManySynonyms(pattern, opts, dsl, termIdx, *synonymsDsl, rankingConfig.synonyms);
	}

	if (!opts.exact) {
		ITokenFilter::ResultsStorage variantsDelimited;
		fast_hash_map<std::wstring, size_t> variantPatternsUsed;
		variantsDelimited.reserve(20);
		holder_.compositeWordsSplitter_->GetVariants(pattern, variantsDelimited, rankingConfig.delimited, variantPatternsUsed);
		for (auto& v : variantsDelimited) {
			if (opts.typos) {
				if (variantsForTypos) {
					variantsForTypos->emplace_back(v.pattern);
				}
			}

			holder_.synonyms_->GetVariants(v.pattern, allVariants, std::min(rankingConfig.synonyms, rankingConfig.delimited), patternsUsed);

			{
				const bool prefSaved = opts.pref;
				if (v.prefAndStemmersForbidden) {
					opts.pref = false;
				}
				auto prefGuard = reindexer::MakeScopeGuard([&]() { opts.pref = prefSaved; });

				holder_.synonyms_->AddOneToManySynonyms(v.pattern, opts, dsl, termIdx, *synonymsDsl,
														std::min(rankingConfig.synonyms, rankingConfig.delimited));
			}

			AddOrUpdateVariant(allVariants, patternsUsed, std::move(v));
		}
	}

	std::string patternWithoutDelims;
	for (auto& v : allVariants) {
		utf16_to_utf8(v.pattern, variantPattern);
		const bool prefSaved = opts.pref;
		if (v.prefAndStemmersForbidden) {
			opts.pref = false;
		}
		auto prefGuard = reindexer::MakeScopeGuard([&]() { opts.pref = prefSaved; });

		// stop words doesn't present in index, so we need to check it only in case of prefix or suffix search
		if (opts.pref || opts.suff) {
			if (variantPattern.empty()) {
				continue;
			}

			if (auto it = stopWords.find(variantPattern); it != stopWords.end() && it->type == StopWord::Type::Stop) {
				continue;
			}

			if (holder_.cfg_->splitOptions.ContainsDelims(variantPattern)) {
				holder_.cfg_->splitOptions.RemoveDelims(variantPattern, patternWithoutDelims);
				if (auto it = stopWords.find(patternWithoutDelims); it != stopWords.end() && it->type == StopWord::Type::Stop) {
					continue;
				}
			}
		}

		variants.emplace_back(variantPattern, opts, v.proc, -1);
		if (!opts.exact && !v.prefAndStemmersForbidden) {
			applyStemmers(variantPattern, v.proc, langs, opts, false, variants, lowRelVariants, buffer);
		}
	}
}

// RX_NO_INLINE just for build test purpose. Do not expect any effect here
template <typename IdCont>
template <FtUseExternStatuses useExternSt, typename MergedDataType>
MergedDataType Selector<IdCont>::Process(FtDSLQuery&& dsl, bool inTransaction, RankSortType rankSortType,
										 FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx) {
	FtSelectContext ctx;
	ctx.rawResults.reserve(dsl.NumTerms());
	// STEP 2: Search dsl terms for each variant
	std::vector<SynonymsDsl> synonymsDsl;
	holder_.synonyms_->AddManyToManySynonyms(dsl, synonymsDsl, holder_.cfg_->rankingConfig.synonyms);
	if (!inTransaction) {
		ThrowOnCancel(rdxCtx);
	}

	for (size_t i = 0; i < dsl.NumTerms(); ++i) {
		const auto irrVariantsCount = ctx.lowRelVariants.size();
		ctx.variantsForTypos.resize(0);
		// Prepare term variants (original + translit + stemmed + kblayout + synonym)
		this->prepareVariants(ctx.variants, &ctx.lowRelVariants, i, holder_.cfg_->stemmers, dsl, &synonymsDsl, &ctx.variantsForTypos);
		auto v = ctx.GetWordsMapPtr(dsl.GetTerm(i).opts);
		ctx.rawResults.emplace_back(std::move(dsl.GetTerm(i)), v);
		TextSearchResults<IdCont>& res = ctx.rawResults.back();

		if (irrVariantsCount != ctx.lowRelVariants.size()) {
			ctx.rawResults.back().SwitchToInternalWordsMap();
		}
		for (unsigned j = irrVariantsCount; j < ctx.lowRelVariants.size(); ++j) {
			ctx.lowRelVariants[j].rawResultIdx = ctx.rawResults.size() - 1;
		}

		if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
			printVariants(ctx, res);
		}

		processVariants<useExternSt>(ctx, mergeStatuses);
		if (res.term.opts.typos) {
			// Lookup typos from typos_ map and fill results
			TyposHandler h(*holder_.cfg_);
			size_t vidsCount = h.Process(ctx.rawResults, holder_, res.term.pattern, ctx.variantsForTypos);
			if (res.term.opts.op == OpOr) {
				ctx.totalORVids += vidsCount;
			}
		}
	}

	std::vector<TextSearchResults<IdCont>> results;
	size_t reserveSize = ctx.rawResults.size();
	for (const SynonymsDsl& synDsl : synonymsDsl) {
		reserveSize += synDsl.dsl.NumTerms();
	}
	results.reserve(reserveSize);
	std::vector<size_t> synonymsBounds;
	synonymsBounds.reserve(synonymsDsl.size());
	if (!inTransaction) {
		ThrowOnCancel(rdxCtx);
	}
	for (SynonymsDsl& synDsl : synonymsDsl) {
		FtSelectContext synCtx;
		synCtx.rawResults.reserve(synDsl.dsl.NumTerms());
		for (size_t i = 0; i < synDsl.dsl.NumTerms(); ++i) {
			prepareVariants(synCtx.variants, nullptr, i, holder_.cfg_->stemmers, synDsl.dsl, nullptr, nullptr);
			if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
				WrSerializer wrSer;
				for (auto& variant : synCtx.variants) {
					if (&variant != &*synCtx.variants.begin()) {
						wrSer << ", ";
					}
					wrSer << variant.pattern;
				}
				logFmt(LogInfo, "Multiword synonyms variants: [{}]", wrSer.Slice());
			}
			auto v = ctx.GetWordsMapPtr(synDsl.dsl.GetTerm(i).opts);
			synCtx.rawResults.emplace_back(std::move(synDsl.dsl.GetTerm(i)), v);
			if (synCtx.rawResults.back().term.opts.op == OpAnd) {
				ctx.rawResults.back().SwitchToInternalWordsMap();
			}
			processVariants<useExternSt>(synCtx, mergeStatuses);
		}
		for (size_t idx : synDsl.termsIdx) {
			assertrx(idx < ctx.rawResults.size());
			ctx.rawResults[idx].synonyms.push_back(results.size());
			ctx.rawResults[idx].synonymsGroups.push_back(synonymsBounds.size());
		}
		for (auto& res : synCtx.rawResults) {
			results.emplace_back(std::move(res));
		}
		synonymsBounds.push_back(results.size());
	}
	processLowRelVariants<useExternSt>(ctx, mergeStatuses);
	// Typos for terms with low relevancy will not be processed

	for (auto& res : ctx.rawResults) {
		results.emplace_back(std::move(res));
	}

	const auto maxMergedSize = std::min(size_t(holder_.cfg_->mergeLimit), ctx.totalORVids);
	if (maxMergedSize < 0xFFFF) {
		return mergeResults<uint16_t, MergedDataType>(results, ctx.totalORVids, synonymsBounds, inTransaction, rankSortType, mergeStatuses,
													  rdxCtx);
	} else if (maxMergedSize < 0xFFFFFFFF) {
		return mergeResults<uint32_t, MergedDataType>(results, ctx.totalORVids, synonymsBounds, inTransaction, rankSortType, mergeStatuses,
													  rdxCtx);
	} else {
		assertrx_throw(false);
	}
	return MergedDataType();
}

template <typename IdCont>
template <typename MergedOffsetT, typename MergedDataType>
MergedDataType Selector<IdCont>::mergeResults(std::vector<TextSearchResults<IdCont>>& results, size_t totalORVids,
											  const std::vector<size_t>& synonymsBounds, bool inTransaction, RankSortType rankSortType,
											  FtMergeStatuses::Statuses& mergeStatuses, const RdxContext& rdxCtx) {
	Merger<IdCont, MergedDataType, MergedOffsetT> merger(holder_, mergeStatuses, fieldSize_, maxAreasInDoc_, inTransaction, rdxCtx);
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
void Selector<IdCont>::processStepVariants(FtSelectContext& ctx, typename DataHolder<IdCont>::CommitStep& step,
										   const FtVariantEntry& variant, unsigned curRawResultIdx,
										   const FtMergeStatuses::Statuses& mergeStatuses, int vidsLimit) {
	auto& res = ctx.rawResults[curRawResultIdx];
	if (variant.opts.op == OpAnd) {
		res.foundWords->clear();
	}
	auto& pattern = variant.pattern;
	auto& suffixes = step.suffixes_;

	int matched = 0, skipped = 0, vids = 0, excludedCnt = 0;

	auto finalLogging = reindexer::MakeScopeGuard([&]() {
		if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
			std::string limitString;
			if (vids >= vidsLimit) {
				logFmt(LogInfo, "Terminating suffix loop on limit ({}). Current variant is '{}{}{}'", vidsLimit,
					   variant.opts.suff ? "*" : "", pattern, variant.opts.pref ? "*" : "");

				limitString = fmt::format(". Lookup terminated by VIDs limit({})", vidsLimit);
			}
			logFmt(LogInfo, "Lookup variant '{}' ({}%), matched {} suffixes, with {} vids, skipped {}, excluded {}{}", pattern,
				   variant.proc, matched, vids, skipped, excludedCnt, limitString);
		}
	});

	// Walk current variant in suffixes array and fill results

	bool needStop = false;
	for (auto wordIt = suffixes.lower_bound(pattern); wordIt != suffixes.end() && vids < vidsLimit && !needStop; ++wordIt) {
		needStop = wordIt.lcp() < int(pattern.length());

		const WordIdType wordId = wordIt->second;
		const auto& word = holder_.GetWordById(wordId);

		if (useExternSt == FtUseExternStatuses::Yes && allVidsExcluded(mergeStatuses, word.vids)) {
			++excludedCnt;
			continue;
		}

		const uint32_t suffixWordId = holder_.GetSuffixWordId(wordId, step);
		const std::string::value_type* suffixWord = suffixes.word_at(suffixWordId);
		const size_t suffixWordLength = suffixes.word_len_at(suffixWordId);
		const ptrdiff_t suffixLen = wordIt->first - suffixWord;

		if (!variant.opts.suff && wordIt->first != suffixWord) {
			continue;
		}
		if (!variant.opts.pref && suffixWordLength != pattern.length() + suffixLen) {
			return;
		}

		const int matchDif = std::abs(long(suffixWordLength - pattern.length() + suffixLen));
		const int proc = std::max<int>(variant.proc - holder_.cfg_->partialMatchDecrease * matchDif / std::max<size_t>(pattern.length(), 3),
									   suffixLen ? holder_.cfg_->rankingConfig.suffixMin : holder_.cfg_->rankingConfig.prefixMin);

		const auto it = res.foundWords->find(wordId);
		if (it != res.foundWords->end() && it->second.first == curRawResultIdx) {
			if (ctx.rawResults[it->second.first][it->second.second].proc < proc) {
				ctx.rawResults[it->second.first][it->second.second].proc = proc;
			}
			skipped++;
			continue;
		}

		res.push_back({&word.vids, wordIt->first, static_cast<float>(proc)});
		res.idsCnt += word.vids.size();
		(*res.foundWords)[wordId] = std::make_pair(curRawResultIdx, res.size() - 1);
		matched++;
		vids += word.vids.size();
		if (variant.opts.op == OpOr) {
			ctx.totalORVids += word.vids.size();
		}

		if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
			logFmt(LogInfo, " matched {} '{}' of word '{}' (variant '{}'), {} vids, {}%", suffixLen ? "suffix" : "prefix", wordIt->first,
				   suffixWord, variant.pattern, holder_.GetWordById(wordId).vids.size(), proc);
		}
	}
}

template <typename IdCont>
template <FtUseExternStatuses useExternSt>
void Selector<IdCont>::processVariants(FtSelectContext& ctx, const FtMergeStatuses::Statuses& mergeStatuses) {
	for (const FtVariantEntry& variant : ctx.variants) {
		for (auto& step : holder_.steps) {
			processStepVariants<useExternSt>(ctx, step, variant, ctx.rawResults.size() - 1, mergeStatuses, std::numeric_limits<int>::max());
		}
	}
}

template <typename IdCont>
template <FtUseExternStatuses useExternSt>
void Selector<IdCont>::processLowRelVariants(FtSelectContext& ctx, const FtMergeStatuses::Statuses& mergeStatuses) {
	if (ctx.lowRelVariants.empty()) {
		return;
	}

	// Add words from low relevancy variants, ordered by length & proc
	if constexpr (kVariantsWithDifLength) {
		boost::sort::pdqsort(ctx.lowRelVariants.begin(), ctx.lowRelVariants.end(),
							 [](FtBoundVariantEntry& l, FtBoundVariantEntry& r) noexcept {
								 const auto lenL = l.GetLenCached();
								 const auto lenR = r.GetLenCached();
								 return lenL == lenR ? l.proc > r.proc : lenL > lenR;
							 });
	} else {
		boost::sort::pdqsort_branchless(ctx.lowRelVariants.begin(), ctx.lowRelVariants.end(),
										[](FtBoundVariantEntry& l, FtBoundVariantEntry& r) noexcept { return l.proc > r.proc; });
	}

	// Those number were taken from nowhere and probably will require some calibration later on
	const unsigned targetORLimit = 4 * holder_.cfg_->mergeLimit;
	const unsigned targetANDLimit = 2 * holder_.cfg_->mergeLimit;

	auto lastVariantLen = ctx.lowRelVariants[0].GetLenCached();
	for (auto& variant : ctx.lowRelVariants) {
		if constexpr (kVariantsWithDifLength) {
			if (variant.GetLenCached() != lastVariantLen) {
				if (ctx.totalORVids >= targetORLimit) {
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
			if (ctx.totalORVids < targetORLimit) {
				int remainingLimit = targetORLimit - ctx.totalORVids;
				for (auto& step : holder_.steps) {
					processStepVariants<useExternSt>(ctx, step, variant, variant.rawResultIdx, mergeStatuses, remainingLimit);
				}
			}
		} else {
			auto& res = ctx.rawResults[variant.rawResultIdx];
			if (res.idsCnt < targetANDLimit) {
				int remainingLimit = targetANDLimit - res.idsCnt;
				for (auto& step : holder_.steps) {
					processStepVariants<useExternSt>(ctx, step, variant, variant.rawResultIdx, mergeStatuses, remainingLimit);
				}
			}
		}
	}
}

template <typename IdCont>
size_t Selector<IdCont>::TyposHandler::Process(std::vector<TextSearchResults<IdCont>>& rawResults, const DataHolder<IdCont>& holder,
											   const std::wstring& pattern, const std::vector<std::wstring>& variantsForTypos) {
	TextSearchResults<IdCont>& res = rawResults.back();
	const unsigned curRawResultIdx = rawResults.size() - 1;

	size_t totalVids = 0;
	for (auto& step : holder.steps) {
		typos_context tctx[kMaxTyposInWord];
		const decltype(step.typosHalf_)* typoses[2]{&step.typosHalf_, &step.typosMax_};
		int matched = 0, skipped = 0, vids = 0;

		auto callback = [&res, &curRawResultIdx, &holder, &typoses, &totalVids, &matched, &skipped, &vids, this](
							std::string_view typo, int level, const typos_context::TyposVec& positions,
							const std::wstring_view typoPattern) {
			const size_t patternSize = utf16_to_utf8_size(typoPattern);
			for (const auto* typos : typoses) {
				const auto typoRng = typos->equal_range(typo);
				for (auto typoIt = typoRng.first; typoIt != typoRng.second; ++typoIt) {
					const WordTypo wordTypo = typoIt->second;
					const int tcount = std::max(positions.size(), wordTypo.positions.size());  // Each letter switch equals to 1 typo
					const auto& step = holder.GetStep(wordTypo.word);
					auto wordIdSfx = holder.GetSuffixWordId(wordTypo.word, step);
					if (positions.size() > wordTypo.positions.size() &&
						(positions.size() - wordTypo.positions.size()) > int(maxExtraLetts_)) {
						logTraceF(LogInfo, " skipping typo '{}' of word '{}': to many extra letters ({})", typoIt->first,
								  step.suffixes_.word_at(wordIdSfx), positions.size() - wordTypo.positions.size());
						++skipped;
						continue;
					}
					if (wordTypo.positions.size() > positions.size() &&
						(wordTypo.positions.size() - positions.size()) > int(maxMissingLetts_)) {
						logTraceF(LogInfo, " skipping typo '{}' of word '{}': to many missing letters ({})", typoIt->first,
								  step.suffixes_.word_at(wordIdSfx), wordTypo.positions.size() - positions.size());
						++skipped;
						continue;
					}
					if (!isWordFitMaxTyposDist(wordTypo, positions)) {
						const bool needMaxLettPermCheck = useMaxTypoDist_ && (!useMaxLettPermDist_ || maxLettPermDist_ > maxTypoDist_);
						if (!needMaxLettPermCheck ||
							!isWordFitMaxLettPerm(step.suffixes_.word_at(wordIdSfx), wordTypo, typoPattern, positions)) {
							logTraceF(LogInfo, " skipping typo '{}' of word '{}' due to max_typos_distance settings", typoIt->first,
									  step.suffixes_.word_at(wordIdSfx));
							++skipped;
							continue;
						}
					}

					const uint8_t wordLength = step.suffixes_.word_len_at(wordIdSfx);
					const int proc =
						std::max(holder.cfg_->rankingConfig.typo -
									 tcount * holder.cfg_->rankingConfig.typoPenalty /
										 std::max((wordLength - tcount) / 3, BaseFTConfig::BaseRankingConfig::kMinProcAfterPenalty),
								 1);
					const auto it = res.foundWords->find(wordTypo.word);
					if (it == res.foundWords->end() || it->second.first != curRawResultIdx) {
						const auto& hword = holder.GetWordById(wordTypo.word);
						res.push_back({&hword.vids, typoIt->first, static_cast<float>(proc)});
						res.idsCnt += hword.vids.size();
						res.foundWords->emplace(wordTypo.word, std::make_pair(curRawResultIdx, res.size() - 1));

						logTraceF(LogInfo, " matched typo '{}' of word '{}', {} ids, {}%", typoIt->first, step.suffixes_.word_at(wordIdSfx),
								  hword.vids.size(), proc);
						++matched;
						vids += hword.vids.size();
						totalVids += hword.vids.size();
					} else {
						++skipped;
					}
				}
				if (dontUseMaxTyposForBoth_ && level == 1 && typo.size() != patternSize) {
					return;
				}
			}
		};

		mktypos(tctx, pattern, maxTyposInWord_, holder.cfg_->maxTypoLen, callback);
		for (const auto& varForTypo : variantsForTypos) {
			mktypos(tctx, varForTypo, maxTyposInWord_, holder.cfg_->maxTypoLen, callback);
		}

		if (holder.cfg_->logLevel >= LogInfo) [[unlikely]] {
			logFmt(LogInfo, "Lookup typos, matched {} typos, with {} vids, skipped {}", matched, vids, skipped);
		}
	}
	return totalVids;
}

RX_ALWAYS_INLINE unsigned uabs(int a) { return unsigned(std::abs(a)); }

template <typename IdCont>
template <typename... Args>
void Selector<IdCont>::TyposHandler::logTraceF(int level, fmt::format_string<Args...> fmt, Args&&... args) {
	if (logLevel_ >= LogTrace) [[unlikely]] {
		logFmt(level, fmt::runtime(fmt), std::forward<Args>(args)...);
	}
}

template <typename IdCont>
bool Selector<IdCont>::TyposHandler::isWordFitMaxTyposDist(const WordTypo& found, const typos_context::TyposVec& current) {
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
bool Selector<IdCont>::TyposHandler::isWordFitMaxLettPerm(const std::string_view foundWord, const WordTypo& found,
														  const std::wstring_view currentWord, const typos_context::TyposVec& current) {
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
void Selector<IdCont>::printVariants(const FtSelectContext& ctx, const TextSearchResults<IdCont>& res) {
	WrSerializer wrSer;
	wrSer << "variants: [";
	for (auto& variant : ctx.variants) {
		if (&variant != &*ctx.variants.begin()) {
			wrSer << ", ";
		}
		wrSer << variant.pattern;
	}
	wrSer << "], variants_with_low_relevancy: [";
	for (auto& variant : ctx.lowRelVariants) {
		if (&variant != &*ctx.lowRelVariants.begin()) {
			wrSer << ", ";
		}
		wrSer << variant.pattern;
	}
	wrSer << "], typos: [";
	if (res.term.opts.typos) {
		typos_context tctx[kMaxTyposInWord];
		mktypos(tctx, res.term.pattern, holder_.cfg_->MaxTyposInWord(), holder_.cfg_->maxTypoLen,
				[&wrSer](std::string_view typo, int, const typos_context::TyposVec& positions, const std::wstring_view) {
					wrSer << typo;
					wrSer << ":(";
					for (unsigned j = 0, sz = positions.size(); j < sz; ++j) {
						if (j) {
							wrSer << ',';
						}
						wrSer << positions[j];
					}
					wrSer << "), ";
				});
	}
	logFmt(LogInfo, "Variants: [{}]", wrSer.Slice());
}

template class Selector<PackedIdRelVec>;
template MergeData Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, MergeData>(FtDSLQuery&&, bool, RankSortType,
																						 FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeDataAreas<Area> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, MergeDataAreas<Area>>(FtDSLQuery&&, bool,
																											   RankSortType,
																											   FtMergeStatuses::Statuses&&,
																											   const RdxContext&);
template MergeDataAreas<AreaDebug> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, MergeDataAreas<AreaDebug>>(
	FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&, const RdxContext&);

template MergeData Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																			   FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeDataAreas<Area> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																						  FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeDataAreas<AreaDebug> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																							   FtMergeStatuses::Statuses&&,
																							   const RdxContext&);

template class Selector<IdRelVec>;
template MergeData Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&,
																		const RdxContext&);
template MergeDataAreas<Area> Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType,
																				   FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeDataAreas<AreaDebug> Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType,
																						FtMergeStatuses::Statuses&&, const RdxContext&);

template MergeData Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&,
																		 const RdxContext&);
template MergeDataAreas<Area> Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																					FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeDataAreas<AreaDebug> Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																						 FtMergeStatuses::Statuses&&, const RdxContext&);

}  // namespace reindexer
