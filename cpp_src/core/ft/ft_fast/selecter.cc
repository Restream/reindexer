#include "selecter.h"
#include "core/ft/bm25.h"
#include "core/ft/typos.h"
#include "core/rdxcontext.h"
#include "estl/defines.h"
#include "sort/pdqsort.hpp"
#include "tools/logger.h"
#include "tools/scope_guard.h"
#include "tools/serializer.h"

namespace {
RX_ALWAYS_INLINE double pos2rank(int pos) {
	if (pos <= 10) {
		return 1.0 - (pos / 100.0);
	}
	if (pos <= 100) {
		return 0.9 - (pos / 1000.0);
	}
	if (pos <= 1000) {
		return 0.8 - (pos / 10000.0);
	}
	if (pos <= 10000) {
		return 0.7 - (pos / 100000.0);
	}
	if (pos <= 100000) {
		return 0.6 - (pos / 1000000.0);
	}
	return 0.5;
}
}  // namespace

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
		if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
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
			if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
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
	const std::wstring& pattern = dsl[termIdx].pattern;
	FtDslOpts& opts = dsl[termIdx].opts;
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
template <FtUseExternStatuses useExternSt, typename MergeType>
MergeType Selector<IdCont>::Process(FtDSLQuery&& dsl, bool inTransaction, RankSortType rankSortType,
									FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx) {
	FtSelectContext ctx;
	ctx.rawResults.reserve(dsl.size());
	// STEP 2: Search dsl terms for each variant
	std::vector<SynonymsDsl> synonymsDsl;
	holder_.synonyms_->AddManyToManySynonyms(dsl, synonymsDsl, holder_.cfg_->rankingConfig.synonyms);
	if (!inTransaction) {
		ThrowOnCancel(rdxCtx);
	}
	for (size_t i = 0; i < dsl.size(); ++i) {
		const auto irrVariantsCount = ctx.lowRelVariants.size();
		ctx.variantsForTypos.resize(0);
		// Prepare term variants (original + translit + stemmed + kblayout + synonym)
		this->prepareVariants(ctx.variants, &ctx.lowRelVariants, i, holder_.cfg_->stemmers, dsl, &synonymsDsl, &ctx.variantsForTypos);
		auto v = ctx.GetWordsMapPtr(dsl[i].opts);
		ctx.rawResults.emplace_back(std::move(dsl[i]), v);
		TextSearchResults& res = ctx.rawResults.back();

		if (irrVariantsCount != ctx.lowRelVariants.size()) {
			ctx.rawResults.back().SwitchToInternalWordsMap();
		}
		for (unsigned j = irrVariantsCount; j < ctx.lowRelVariants.size(); ++j) {
			ctx.lowRelVariants[j].rawResultIdx = ctx.rawResults.size() - 1;
		}

		if rx_unlikely (holder_.cfg_->logLevel >= LogInfo) {
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

	std::vector<TextSearchResults> results;
	size_t reserveSize = ctx.rawResults.size();
	for (const SynonymsDsl& synDsl : synonymsDsl) {
		reserveSize += synDsl.dsl.size();
	}
	results.reserve(reserveSize);
	std::vector<size_t> synonymsBounds;
	synonymsBounds.reserve(synonymsDsl.size());
	if (!inTransaction) {
		ThrowOnCancel(rdxCtx);
	}
	for (SynonymsDsl& synDsl : synonymsDsl) {
		FtSelectContext synCtx;
		synCtx.rawResults.reserve(synDsl.dsl.size());
		for (size_t i = 0; i < synDsl.dsl.size(); ++i) {
			prepareVariants(synCtx.variants, nullptr, i, holder_.cfg_->stemmers, synDsl.dsl, nullptr, nullptr);
			if rx_unlikely (holder_.cfg_->logLevel >= LogInfo) {
				WrSerializer wrSer;
				for (auto& variant : synCtx.variants) {
					if (&variant != &*synCtx.variants.begin()) {
						wrSer << ", ";
					}
					wrSer << variant.pattern;
				}
				logFmt(LogInfo, "Multiword synonyms variants: [{}]", wrSer.Slice());
			}
			auto v = ctx.GetWordsMapPtr(synDsl.dsl[i].opts);
			synCtx.rawResults.emplace_back(std::move(synDsl.dsl[i]), v);
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
		return mergeResultsBmType<uint16_t, MergeType>(std::move(results), ctx.totalORVids, synonymsBounds, inTransaction, rankSortType,
													   std::move(mergeStatuses), rdxCtx);
	} else if (maxMergedSize < 0xFFFFFFFF) {
		return mergeResultsBmType<uint32_t, MergeType>(std::move(results), ctx.totalORVids, synonymsBounds, inTransaction, rankSortType,
													   std::move(mergeStatuses), rdxCtx);
	} else {
		assertrx_throw(false);
	}
	return MergeType();
}

template <typename IdCont>
template <typename MergedOffsetT, typename MergeType>
MergeType Selector<IdCont>::mergeResultsBmType(std::vector<TextSearchResults>&& results, size_t totalORVids,
											   const std::vector<size_t>& synonymsBounds, bool inTransaction, RankSortType rankSortType,
											   FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx) {
	switch (holder_.cfg_->bm25Config.bm25Type) {
		case FtFastConfig::Bm25Config::Bm25Type::rx:
			return mergeResults<Bm25Rx, MergedOffsetT, MergeType>(std::move(results), totalORVids, synonymsBounds, inTransaction,
																  rankSortType, std::move(mergeStatuses), rdxCtx);
		case FtFastConfig::Bm25Config::Bm25Type::classic:
			return mergeResults<Bm25Classic, MergedOffsetT, MergeType>(std::move(results), totalORVids, synonymsBounds, inTransaction,
																	   rankSortType, std::move(mergeStatuses), rdxCtx);
		case FtFastConfig::Bm25Config::Bm25Type::wordCount:
			return mergeResults<TermCount, MergedOffsetT, MergeType>(std::move(results), totalORVids, synonymsBounds, inTransaction,
																	 rankSortType, std::move(mergeStatuses), rdxCtx);
	}
	assertrx_throw(false);
	return MergeType();
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
	auto& tmpstr = variant.pattern;
	auto& suffixes = step.suffixes_;
	//  Lookup current variant in suffixes array
	auto keyIt = suffixes.lower_bound(tmpstr);

	int matched = 0, skipped = 0, vids = 0, excludedCnt = 0;
	bool withPrefixes = variant.opts.pref;
	bool withSuffixes = variant.opts.suff;
	const auto initialLimit = vidsLimit;

	// Walk current variant in suffixes array and fill results
	do {
		if (keyIt == suffixes.end()) {
			break;
		}
		if (vidsLimit <= vids) {
			if rx_unlikely (holder_.cfg_->logLevel >= LogInfo) {
				logFmt(LogInfo, "Terminating suffix loop on limit ({}). Current variant is '{}{}{}'", initialLimit,
					   variant.opts.suff ? "*" : "", variant.pattern, variant.opts.pref ? "*" : "");
			}
			break;
		}

		const WordIdType glbwordId = keyIt->second;
		const auto& hword = holder_.GetWordById(glbwordId);

		if constexpr (useExternSt == FtUseExternStatuses::Yes) {
			bool excluded = true;
			for (const auto& id : hword.vids) {
				if (mergeStatuses[id.Id()] != FtMergeStatuses::kExcluded) {
					excluded = false;
					break;
				}
			}
			if (excluded) {
				++excludedCnt;
				continue;
			}
		}

		const uint32_t suffixWordId = holder_.GetSuffixWordId(glbwordId, step);
		const std::string::value_type* word = suffixes.word_at(suffixWordId);

		const int16_t wordLength = suffixes.word_len_at(suffixWordId);

		const ptrdiff_t suffixLen = keyIt->first - word;
		const int matchLen = tmpstr.length();

		if (!withSuffixes && suffixLen) {
			continue;
		}
		if (!withPrefixes && wordLength != matchLen + suffixLen) {
			break;
		}

		const int matchDif = std::abs(long(wordLength - matchLen + suffixLen));
		const int proc = std::max(variant.proc - holder_.cfg_->partialMatchDecrease * matchDif / std::max(matchLen, 3),
								  suffixLen ? holder_.cfg_->rankingConfig.suffixMin : holder_.cfg_->rankingConfig.prefixMin);

		const auto it = res.foundWords->find(glbwordId);
		if (it == res.foundWords->end() || it->second.first != curRawResultIdx) {
			res.push_back({&hword.vids, keyIt->first, proc});
			const auto vidsSize = hword.vids.size();
			res.idsCnt_ += vidsSize;
			if (variant.opts.op == OpOr) {
				ctx.totalORVids += vidsSize;
			}
			(*res.foundWords)[glbwordId] = std::make_pair(curRawResultIdx, res.size() - 1);
			if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
				logFmt(LogInfo, " matched {} '{}' of word '{}' (variant '{}'), {} vids, {}%", suffixLen ? "suffix" : "prefix", keyIt->first,
					   word, variant.pattern, holder_.GetWordById(glbwordId).vids.size(), proc);
			}
			++matched;
			vids += vidsSize;
		} else {
			if (ctx.rawResults[it->second.first][it->second.second].proc < proc) {
				ctx.rawResults[it->second.first][it->second.second].proc = proc;
			}
			++skipped;
		}
	} while ((keyIt++).lcp() >= int(tmpstr.length()));
	if rx_unlikely (holder_.cfg_->logLevel >= LogInfo) {
		std::string limitString;
		if (vidsLimit <= vids) {
			limitString = fmt::format(". Lookup terminated by VIDs limit({})", initialLimit);
		}
		logFmt(LogInfo, "Lookup variant '{}' ({}%), matched {} suffixes, with {} vids, skipped {}, excluded {}{}", tmpstr, variant.proc,
			   matched, vids, skipped, excludedCnt, limitString);
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
	// Add words from low relevancy variants, ordered by length & proc
	if constexpr (kVariantsWithDifLength) {
		boost::sort::pdqsort(ctx.lowRelVariants.begin(), ctx.lowRelVariants.end(),
							 [](FtBoundVariantEntry& l, FtBoundVariantEntry& r) noexcept {
								 const auto lenL = l.GetLenCached();
								 const auto lenR = r.GetLenCached();
								 if (lenL > lenR) {
									 return true;
								 }
								 if (lenL == lenR) {
									 return l.proc > r.proc;
								 }
								 return false;
							 });
	} else {
		boost::sort::pdqsort_branchless(ctx.lowRelVariants.begin(), ctx.lowRelVariants.end(),
										[](FtBoundVariantEntry& l, FtBoundVariantEntry& r) noexcept { return l.proc > r.proc; });
	}

	auto lastVariantLen = ctx.lowRelVariants.size() ? ctx.lowRelVariants[0].GetLenCached() : -1;
	// Those number were taken from nowhere and probably will require some calibration later on
	const auto targetORLimit = 4 * holder_.cfg_->mergeLimit;
	const auto targetANDLimit = 2 * holder_.cfg_->mergeLimit;

	for (auto& variant : ctx.lowRelVariants) {
		if constexpr (kVariantsWithDifLength) {
			if (variant.GetLenCached() != lastVariantLen) {
				if (unsigned(targetORLimit) <= ctx.totalORVids) {
					if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
						logFmt(LogInfo, "Terminating on target OR limit. Current variant is '{}{}{}'", variant.opts.suff ? "*" : "",
							   variant.pattern, variant.opts.pref ? "*" : "");
					}
					break;
				}
				lastVariantLen = variant.GetLenCached();
			}
		} else {
			(void)lastVariantLen;
		}
		if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
			logFmt(LogInfo, "Handling '{}{}{}' as variant with low relevancy", variant.opts.suff ? "*" : "", variant.pattern,
				   variant.opts.pref ? "*" : "");
		}
		switch (variant.opts.op) {
			case OpOr: {
				int remainingLimit = targetORLimit - ctx.totalORVids;
				if (remainingLimit > 0) {
					for (auto& step : holder_.steps) {
						processStepVariants<useExternSt>(ctx, step, variant, variant.rawResultIdx, mergeStatuses, remainingLimit);
					}
				}
				break;
			}
			case OpAnd:
			case OpNot: {
				auto& res = ctx.rawResults[variant.rawResultIdx];
				int remainingLimit = targetANDLimit - res.idsCnt_;
				if (remainingLimit > 0) {
					for (auto& step : holder_.steps) {
						processStepVariants<useExternSt>(ctx, step, variant, variant.rawResultIdx, mergeStatuses, remainingLimit);
					}
				}
				break;
			}
		}
	}
}

RX_ALWAYS_INLINE double bound(double k, double weight, double boost) noexcept { return (1.0 - weight) + k * boost * weight; }

template <typename IdCont>
RX_ALWAYS_INLINE void Selector<IdCont>::debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank,
													   int prevRank) {
#ifdef REINDEX_FT_EXTRA_DEBUG
	if (holder_.cfg_->logLevel < LogTrace) {
		return;
	}

	logFmt(LogInfo, "{} - '{}' (vid {}), bm25 {}, dist {}, rank {} (prev rank {})", msg, holder_.vdocs_[vid].keyDoc, vid, normBm25,
		   normDist, finalRank, prevRank);
#else
	(void)msg;
	(void)vid;
	(void)normBm25;
	(void)normDist;
	(void)finalRank;
	(void)prevRank;
#endif
}
template <typename IdCont>
template <typename Calculator>
RX_ALWAYS_INLINE void Selector<IdCont>::calcFieldBoost(const Calculator& bm25Calc, unsigned long long f, const IdRelType& relid,
													   const FtDslOpts& opts, TermRankInfo& termInf, bool& dontSkipCurTermRank,
													   h_vector<double, 4>& ranksInFields, int& field) {
	assertrx(f < holder_.cfg_->fieldsCfg.size());
	const auto& fldCfg = holder_.cfg_->fieldsCfg[f];
	// raw bm25
	const double bm25 = bm25Calc.Get(relid.WordsInField(f), holder_.vdocs_[relid.Id()].wordsCount[f], holder_.avgWordsCount_[f]);
	// normalized bm25
	const double normBm25Tmp = bound(bm25, fldCfg.bm25Weight, fldCfg.bm25Boost);
	termInf.positionRank = bound(::pos2rank(relid.MinPositionInField(f)), fldCfg.positionWeight, fldCfg.positionBoost);
	termInf.termLenBoost = bound(opts.termLenBoost, fldCfg.termLenWeight, fldCfg.termLenBoost);

	// final term rank calculation
	const double termRankTmp =
		opts.fieldsOpts[f].boost * termInf.proc * normBm25Tmp * opts.boost * termInf.termLenBoost * termInf.positionRank;
	const bool needSumRank = opts.fieldsOpts[f].needSumRank;
	if (termRankTmp > termInf.termRank) {
		if (dontSkipCurTermRank) {
			ranksInFields.push_back(termInf.termRank);
		}
		field = f;
		termInf.termRank = termRankTmp;
		termInf.bm25Norm = normBm25Tmp;
		dontSkipCurTermRank = needSumRank;
	} else if (!dontSkipCurTermRank && needSumRank && termInf.termRank == termRankTmp) {
		field = f;
		termInf.bm25Norm = normBm25Tmp;
		dontSkipCurTermRank = true;
	} else if (termRankTmp && needSumRank) {
		ranksInFields.push_back(termRankTmp);
	}
}

template <typename IdCont>
template <typename PosT>
void Selector<IdCont>::insertSubMergeArea(const MergedIdRelGroupArea<PosT>& posInfo, PosT cur, int prevIndex, AreasInDocument<Area>& area) {
	PosT last = cur, first = cur;
	int indx = int(posInfo.wordPosForChain.size()) - 2;
	while (indx >= 0 && prevIndex != -1) {
		auto pos = posInfo.wordPosForChain[indx][prevIndex].first;
		prevIndex = posInfo.wordPosForChain[indx][prevIndex].second;
		first = pos;
		indx--;
	}
	assertrx_throw(first.field() == last.field());
	if (area.InsertArea(Area(first.pos(), last.pos() + 1), cur.field(), posInfo.rank, maxAreasInDoc_)) {
		area.UpdateRank(float(posInfo.rank));
	}
}

template <typename IdCont>
template <typename PosT>
void Selector<IdCont>::insertSubMergeArea(const MergedIdRelGroupArea<PosT>& posInfo, PosT cur, int prevIndex,
										  AreasInDocument<AreaDebug>& area) {
	int indx = int(posInfo.wordPosForChain.size()) - 1;
	while (indx >= 0 && prevIndex != -1) {
		PosT pos = posInfo.wordPosForChain[indx][prevIndex].first;
		prevIndex = posInfo.wordPosForChain[indx][prevIndex].second;
		AreaDebug::PhraseMode mode = AreaDebug::PhraseMode::None;
		if (indx == int(posInfo.wordPosForChain.size()) - 1) {
			mode = AreaDebug::PhraseMode::End;
		} else if (indx == 0) {
			mode = AreaDebug::PhraseMode::Start;
		}
		if (area.InsertArea(AreaDebug(pos.pos(), pos.pos() + 1, std::move(pos.info), mode), cur.field(), posInfo.rank, -1)) {
			area.UpdateRank(float(posInfo.rank));
		}

		indx--;
	}
}

template <typename IdCont>
template <typename AreaType, typename PosT>
AreasInDocument<AreaType> Selector<IdCont>::createAreaFromSubMerge(const MergedIdRelGroupArea<PosT>& posInfo) {
	AreasInDocument<AreaType> area;
	if (posInfo.wordPosForChain.empty()) {
		return area;
	}
	for (const auto& v : posInfo.wordPosForChain.back()) {
		insertSubMergeArea(posInfo, v.first, v.second, area);
	}
	return area;
}

template <typename IdCont>
template <typename AreaType>
void Selector<IdCont>::copyAreas(AreasInDocument<AreaType>& subMerged, AreasInDocument<AreaType>& merged, int32_t rank) {
	for (size_t f = 0; f < fieldSize_; f++) {
		auto areas = subMerged.GetAreas(f);
		if (areas) {
			areas->MoveAreas(merged, f, rank, std::is_same_v<AreaType, AreaDebug> ? -1 : maxAreasInDoc_);
		}
	}
}

template <typename IdCont>
template <typename PosType, typename MergedOffsetT, typename MergeType>
void Selector<IdCont>::subMergeLoop(MergeType& subMerged, std::vector<PosType>& subMergedPos, MergeType& merged,
									std::vector<MergedIdRel>& merged_rd, FtMergeStatuses::Statuses& mergeStatuses,
									std::vector<MergedOffsetT>& idoffsets, std::vector<bool>* checkAndOpMerge, const bool hasBeenAnd) {
	for (size_t subMergedIndex = 0, sz = subMerged.size(); subMergedIndex < sz; subMergedIndex++) {
		const auto subMergeInfo = subMerged[subMergedIndex];
		if (subMergeInfo.proc == 0) {
			continue;
		}
		index_t& mergeStatus = mergeStatuses[subMergeInfo.id];
		if (mergeStatus == 0 && !hasBeenAnd && merged.size() < holder_.cfg_->mergeLimit) {	// add new
			mergeStatus = 1;
			MergeInfo m;

			m.id = subMergeInfo.id;
			m.proc = subMergeInfo.proc;
			m.field = subMergeInfo.field;
			PosType& smPos = subMergedPos[subMergedIndex];
			MergedIdRel mPos(smPos.rank, smPos.qpos);
			if constexpr (isGroupMergeWithAreas<PosType>()) {
				mPos.next.reserve(smPos.posTmp.size());
				for (const auto& p : smPos.posTmp) {
					mPos.next.Add(p.first);
				}
				merged.vectorAreas.emplace_back(createAreaFromSubMerge<typename MergeType::AT>(smPos));
				m.areaIndex = merged.vectorAreas.size() - 1;
			} else {
				mPos.next = std::move(smPos.posTmp);
			}

			mPos.cur = std::move(smPos.cur);

			merged.emplace_back(std::move(m));
			merged_rd.emplace_back(std::move(mPos));
			idoffsets[subMergeInfo.id] = merged.size() - 1;
		} else if (mergeStatus != 0 && mergeStatus != FtMergeStatuses::kExcluded) {
			const size_t mergedIndex = idoffsets[subMergeInfo.id];

			merged[mergedIndex].proc += subMergeInfo.proc;

			auto& subPos = subMergedPos[subMergedIndex];
			if constexpr (isGroupMergeWithAreas<PosType>()) {
				subPos.next.reserve(subPos.posTmp.size());
				for (const auto& p : subPos.posTmp) {
					subPos.next.Add(p.first);
				}
				AreasInDocument<typename MergeType::AT> area = createAreaFromSubMerge<typename MergeType::AT>(subPos);
				int32_t areaIndex = merged[mergedIndex].areaIndex;
				if (areaIndex != -1 && areaIndex >= int(merged.vectorAreas.size())) {
					throw Error(errLogic, "FT merge: Incorrect area index {} (areas vector size is {})", areaIndex,
								merged.vectorAreas.size());
				}
				copyAreas(area, merged.vectorAreas[areaIndex], subMergeInfo.proc);
			} else {
				subPos.next = std::move(subPos.posTmp);
			}
			MergedIdRel& mergedPosVectorElemPointer = merged_rd[mergedIndex];
			mergedPosVectorElemPointer.cur = std::move(subPos.cur);
			mergedPosVectorElemPointer.next = std::move(subPos.next);
			mergedPosVectorElemPointer.rank = subPos.rank;
			mergedPosVectorElemPointer.qpos = subPos.qpos;
		}
		if (checkAndOpMerge) {
			(*checkAndOpMerge)[subMergeInfo.id] = true;
		}
	}
}

template <typename IdCont>
template <typename MergedIdRelGroupType, typename Bm25T, typename MergedOffsetT, typename MergeType>
void Selector<IdCont>::mergeGroupResult(std::vector<TextSearchResults>& rawResults, size_t from, size_t to,
										FtMergeStatuses::Statuses& mergeStatuses, MergeType& merged, std::vector<MergedIdRel>& merged_rd,
										OpType op, const bool hasBeenAnd, std::vector<MergedOffsetT>& idoffsets, const bool inTransaction,
										const RdxContext& rdxCtx) {
	// And - MustPresent
	// Or  - MayBePresent
	// Not - NotPresent
	// hasBeenAnd shows whether it is possible to expand the set of documents (if there was already And, then the set of documents is not
	// expandable)
	MergeType subMerged;
	std::vector<MergedIdRelGroupType> subMergedPositionData;

	mergeResultsPart<MergedIdRelGroupType, Bm25T, MergedOffsetT, MergeType>(rawResults, from, to, subMerged, subMergedPositionData,
																			inTransaction, rdxCtx);

	switch (op) {
		case OpOr: {
			subMergeLoop<MergedIdRelGroupType, MergedOffsetT, MergeType>(subMerged, subMergedPositionData, merged, merged_rd, mergeStatuses,
																		 idoffsets, nullptr, hasBeenAnd);
			break;
		}
		case OpAnd: {
			// when executing And, you need to remove documents from mergeStatuses that are not in the results for this word
			// To do this, we intersect the checkAndOpMerge array with the merged array
			std::vector<bool> checkAndOpMerge;
			checkAndOpMerge.resize(holder_.vdocs_.size(), false);
			subMergeLoop<MergedIdRelGroupType, MergedOffsetT, MergeType>(subMerged, subMergedPositionData, merged, merged_rd, mergeStatuses,
																		 idoffsets, &checkAndOpMerge, hasBeenAnd);
			// intersect checkAndOpMerge with merged
			for (auto& mergedDocInfo : merged) {
				if (!checkAndOpMerge[mergedDocInfo.id]) {
					mergeStatuses[mergedDocInfo.id] = 0;
					mergedDocInfo.proc = 0;
				}
			}
		} break;
		case OpNot:
			for (const auto& mergeDocInfo : subMerged) {
				if (mergeDocInfo.proc == 0) {
					continue;
				}
				if (mergeStatuses[mergeDocInfo.id] != 0 && mergeStatuses[mergeDocInfo.id] != FtMergeStatuses::kExcluded) {
					merged[idoffsets[mergeDocInfo.id]].proc = 0;
				}
				mergeStatuses[mergeDocInfo.id] = FtMergeStatuses::kExcluded;
			}
			break;
		default:
			abort();
	}
}
template <typename IdCont>
template <typename MergedOffsetT, typename MergeType>
void Selector<IdCont>::addNewTerm(FtMergeStatuses::Statuses& mergeStatuses, MergeType& merged, std::vector<MergedOffsetT>& idoffsets,
								  std::vector<bool>& curExists, const IdRelType& relid, index_t rawResIndex, int32_t termRank, int field) {
	const int vid = relid.Id();
	MergeInfo info;
	info.id = vid;
	info.proc = termRank;
	info.field = field;

	if constexpr (std::is_same_v<MergeData<Area>&, decltype(merged)> || std::is_same_v<MergeData<AreaDebug>&, decltype(merged)>) {
		auto& area = merged.vectorAreas.emplace_back();
		info.areaIndex = merged.vectorAreas.size() - 1;
		area.ReserveField(fieldSize_);
	}
	merged.push_back(std::move(info));
	mergeStatuses[vid] = rawResIndex + 1;
	if (!curExists.empty()) {
		curExists[vid] = true;
		idoffsets[vid] = merged.size() - 1;
	}
}

template <typename IdCont>
void Selector<IdCont>::addAreas(AreasInDocument<Area>& area, const IdRelType& relid, int32_t termRank, const TermRankInfo& termInf,
								const std::wstring& pattern) {
	(void)termInf;
	(void)pattern;
	for (auto pos : relid.Pos()) {
		if (!area.AddWord(Area(pos.pos(), pos.pos() + 1), pos.field(), termRank, maxAreasInDoc_)) {
			break;
		}
	}
	area.UpdateRank(termRank);
}

template <typename IdCont>
void Selector<IdCont>::addAreas(AreasInDocument<AreaDebug>& area, const IdRelType& relid, int32_t termRank, const TermRankInfo& termInf,
								const std::wstring& pattern) {
	(void)termRank;
	utf16_to_utf8(pattern, const_cast<std::string&>(termInf.ftDslTerm));
	for (auto pos : relid.Pos()) {
		if (!area.AddWord(AreaDebug(pos.pos(), pos.pos() + 1, termInf.ToString(), AreaDebug::PhraseMode::None), pos.field(),
						  termInf.termRank, -1)) {
			break;
		}
	}
	area.UpdateRank(termInf.termRank);
}

// idf=max(0.2, log((N-M+1)/M)/log(1+N))
// N - document count
// M - the number of documents in which the term was found
// bm25= idf* T * (k1 + 1.0) / (T + k1 * (1.0 - b + b * wordsInDoc / avgDocLen)
//  T - the number of terms in the document
//  bm25_norm= (1.0 - weight) + b525 * boost * weight
//  weight - fieldCfg.bm25Weight,
//  boost - fieldCfg.bm25Boost
//  subTermRank = opts.fieldsOpts[f].boost * termProc * bm25_norm * opts.boost * termLenBoost * positionRank
//  positionRank - weight depending on the position of the word
//  termLenBoost - weight depending on the length of the word
//  termProc - weight depending on the type of subTerm
//  docRank=summ(max(subTermRank))*255/allmax
//  allmax=max(docRank)
template <typename IdCont>
template <typename Bm25Type, typename MergedOffsetT, typename MergedType>
void Selector<IdCont>::mergeIteration(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses,
									  MergedType& merged, std::vector<MergedIdRel>& merged_rd, std::vector<MergedOffsetT>& idoffsets,
									  std::vector<bool>& curExists, const bool hasBeenAnd, const bool inTransaction,
									  const RdxContext& rdxCtx) {
	const auto& vdocs = holder_.vdocs_;

	const size_t totalDocsCount = vdocs.size();
	const bool simple = idoffsets.empty();
	const auto op = rawRes.term.opts.op;

	curExists.clear();
	if (!simple || rawRes.size() > 1) {
		curExists.resize(totalDocsCount, false);
	}
	if (simple && rawRes.size() > 1) {
		idoffsets.resize(totalDocsCount);
	}

	for (auto& m_rd : merged_rd) {
		if (m_rd.next.Size()) {
			m_rd.cur = std::move(m_rd.next);
			m_rd.rank = 0;
		}
	}

	// loop on subterm (word, translit, stemmer,...)
	for (auto& r : rawRes) {
		if (!inTransaction) {
			ThrowOnCancel(rdxCtx);
		}
		Bm25Calculator<Bm25Type> bm25{double(totalDocsCount), double(r.vids->size()), holder_.cfg_->bm25Config.bm25k1,
									  holder_.cfg_->bm25Config.bm25b};
		static_assert(sizeof(bm25) <= 32, "Bm25Calculator<Bm25Type> size is greater than 32 bytes");
		// cycle through the documents for the given subterm
		for (auto&& relid : *r.vids) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(relid), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(relid), IdRelType&>),
						  "Expecting relid is movable for packed vector and not movable for simple vector");

			// relid contains all subterm positions in the given document
			const int vid = relid.Id();
			index_t vidStatus = mergeStatuses[vid];
			// Do not calc anything if
			if ((vidStatus == FtMergeStatuses::kExcluded) | (hasBeenAnd & (vidStatus == 0))) {
				continue;
			}
			if (op == OpNot) {
				if (!simple & (vidStatus != 0)) {
					merged[idoffsets[vid]].proc = 0;
				}
				mergeStatuses[vid] = FtMergeStatuses::kExcluded;
				continue;
			}

			// keyEntry can be assigned nullptr when removed
			if (!vdocs[vid].keyEntry) {
				continue;
			}

			// Find field with max rank
			TermRankInfo termInf;
			termInf.proc = r.proc;
			termInf.pattern = r.pattern;
			auto [termRank, field] = calcTermRank(rawRes, bm25, relid, termInf);
			if (!termRank) {
				continue;
			}
			if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", r.pattern, bm25.GetIDF(), rawRes.term.opts.termLenBoost);
			}

			if (simple) {  // one term
				if (vidStatus) {
					MergeInfo& info = merged[idoffsets[vid]];
					if constexpr (std::is_same_v<MergeData<Area>, MergedType> || std::is_same_v<MergeData<AreaDebug>, MergedType>) {
						addAreas(merged.vectorAreas[info.areaIndex], relid, termRank, termInf, rawRes.term.pattern);
					}
					if (info.proc < static_cast<int32_t>(termRank)) {
						info.proc = termRank;
						info.field = field;
					}
				} else if (merged.size() < holder_.cfg_->mergeLimit) {	// add new
					addNewTerm(mergeStatuses, merged, idoffsets, curExists, relid, rawResIndex, int32_t(termRank), field);
					if constexpr (std::is_same_v<MergeData<Area>, MergedType> || std::is_same_v<MergeData<AreaDebug>, MergedType>) {
						addAreas(merged.vectorAreas.back(), relid, termRank, termInf, rawRes.term.pattern);
					}
				}
			} else {
				if (vidStatus) {
					int distance = 0;
					float normDist = 1;
					auto& info = merged[idoffsets[vid]];
					auto& curMerged_rd = merged_rd[idoffsets[vid]];
					if (!curMerged_rd.cur.empty()) {  // do not calculate the distance if it is a subterm of the FIRST added term
						distance = curMerged_rd.cur.Distance(relid, INT_MAX);
						// Normalized distance
						normDist = bound(1.0 / double(std::max(distance, 1)), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
					}
					int finalRank = normDist * termRank;
					if constexpr (std::is_same_v<MergeData<Area>, MergedType> || std::is_same_v<MergeData<AreaDebug>, MergedType>) {
						addAreas(merged.vectorAreas[info.areaIndex], relid, termRank, termInf, rawRes.term.pattern);
					}
					if (finalRank > curMerged_rd.rank) {
						info.proc -= curMerged_rd.rank;
						info.proc += finalRank;
						curMerged_rd.rank = finalRank;
						curMerged_rd.next = std::move(relid);
					}
					curExists[vid] = true;
				} else if (merged.size() < holder_.cfg_->mergeLimit && !hasBeenAnd) {  // add new
					addNewTerm(mergeStatuses, merged, idoffsets, curExists, relid, rawResIndex, termRank, field);
					MergeInfo& info = merged[idoffsets[vid]];
					if constexpr (std::is_same_v<MergeData<Area>, MergedType> || std::is_same_v<MergeData<AreaDebug>, MergedType>) {
						addAreas(merged.vectorAreas[info.areaIndex], relid, termRank, termInf, rawRes.term.pattern);
					}
					merged_rd.emplace_back(IdRelType(std::move(relid)), int32_t(termRank), rawRes.term.opts.qpos);
				}
			}
		}
	}
}

template <typename IdCont>
template <typename Calculator>
std::pair<double, int> Selector<IdCont>::calcTermRank(const TextSearchResults& rawRes, Calculator bm25Calc, const IdRelType& relid,
													  TermRankInfo& termInf) {
	// Find field with max rank
	int field = 0;
	bool dontSkipCurTermRank = false;

	h_vector<double, 4> ranksInFields;
	for (unsigned long long fieldsMask = relid.UsedFieldsMask(), f = 0; fieldsMask; ++f, fieldsMask >>= 1) {
#if defined(__GNUC__) || defined(__clang__)
		const auto bits = __builtin_ctzll(fieldsMask);
		f += bits;
		fieldsMask >>= bits;
#else
		while ((fieldsMask & 1) == 0) {
			++f;
			fieldsMask >>= 1;
		}
#endif
		//		assertrx(f < vdocs[vid].wordsCount.size());
		assertrx(f < rawRes.term.opts.fieldsOpts.size());
		const auto fboost = rawRes.term.opts.fieldsOpts[f].boost;
		if (fboost) {
			calcFieldBoost(bm25Calc, f, relid, rawRes.term.opts, termInf, dontSkipCurTermRank, ranksInFields, field);
		}
	}

	if (!termInf.termRank) {
		return std::make_pair(termInf.termRank, field);
	}

	if (holder_.cfg_->summationRanksByFieldsRatio > 0) {
		boost::sort::pdqsort_branchless(ranksInFields.begin(), ranksInFields.end());
		double k = holder_.cfg_->summationRanksByFieldsRatio;
		for (auto rank : ranksInFields) {
			termInf.termRank += (k * rank);
			k *= holder_.cfg_->summationRanksByFieldsRatio;
		}
	}
	return std::make_pair(termInf.termRank, field);
}

template <typename IdCont>
template <typename MergedIdRelGroupType, typename Bm25Type, typename MergedOffsetT, typename MergeType>
void Selector<IdCont>::mergeIterationGroup(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses,
										   MergeType& merged, std::vector<MergedIdRelGroupType>& mergedPos,
										   std::vector<MergedOffsetT>& idoffsets, std::vector<bool>& present, const bool firstTerm,
										   const bool inTransaction, const RdxContext& rdxCtx) {
	const auto& vdocs = holder_.vdocs_;

	const size_t totalDocsCount = vdocs.size();

	present.clear();
	present.resize(totalDocsCount, false);

	// loop on subterm (word, translit, stemmer,...)
	for (auto& r : rawRes) {
		if (!inTransaction) {
			ThrowOnCancel(rdxCtx);
		}
		Bm25Calculator<Bm25Type> bm25(totalDocsCount, r.vids->size(), holder_.cfg_->bm25Config.bm25k1, holder_.cfg_->bm25Config.bm25b);
		static_assert(sizeof(bm25) <= 32, "Bm25Calculator<Bm25Type> size is greater than 32 bytes");
		int vid = -1;
		// cycle through the documents for the given subterm
		for (auto&& relid : *r.vids) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(relid), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(relid), IdRelType&>),
						  "Expecting relid is movable for packed vector and not movable for simple vector");

			// relid contains all subterm positions in the given document
			vid = relid.Id();
			index_t vidStatus = mergeStatuses[vid];
			// Do not calc anything if
			if ((vidStatus == FtMergeStatuses::kExcluded) | (!firstTerm & (vidStatus == 0))) {
				continue;
			}
			// keyEntry can be assigned nullptr when removed
			if (!vdocs[vid].keyEntry) {
				continue;
			}

			// Find field with max rank
			TermRankInfo termInf;
			termInf.proc = r.proc;
			termInf.pattern = r.pattern;
			auto [termRank, field] = calcTermRank(rawRes, bm25, relid, termInf);
			if (!termRank) {
				continue;
			}

			if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", r.pattern, bm25.GetIDF(), rawRes.term.opts.termLenBoost);
			}

			// match of 2-rd, and next terms
			if (!firstTerm) {
				auto& curMerged = merged[idoffsets[vid]];
				auto& curMergedPos = mergedPos[idoffsets[vid]];
				int minDist = -1;
				if constexpr (isGroupMergeWithAreas<MergedIdRelGroupType>()) {
					if constexpr (std::is_same_v<typename MergedIdRelGroupType::TypeTParam, PosTypeDebug>) {
						utf16_to_utf8(rawRes.term.pattern, termInf.ftDslTerm);
						minDist = curMergedPos.cur.MergeWithDist(relid, rawRes.term.opts.distance, curMergedPos.posTmp, termInf.ToString());
					} else {
						minDist = curMergedPos.cur.MergeWithDist(relid, rawRes.term.opts.distance, curMergedPos.posTmp, "");
					}
				} else {
					minDist = curMergedPos.cur.MergeWithDist(relid, rawRes.term.opts.distance, curMergedPos.posTmp, "");
				}
				if (!curMergedPos.posTmp.empty()) {
					present[vid] = true;
					double normDist = bound(1.0 / (minDist < 1 ? 1 : minDist), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
					int finalRank = normDist * termRank;
					//'rank' of the current subTerm is greater than the previous subTerm, update the overall 'rank' and save the rank of the
					// subTerm for possible
					// further updates
					if (finalRank > curMergedPos.rank) {
						curMerged.proc -= curMergedPos.rank;
						curMergedPos.rank = finalRank;
						curMerged.proc += finalRank;
					}
				}
			} else {
				if (vidStatus) {
					if constexpr (isGroupMergeWithAreas<MergedIdRelGroupType>()) {
						auto& pos = mergedPos[idoffsets[vid]].posTmp;
						pos.reserve(pos.size() + relid.Size());
						for (const auto& p : relid.Pos()) {
							if constexpr (std::is_same_v<typename MergedIdRelGroupType::TypeTParam, IdRelType::PosType>) {
								pos.emplace_back(p, -1);
							} else if constexpr (std::is_same_v<typename MergedIdRelGroupType::TypeTParam, PosTypeDebug>) {
								utf16_to_utf8(rawRes.term.pattern, termInf.ftDslTerm);
								pos.emplace_back(PosTypeDebug(p, termInf.ToString()), -1);
							} else {
								static_assert(!sizeof(MergedIdRelGroupType), "incorrect MergedIdRelGroupType::TypeTParam type");
							}
						}
					} else {
						auto& pos = mergedPos[idoffsets[vid]].posTmp;
						pos.reserve(pos.Size() + relid.Size());

						for (const auto& p : relid.Pos()) {
							pos.Add(p);
						}
					}
				} else if (merged.size() < holder_.cfg_->mergeLimit) {
					MergeInfo info;
					info.id = vid;
					info.proc = termRank;
					info.field = field;
					merged.push_back(std::move(info));
					mergeStatuses[vid] = rawResIndex + 1;
					present[vid] = true;
					idoffsets[vid] = merged.size() - 1;
					if constexpr (isGroupMergeWithAreas<MergedIdRelGroupType>()) {
						h_vector<std::pair<typename MergedIdRelGroupType::TypeTParam, int>, 4> posTmp;
						posTmp.reserve(relid.Size());
						for (const auto& p : relid.Pos()) {
							if constexpr (std::is_same_v<MergedIdRelGroupType, MergedIdRelGroupArea<IdRelType::PosType>>) {
								posTmp.emplace_back(p, -1);
							} else if constexpr (std::is_same_v<MergedIdRelGroupType, MergedIdRelGroupArea<PosTypeDebug>>) {
								utf16_to_utf8(rawRes.term.pattern, termInf.ftDslTerm);
								PosTypeDebug pd{p, termInf.ToString()};
								posTmp.emplace_back(pd, -1);
							} else {
								static_assert(!sizeof(MergedIdRelGroupType), "incorrect MergedIdRelGroupType type");
							}
						}
						mergedPos.emplace_back(IdRelType(std::move(relid)), int(termRank), rawRes.term.opts.qpos, std::move(posTmp));

					} else {
						mergedPos.emplace_back(IdRelType(std::move(relid)), int(termRank), rawRes.term.opts.qpos);
					}
				}
			}
		}
	}
	for (size_t mergedIndex = 0; mergedIndex < merged.size(); mergedIndex++) {
		auto& mergedInfo = merged[mergedIndex];
		auto& mergedPosInfo = mergedPos[mergedIndex];
		if (mergedPosInfo.posTmp.empty()) {
			mergedInfo.proc = 0;
			mergeStatuses[mergedInfo.id] = 0;
			mergedPosInfo.cur.Clear();
			mergedPosInfo.next.Clear();
			mergedPosInfo.rank = 0;
			continue;
		}
		if constexpr (isGroupMerge<MergedIdRelGroupType>()) {
			mergedPosInfo.posTmp.SortAndUnique();
			mergedPosInfo.cur = std::move(mergedPosInfo.posTmp);
			mergedPosInfo.next.Clear();
			mergedPosInfo.posTmp.Clear();
			mergedPosInfo.rank = 0;
		} else {
			auto& posTmp = mergedPosInfo.posTmp;
			boost::sort::pdqsort_branchless(
				posTmp.begin(), posTmp.end(),
				[](const std::pair<typename MergedIdRelGroupType::TypeTParam, int>& l,
				   const std::pair<typename MergedIdRelGroupType::TypeTParam, int>& r) noexcept { return l.first < r.first; });

			auto last = std::unique(posTmp.begin(), posTmp.end());
			posTmp.resize(last - posTmp.begin());

			mergedPosInfo.cur.Clear();
			for (const auto& p : mergedPosInfo.posTmp) {
				mergedPosInfo.cur.Add(p.first);
			}
			mergedPosInfo.wordPosForChain.emplace_back(std::move(mergedPosInfo.posTmp));
			mergedPosInfo.posTmp.clear();
			mergedPosInfo.next.Clear();
			mergedPosInfo.rank = 0;
		}
	}
}

template <typename IdCont>
template <typename MergedIdRelGroupType, typename Bm25Type, typename MergedOffsetT, typename MergeType>
void Selector<IdCont>::mergeResultsPart(std::vector<TextSearchResults>& rawResults, size_t from, size_t to, MergeType& merged,
										std::vector<MergedIdRelGroupType>& mergedPos, const bool inTransaction, const RdxContext& rdxCtx) {
	// Current implementation supports OpAnd only
	assertrx_throw(to <= rawResults.size());
	FtMergeStatuses::Statuses mergeStatuses;
	std::vector<MergedOffsetT> idoffsets;

	mergeStatuses.resize(holder_.vdocs_.size(), 0);

	// upper estimate number of documents
	uint32_t idsMaxCnt = rawResults[from].idsCnt_;

	merged.reserve(std::min(holder_.cfg_->mergeLimit, idsMaxCnt));

	if (to - from > 1) {
		idoffsets.resize(holder_.vdocs_.size());
	}
	std::vector<bool> exists;
	bool firstTerm = true;
	for (size_t i = from; i < to; ++i) {
		mergeIterationGroup<MergedIdRelGroupType, Bm25Type, MergedOffsetT, MergeType>(rawResults[i], i, mergeStatuses, merged, mergedPos,
																					  idoffsets, exists, firstTerm, inTransaction, rdxCtx);
		firstTerm = false;
		// set proc=0 (exclude) for document not containing term
		for (auto& info : merged) {
			const auto vid = info.id;
			if (exists[vid] || mergeStatuses[vid] == FtMergeStatuses::kExcluded || info.proc == 0) {
				continue;
			}
			info.proc = 0;
			mergeStatuses[vid] = 0;
		}
	}

	// Update full match rank
	for (size_t ofs = 0; ofs < merged.size(); ++ofs) {
		auto& m = merged[ofs];
		if (size_t(holder_.vdocs_[m.id].wordsCount[m.field]) == rawResults.size()) {
			m.proc *= holder_.cfg_->fullMatchBoost;
		}
	}
}

template <typename IdCont>
size_t Selector<IdCont>::TyposHandler::Process(std::vector<TextSearchResults>& rawResults, const DataHolder<IdCont>& holder,
											   const std::wstring& pattern, const std::vector<std::wstring>& variantsForTypos) {
	TextSearchResults& res = rawResults.back();
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
						res.push_back({&hword.vids, typoIt->first, proc});
						res.idsCnt_ += hword.vids.size();
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

		if rx_unlikely (holder.cfg_->logLevel >= LogInfo) {
			logFmt(LogInfo, "Lookup typos, matched {} typos, with {} vids, skipped {}", matched, vids, skipped);
		}
	}
	return totalVids;
}

RX_ALWAYS_INLINE unsigned uabs(int a) { return unsigned(std::abs(a)); }

template <typename IdCont>
template <typename... Args>
void Selector<IdCont>::TyposHandler::logTraceF(int level, fmt::format_string<Args...> fmt, Args&&... args) {
	if rx_unlikely (logLevel_ >= LogTrace) {
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
			// current.len == 1 && found.len == 2. I.e. exactly one letter must be changed and moved up to maxTypoDist_ value and the other
			// letter is missing in 'current'
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
			// current.len == 1 && found.len == 2. I.e. exactly one letter must be moved up to maxLettPermDist_ value and the other letter
			// is missing in 'current'
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
				// current.len == 2 && found.len == 1. I.e. exactly one letter must be moved up to maxLettPermDist_ value and 'current' also
				// has one extra letter
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
template <typename Bm25T, typename MergedOffsetT, typename MergedType>
MergedType Selector<IdCont>::mergeResults(std::vector<TextSearchResults>&& rawResults, size_t maxMergedSize,
										  const std::vector<size_t>& synonymsBounds, bool inTransaction, RankSortType rankSortType,
										  FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx) {
	const auto& vdocs = holder_.vdocs_;

	MergedType merged;
	if (!rawResults.size() || !vdocs.size()) {
		return merged;
	}

	assertrx_throw(FtMergeStatuses::kExcluded > rawResults.size());
	assertrx_throw(mergeStatuses.size() == vdocs.size());
	std::vector<MergedIdRel> merged_rd;

	std::vector<MergedOffsetT> idoffsets;
	for (auto& rawRes : rawResults) {
		boost::sort::pdqsort_branchless(
			rawRes.begin(), rawRes.end(),
			[](const TextSearchResult& lhs, const TextSearchResult& rhs) noexcept { return lhs.proc > rhs.proc; });
	}
	merged.reserve(maxMergedSize);

	if (rawResults.size() > 1) {
		idoffsets.resize(vdocs.size());
		merged_rd.reserve(maxMergedSize);
	}
	std::vector<std::vector<bool>> exists(synonymsBounds.size() + 1);
	size_t curExists = 0;
	auto nextSynonymsBound = synonymsBounds.cbegin();
	bool hasBeenAnd = false;
	for (index_t i = 0, lastGroupStart = 0; i < rawResults.size(); ++i) {
		if (rawResults[i].term.opts.groupNum != -1) {
			size_t k = i;
			OpType op = rawResults[i].term.opts.op;
			int groupNum = rawResults[i].term.opts.groupNum;
			while (k < rawResults.size() && rawResults[k].term.opts.groupNum == groupNum) {
				rawResults[k].term.opts.op = OpAnd;
				k++;
			}
			if constexpr (std::is_same_v<MergedType, MergeData<Area>>) {
				mergeGroupResult<MergedIdRelGroupArea<IdRelType::PosType>, Bm25T, MergedOffsetT, MergedType>(
					rawResults, i, k, mergeStatuses, merged, merged_rd, op, hasBeenAnd, idoffsets, inTransaction, rdxCtx);
			} else if constexpr (std::is_same_v<MergedType, MergeData<AreaDebug>>) {
				mergeGroupResult<MergedIdRelGroupArea<PosTypeDebug>, Bm25T, MergedOffsetT, MergedType>(
					rawResults, i, k, mergeStatuses, merged, merged_rd, op, hasBeenAnd, idoffsets, inTransaction, rdxCtx);
			} else {
				mergeGroupResult<MergedIdRelGroup, Bm25T, MergedOffsetT, MergedType>(rawResults, i, k, mergeStatuses, merged, merged_rd, op,
																					 hasBeenAnd, idoffsets, inTransaction, rdxCtx);
			}
			if (op == OpAnd) {
				hasBeenAnd = true;
			}
			i = k - 1;
			continue;
		}

		if (nextSynonymsBound != synonymsBounds.cend() && *nextSynonymsBound == i) {
			hasBeenAnd = false;
			++curExists;
			++nextSynonymsBound;
			if (nextSynonymsBound == synonymsBounds.cend()) {
				lastGroupStart = 0;
			} else {
				lastGroupStart = i;
			}
		}

		mergeIteration<Bm25T>(rawResults[i], i, mergeStatuses, merged, merged_rd, idoffsets, exists[curExists], hasBeenAnd, inTransaction,
							  rdxCtx);

		if (rawResults[i].term.opts.op == OpAnd && !exists[curExists].empty()) {
			hasBeenAnd = true;
			for (auto& info : merged) {
				const auto vid = info.id;
				if (exists[curExists][vid] || mergeStatuses[vid] == FtMergeStatuses::kExcluded || mergeStatuses[vid] <= lastGroupStart ||
					info.proc == 0) {
					continue;
				}
				bool matchSyn = false;
				for (size_t synGrpIdx : rawResults[i].synonymsGroups) {
					assertrx(synGrpIdx < curExists);
					if (exists[synGrpIdx][vid]) {
						matchSyn = true;
						break;
					}
				}
				if (matchSyn) {
					continue;
				}
				info.proc = 0;
				mergeStatuses[vid] = 0;
			}
		}
	}
	if rx_unlikely (holder_.cfg_->logLevel >= LogInfo) {
		logFmt(LogInfo, "Complex merge ({} patterns): out {} vids", rawResults.size(), merged.size());
	}

	// Update full match rank
	for (size_t ofs = 0; ofs < merged.size(); ++ofs) {
		auto& m = merged[ofs];
		if (size_t(vdocs[m.id].wordsCount[m.field]) == rawResults.size()) {
			m.proc *= holder_.cfg_->fullMatchBoost;
		}
		if (merged.maxRank < m.proc) {
			merged.maxRank = m.proc;
		}
	}
	switch (rankSortType) {
		case RankSortType::RankOnly:
		case RankSortType::IDAndPositions:
			boost::sort::pdqsort_branchless(merged.begin(), merged.end(),
											[](const MergeInfo& lhs, const MergeInfo& rhs) noexcept { return lhs.proc > rhs.proc; });
			return merged;
		case RankSortType::RankAndID:
		case RankSortType::IDOnly:
			return merged;
		case RankSortType::ExternalExpression:
			throw Error(errLogic, "RankSortType::ExternalExpression not implemented.");
			break;
	}
	return merged;
}

template <typename IdCont>
void Selector<IdCont>::printVariants(const FtSelectContext& ctx, const TextSearchResults& res) {
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
template MergeDataBase Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, MergeDataBase>(FtDSLQuery&&, bool, RankSortType,
																								 FtMergeStatuses::Statuses&&,
																								 const RdxContext&);
template MergeData<Area> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, MergeData<Area>>(FtDSLQuery&&, bool, RankSortType,
																									 FtMergeStatuses::Statuses&&,
																									 const RdxContext&);
template MergeData<AreaDebug> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::No, MergeData<AreaDebug>>(FtDSLQuery&&, bool,
																											   RankSortType,
																											   FtMergeStatuses::Statuses&&,
																											   const RdxContext&);

template MergeDataBase Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																				   FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeData<Area> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																					 FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeData<AreaDebug> Selector<PackedIdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																						  FtMergeStatuses::Statuses&&, const RdxContext&);

template class Selector<IdRelVec>;
template MergeDataBase Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&,
																			const RdxContext&);
template MergeData<Area> Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&,
																			  const RdxContext&);
template MergeData<AreaDebug> Selector<IdRelVec>::Process<FtUseExternStatuses::No>(FtDSLQuery&&, bool, RankSortType,
																				   FtMergeStatuses::Statuses&&, const RdxContext&);

template MergeDataBase Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType, FtMergeStatuses::Statuses&&,
																			 const RdxContext&);
template MergeData<Area> Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																			   FtMergeStatuses::Statuses&&, const RdxContext&);
template MergeData<AreaDebug> Selector<IdRelVec>::Process<FtUseExternStatuses::Yes>(FtDSLQuery&&, bool, RankSortType,
																					FtMergeStatuses::Statuses&&, const RdxContext&);

}  // namespace reindexer
