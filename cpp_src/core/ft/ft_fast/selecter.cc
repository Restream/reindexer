#include "selecter.h"
#include <map>
#include "core/ft/bm25.h"
#include "core/ft/typos.h"
#include "core/rdxcontext.h"
#include "sort/pdqsort.hpp"
#include "tools/logger.h"

namespace {
inline double pos2rank(int pos) {
	if (pos <= 10) return 1.0 - (pos / 100.0);
	if (pos <= 100) return 0.9 - (pos / 1000.0);
	if (pos <= 1000) return 0.8 - (pos / 10000.0);
	if (pos <= 10000) return 0.7 - (pos / 100000.0);
	if (pos <= 100000) return 0.6 - (pos / 1000000.0);
	return 0.5;
}
}  // namespace

namespace reindexer {
// Relevancy procent of full word match
const int kFullMatchProc = 100;
// Mininum relevancy procent of prefix word match.
const int kPrefixMinProc = 50;
const int kSuffixMinProc = 10;
// Maximum relevancy procent of typo match
const int kTypoProc = 85;
// Relevancy step of typo match
const int kTypoStepProc = 15;
// Decrease procent of relevancy if pattern found by word stem
const int kStemProcDecrease = 15;
// Minimal relevant length of the stemmer's term
constexpr int kMinStemRellevantLen = 3;
// Max length of the stemming result, which will be skipped
constexpr int kMaxStemSkipLen = 1;
constexpr bool kVariantsWithDifLength = (kMinStemRellevantLen - kMaxStemSkipLen) > 2;

template <typename IdCont>
void Selecter<IdCont>::prepareVariants(std::vector<FtVariantEntry>& variants, h_vector<FtBoundVariantEntry, 4>* lowRelVariants,
									   size_t termIdx, const std::vector<std::string>& langs, const FtDSLQuery& dsl,
									   std::vector<SynonymsDsl>* synonymsDsl) {
	const FtDSLEntry& term = dsl[termIdx];
	variants.clear();

	std::vector<FtDSLVariant> variantsUtf16{{term.pattern, kFullMatchProc}};

	if (synonymsDsl && (!holder_.cfg_->enableNumbersSearch || !term.opts.number)) {
		// Make translit and kblayout variants
		if (holder_.cfg_->enableTranslit && !term.opts.exact) {
			holder_.translit_->GetVariants(term.pattern, variantsUtf16);
		}
		if (holder_.cfg_->enableKbLayout && !term.opts.exact) {
			holder_.kbLayout_->GetVariants(term.pattern, variantsUtf16);
		}
		// Synonyms
		if (term.opts.op != OpNot) {
			holder_.synonyms_->GetVariants(term.pattern, variantsUtf16);
			holder_.synonyms_->PostProcess(term, dsl, termIdx, *synonymsDsl);
		}
	}

	// Apply stemmers
	std::string tmpstr, stemstr;
	for (auto& v : variantsUtf16) {
		utf16_to_utf8(v.pattern, tmpstr);
		if (tmpstr.empty()) {
			continue;
		}
		variants.emplace_back(tmpstr, term.opts, v.proc, -1);
		if (!term.opts.exact) {
			if (term.opts.op == OpNot && term.opts.suff) {
				// More strict match for negative (excluding) suffix terms
				if (holder_.cfg_->logLevel >= LogTrace) {
					logPrintf(LogInfo, "Skipping stemming for '%s%s%s'", term.opts.suff ? "*" : "", tmpstr, term.opts.pref ? "*" : "");
				}
				continue;
			}
			for (auto& lang : langs) {
				auto stemIt = holder_.stemmers_.find(lang);
				if (stemIt == holder_.stemmers_.end()) {
					throw Error(errParams, "Stemmer for language %s is not available", lang);
				}
				stemstr.clear();
				stemIt->second.stem(tmpstr, stemstr);
				if (tmpstr != stemstr && !stemstr.empty()) {
					const auto charsCount = getUTF8StringCharactersCount(stemstr);
					if (charsCount <= kMaxStemSkipLen) {
						if (holder_.cfg_->logLevel >= LogTrace) {
							logPrintf(LogInfo, "Skipping too short stemmer's term '%s%s*'",
									  term.opts.suff && &v != &variantsUtf16[0] ? "*" : "", stemstr);
						}
						continue;
					}
					FtDslOpts opts = term.opts;
					opts.pref = true;

					if (&v != &variantsUtf16[0]) opts.suff = false;

					const auto charCount = getUTF8StringCharactersCount(stemstr);
					if (charCount >= kMinStemRellevantLen || !lowRelVariants) {
						variants.emplace_back(std::move(stemstr), std::move(opts), v.proc - kStemProcDecrease, charsCount);
					} else {
						lowRelVariants->emplace_back(std::move(stemstr), std::move(opts), v.proc - kStemProcDecrease, charsCount);
					}
				}
			}
		}
	}
}

template <typename IdCont>
template <bool mergeStatusesEmpty>
IDataHolder::MergeData Selecter<IdCont>::Process(FtDSLQuery&& dsl, bool inTransaction, FtMergeStatuses::Statuses&& mergeStatuses,
												 const RdxContext& rdxCtx) {
	FtSelectContext ctx;
	ctx.rawResults.reserve(dsl.size());
	// STEP 2: Search dsl terms for each variant
	std::vector<SynonymsDsl> synonymsDsl;
	holder_.synonyms_->PreProcess(dsl, synonymsDsl);
	if (!inTransaction) ThrowOnCancel(rdxCtx);
	for (size_t i = 0; i < dsl.size(); ++i) {
		const auto irrVariantsCount = ctx.lowRelVariants.size();

		// Prepare term variants (original + translit + stemmed + kblayout + synonym)
		this->prepareVariants(ctx.variants, &ctx.lowRelVariants, i, holder_.cfg_->stemmers, dsl, &synonymsDsl);
		auto v = ctx.GetWordsMapPtr(dsl[i].opts);
		ctx.rawResults.emplace_back(std::move(dsl[i]), v);
		TextSearchResults& res = ctx.rawResults.back();

		if (irrVariantsCount != ctx.lowRelVariants.size()) {
			ctx.rawResults.back().SwitchToInternalWordsMap();
		}
		for (unsigned j = irrVariantsCount; j < ctx.lowRelVariants.size(); ++j) {
			ctx.lowRelVariants[j].rawResultIdx = ctx.rawResults.size() - 1;
		}

		if (holder_.cfg_->logLevel >= LogInfo) {
			WrSerializer wrSer;
			wrSer << "variants: [";
			for (auto& variant : ctx.variants) {
				if (&variant != &*ctx.variants.begin()) wrSer << ", ";
				wrSer << variant.pattern;
			}
			wrSer << "], variants_with_low_relevancy: [";
			for (auto& variant : ctx.lowRelVariants) {
				if (&variant != &*ctx.lowRelVariants.begin()) wrSer << ", ";
				wrSer << variant.pattern;
			}
			wrSer << "], typos: [";
			if (res.term.opts.typos) {
				typos_context tctx[kMaxTyposInWord];
				mktypos(tctx, res.term.pattern, holder_.cfg_->MaxTyposInWord(), holder_.cfg_->maxTypoLen,
						[&wrSer](std::string_view typo, int) {
							wrSer << typo;
							wrSer << ", ";
						});
			}
			logPrintf(LogInfo, "Variants: [%s]", wrSer.Slice());
		}

		processVariants<mergeStatusesEmpty>(ctx, mergeStatuses);
		if (res.term.opts.typos) {
			// Lookup typos from typos_ map and fill results
			processTypos(ctx, res.term);
		}
	}

	std::vector<TextSearchResults> results;
	size_t reserveSize = ctx.rawResults.size();
	for (const SynonymsDsl& synDsl : synonymsDsl) reserveSize += synDsl.dsl.size();
	results.reserve(reserveSize);
	std::vector<size_t> synonymsBounds;
	synonymsBounds.reserve(synonymsDsl.size());
	if (!inTransaction) ThrowOnCancel(rdxCtx);
	for (SynonymsDsl& synDsl : synonymsDsl) {
		FtSelectContext synCtx;
		synCtx.rawResults.reserve(synDsl.dsl.size());
		for (size_t i = 0; i < synDsl.dsl.size(); ++i) {
			prepareVariants(synCtx.variants, nullptr, i, holder_.cfg_->stemmers, synDsl.dsl, nullptr);
			if (holder_.cfg_->logLevel >= LogInfo) {
				WrSerializer wrSer;
				for (auto& variant : synCtx.variants) {
					if (&variant != &*synCtx.variants.begin()) wrSer << ", ";
					wrSer << variant.pattern;
				}
				logPrintf(LogInfo, "Multiword synonyms variants: [%s]", wrSer.Slice());
			}
			auto v = ctx.GetWordsMapPtr(synDsl.dsl[i].opts);
			synCtx.rawResults.emplace_back(std::move(synDsl.dsl[i]), v);
			if (synCtx.rawResults.back().term.opts.op == OpAnd) {
				ctx.rawResults.back().SwitchToInternalWordsMap();
			}
			processVariants<mergeStatusesEmpty>(synCtx, mergeStatuses);
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
	processLowRelVariants<mergeStatusesEmpty>(ctx, mergeStatuses);
	// Typos for terms with low relevancy will not be processed

	for (auto& res : ctx.rawResults) results.emplace_back(std::move(res));
	return mergeResults(std::move(results), ctx.totalORVids, synonymsBounds, inTransaction, std::move(mergeStatuses), rdxCtx);
}

template <typename IdCont>
template <bool mergeStatusesEmpty>
void Selecter<IdCont>::processStepVariants(FtSelectContext& ctx, typename DataHolder<IdCont>::CommitStep& step,
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
		if (keyIt == suffixes.end()) break;
		if (vidsLimit <= vids) {
			if (holder_.cfg_->logLevel >= LogInfo) {
				logPrintf(LogInfo, "Terminating suffix loop on limit (%d). Current vairiant is '%s%s%s'", initialLimit,
						  variant.opts.suff ? "*" : "", variant.pattern, variant.opts.pref ? "*" : "");
			}
			break;
		}

		const WordIdType glbwordId = keyIt->second;
		const auto& hword = holder_.getWordById(glbwordId);

		if constexpr (!mergeStatusesEmpty) {
			bool excluded = true;
			for (const auto& id : hword.vids_) {
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

		if (!withSuffixes && suffixLen) continue;
		if (!withPrefixes && wordLength != matchLen + suffixLen) break;

		const int matchDif = std::abs(long(wordLength - matchLen + suffixLen));
		const int proc = std::max(variant.proc - holder_.cfg_->partialMatchDecrease * matchDif / std::max(matchLen, 3),
								  suffixLen ? kSuffixMinProc : kPrefixMinProc);

		const auto it = res.foundWords->find(glbwordId);
		if (it == res.foundWords->end() || it->second.first != curRawResultIdx) {
			res.push_back({&hword.vids_, keyIt->first, proc, suffixes.virtual_word_len(suffixWordId)});
			const auto vidsSize = hword.vids_.size();
			res.idsCnt_ += vidsSize;
			if (variant.opts.op == OpOr) {
				ctx.totalORVids += vidsSize;
			}
			(*res.foundWords)[glbwordId] = std::make_pair(curRawResultIdx, res.size() - 1);
			if (holder_.cfg_->logLevel >= LogTrace) {
				logPrintf(LogInfo, " matched %s '%s' of word '%s' (variant '%s'), %d vids, %d%%", suffixLen ? "suffix" : "prefix",
						  keyIt->first, word, variant.pattern, holder_.getWordById(glbwordId).vids_.size(), proc);
			}
			++matched;
			vids += vidsSize;
		} else {
			if (ctx.rawResults[it->second.first][it->second.second].proc_ < proc)
				ctx.rawResults[it->second.first][it->second.second].proc_ = proc;
			++skipped;
		}
	} while ((keyIt++).lcp() >= int(tmpstr.length()));
	if (holder_.cfg_->logLevel >= LogInfo) {
		std::string limitString;
		if (vidsLimit <= vids) {
			limitString = fmt::sprintf(". Lookup terminated by VIDs limit(%d)", initialLimit);
		}
		logPrintf(LogInfo, "Lookup variant '%s' (%d%%), matched %d suffixes, with %d vids, skiped %d, excluded %d%s", tmpstr, variant.proc,
				  matched, vids, skipped, excludedCnt, limitString);
	}
}

template <typename IdCont>
template <bool mergeStatusesEmpty>
void Selecter<IdCont>::processVariants(FtSelectContext& ctx, const FtMergeStatuses::Statuses& mergeStatuses) {
	for (const FtVariantEntry& variant : ctx.variants) {
		for (auto& step : holder_.steps) {
			processStepVariants<mergeStatusesEmpty>(ctx, step, variant, ctx.rawResults.size() - 1, mergeStatuses,
													std::numeric_limits<int>::max());
		}
	}
}

template <typename IdCont>
template <bool mergeStatusesEmpty>
void Selecter<IdCont>::processLowRelVariants(FtSelectContext& ctx, const FtMergeStatuses::Statuses& mergeStatuses) {
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
		boost::sort::pdqsort(ctx.lowRelVariants.begin(), ctx.lowRelVariants.end(),
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
					if (holder_.cfg_->logLevel >= LogTrace) {
						logPrintf(LogInfo, "Terminating on target OR limit. Current vairiant is '%s%s%s'", variant.opts.suff ? "*" : "",
								  variant.pattern, variant.opts.pref ? "*" : "");
					}
					break;
				}
				lastVariantLen = variant.GetLenCached();
			}
		} else {
			(void)lastVariantLen;
		}
		if (holder_.cfg_->logLevel >= LogTrace) {
			logPrintf(LogInfo, "Handling '%s%s%s' as variant with low relevancy", variant.opts.suff ? "*" : "", variant.pattern,
					  variant.opts.pref ? "*" : "");
		}
		switch (variant.opts.op) {
			case OpOr: {
				int remainingLimit = targetORLimit - ctx.totalORVids;
				if (remainingLimit > 0) {
					for (auto& step : holder_.steps) {
						processStepVariants<mergeStatusesEmpty>(ctx, step, variant, variant.rawResultIdx, mergeStatuses, remainingLimit);
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
						processStepVariants<mergeStatusesEmpty>(ctx, step, variant, variant.rawResultIdx, mergeStatuses, remainingLimit);
					}
				}
				break;
			}
		}
	}
}

template <typename IdCont>
void Selecter<IdCont>::processTypos(FtSelectContext& ctx, const FtDSLEntry& term) {
	TextSearchResults& res = ctx.rawResults.back();
	const unsigned curRawResultIdx = ctx.rawResults.size() - 1;
	const auto maxTyposInWord = holder_.cfg_->MaxTyposInWord();
	const bool dontUseMaxTyposForBoth = maxTyposInWord != holder_.cfg_->maxTypos / 2;
	const size_t patternSize = utf16_to_utf8(term.pattern).size();
	for (auto& step : holder_.steps) {
		typos_context tctx[kMaxTyposInWord];
		const decltype(step.typosHalf_)* typoses[2]{&step.typosHalf_, &step.typosMax_};
		int matched = 0, skiped = 0, vids = 0;
		mktypos(tctx, term.pattern, maxTyposInWord, holder_.cfg_->maxTypoLen, [&](std::string_view typo, int level) {
			const int tcount = maxTyposInWord - level;
			for (const auto* typos : typoses) {
				const auto typoRng = typos->equal_range(typo);
				for (auto typoIt = typoRng.first; typoIt != typoRng.second; ++typoIt) {
					const WordIdType wordIdglb = typoIt->second;
					auto& step = holder_.GetStep(wordIdglb);

					auto wordIdSfx = holder_.GetSuffixWordId(wordIdglb, step);

					// bool virtualWord = suffixes_.is_word_virtual(wordId);
					uint8_t wordLength = step.suffixes_.word_len_at(wordIdSfx);
					int proc = kTypoProc - tcount * kTypoStepProc / std::max((wordLength - tcount) / 3, 1);
					auto it = res.foundWords->find(wordIdglb);
					if (it == res.foundWords->end() || it->second.first != curRawResultIdx) {
						const auto& hword = holder_.getWordById(wordIdglb);
						res.push_back({&hword.vids_, typoIt->first, proc, step.suffixes_.virtual_word_len(wordIdSfx)});
						res.idsCnt_ += hword.vids_.size();
						res.foundWords->emplace(wordIdglb, std::make_pair(curRawResultIdx, res.size() - 1));

						if (holder_.cfg_->logLevel >= LogTrace)
							logPrintf(LogInfo, " matched typo '%s' of word '%s', %d ids, %d%%", typoIt->first,
									  step.suffixes_.word_at(wordIdSfx), hword.vids_.size(), proc);
						++matched;
						vids += hword.vids_.size();
					} else {
						++skiped;
					}
				}
				if (dontUseMaxTyposForBoth && level == 1 && typo.size() != patternSize) return;
			}
		});
		if (holder_.cfg_->logLevel >= LogInfo) {
			logPrintf(LogInfo, "Lookup typos, matched %d typos, with %d vids, skiped %d", matched, vids, skiped);
		}
	}
}

static double bound(double k, double weight, double boost) noexcept { return (1.0 - weight) + k * boost * weight; }

template <typename IdCont>
void Selecter<IdCont>::debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank, int prevRank) {
#ifdef REINDEX_FT_EXTRA_DEBUG
	if (holder_.cfg_->logLevel < LogTrace) return;

	logPrintf(LogInfo, "%s - '%s' (vid %d), bm25 %f, dist %f, rank %d (prev rank %d)", msg, holder_.vdocs_[vid].keyDoc, vid, normBm25,
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
void Selecter<IdCont>::calcFieldBoost(double idf, unsigned long long f, const IdRelType& relid, const FtDslOpts& opts, int termProc,
									  double& termRank, double& normBm25, bool& dontSkipCurTermRank, h_vector<double, 4>& ranksInFields,
									  int& field) {
	assertrx(f < holder_.cfg_->fieldsCfg.size());
	const auto& fldCfg = holder_.cfg_->fieldsCfg[f];
	// raw bm25
	const double bm25 = idf * bm25score(relid.WordsInField(f), holder_.vdocs_[relid.Id()].mostFreqWordCount[f],
										holder_.vdocs_[relid.Id()].wordsCount[f], holder_.avgWordsCount_[f]);

	// normalized bm25
	const double normBm25Tmp = bound(bm25, fldCfg.bm25Weight, fldCfg.bm25Boost);

	const double positionRank = bound(::pos2rank(relid.MinPositionInField(f)), fldCfg.positionWeight, fldCfg.positionBoost);

	float termLenBoost = bound(opts.termLenBoost, fldCfg.termLenWeight, fldCfg.termLenBoost);
	// final term rank calculation
	const double termRankTmp = opts.fieldsOpts[f].boost * termProc * normBm25Tmp * opts.boost * termLenBoost * positionRank;
	const bool needSumRank = opts.fieldsOpts[f].needSumRank;
	if (termRankTmp > termRank) {
		if (dontSkipCurTermRank) {
			ranksInFields.push_back(termRank);
		}
		field = f;
		normBm25 = normBm25Tmp;
		termRank = termRankTmp;
		dontSkipCurTermRank = needSumRank;
	} else if (!dontSkipCurTermRank && needSumRank && termRank == termRankTmp) {
		field = f;
		normBm25 = normBm25Tmp;
		dontSkipCurTermRank = true;
	} else if (termRankTmp && needSumRank) {
		ranksInFields.push_back(termRankTmp);
	}
}

template <typename IdCont>
AreaHolder Selecter<IdCont>::createAreaFromSubMerge(const IDataHolder::MergedIdRelExArea& posInfo) {
	AreaHolder area;
	if (posInfo.wordPosForChain.empty()) {
		return area;
	}

	for (const auto& v : posInfo.wordPosForChain.back()) {
		IdRelType::PosType last = v.first;
		IdRelType::PosType first = v.first;
		int indx = int(posInfo.wordPosForChain.size()) - 2;
		int prevIndex = v.second;
		while (indx >= 0 && prevIndex != -1) {
			auto pos = posInfo.wordPosForChain[indx][prevIndex].first;
			prevIndex = posInfo.wordPosForChain[indx][prevIndex].second;
			first = pos;
			indx--;
		}
		assert(first.field() == last.field());
		area.InsertArea(Area(first.pos(), last.pos() + 1), v.first.field(), maxAreasInDoc_);
	}
	return area;
}
template <typename IdCont>
void Selecter<IdCont>::copyAreas(AreaHolder& subMerged, AreaHolder& merged) {
	for (size_t f = 0; f < fieldSize_; f++) {
		auto areas = subMerged.GetAreas(f);
		if (areas) {
			for (auto& v : *areas) {
				merged.InsertArea(std::move(v), f, maxAreasInDoc_);
			}
		}
	}
}

template <typename IdCont>
template <typename PosType>
void Selecter<IdCont>::subMergeLoop(std::vector<IDataHolder::MergeInfo>& subMerged, std::vector<PosType>& subMergedPos,
									IDataHolder::MergeData& merged, std::vector<IDataHolder::MergedIdRel>& merged_rd,
									FtMergeStatuses::Statuses& mergeStatuses, std::vector<uint16_t>& idoffsets,
									std::vector<bool>* checkAndOpMerge, const bool hasBeenAnd) {
	for (auto& mergeInfo : subMerged) {
		if (mergeInfo.proc == 0) {
			break;
		}

		index_t& mergeStatus = mergeStatuses[mergeInfo.id];
		if (mergeStatus == 0 && !hasBeenAnd && int(merged.size()) < holder_.cfg_->mergeLimit) {
			mergeStatus = 1;
			IDataHolder::MergeInfo m;
			IDataHolder::MergedIdRel mPos;

			m.id = mergeInfo.id;
			m.proc = mergeInfo.proc;
			m.matched = mergeInfo.matched;
			m.field = mergeInfo.field;
			PosType& smPos = subMergedPos[mergeInfo.indexAdd];
			if constexpr (isGroupMergeWithAreas<PosType>()) {
				mPos.next.reserve(smPos.posTmp.size());
				for (const auto& p : smPos.posTmp) {
					mPos.next.Add(p.first);
				}
				AreaHolder area = createAreaFromSubMerge(smPos);
				merged.vectorAreas.push_back(std::move(area));
				m.areaIndex = merged.vectorAreas.size() - 1;
			} else {
				mPos.next = std::move(smPos.posTmp);
			}

			mPos.cur = std::move(smPos.cur);
			mPos.rank = smPos.rank;
			mPos.qpos = smPos.qpos;

			m.indexAdd = merged.size();
			merged.emplace_back(std::move(m));
			merged_rd.emplace_back(std::move(mPos));
			idoffsets[mergeInfo.id] = merged.size() - 1;
		} else if (mergeStatus != 0 && mergeStatus != FtMergeStatuses::kExcluded) {
			const size_t mergedIndex = idoffsets[mergeInfo.id];
			if (merged[mergedIndex].proc < mergeInfo.proc) {
				// TODO calc proc
				merged[mergedIndex].proc = mergeInfo.proc;
			}
			auto& subPos = subMergedPos[mergeInfo.indexAdd];
			if constexpr (isGroupMergeWithAreas<PosType>()) {
				subPos.next.reserve(subPos.posTmp.size());
				for (const auto& p : subPos.posTmp) {
					subPos.next.Add(p.first);
				}
				AreaHolder area = createAreaFromSubMerge(subPos);
				int32_t areaIndex = merged[mergedIndex].areaIndex;
				if (areaIndex != -1 && areaIndex >= int(merged.vectorAreas.size())) {
					throw Error(errLogic, "FT merge: Incorrect area index %d (areas vector size is %d)", areaIndex,
								merged.vectorAreas.size());
				}
				AreaHolder& areaTo = merged.vectorAreas[areaIndex];
				copyAreas(area, areaTo);
			} else {
				subPos.next = std::move(subPos.posTmp);
			}
			IDataHolder::MergedIdRel& mergedPosVectorElemPointer = merged_rd[merged[mergedIndex].indexAdd];
			mergedPosVectorElemPointer.cur = std::move(subPos.cur);
			mergedPosVectorElemPointer.next = std::move(subPos.next);
			mergedPosVectorElemPointer.rank = subPos.rank;
			mergedPosVectorElemPointer.qpos = subPos.qpos;
			merged[mergedIndex].matched += mergeInfo.matched;
		}
		if (checkAndOpMerge) {
			(*checkAndOpMerge)[mergeInfo.id] = true;
		}
	}
}

template <typename IdCont>
template <typename PosType>
void Selecter<IdCont>::mergeGroupResult(std::vector<TextSearchResults>& rawResults, size_t from, size_t to,
										FtMergeStatuses::Statuses& mergeStatuses, IDataHolder::MergeData& merged,
										std::vector<IDataHolder::MergedIdRel>& merged_rd, OpType op, const bool hasBeenAnd,
										std::vector<uint16_t>& idoffsets, const bool inTransaction, const RdxContext& rdxCtx) {
	// And - MustPresent
	// Or  - MayBePresent
	// Not - NotPresent
	// hasBeenAnd shows whether it is possible to expand the set of documents (if there was already And, then the set of documents is not
	// expandable)
	IDataHolder::MergeData subMerged;
	std::vector<PosType> subMergedPositionData;
	mergeResultsPart(rawResults, from, to, subMerged, subMergedPositionData, inTransaction, rdxCtx);

	switch (op) {
		case OpOr: {
			subMergeLoop(subMerged, subMergedPositionData, merged, merged_rd, mergeStatuses, idoffsets, nullptr, hasBeenAnd);
			break;
		}
		case OpAnd: {
			// when executing And, you need to remove documents from mergeStatuses that are not in the results for this word
			// To do this, we intersect the checkAndOpMerge array with the merged array
			std::vector<bool> checkAndOpMerge;
			checkAndOpMerge.resize(holder_.vdocs_.size(), false);
			subMergeLoop(subMerged, subMergedPositionData, merged, merged_rd, mergeStatuses, idoffsets, &checkAndOpMerge, hasBeenAnd);
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
					break;	// subMerged sorted by proc (documents with proc==0 are not used)
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
void Selecter<IdCont>::mergeIteration(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses,
									  IDataHolder::MergeData& merged, std::vector<IDataHolder::MergedIdRel>& merged_rd,
									  std::vector<uint16_t>& idoffsets, std::vector<bool>& curExists, const bool hasBeenAnd,
									  const bool inTransaction, const RdxContext& rdxCtx) {
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
		if (m_rd.next.Size()) m_rd.cur = std::move(m_rd.next);
	}

	// loop on subterm (word, translit, stemmmer,...)
	for (auto& r : rawRes) {
		if (!inTransaction) ThrowOnCancel(rdxCtx);
		auto idf = IDF(totalDocsCount, r.vids_->size());
		// cycle through the documents for the given subterm
		for (auto&& relid : *r.vids_) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(relid), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(relid), IdRelType&>),
						  "Expecting relid is movable for packed vector and not movable for simple vector");

			// relid contains all subterm positions in the given document
			const int vid = relid.Id();
			index_t vidStatus = mergeStatuses[vid];
			// Do not calc anithing if
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
			if (!vdocs[vid].keyEntry) continue;

			// Find field with max rank
			double normBm25 = 0.0;
			auto [termRank, field] = calcTermRank(rawRes, idf, relid, r.proc_, normBm25);
			if (!termRank) continue;
			if (holder_.cfg_->logLevel >= LogTrace) {
				logPrintf(LogInfo, "Pattern %s, idf %f, termLenBoost %f", r.pattern, idf, rawRes.term.opts.termLenBoost);
			}
			// match of 2-rd, and next terms (we will get here with the second subterm)
			// non text queries put kExcluded (not 0) in mergeStatuses
			const IdRelType* movedRelId = nullptr;
			if (!simple && vidStatus) {
				assertrx(relid.Size());
				auto& curMerged = merged[idoffsets[vid]];
				auto& curMerged_rd = merged_rd[curMerged.indexAdd];

				assertrx(curMerged_rd.cur.Size());

				// Calculate words distance
				int distance = 0;
				float normDist = 1;
				if (curMerged_rd.qpos != rawRes.term.opts.qpos) {  // do not calculate the distance if it is a subterm of the FIRST term
					distance = curMerged_rd.cur.Distance(relid, INT_MAX);
					// Normalized distance
					normDist = bound(1.0 / double(std::max(distance, 1)), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
				}
				int finalRank = normDist * termRank;
				if (distance <= rawRes.term.opts.distance && (!curExists[vid] || finalRank > curMerged_rd.rank)) {
					// distance and rank is better, than prev. update rank
					if (curExists[vid]) {
						curMerged.proc -= curMerged_rd.rank;
						debugMergeStep("merged better score ", vid, normBm25, normDist, finalRank, curMerged_rd.rank);
					} else {
						debugMergeStep("merged new ", vid, normBm25, normDist, finalRank, curMerged_rd.rank);
						curMerged.matched++;
					}
					curMerged.proc += finalRank;
					if (needArea_) {
						for (auto pos : relid.Pos()) {
							if (!merged.vectorAreas[curMerged.areaIndex].AddWord(pos.pos(), pos.field(), maxAreasInDoc_)) {
								break;
							}
						}
					}
					curMerged_rd.rank = finalRank;
					curMerged_rd.next = std::move(relid);
					movedRelId = &curMerged_rd.next;
					curExists[vid] = true;
				} else {
					debugMergeStep("skiped ", vid, normBm25, normDist, finalRank, curMerged_rd.rank);
				}
			}

			if (int(merged.size()) < holder_.cfg_->mergeLimit && !hasBeenAnd) {
				const bool currentlyAddedLessRankedMerge =
					!curExists.empty() && curExists[vid] && merged[idoffsets[vid]].proc < static_cast<int32_t>(termRank);
				if (!(simple && currentlyAddedLessRankedMerge) && vidStatus) {
					continue;
				}
				// match of 1-st term
				IDataHolder::MergeInfo info;
				info.id = vid;
				info.proc = termRank;
				info.matched = 1;
				info.field = field;

				if (needArea_) {
					merged.vectorAreas.push_back(AreaHolder());
					info.areaIndex = merged.vectorAreas.size() - 1;
					auto& area = merged.vectorAreas.back();
					area.ReserveField(fieldSize_);
					auto& relidRef = movedRelId ? *movedRelId : relid;
					for (auto pos : relidRef.Pos()) {
						area.AddWord(pos.pos(), pos.field(), maxAreasInDoc_);
					}
				}

				if (vidStatus) {
					info.indexAdd = merged[idoffsets[vid]].indexAdd;
					if (!simple) {
						if (movedRelId) {
							merged_rd[info.indexAdd].cur = *movedRelId;
						} else {
							// NOLINTNEXTLINE(bugprone-use-after-move)
							merged_rd[info.indexAdd].cur = std::move(relid);
						}
						merged_rd[info.indexAdd].next = IdRelType();
						merged_rd[info.indexAdd].rank = termRank;
						merged_rd[info.indexAdd].qpos = rawRes.term.opts.qpos;
					}
					merged[idoffsets[vid]] = std::move(info);
				} else {  // add new document
					info.indexAdd = merged.size();
					merged.push_back(std::move(info));
					mergeStatuses[vid] = rawResIndex + 1;
					if (!curExists.empty()) {
						curExists[vid] = true;
						idoffsets[vid] = merged.size() - 1;
					}

					if (!simple) {
						if (movedRelId) {
							// Due to move conditions, this branch is actually never works, so it's just double check
							merged_rd.emplace_back(IdRelType(*movedRelId), int(termRank), rawRes.term.opts.qpos);
						} else {
							// Moving relid only if it was not already moved before (movedRelId must be set right after move)
							merged_rd.emplace_back(IdRelType(std::move(relid)), int(termRank), rawRes.term.opts.qpos);
						}
					}
				}
			}
		}
	}
}
template <typename IdCont>
std::pair<double, int> Selecter<IdCont>::calcTermRank(const TextSearchResults& rawRes, double idf, const IdRelType& relid, int proc,
													  double& normBm25) {
	// Find field with max rank
	int field = 0;
	double termRank = 0.0;
	bool dontSkipCurTermRank = false;
	normBm25 = 0.0;

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
			calcFieldBoost(idf, f, relid, rawRes.term.opts, proc, termRank, normBm25, dontSkipCurTermRank, ranksInFields, field);
		}
	}

	if (!termRank) return std::make_pair(termRank, field);

	if (holder_.cfg_->summationRanksByFieldsRatio > 0) {
		std::sort(ranksInFields.begin(), ranksInFields.end());
		double k = holder_.cfg_->summationRanksByFieldsRatio;
		for (auto rank : ranksInFields) {
			termRank += (k * rank);
			k *= holder_.cfg_->summationRanksByFieldsRatio;
		}
	}
	return std::make_pair(termRank, field);
}

template <typename IdCont>
template <typename P>
void Selecter<IdCont>::mergeIterationGroup(TextSearchResults& rawRes, index_t rawResIndex, FtMergeStatuses::Statuses& mergeStatuses,
										   IDataHolder::MergeData& merged, std::vector<P>& merged_rd, std::vector<uint16_t>& idoffsets,
										   std::vector<bool>& present, const bool firstTerm, const bool inTransaction,
										   const RdxContext& rdxCtx) {
	const auto& vdocs = holder_.vdocs_;

	const size_t totalDocsCount = vdocs.size();

	present.clear();
	present.resize(totalDocsCount, false);

	// loop on subterm (word, translit, stemmmer,...)
	for (auto& r : rawRes) {
		if (!inTransaction) ThrowOnCancel(rdxCtx);
		double idf = IDF(totalDocsCount, r.vids_->size());
		int vid = -1;
		// cycle through the documents for the given subterm
		for (auto&& relid : *r.vids_) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(relid), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(relid), IdRelType&>),
						  "Expecting relid is movable for packed vector and not movable for simple vector");

			// relid contains all subterm positions in the given document
			vid = relid.Id();
			index_t vidStatus = mergeStatuses[vid];
			// Do not calc anithing if
			if ((vidStatus == FtMergeStatuses::kExcluded) | (!firstTerm & (vidStatus == 0))) {
				continue;
			}
			// keyEntry can be assigned nullptr when removed
			if (!vdocs[vid].keyEntry) continue;

			// Find field with max rank
			double normBm25 = 0.0;
			auto [termRank, field] = calcTermRank(rawRes, idf, relid, r.proc_, normBm25);
			if (!termRank) continue;

			if (holder_.cfg_->logLevel >= LogTrace) {
				logPrintf(LogInfo, "Pattern %s, idf %f, termLenBoost %f", r.pattern, idf, rawRes.term.opts.termLenBoost);
			}

			// match of 2-rd, and next terms
			if (!firstTerm) {
				auto& curMerged = merged[idoffsets[vid]];
				auto& curMergedPos = merged_rd[curMerged.indexAdd];

				int minDist = curMergedPos.cur.MergeWithDist(relid, rawRes.term.opts.distance, curMergedPos.posTmp);
				if (!curMergedPos.posTmp.empty()) {
					present[vid] = true;
					double normDist = bound(1.0 / minDist, holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
					int finalRank = normDist * termRank;
					if (finalRank > curMergedPos.rank) {
						curMerged.proc -= curMergedPos.rank;
						curMergedPos.rank = finalRank;
						curMerged.proc += finalRank;
					}
				}
			} else {
				if (vidStatus) {
					if constexpr (isGroupMergeWithAreas<P>()) {
						auto& pos = merged_rd[merged[idoffsets[vid]].indexAdd].posTmp;
						pos.reserve(pos.size() + relid.Size());
						for (const auto& p : relid.Pos()) {
							pos.emplace_back(p, -1);
						}
					} else {
						auto& pos = merged_rd[merged[idoffsets[vid]].indexAdd].posTmp;
						pos.reserve(pos.Size() + relid.Size());

						for (const auto& p : relid.Pos()) {
							pos.Add(p);
						}
					}
				} else if (int(merged.size()) < holder_.cfg_->mergeLimit) {
					IDataHolder::MergeInfo info;
					info.id = vid;
					info.proc = termRank;
					info.matched = 1;
					info.field = field;
					info.indexAdd = merged.size();
					merged.push_back(std::move(info));
					mergeStatuses[vid] = rawResIndex + 1;
					present[vid] = true;
					idoffsets[vid] = merged.size() - 1;
					if constexpr (isGroupMergeWithAreas<P>()) {
						RVector<std::pair<IdRelType::PosType, int>, 4> posTmp;
						posTmp.reserve(relid.Size());
						for (const auto& p : relid.Pos()) {
							posTmp.emplace_back(p, -1);
						}
						merged_rd.emplace_back(IdRelType(std::move(relid)), int(termRank), rawRes.term.opts.qpos, std::move(posTmp));

					} else {
						merged_rd.emplace_back(IdRelType(std::move(relid)), int(termRank), rawRes.term.opts.qpos);
					}
				}
			}
		}
	}
	for (auto& mergedInfo : merged) {
		auto& mergedPosInfo = merged_rd[mergedInfo.indexAdd];

		if (mergedPosInfo.posTmp.empty()) {
			mergedInfo.proc = 0;
			mergeStatuses[mergedInfo.id] = 0;
			mergedPosInfo.cur.Clear();
			mergedPosInfo.next.Clear();
			continue;
		}
		if constexpr (isGroupMerge<P>()) {
			mergedPosInfo.posTmp.SortAndUnique();
			mergedPosInfo.cur = std::move(mergedPosInfo.posTmp);
			mergedPosInfo.next.Clear();
			mergedPosInfo.posTmp.Clear();
		} else {
			auto& posTmp = mergedPosInfo.posTmp;
			boost::sort::pdqsort(
				posTmp.begin(), posTmp.end(),
				[](const std::pair<IdRelType::PosType, int>& l, const std::pair<IdRelType::PosType, int>& r) { return l.first < r.first; });
			auto last = std::unique(posTmp.begin(), posTmp.end());
			posTmp.resize(last - posTmp.begin());

			mergedPosInfo.cur.Clear();
			for (const auto& p : mergedPosInfo.posTmp) {
				mergedPosInfo.cur.Add(p.first);
			}
			mergedPosInfo.wordPosForChain.emplace_back(std::move(mergedPosInfo.posTmp));
			mergedPosInfo.posTmp.clear();
			mergedPosInfo.next.Clear();
		}
	}
}

template <typename IdCont>
template <typename PosType>
void Selecter<IdCont>::mergeResultsPart(std::vector<TextSearchResults>& rawResults, size_t from, size_t to, IDataHolder::MergeData& merged,
										std::vector<PosType>& mergedPos, const bool inTransaction, const RdxContext& rdxCtx) {
	// Current implementation supports OpAnd only
	assertrx(to <= rawResults.size());
	FtMergeStatuses::Statuses mergeStatuses;
	std::vector<uint16_t> idoffsets;

	mergeStatuses.resize(holder_.vdocs_.size(), 0);

	// upper estimate number of documents
	int idsMaxCnt = rawResults[from].idsCnt_;

	merged.reserve(std::min(holder_.cfg_->mergeLimit, idsMaxCnt));

	if (to - from > 1) {
		idoffsets.resize(holder_.vdocs_.size());
	}
	std::vector<bool> exists;
	bool firstTerm = true;
	for (size_t i = from; i < to; ++i) {
		mergeIterationGroup(rawResults[i], i, mergeStatuses, merged, mergedPos, idoffsets, exists, firstTerm, inTransaction, rdxCtx);
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
		if (merged.maxRank < m.proc) {
			merged.maxRank = m.proc;
		}
	}

	boost::sort::pdqsort(merged.begin(), merged.end(),
						 [](const IDataHolder::MergeInfo& lhs, const IDataHolder::MergeInfo& rhs) { return lhs.proc > rhs.proc; });
}

template <typename IdCont>

typename IDataHolder::MergeData Selecter<IdCont>::mergeResults(std::vector<TextSearchResults>&& rawResults, size_t totalORVids,
															   const std::vector<size_t>& synonymsBounds, bool inTransaction,
															   FtMergeStatuses::Statuses&& mergeStatuses, const RdxContext& rdxCtx) {
	const auto& vdocs = holder_.vdocs_;
	IDataHolder::MergeData merged;

	if (!rawResults.size() || !vdocs.size()) return merged;

	assertrx(FtMergeStatuses::kExcluded > rawResults.size());
	assertrx(mergeStatuses.size() == vdocs.size());
	std::vector<IDataHolder::MergedIdRel> merged_rd;

	std::vector<uint16_t> idoffsets;

	for (auto& rawRes : rawResults) {
		boost::sort::pdqsort(rawRes.begin(), rawRes.end(),
							 [](const TextSearchResult& lhs, const TextSearchResult& rhs) { return lhs.proc_ > rhs.proc_; });
	}

	const auto maxMergedSize = std::min(size_t(holder_.cfg_->mergeLimit), totalORVids);
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
			if (needArea_) {
				mergeGroupResult<IDataHolder::MergedIdRelExArea>(rawResults, i, k, mergeStatuses, merged, merged_rd, op, hasBeenAnd,
																 idoffsets, inTransaction, rdxCtx);
			} else {
				mergeGroupResult<IDataHolder::MergedIdRelEx>(rawResults, i, k, mergeStatuses, merged, merged_rd, op, hasBeenAnd, idoffsets,
															 inTransaction, rdxCtx);
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
		mergeIteration(rawResults[i], i, mergeStatuses, merged, merged_rd, idoffsets, exists[curExists], hasBeenAnd, inTransaction, rdxCtx);
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
				if (matchSyn) continue;
				info.proc = 0;
				mergeStatuses[vid] = 0;
			}
		}
	}
	if (holder_.cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Complex merge (%d patterns): out %d vids", rawResults.size(), merged.size());
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

	boost::sort::pdqsort(merged.begin(), merged.end(),
						 [](const IDataHolder::MergeInfo& lhs, const IDataHolder::MergeInfo& rhs) { return lhs.proc > rhs.proc; });
	return merged;
}

template class Selecter<PackedIdRelVec>;
template IDataHolder::MergeData Selecter<PackedIdRelVec>::Process<true>(FtDSLQuery&&, bool, FtMergeStatuses::Statuses&&, const RdxContext&);
template IDataHolder::MergeData Selecter<PackedIdRelVec>::Process<false>(FtDSLQuery&&, bool, FtMergeStatuses::Statuses&&,
																		 const RdxContext&);
template class Selecter<IdRelVec>;
template IDataHolder::MergeData Selecter<IdRelVec>::Process<true>(FtDSLQuery&&, bool, FtMergeStatuses::Statuses&&, const RdxContext&);
template IDataHolder::MergeData Selecter<IdRelVec>::Process<false>(FtDSLQuery&&, bool, FtMergeStatuses::Statuses&&, const RdxContext&);

}  // namespace reindexer
