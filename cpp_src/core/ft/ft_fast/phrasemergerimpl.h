#include <type_traits>
#include <utility>
#include "core/ft/bm25.h"
#include "core/id_type.h"
#include "core/rdxcontext.h"
#include "merger.h"
#include "tools/logger.h"

namespace reindexer {

namespace ft {

constexpr size_t kUseBinarySearchBorder = 8;

// occurrenceScore = queryBoost * subtermProc * fieldBoost * bm25Norm * termLenBoost * positionRank
// bm25Norm = (1 - bm25Weight) + bm25 * bm25Boost * bm25Weight
// See fulltext_ranking.md#score-of-one-subterm-occurrence, fulltext.md#field-selection,
// fulltext.md#basic-document-ranking-algorithms
template <typename Calculator, bool UseBinarySearch, typename OccurenceT, typename DocsStatsGetter>
std::pair<float, uint8_t> calcTermRankImpl(const FtDslOpts& termOpts, Calculator bm25Calc, OccurenceT&& relid, uint32_t vdocId,
										   TermRankInfo& termInf, const FTConfig* cfg, const DocsStatsGetter& docsStatsGetter) {
	assertrx_dbg(vdocId != kEmptyVDocId);

	uint8_t fieldWithMaxRank = 0;

	h_vector<float, 4> ranksInFields;
	bool needToSumWinner = false;

	const bool needSumRanks = cfg->summationRanksByFieldsRatio > 0.0;
	const auto& positions = OccurencePositions(relid);

	for (size_t idx = 0; idx < positions.size();) {
		const unsigned f = positions[idx].field();
		assertrx(f < cfg->fieldsCfg.size());
		const size_t fieldBegin = idx;

		++idx;
		if constexpr (UseBinarySearch) {
			auto nextFieldIt =
				std::lower_bound(positions.cbegin() + idx, positions.cend(), f, [](PosType p, uint32_t f) { return p.field() <= f; });
			idx += (nextFieldIt - (positions.cbegin() + idx));
		} else {
			while (idx < positions.size() && positions[idx].field() == f) {
				++idx;
			}
		}

		// skip field with zero boost
		if (reindexer::fp::IsZero(termOpts.fieldsOpts[f].boost)) {
			continue;
		}

		auto& fldCfg = cfg->fieldsCfg[f];
		const size_t fieldEnd = idx;
		const size_t wordsInField = fieldEnd - fieldBegin;
		const float bm25 = bm25Calc.Get(wordsInField, docsStatsGetter.NumWordsInField(vdocId, f), docsStatsGetter.AvgWordsCount(f));
		const float normBm25Tmp = FTFieldConfig::bound(bm25, fldCfg.bm25Weight, fldCfg.bm25Boost);
		termInf.positionRank = fldCfg.calcPositionRank(positions[fieldBegin].pos());
		termInf.termLenBoost = FTFieldConfig::bound(termOpts.termLenBoost, fldCfg.termLenWeight, fldCfg.termLenBoost);

		// final term rank calculation
		const float termRankTmp = termOpts.fieldsOpts[f].boost * normBm25Tmp * termInf.termLenBoost * termInf.positionRank;

		if (termRankTmp > termInf.termRank) {
			fieldWithMaxRank = f;
			termInf.termRank = termRankTmp;
			termInf.bm25Norm = normBm25Tmp;
			needToSumWinner = termOpts.fieldsOpts[f].needSumRank;
		}

		if (termOpts.fieldsOpts[f].needSumRank) {
			ranksInFields.push_back(termRankTmp);
		}
	}

	if (termInf.termRank > 0.0 && needSumRanks) {
		boost::sort::pdqsort_branchless(ranksInFields.begin(), ranksInFields.end(), [](float a, float b) { return a > b; });
		float k = cfg->summationRanksByFieldsRatio;

		for (size_t i = needToSumWinner ? 1 : 0; i < ranksInFields.size(); ++i) {
			termInf.termRank += (k * ranksInFields[i]);
			k *= cfg->summationRanksByFieldsRatio;
		}
	}

	assertrx_dbg(termOpts.boost >= 0.0 && termInf.proc >= 0.0);
	termInf.termRank = termOpts.boost * termInf.proc * termInf.termRank;
	return {termInf.termRank, fieldWithMaxRank};
}

template <typename Calculator, typename OccurenceT, typename DocsStatsGetter>
std::pair<float, uint8_t> calcTermRankSimple(const FtDslOpts& termOpts, Calculator bm25Calc, const OccurenceT& relid, uint32_t vdocId,
											 TermRankInfo& termInf, const FTConfig* cfg, const DocsStatsGetter& docsStatsGetter) {
	assertrx_dbg(vdocId != kEmptyVDocId);
	assertrx_dbg(relid.IsSimple());

	const PosType hit = relid.PeekSimplePos();
	const unsigned f = hit.field();
	assertrx(f < cfg->fieldsCfg.size());
	assertrx(f < termOpts.fieldsOpts.size());

	if (reindexer::fp::IsZero(termOpts.fieldsOpts[f].boost)) {
		assertrx_dbg(termOpts.boost >= 0.0 && termInf.proc >= 0.0);
		termInf.termRank = 0.0f;
		return {0.0f, 0};
	}

	auto& fldCfg = cfg->fieldsCfg[f];
	const float bm25 = bm25Calc.Get(1, docsStatsGetter.NumWordsInField(vdocId, f), docsStatsGetter.AvgWordsCount(f));
	const float normBm25Tmp = FTFieldConfig::bound(bm25, fldCfg.bm25Weight, fldCfg.bm25Boost);
	termInf.positionRank = fldCfg.calcPositionRank(hit.pos());
	termInf.termLenBoost = FTFieldConfig::bound(termOpts.termLenBoost, fldCfg.termLenWeight, fldCfg.termLenBoost);
	termInf.termRank = termOpts.fieldsOpts[f].boost * normBm25Tmp * termInf.termLenBoost * termInf.positionRank;
	termInf.bm25Norm = normBm25Tmp;

	assertrx_dbg(termOpts.boost >= 0.0 && termInf.proc >= 0.0);
	termInf.termRank = termOpts.boost * termInf.proc * termInf.termRank;
	return {termInf.termRank, uint8_t(f)};
}

template <typename Calculator, typename OccurenceT, typename DocsStatsGetter>
std::pair<float, uint8_t> calcTermRank(const FtDslOpts& termOpts, Calculator bm25Calc, OccurenceT&& relid, uint32_t vdocId,
									   TermRankInfo& termInf, const FTConfig* cfg, const DocsStatsGetter& docsStatsGetter) {
	if (relid.IsSimple()) {
		return calcTermRankSimple(termOpts, bm25Calc, relid, vdocId, termInf, cfg, docsStatsGetter);
	}
	if (OccurenceSize(relid) >= kUseBinarySearchBorder) {
		return calcTermRankImpl<Calculator, true>(termOpts, bm25Calc, std::forward<OccurenceT>(relid), vdocId, termInf, cfg,
												  docsStatsGetter);
	} else {
		return calcTermRankImpl<Calculator, false>(termOpts, bm25Calc, std::forward<OccurenceT>(relid), vdocId, termInf, cfg,
												   docsStatsGetter);
	}
}

template <bool UseBinarySearch, typename OccurenceT>
inline bool checkFieldsRelevanceImpl(OccurenceT&& relid, const FtDslOpts& termOpts) {
	const auto& positions = OccurencePositions(relid);

	for (size_t idx = 0; idx < positions.size();) {
		const unsigned f = positions[idx].field();
		assertrx(f < termOpts.fieldsOpts.size());
		if (!reindexer::fp::IsZero(termOpts.fieldsOpts[f].boost)) {
			return true;
		}

		++idx;
		if constexpr (UseBinarySearch) {
			auto nextFieldIt =
				std::lower_bound(positions.cbegin() + idx, positions.cend(), f, [](PosType p, unsigned f) { return p.field() <= f; });
			idx += (nextFieldIt - (positions.cbegin() + idx));
		} else {
			while (idx < positions.size() && positions[idx].field() == f) {
				++idx;
			}
		}
	}

	return false;
}

template <typename OccurenceT>
inline bool checkFieldsRelevance(OccurenceT&& relid, const FtDslOpts& termOpts) {
	if (relid.IsSimple()) {
		const unsigned f = relid.PeekSimplePos().field();
		assertrx(f < termOpts.fieldsOpts.size());
		return !reindexer::fp::IsZero(termOpts.fieldsOpts[f].boost);
	}
	if (OccurenceSize(relid) >= kUseBinarySearchBorder) {
		return checkFieldsRelevanceImpl<true>(relid, termOpts);
	} else {
		return checkFieldsRelevanceImpl<false>(relid, termOpts);
	}
}

template <bool UseBinarySearch, typename OccurenceT>
inline float maxFieldsBoostImpl(OccurenceT&& relid, const FtDslOpts& termOpts) {
	float res = 0.0;
	const auto& positions = OccurencePositions(relid);

	for (size_t idx = 0; idx < positions.size();) {
		const unsigned f = positions[idx].field();
		assertrx(f < termOpts.fieldsOpts.size());
		res = std::max(res, termOpts.fieldsOpts[f].boost);

		++idx;
		if constexpr (UseBinarySearch) {
			auto nextFieldIt =
				std::lower_bound(positions.cbegin() + idx, positions.cend(), f, [](PosType p, unsigned f) { return p.field() <= f; });
			idx += (nextFieldIt - (positions.cbegin() + idx));
		} else {
			while (idx < positions.size() && positions[idx].field() == f) {
				++idx;
			}
		}
	}

	return res;
}

template <typename OccurenceT>
inline float maxFieldsBoost(OccurenceT&& relid, const FtDslOpts& termOpts) {
	if (relid.IsSimple()) {
		const unsigned f = relid.PeekSimplePos().field();
		assertrx(f < termOpts.fieldsOpts.size());
		return termOpts.fieldsOpts[f].boost;
	}
	if (OccurenceSize(relid) >= kUseBinarySearchBorder) {
		return maxFieldsBoostImpl<true>(relid, termOpts);
	} else {
		return maxFieldsBoostImpl<false>(relid, termOpts);
	}
}

// Phrase term merge: calcTermRank per subterm; for non-first terms apply distance penalty:
// normDist = bound(1/distance, distanceWeight, distanceBoost); finalRank = normDist * termRank.
// See fulltext.md#phrase-search, fulltext_ranking.md#multi-term-and-phrase-queries
template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T, typename DocsStatsGetter>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::mergePhraseTerm(TermResults<IdCont>& term, bool isFirstTerm, unsigned distance,
																		const h_vector<FtDslFieldOpts, 8>& fieldsOpts,
																		const DocsStatsGetter& docsStatsGetter) {
	FtDslOpts termOpts = term.Opts();
	termOpts.fieldsOpts = fieldsOpts;
	// loop on subterm (word, translit, stemmer,...)
	for (SubtermResults<IdCont>& subterm : term) {
		termOpts.termLenBoost = subterm.TermLenBoost();
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		Bm25Calculator<Bm25T> bm25(docsStatsGetter.NumLiveDocs(), subterm.Occurences().size(), cfg_->bm25Config.bm25k1,
								   cfg_->bm25Config.bm25b);

		for (auto&& occurence : subterm.Occurences()) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(occurence), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(occurence), IdRelTypePacked&>),
						  "Expecting occurence is movable for packed vector and not movable for simple vector");

			const index_t vdocId = occurence.VdocId();
			if (vdocId >= totalNumDocs_ || !preselectedDocs_[vdocId]) {
				continue;
			}
			if (docsStatsGetter.IsDeleted(occurence)) {
				continue;
			}

			const MergeOffsetT mdIdx = idoffsets_[vdocId];
			const bool added = mdIdx != kNotInMerge;
			if (!added && (!isFirstTerm || NumDocsMerged() >= maxMergedDocs_)) {
				continue;
			}

			// Find field with max rank
			TermRankInfo termInf;
			termInf.proc = subterm.Proc();
			termInf.pattern = subterm.Pattern();

			auto [termRank, field] = calcTermRank(termOpts, bm25, occurence, vdocId, termInf, cfg_, docsStatsGetter);
			if (reindexer::fp::IsZero(termRank)) {
				continue;
			}

			if (cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subterm.Pattern(), bm25.GetIDF(), termOpts.termLenBoost);
			}

			if (isFirstTerm) {
				if (added) {
					auto& md = GetMergeData(mdIdx);
					auto& mdExt = GetMergeDataExtended(mdIdx);
					if (termRank > mdExt.rank) {
						mdExt.rank = termRank;
						md.proc = termRank;
					}
					auto positions = TakeOccurencePos(occurence);
					mergeDataExtended_[mdIdx].AddPositions(positions, term.Pattern(), termInf);
					continue;
				}

				InfoType info{.id = IdType::FromNumber(vdocId), .proc = termRank, .field = field};
				mergeData_.emplace_back(std::move(info));
				auto positions = TakeOccurencePos(occurence);
				mergeDataExtended_.emplace_back(std::move(positions), termRank, term.Pattern(), termInf);
				idoffsets_[vdocId] = MergeOffsetT(mergeData_.size() - 1);
			} else {
				auto& md = GetMergeData(mdIdx);
				auto& mdExt = GetMergeDataExtended(mdIdx);
				auto positions = TakeOccurencePos(occurence);
				const int minDist = mdExt.MergeWithDist(positions, distance, term.Pattern(), termInf);

				if (mdExt.nextPhrasePositions.empty()) {
					continue;
				}

				const float normDist = FTFieldConfig::bound(1.0 / (minDist < 1 ? 1 : minDist), cfg_->distanceWeight, cfg_->distanceBoost);
				const float finalRank = normDist * termRank;
				//'rank' of the current subTerm is greater than the previous subTerm, update the overall 'rank'
				// and save the rank of the subTerm for possible further updates
				if (finalRank > mdExt.rank) {
					md.proc -= mdExt.rank;
					mdExt.rank = finalRank;
					md.proc += finalRank;
				}
			}
		}
	}

	for (size_t idx = 0; idx < mergeData_.size(); ++idx) {
		auto& md = mergeData_[idx];
		auto& mdExt = mergeDataExtended_[idx];

		if (mdExt.nextPhrasePositions.empty()) {
			preselectedDocs_.reset(md.id.ToNumber());
			md.proc = 0;
			mdExt.lastPhrasePositions.clear();
			mdExt.rank = 0;
			continue;
		}

		mdExt.SwitchPositions();
		mdExt.rank = 0;
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename DocsStatsGetter>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::preselectDocsContainingAllTerms(PhraseResults<IdCont>& phrase,
																						const DocsStatsGetter& docsStatsGetter) {
	for (size_t i = 0; i < phrase.NumTerms(); ++i) {
		if (i > 0) {
			nextTermDocs_.reset();
		}

		for (SubtermResults<IdCont>& subterm : phrase.Term(i)) {
			if (!inTransaction_) {
				ThrowOnCancel(ctx_);
			}

			auto& occurences = subterm.Occurences();
			for (auto&& occurence : occurences) {
				const index_t vdocId = occurence.VdocId();
				if (vdocId >= totalNumDocs_) {
					continue;
				}
				if (docsStatsGetter.IsDeleted(occurence)) {
					continue;
				}

				nextTermDocs_.set(vdocId);
			}
		}

		preselectedDocs_ &= nextTermDocs_;
	}

	if (docsExcluded_.size() != 0) {
		std::ignore = preselectedDocs_.Exclude(docsExcluded_);
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T, typename DocsStatsGetter>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::Merge(PhraseResults<IdCont>& phrase, const DocsStatsGetter& docsStatsGetter) {
	init(phrase);
	preselectDocsContainingAllTerms(phrase, docsStatsGetter);

	for (size_t i = 0; i < phrase.NumTerms(); ++i) {
		bool isFirstTerm = (i == 0);
		mergePhraseTerm<Bm25T>(phrase.Term(i), isFirstTerm, phrase.Distance(), phrase.FieldsOpts(), docsStatsGetter);
	}
}

}  // namespace ft
}  // namespace reindexer
