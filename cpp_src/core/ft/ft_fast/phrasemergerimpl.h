#include "core/ft/bm25.h"
#include "core/id_type.h"
#include "core/rdxcontext.h"
#include "merger.h"
#include "tools/logger.h"

namespace reindexer {

namespace ft {

constexpr size_t kUseBinarySearchBorder = 8;

template <typename Calculator, bool UseBinarySearch, typename DocsStatsGetter>
std::pair<float, uint8_t> calcTermRankImpl(const FtDslOpts& termOpts, Calculator bm25Calc, const IdRelType& relid, TermRankInfo& termInf,
										   const FTConfig* cfg, const DocsStatsGetter& docsStatsGetter) {
	uint8_t fieldWithMaxRank = 0;

	h_vector<float, 4> ranksInFields;
	bool needToSumWinner = false;

	const bool needSumRanks = cfg->summationRanksByFieldsRatio > 0.0;
	const auto& positions = relid.Pos();

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
		const float bm25 = bm25Calc.Get(wordsInField, docsStatsGetter.NumWordsInField(relid.Id(), f), docsStatsGetter.AvgWordsCount(f));
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

template <typename Calculator, typename DocsStatsGetter>
std::pair<float, uint8_t> calcTermRank(const FtDslOpts& termOpts, Calculator bm25Calc, const IdRelType& relid, TermRankInfo& termInf,
									   const FTConfig* cfg, const DocsStatsGetter& docsStatsGetter) {
	if (relid.Pos().size() >= kUseBinarySearchBorder) {
		return calcTermRankImpl<Calculator, true>(termOpts, bm25Calc, relid, termInf, cfg, docsStatsGetter);
	} else {
		return calcTermRankImpl<Calculator, false>(termOpts, bm25Calc, relid, termInf, cfg, docsStatsGetter);
	}
}

template <bool UseBinarySearch>
inline bool checkFieldsRelevanceImpl(const IdRelType& relid, const FtDslOpts& termOpts) {
	const auto& positions = relid.Pos();

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

inline bool checkFieldsRelevance(const IdRelType& relid, const FtDslOpts& termOpts) {
	if (relid.Pos().size() >= kUseBinarySearchBorder) {
		return checkFieldsRelevanceImpl<true>(relid, termOpts);
	} else {
		return checkFieldsRelevanceImpl<false>(relid, termOpts);
	}
}

template <bool UseBinarySearch>
inline float maxFieldsBoostImpl(const IdRelType& relid, const FtDslOpts& termOpts) {
	float res = 0.0;
	const auto& positions = relid.Pos();

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

inline float maxFieldsBoost(const IdRelType& relid, const FtDslOpts& termOpts) {
	if (relid.Pos().size() >= kUseBinarySearchBorder) {
		return maxFieldsBoostImpl<true>(relid, termOpts);
	} else {
		return maxFieldsBoostImpl<false>(relid, termOpts);
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T, typename DocsStatsGetter>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::mergePhraseTerm(TermResults<IdCont>& term, bool isFirstTerm,
																		const DocsStatsGetter& docsStatsGetter) {
	// loop on subterm (word, translit, stemmer,...)
	for (SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		// first doc is always empty, so number of unempty docs always equal to totalNumDocs_ - 1
		Bm25Calculator<Bm25T> bm25(totalNumDocs_ - 1, subterm.Occurences().size(), cfg_->bm25Config.bm25k1, cfg_->bm25Config.bm25b);

		for (auto&& occurence : subterm.Occurences()) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(occurence), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(occurence), IdRelType&>),
						  "Expecting occurence is movable for packed vector and not movable for simple vector");

			const index_t docId = occurence.Id();
			if (docId > maxDocId_) {
				break;
			}

			if (!preselectedDocs_[docId]) {
				continue;
			}

			if (idoffsets_[docId] == maxMergedDocs_ && (!isFirstTerm || NumDocsMerged() >= maxMergedDocs_)) {
				continue;
			}

			// Find field with max rank
			TermRankInfo termInf;
			termInf.proc = subterm.Proc();
			termInf.pattern = subterm.Pattern();

			auto [termRank, field] = calcTermRank(term.Opts(), bm25, occurence, termInf, cfg_, docsStatsGetter);
			if (reindexer::fp::IsZero(termRank)) {
				continue;
			}

			if (cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subterm.Pattern(), bm25.GetIDF(), term.Opts().termLenBoost);
			}

			if (isFirstTerm) {
				if (idoffsets_[docId] < maxMergedDocs_) {
					auto& md = GetMergeData(idoffsets_[docId]);
					auto& mdExt = GetMergeDataExtended(idoffsets_[docId]);
					if (termRank > mdExt.rank) {
						mdExt.rank = termRank;
						md.proc = termRank;
					}
					mergeDataExtended_[idoffsets_[docId]].AddPositions(occurence.Pos(), term.Pattern(), termInf);
					continue;
				}

				InfoType info{.id = IdType::FromNumber(docId), .proc = termRank, .field = field};
				mergeData_.emplace_back(std::move(info));
				PositionsVector positions;
				InitFrom(std::move(occurence.Pos()), positions);
				mergeDataExtended_.emplace_back(std::move(positions), termRank, term.Pattern(), termInf);
				idoffsets_[docId] = mergeData_.size() - 1;
			} else {
				auto& md = GetMergeData(idoffsets_[docId]);
				auto& mdExt = GetMergeDataExtended(idoffsets_[docId]);
				const int minDist = mdExt.MergeWithDist(occurence.Pos(), term.Distance(), term.Pattern(), termInf);

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

		index_t maxTermDocId = 0;

		for (SubtermResults<IdCont>& subterm : phrase.Term(i)) {
			if (!inTransaction_) {
				ThrowOnCancel(ctx_);
			}

			auto& occurences = subterm.Occurences();
			for (auto&& occurence : occurences) {
				const index_t docId = occurence.Id();
				if (docId > maxDocId_) {
					break;
				}

				maxTermDocId = std::max(maxTermDocId, docId);
				// check removed only on final stage
				if (i + 1 == phrase.NumTerms() && preselectedDocs_[docId] && docsStatsGetter.DocRemoved(docId)) {
					continue;
				}

				nextTermDocs_.set(docId);
			}
		}

		maxDocId_ = std::min(maxDocId_, maxTermDocId);

		preselectedDocs_ &= nextTermDocs_;
	}

	std::ignore = preselectedDocs_.Exclude(docsExcluded_);
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T, typename DocsStatsGetter>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::Merge(PhraseResults<IdCont>& phrase, const DocsStatsGetter& docsStatsGetter) {
	init(phrase);
	preselectDocsContainingAllTerms(phrase, docsStatsGetter);

	for (size_t i = 0; i < phrase.NumTerms(); ++i) {
		bool isFirstTerm = (i == 0);
		mergePhraseTerm<Bm25T>(phrase.Term(i), isFirstTerm, docsStatsGetter);
	}
}

}  // namespace ft
}  // namespace reindexer
