#include "core/ft/bm25.h"
#include "core/rdxcontext.h"
#include "merger.h"
#include "tools/logger.h"

namespace reindexer {

constexpr size_t kUseBinarySearchBorder = 8;

template <typename IdCont, typename Calculator, bool UseBinarySearch>
std::pair<float, uint8_t> calcTermRankImpl(const TextSearchResults<IdCont>& rawRes, Calculator bm25Calc, const IdRelType& relid,
										   TermRankInfo& termInf, const DataHolder<IdCont>& holder) {
	// Find field with max rank
	const auto& opts = rawRes.term.opts;
	uint8_t field = 0;

	h_vector<float, 4> ranksInFields;
	bool needToSumWinner = false;

	const bool needSumRanks = holder.cfg_->summationRanksByFieldsRatio > 0.0;
	const auto& positions = relid.Pos();

	for (size_t idx = 0; idx < positions.size();) {
		const unsigned f = positions[idx].field();
		assertrx(f < holder.cfg_->fieldsCfg.size());
		const size_t fieldBegin = idx;

		++idx;
		if constexpr (UseBinarySearch) {
			auto nextFieldIt =
				std::lower_bound(positions.cbegin() + idx, positions.cend(), field, [](PosType p, uint32_t f) { return p.field() <= f; });
			idx += (nextFieldIt - (positions.cbegin() + idx));
		} else {
			while (idx < positions.size() && positions[idx].field() == f) {
				++idx;
			}
		}

		// skip field with zero boost
		if (reindexer::fp::IsZero(rawRes.term.opts.fieldsOpts[f].boost)) {
			continue;
		}

		auto& fldCfg = holder.cfg_->fieldsCfg[f];
		const size_t fieldEnd = idx;
		const size_t wordsInField = fieldEnd - fieldBegin;
		const float bm25 = bm25Calc.Get(wordsInField, holder.vdocs_[relid.Id()].wordsCount[f], holder.avgWordsCount_[f]);
		const float normBm25Tmp = FtFastFieldConfig::bound(bm25, fldCfg.bm25Weight, fldCfg.bm25Boost);
		termInf.positionRank = fldCfg.calcPositionRank(positions[fieldBegin].pos());
		termInf.termLenBoost = FtFastFieldConfig::bound(opts.termLenBoost, fldCfg.termLenWeight, fldCfg.termLenBoost);

		// final term rank calculation
		const float termRankTmp = opts.fieldsOpts[f].boost * normBm25Tmp * termInf.termLenBoost * termInf.positionRank;

		if (termRankTmp > termInf.termRank) {
			field = f;
			termInf.termRank = termRankTmp;
			termInf.bm25Norm = normBm25Tmp;
			needToSumWinner = opts.fieldsOpts[f].needSumRank;
		}

		if (opts.fieldsOpts[f].needSumRank) {
			ranksInFields.push_back(termRankTmp);
		}
	}

	if (termInf.termRank > 0.0 && needSumRanks) {
		boost::sort::pdqsort_branchless(ranksInFields.begin(), ranksInFields.end(), [](float a, float b) { return a > b; });
		float k = holder.cfg_->summationRanksByFieldsRatio;

		for (size_t i = needToSumWinner ? 1 : 0; i < ranksInFields.size(); ++i) {
			termInf.termRank += (k * ranksInFields[i]);
			k *= holder.cfg_->summationRanksByFieldsRatio;
		}
	}

	assertrx_dbg(opts.boost >= 0.0 && termInf.proc >= 0.0);
	termInf.termRank = opts.boost * termInf.proc * termInf.termRank;
	return std::make_pair(termInf.termRank, field);
}

template <typename IdCont, typename Calculator>
std::pair<float, uint8_t> calcTermRank(const TextSearchResults<IdCont>& rawRes, Calculator bm25Calc, const IdRelType& relid,
									   TermRankInfo& termInf, const DataHolder<IdCont>& holder) {
	if (relid.Pos().size() >= kUseBinarySearchBorder) {
		return calcTermRankImpl<IdCont, Calculator, true>(rawRes, bm25Calc, relid, termInf, holder);
	} else {
		return calcTermRankImpl<IdCont, Calculator, false>(rawRes, bm25Calc, relid, termInf, holder);
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
template <typename Bm25T>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::mergePhraseTerm(TextSearchResults<IdCont>& termRes, bool firstTerm) {
	const auto& vdocs = holder_.vdocs_;
	const size_t totalDocsCount = vdocs.size();

	// loop on subterm (word, translit, stemmer,...)
	for (TextSearchResult<IdCont>& subtermRes : termRes) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}
		Bm25Calculator<Bm25T> bm25(totalDocsCount, subtermRes.vids->size(), holder_.cfg_->bm25Config.bm25k1,
								   holder_.cfg_->bm25Config.bm25b);

		auto& subtermDocs = *subtermRes.vids;
		for (auto&& positionsInDoc : subtermDocs) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(positionsInDoc), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(positionsInDoc), IdRelType&>),
						  "Expecting positionsInDoc is movable for packed vector and not movable for simple vector");

			const index_t docId = positionsInDoc.Id();
			if (docId > maxDocId_) {
				break;
			}

			if (!preselectedDocs_[docId]) {
				continue;
			}

			if (idoffsets_[docId] == maxMergedDocs_ && (!firstTerm || NumDocsMerged() >= holder_.cfg_->mergeLimit)) {
				continue;
			}

			// Find field with max rank
			TermRankInfo termInf;
			termInf.proc = subtermRes.proc;
			termInf.pattern = subtermRes.pattern;

			auto [termRank, field] = calcTermRank(termRes, bm25, positionsInDoc, termInf, holder_);
			if (reindexer::fp::IsZero(termRank)) {
				continue;
			}

			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subtermRes.pattern, bm25.GetIDF(), termRes.term.opts.termLenBoost);
			}

			if (firstTerm) {
				if (idoffsets_[docId] < maxMergedDocs_) {
					auto& md = GetMergeData(idoffsets_[docId]);
					auto& mdExt = GetMergeDataExtended(idoffsets_[docId]);
					if (termRank > mdExt.rank) {
						mdExt.rank = termRank;
						md.proc = termRank;
					}
					mergeDataExtended_[idoffsets_[docId]].AddPositions(positionsInDoc, termRes.term.pattern, termInf);
					continue;
				}

				InfoType info{.id = int(docId), .proc = termRank, .field = field};
				mergeData_.emplace_back(std::move(info));
				mergeDataExtended_.emplace_back(IdRelType(std::move(positionsInDoc)), termRank, termRes.term.pattern, termInf);
				idoffsets_[docId] = mergeData_.size() - 1;
			} else {
				auto& md = GetMergeData(idoffsets_[docId]);
				auto& mdExt = GetMergeDataExtended(idoffsets_[docId]);
				const int minDist = mdExt.MergeWithDist(positionsInDoc, termRes.Distance(), termRes.term.pattern, termInf);

				if (mdExt.nextPhrasePositions.empty()) {
					continue;
				}

				const float normDist =
					FtFastFieldConfig::bound(1.0 / (minDist < 1 ? 1 : minDist), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
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
			preselectedDocs_.reset(md.id);
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
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::preselectDocsContainingAllTerms(std::vector<TextSearchResults<IdCont>>& rawResults,
																						size_t from, size_t to) {
	const auto& vdocs = holder_.vdocs_;

	for (size_t i = from; i < to; ++i) {
		if (i > from) {
			nextTermDocs_.reset();
		}

		index_t maxTermDocId = 0;

		for (TextSearchResult<IdCont>& subtermRes : rawResults[i]) {
			if (!inTransaction_) {
				ThrowOnCancel(ctx_);
			}

			auto& subtermDocs = *subtermRes.vids;
			for (auto&& positionsInDoc : subtermDocs) {
				static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(positionsInDoc), const IdRelType&>) ||
								  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(positionsInDoc), IdRelType&>),
							  "Expecting positionsInDoc is movable for packed vector and not movable for simple vector");

				const index_t docId = positionsInDoc.Id();
				if (docId > maxDocId_) {
					break;
				}

				maxTermDocId = std::max(maxTermDocId, docId);
				// check removed only on final stage
				if (i + 1 == to && preselectedDocs_[docId] && vdocs[docId].IsRemoved()) {
					continue;
				}

				nextTermDocs_.set(docId);
			}
		}

		maxDocId_ = std::min(maxDocId_, maxTermDocId);

		preselectedDocs_ &= nextTermDocs_;
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::Merge(std::vector<TextSearchResults<IdCont>>& rawResults, size_t from, size_t to) {
	static_assert(sizeof(Bm25Calculator<Bm25T>) <= 32, "Bm25Calculator<Bm25T> size is greater than 32 bytes");

	init(rawResults, from, to);

	preselectDocsContainingAllTerms(rawResults, from, to);
	for (size_t i = from; i < to; ++i) {
		bool firstTerm = (i == from);
		mergePhraseTerm<Bm25T>(rawResults[i], firstTerm);
	}

	// looks like that doesnt work correctly
	// Update full match rank
	for (auto& md : mergeData_) {
		if (size_t(holder_.vdocs_[md.id].wordsCount[md.field]) == rawResults.size()) {
			md.proc *= holder_.cfg_->fullMatchBoost;
		}
	}
}

}  // namespace reindexer