#include "core/ft/bm25.h"
#include "core/rdxcontext.h"
#include "merger.h"
#include "tools/logger.h"

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

RX_ALWAYS_INLINE double bound(double k, double weight, double boost) noexcept { return (1.0 - weight) + k * boost * weight; }

template <typename IdCont, typename Calculator>
RX_ALWAYS_INLINE void calcFieldBoost(const Calculator& bm25Calc, unsigned long long f, const IdRelType& relid, const FtDslOpts& opts,
									 TermRankInfo& termInf, bool& dontSkipCurTermRank, h_vector<double, 4>& ranksInFields, int& field,
									 const DataHolder<IdCont>& holder) {
	assertrx(f < holder.cfg_->fieldsCfg.size());
	const auto& fldCfg = holder.cfg_->fieldsCfg[f];
	// raw bm25
	const double bm25 = bm25Calc.Get(relid.WordsInField(f), holder.vdocs_[relid.Id()].wordsCount[f], holder.avgWordsCount_[f]);
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

template <typename IdCont, typename Calculator>
std::pair<double, int> calcTermRank(const TextSearchResults<IdCont>& rawRes, Calculator bm25Calc, const IdRelType& relid,
									TermRankInfo& termInf, const DataHolder<IdCont>& holder) {
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
		assertrx(f < rawRes.term.opts.fieldsOpts.size());
		if (rawRes.term.opts.fieldsOpts[f].boost) {
			calcFieldBoost(bm25Calc, f, relid, rawRes.term.opts, termInf, dontSkipCurTermRank, ranksInFields, field, holder);
		}
	}

	if (!termInf.termRank) {
		return std::make_pair(termInf.termRank, field);
	}

	if (holder.cfg_->summationRanksByFieldsRatio > 0) {
		boost::sort::pdqsort_branchless(ranksInFields.begin(), ranksInFields.end());
		double k = holder.cfg_->summationRanksByFieldsRatio;
		for (auto rank : ranksInFields) {
			termInf.termRank += (k * rank);
			k *= holder.cfg_->summationRanksByFieldsRatio;
		}
	}
	return std::make_pair(termInf.termRank, field);
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
void Merger<IdCont, MergeDataType, MergeOffsetT>::mergePhraseResults(std::vector<TextSearchResults<IdCont>>& rawResults, size_t phraseBegin,
																	 size_t phraseEnd, OpType phraseOp, int phraseQPos) {
	PhraseMerger<IdCont, MergeDataType, MergeOffsetT> phraseMerger(holder_, fieldSize_, maxAreasInDoc_, inTransaction_, ctx_);

	phraseMerger.template Merge<Bm25T>(rawResults, phraseBegin, phraseEnd);

	if (phraseOp == OpNot) {
		removeDocs(phraseMerger.GetMergeData());
		return;
	}

	std::vector<bool> phraseDocsMask;
	if (phraseOp == OpAnd) {
		phraseDocsMask.resize(holder_.vdocs_.size(), false);
	}

	for (size_t phraseDocIdx = 0; phraseDocIdx < phraseMerger.NumDocsMerged(); ++phraseDocIdx) {
		const InfoType& phraseDocMergeData = phraseMerger.GetMergeData(phraseDocIdx);
		if (phraseDocMergeData.proc == 0) {
			continue;
		}

		const auto& phraseDocMergeDataExt = phraseMerger.GetMergeDataExtended(phraseDocIdx);
		const index_t phraseDocId = phraseDocMergeData.id;

		if (mergeStatuses_[phraseDocId] == 0 && !hasBeenAnd_ && numDocs() < holder_.cfg_->mergeLimit) {	 // add new
			mergeStatuses_[phraseDocId] = phraseBegin + 1;
			InfoType md{.id = int(phraseDocId), .proc = phraseDocMergeData.proc, .field = phraseDocMergeData.field};

			MergerDocumentData mdExt(phraseDocMergeDataExt.rank, phraseQPos);
			if constexpr (kWithAreas) {
				mergeData_.vectorAreas.emplace_back(phraseDocMergeDataExt.CreateAreas(maxAreasInDoc_));
				md.areaIndex = mergeData_.vectorAreas.size() - 1;
			}

			if (phraseOp == OpAnd) {
				phraseDocsMask[phraseDocId] = true;
			}

			mdExt.lastTermPositions = std::move(phraseDocMergeDataExt.lastPhrasePositions);

			mergeData_.emplace_back(std::move(md));
			mergeDataExtended_.emplace_back(std::move(mdExt));
			idoffsets_[phraseDocId] = mergeData_.size() - 1;
		} else if (mergeStatuses_[phraseDocId] != 0 && mergeStatuses_[phraseDocId] != FtMergeStatuses::kExcluded) {
			auto& md = getMergeData(phraseDocId);
			auto& mdExt = getMergeDataExtended(phraseDocId);

			if (phraseOp == OpAnd) {
				phraseDocsMask[phraseDocId] = true;
			}

			md.proc += phraseDocMergeData.proc;
			mdExt.lastTermPositions = std::move(phraseDocMergeDataExt.lastPhrasePositions);
			mdExt.rank = 0;
			mdExt.qpos = phraseQPos;

			if constexpr (kWithAreas) {
				auto areas = phraseDocMergeDataExt.CreateAreas(maxAreasInDoc_);
				copyAreas(areas, mergeData_.vectorAreas[md.areaIndex], phraseDocMergeDataExt.rank, fieldSize_, maxAreasInDoc_);
			}
		}
	}

	if (phraseOp == OpAnd) {
		removeAllDocsExcept(phraseDocsMask);
	}
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

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
void Merger<IdCont, MergeDataType, MergeOffsetT>::mergeTermResults(TextSearchResults<IdCont>& termRes, index_t termIndex,
																   std::vector<bool>* bitmask) {
	const auto& vdocs = holder_.vdocs_;
	const size_t totalDocsCount = vdocs.size();

	switchToNextWord();

	// loop on subterm (word, translit, stemmer,...)
	for (TextSearchResult<IdCont>& subtermRes : termRes) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		Bm25Calculator<Bm25T> bm25{double(totalDocsCount), double(subtermRes.vids->size()), holder_.cfg_->bm25Config.bm25k1,
								   holder_.cfg_->bm25Config.bm25b};

		auto& subtermDocs = *subtermRes.vids;

		for (auto&& positionsInDoc : subtermDocs) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(positionsInDoc), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(positionsInDoc), IdRelType&>),
						  "Expecting positionsInDoc is movable for packed vector and not movable for simple vector");

			const int docId = positionsInDoc.Id();
			const index_t docMergeStatus = mergeStatuses_[docId];

			if ((docMergeStatus == FtMergeStatuses::kExcluded) || vdocs[docId].IsRemoved()) {
				continue;
			}

			if (docMergeStatus == 0 && (hasBeenAnd_ || numDocs() >= holder_.cfg_->mergeLimit)) {
				continue;
			}

			if (termRes.Op() == OpNot) {
				if (docMergeStatus != 0) {
					getMergeData(docId).proc = 0;
				}
				mergeStatuses_[docId] = FtMergeStatuses::kExcluded;
				continue;
			}

			// Find field with max rank
			TermRankInfo subtermInf;
			subtermInf.proc = subtermRes.proc;
			subtermInf.pattern = subtermRes.pattern;
			auto [rank, field] = calcTermRank(termRes, bm25, positionsInDoc, subtermInf, holder_);
			if (!rank) {
				continue;
			}
			if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subtermRes.pattern, bm25.GetIDF(), termRes.term.opts.termLenBoost);
			}

			if (bitmask != nullptr) {
				(*bitmask)[docId] = true;
			}

			if (docMergeStatus == 0) {
				addDoc(docId, termIndex + 1, int32_t(rank), field, IdRelType(std::move(positionsInDoc)), termRes.QPos(), subtermInf,
					   termRes.term.pattern);
			} else {
				addDocAreas(docId, positionsInDoc, rank, subtermInf, termRes.term.pattern);

				auto& md = getMergeData(docId);
				auto& mdExt = getMergeDataExtended(docId);

				const int distance =
					mdExt.lastTermPositions.empty() ? 0 : PositionsDistance(mdExt.lastTermPositions, positionsInDoc.Pos(), INT_MAX);
				const float normDist =
					bound(1.0 / double(std::max(distance, 1)), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
				const int finalRank = normDist * rank;

				if (finalRank > mdExt.rank) {
					md.proc -= mdExt.rank;
					md.proc += finalRank;
					mdExt.nextTermPositions = std::move(positionsInDoc.Pos());
					mdExt.rank = finalRank;
				}
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
MergeDataType Merger<IdCont, MergeDataType, MergeOffsetT>::mergeSimple(TextSearchResults<IdCont>& singleTermRes,
																	   RankSortType rankSortType) {
	const auto& vdocs = holder_.vdocs_;
	const size_t totalDocsCount = vdocs.size();

	// loop on subterm (word, translit, stemmer,...)
	for (auto& subtermRes : singleTermRes) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}
		Bm25Calculator<Bm25T> bm25{double(totalDocsCount), double(subtermRes.vids->size()), holder_.cfg_->bm25Config.bm25k1,
								   holder_.cfg_->bm25Config.bm25b};

		const auto& subtermDocs = *subtermRes.vids;

		for (const IdRelType& positionsInDoc : subtermDocs) {
			const int docId = positionsInDoc.Id();
			const index_t docMergeStatus = mergeStatuses_[docId];

			if ((docMergeStatus == FtMergeStatuses::kExcluded) || vdocs[docId].IsRemoved()) {
				continue;
			}

			if (docMergeStatus == 0 && numDocs() >= holder_.cfg_->mergeLimit) {
				continue;
			}

			// Find field with max rank
			TermRankInfo subtermInf;
			subtermInf.proc = subtermRes.proc;
			subtermInf.pattern = subtermRes.pattern;
			auto [rank, field] = calcTermRank(singleTermRes, bm25, positionsInDoc, subtermInf, holder_);
			if (!rank) {
				continue;
			}

			if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subtermRes.pattern, bm25.GetIDF(),
					   singleTermRes.term.opts.termLenBoost);
			}

			if (docMergeStatus == 0) {
				// only 1 term in query
				const index_t simpleTermMergeStatus = 1;
				addDoc(docId, simpleTermMergeStatus, static_cast<int32_t>(rank), field);
				addLastDocAreas(positionsInDoc, rank, subtermInf, singleTermRes.term.pattern);
			} else {
				auto& md = getMergeData(docId);
				if (md.proc < static_cast<int32_t>(rank)) {
					md.proc = rank;
					md.field = field;
				}

				addDocAreas(docId, positionsInDoc, rank, subtermInf, singleTermRes.term.pattern);
			}
		}
	}

	addFullMatchBoost(1);
	sortResults(rankSortType);

	return std::move(mergeData_);
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
MergeDataType Merger<IdCont, MergeDataType, MergeOffsetT>::Merge(std::vector<TextSearchResults<IdCont>>& rawResults, size_t totalORVids,
																 const std::vector<size_t>& synonymsBounds, RankSortType rankSortType) {
	static_assert(sizeof(Bm25Calculator<Bm25T>) <= 32, "Bm25Calculator<Bm25T> size is greater than 32 bytes");

	const auto& vdocs = holder_.vdocs_;
	assertrx_throw(rawResults.size() < FtMergeStatuses::kExcluded);
	if (!rawResults.size() || !vdocs.size()) {
		return std::move(mergeData_);
	}

	const auto maxMergedSize = std::min(size_t(holder_.cfg_->mergeLimit), totalORVids);
	init(rawResults, vdocs.size(), maxMergedSize);

	for (auto& res : rawResults) {
		boost::sort::pdqsort_branchless(
			res.begin(), res.end(),
			[](const TextSearchResult<IdCont>& l, const TextSearchResult<IdCont>& r) noexcept { return l.proc > r.proc; });
	}

	if (rawResults.size() == 1) {
		if (rawResults[0].Op() != OpNot) {
			return mergeSimple<Bm25T>(rawResults[0], rankSortType);
		}
		// empty result
		return std::move(mergeData_);
	}

	const size_t synonymsGroupsEnd = synonymsBounds.empty() ? 0 : synonymsBounds.back();
	std::vector<std::vector<bool>> synonymGroupsBitmasks(synonymsBounds.size());

	for (size_t synGrpIdx = 0; synGrpIdx < synonymsBounds.size(); ++synGrpIdx) {
		hasBeenAnd_ = false;
		auto& bitmask = synonymGroupsBitmasks[synGrpIdx];
		const size_t synGroupBegin = synGrpIdx > 0 ? synonymsBounds[synGrpIdx - 1] : 0;
		const size_t synGroupEnd = synonymsBounds[synGrpIdx];

		for (index_t termIdx = synGroupBegin; termIdx < synGroupEnd; ++termIdx) {
			const auto termOp = rawResults[termIdx].Op();
			bitmask.resize(0);
			bitmask.resize(vdocs.size(), false);
			mergeTermResults<Bm25T>(rawResults[termIdx], termIdx, &bitmask);
			if (termOp == OpAnd) {
				hasBeenAnd_ = true;
				for (auto& md : mergeData_) {
					if (bitmask[md.id] || md.proc == 0 || mergeStatuses_[md.id] == FtMergeStatuses::kExcluded ||
						mergeStatuses_[md.id] <= synGroupBegin) {
						continue;
					}

					md.proc = 0;
					mergeStatuses_[md.id] = 0;
				}
			}
		}
	}

	std::vector<bool> bitmask;
	hasBeenAnd_ = false;
	for (index_t termIdx = synonymsGroupsEnd; termIdx < rawResults.size(); ++termIdx) {
		if (rawResults[termIdx].GroupNum() != -1) {
			const OpType phraseOp = rawResults[termIdx].Op();
			const int groupNum = rawResults[termIdx].GroupNum();
			const size_t phraseBegin = termIdx;
			size_t phraseEnd = termIdx;

			while (phraseEnd < rawResults.size() && rawResults[phraseEnd].GroupNum() == groupNum) {
				rawResults[phraseEnd].Op() = OpAnd;
				phraseEnd++;
			}

			mergePhraseResults<Bm25T>(rawResults, phraseBegin, phraseEnd, phraseOp, rawResults[phraseBegin].QPos());

			if (phraseOp == OpAnd) {
				hasBeenAnd_ = true;
			}
			termIdx = phraseEnd - 1;
			continue;
		}

		const auto termOp = rawResults[termIdx].Op();
		if (termOp == OpAnd) {
			bitmask.resize(0);
			bitmask.resize(vdocs.size(), false);
		}

		mergeTermResults<Bm25T>(rawResults[termIdx], termIdx, termOp == OpAnd ? &bitmask : nullptr);

		if (termOp == OpAnd) {
			hasBeenAnd_ = true;
			for (auto& md : mergeData_) {
				const auto docId = md.id;
				if (bitmask[docId] || md.proc == 0 || mergeStatuses_[docId] == FtMergeStatuses::kExcluded) {
					continue;
				}

				bool matchSyn = false;
				for (size_t synGrpIdx : rawResults[termIdx].synonymsGroups) {
					assertrx(synGrpIdx < synonymsBounds.size());
					if (synonymGroupsBitmasks[synGrpIdx][docId]) {
						matchSyn = true;
						break;
					}
				}

				if (!matchSyn) {
					md.proc = 0;
					mergeStatuses_[docId] = FtMergeStatuses::kExcluded;
				}
			}
		}
	}

	if rx_unlikely (holder_.cfg_->logLevel >= LogInfo) {
		logFmt(LogInfo, "Complex merge ({} patterns): out {} vids", rawResults.size(), mergeData_.size());
	}

	addFullMatchBoost(rawResults.size());
	sortResults(rankSortType);

	return std::move(mergeData_);
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::mergePhraseTerm(TextSearchResults<IdCont>& termRes, index_t termIdx,
																		bool firstTerm) {
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
			const index_t docMergeStatus = mergeStatuses_[docId];

			if ((docMergeStatus == FtMergeStatuses::kExcluded) || vdocs[docId].IsRemoved()) {
				continue;
			}

			if (docMergeStatus == 0 && (!firstTerm || NumDocsMerged() >= holder_.cfg_->mergeLimit)) {
				continue;
			}

			// Find field with max rank
			TermRankInfo termInf;
			termInf.proc = subtermRes.proc;
			termInf.pattern = subtermRes.pattern;
			auto [termRank, field] = calcTermRank(termRes, bm25, positionsInDoc, termInf, holder_);
			if (!termRank) {
				continue;
			}

			if rx_unlikely (holder_.cfg_->logLevel >= LogTrace) {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subtermRes.pattern, bm25.GetIDF(), termRes.term.opts.termLenBoost);
			}

			if (firstTerm) {
				if (docMergeStatus > 0) {
					mergeDataExtended_[idoffsets_[docId]].AddPositions(positionsInDoc, termRes.term.pattern, termInf);
					continue;
				}

				InfoType info{.id = int(docId), .proc = int32_t(termRank), .field = int8_t(field)};
				mergeData_.emplace_back(std::move(info));
				mergeDataExtended_.emplace_back(IdRelType(std::move(positionsInDoc)), int(termRank), termRes.term.pattern, termInf);
				mergeStatuses_[docId] = termIdx + 1;
				idoffsets_[docId] = mergeData_.size() - 1;
			} else {
				auto& md = GetMergeData(idoffsets_[docId]);
				auto& mdExt = GetMergeDataExtended(idoffsets_[docId]);
				const int minDist = mdExt.MergeWithDist(positionsInDoc, termRes.Distance(), termRes.term.pattern, termInf);

				if (mdExt.nextPhrasePositions.empty()) {
					continue;
				}

				const double normDist = bound(1.0 / (minDist < 1 ? 1 : minDist), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
				const int finalRank = normDist * termRank;
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
			md.proc = 0;
			mergeStatuses_[md.id] = FtMergeStatuses::kExcluded;
			mdExt.lastPhrasePositions.clear();
			mdExt.rank = 0;
			continue;
		}

		mdExt.SwitchPositions();
		mdExt.rank = 0;
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
void PhraseMerger<IdCont, MergeDataType, MergeOffsetT>::Merge(std::vector<TextSearchResults<IdCont>>& rawResults, size_t from, size_t to) {
	static_assert(sizeof(Bm25Calculator<Bm25T>) <= 32, "Bm25Calculator<Bm25T> size is greater than 32 bytes");

	init(rawResults, from, to);

	for (size_t i = from; i < to; ++i) {
		bool firstTerm = (i == from);
		mergePhraseTerm<Bm25T>(rawResults[i], i, firstTerm);
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