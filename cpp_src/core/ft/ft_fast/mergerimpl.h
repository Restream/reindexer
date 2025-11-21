#include "core/ft/bm25.h"
#include "core/rdxcontext.h"
#include "merger.h"
#include "phrasemergerimpl.h"
#include "tools/logger.h"

namespace reindexer {

template <typename AreaType>
void copyAreas(AreasInDocument<AreaType>& from, AreasInDocument<AreaType>& to, float rank, size_t fieldSize, int maxAreasInDoc) {
	for (size_t f = 0; f < fieldSize; f++) {
		auto areas = from.GetAreas(f);
		if (areas) {
			areas->MoveAreas(to, f, rank, std::is_same_v<AreaType, AreaDebug> ? -1 : maxAreasInDoc);
		}
	}
}

RX_ALWAYS_INLINE unsigned PositionsDistance(const h_vector<PosType, 3>& positions, const h_vector<PosType, 3>& otherPositions) {
	unsigned res = std::numeric_limits<unsigned>::max();
	for (auto it1 = positions.begin(), it2 = otherPositions.begin(); it1 != positions.end() && it2 != otherPositions.end();) {
		bool sign = it1->fullPos() > it2->fullPos();
		if (it1->fullField() == it2->fullField()) {
			unsigned dst = sign ? it1->fullPos() - it2->fullPos() : it2->fullPos() - it1->fullPos();
			if (dst < res) {
				res = dst;
				if (res <= 1) {
					break;
				}
			}
		}

		(sign) ? it2++ : it1++;
	}
	return res;
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
void Merger<IdCont, MergeDataType, MergeOffsetT>::mergePhraseResults(size_t phraseIdx, size_t phraseBegin, OpType phraseOp,
																	 int phraseQPos) {
	auto& phraseMerger = phraseMergers_[phraseIdx];

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
		if (reindexer::fp::IsZero(phraseDocMergeData.proc)) {
			continue;
		}

		const index_t phraseDocId = phraseDocMergeData.id;

		if (restrictingMask_.size() && !restrictingMask_[phraseDocId]) {
			continue;
		}

		const auto& phraseDocMergeDataExt = phraseMerger.GetMergeDataExtended(phraseDocIdx);

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

	if (phraseOp == OpAnd && !restrictingMask_.size()) {
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

			if (restrictingMask_.size()) {
				if (!restrictingMask_[docId]) {
					continue;
				}

				if (docMergeStatus == 0 && hasBeenAnd_) {
					continue;
				}
			} else {
				if ((docMergeStatus == FtMergeStatuses::kExcluded) || vdocs[docId].IsRemoved()) {
					continue;
				}

				if (docMergeStatus == 0 && (hasBeenAnd_ || numDocs() >= holder_.cfg_->mergeLimit)) {
					continue;
				}
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
			if (fp::IsZero(rank)) {
				continue;
			}
			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subtermRes.pattern, bm25.GetIDF(), termRes.term.opts.termLenBoost);
			}

			if (bitmask != nullptr) {
				(*bitmask)[docId] = true;
			}

			if (docMergeStatus == 0) {
				addDoc(docId, termIndex + 1, rank, field, IdRelType(std::move(positionsInDoc)), termRes.QPos(), subtermInf,
					   termRes.term.pattern);
			} else {
				addDocAreas(docId, positionsInDoc, rank, subtermInf, termRes.term.pattern);

				auto& md = getMergeData(docId);
				auto& mdExt = getMergeDataExtended(docId);

				const unsigned distance =
					mdExt.lastTermPositions.empty() ? 0 : PositionsDistance(mdExt.lastTermPositions, positionsInDoc.Pos());
				const float normDist = FtFastFieldConfig::bound(1.0 / float(std::max(distance, 1U)), holder_.cfg_->distanceWeight,
																holder_.cfg_->distanceBoost);
				const float finalRank = normDist * rank;

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
			if (fp::IsZero(rank)) {
				continue;
			}

			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subtermRes.pattern, bm25.GetIDF(),
					   singleTermRes.term.opts.termLenBoost);
			}

			if (docMergeStatus == 0) {
				// only 1 term in query
				const index_t simpleTermMergeStatus = 1;
				addDoc(docId, simpleTermMergeStatus, rank, field);
				addLastDocAreas(positionsInDoc, rank, subtermInf, singleTermRes.term.pattern);
			} else {
				auto& md = getMergeData(docId);
				if (md.proc < rank) {
					md.proc = rank;
					md.field = field;
				}

				addDocAreas(docId, positionsInDoc, rank, subtermInf, singleTermRes.term.pattern);
			}
		}
	}

	addFullMatchBoost(1);
	postProcessResults(rankSortType);

	return std::move(mergeData_);
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::calcTermBitmask(const TextSearchResults<IdCont>& termRes, BitsetType& termMask) {
	const auto& vdocs = holder_.vdocs_;
	termMask.resize(0);
	termMask.resize(vdocs.size(), false);

	bool allFieldsHavePositiveBoost = std::ranges::all_of(termRes.term.opts.fieldsOpts, [](const auto& opts) { return opts.boost; });

	// loop on subterm (word, translit, stemmer,...)
	for (const TextSearchResult<IdCont>& subtermRes : termRes) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (const auto& positionsInDoc : *subtermRes.vids) {
			if (allFieldsHavePositiveBoost || checkFieldsRelevance(positionsInDoc, termRes.term.opts)) {
				index_t docId = positionsInDoc.Id();
				termMask.set(docId);
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::calcTermScores(TextSearchResults<IdCont>& termRes, const BitsetType* restrictingMask,
																 BitsetType& termMask, std::vector<uint16_t>& docsScores) {
	const auto& vdocs = holder_.vdocs_;
	termMask.resize(0);
	termMask.resize(vdocs.size(), false);

	const float fieldsBoost = termRes.term.opts.fieldsOpts[0].boost;
	bool allFieldsHaveSameBoost = std::ranges::all_of(
		termRes.term.opts.fieldsOpts, [fieldsBoost](const auto& opts) { return reindexer::fp::ExactlyEqual(opts.boost, fieldsBoost); });

	// loop on subterm (word, translit, stemmer,...)
	for (TextSearchResult<IdCont>& subtermRes : termRes) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (const auto& positionsInDoc : *subtermRes.vids) {
			const float maxBoostFromFields = allFieldsHaveSameBoost ? fieldsBoost : maxFieldsBoost(positionsInDoc, termRes.term.opts);
			if (maxBoostFromFields > 0.0) {
				index_t docId = positionsInDoc.Id();
				if (restrictingMask != nullptr && !(*restrictingMask)[docId]) {
					continue;
				}

				if (!termMask[docId]) {
					float proc = subtermRes.proc * maxBoostFromFields * termRes.term.opts.boost;
					uint16_t proc16 = 0;
					if (proc > std::numeric_limits<uint16_t>::max() / 4) {
						proc16 = std::numeric_limits<uint16_t>::max() / 4;
					} else {
						proc16 = static_cast<uint16_t>(proc);
					}

					if (proc16 <= std::numeric_limits<uint16_t>::max() - docsScores[docId]) {
						docsScores[docId] += proc16;
					} else {
						docsScores[docId] = std::numeric_limits<uint16_t>::max();
					}

					termMask.set(docId);
				}
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::collectRestrictingMask(std::vector<TextSearchResults<IdCont>>& rawResults,
																		 const std::vector<size_t>& synonymsBounds) {
	const auto& vdocs = holder_.vdocs_;
	std::vector<uint16_t> docsScores(vdocs.size());

	const size_t synonymsGroupsEnd = synonymsBounds.empty() ? 0 : synonymsBounds.back();
	std::vector<BitsetType> synonymGroupsBitmasks(synonymsBounds.size());

	BitsetType tmpMask;
	for (size_t synGrpIdx = 0; synGrpIdx < synonymsBounds.size(); ++synGrpIdx) {
		const size_t synGroupBegin = synGrpIdx > 0 ? synonymsBounds[synGrpIdx - 1] : 0;
		const size_t synGroupEnd = synonymsBounds[synGrpIdx];
		auto& groupBitmask = synonymGroupsBitmasks[synGrpIdx];

		for (index_t termIdx = synGroupBegin; termIdx + 1 < synGroupEnd; ++termIdx) {
			[[maybe_unused]] const auto termOp = rawResults[termIdx].Op();
			assertrx(termOp == OpAnd || termOp == OpOr);

			calcTermBitmask(rawResults[termIdx], tmpMask);
			if (!groupBitmask.size()) {
				groupBitmask.swap(tmpMask);
			} else {
				groupBitmask &= tmpMask;
			}
		}

		const BitsetType* restrictingMask = groupBitmask.size() > 0 ? &groupBitmask : nullptr;
		calcTermScores(rawResults[synGroupEnd - 1], restrictingMask, tmpMask, docsScores);
		if (!groupBitmask.size()) {
			groupBitmask.swap(tmpMask);
		} else {
			groupBitmask &= tmpMask;
		}
	}

	BitsetType andBitmask;
	BitsetType notBitmask;

	size_t phraseIdx = 0;
	for (index_t termIdx = synonymsGroupsEnd; termIdx < rawResults.size(); ++termIdx) {
		if (rawResults[termIdx].GroupNum() != -1) {
			const OpType phraseOp = rawResults[termIdx].Op();
			const int groupNum = rawResults[termIdx].GroupNum();
			size_t phraseEnd = termIdx + 1;

			while (phraseEnd < rawResults.size() && rawResults[phraseEnd].GroupNum() == groupNum) {
				phraseEnd++;
			}

			phraseMergers_[phraseIdx].GetMergedDocsScores(tmpMask, docsScores);

			if (phraseOp == OpAnd) {
				if (!andBitmask.size()) {
					andBitmask.swap(tmpMask);
				} else {
					andBitmask &= tmpMask;
				}
			} else if (phraseOp == OpNot) {
				phraseMergers_[phraseIdx].GetMergedDocsScores(tmpMask, docsScores);
				if (!notBitmask.size()) {
					notBitmask.swap(tmpMask);
				} else {
					notBitmask |= tmpMask;
				}
			}

			termIdx = phraseEnd - 1;
			phraseIdx++;
			continue;
		}

		const auto termOp = rawResults[termIdx].Op();
		if (termOp == OpNot) {
			calcTermBitmask(rawResults[termIdx], tmpMask);
			if (!notBitmask.size()) {
				notBitmask.swap(tmpMask);
			} else {
				notBitmask |= tmpMask;
			}

			continue;
		}

		calcTermScores(rawResults[termIdx], nullptr, tmpMask, docsScores);

		if (termOp == OpAnd) {
			for (size_t synGrpIdx : rawResults[termIdx].synonymsGroups) {
				assertrx_throw(synGrpIdx < synonymsBounds.size());
				tmpMask |= synonymGroupsBitmasks[synGrpIdx];
			}

			if (!andBitmask.size()) {
				andBitmask.swap(tmpMask);
			} else {
				andBitmask &= tmpMask;
			}
		}
	}

	std::vector<size_t> sortData(std::numeric_limits<uint16_t>::max() + 1);
	size_t docsWithPositiveScores = 0;

	for (size_t i = 0; i < docsScores.size(); i++) {
		if (andBitmask.size() && !andBitmask[i]) {
			docsScores[i] = 0;
		}

		if (notBitmask.size() && notBitmask[i]) {
			docsScores[i] = 0;
		}

		sortData[docsScores[i]]++;
		if (docsScores[i] > 0) {
			docsWithPositiveScores++;
		}
	}

	if (docsWithPositiveScores > holder_.cfg_->mergeLimit && holder_.cfg_->logLevel >= LogWarning) {
		logFmt(LogWarning,
			   "The number of documents satisfying the query exceeds merge_limit : number_of_results={}, merge_limit={}. Selecting only "
			   "the most relevant documents",
			   docsWithPositiveScores, holder_.cfg_->mergeLimit);
	}

	size_t minScore = std::numeric_limits<uint16_t>::max();
	size_t minScoreDocs = 0;

	size_t docsTaken = 0;
	for (size_t score = sortData.size() - 1; score > 0; score--) {
		if (docsTaken >= holder_.cfg_->mergeLimit) {
			break;
		}

		minScore = score;
		minScoreDocs = holder_.cfg_->mergeLimit - docsTaken;

		docsTaken += sortData[score];
	}

	restrictingMask_.resize(0);
	restrictingMask_.resize(vdocs.size(), false);

	size_t minScoreDocsTaken = 0;
	for (size_t i = 0; i < docsScores.size(); i++) {
		if (docsScores[i] > minScore) {
			restrictingMask_.set(i);
		} else if (docsScores[i] == minScore && minScoreDocsTaken < minScoreDocs) {
			restrictingMask_.set(i);
			minScoreDocsTaken++;
		}
	}
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
	init<Bm25T>(rawResults, vdocs.size(), maxMergedSize, synonymsBounds);

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

	static const bool kDisable2PhaseMerge = std::getenv("REINDEXER_NO_2PHASE_FT_MERGE");
	if (!kDisable2PhaseMerge && estimateNumDocsInMerge(rawResults, synonymsBounds) > holder_.cfg_->mergeLimit) {
		collectRestrictingMask(rawResults, synonymsBounds);
	}

	const size_t synonymsGroupsEnd = synonymsBounds.empty() ? 0 : synonymsBounds.back();
	std::vector<std::vector<bool>> synonymGroupsBitmasks(synonymsBounds.size());
	std::vector<bool> synonymGroupsBitmasksNeeded(synonymsBounds.size(), false);
	if (!restrictingMask_.size()) {
		for (index_t termIdx = synonymsGroupsEnd; termIdx < rawResults.size(); ++termIdx) {
			if (rawResults[termIdx].GroupNum() != -1) {
				const int groupNum = rawResults[termIdx].GroupNum();
				size_t phraseEnd = termIdx + 1;

				while (phraseEnd < rawResults.size() && rawResults[phraseEnd].GroupNum() == groupNum) {
					phraseEnd++;
				}
				termIdx = phraseEnd - 1;
				continue;
			}

			const auto termOp = rawResults[termIdx].Op();
			if (termOp != OpAnd) {
				continue;
			}

			for (size_t synGrpIdx : rawResults[termIdx].synonymsGroups) {
				assertrx_throw(synGrpIdx < synonymsBounds.size());
				synonymGroupsBitmasksNeeded[synGrpIdx] = true;
			}
		}
	}

	for (size_t synGrpIdx = 0; synGrpIdx < synonymsBounds.size(); ++synGrpIdx) {
		hasBeenAnd_ = false;
		auto& bitmask = synonymGroupsBitmasks[synGrpIdx];
		const size_t synGroupBegin = synGrpIdx > 0 ? synonymsBounds[synGrpIdx - 1] : 0;
		const size_t synGroupEnd = synonymsBounds[synGrpIdx];

		if (synGroupEnd - synGroupBegin > 1) {
			synonymGroupsBitmasksNeeded[synGrpIdx] = true;
		}

		for (index_t termIdx = synGroupBegin; termIdx < synGroupEnd; ++termIdx) {
			bool bitmaskNeeded = synonymGroupsBitmasksNeeded[synGrpIdx];
			if (bitmaskNeeded) {
				bitmask.resize(0);
				bitmask.resize(vdocs.size(), false);
			}
			mergeTermResults<Bm25T>(rawResults[termIdx], termIdx, bitmaskNeeded ? &bitmask : nullptr);
			hasBeenAnd_ = true;

			if (termIdx > synGroupBegin) {
				assertrx_throw(bitmask.size());
				for (auto& md : mergeData_) {
					if (bitmask[md.id] || reindexer::fp::IsZero(md.proc) || mergeStatuses_[md.id] == FtMergeStatuses::kExcluded ||
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
	size_t phraseIdx = 0;

	for (index_t termIdx = synonymsGroupsEnd; termIdx < rawResults.size(); ++termIdx) {
		if (rawResults[termIdx].GroupNum() != -1) {
			const OpType phraseOp = rawResults[termIdx].Op();
			const int groupNum = rawResults[termIdx].GroupNum();
			const size_t phraseBegin = termIdx;
			size_t phraseEnd = termIdx + 1;

			while (phraseEnd < rawResults.size() && rawResults[phraseEnd].GroupNum() == groupNum) {
				rawResults[phraseEnd].Op() = OpAnd;
				phraseEnd++;
			}

			mergePhraseResults<Bm25T>(phraseIdx, phraseBegin, phraseOp, rawResults[phraseBegin].QPos());

			if (phraseOp == OpAnd) {
				hasBeenAnd_ = true;
			}
			termIdx = phraseEnd - 1;
			phraseIdx++;
			continue;
		}

		const auto termOp = rawResults[termIdx].Op();
		bool bitmaskNeeded = ((termOp == OpAnd) && !restrictingMask_.size());
		if (bitmaskNeeded) {
			bitmask.resize(0);
			bitmask.resize(vdocs.size(), false);
		}

		// already processed by restricting mask
		if (termOp == OpNot && restrictingMask_.size()) {
			continue;
		}

		mergeTermResults<Bm25T>(rawResults[termIdx], termIdx, bitmaskNeeded ? &bitmask : nullptr);

		if (termOp == OpAnd) {
			// already processed by restricting mask
			if (restrictingMask_.size()) {
				continue;
			}

			hasBeenAnd_ = true;
			for (auto& md : mergeData_) {
				const auto docId = md.id;
				if (bitmask[docId] || reindexer::fp::IsZero(md.proc) || mergeStatuses_[docId] == FtMergeStatuses::kExcluded) {
					continue;
				}

				bool matchSyn = false;
				for (size_t synGrpIdx : rawResults[termIdx].synonymsGroups) {
					assertrx_throw(synGrpIdx < synonymsBounds.size());
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

	if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Complex merge ({} patterns): out {} vids", rawResults.size(), mergeData_.size());
	}

	addFullMatchBoost(rawResults.size());
	postProcessResults(rankSortType);

	return std::move(mergeData_);
}

}  // namespace reindexer
