#include "core/ft/bm25.h"
#include "core/rdxcontext.h"
#include "merger.h"
#include "phrasemergerimpl.h"
#include "tools/logger.h"

namespace reindexer {
namespace ft {

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
	return (res == std::numeric_limits<unsigned>::max()) ? 0 : res;
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
void Merger<IdCont, MergeDataType, MergeOffsetT>::mergePhrase(size_t phraseIdx, PhraseResults<IdCont>& phrase, uint16_t qpIdx) {
	if (phrase.Op() == OpNot) {
		return;
	}

	auto& phraseMerger = phraseMergers_.at(phraseIdx);
	for (size_t phraseDocIdx = 0; phraseDocIdx < phraseMerger.NumDocsMerged(); ++phraseDocIdx) {
		const InfoType& phraseDocMergeData = phraseMerger.GetMergeData(phraseDocIdx);
		if (reindexer::fp::IsZero(phraseDocMergeData.proc)) {
			continue;
		}

		const index_t phraseDocId = phraseDocMergeData.id.ToNumber();

		if (!restrictingMask_[phraseDocId]) {
			continue;
		}

		const auto& phraseDocMergeDataExt = phraseMerger.GetMergeDataExtended(phraseDocIdx);

		if (!docAdded(phraseDocId) && numDocs() < maxMergedDocs_) {	 // add new
			InfoType md{.id = IdType::FromNumber(phraseDocId), .proc = phraseDocMergeData.proc, .field = phraseDocMergeData.field};

			MergerDocumentData mdExt(phraseDocMergeDataExt.rank);
			if constexpr (kWithAreas) {
				mergeData_.vectorAreas.emplace_back(phraseDocMergeDataExt.CreateAreas(phraseDocMergeData.proc, maxAreasInDoc_));
				md.areaIndex = mergeData_.vectorAreas.size() - 1;
			}

			mdExt.lastTermPositions = std::move(phraseDocMergeDataExt.lastPhrasePositions);
			mdExt.InreaseTermsCounter(qpIdx);
			mergeData_.emplace_back(std::move(md));
			mergeDataExtended_.emplace_back(std::move(mdExt));
			idoffsets_[phraseDocId] = mergeData_.size() - 1;
		} else if (docAdded(phraseDocId)) {
			auto& md = getMergeData(phraseDocId);
			auto& mdExt = getMergeDataExtended(phraseDocId);

			mdExt.InreaseTermsCounter(qpIdx);
			md.proc += phraseDocMergeData.proc;
			mdExt.lastTermPositions = std::move(phraseDocMergeDataExt.lastPhrasePositions);
			mdExt.rank = 0;

			if constexpr (kWithAreas) {
				auto areas = phraseDocMergeDataExt.CreateAreas(phraseDocMergeData.proc, maxAreasInDoc_);
				copyAreas(areas, mergeData_.vectorAreas[md.areaIndex], phraseDocMergeDataExt.rank, fieldSize_, maxAreasInDoc_);
			}
		}
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
void Merger<IdCont, MergeDataType, MergeOffsetT>::mergeTerm(TermResults<IdCont>& term, uint16_t qpIdx) {
	if (term.Op() == OpNot) {
		return;
	}

	const auto& vdocs = holder_.vdocs_;
	switchToNextWord();

	for (SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		Bm25Calculator<Bm25T> bm25{static_cast<double>(holder_.VDocsNumberInIndex()), static_cast<double>(subterm.Occurences().size()),
								   holder_.cfg_->bm25Config.bm25k1, holder_.cfg_->bm25Config.bm25b};

		for (auto&& occurence : subterm.Occurences()) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(occurence), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(occurence), IdRelType&>),
						  "Expecting positionsInDoc is movable for packed vector and not movable for simple vector");

			const int docId = occurence.Id();
			if (!restrictingMask_[docId]) {
				continue;
			}

			if (!docAdded(docId) && numDocs() >= maxMergedDocs_) {
				continue;
			}

			if (needToCheckRemoved_ && vdocs[docId].IsRemoved()) {
				continue;
			}

			if (subterm.Suppressed()) {
				// doc has been added
				assertrx_dbg(idoffsets_[docId] < mergeDataExtended_.size());
				auto& mdExt = getMergeDataExtended(docId);
				mdExt.InreaseTermsCounter(qpIdx);
				continue;
			}

			// Find field with max rank
			TermRankInfo subtermInf;
			subtermInf.proc = subterm.Proc();
			subtermInf.pattern = subterm.Pattern();
			auto [rank, field] = calcTermRank(term.Opts(), bm25, occurence, subtermInf, holder_);
			if (fp::IsZero(rank)) {
				continue;
			}
			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subterm.Pattern(), bm25.GetIDF(), term.Opts().termLenBoost);
			}

			if (!docAdded(docId)) {
				PositionsVector positions;
				InitFrom(std::move(occurence.Pos()), positions);
				addDoc(docId, rank, field, std::move(positions), subtermInf, term.Pattern());
				auto& mdExt = getMergeDataExtended(docId);
				mdExt.InreaseTermsCounter(qpIdx);
			} else {
				addDocAreas(docId, occurence.Pos(), rank, subtermInf, term.Pattern());

				auto& md = getMergeData(docId);
				auto& mdExt = getMergeDataExtended(docId);
				mdExt.InreaseTermsCounter(qpIdx);

				// zero for first occurence in field
				unsigned distance = PositionsDistance(mdExt.lastTermPositions, occurence.Pos());
				const float normDist =
					FTFieldConfig::bound(1.0 / float(std::max(distance, 1U)), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
				const float finalRank = normDist * rank;

				if (finalRank > mdExt.rank) {
					md.proc -= mdExt.rank;
					md.proc += finalRank;
					InitFrom(std::move(occurence.Pos()), mdExt.nextTermPositions);
					mdExt.rank = finalRank;
				}
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
MergeDataType Merger<IdCont, MergeDataType, MergeOffsetT>::mergeSimple(TermResults<IdCont>& singleTerm, RankSortType rankSortType) {
	const auto& vdocs = holder_.vdocs_;

	// loop on subterm (word, translit, stemmer,...)
	for (auto& subterm : singleTerm) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}
		Bm25Calculator<Bm25T> bm25{static_cast<double>(holder_.VDocsNumberInIndex()), static_cast<double>(subterm.Occurences().size()),
								   holder_.cfg_->bm25Config.bm25k1, holder_.cfg_->bm25Config.bm25b};

		for (const IdRelType& occurence : subterm.Occurences()) {
			const int docId = occurence.Id();

			if (docsExcluded_[docId] || vdocs[docId].IsRemoved()) {
				continue;
			}

			if (!docAdded(docId) && numDocs() >= maxMergedDocs_) {
				continue;
			}

			// Find field with max rank
			TermRankInfo subtermInf;
			subtermInf.proc = subterm.Proc();
			subtermInf.pattern = subterm.Pattern();
			auto [rank, field] = calcTermRank(singleTerm.Opts(), bm25, occurence, subtermInf, holder_);
			if (fp::IsZero(rank)) {
				continue;
			}

			if (holder_.cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subterm.Pattern(), bm25.GetIDF(), singleTerm.Opts().termLenBoost);
			}

			if (!docAdded(docId)) {
				// only 1 term in query
				addDoc(docId, rank, field);
				addLastDocAreas(occurence.Pos(), rank, subtermInf, singleTerm.Pattern());
			} else {
				auto& md = getMergeData(docId);
				if (md.proc < rank) {
					md.proc = rank;
					md.field = field;
				}

				addDocAreas(docId, occurence.Pos(), rank, subtermInf, singleTerm.Pattern());
			}
		}
	}

	addFullMatchBoost(1);  // ToDo #2455
	postProcessResults(rankSortType);

	return std::move(mergeData_);
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::calcTermBitmask(const TermResults<IdCont>& term, BitsetType& termMask) {
	termMask.ResizeAndReset(holder_.VDocsNumberInIndex());

	bool allFieldsHavePositiveBoost = std::ranges::all_of(term.Opts().fieldsOpts, [](const auto& opts) { return opts.boost; });

	// loop on subterm (word, translit, stemmer,...)
	for (const SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (const auto& occurence : subterm.Occurences()) {
			if (termMask[occurence.Id()]) {
				continue;
			}

			if (allFieldsHavePositiveBoost || checkFieldsRelevance(occurence, term.Opts())) {
				termMask.set(occurence.Id());
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::excludeTermFromBitmask(const TermResults<IdCont>& term, BitsetType& mask) {
	for (const SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (const auto& occurence : subterm.Occurences()) {
			mask.reset(occurence.Id());
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::calcTermScores(TermResults<IdCont>& term, const BitsetType& restrictingMask,
																 BitsetType& termMask, std::vector<uint16_t>& docsScore) {
	termMask.resize(0);
	termMask.resize(holder_.VDocsNumberInIndex(), false);

	const float fieldsBoost = term.Opts().fieldsOpts[0].boost;
	bool allFieldsHaveSameBoost = std::ranges::all_of(
		term.Opts().fieldsOpts, [fieldsBoost](const auto& opts) { return reindexer::fp::ExactlyEqual(opts.boost, fieldsBoost); });

	// loop on subterm (word, translit, stemmer,...)
	for (SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (const auto& occurence : subterm.Occurences()) {
			index_t docId = occurence.Id();
			if (!restrictingMask[docId]) {
				continue;
			}

			const float maxBoostFromFields = allFieldsHaveSameBoost ? fieldsBoost : maxFieldsBoost(occurence, term.Opts());
			if (maxBoostFromFields > 0.0) {
				if (!termMask[docId]) {
					float proc = subterm.Proc() * maxBoostFromFields * term.Opts().boost;

					uint16_t proc16 = std::min<uint16_t>(static_cast<uint16_t>(proc), std::numeric_limits<uint16_t>::max() / 4);
					proc16 = std::min<uint16_t>(proc16, std::numeric_limits<uint16_t>::max() - docsScore[docId]);
					docsScore[docId] += proc16;
					termMask.set(docId);
				}
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::buildRestrictingBitmask(QueryMergeData<IdCont>& queryMergeData) {
	restrictingMask_.resize(0);
	restrictingMask_.swap(docsExcluded_);
	std::ignore = restrictingMask_.Invert();
	BitsetType termMask, synonymTermMask;
	std::vector<BitsetType> synonymsMasks(queryMergeData.synonyms.size());

	// processing and terms
	size_t phraseIdx = 0;
	for (auto& qp : queryMergeData.queryParts) {
		if (qp.IsPhrase()) {
			++phraseIdx;
		}

		if (qp.Op() != OpAnd) {
			continue;
		}

		if (qp.IsPhrase()) {
			phraseMergers_.at(phraseIdx - 1).GetMergedDocsBitmask(termMask);
		} else {
			calcTermBitmask(qp.Term(), termMask);
		}

		for (size_t synId : qp.SynonymsIds()) {
			auto& syn = queryMergeData.synonyms[synId];
			BitsetType& synMask = synonymsMasks[synId];

			if (!synMask.size()) {
				for (auto& term : syn.Terms()) {
					calcTermBitmask(term, synonymTermMask);
					synMask.AccumulateAnd(synonymTermMask);
				}
			}
			termMask |= synMask;
		}

		restrictingMask_ &= termMask;
	}

	// processing not terms
	phraseIdx = 0;
	for (auto& qp : queryMergeData.queryParts) {
		if (qp.IsPhrase()) {
			++phraseIdx;
		}

		if (qp.Op() != OpNot) {
			continue;
		}

		if (qp.IsPhrase()) {
			phraseMergers_.at(phraseIdx - 1).ExcludeMergedDocsFromBitmask(restrictingMask_);
		} else {
			excludeTermFromBitmask(qp.Term(), restrictingMask_);
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
void Merger<IdCont, MergeDataType, MergeOffsetT>::preselectMostRelevantDocs(QueryMergeData<IdCont>& queryMergeData) {
	std::vector<uint16_t> docsScore(holder_.VDocsNumberInIndex());
	auto& vdocs = holder_.vdocs_;
	BitsetType tmpMask;

	for (auto& syn : queryMergeData.synonyms) {
		for (auto& term : syn.Terms()) {
			calcTermScores(term, restrictingMask_, tmpMask, docsScore);
		}
	}

	size_t phraseIdx = 0;
	for (auto& qp : queryMergeData.queryParts) {
		if (qp.IsPhrase()) {
			++phraseIdx;
		}

		if (qp.Op() == OpNot) {
			continue;
		}

		if (qp.IsPhrase()) {
			phraseMergers_.at(phraseIdx - 1).GetMergedDocsScore(docsScore);
		} else {
			calcTermScores(qp.Term(), restrictingMask_, tmpMask, docsScore);
		}
	}

	// sorting docs scores and collecting resulting mask
	std::vector<size_t> sortData(std::numeric_limits<uint16_t>::max() + 1);
	for (size_t i = 0; i < docsScore.size(); i++) {
		if (!restrictingMask_[i] || vdocs[i].IsRemoved()) {
			docsScore[i] = 0;
		}
		sortData[docsScore[i]]++;
	}

	size_t docsWithPositiveScores = docsScore.size() - sortData[0];
	if (docsWithPositiveScores > maxMergedDocs_ && holder_.cfg_->logLevel >= LogWarning) {
		logFmt(LogWarning,
			   "The number of documents satisfying the query exceeds merge_limit : number_of_results={}, merge_limit={}. Selecting only "
			   "the most relevant documents",
			   docsWithPositiveScores, maxMergedDocs_);
	}

	size_t minScore = std::numeric_limits<uint16_t>::max();
	size_t minScoreDocs = 0;

	size_t docsTaken = 0;
	for (size_t score = sortData.size() - 1; score > 0; score--) {
		if (docsTaken >= maxMergedDocs_) {
			break;
		}

		minScore = score;
		minScoreDocs = maxMergedDocs_ - docsTaken;

		docsTaken += sortData[score];
	}

	size_t minScoreDocsTaken = 0;
	for (size_t i = 0; i < docsScore.size(); i++) {
		if (!restrictingMask_[i]) {
			continue;
		}

		if (docsScore[i] > minScore) {
			continue;
		} else if (docsScore[i] == minScore && minScoreDocsTaken < minScoreDocs) {
			++minScoreDocsTaken;
			continue;
		}

		restrictingMask_.reset(i);
	}
	needToCheckRemoved_ = false;
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T>
MergeDataType Merger<IdCont, MergeDataType, MergeOffsetT>::Merge(QueryMergeData<IdCont>& queryMergeData, RankSortType rankSortType) {
	static_assert(sizeof(Bm25Calculator<Bm25T>) <= 32, "Bm25Calculator<Bm25T> size is greater than 32 bytes");

	if (queryMergeData.Empty() || holder_.VDocsNumberInIndex() == 0) {
		return std::move(mergeData_);
	}

	const size_t maxMergedSize = std::min(size_t(holder_.cfg_->mergeLimit), queryMergeData.totalORVids);
	init<Bm25T>(queryMergeData, maxMergedSize);

	queryMergeData.SortSubterms();
	if (queryMergeData.Simple()) {
		auto& singleTerm = queryMergeData.queryParts[0].Term();
		return mergeSimple<Bm25T>(singleTerm, rankSortType);
	}

	buildRestrictingBitmask(queryMergeData);
	static const bool kDisable2PhaseMerge = std::getenv("REINDEXER_NO_2PHASE_FT_MERGE");
	if (!kDisable2PhaseMerge && estimateNumDocsInMerge(queryMergeData) > holder_.cfg_->mergeLimit &&
		holder_.VDocsNumberInIndex() > holder_.cfg_->mergeLimit && restrictingMask_.PopCount() > holder_.cfg_->mergeLimit) {
		preselectMostRelevantDocs(queryMergeData);
	}

	size_t phraseIdx = 0;
	uint16_t qpIdx = 0;
	for (auto& qp : queryMergeData.queryParts) {
		if (qp.IsPhrase()) {
			++phraseIdx;
		}

		if (qp.Op() == OpNot) {
			continue;
		}

		if (qp.IsPhrase()) {
			mergePhrase<Bm25T>(phraseIdx - 1, qp.Phrase(), ++qpIdx);
		} else {
			mergeTerm<Bm25T>(qp.Term(), ++qpIdx);
		}
	}

	// processing multiword synonyms
	size_t numDocsBeforeSynonyms = mergeDataExtended_.size();
	for (auto& syn : queryMergeData.synonyms) {
		for (auto& term : syn.Terms()) {
			mergeTerm<Bm25T>(term, ++qpIdx);
		}

		// mark docs which contain all terms of syn
		for (size_t idx = numDocsBeforeSynonyms; idx < mergeDataExtended_.size(); ++idx) {
			if (mergeDataExtended_[idx].termsCounter < syn.NumTerms()) {
				mergeDataExtended_[idx].termsCounter = 0;
			} else {
				mergeDataExtended_[idx].containsFullMultiWordSynonym = true;
			}
		}
	}

	for (auto& mdExt : mergeDataExtended_) {
		if (mdExt.termsCounter == queryMergeData.queryParts.size()) {
			mdExt.canBeBoostedByFullMatch = true;
		}
	}

	// remove synonyms docs which contains only parts of multiword synonyms
	size_t newIdx = numDocsBeforeSynonyms;
	for (size_t idx = numDocsBeforeSynonyms; idx < mergeDataExtended_.size(); ++idx) {
		if (!mergeDataExtended_[idx].containsFullMultiWordSynonym) {
			if (!idoffsets_.empty()) {
				idoffsets_[mergeData_[idx].id.ToNumber()] = maxMergedDocs_;
			}
			continue;
		}

		if (newIdx < idx) {
			mergeData_[newIdx] = std::move(mergeData_[idx]);
			mergeDataExtended_[newIdx] = std::move(mergeDataExtended_[idx]);
			if (!idoffsets_.empty()) {
				idoffsets_[mergeData_[newIdx].id.ToNumber()] = newIdx;
			}
		}

		++newIdx;
	}

	mergeData_.resize(newIdx);
	mergeDataExtended_.resize(newIdx);

	if (holder_.cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Complex merge ({} patterns, {} synonyms): out {} vids", queryMergeData.QueryLength(),
			   queryMergeData.synonyms.size(), mergeData_.size());
	}

	addFullMatchBoost(queryMergeData.QueryLength());  // ToDo #2455
	postProcessResults(rankSortType);

	return std::move(mergeData_);
}

}  // namespace ft
}  // namespace reindexer
