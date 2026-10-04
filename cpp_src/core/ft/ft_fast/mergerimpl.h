#include <type_traits>
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

RX_ALWAYS_INLINE unsigned PositionsDistance(PosType a, PosType b) noexcept {
	if (a.fullField() != b.fullField()) {
		return 0;
	}
	return a.fullPos() > b.fullPos() ? a.fullPos() - b.fullPos() : b.fullPos() - a.fullPos();
}

RX_ALWAYS_INLINE unsigned PositionsDistance(const PositionsVector& positions, const PositionsVector& otherPositions) {
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

RX_ALWAYS_INLINE unsigned PositionsDistance(const PositionsVector& positions, PosType other) noexcept {
	unsigned res = std::numeric_limits<unsigned>::max();
	for (const auto& p : positions) {
		if (p.fullField() != other.fullField()) {
			continue;
		}
		const unsigned dst = p.fullPos() > other.fullPos() ? p.fullPos() - other.fullPos() : other.fullPos() - p.fullPos();
		if (dst < res) {
			res = dst;
			if (res <= 1) {
				break;
			}
		}
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

		const uint32_t vdocId = phraseDocMergeData.id.ToNumber();

		if (!restrictingMask_[vdocId]) {
			continue;
		}

		const auto& phraseDocMergeDataExt = phraseMerger.GetMergeDataExtended(phraseDocIdx);

		if (!docAdded(vdocId) && numDocs() < maxMergedDocs_) {	// add new
			InfoType md{.id = IdType::FromNumber(vdocId), .proc = phraseDocMergeData.proc, .field = phraseDocMergeData.field};

			MergerDocumentData mdExt(phraseDocMergeDataExt.rank);
			if constexpr (kWithAreas) {
				mergeData_.vectorAreas.emplace_back(phraseDocMergeDataExt.CreateAreas(phraseDocMergeData.proc, maxAreasInDoc_));
				md.areaIndex = mergeData_.vectorAreas.size() - 1;
			}

			mdExt.lastTermPositions = phraseDocMergeDataExt.lastPhrasePositions;
			mdExt.InreaseTermsCounter(qpIdx);
			mergeData_.emplace_back(std::move(md));
			mergeDataExtended_.emplace_back(std::move(mdExt));
			if (useIdoffsets_) {
				idoffsets_[vdocId] = MergeOffsetT(mergeData_.size() - 1);
			}
		} else if (docAdded(vdocId)) {
			auto& md = getMergeData(vdocId);
			auto& mdExt = getMergeDataExtended(vdocId);

			mdExt.InreaseTermsCounter(qpIdx);
			md.proc += phraseDocMergeData.proc;
			mdExt.lastTermPositions = phraseDocMergeDataExt.lastPhrasePositions;
			mdExt.rank = 0;

			if constexpr (kWithAreas) {
				auto areas = phraseDocMergeDataExt.CreateAreas(phraseDocMergeData.proc, maxAreasInDoc_);
				copyAreas(areas, mergeData_.vectorAreas[md.areaIndex], phraseDocMergeDataExt.rank, fieldSize_, maxAreasInDoc_);
			}
		}
	}
}

// occurrenceScore = queryBoost * subtermProc * bm25Norm * termLenBoost * positionRank  (× fieldBoost inside calcTermRank)
// bm25: fulltext.md#basic-document-ranking-algorithms
// Final rank pipeline: fulltext_ranking.md#how-document-rank-is-built

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T, typename DocsStatsGetter>
void Merger<IdCont, MergeDataType, MergeOffsetT>::mergeTerm(TermResults<IdCont>& term, uint16_t qpIdx,
															const DocsStatsGetter& docsStatsGetter) {
	if (term.Op() == OpNot) {
		return;
	}

	switchToNextWord();
	FtDslOpts termOpts = term.Opts();

	for (SubtermResults<IdCont>& subterm : term) {
		termOpts.termLenBoost = subterm.TermLenBoost();
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		Bm25Calculator<Bm25T> bm25{static_cast<double>(docsStatsGetter.NumLiveDocs()), static_cast<double>(subterm.Occurences().size()),
								   cfg_->bm25Config.bm25k1, cfg_->bm25Config.bm25b};

		for (auto&& occurence : subterm.Occurences()) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(occurence), const IdRelType&>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(occurence), IdRelTypePacked&>),
						  "Expecting positionsInDoc is movable for packed vector and not movable for simple vector");

			const uint32_t vdocId = occurence.VdocId();
			if (vdocId >= totalNumDocs_ || !restrictingMask_[vdocId]) {
				continue;
			}
			if (docsStatsGetter.IsDeleted(occurence)) {
				continue;
			}

			if (!docAdded(vdocId) && numDocs() >= maxMergedDocs_) {
				continue;
			}

			if (subterm.Suppressed()) {
				if (docAdded(vdocId)) {
					auto& mdExt = getMergeDataExtended(vdocId);
					mdExt.InreaseTermsCounter(qpIdx);
				}
				continue;
			}

			// Find field with max rank
			TermRankInfo subtermInf;
			subtermInf.proc = subterm.Proc();
			subtermInf.pattern = subterm.Pattern();
			auto [rank, field] = calcTermRank(termOpts, bm25, occurence, vdocId, subtermInf, cfg_, docsStatsGetter);
			if (fp::IsZero(rank)) {
				continue;
			}
			if (cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subterm.Pattern(), bm25.GetIDF(), termOpts.termLenBoost);
			}

			if (!docAdded(vdocId)) {
				if (occurence.IsSimple()) {
					addDoc(vdocId, rank, field, occurence.PeekSimplePos(), subtermInf, term.Pattern());
				} else {
					auto positions = TakeOccurencePos(occurence);
					addDoc(vdocId, rank, field, std::move(positions), subtermInf, term.Pattern());
				}
				auto& mdExt = getMergeDataExtended(vdocId);
				mdExt.InreaseTermsCounter(qpIdx);
			} else if (occurence.IsSimple()) {
				const PosType hit = occurence.PeekSimplePos();
				addDocAreas(vdocId, hit, rank, subtermInf, term.Pattern());

				auto& md = getMergeData(vdocId);
				auto& mdExt = getMergeDataExtended(vdocId);
				mdExt.InreaseTermsCounter(qpIdx);

				unsigned distance = PositionsDistance(mdExt.lastTermPositions, hit);
				const float normDist = FTFieldConfig::bound(1.0 / float(std::max(distance, 1U)), cfg_->distanceWeight, cfg_->distanceBoost);
				const float finalRank = normDist * rank;

				if (finalRank > mdExt.rank) {
					md.proc -= mdExt.rank;
					md.proc += finalRank;
					mdExt.nextTermPositions.clear();
					mdExt.nextTermPositions.emplace_back(hit);
					mdExt.rank = finalRank;
				}
			} else {
				auto positions = TakeOccurencePos(occurence);
				addDocAreas(vdocId, positions, rank, subtermInf, term.Pattern());

				auto& md = getMergeData(vdocId);
				auto& mdExt = getMergeDataExtended(vdocId);
				mdExt.InreaseTermsCounter(qpIdx);

				unsigned distance = PositionsDistance(mdExt.lastTermPositions, positions);
				const float normDist = FTFieldConfig::bound(1.0 / float(std::max(distance, 1U)), cfg_->distanceWeight, cfg_->distanceBoost);
				const float finalRank = normDist * rank;

				if (finalRank > mdExt.rank) {
					md.proc -= mdExt.rank;
					md.proc += finalRank;
					mdExt.nextTermPositions = std::move(positions);
					mdExt.rank = finalRank;
				}
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T, typename DocsStatsGetter>
MergeDataType Merger<IdCont, MergeDataType, MergeOffsetT>::mergeSimple(TermResults<IdCont>& singleTerm, RankSortType rankSortType,
																	   const DocsStatsGetter& docsStatsGetter) {
	FtDslOpts termOpts = singleTerm.Opts();
	// loop on subterm (word, translit, stemmer,...)
	for (auto& subterm : singleTerm) {
		termOpts.termLenBoost = subterm.TermLenBoost();
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}
		Bm25Calculator<Bm25T> bm25{static_cast<double>(docsStatsGetter.NumLiveDocs()), static_cast<double>(subterm.Occurences().size()),
								   cfg_->bm25Config.bm25k1, cfg_->bm25Config.bm25b};

		for (auto& occurence : subterm.Occurences()) {
			const uint32_t vdocId = occurence.VdocId();
			if (vdocId >= totalNumDocs_) {
				continue;
			}
			if (docsExcluded_.size() != 0 && docsExcluded_[vdocId]) {
				continue;
			}
			if (docsStatsGetter.IsDeleted(occurence)) {
				continue;
			}

			if (!docAdded(vdocId) && numDocs() >= maxMergedDocs_) {
				continue;
			}

			// Find field with max rank
			TermRankInfo subtermInf;
			subtermInf.proc = subterm.Proc();
			subtermInf.pattern = subterm.Pattern();
			auto [rank, field] = calcTermRank(termOpts, bm25, occurence, vdocId, subtermInf, cfg_, docsStatsGetter);
			if (fp::IsZero(rank)) {
				continue;
			}

			if (cfg_->logLevel >= LogTrace) [[unlikely]] {
				logFmt(LogInfo, "Pattern {}, idf {}, termLenBoost {}", subterm.Pattern(), bm25.GetIDF(), termOpts.termLenBoost);
			}

			if (!docAdded(vdocId)) {
				// only 1 term in query
				addDoc(vdocId, rank, field);
				if (occurence.IsSimple()) {
					addLastDocAreas(occurence.PeekSimplePos(), rank, subtermInf, singleTerm.Pattern());
				} else {
					auto positions = TakeOccurencePos(occurence);
					addLastDocAreas(positions, rank, subtermInf, singleTerm.Pattern());
				}
			} else {
				auto& md = getMergeData(vdocId);
				if (md.proc < rank) {
					md.proc = rank;
					md.field = field;
				}

				if (occurence.IsSimple()) {
					addDocAreas(vdocId, occurence.PeekSimplePos(), rank, subtermInf, singleTerm.Pattern());
				} else {
					auto positions = TakeOccurencePos(occurence);
					addDocAreas(vdocId, positions, rank, subtermInf, singleTerm.Pattern());
				}
			}
		}
	}

	addFullMatchBoost(1, docsStatsGetter);	// ToDo #2455
	postProcessResults(rankSortType);

	return std::move(mergeData_);
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename DocsStatsGetter>
void Merger<IdCont, MergeDataType, MergeOffsetT>::calcTermBitmask(const TermResults<IdCont>& term, BitsetType& termMask,
																  const DocsStatsGetter& docsStatsGetter) {
	termMask.ResizeAndReset(totalNumDocs_);

	bool allFieldsHavePositiveBoost = std::ranges::all_of(term.Opts().fieldsOpts, [](const auto& opts) { return opts.boost; });

	// loop on subterm (word, translit, stemmer,...)
	for (const SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (auto& occurence : subterm.Occurences()) {
			const uint32_t vdocId = occurence.VdocId();
			if (vdocId >= totalNumDocs_ || termMask[vdocId]) {
				continue;
			}
			if (docsStatsGetter.IsDeleted(occurence)) {
				continue;
			}

			if (allFieldsHavePositiveBoost || checkFieldsRelevance(occurence, term.Opts())) {
				termMask.set(vdocId);
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename DocsStatsGetter>
void Merger<IdCont, MergeDataType, MergeOffsetT>::excludeTermFromBitmask(const TermResults<IdCont>& term, BitsetType& mask,
																		 const DocsStatsGetter& docsStatsGetter) {
	for (const SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (auto& occurence : subterm.Occurences()) {
			if (!docsStatsGetter.IsDeleted(occurence)) {
				mask.reset(occurence.VdocId());
			}
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename DocsStatsGetter>
void Merger<IdCont, MergeDataType, MergeOffsetT>::calcTermScores(TermResults<IdCont>& term, const BitsetType& restrictingMask,
																 BitsetType& termMask, std::vector<uint16_t>& docsScore,
																 const DocsStatsGetter& docsStatsGetter) {
	termMask.resize(0);
	termMask.resize(totalNumDocs_, false);

	const float fieldsBoost = term.Opts().fieldsOpts[0].boost;
	bool allFieldsHaveSameBoost = std::ranges::all_of(
		term.Opts().fieldsOpts, [fieldsBoost](const auto& opts) { return reindexer::fp::ExactlyEqual(opts.boost, fieldsBoost); });

	// loop on subterm (word, translit, stemmer,...)
	for (SubtermResults<IdCont>& subterm : term) {
		if (!inTransaction_) {
			ThrowOnCancel(ctx_);
		}

		for (auto& occurence : subterm.Occurences()) {
			const index_t vdocId = occurence.VdocId();
			if (vdocId >= totalNumDocs_ || !restrictingMask[vdocId]) {
				continue;
			}
			if (docsStatsGetter.IsDeleted(occurence)) {
				continue;
			}

			const float maxBoostFromFields = allFieldsHaveSameBoost ? fieldsBoost : maxFieldsBoost(occurence, term.Opts());
			if (maxBoostFromFields > 0.0) {
				if (!termMask[vdocId]) {
					float proc = subterm.Proc() * maxBoostFromFields * term.Opts().boost;

					uint16_t proc16 = std::min<uint16_t>(static_cast<uint16_t>(proc), std::numeric_limits<uint16_t>::max() / 4);
					proc16 = std::min<uint16_t>(proc16, std::numeric_limits<uint16_t>::max() - docsScore[vdocId]);
					docsScore[vdocId] += proc16;
					termMask.set(vdocId);
				}
			}
		}
	}
}

// Builds doc mask for required (+) terms and phrases before ranking.
// See fulltext.md#binary-operators
template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename DocsStatsGetter>
void Merger<IdCont, MergeDataType, MergeOffsetT>::buildRestrictingBitmask(QueryMergeData<IdCont>& queryMergeData,
																		  const DocsStatsGetter& docsStatsGetter) {
	if (docsExcluded_.size() == 0) {
		// Empty extern mask: all vdocs are allowed until AND/NOT terms narrow the set.
		restrictingMask_.ResizeAndSet(totalNumDocs_);
	} else {
		restrictingMask_.resize(0);
		restrictingMask_.swap(docsExcluded_);
		std::ignore = restrictingMask_.Invert();
	}
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
			calcTermBitmask(qp.Term(), termMask, docsStatsGetter);
		}

		for (size_t synId : qp.SynonymsIds()) {
			auto& syn = queryMergeData.synonyms[synId];
			BitsetType& synMask = synonymsMasks[synId];

			if (!synMask.size()) {
				for (auto& term : syn.Terms()) {
					calcTermBitmask(term, synonymTermMask, docsStatsGetter);
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
			excludeTermFromBitmask(qp.Term(), restrictingMask_, docsStatsGetter);
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename DocsStatsGetter>
void Merger<IdCont, MergeDataType, MergeOffsetT>::preselectMostRelevantDocs(QueryMergeData<IdCont>& queryMergeData,
																			const DocsStatsGetter& docsStatsGetter) {
	std::vector<uint16_t> docsScore(totalNumDocs_);
	BitsetType tmpMask;

	for (auto& syn : queryMergeData.synonyms) {
		for (auto& term : syn.Terms()) {
			calcTermScores(term, restrictingMask_, tmpMask, docsScore, docsStatsGetter);
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
			calcTermScores(qp.Term(), restrictingMask_, tmpMask, docsScore, docsStatsGetter);
		}
	}

	// sorting docs scores and collecting resulting mask
	std::vector<size_t> sortData(std::numeric_limits<uint16_t>::max() + 1);
	for (size_t i = 0; i < docsScore.size(); i++) {
		if (!restrictingMask_[i]) {
			docsScore[i] = 0;
		}
		sortData[docsScore[i]]++;
	}

	size_t docsWithPositiveScores = docsScore.size() - sortData[0];
	if (docsWithPositiveScores > maxMergedDocs_ && cfg_->logLevel >= LogWarning) {
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
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
template <typename Bm25T, typename DocsStatsGetter>
MergeDataType Merger<IdCont, MergeDataType, MergeOffsetT>::Merge(QueryMergeData<IdCont>& queryMergeData, RankSortType rankSortType,
																 const DocsStatsGetter& docsStatsGetter) {
	static_assert(sizeof(Bm25Calculator<Bm25T>) <= 32, "Bm25Calculator<Bm25T> size is greater than 32 bytes");

	if (queryMergeData.Empty() || totalNumDocs_ == 0) {
		return std::move(mergeData_);
	}

	const size_t maxMergedSize = std::min(size_t(cfg_->mergeLimit), queryMergeData.totalORVids);
	init<Bm25T>(queryMergeData, maxMergedSize, docsStatsGetter);

	queryMergeData.SortSubterms();
	if (queryMergeData.Simple()) {
		auto& singleTerm = queryMergeData.queryParts[0].Term();
		return mergeSimple<Bm25T>(singleTerm, rankSortType, docsStatsGetter);
	}

	buildRestrictingBitmask(queryMergeData, docsStatsGetter);
	static const bool kDisable2PhaseMerge = std::getenv("REINDEXER_NO_2PHASE_FT_MERGE");
	if (!kDisable2PhaseMerge && estimateNumDocsInMerge(queryMergeData) > cfg_->mergeLimit && totalNumDocs_ > cfg_->mergeLimit &&
		restrictingMask_.PopCount() > cfg_->mergeLimit) {
		preselectMostRelevantDocs(queryMergeData, docsStatsGetter);
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
			mergeTerm<Bm25T>(qp.Term(), ++qpIdx, docsStatsGetter);
		}
	}

	// processing multiword synonyms
	size_t numDocsBeforeSynonyms = mergeDataExtended_.size();
	for (auto& syn : queryMergeData.synonyms) {
		for (auto& term : syn.Terms()) {
			mergeTerm<Bm25T>(term, ++qpIdx, docsStatsGetter);
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
			if (useIdoffsets_) {
				idoffsets_[mergeData_[idx].id.ToNumber()] = kNotInMerge;
			}
			continue;
		}

		if (newIdx < idx) {
			mergeData_[newIdx] = std::move(mergeData_[idx]);
			mergeDataExtended_[newIdx] = std::move(mergeDataExtended_[idx]);
			if (useIdoffsets_) {
				idoffsets_[mergeData_[newIdx].id.ToNumber()] = MergeOffsetT(newIdx);
			}
		}

		++newIdx;
	}

	mergeData_.resize(newIdx);
	mergeDataExtended_.resize(newIdx);

	if (cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Complex merge ({} patterns, {} synonyms): out {} vids", queryMergeData.QueryLength(),
			   queryMergeData.synonyms.size(), mergeData_.size());
	}

	addFullMatchBoost(queryMergeData.QueryLength(), docsStatsGetter);  // ToDo #2455
	postProcessResults(rankSortType);

	return std::move(mergeData_);
}

}  // namespace ft
}  // namespace reindexer
