#pragma once
#include "estl/lazy_deque.h"
#include "phrasemerger.h"
#include "querymergedata.h"

namespace reindexer {

namespace ft {

// Intermediate information about document found at current merge step. Used only for queries with 2 or more terms
struct [[nodiscard]] MergerDocumentData {
	MergerDocumentData() noexcept = default;
	explicit MergerDocumentData(PositionsVector&& positions, float r) noexcept : nextTermPositions(std::move(positions)), rank(r) {}
	explicit MergerDocumentData(float r) noexcept : rank(r) {}
	MergerDocumentData(MergerDocumentData&&) noexcept = default;
	MergerDocumentData& operator=(MergerDocumentData&&) noexcept = default;
	MergerDocumentData& operator=(const MergerDocumentData&) = default;

	PositionsVector lastTermPositions;	// positions of matched document of current step
	PositionsVector nextTermPositions;	// positions of matched document of next step
	float rank = 0;						// Rank of current matched document

	uint16_t lastTermCounted = 0;
	uint16_t termsCounter = 0;
	bool containsFullMultiWordSynonym = false;
	bool canBeBoostedByFullMatch = false;

	void InreaseTermsCounter(uint16_t termIdx) {
		if (lastTermCounted < termIdx) {
			++termsCounter;
			lastTermCounted = termIdx;
		}
	}
};

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
class [[nodiscard]] Merger {
public:
	constexpr static bool kWithRegularAreas = std::is_same_v<MergeDataType, MergeDataAreas<Area>>;
	constexpr static bool kWithDebugAreas = std::is_same_v<MergeDataType, MergeDataAreas<AreaDebug>>;
	constexpr static bool kWithAreas = kWithRegularAreas || kWithDebugAreas;

	using InfoType = typename MergeDataType::InfoType;

	Merger(size_t totalNumDocs, FTConfig* cfg, FtMergeStatuses::Statuses& docsExcluded, size_t fieldSize, int maxAreasInDoc,
		   bool inTransaction, const RdxContext& ctx)
		: fieldSize_(fieldSize),
		  maxAreasInDoc_(maxAreasInDoc),
		  docsExcluded_(docsExcluded),
		  inTransaction_(inTransaction),
		  ctx_(ctx),
		  totalNumDocs_(totalNumDocs),
		  cfg_(cfg) {}

	template <typename Bm25Type, typename DocsStatsGetter>
	MergeDataType Merge(QueryMergeData<IdCont>& queryMergeData, RankSortType rankSortType, const DocsStatsGetter& docsStatsGetter);

private:
	const InfoType& getMergeData(index_t docId) const noexcept { return mergeData_[idoffsets_[docId]]; }
	const MergerDocumentData& getMergeDataExtended(index_t docId) const noexcept { return mergeDataExtended_[idoffsets_[docId]]; }
	InfoType& getMergeData(index_t docId) noexcept { return mergeData_[idoffsets_[docId]]; }
	MergerDocumentData& getMergeDataExtended(index_t docId) noexcept { return mergeDataExtended_[idoffsets_[docId]]; }

	template <typename Bm25Type, typename DocsStatsGetter>
	void init(QueryMergeData<IdCont>& queryMergeData, uint32_t maxMergedSize, const DocsStatsGetter& docsStatsGetter) {
		maxMergedDocs_ = std::min<uint32_t>(maxMergedSize, std::numeric_limits<MergeOffsetT>::max());
		maxMergedDocs_ = std::min(maxMergedSize, cfg_->mergeLimit);
		mergeData_.reserve(maxMergedDocs_);

		if (!queryMergeData.Trivial()) {
			idoffsets_.resize(totalNumDocs_, maxMergedDocs_);
		}

		if (!queryMergeData.Simple()) {
			mergeDataExtended_.reserve(maxMergedDocs_);

			if constexpr (kWithAreas) {
				mergeData_.vectorAreas.reserve(maxMergedDocs_);
			}
		}

		// init phraseMerger for each phrase and merge phrase results
		for (auto& qp : queryMergeData.queryParts) {
			if (qp.IsPhrase()) {
				auto& phraseMerger =
					phraseMergers_.emplace_back(totalNumDocs_, cfg_, docsExcluded_, fieldSize_, maxAreasInDoc_, inTransaction_, ctx_);
				phraseMerger.template Merge<Bm25Type>(qp.Phrase(), docsStatsGetter);
			}
		}
	}

	template <typename Bm25Type, typename DocsStatsGetter>
	MergeDataType mergeSimple(TermResults<IdCont>& singleTerm, RankSortType rankSortType, const DocsStatsGetter& docsStatsGetter);
	template <typename Bm25Type, typename DocsStatsGetter>
	void mergeTerm(TermResults<IdCont>& term, uint16_t qpIdx, const DocsStatsGetter& docsStatsGetter);

	template <typename Bm25T>
	void mergePhrase(size_t phraseIdx, PhraseResults<IdCont>& phrase, uint16_t qpIdx);

	template <typename DocsStatsGetter>
	void addFullMatchBoost(size_t numTerms, const DocsStatsGetter& docsStatsGetter) {
		for (size_t idx = 0; idx < mergeData_.size(); ++idx) {
			auto& md = mergeData_[idx];
			if ((mergeDataExtended_.empty() || mergeDataExtended_[idx].canBeBoostedByFullMatch) &&
				docsStatsGetter.NumWordsInField(md.id.ToNumber(), md.field) == numTerms) {
				md.proc *= cfg_->fullMatchBoost;
			}
		}
	}

	void postProcessResults(RankSortType rankSortType) {
		float maxProc = 0.0;
		for (auto& md : mergeData_) {
			maxProc = std::max(maxProc, md.proc);
		}

		const float scalingFactor = maxProc > 255 ? 255.0 / maxProc : 1.0;
		const float minProc = cfg_->minRank;

		size_t numDocsPassed = mergeData_.size();
		while (numDocsPassed > 0 && mergeData_[numDocsPassed - 1].proc < minProc) {
			numDocsPassed--;
		}

		for (size_t i = 0; i + 1 < numDocsPassed; i++) {
			if (mergeData_[i].proc < minProc) {
				mergeData_[i] = std::move(mergeData_[numDocsPassed - 1]);
				numDocsPassed--;
				while (numDocsPassed > i && mergeData_[numDocsPassed - 1].proc < minProc) {
					numDocsPassed--;
				}
			}
		}
		mergeData_.resize(numDocsPassed);

		for (auto& md : mergeData_) {
			md.normalizedProc = static_cast<uint8_t>(md.proc * scalingFactor);
			md.proc = md.normalizedProc;
		}

		switch (rankSortType) {
			case RankSortType::RankOnly:
			case RankSortType::IDAndPositions:
				boost::sort::pdqsort_branchless(mergeData_.begin(), mergeData_.end(), [](const InfoType& l, const InfoType& r) noexcept {
					return l.normalizedProc > r.normalizedProc;
				});
				return;
			case RankSortType::RankAndID:
			case RankSortType::IDOnly:
				return;
			case RankSortType::ExternalExpression:
				throw Error(errLogic, "RankSortType::ExternalExpression not implemented.");
				break;
		}
	}

	size_t numDocs() const noexcept { return mergeData_.size(); }

	bool docAdded(int docId) const noexcept { return !idoffsets_.empty() && idoffsets_[docId] != maxMergedDocs_; }

	void addDoc(int docId, float proc, uint8_t field) {
		InfoType info{.id = IdType::FromNumber(docId), .proc = proc, .field = field};

		if constexpr (kWithAreas) {
			auto& area = mergeData_.vectorAreas.emplace_back();
			area.ReserveField(fieldSize_);
			info.areaIndex = mergeData_.vectorAreas.size() - 1;
		}

		mergeData_.push_back(std::move(info));
		if (!idoffsets_.empty()) {
			idoffsets_[docId] = mergeData_.size() - 1;
		}
	}

	void addDoc(int docId, float proc, uint8_t field, PositionsVector&& positions, TermRankInfo& subtermInf, const std::wstring& pattern) {
		addDoc(docId, proc, field);
		addLastDocAreas(positions, proc, subtermInf, pattern);
		mergeDataExtended_.emplace_back(std::move(positions), proc);
	}

	void addDocAreas(int docId, const PositionsVector& positions, float rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithAreas) {
			auto& md = getMergeData(docId);
			addAreas(md.areaIndex, positions, rank, termInf, pattern);
		}
	}

	void addLastDocAreas(const PositionsVector& positions, float rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithAreas) {
			size_t lastAreaIdx = mergeData_.vectorAreas.size() - 1;
			addAreas(lastAreaIdx, positions, rank, termInf, pattern);
		}
	}

	void addAreas(size_t areaIdx, const PositionsVector& positions, float rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithRegularAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			for (auto pos : positions) {
				if (!docAreas.AddWord(Area(pos.pos(), pos.pos() + 1, pos.arrayIdx()), pos.field(), rank, maxAreasInDoc_)) {
					break;
				}
			}
			docAreas.UpdateRank(rank);
		} else if constexpr (kWithDebugAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			utf16_to_utf8(pattern, const_cast<std::string&>(termInf.ftDslTerm));
			for (auto pos : positions) {
				if (!docAreas.AddWord(AreaDebug(pos.pos(), pos.pos() + 1, pos.arrayIdx(), termInf.ToString(), AreaDebug::PhraseMode::None),
									  pos.field(), termInf.termRank, -1)) {
					break;
				}
			}
			docAreas.UpdateRank(termInf.termRank);
		}
	}

	void switchToNextWord() {
		for (auto& mdExt : mergeDataExtended_) {
			if (mdExt.nextTermPositions.size()) {
				mdExt.lastTermPositions.swap(mdExt.nextTermPositions);
				mdExt.nextTermPositions.resize(0);
				mdExt.rank = 0;
			}
		}
	}

	void calcTermBitmask(const TermResults<IdCont>& term, BitsetType& termMask);
	void excludeTermFromBitmask(const TermResults<IdCont>& term, BitsetType& mask);

	void calcTermScores(TermResults<IdCont>& term, const BitsetType& restrictingMask, BitsetType& termMask,
						std::vector<uint16_t>& docsScore);

	void buildRestrictingBitmask(QueryMergeData<IdCont>& queryData);

	template <typename DocsStatsGetter>
	void preselectMostRelevantDocs(QueryMergeData<IdCont>& queryData, const DocsStatsGetter& docsStatsGetter);

	size_t estimateNumDocsInMerge(QueryMergeData<IdCont>& queryData) {
		size_t estimatedNumDocsOr = 0;
		size_t estimatedNumDocsAnd = std::numeric_limits<size_t>::max();

		size_t phraseIdx = 0;
		for (auto& qp : queryData.queryParts) {
			if (qp.IsPhrase()) {
				++phraseIdx;
			}

			if (qp.Op() == OpNot) {
				continue;
			}

			size_t numDocs = qp.IsPhrase() ? phraseMergers_.at(phraseIdx - 1).NumDocsMerged() : qp.Term().EstimateNumDocsInMerge();
			for (size_t synId : qp.SynonymsIds()) {
				auto& syn = queryData.synonyms[synId];
				numDocs += syn.Terms()[0].EstimateNumDocsInMerge();
			}

			if (qp.Op() == OpAnd) {
				estimatedNumDocsAnd = std::min(estimatedNumDocsAnd, numDocs);
			} else {
				estimatedNumDocsOr += numDocs;
			}
		}

		return std::min(std::min(estimatedNumDocsOr, estimatedNumDocsAnd), totalNumDocs_);
	}

private:
	size_t fieldSize_ = 0;
	int maxAreasInDoc_ = 0;

	FtMergeStatuses::Statuses& docsExcluded_;
	lazy_deque<PhraseMerger<IdCont, MergeDataType, MergeOffsetT>> phraseMergers_;
	MergeDataType mergeData_;
	std::vector<MergerDocumentData> mergeDataExtended_;
	uint32_t maxMergedDocs_ = 0;
	std::vector<MergeOffsetT> idoffsets_;
	BitsetType restrictingMask_;
	bool needToCheckRemoved_ = true;

	bool inTransaction_ = false;
	const RdxContext& ctx_;

	size_t totalNumDocs_ = 0;
	FTConfig* cfg_ = nullptr;
};

}  // namespace ft
}  // namespace reindexer
