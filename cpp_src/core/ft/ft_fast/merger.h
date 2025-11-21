#pragma once
#include <deque>
#include "phrasemerger.h"
#include "tools/float_comparison.h"

namespace reindexer {

// Intermediate information about document found at current merge step. Used only for queries with 2 or more terms
struct [[nodiscard]] MergerDocumentData {
	explicit MergerDocumentData(IdRelType&& c, int r, int q) noexcept : nextTermPositions(std::move(c.Pos())), rank(r), qpos(q) {}
	explicit MergerDocumentData(int r, int q) noexcept : rank(r), qpos(q) {}
	MergerDocumentData(MergerDocumentData&&) noexcept = default;
	h_vector<PosType, 3> lastTermPositions;	 // positions of matched document of current step
	h_vector<PosType, 3> nextTermPositions;	 // positions of matched document of next step
	float rank = 0;							 // Rank of current matched document
	int32_t qpos = 0;						 // Position in query
};

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
class [[nodiscard]] Merger {
public:
	using BitsetType = DynamicBitset<64>;

	constexpr static bool kWithRegularAreas = std::is_same_v<MergeDataType, MergeDataAreas<Area>>;
	constexpr static bool kWithDebugAreas = std::is_same_v<MergeDataType, MergeDataAreas<AreaDebug>>;
	constexpr static bool kWithAreas = kWithRegularAreas || kWithDebugAreas;

	using InfoType = typename MergeDataType::InfoType;

	Merger(DataHolder<IdCont>& holder, FtMergeStatuses::Statuses& mergeStatuses, size_t fieldSize, int maxAreasInDoc, bool inTransaction,
		   const RdxContext& ctx)
		: holder_(holder),
		  fieldSize_(fieldSize),
		  maxAreasInDoc_(maxAreasInDoc),
		  mergeStatuses_(mergeStatuses),
		  inTransaction_(inTransaction),
		  ctx_(ctx) {
		assertrx_throw(mergeStatuses.size() == holder_.vdocs_.size());
	}

	template <typename Bm25Type>
	MergeDataType Merge(std::vector<TextSearchResults<IdCont>>& rawResults, size_t totalORVids, const std::vector<size_t>& synonymsBounds,
						RankSortType rankSortType);

private:
	const InfoType& getMergeData(index_t docId) const noexcept { return mergeData_[idoffsets_[docId]]; }
	const MergerDocumentData& getMergeDataExtended(index_t docId) const noexcept { return mergeDataExtended_[idoffsets_[docId]]; }
	InfoType& getMergeData(index_t docId) noexcept { return mergeData_[idoffsets_[docId]]; }
	MergerDocumentData& getMergeDataExtended(index_t docId) noexcept { return mergeDataExtended_[idoffsets_[docId]]; }

	template <typename Bm25Type>
	void init(std::vector<TextSearchResults<IdCont>>& rawResults, size_t docsSize, size_t maxMergedSize,
			  const std::vector<size_t>& synonymsBounds) {
		mergeData_.reserve(maxMergedSize);
		if (rawResults.size() > 1) {
			idoffsets_.resize(docsSize);
			mergeDataExtended_.reserve(maxMergedSize);

			if constexpr (kWithAreas) {
				mergeData_.vectorAreas.reserve(maxMergedSize);
			}
		}

		if (rawResults.size() == 1 && rawResults[0].size() > 1) {
			idoffsets_.resize(docsSize);
		}

		const size_t synonymsGroupsEnd = synonymsBounds.empty() ? 0 : synonymsBounds.back();

		for (index_t termIdx = synonymsGroupsEnd; termIdx < rawResults.size(); ++termIdx) {
			if (rawResults[termIdx].GroupNum() != -1) {
				const int groupNum = rawResults[termIdx].GroupNum();
				const size_t phraseBegin = termIdx;
				size_t phraseEnd = phraseBegin + 1;

				while (phraseEnd < rawResults.size() && rawResults[phraseEnd].GroupNum() == groupNum) {
					phraseEnd++;
				}

				phraseMergers_.emplace_back(holder_, fieldSize_, maxAreasInDoc_, inTransaction_, ctx_);

				phraseMergers_.back().template Merge<Bm25Type>(rawResults, phraseBegin, phraseEnd);

				termIdx = phraseEnd - 1;
				continue;
			}
		}
	}

	size_t estimateNumDocsInMerge(const std::vector<TextSearchResults<IdCont>>& rawResults, const std::vector<size_t>& synonymsBounds) {
		size_t estimatedNumDocs = 0;
		const size_t synonymsGroupsEnd = synonymsBounds.empty() ? 0 : synonymsBounds.back();

		for (size_t synGrpIdx = 0; synGrpIdx < synonymsBounds.size(); ++synGrpIdx) {
			const size_t synGroupBegin = synGrpIdx > 0 ? synonymsBounds[synGrpIdx - 1] : 0;
			const size_t synGroupEnd = synonymsBounds[synGrpIdx];

			for (index_t termIdx = synGroupBegin; termIdx < synGroupEnd; ++termIdx) {
				for (const TextSearchResult<IdCont>& subtermRes : rawResults[termIdx]) {
					estimatedNumDocs += subtermRes.vids->size();
				}

				if (rawResults[termIdx].Op() == OpAnd) {
					break;
				}
			}
		}

		size_t phraseIdx = 0;
		for (index_t termIdx = synonymsGroupsEnd; termIdx < rawResults.size(); ++termIdx) {
			if (rawResults[termIdx].GroupNum() != -1) {
				const OpType phraseOp = rawResults[termIdx].Op();
				const int groupNum = rawResults[termIdx].GroupNum();
				size_t phraseEnd = termIdx + 1;

				while (phraseEnd < rawResults.size() && rawResults[phraseEnd].GroupNum() == groupNum) {
					phraseEnd++;
				}

				if (phraseOp != OpNot) {
					estimatedNumDocs += phraseMergers_[phraseIdx].NumDocsMerged();
				}

				if (phraseOp == OpAnd) {
					return estimatedNumDocs;
				}

				termIdx = phraseEnd - 1;
				phraseIdx++;
				continue;
			}

			const auto termOp = rawResults[termIdx].Op();
			if (termOp != OpNot) {
				for (const TextSearchResult<IdCont>& subtermRes : rawResults[termIdx]) {
					estimatedNumDocs += subtermRes.vids->size();
				}
			}

			if (termOp == OpAnd) {
				return estimatedNumDocs;
			}
		}

		const auto& vdocs = holder_.vdocs_;
		const size_t totalDocsCount = vdocs.size();

		return std::min(estimatedNumDocs, totalDocsCount);
	}

	template <typename Bm25Type>
	MergeDataType mergeSimple(TextSearchResults<IdCont>& singleTermRes, RankSortType rankSortType);
	template <typename Bm25Type>
	void mergeTermResults(TextSearchResults<IdCont>& termRes, index_t termIndex, std::vector<bool>* bitmask);

	template <typename Bm25T>
	void mergePhraseResults(size_t phraseIdx, size_t phraseBegin, OpType phraseOp, int phraseQPos);

	void addFullMatchBoost(size_t numTerms) {
		const auto& vdocs = holder_.vdocs_;
		for (size_t idx = 0; idx < mergeData_.size(); ++idx) {
			auto& md = mergeData_[idx];
			if (size_t(vdocs[md.id].wordsCount[md.field]) == numTerms) {
				md.proc *= holder_.cfg_->fullMatchBoost;
			}
		}
	}

	void postProcessResults(RankSortType rankSortType) {
		float maxProc = 0.0;
		for (auto& md : mergeData_) {
			maxProc = std::max(maxProc, md.proc);
		}

		const float scalingFactor = maxProc > 255 ? 255.0 / maxProc : 1.0;
		const float minProc = holder_.cfg_->minRelevancy * 100.0;

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

	void addDoc(int docId, index_t mergeStatus, float proc, uint8_t field) {
		InfoType info{.id = docId, .proc = proc, .field = field};

		if constexpr (kWithAreas) {
			auto& area = mergeData_.vectorAreas.emplace_back();
			area.ReserveField(fieldSize_);
			info.areaIndex = mergeData_.vectorAreas.size() - 1;
		}

		mergeData_.push_back(std::move(info));
		mergeStatuses_[docId] = mergeStatus;
		if (!idoffsets_.empty()) {
			idoffsets_[docId] = mergeData_.size() - 1;
		}
	}

	void addDoc(int docId, index_t mergeStatus, float proc, uint8_t field, IdRelType positions, int qpos, TermRankInfo& subtermInf,
				const std::wstring& pattern) {
		addDoc(docId, mergeStatus, proc, field);
		addLastDocAreas(positions, proc, subtermInf, pattern);
		mergeDataExtended_.emplace_back(std::move(positions), proc, qpos);
	}

	void addDocAreas(int docId, const IdRelType& positions, float rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithAreas) {
			auto& md = getMergeData(docId);
			addAreas(md.areaIndex, positions, rank, termInf, pattern);
		}
	}

	void addLastDocAreas(const IdRelType& positions, float rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithAreas) {
			size_t lastAreaIdx = mergeData_.vectorAreas.size() - 1;
			addAreas(lastAreaIdx, positions, rank, termInf, pattern);
		}
	}

	void addAreas(size_t areaIdx, const IdRelType& positionsInDoc, float rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithRegularAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			for (auto pos : positionsInDoc.Pos()) {
				if (!docAreas.AddWord(Area(pos.pos(), pos.pos() + 1, pos.arrayIdx()), pos.field(), rank, maxAreasInDoc_)) {
					break;
				}
			}
			docAreas.UpdateRank(rank);
		} else if constexpr (kWithDebugAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			utf16_to_utf8(pattern, const_cast<std::string&>(termInf.ftDslTerm));
			for (auto pos : positionsInDoc.Pos()) {
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

	void removeDocs(const MergeDataType& docsToRemove) {
		for (const auto& mdToRemove : docsToRemove) {
			if (reindexer::fp::IsZero(mdToRemove.proc)) {
				continue;
			}
			int docId = mdToRemove.id;
			if (mergeStatuses_[docId] != 0 && mergeStatuses_[docId] != FtMergeStatuses::kExcluded) {
				getMergeData(docId).proc = 0;
			}
			mergeStatuses_[docId] = FtMergeStatuses::kExcluded;
		}
	}

	void removeAllDocsExcept(const std::vector<bool>& docsMask) noexcept {
		for (auto& md : mergeData_) {
			if (!docsMask[md.id]) {
				mergeStatuses_[md.id] = FtMergeStatuses::kExcluded;
				md.proc = 0;
			}
		}
	}

	void calcTermBitmask(const TextSearchResults<IdCont>& termRes, BitsetType& termMask);
	void calcTermScores(TextSearchResults<IdCont>& termRes, const BitsetType* restrictingMask, BitsetType& termMask,
						std::vector<uint16_t>& docsScores);
	void collectRestrictingMask(std::vector<TextSearchResults<IdCont>>& rawResults, const std::vector<size_t>& synonymsBounds);

private:
	MergeDataType mergeData_;
	std::vector<MergerDocumentData> mergeDataExtended_;
	std::vector<MergeOffsetT> idoffsets_;
	bool hasBeenAnd_ = false;

	BitsetType restrictingMask_;

	DataHolder<IdCont>& holder_;
	size_t fieldSize_ = 0;
	int maxAreasInDoc_ = 0;

	FtMergeStatuses::Statuses& mergeStatuses_;
	std::deque<PhraseMerger<IdCont, MergeDataType, MergeOffsetT>> phraseMergers_;

	bool inTransaction_ = false;
	const RdxContext& ctx_;
};

}  // namespace reindexer
