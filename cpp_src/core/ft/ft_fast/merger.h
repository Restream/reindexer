#pragma once
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>
#include "estl/id_offset_hash.h"
#include "estl/lazy_deque.h"
#include "phrasemerger.h"
#include "querymergedata.h"

namespace reindexer {

namespace ft {

template <typename T, size_t Alignment>
class [[nodiscard]] OverallocatingAlignedAllocator {
	static_assert((Alignment & (Alignment - 1)) == 0);
	static_assert(Alignment >= alignof(T));

public:
	using value_type = T;
	using size_type = size_t;
	using is_always_equal = std::true_type;

	template <typename U>
	struct [[nodiscard]] rebind {
		using other = OverallocatingAlignedAllocator<U, Alignment>;
	};

	OverallocatingAlignedAllocator() noexcept = default;

	template <typename U>
	OverallocatingAlignedAllocator(const OverallocatingAlignedAllocator<U, Alignment>&) noexcept {}

	[[nodiscard]] T* allocate(size_t count) {
		if (count == 0) {
			return nullptr;
		}

		constexpr size_t kOverhead = Alignment - 1 + sizeof(void*);
		if (count > (std::numeric_limits<size_t>::max() - kOverhead) / sizeof(T)) [[unlikely]] {
			throw std::bad_array_new_length();
		}

		void* allocation = ::operator new(count * sizeof(T) + kOverhead);
		const uintptr_t begin = reinterpret_cast<uintptr_t>(allocation) + sizeof(void*);
		const uintptr_t aligned = (begin + Alignment - 1) & ~(Alignment - 1);
		reinterpret_cast<void**>(aligned)[-1] = allocation;
		return reinterpret_cast<T*>(aligned);
	}

	void deallocate(T* ptr, size_t) noexcept {
		if (ptr) {
			::operator delete(reinterpret_cast<void**>(ptr)[-1]);
		}
	}

	template <typename U>
	bool operator==(const OverallocatingAlignedAllocator<U, Alignment>&) const noexcept {
		return true;
	}
};

// Complete document state used while merging queries with 2 or more terms.
// The compact result type is extracted before result post-processing and sorting.
struct [[nodiscard]] MergerDocumentData {
	static constexpr uint16_t kMaxMergedPart = (1U << 15) - 1;

	MergerDocumentData() noexcept = default;
	MergerDocumentData(MergerDocumentData&&) noexcept = default;
	MergerDocumentData& operator=(MergerDocumentData&&) noexcept = default;
	MergerDocumentData& operator=(const MergerDocumentData&) = default;

	PositionsVector lastTermPositions;	// Positions of the previous matched part, used for distance ranking
	PositionsVector nextTermPositions;	// Best positions of the current part, promoted lazily on the next match
	float rank = 0;						// Best term contribution for the current query part
	float proc = 0;						// Accumulated document rank before normalization
	IdType id = IdType::Zero();			// Virtual document ID in the fulltext index

	uint16_t lastMergedPart : 15 = 0;				   // Last matched query part; 0 before the first match
	uint16_t notAllPartsPresent : 1 = false;		   // A part was skipped within the current query/synonym group
	uint8_t field : 6 = 0;							   // Field selected when the document entered merge
	uint8_t containsFullMultiWordSynonym : 1 = false;  // At least one complete multiword synonym matched
	uint8_t canBeBoostedByFullMatch : 1 = false;	   // All original query parts matched; field length is checked separately

	void SetField(uint8_t value) noexcept {
		assertrx_dbg(value < (1U << 6));
		field = value;
	}

	// Prepare positions for a new query part. The previous matched part remains the distance
	// anchor even if intermediate parts were missed. Repeated subterms of one part are a no-op.
	void PrepareForPart(uint16_t partIdx, uint16_t groupBegin) noexcept {
		assertrx_dbg(partIdx >= groupBegin);
		assertrx_dbg(partIdx >= lastMergedPart);
		if (lastMergedPart == partIdx) {
			return;
		}

		const uint16_t expectedGroupPart = lastMergedPart < groupBegin ? groupBegin : uint16_t(lastMergedPart + 1);
		if (partIdx != expectedGroupPart) {
			notAllPartsPresent = true;
		}

		if (!nextTermPositions.empty()) {
			lastTermPositions.swap(nextTermPositions);
			nextTermPositions.clear();
		}
		rank = 0;
		lastMergedPart = partIdx;
	}
};

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
class [[nodiscard]] Merger {
public:
	constexpr static bool kWithRegularAreas = std::is_same_v<MergeDataType, MergeDataAreas<Area>>;
	constexpr static bool kWithDebugAreas = std::is_same_v<MergeDataType, MergeDataAreas<AreaDebug>>;
	constexpr static bool kWithAreas = kWithRegularAreas || kWithDebugAreas;

	using InfoType = typename MergeDataType::InfoType;
	using DocumentData = MergerDocumentData;
	static constexpr size_t kDocumentAlignment = 64;
	using DocumentVector = std::vector<DocumentData, OverallocatingAlignedAllocator<DocumentData, kDocumentAlignment>>;
	static constexpr MergeOffsetT kNotInMerge = IdOffsetHash<VDocId, MergeOffsetT>::kEmpty;

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
	MergeOffsetT& findOffsetSlot(VDocId vdocId) noexcept {
		assertrx_dbg(useIdoffsets_);
		return idoffsets_.FindSlot(vdocId);
	}

	template <typename Bm25Type, typename DocsStatsGetter>
	void init(QueryMergeData<IdCont>& queryMergeData, uint32_t maxMergedSize, const DocsStatsGetter& docsStatsGetter) {
		maxMergedDocs_ = std::min<uint32_t>(maxMergedSize, std::numeric_limits<MergeOffsetT>::max());
		maxMergedDocs_ = std::min(maxMergedSize, cfg_->mergeLimit);

		if (!queryMergeData.Trivial()) {
			useIdoffsets_ = true;
			idoffsets_.Init(maxMergedDocs_, totalNumDocs_);
		}

		if (!queryMergeData.Simple()) {
			mergedDocs_.reserve(maxMergedDocs_);
			assertrx_dbg(reinterpret_cast<uintptr_t>(mergedDocs_.data()) % kDocumentAlignment == 0);

			if constexpr (kWithAreas) {
				mergeData_.vectorAreas.reserve(maxMergedDocs_);
			}
		} else {
			mergeData_.reserve(maxMergedDocs_);
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

	// FullMatchBoost when field word count equals query term count.
	// See fulltext_ranking.md#full-match-boost
	template <typename DocsStatsGetter>
	void addFullMatchBoost(size_t numTerms, const DocsStatsGetter& docsStatsGetter) {
		for (auto& md : mergedDocs_) {
			if (md.canBeBoostedByFullMatch && docsStatsGetter.NumWordsInField(md.id.ToNumber(), md.field) == numTerms) {
				md.proc *= cfg_->fullMatchBoost;
			}
		}
	}

	template <typename DocsStatsGetter>
	void addSimpleFullMatchBoost(const DocsStatsGetter& docsStatsGetter) {
		for (auto& md : mergeData_) {
			if (docsStatsGetter.NumWordsInField(md.id.ToNumber(), md.field) == 1) {
				md.proc *= cfg_->fullMatchBoost;
			}
		}
	}

	void extractMergeData() {
		mergeData_.reserve(mergedDocs_.size());
		for (auto& md : mergedDocs_) {
			InfoType info{.id = md.id, .proc = md.proc, .field = md.field};
			if constexpr (kWithAreas) {
				info.areaIndex = uint32_t(mergeData_.size());
			}
			mergeData_.emplace_back(std::move(info));
		}
		DocumentVector().swap(mergedDocs_);
	}

	// MinRank filter, scale to 0-255, sort by rank.
	// See fulltext_ranking.md#normalization-and-filtering
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

	size_t numMergedDocs() const noexcept { return mergedDocs_.size(); }

	void addSimpleDoc(VDocId vdocId, float proc, uint8_t field, MergeOffsetT* offsetSlot) {
		InfoType info{.id = IdType::FromNumber(vdocId), .proc = proc, .field = field};

		if constexpr (kWithAreas) {
			auto& area = mergeData_.vectorAreas.emplace_back();
			area.ReserveField(fieldSize_);
			info.areaIndex = mergeData_.vectorAreas.size() - 1;
		}

		mergeData_.push_back(std::move(info));
		if (offsetSlot) {
			assertrx_dbg(*offsetSlot == kNotInMerge);
			*offsetSlot = MergeOffsetT(mergeData_.size() - 1);
		}
	}

	DocumentData& addMergedDoc(VDocId vdocId, float proc, uint8_t field, MergeOffsetT& offsetSlot) {
		auto& md = mergedDocs_.emplace_back();
		md.id = IdType::FromNumber(vdocId);
		md.proc = proc;
		md.SetField(field);

		if constexpr (kWithAreas) {
			auto& area = mergeData_.vectorAreas.emplace_back();
			area.ReserveField(fieldSize_);
		}

		assertrx_dbg(offsetSlot == kNotInMerge);
		offsetSlot = MergeOffsetT(mergedDocs_.size() - 1);
		return md;
	}

	void addMergedDoc(VDocId vdocId, float proc, uint8_t field, PositionsVector&& positions, TermRankInfo& subtermInf,
					  const std::u16string& pattern, uint16_t partIdx, MergeOffsetT& offsetSlot) {
		auto& md = addMergedDoc(vdocId, proc, field, offsetSlot);
		addLastDocAreas(positions, proc, subtermInf, pattern);
		md.PrepareForPart(partIdx, partsGroupBegin_);
		md.nextTermPositions = std::move(positions);
		md.rank = proc;
	}

	void addMergedDoc(VDocId vdocId, float proc, uint8_t field, PosType position, TermRankInfo& subtermInf, const std::u16string& pattern,
					  uint16_t partIdx, MergeOffsetT& offsetSlot) {
		auto& md = addMergedDoc(vdocId, proc, field, offsetSlot);
		addLastDocAreas(position, proc, subtermInf, pattern);
		md.PrepareForPart(partIdx, partsGroupBegin_);
		md.nextTermPositions.emplace_back(position);
		md.rank = proc;
	}

	void addLastDocAreas(const PositionsVector& positions, float rank, TermRankInfo& termInf, const std::u16string& pattern) {
		if constexpr (kWithAreas) {
			size_t lastAreaIdx = mergeData_.vectorAreas.size() - 1;
			addAreas(lastAreaIdx, positions, rank, termInf, pattern);
		}
	}

	void addLastDocAreas(PosType position, float rank, TermRankInfo& termInf, const std::u16string& pattern) {
		if constexpr (kWithAreas) {
			size_t lastAreaIdx = mergeData_.vectorAreas.size() - 1;
			addAreas(lastAreaIdx, position, rank, termInf, pattern);
		}
	}

	void addAreas(size_t areaIdx, PosType pos, float rank, TermRankInfo& termInf, const std::u16string& pattern) {
		if constexpr (kWithRegularAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			std::ignore = docAreas.AddWord(Area(pos.pos(), pos.pos() + 1, pos.arrayIdx()), pos.field(), rank, maxAreasInDoc_);
			docAreas.UpdateRank(rank);
		} else if constexpr (kWithDebugAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			utf16_to_utf8(pattern, const_cast<std::string&>(termInf.ftDslTerm));
			std::ignore =
				docAreas.AddWord(AreaDebug(pos.pos(), pos.pos() + 1, pos.arrayIdx(), termInf.ToString(), AreaDebug::PhraseMode::None),
								 pos.field(), termInf.termRank, -1);
			docAreas.UpdateRank(termInf.termRank);
		}
	}

	void addAreas(size_t areaIdx, const PositionsVector& positions, float rank, TermRankInfo& termInf, const std::u16string& pattern) {
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

	uint16_t advancePart() {
		if (currentPart_ == DocumentData::kMaxMergedPart) [[unlikely]] {
			throw Error(errLogic, "Fulltext merge exceeded max term steps ({})", DocumentData::kMaxMergedPart);
		}
		return ++currentPart_;
	}

	void beginPartsGroup() {
		if (currentPart_ == DocumentData::kMaxMergedPart) [[unlikely]] {
			throw Error(errLogic, "Fulltext merge exceeded max term steps ({})", DocumentData::kMaxMergedPart);
		}
		partsGroupBegin_ = uint16_t(currentPart_ + 1);
	}

	template <typename DocsStatsGetter>
	void calcTermBitmask(const TermResults<IdCont>& term, BitsetType& termMask, const DocsStatsGetter& docsStatsGetter);
	template <typename DocsStatsGetter>
	void excludeTermFromBitmask(const TermResults<IdCont>& term, BitsetType& mask, const DocsStatsGetter& docsStatsGetter);

	template <typename DocsStatsGetter>
	void calcTermScores(TermResults<IdCont>& term, const BitsetType& restrictingMask, BitsetType& termMask,
						std::vector<uint16_t>& docsScore, const DocsStatsGetter& docsStatsGetter);

	template <typename DocsStatsGetter>
	void buildRestrictingBitmask(QueryMergeData<IdCont>& queryData, const DocsStatsGetter& docsStatsGetter);

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
	DocumentVector mergedDocs_;
	uint32_t maxMergedDocs_ = 0;
	IdOffsetHash<VDocId, MergeOffsetT> idoffsets_;
	bool useIdoffsets_ = false;
	BitsetType restrictingMask_;
	uint16_t currentPart_ = 0;
	uint16_t partsGroupBegin_ = 1;

	bool inTransaction_ = false;
	const RdxContext& ctx_;

	size_t totalNumDocs_ = 0;
	FTConfig* cfg_ = nullptr;
};

}  // namespace ft
}  // namespace reindexer
