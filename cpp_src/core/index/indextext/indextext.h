#pragma once

#include <atomic>
#include <span>
#include "core/enums.h"
#include "core/ft/config/ftconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ftctx.h"
#include "core/ft/ftdsl.h"
#include "core/ft/ftsetcashe.h"
#include "core/index/index.h"
#include "estl/algorithm.h"
#include "estl/marked_mutex.h"
#include "estl/shared_mutex.h"
#include "fieldsgetter.h"
#include "vendor/sparse-map/sparse_set.h"

namespace reindexer {

template <class StoreType>
class VDoc;

// Merge-time vdoc state: version for stale-posting filter + per-field word counts for BM25.
class [[nodiscard]] VDocMergeInfo {
public:
	VDocVersion version_ = kEmptyVDocVersion;
	VDocWordCounts wordCounts_;

	size_t heap_size() const noexcept { return wordCounts_.heap_size(); }
};

template <>
class [[nodiscard]] VDoc<key_string> {
public:
	using DataType = h_vector<key_string, 1>;

	size_t NumRows() const noexcept { return rowIds_.size(); }
	size_t NumStoredRows() const noexcept { return storedRows_; }
	bool Removed() const noexcept { return storedRows_ == 0; }
	bool Active() const noexcept { return !rowIds_.empty(); }

	const h_vector<IdType, 3>& RowIds() const& noexcept { return rowIds_; }
	auto RowIds() const&& = delete;

	DataType datas_;

	size_t heap_size() const noexcept { return rowIds_.heap_size() + datas_.heap_size(); }
	size_t strings_heap_size() const noexcept {
		size_t res = 0;
		for (auto& str : datas_) {
			res += str.heap_size();
		}

		return res;
	}

	DataType& DataRef() noexcept { return datas_; }

	void ActivateRow(IdType rowId) { rowIds_.emplace_back(rowId); }
	void ClearIndexedRows() noexcept { rowIds_.clear<false>(); }
	void DeactivateRow(IdType rowId) noexcept {
		for (size_t idx = 0; idx < rowIds_.size(); ++idx) {
			if (rowIds_[idx] == rowId) {
				std::swap(rowIds_[idx], rowIds_.back());
				rowIds_.pop_back();
				return;
			}
		}
		assertrx(false);
	}
	void RemoveRow(IdType, DataType& retired) noexcept {
		assertrx_dbg(storedRows_);
		if (--storedRows_ == 0) {
			retired.swap(datas_);
			h_vector<IdType, 3>().swap(rowIds_);
		}
	}

	void AddRow(IdType, const DataType& datas) {
		ensure_capacity_for_one_more(rowIds_);
		if (storedRows_ == 0) {
			datas_ = datas;
		}
		++storedRows_;
	}

	size_t Hash(const PayloadType& /*type_*/, const FieldsSet& /*fields_*/) const noexcept {
		size_t ret = datas_.size();
		for (auto& data : datas_) {
			ret = (ret * kHashMagic) ^ collateHash(data, CollateNone);
		}
		return ret;
	}

	bool Equal(const VDoc& other, const PayloadType& /*type_*/, const FieldsSet& /*fields_*/) const noexcept {
		if (datas_.size() != other.datas_.size()) {
			return false;
		}
		equal_key_string eq;
		for (size_t idx = 0; idx < datas_.size(); ++idx) {
			if (!eq(datas_[idx], other.datas_[idx])) {
				return false;
			}
		}

		return true;
	}

private:
	static constexpr uint64_t kHashMagic = 0x9e3779b97f4a7c15ull;
	size_t storedRows_{0};
	h_vector<IdType, 3> rowIds_;
};

template <>
class [[nodiscard]] VDoc<PayloadValue> {
public:
	size_t NumRows() const noexcept { return activeRows_; }
	size_t NumStoredRows() const noexcept { return rowIds_.size(); }
	bool Removed() const noexcept { return rowIds_.empty(); }
	bool Active() const noexcept { return activeRows_ != 0; }

	std::span<const IdType> RowIds() const& noexcept { return {rowIds_.data(), activeRows_}; }
	auto RowIds() const&& = delete;

	h_vector<PayloadValue, 1> datas_;

	size_t heap_size() const noexcept { return rowIds_.heap_size() + datas_.heap_size(); }
	size_t strings_heap_size() const noexcept { return 0; }

	PayloadValue DataRef() noexcept {
		assertrx(datas_.size() > 0);
		return datas_[0];
	}

	void RemoveRow(IdType rowId, PayloadValue& dataDetached) noexcept {
		// The caller deactivates the row before removing its storage.
		for (size_t idx = activeRows_; idx < rowIds_.size(); ++idx) {
			if (rowIds_[idx] == rowId) {
				dataDetached = std::move(datas_[idx]);
				swapRows(idx, rowIds_.size() - 1);
				rowIds_.pop_back();
				datas_.pop_back();
				if (rowIds_.empty()) {
					h_vector<IdType, 3>().swap(rowIds_);
					h_vector<PayloadValue, 1>().swap(datas_);
				}
				return;
			}
		}
		assertrx(false);
	}
	void ActivateRow(IdType rowId) noexcept {
		// Newly added rows are at the back, so their activation needs no scan.
		for (size_t idx = rowIds_.size(); idx > activeRows_;) {
			if (rowIds_[--idx] == rowId) {
				swapRows(idx, activeRows_++);
				return;
			}
		}
		assertrx(false);
	}
	void DeactivateRow(IdType rowId) noexcept {
		for (size_t idx = 0; idx < activeRows_; ++idx) {
			if (rowIds_[idx] == rowId) {
				swapRows(idx, --activeRows_);
				return;
			}
		}
		assertrx(false);
	}

	template <typename Predicate>
	void UpdateIndexedRows(Predicate&& matches) {
		activeRows_ = 0;
		for (size_t idx = 0; idx < rowIds_.size(); ++idx) {
			if (matches(rowIds_[idx])) {
				swapRows(idx, activeRows_++);
			}
		}
	}

	void UpdateRowData(IdType rowId, const PayloadValue& data) noexcept {
		for (size_t idx = 0; idx < rowIds_.size(); ++idx) {
			if (rowIds_[idx] == rowId) {
				datas_[idx] = data;
				return;
			}
		}
		assertrx(false);
	}

	void AddRow(IdType rowId, const PayloadValue& data) {
		ensure_capacity_for_one_more(rowIds_);
		ensure_capacity_for_one_more(datas_);
		datas_.push_back(data);
		rowIds_.push_back(rowId);
	}

	size_t Hash(const PayloadType&, const FieldsSet&) const noexcept;
	bool Equal(const VDoc&, const PayloadType&, const FieldsSet&) const noexcept;

private:
	void swapRows(size_t lhs, size_t rhs) noexcept {
		if (lhs != rhs) {
			std::swap(rowIds_[lhs], rowIds_[rhs]);
			std::swap(datas_[lhs], datas_[rhs]);
		}
	}

	// IDs and payloads have the same order; only the prefix passes indexing_condition.
	h_vector<IdType, 3> rowIds_;
	uint32_t activeRows_{0};
};

template <typename StoreType>
class [[nodiscard]] IndexText : public Index {
	using Base = Index;

public:
	IndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg);

	bool HoldsStrings() const noexcept override { return std::is_same_v<StoreType, key_string>; }

	bool RefreshCompositeKey(const Variant& key, IdType id) noexcept override;
#if REINDEX_WITH_STRHOLDER_CHECKS
	void CollectOwnedStringPtrs(OwnedStringPtrCollector& collector) const override;
#endif	// REINDEX_WITH_STRHOLDER_CHECKS
	SelectKeyResults SelectKey(const VariantArray& keys, CondType, SortType, const Index::SelectContext&, const RdxContext&) override final;
	SelectKeyResults SelectKey(const VariantArray& keys, CondType, const Index::SelectContext&, FtPreselectT&&, const RdxContext&) override;

	WasCanceled UpdateSortedIds(const index::IUpdateSortedContext&, const index::ICancelable&) override {
		assertrx_dbg(!IsSupportSortedIdsBuild());
		return WasCanceled_False;
	}
	bool IsSupportSortedIdsBuild() const noexcept override { return false; }

	WasCanceled Commit(const index::ICancelable&) override final {
		// Do nothing
		// Rebuild will be done on first select
		return WasCanceled_False;
	}
	bool NeedsClean() const noexcept override final;
	void Clean(const index::ICancelable& cancelable, bool enablePerfCounters) override final;

	void CommitFulltext() override final {
		cache_ft_.Reinitialize(cacheMaxSize_, hitsToCache_);
		commitFulltextImpl();
		this->isBuilt_ = true;
	}
	void SetSortedIdxCount(unsigned) override final {}
	void DestroyCache() override {
		Base::DestroyCache();
		cache_ft_.ResetImpl();
	}
	void ClearCache() override {
		Base::ClearCache();
		cache_ft_.Clear();
	}
	void MarkBuilt() noexcept override { assertrx(0); }
	bool IsFulltext() const noexcept override final { return true; }
	void ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) override final;

	IndexPerfStat GetIndexPerfStat() override final {
		auto stats = Base::GetIndexPerfStat();
		stats.cache = cache_ft_.GetPerfStat();
		return stats;
	}

	void ResetIndexPerfStat() override final {
		Base::ResetIndexPerfStat();
		cache_ft_.ResetPerfStat();
	}

	QueryRankType RankedType() const noexcept override final { return QueryRankType::FullText; }

	std::unique_ptr<Index> Clone(size_t /*newCapacity*/, IndexCloneKind kind) const override {
		return std::unique_ptr<Index>(new IndexText<StoreType>(*this, kind));
	}

	IdSetPlain::Ptr Select(FtCtx&, FtDSLQuery&& dsl, bool inTransaction, RankSortType, FtMergeStatuses&&, FtUseExternStatuses,
						   const RdxContext&);

	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override;
	void Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) override;

	void Delete(const Variant& key, IdType id, MustExist mustExist, StringsHolder&, bool& clearCache) override;
	void Delete(const VariantArray& keys, IdType id, MustExist mustExist, StringsHolder& strHolder, bool& clearCache) override;
	bool Reconcile(VariantArray& result, const VariantArray& keys, IdType id, MatchesCondition, StringsHolder&, bool& clearCache) override;
	void Reconcile(const Variant& key, IdType id, MatchesCondition, StringsHolder&, bool& clearCache) override;
	std::unique_ptr<Index> PrepareIndexingConditionUpdate(std::span<const MatchesCondition> verdicts) const override;
	void CommitIndexingConditionUpdate(Index& source) noexcept override;

	const void* ColumnData() const noexcept override final { return nullptr; }

	void SetOpts(const IndexOpts& opts) override;
	reindexer::FtPreselectT FtPreselect(const RdxContext& rdxCtx) override {
		this->build(rdxCtx);
		return FtMergeStatuses{.docsExcluded = {}, .rowIds = {}, .rowId2VdocId = &indexedRowId2VdocId_, .vdocsCount = vdocs_.size()};
	}
	bool EnablePreselectBeforeFt() const override { return cfg_->enablePreselectBeforeFt; }

	IndexMemStat GetMemStat(const RdxContext& ctx) const override;

	bool IsDeleted(uint32_t vdocId, VDocVersion version) const noexcept {
		if (vdocId == kEmptyVDocId) {
			return true;
		}
		if (vdocId >= vdocMergeInfo_.size()) {
			return true;
		}
		if (vdocMergeInfo_[vdocId].version_ != version) {
			return true;
		}
		assertrx_dbg(vdocs_[vdocId].Active());
		return false;
	}
	template <typename OccurenceT>
	bool IsDeleted(const OccurenceT& occ) const noexcept {
		return IsDeleted(occ.VdocId(), occ.VdocVersion());
	}
	uint32_t NumWordsInField(uint32_t vdocId, uint32_t fieldIdx) const noexcept {
		assertrx_dbg(vdocId < vdocs_.size());
		assertrx_dbg(fieldIdx < vdocMergeInfo_[vdocId].wordCounts_.size());
		return vdocMergeInfo_[vdocId].wordCounts_[fieldIdx];
	}
	size_t NumLiveDocs() const noexcept { return activeVdocs_; }
	uint64_t NumDeletedDocs() const noexcept { return numDeletedDocs_; }

	float AvgWordsCount(uint32_t fieldIdx) const noexcept {
		const size_t n = NumLiveDocs();
		if (n == 0) {
			return 0.0f;
		}
		assertrx_dbg(fieldIdx < sumWordsCount_.size());
		return float(sumWordsCount_[fieldIdx]) / float(n);
	}

protected:
	void upsertConditional(VariantArray& result, const VariantArray& keys, IdType id, MatchesCondition, bool& clearCache) override;

private:
	class [[nodiscard]] hash_vdoc {
	public:
		hash_vdoc(const PayloadType& type, const FieldsSet& fields, const std::vector<VDoc<StoreType>>& vdocs)
			: type_(&type), fields_(&fields), vdocs_(&vdocs) {}
		hash_vdoc(const hash_vdoc& hc) = default;
		hash_vdoc(hash_vdoc&& hc) noexcept = default;
		hash_vdoc& operator=(const hash_vdoc& hc) = default;
		hash_vdoc& operator=(hash_vdoc&& hc) = default;

		size_t operator()(uint32_t pos) const noexcept { return (*vdocs_)[pos].Hash(*type_, *fields_); }
		size_t operator()(const VDoc<StoreType>& vdoc) const noexcept { return vdoc.Hash(*type_, *fields_); }

	private:
		const PayloadType* type_;
		const FieldsSet* fields_;
		const std::vector<VDoc<StoreType>>* vdocs_;
	};

	class [[nodiscard]] equal_vdoc {
	public:
		using is_transparent = void;

		equal_vdoc(const PayloadType& type, const FieldsSet& fields, const std::vector<VDoc<StoreType>>& vdocs)
			: type_(&type), fields_(&fields), vdocs_(&vdocs) {}

		bool operator()(uint32_t l, uint32_t r) const {
			if (l == r) {
				return true;
			}

			return (*vdocs_)[l].Equal((*vdocs_)[r], *type_, *fields_);
		}
		bool operator()(const VDoc<StoreType>& l, uint32_t r) const { return l.Equal((*vdocs_)[r], *type_, *fields_); }

	private:
		const PayloadType* type_;
		const FieldsSet* fields_;
		const std::vector<VDoc<StoreType>>* vdocs_;
	};

	using VDocSetType = tsl::sparse_set<uint32_t, hash_vdoc, equal_vdoc, std::allocator<uint32_t>, tsl::sh::power_of_two_growth_policy<2>,
										tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

	static_assert(kEmptyVDocId == FtMergeStatuses::kEmpty);

	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::IndexText>;

	IndexText(const IndexText<StoreType>& other, IndexCloneKind kind);
	SelectKeyResults doSelectKey(std::string_view key, FtDSLQuery&&, std::optional<IdSetCacheKey>&&, FtMergeStatuses&&,
								 FtUseExternStatuses useExternSt, bool inTransaction, RankSortType, FtCtx&, const RdxContext&);

	SelectKeyResults resultFromCache(std::string_view key, FtIdSetCache::Iterator&&, FtCtx&, RanksHolder::Ptr&);
	void build(const RdxContext& rdxCtx);

	void initSearchers();

	template <typename MergeType>
	IdSetPlain::Ptr afterSelect(FtCtx& fctx, MergeType&& mergeData, RankSortType, FtMergeStatuses&& statuses, FtUseExternStatuses);

	template <auto(RanksHolder::* rankGetter)>
	void sortAfterSelect(IdSetPlain& mergedIds, RanksHolder&, RankSortType);

	template <typename VectorType>
	IdSetPlain::Ptr applyCtxTypeAndSelect(DataHolder<VectorType>* d, FtCtx&, FtDSLQuery&& dsl, bool inTransaction, RankSortType,
										  FtMergeStatuses&& statuses, FtUseExternStatuses useExternSt, const RdxContext& rdxCtx);

	void commitFulltextImpl();
	void initConfig(const FTConfig* = nullptr);
	void initHolder(FTConfig&);
	void initTermBoosts(FTConfig&);
	void clearInvertedIndex() {
		holder_->Clear();
		deletedDocsSinceOptimization_ = 0;
	}

	uint32_t getVdocId(IdType rowId) const {
		return static_cast<size_t>(rowId.ToNumber()) < rowId2VdocId_.size() ? rowId2VdocId_[rowId.ToNumber()] : kEmptyVDocId;
	}
	bool isRowIndexed(IdType rowId) const noexcept {
		return static_cast<size_t>(rowId.ToNumber()) < indexedRowId2VdocId_.size() &&
			   indexedRowId2VdocId_[rowId.ToNumber()] != kEmptyVDocId;
	}
	void setRowIndexed(IdType rowId, MatchesCondition matches, bool& clearCache);
	void updateVdocActivity(uint32_t vdocId, bool wasActive);
	void invalidateVdoc(uint32_t vdocId);
	void resetSearch();
	void upsertRow(VariantArray& result, const VariantArray& keys, IdType id, MatchesCondition, bool& clearCache);
	void upsertRowData(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache);
	void clearVdocSlot(uint32_t vdocId) {
		vdocs_[vdocId] = {};
		vdocMergeInfoHeapSize_ -= vdocMergeInfo_[vdocId].heap_size();
		// Swap also resets the capacity reported by pedantic debug vectors after move assignment.
		VDocWordCounts{}.swap(vdocMergeInfo_[vdocId].wordCounts_);
	}
	void retireVdocSlot(uint32_t vdocId) {
		clearVdocSlot(vdocId);
		freeVdocIds_.push_back(vdocId);
	}

	uint32_t appendVdocSlot() {
		assertrx_dbg(vdocs_.size() == vdocMergeInfo_.size());
		ensure_capacity_for_one_more(vdocs_);
		ensure_capacity_for_one_more(vdocMergeInfo_);
		vdocs_.resize(vdocs_.size() + 1);
		vdocMergeInfo_.resize(vdocMergeInfo_.size() + 1);
		return uint32_t(vdocs_.size() - 1);
	}

	void addToWordCountSum(const VDocWordCounts& counts) {
		if (counts.empty()) {
			return;
		}
		if (sumWordsCount_.size() < counts.size()) {
			sumWordsCount_.resize(counts.size(), 0);
		}
		for (size_t i = 0; i < counts.size(); ++i) {
			sumWordsCount_[i] += counts[i];
		}
	}
	void subFromWordCountSum(const VDocWordCounts& counts) {
		if (counts.empty()) {
			return;
		}
		assertrx_dbg(sumWordsCount_.size() >= counts.size());
		for (size_t i = 0; i < counts.size(); ++i) {
			sumWordsCount_[i] -= counts[i];
		}
	}

	template <typename DataType>
	void excludeFromVdoc(IdType rowId, DataType& dataDetached);

	template <typename DataType>
	uint32_t setVdocId(IdType rowId, const DataType& data);

	std::unique_ptr<IDataHolder> holder_;

	FtIdSetCache cache_ft_;
	size_t cacheMaxSize_;
	uint32_t hitsToCache_;

	RHashMap<std::string, FtIndexFieldPros> ftFields_;
	std::unique_ptr<FTConfig> cfg_;
	mutable Mutex mtx_;
	// Bumped by build() before Unique on mtx_ so background Clean (NonUnique) yields for incremental commit.
	mutable std::atomic_int32_t cancelCleanCnt_{0};

	// Storage includes excluded rows; preselection sees only the searchable mapping.
	std::vector<uint32_t> rowId2VdocId_;
	std::vector<uint32_t> indexedRowId2VdocId_;
	size_t activeVdocs_{0};
	size_t vdocsHeapSize_ = 0;

	size_t stringsHeapSize_ = 0;

	std::vector<VDoc<StoreType>> vdocs_;
	std::vector<VDocMergeInfo> vdocMergeInfo_;
	size_t vdocMergeInfoHeapSize_ = 0;
	std::vector<uint32_t> freeVdocIds_;
	std::vector<VDocPosting> unindexedVdocs_;
	std::vector<uint64_t> sumWordsCount_;
	size_t deletedDocsSinceOptimization_ = 0;
	uint64_t numDeletedDocs_ = 0;  // Monotonic across cleanup sweeps and full rebuilds.
	// A condition-update clone can reuse source postings unless it resets VDoc versions.
	bool reuseSourceHolder_{false};
	VDocSetType vdocSet_;
};

std::unique_ptr<Index> IndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									 const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
