#pragma once

#include "core/enums.h"
#include "core/ft/config/ftconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ftctx.h"
#include "core/ft/ftdsl.h"
#include "core/ft/ftsetcashe.h"
#include "core/index/index.h"
#include "estl/marked_mutex.h"
#include "estl/shared_mutex.h"
#include "fieldsgetter.h"

namespace reindexer {

template <class StoreType>
class VDoc;

class [[nodiscard]] VDocBase {
public:
	h_vector<IdType, 1> rowIds_;
	h_vector<float, 3> wordCounts_;

	size_t NumRows() const noexcept { return rowIds_.size(); }
	bool Removed() const noexcept { return NumRows() == 0; }

	const h_vector<IdType, 1>& RowIds() const noexcept { return rowIds_; }

protected:
	constexpr static uint64_t kHashMagic = 0x9e3779b97f4a7c15ull;
};

template <>
class [[nodiscard]] VDoc<key_string> : public VDocBase {
public:
	using DataType = h_vector<key_string, 1>;

	DataType datas_;

	size_t heap_size() const noexcept { return rowIds_.heap_size() + datas_.heap_size() + wordCounts_.heap_size(); }
	size_t strings_heap_size() const noexcept {
		size_t res = 0;
		for (auto& str : datas_) {
			res += str.heap_size();
		}

		return res;
	}

	DataType& DataRef() noexcept { return datas_; }

	void RemoveRow(IdType rowId, DataType& dataDetached) {
		for (size_t idx = 0; idx < rowIds_.size(); ++idx) {
			if (rowIds_[idx] == rowId) {
				std::swap(rowIds_[idx], rowIds_.back());
				rowIds_.pop_back();
				if (rowIds_.empty()) {
					dataDetached.swap(datas_);
					datas_.clear();
					rowIds_.shrink_to_fit();
					wordCounts_.clear();
				} else {
					dataDetached = datas_;
				}

				return;
			}
		}
		assertrx(false);
	}

	void AddRow(IdType rowId, const DataType& datas) {
		if (rowIds_.empty()) {
			datas_ = datas;
		}
		rowIds_.emplace_back(rowId);
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
};

template <>
class [[nodiscard]] VDoc<PayloadValue> : public VDocBase {
public:
	h_vector<PayloadValue, 1> datas_;

	size_t heap_size() const noexcept { return rowIds_.heap_size() + datas_.heap_size() + wordCounts_.heap_size(); }
	size_t strings_heap_size() const noexcept { return 0; }

	PayloadValue DataRef() noexcept {
		assertrx(datas_.size() > 0);
		return datas_[0];
	}

	void RemoveRow(IdType rowId, PayloadValue& dataDetached) {
		for (size_t idx = 0; idx < rowIds_.size(); ++idx) {
			if (rowIds_[idx] == rowId) {
				dataDetached = std::move(datas_[idx]);
				std::swap(rowIds_[idx], rowIds_.back());
				rowIds_.pop_back();
				std::swap(datas_[idx], datas_.back());
				datas_.pop_back();
				return;
			}
		}
		assertrx(false);
	}

	void UpdateRowData(IdType rowId, const PayloadValue& data) {
		PayloadValue res;
		for (size_t idx = 0; idx < rowIds_.size(); ++idx) {
			if (rowIds_[idx] == rowId) {
				datas_[idx] = data;
				return;
			}
		}
		assertrx(false);
	}

	void AddRow(IdType rowId, const PayloadValue& data) {
		rowIds_.push_back(rowId);
		datas_.push_back(data);
	}

	size_t Hash(const PayloadType& type_, const FieldsSet& fields_) const noexcept {
		if (datas_.empty()) {
			return 0;
		}
		return ConstPayload(type_, datas_[0]).GetHash(fields_);
	}

	bool Equal(const VDoc& other, const PayloadType& type_, const FieldsSet& fields_) const noexcept {
		if (datas_.empty()) {
			return other.datas_.empty();
		}

		return ConstPayload(type_, datas_[0]).IsEQ(other.datas_[0], fields_);
	}
};

template <typename StoreType>
class [[nodiscard]] IndexText : public Index {
	using Base = Index;

public:
	IndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg);

	bool HoldsStrings() const noexcept override { return std::is_same_v<StoreType, key_string>; }

	bool RefreshCompositeKey(const Variant& key, IdType id) noexcept override;
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

	const void* ColumnData() const noexcept override final { return nullptr; }

	void SetOpts(const IndexOpts& opts) override;
	FtMergeStatuses GetFtMergeStatuses(const RdxContext& rdxCtx) override {
		this->build(rdxCtx);
		return {FtMergeStatuses::Statuses(vdocs_.size(), false), std::vector<bool>(rowId2Vdoc_.size(), false), &rowId2Vdoc_};
	}
	reindexer::FtPreselectT FtPreselect(const RdxContext& rdxCtx) override {
		this->build(rdxCtx);
		return FtMergeStatuses{FtMergeStatuses::Statuses(vdocs_.size(), true), std::vector<bool>(rowId2Vdoc_.size(), false), &rowId2Vdoc_};
	}
	bool EnablePreselectBeforeFt() const override { return cfg_->enablePreselectBeforeFt; }

	IndexMemStat GetMemStat(const RdxContext& ctx) const override;

	bool DocRemoved(uint32_t vdocId) const noexcept {
		assertrx_dbg(vdocId < vdocs_.size());
		return vdocs_[vdocId].Removed();
	}
	size_t NumWordsInField(uint32_t vdocId, uint32_t fieldIdx) const noexcept {
		assertrx_dbg(vdocId < vdocs_.size());
		assertrx_dbg(fieldIdx < vdocs_[vdocId].wordCounts_.size());
		return vdocs_[vdocId].wordCounts_[fieldIdx];
	}

	float AvgWordsCount(uint32_t fieldIdx) const noexcept {
		assertrx_dbg(fieldIdx < avgWordsCount_.size());
		return avgWordsCount_[fieldIdx];
	}

private:
	class [[nodiscard]] hash_vdoc {
	public:
		hash_vdoc(const PayloadType& type, const FieldsSet& fields, const std::vector<VDoc<StoreType>>& vdocs)
			: type_(&type), fields_(&fields), vdocs_(&vdocs) {}
		hash_vdoc(const hash_vdoc& hc) = default;
		hash_vdoc(hash_vdoc&& hc) noexcept = default;
		hash_vdoc& operator=(const hash_vdoc& hc) = default;
		hash_vdoc& operator=(hash_vdoc&& hc) = default;

		size_t operator()(uint32_t vdocId) const noexcept { return (*vdocs_)[vdocId].Hash(*type_, *fields_); }

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

	private:
		const PayloadType* type_;
		const FieldsSet* fields_;
		const std::vector<VDoc<StoreType>>* vdocs_;
	};

	using VDocSetType = tsl::sparse_set<uint32_t, hash_vdoc, equal_vdoc, std::allocator<uint32_t>, tsl::sh::power_of_two_growth_policy<2>,
										tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

	static constexpr uint32_t kEmptyVDocId = 0;

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

	void cleanRemovedVdocs();
	void commitFulltextImpl();
	void initConfig(const FTConfig* = nullptr);
	void initHolder(FTConfig&);
	void initTermBoosts(FTConfig&);

	uint32_t getVdocId(IdType rowId) const {
		return static_cast<size_t>(rowId.ToNumber()) < rowId2Vdoc_.size() ? rowId2Vdoc_[rowId.ToNumber()] : kEmptyVDocId;
	}

	template <typename DataType>
	void excludeFromVdoc(IdType rowId, DataType& dataDetached);

	template <typename DataType>
	bool setVdocId(IdType rowId, const DataType& data);

	std::unique_ptr<IDataHolder> holder_;

	FtIdSetCache cache_ft_;
	size_t cacheMaxSize_;
	uint32_t hitsToCache_;

	RHashMap<std::string, FtIndexFieldPros> ftFields_;
	std::unique_ptr<FTConfig> cfg_;
	mutable Mutex mtx_;

	std::vector<uint32_t> rowId2Vdoc_;
	size_t vdocsHeapSize_ = 0;

	uint32_t vdocsCommited_ = 0;
	uint32_t vdocsIndexed_ = 0;

	size_t stringsHeapSize_ = 0;

	std::vector<VDoc<StoreType>> vdocs_;
	std::vector<double> avgWordsCount_;
	VDocSetType vdocSet_;
};

std::unique_ptr<Index> IndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									 const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
