#pragma once

#if RX_WITH_BUILTIN_ANN_INDEXES

#include "float_vector_index.h"
#include "hnswlib/hnswlib.h"

namespace reindexer {

class HnswKnnRawResult;

template <typename Map>
class [[nodiscard]] HnswIndexBase final : public FloatVectorIndex {
	using Base = FloatVectorIndex;
	using FloatType = float;

public:
	HnswIndexBase(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);

	void Delete(const Variant& key, IdType id, MustExist mustExist, StringsHolder&, bool& clearCache) override;
	using FloatVectorIndex::Delete;

	std::unique_ptr<Index> Clone(size_t newCapacity) const override;
	IndexMemStat GetMemStat(const RdxContext&) noexcept override;
	bool IsSupportMultithreadTransactions() const noexcept override { return std::is_same_v<Map, hnswlib::HierarchicalNSWMT>; }
	void GrowFor(size_t newElementsCount) override;
	StorageCacheWriteResult WriteIndexCache(WrSerializer&, PKGetterF&&, bool isCompositePK,
											const std::atomic_int32_t& cancel) noexcept override;
	Error LoadIndexCache(std::string_view data, bool isCompositePK, VecDataGetterF&& getVectorData) override;

private:
	constexpr static uint64_t kStorageMagic = 0x3A3A3A3A2B2B2B2B;

	HnswIndexBase(const HnswIndexBase&, size_t newCapacity);

	SelectKeyResult select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&) const override;
	KnnRawResult selectRaw(ConstFloatVectorView, const KnnSearchParams&) const override;
	HnswKnnRawResult selectRawImpl(ConstFloatVectorView, const KnnSearchParams&) const;
	Variant upsert(ConstFloatVectorView, IdType id, bool& clearCache) override;
	Variant upsertConcurrent(ConstFloatVectorView, IdType id, bool& clearCache) override;

	FloatVector getFloatVector(IdType rowId) const override { return FloatVector{getFloatVectorView(rowId)}; }
	ConstFloatVectorView getFloatVectorView(IdType) const override;

	static size_t newSize(size_t currentSize) noexcept;
	template <typename ParamsT>
	HnswKnnRawResult search(const float*, const ParamsT&) const;
	static std::unique_ptr<hnswlib::SpaceInterface> newSpace(size_t dimension, VectorMetric);
	void clearMap() noexcept;

	std::unique_ptr<hnswlib::SpaceInterface> space_;
	std::unique_ptr<Map> map_;
};

using HnswIndexST = HnswIndexBase<hnswlib::HierarchicalNSWST>;
using HnswIndexMT = HnswIndexBase<hnswlib::HierarchicalNSWMT>;
using BruteForceVectorIndex = HnswIndexBase<hnswlib::BruteforceSearch>;

std::unique_ptr<Index> HnswIndex_New(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);
std::unique_ptr<Index> BruteForceVectorIndex_New(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);

}  // namespace reindexer

#endif	// RX_WITH_BUILTIN_ANN_INDEXES
