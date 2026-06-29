#pragma once

#if RX_WITH_BUILTIN_ANN_INDEXES

#include "core/enums.h"
#include "core/index/float_vector/hnswlib/hnsw_interface.h"
#include "float_vector_index.h"

namespace reindexer {

class HnswKnnRawResult;

template <typename Map>
class [[nodiscard]] HnswIndexBase final : public FloatVectorIndex {
public:
	HnswIndexBase(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);

	std::unique_ptr<Index> Clone(size_t newCapacity) const override;
	IndexMemStat GetMemStat(const RdxContext&) const noexcept override;
	bool IsSupportMultithreadTransactions() const noexcept override;
	void GrowFor(size_t newElementsCount) override;
	StorageCacheWriteResult WriteIndexCache(WrSerializer&, PKGetterF&&, bool isCompositePK,
											const std::atomic_int32_t& cancel) noexcept override;
	Error LoadIndexCache(std::string_view data, bool isCompositePK, FloatVectorIndexRawDataInserter&& getVectorData,
						 LoadWithQuantizer) override;

	bool QuantizationAvailable() const override;
	bool IsQuantized() const override;
	void Quantize() override;
	void SwitchMapOnQuantized() override;

	uint64_t GetHash(FloatVectorId) const override;

private:
	using Base = FloatVectorIndex;

	constexpr static uint64_t kStorageMagic = 0x3A3A3A3A2B2B2B2B;

	HnswIndexBase(const HnswIndexBase&, size_t newCapacity);

	SelectKeyResult select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&) const override;
	KnnRawResult selectRaw(ConstFloatVectorView, const KnnSearchParams&) const override;
	void del(FloatVectorId, MustExist) override;
	Variant upsert(ConstFloatVectorView, FloatVectorId id, bool& clearCache) override;
	Variant upsertConcurrent(ConstFloatVectorView, FloatVectorId id, bool& clearCache) override;

	ConstFloatVectorView getFloatVectorViewImpl(FloatVectorId) const override;

	static size_t newSize(size_t currentSize) noexcept;
	hnswlib::SearchResultQueue search(ConstFloatVectorView key, const KnnSearchParams& params) const;
	void clearMap() noexcept;
	void removeOverK(auto& ids, h_vector<RankT, 128>& dists, const auto& params) const;

	Map map_;
};

std::unique_ptr<Index> HnswIndex_New(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);
std::unique_ptr<Index> BruteForceVectorIndex_New(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);

}  // namespace reindexer

#endif	// RX_WITH_BUILTIN_ANN_INDEXES
