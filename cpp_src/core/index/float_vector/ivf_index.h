#pragma once

#if RX_WITH_FAISS_ANN_INDEXES

#include "faiss/MetricType.h"
#include "float_vector_index.h"

namespace faiss {

struct IndexFlat;
struct IndexIVFFlat;

};	// namespace faiss

namespace reindexer {

class IvfKnnRawResult;

class [[nodiscard]] IvfIndex final : public FloatVectorIndex {
	using Base = FloatVectorIndex;

public:
	IvfIndex(const IndexDef&, PayloadType&&, FieldsSet&&, LogCreation);

	std::unique_ptr<Index> Clone(size_t newCapacity) const override;
	IndexMemStat GetMemStat(const RdxContext&) const noexcept override;
	StorageCacheWriteResult WriteIndexCache(WrSerializer&, PKGetterF&&, bool isCompositePK,
											const std::atomic_int32_t& cancel) noexcept override;
	Error LoadIndexCache(std::string_view data, bool isCompositePK, FloatVectorIndexRawDataInserter&&, LoadWithQuantizer,
						 uint8_t version) override;
	void RebuildCentroids(float dataPart) override;

	uint64_t GetHash(FloatVectorId id) const override { return FloatVectorIndex::getFloatVectorView(id).Hash(); }

	bool QuantizationAvailable() const override { return false; }
	bool IsQuantized() const override { return false; }
	void Quantize(size_t /*itemsCount*/) override { assertrx(false); }
	void SwitchMapOnQuantized() override { assertrx(false); }

private:
	template <typename K, typename V>
	using IDHashMapT = tsl::hopscotch_sc_map<K, V, std::hash<K>, std::equal_to<K>, std::less<K>, std::allocator<std::pair<const K, V>>, 30,
											 false, tsl::mod_growth_policy<std::ratio<3, 2>>>;

	constexpr static uint64_t kStorageMagic = 0x3B3B3B3B2A2A2A2A;
	IvfIndex(const IvfIndex&);

	template <bool isArray>
	SelectKeyResult select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&, const auto& map, const auto& prepareId) const;
	SelectKeyResult select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&) const override;
	template <bool isArray>
	KnnRawResult selectRaw(ConstFloatVectorView, const KnnSearchParams&) const;
	KnnRawResult selectRaw(ConstFloatVectorView, const KnnSearchParams&) const override;

	Variant upsert(ConstFloatVectorView, FloatVectorId id, bool& clearCache) override;
	[[noreturn]] Variant upsertConcurrent(ConstFloatVectorView, FloatVectorId id, bool& clearCache) override;
	void del(FloatVectorId, MustExist, IsLast) override;

	ConstFloatVectorView getFloatVectorViewImpl(FloatVectorId) const override;

	static std::unique_ptr<faiss::IndexFlat> newSpace(size_t dimension, VectorMetric);
	void clearMap() noexcept;
	constexpr static size_t ivfTrainingSize(size_t nCentroids) noexcept { return nCentroids * 39; }
	faiss::MetricType faissMetric() const noexcept;
	void reconstruct(FloatVectorId, FloatVector&) const;
	static void trainIdx(faiss::IndexIVFFlat& idx, const float* vecs, const float* norms, size_t vecsCount);

	size_t nCentroids_;
	std::unique_ptr<faiss::IndexFlat> space_;

	std::vector<faiss::idx_t> n2FvId_;
	IDHashMapT<FloatVectorId, size_t> fvId2N_;
	std::unique_ptr<faiss::IndexIVFFlat> map_;
};

std::unique_ptr<Index> IvfIndex_New(const IndexDef&, PayloadType&&, FieldsSet&&, LogCreation);

}  // namespace reindexer

#endif	// RX_WITH_FAISS_ANN_INDEXES
