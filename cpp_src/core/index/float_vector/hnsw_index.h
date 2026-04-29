#pragma once

#include <queue>
#include <type_traits>
#include "core/enums.h"
#include "core/index/float_vector/float_vector_id.h"
#include "core/index/float_vector/hnswlib/type_consts.h"
#include "estl/lock.h"
#if RX_WITH_BUILTIN_ANN_INDEXES

#include "float_vector_index.h"
#include "hnswlib/hnswlib.h"

namespace reindexer {

class HnswKnnRawResult;

template <typename Map>
class [[nodiscard]] HnswIndexBase final : public FloatVectorIndex {
	using Base = FloatVectorIndex;
	using FloatType = float;

	class [[nodiscard]] HashesContainer : private std::vector<h_vector<int64_t, 1>> {
	public:
		HashesContainer(IsArray isArray) noexcept : isArray_(isArray) {}

		int64_t Get(FloatVectorId id) const {
			static const auto emptyVectorHash = ConstFloatVectorView{}.Hash();
			assertrx(id.RowId().IsValid());
			const size_t rowId = id.RowId().ToNumber();
			if (rowId >= size()) {
				return emptyVectorHash;
			}
			const auto& row = (*this)[rowId];
			const size_t arrIdx = id.ArrayIndex();
			if (arrIdx >= row.size()) {
				return emptyVectorHash;
			}
			return row[arrIdx];
		}
		void Set(FloatVectorId id, int64_t hash) {
			assertrx(id.RowId().IsValid());
			const size_t rowId = id.RowId().ToNumber();
			if (rowId >= size()) {
				resize(rowId + 1);
			}
			auto& row = (*this)[rowId];
			const size_t arrIdx = id.ArrayIndex();
			if (arrIdx >= row.size()) {
				row.resize(arrIdx + 1, 0);
			}
			row[arrIdx] = hash;
		}
		size_t MemStat() const noexcept {
			size_t res = capacity() * sizeof(value_type);
			for (const auto& v : *this) {
				res += v.heap_size();
			}
			return res;
		}
		void Reserve(size_t newSize) {
			if (!isArray_) {
				reserve(newSize);
			}
		}
		void ReserveForce(size_t newSize) { reserve(newSize); }
		void Erase(IdType rowId) noexcept {
			assertrx(rowId.IsValid());
			assertrx(size_t(rowId.ToNumber()) < size());
			(*this)[rowId.ToNumber()].clear();
		}

	private:
		IsArray isArray_;
	};

public:
	HnswIndexBase(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);

	std::unique_ptr<Index> Clone(size_t newCapacity) const override;
	IndexMemStat GetMemStat(const RdxContext&) const noexcept override;
	bool IsSupportMultithreadTransactions() const noexcept override { return std::is_same_v<Map, hnswlib::HierarchicalNSWMT>; }
	void GrowFor(size_t newElementsCount) override;
	StorageCacheWriteResult WriteIndexCache(WrSerializer&, PKGetterF&&, bool isCompositePK,
											const std::atomic_int32_t& cancel) noexcept override;
	Error LoadIndexCache(std::string_view data, bool isCompositePK, FloatVectorIndexRawDataInserter&& getVectorData, LoadWithQuantizer,
						 uint8_t version) override;

	bool QuantizationAvailable() const override;
	bool IsQuantized() const override;
	void Quantize(size_t itemsCount) override;
	void SwitchMapOnQuantized() override;

	uint64_t GetHash(FloatVectorId) const override;

private:
	constexpr static uint64_t kStorageMagic = 0x3A3A3A3A2B2B2B2B;

	HnswIndexBase(const HnswIndexBase&, size_t newCapacity);

	SelectKeyResult select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&) const override;
	KnnRawResult selectRaw(ConstFloatVectorView, const KnnSearchParams&) const override;
	void del(FloatVectorId, MustExist, IsLast) override;
	Variant upsert(ConstFloatVectorView, FloatVectorId id, bool& clearCache) override;
	Variant upsertConcurrent(ConstFloatVectorView, FloatVectorId id, bool& clearCache) override;

	ConstFloatVectorView getFloatVectorViewImpl(FloatVectorId) const override;

	static size_t newSize(size_t currentSize) noexcept;
	std::priority_queue<std::pair<float, hnswlib::labeltype>> search(ConstFloatVectorView key, const KnnSearchParams& params) const;
	static std::unique_ptr<hnswlib::SpaceInterface> newSpace(size_t dimension, VectorMetric, bool quantized = false);
	void clearMap() noexcept;
	void removeOverK(auto& ids, h_vector<RankT, 128>& dists, const auto& params) const;

	std::unique_ptr<hnswlib::SpaceInterface> space_;

	struct [[nodiscard]] MapWrapper {
		MapWrapper(IsArray isArray) noexcept : isArray_(isArray) {}
		MapWrapper(IsArray isArray, std::unique_ptr<Map>&& map)	 // noexcept TOOD add noexcept in #2478
			: map_(std::move(map)), isArray_(isArray) {}

		MapWrapper(IsArray isArray, const MapWrapper& other, size_t dimension, VectorMetric metric, size_t newCapacity)
			: isArray_(isArray) {
			assertrx_throw(other.map_);
			if (other.quantizedMap_) {
				assertrx_throw(*other.quantizedMap_);
				assertrx_throw(intermediateHashes_);
			}

			const auto& map = other.quantizedMap_ ? **other.quantizedMap_ : *other.map_;
			map_ = std::make_unique<Map>(newSpace(dimension, metric, map.IsQuantized()).get(), map, newCapacity);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			hashes_ = other.quantizedMap_ ? *other.intermediateHashes_ : other.hashes_;
		}
		MapWrapper& operator=(MapWrapper&& other) & noexcept {	// TODO remove this #2478
			map_ = std::move(other.map_);
			quantizedMap_ = std::move(other.quantizedMap_);
			isArray_ = std::move(other.isArray_);
			hashes_ = std::move(other.hashes_);
			intermediateHashes_ = std::move(other.intermediateHashes_);
			return *this;
		}

		bool QuantizationAvailable() const { return !map_->IsQuantized() && !quantizedMap_; }

		// This method must only be used in index-modifying operations (like upsert/delete/resize) under the namespace writeLock,
		// in no case under namespace readLock.
		auto& GetWritable() noexcept { return get(); }
		const auto& operator->() const noexcept { return get(); }
		const auto& operator*() const noexcept { return *get(); }

		void Reset() noexcept {
			map_.reset();
			quantizedMap_ = std::nullopt;
		}

		void CalcOrigHashes(size_t dim, size_t itemsCount);
		void Quantize(const hnswlib::QuantizationConfig& config) { quantizedMap_ = map_->QuantizedCopy(config); }

		size_t HashMemStat() const noexcept { return hashes_.MemStat(); }
		void SetHash(FloatVectorId id, uint64_t hash) {
			if constexpr (std::is_same_v<Map, hnswlib::BruteforceSearch>) {
				assertrx(0);
			} else {
				hashes_.Set(id, hash);
			}
		}
		void SetHashConcurrent(FloatVectorId id, uint64_t hash) {  // TODO remove this #2478
			if constexpr (std::is_same_v<Map, hnswlib::BruteforceSearch>) {
				assertrx(0);
			} else {
				lock_guard lck(hashesMtx_);
				hashes_.Set(id, hash);
			}
		}
		uint64_t GetHash(FloatVectorId id) const {
			if constexpr (std::is_same_v<Map, hnswlib::BruteforceSearch>) {
				assertrx(0);
				abort();
			} else {
				return hashes_.Get(id);
			}
		}
		void ClearHash(IdType rowId) {
			if constexpr (std::is_same_v<Map, hnswlib::BruteforceSearch>) {
				assertrx(0);
			} else {
				hashes_.Erase(rowId);
			}
		}
		HashesContainer& Hashes() & noexcept { return hashes_; }
		std::optional<HashesContainer>& IntermediateHashes() & noexcept { return intermediateHashes_; }
		void ReserveHashes(size_t newSize) { hashes_.Reserve(newSize); }

	private:
		auto& get() noexcept {
			if (quantizedMap_) {
				map_ = std::move(*quantizedMap_);
				quantizedMap_ = std::nullopt;

				assertrx(intermediateHashes_);
				hashes_ = std::move(*intermediateHashes_);
				intermediateHashes_ = std::nullopt;
			}
			return map_;
		}

		const auto& get() const noexcept { return map_; }

		std::unique_ptr<Map> map_;
		std::optional<std::unique_ptr<Map>> quantizedMap_;
		IsArray isArray_;
		// origin float vector hashes for quantized vectors
		HashesContainer hashes_{isArray_};
		mutex hashesMtx_;  // TODO remove this #2478
		// In order to avoid a data race, filled in CalcOrigHashes under the ns RLock.
		std::optional<HashesContainer> intermediateHashes_;
	} map_;
};

using HnswIndexST = HnswIndexBase<hnswlib::HierarchicalNSWST>;
using HnswIndexMT = HnswIndexBase<hnswlib::HierarchicalNSWMT>;
using BruteForceVectorIndex = HnswIndexBase<hnswlib::BruteforceSearch>;

std::unique_ptr<Index> HnswIndex_New(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);
std::unique_ptr<Index> BruteForceVectorIndex_New(const IndexDef&, PayloadType&&, FieldsSet&&, size_t currentNsSize, LogCreation);

}  // namespace reindexer

#endif	// RX_WITH_BUILTIN_ANN_INDEXES
