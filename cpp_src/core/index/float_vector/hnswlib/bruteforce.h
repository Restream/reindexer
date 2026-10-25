// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include "core/index/float_vector/float_vector_id.h"
#include "core/index/float_vector/hnswlib/hnsw_interface.h"
#include "core/index/float_vector/hnswlib/hnswlib.h"
#include "core/keyvalue/float_vector.h"
#include "vendor/hopscotch/hopscotch_sc_map.h"

namespace hnswlib {
class [[nodiscard]] BruteforceSearch {
public:
	BruteforceSearch(reindexer::VectorMetric metric, size_t dim, size_t maxElements);
	BruteforceSearch(const BruteforceSearch& other, size_t newMaxElements);

	~BruteforceSearch() { free(data_); }

	RX_ALWAYS_INLINE size_t MaxElements() const noexcept { return maxElements_; }
	RX_ALWAYS_INLINE size_t CurrentElementCount() const noexcept { return curElementCount_; }
	RX_ALWAYS_INLINE size_t ElementSize() const noexcept { return sizePerElement_; }

	const float* FloatPtrByExternalLabel(labeltype label) const;

	void AddPointNoLock(reindexer::ConstFloatVectorView vect, reindexer::FloatVectorId id);
	[[noreturn]] void AddPointConcurrent(reindexer::ConstFloatVectorView, reindexer::FloatVectorId);

	void RemovePoint(labeltype cur_external);

	void ResizeIndex(size_t newMaxElements);

	SearchResultQueue SearchKnn(const float* queryData, std::optional<float> /*queryDataNorm*/, size_t k, size_t /*ef*/ = 0) const;

	SearchResultQueue SearchRange(const float* queryData, std::optional<float> /*queryDataNorm*/, float radius, size_t /*ef*/) const;

	RX_ALWAYS_INLINE bool IsQuantized() const noexcept { return false; }
	RX_ALWAYS_INLINE bool QuantizationAvailable() const noexcept { return false; }

	RX_ALWAYS_INLINE size_t AllocatedMemSize() const noexcept {
		return dictExternalToInternal_.allocated_mem_size() + (MaxElements() - CurrentElementCount()) * sizePerElement_ +
			   sizeof(hnswlib::BruteforceSearch);
	}

private:
	const size_t dataSize_;
	const size_t sizePerElement_ = dataSize_ + sizeof(labeltype);

	size_t maxElements_;
	size_t curElementCount_ = 0;

	DistCalculator<float> dist_;
	char* data_;

	template <typename K, typename V>
	using HashMapT = tsl::hopscotch_sc_map<K, V, std::hash<K>, std::equal_to<K>, std::less<K>, std::allocator<std::pair<const K, V>>, 30,
										   false, tsl::mod_growth_policy<std::ratio<3, 2>>>;
	HashMapT<labeltype, size_t> dictExternalToInternal_;

	labeltype label(int idx) const noexcept;

	const char* ptrByIdx(int idx) const noexcept;
	char* ptrByIdx(int idx) noexcept;
	const float* floatPtrByIdx(int idx) const noexcept;
};
}  // namespace hnswlib
