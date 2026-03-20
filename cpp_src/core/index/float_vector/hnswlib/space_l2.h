// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once
#include "hnswlib.h"

#include "tools/distances/l2_dist.h"

namespace hnswlib {

template <typename T>
RX_ALWAYS_INLINE static float L2SqrDistance(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2SqrDistance((const T*)pVect1v, (const T*)pVect2v, *((const size_t*)qty_ptr));
}

template <typename T>
class [[nodiscard]] L2SpaceBase final : public SpaceInterface {
	DISTFUNC fstdistfunc_;
	size_t data_size_;
	size_t dim_;

public:
	L2SpaceBase(size_t dim) noexcept {
		fstdistfunc_ = L2SqrDistance<T>;
		dim_ = dim;
		data_size_ = dim * sizeof(T);
	}

	size_t get_data_size() noexcept override { return data_size_; }

	DistCalculatorParam get_dist_calculator_param() noexcept override {
		return {.f = fstdistfunc_, .metric = MetricType::L2, .dims = dim_};
	}
};

template class L2SpaceBase<float>;
using L2Space = L2SpaceBase<float>;
template class L2SpaceBase<uint8_t>;
using L2SpaceSq8 = L2SpaceBase<uint8_t>;
}  // namespace hnswlib
