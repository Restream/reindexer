// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include "hnswlib.h"
#include "tools/distances/ip_dist.h"

namespace hnswlib {
template <typename T>
RX_ALWAYS_INLINE static float InnerProductDistance(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -reindexer::vector_dists::InnerProductDistance((const T*)pVect1v, (const T*)pVect2v, *((const size_t*)qty_ptr));
}

template <typename T>
class [[nodiscard]] InnerProductSpaceBase final : public SpaceInterface {
	DISTFUNC fstdistfunc_;
	size_t data_size_;
	size_t dim_;

public:
	InnerProductSpaceBase(size_t dim) noexcept {
		fstdistfunc_ = InnerProductDistance<T>;
		dim_ = dim;
		data_size_ = dim * sizeof(T);
	}

	size_t get_data_size() noexcept override { return data_size_; }

	DistCalculatorParam get_dist_calculator_param() noexcept override {
		return {.f = fstdistfunc_, .metric = MetricType::INNER_PRODUCT, .dims = dim_};
	}
};

template class InnerProductSpaceBase<float>;
using InnerProductSpace = InnerProductSpaceBase<float>;

template class InnerProductSpaceBase<uint8_t>;
using InnerProductSpaceSq8 = InnerProductSpaceBase<uint8_t>;

}  // namespace hnswlib
