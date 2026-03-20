#pragma once

#include "space_ip.h"

namespace hnswlib {

template <typename T>
class [[nodiscard]] CosineSpaceBase final : public SpaceInterface {
	DISTFUNC fstdistfunc_;
	size_t data_size_;
	size_t dim_;

public:
	CosineSpaceBase(size_t dim) noexcept {
		fstdistfunc_ = InnerProductDistance<T>;
		dim_ = dim;
		data_size_ = dim * sizeof(T);
	}

	size_t get_data_size() noexcept override { return data_size_; }

	DistCalculatorParam get_dist_calculator_param() noexcept override {
		return {.f = fstdistfunc_, .metric = MetricType::COSINE, .dims = dim_};
	}
};

template class CosineSpaceBase<float>;
using CosineSpace = CosineSpaceBase<float>;

template class CosineSpaceBase<uint8_t>;
using CosineSpaceSq8 = CosineSpaceBase<uint8_t>;

}  // namespace hnswlib
