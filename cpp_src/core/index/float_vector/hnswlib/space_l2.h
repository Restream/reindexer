// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once
#include "hnswlib.h"

#include "tools/distances/l2_dist.h"

namespace hnswlib {

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
template <typename T>
static float L2Sqr(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2Sqr((const T*)pVect1v, (const T*)pVect2v, *((const size_t*)qty_ptr));
}

#if REINDEXER_WITH_SSE

RX_AVX512_TARGET_ATTR
static float L2SqrSIMD16ExtAVX512(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2SqrSIMD16ExtAVX512((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

RX_AVX_TARGET_ATTR
static float L2SqrSIMD16ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2SqrSIMD16ExtAVX((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float L2SqrSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2SqrSIMD16ExtSSE((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float L2SqrSIMD4Ext(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2SqrSIMD4Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static DISTFUNC initL2SqrSIMD16Ext() noexcept {
	if (reindexer::vector_dists::L2WithAVX512()) {
		return L2SqrSIMD16ExtAVX512;
	}
	if (reindexer::vector_dists::L2WithAVX()) {
		return L2SqrSIMD16ExtAVX;
	}
	return L2SqrSIMD16ExtSSE;
}

static const DISTFUNC L2SqrSIMD16Ext = initL2SqrSIMD16Ext();

static float L2SqrSIMD16ExtResiduals(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2SqrResiduals16Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float L2SqrSIMD4ExtResiduals(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::L2SqrResiduals4Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}
#endif	// REINDEXER_WITH_SSE
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

template <typename T>
class [[nodiscard]] L2SpaceBase final : public SpaceInterface {
	DISTFUNC fstdistfunc_;
	size_t data_size_;
	size_t dim_;

public:
	L2SpaceBase(size_t dim) noexcept {
		fstdistfunc_ = L2Sqr<T>;
#if REINDEXER_WITH_SSE
		if constexpr (std::is_same_v<T, float>) {
			if (dim % 16 == 0) {
				fstdistfunc_ = L2SqrSIMD16Ext;
			} else if (dim > 16) {
				fstdistfunc_ = L2SqrSIMD16ExtResiduals;
			} else if (dim % 4 == 0) {
				fstdistfunc_ = L2SqrSIMD4Ext;
			} else if (dim > 4) {
				fstdistfunc_ = L2SqrSIMD4ExtResiduals;
			}
		}
#endif
		dim_ = dim;
		data_size_ = dim * sizeof(T);
	}

	size_t get_data_size() noexcept override { return data_size_; }

	DistCalculatorParam get_dist_calculator_param() noexcept override {
		return {.f = fstdistfunc_, .metric = MetricType::L2, .dims = dim_};
	}

	void* get_dist_func_param() noexcept override { return &dim_; }
};

template class L2SpaceBase<float>;
using L2Space = L2SpaceBase<float>;
template class L2SpaceBase<uint8_t>;
using L2SpaceSq8 = L2SpaceBase<uint8_t>;
}  // namespace hnswlib
