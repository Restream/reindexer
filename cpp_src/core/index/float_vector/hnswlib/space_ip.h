// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include "hnswlib.h"
#include "tools/distances/ip_dist.h"

namespace hnswlib {

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
static float InnerProductDistance(const void* pVect1, const void* pVect2, const void* qty_ptr) noexcept {
	return -reindexer::vector_dists::InnerProduct((const float*)pVect1, (const float*)pVect2, *((const size_t*)qty_ptr));
}

static float InnerProduct(const void* pVect1, const void* pVect2, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::InnerProduct((const float*)pVect1, (const float*)pVect2, *((const size_t*)qty_ptr));
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

//=======================================================

#if REINDEXER_WITH_SSE

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
RX_AVX_TARGET_ATTR
static float InnerProductDistanceSIMD4ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -reindexer::vector_dists::InnerProductSIMD4ExtAVX((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

RX_AVX_TARGET_ATTR
static float InnerProductSIMD4ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::InnerProductSIMD4ExtAVX((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float InnerProductDistanceSIMD4ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -reindexer::vector_dists::InnerProductSIMD4ExtSSE((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float InnerProductSIMD4ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::InnerProductSIMD4ExtSSE((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

RX_AVX512_TARGET_ATTR
static float InnerProductDistanceSIMD16ExtAVX512(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -reindexer::vector_dists::InnerProductSIMD16ExtAVX512((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

RX_AVX512_TARGET_ATTR
static float InnerProductSIMD16ExtAVX512(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::InnerProductSIMD16ExtAVX512((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

RX_AVX_TARGET_ATTR
static float InnerProductDistanceSIMD16ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -reindexer::vector_dists::InnerProductSIMD16ExtAVX((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

RX_AVX_TARGET_ATTR
static float InnerProductSIMD16ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::InnerProductSIMD16ExtAVX((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float InnerProductDistanceSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -reindexer::vector_dists::InnerProductSIMD16ExtSSE((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}

static float InnerProductSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return reindexer::vector_dists::InnerProductSIMD16ExtSSE((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr));
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

static DISTFUNC<float> initInnerProductSIMD16Ext() noexcept {
	if (reindexer::vector_dists::InnerProductWithAVX512()) {
		return InnerProductSIMD16ExtAVX512;
	}
	if (reindexer::vector_dists::InnerProductWithAVX()) {
		return InnerProductSIMD16ExtAVX;
	}
	return InnerProductSIMD16ExtSSE;
}

static DISTFUNC<float> initInnerProductSIMD4Ext() noexcept {
	if (reindexer::vector_dists::InnerProductWithAVX()) {
		return InnerProductSIMD4ExtAVX;
	}
	return InnerProductSIMD4ExtSSE;
}

static DISTFUNC<float> initInnerProductDistanceSIMD16Ext() noexcept {
	if (reindexer::vector_dists::InnerProductWithAVX512()) {
		return InnerProductDistanceSIMD16ExtAVX512;
	}
	if (reindexer::vector_dists::InnerProductWithAVX()) {
		return InnerProductDistanceSIMD16ExtAVX;
	}
	return InnerProductDistanceSIMD16ExtSSE;
}

static DISTFUNC<float> initInnerProductDistanceSIMD4Ext() noexcept {
	if (reindexer::vector_dists::InnerProductWithAVX()) {
		return InnerProductDistanceSIMD4ExtAVX;
	}
	return InnerProductDistanceSIMD4ExtSSE;
}

static const DISTFUNC<float> InnerProductSIMD16Ext = initInnerProductSIMD16Ext();
static const DISTFUNC<float> InnerProductSIMD4Ext = initInnerProductSIMD4Ext();
static const DISTFUNC<float> InnerProductDistanceSIMD16Ext = initInnerProductDistanceSIMD16Ext();
static const DISTFUNC<float> InnerProductDistanceSIMD4Ext = initInnerProductDistanceSIMD4Ext();

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
static float InnerProductDistanceSIMD16ExtResiduals(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -(reindexer::vector_dists::InnerProductResiduals16Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr)));
}

static float InnerProductDistanceSIMD4ExtResiduals(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -(reindexer::vector_dists::InnerProductResiduals4Ext((const float*)pVect1v, (const float*)pVect2v, *((const size_t*)qty_ptr)));
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END
#endif	// REINDEXER_WITH_SSE

//=======================================================

class [[nodiscard]] InnerProductSpace final : public SpaceInterface<float> {
	DISTFUNC<float> fstdistfunc_;
	size_t data_size_;
	size_t dim_;

public:
	InnerProductSpace(size_t dim) noexcept {
		fstdistfunc_ = InnerProductDistance;
#if REINDEXER_WITH_SSE
		if (dim % 16 == 0) {
			fstdistfunc_ = InnerProductDistanceSIMD16Ext;
		} else if (dim > 16) {
			fstdistfunc_ = InnerProductDistanceSIMD16ExtResiduals;
		} else if (dim % 4 == 0) {
			fstdistfunc_ = InnerProductDistanceSIMD4Ext;
		} else if (dim > 4) {
			fstdistfunc_ = InnerProductDistanceSIMD4ExtResiduals;
		}
#endif	// REINDEXER_WITH_SSE
		dim_ = dim;
		data_size_ = dim * sizeof(float);
	}

	size_t get_data_size() noexcept override { return data_size_; }

	DistCalculatorParam<float> get_dist_calculator_param() noexcept override {
		return DistCalculatorParam<float>{.f = fstdistfunc_, .metric = MetricType::INNER_PRODUCT, .dims = dim_};
	}

	void* get_dist_func_param() noexcept override { return &dim_; }
};

}  // namespace hnswlib
