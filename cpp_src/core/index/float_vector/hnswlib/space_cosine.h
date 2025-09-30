#pragma once

#include "space_ip.h"

namespace hnswlib {

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
template <typename T>
static float CosineDistance(const void* pVect1, const void* pVect2, const void* qty_ptr) noexcept {
	return -InnerProduct((const T*)pVect1, (const T*)pVect2, qty_ptr);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

//=======================================================

#if REINDEXER_WITH_SSE

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
static float CosineDistanceSIMD4ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -InnerProductSIMD4ExtAVX(pVect1v, pVect2v, qty_ptr);
}

RX_AVX_TARGET_ATTR
static float CosineDistanceSIMD4ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -InnerProductSIMD4ExtSSE(pVect1v, pVect2v, qty_ptr);
}

RX_AVX512_TARGET_ATTR
static float CosineDistanceSIMD16ExtAVX512(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -InnerProductSIMD16ExtAVX512(pVect1v, pVect2v, qty_ptr);
}

RX_AVX_TARGET_ATTR
static float CosineDistanceSIMD16ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -InnerProductSIMD16ExtAVX(pVect1v, pVect2v, qty_ptr);
}

static float CosineDistanceSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	return -InnerProductSIMD16ExtSSE(pVect1v, pVect2v, qty_ptr);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

inline bool CosineWithAVX512() noexcept { return reindexer::vector_dists::InnerProductWithAVX512(); }
inline bool CosineWithAVX() noexcept { return reindexer::vector_dists::InnerProductWithAVX(); }
inline bool CosineWithSSE() noexcept { return reindexer::vector_dists::InnerProductWithSSE(); }

static DISTFUNC initCosineDistanceSIMD16Ext() {
	if (CosineWithAVX512()) {
		return CosineDistanceSIMD16ExtAVX512;
	}
	if (CosineWithAVX()) {
		return CosineDistanceSIMD16ExtAVX;
	}
	return CosineDistanceSIMD16ExtSSE;
}

static DISTFUNC initCosineDistanceSIMD4Ext() {
	if (CosineWithAVX()) {
		return CosineDistanceSIMD4ExtAVX;
	}
	return CosineDistanceSIMD4ExtSSE;
}

static const DISTFUNC CosineDistanceSIMD16Ext = initCosineDistanceSIMD16Ext();
static const DISTFUNC CosineDistanceSIMD4Ext = initCosineDistanceSIMD4Ext();

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
static float CosineDistanceSIMD16ExtResiduals(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	size_t qty = *((size_t*)qty_ptr);
	size_t qty16 = qty >> 4 << 4;
	// Reusing inner product impl here
	float res = InnerProductSIMD16Ext(pVect1v, pVect2v, &qty16);
	float* pVect1 = (float*)pVect1v + qty16;
	float* pVect2 = (float*)pVect2v + qty16;

	size_t qty_left = qty - qty16;
	float res_tail = InnerProduct<float>(pVect1, pVect2, &qty_left);
	return -(res + res_tail);
}

static float CosineDistanceSIMD4ExtResiduals(const void* pVect1v, const void* pVect2v, const void* qty_ptr) noexcept {
	size_t qty = *((size_t*)qty_ptr);
	size_t qty4 = qty >> 2 << 2;
	// Reusing inner product impl here
	float res = InnerProductSIMD4Ext(pVect1v, pVect2v, &qty4);
	size_t qty_left = qty - qty4;

	float* pVect1 = (float*)pVect1v + qty4;
	float* pVect2 = (float*)pVect2v + qty4;
	float res_tail = InnerProduct<float>(pVect1, pVect2, &qty_left);

	return -(res + res_tail);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END
#else	// REINDEXER_WITH_SSE
inline bool CosineWithAVX512() noexcept { return false; }
inline bool CosineWithAVX() noexcept { return false; }
inline bool CosineWithSSE() noexcept { return false; }
#endif	// REINDEXER_WITH_SSE

//=======================================================

template <typename T>
class [[nodiscard]] CosineSpaceBase final : public SpaceInterface {
	DISTFUNC fstdistfunc_;
	size_t data_size_;
	size_t dim_;

public:
	CosineSpaceBase(size_t dim) noexcept {
		fstdistfunc_ = CosineDistance<T>;
#if REINDEXER_WITH_SSE
		if constexpr (std::is_same_v<T, float>) {
			if (dim % 16 == 0) {
				fstdistfunc_ = CosineDistanceSIMD16Ext;
			} else if (dim > 16) {
				fstdistfunc_ = CosineDistanceSIMD16ExtResiduals;
			} else if (dim % 4 == 0) {
				fstdistfunc_ = CosineDistanceSIMD4Ext;
			} else if (dim > 4) {
				fstdistfunc_ = CosineDistanceSIMD4ExtResiduals;
			}
		}
#endif	// REINDEXER_WITH_SSE
		dim_ = dim;
		data_size_ = dim * sizeof(T);
	}

	size_t get_data_size() noexcept override { return data_size_; }

	DistCalculatorParam get_dist_calculator_param() noexcept override {
		return {.f = fstdistfunc_, .metric = MetricType::COSINE, .dims = dim_};
	}

	void* get_dist_func_param() noexcept override { return &dim_; }
};

template class CosineSpaceBase<float>;
using CosineSpace = CosineSpaceBase<float>;

template class CosineSpaceBase<uint8_t>;
using CosineSpaceSq8 = CosineSpaceBase<uint8_t>;

}  // namespace hnswlib
