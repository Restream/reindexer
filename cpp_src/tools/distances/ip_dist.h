#pragma once

#include <type_traits>
#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#include "estl/defines.h"
#include "faiss/impl/platform_macros.h"
#include "tools/cpucheck.h"

namespace reindexer::vector_dists {

#if REINDEXER_WITH_SSE
inline bool InnerProductWithAVX512() noexcept { return IsAVX512Allowed(); }
inline bool InnerProductWithAVX2() noexcept { return IsAVX2Allowed(); }
inline bool InnerProductWithAVX() noexcept { return IsAVXAllowed(); }
inline bool InnerProductWithSSE() noexcept { return true; }
#else	// REINDEXER_WITH_SSE
inline bool InnerProductWithAVX512() noexcept { return false; }
inline bool InnerProductWithAVX2() noexcept { return false; }
inline bool InnerProductWithAVX() noexcept { return false; }
inline bool InnerProductWithSSE() noexcept { return false; }
#endif	// REINDEXER_WITH_SSE

namespace impl {
extern float (*InnerProductResiduals16ExtPtr)(const float*, const float*, size_t);
extern float (*InnerProductResiduals4ExtPtr)(const float*, const float*, size_t);
}  // namespace impl

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
template <typename T>
static inline float InnerProduct(const T* pVect1, const T* pVect2, size_t qty) noexcept {
	std::conditional_t<std::is_same_v<T, float>, float, int> res = 0;
#if REINDEXER_WITH_SSE
	FAISS_PRAGMA_IMPRECISE_LOOP
#endif	// REINDEXER_WITH_SSE
	for (unsigned i = 0; i < qty; i++) {
		res += pVect1[i] * pVect2[i];
	}
	return res;
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

RX_ALWAYS_INLINE float InnerProductResiduals16Ext(const float* x, const float* y, size_t d) {
	return impl::InnerProductResiduals16ExtPtr(x, y, d);
}
RX_ALWAYS_INLINE float InnerProductResiduals4Ext(const float* x, const float* y, size_t d) {
	return impl::InnerProductResiduals4ExtPtr(x, y, d);
}

#if REINDEXER_WITH_SSE
RX_AVX_TARGET_ATTR
float InnerProductSIMD4ExtAVX(const float* pVect1, const float* pVect2, size_t qty) noexcept;
float InnerProductSIMD4ExtSSE(const float* pVect1, const float* pVect2, size_t qty) noexcept;
RX_AVX512_TARGET_ATTR
float InnerProductSIMD16ExtAVX512(const float* pVect1, const float* pVect2, size_t qty) noexcept;
RX_AVX_TARGET_ATTR
float InnerProductSIMD16ExtAVX(const float* pVect1, const float* pVect2, size_t qty) noexcept;
float InnerProductSIMD16ExtSSE(const float* pVect1, const float* pVect2, size_t qty) noexcept;
#endif	// REINDEXER_WITH_SSE

}  // namespace reindexer::vector_dists

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
