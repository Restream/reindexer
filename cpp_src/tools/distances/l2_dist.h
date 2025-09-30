#pragma once

#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#include <type_traits>

#include "estl/defines.h"
#include "faiss/impl/platform_macros.h"
#include "tools/cpucheck.h"

namespace reindexer::vector_dists {

#if REINDEXER_WITH_SSE
inline bool L2WithAVX512() noexcept { return IsAVX512Allowed(); }
inline bool L2WithAVX2() noexcept { return IsAVX2Allowed(); }
inline bool L2WithAVX() noexcept { return IsAVXAllowed(); }
inline bool L2WithSSE() noexcept { return true; }
#else	// REINDEXER_WITH_SSE
inline bool L2WithAVX512() noexcept { return false; }
inline bool L2WithAVX2() noexcept { return false; }
inline bool L2WithAVX() noexcept { return false; }
inline bool L2WithSSE() noexcept { return false; }
#endif	// REINDEXER_WITH_SSE

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
template <typename T>
static inline float L2Sqr(const T* pVect1, const T* pVect2, size_t qty) noexcept {
	using ResT = std::conditional_t<std::is_same_v<T, float>, float, int>;
	ResT res = 0;
#if REINDEXER_WITH_SSE
	FAISS_PRAGMA_IMPRECISE_LOOP
#endif	// REINDEXER_WITH_SSE
	for (size_t i = 0; i < qty; i++) {
		ResT t = *pVect1 - *pVect2;
		pVect1++;
		pVect2++;
		res += t * t;
	}
	return (res);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

namespace impl {
extern float (*L2SqrResiduals16ExtPtr)(const float*, const float*, size_t);
extern float (*L2SqrResiduals4ExtPtr)(const float*, const float*, size_t);
}  // namespace impl

RX_ALWAYS_INLINE float L2SqrResiduals16Ext(const float* x, const float* y, size_t d) { return impl::L2SqrResiduals16ExtPtr(x, y, d); }
RX_ALWAYS_INLINE float L2SqrResiduals4Ext(const float* x, const float* y, size_t d) { return impl::L2SqrResiduals4ExtPtr(x, y, d); }

#if REINDEXER_WITH_SSE
RX_AVX512_TARGET_ATTR
float L2SqrSIMD16ExtAVX512(const float* pVect1, const float* pVect2, size_t qty) noexcept;
RX_AVX_TARGET_ATTR
float L2SqrSIMD16ExtAVX(const float* pVect1, const float* pVect2, size_t qty) noexcept;
float L2SqrSIMD16ExtSSE(const float* pVect1, const float* pVect2, size_t qty) noexcept;
float L2SqrSIMD4Ext(const float* pVect1, const float* pVect2, size_t qty) noexcept;
#endif	// REINDEXER_WITH_SSE

}  // namespace reindexer::vector_dists

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
