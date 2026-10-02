#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#include "ip_dist.h"
#include "faiss/impl/platform_macros.h"
#include "tools/cpucheck.h"
#include "tools/distances/common.h"

namespace reindexer::vector_dists {
FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
template <typename T>
static RX_ALWAYS_INLINE float InnerProduct(const T* pVect1, const T* pVect2, size_t qty) noexcept {
	std::conditional_t<std::is_same_v<T, float>, float, int> res = 0;
#if REINDEXER_WITH_SSE
	FAISS_PRAGMA_IMPRECISE_LOOP
#endif	// REINDEXER_WITH_SSE
	for (unsigned i = 0; i < qty; i++) {
		res += pVect1[i] * pVect2[i];
	}
	return res;
}

#if REINDEXER_WITH_SSE
#ifndef _MSC_VER
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#endif
RX_AVX512_TARGET_ATTR static float InnerProductAVX512(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	size_t simdEnd = qty & ~size_t(15);

	__m512 sum0 = _mm512_setzero_ps();
	__m512 sum1 = _mm512_setzero_ps();
	__m512 sum2 = _mm512_setzero_ps();
	__m512 sum3 = _mm512_setzero_ps();

	size_t i = 0;

	for (; i + 64 <= simdEnd; i += 64) {
		__m512 a0 = _mm512_loadu_ps(pVect1 + i);
		__m512 b0 = _mm512_loadu_ps(pVect2 + i);

		__m512 a1 = _mm512_loadu_ps(pVect1 + i + 16);
		__m512 b1 = _mm512_loadu_ps(pVect2 + i + 16);

		__m512 a2 = _mm512_loadu_ps(pVect1 + i + 32);
		__m512 b2 = _mm512_loadu_ps(pVect2 + i + 32);

		__m512 a3 = _mm512_loadu_ps(pVect1 + i + 48);
		__m512 b3 = _mm512_loadu_ps(pVect2 + i + 48);

		sum0 = _mm512_fmadd_ps(a0, b0, sum0);
		sum1 = _mm512_fmadd_ps(a1, b1, sum1);
		sum2 = _mm512_fmadd_ps(a2, b2, sum2);
		sum3 = _mm512_fmadd_ps(a3, b3, sum3);
	}

	__m512 sum = _mm512_add_ps(_mm512_add_ps(sum0, sum1), _mm512_add_ps(sum2, sum3));

	for (; i < simdEnd; i += 16) {
		__m512 v1 = _mm512_loadu_ps(pVect1 + i);
		__m512 v2 = _mm512_loadu_ps(pVect2 + i);
		sum = _mm512_fmadd_ps(v1, v2, sum);
	}

	return _mm512_reduce_add_ps(sum) + InnerProduct(pVect1 + simdEnd, pVect2 + simdEnd, qty - simdEnd);
}
#ifndef _MSC_VER
#ifndef __clang__
#pragma GCC diagnostic pop
#endif
#pragma GCC diagnostic pop
#endif

RX_AVX_TARGET_ATTR static float InnerProductAVX(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	size_t simdEnd = qty & ~size_t(31);

	__m256 sum0 = _mm256_setzero_ps();
	__m256 sum1 = _mm256_setzero_ps();
	__m256 sum2 = _mm256_setzero_ps();
	__m256 sum3 = _mm256_setzero_ps();

	for (size_t i = 0; i + 32 <= simdEnd; i += 32) {
		__m256 a0 = _mm256_loadu_ps(pVect1 + i);
		__m256 b0 = _mm256_loadu_ps(pVect2 + i);

		__m256 a1 = _mm256_loadu_ps(pVect1 + i + 8);
		__m256 b1 = _mm256_loadu_ps(pVect2 + i + 8);

		__m256 a2 = _mm256_loadu_ps(pVect1 + i + 16);
		__m256 b2 = _mm256_loadu_ps(pVect2 + i + 16);

		__m256 a3 = _mm256_loadu_ps(pVect1 + i + 24);
		__m256 b3 = _mm256_loadu_ps(pVect2 + i + 24);

		sum0 = _mm256_add_ps(sum0, _mm256_mul_ps(a0, b0));
		sum1 = _mm256_add_ps(sum1, _mm256_mul_ps(a1, b1));
		sum2 = _mm256_add_ps(sum2, _mm256_mul_ps(a2, b2));
		sum3 = _mm256_add_ps(sum3, _mm256_mul_ps(a3, b3));
	}

	__m256 sum = _mm256_add_ps(_mm256_add_ps(sum0, sum1), _mm256_add_ps(sum2, sum3));

	PORTABLE_ALIGN64 float tmp[8];
	_mm256_storeu_ps(tmp, sum);

	return tmp[0] + tmp[1] + tmp[2] + tmp[3] + tmp[4] + tmp[5] + tmp[6] + tmp[7] +
		   InnerProduct(pVect1 + simdEnd, pVect2 + simdEnd, qty - simdEnd);
}

static float InnerProductSSE(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];

	size_t simdEnd = qty & ~size_t(15);
	size_t qty16 = simdEnd / 16;

	const float* pEnd1 = pVect1 + 16 * qty16;

	__m128 v1, v2;
	__m128 sum_prod = _mm_set1_ps(0);

	while (pVect1 < pEnd1) {
		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));

		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));

		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));

		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
	}
	_mm_store_ps(TmpRes, sum_prod);

	return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + InnerProduct(pVect1, pVect2, qty - simdEnd);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

#ifndef _MSC_VER
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#endif
RX_AVX512_TARGET_ATTR static float InnerProductAVX512(const uint8_t* pVect1, const uint8_t* pVect2, size_t qty) noexcept {
	size_t simdEnd = qty & ~63;
	__m512i sum = _mm512_setzero_si512();

	for (size_t i = 0; i < simdEnd; i += 64) {
		__m512i v0 = _mm512_loadu_si512(pVect1 + i);
		__m512i v1 = _mm512_loadu_si512(pVect2 + i);

		__m512i v0lo = _mm512_cvtepu8_epi16(_mm512_extracti64x4_epi64(v0, 0));
		__m512i v0hi = _mm512_cvtepu8_epi16(_mm512_extracti64x4_epi64(v0, 1));
		__m512i v1lo = _mm512_cvtepu8_epi16(_mm512_extracti64x4_epi64(v1, 0));
		__m512i v1hi = _mm512_cvtepu8_epi16(_mm512_extracti64x4_epi64(v1, 1));

		__m512i prod_lo = _mm512_madd_epi16(v0lo, v1lo);
		__m512i prod_hi = _mm512_madd_epi16(v0hi, v1hi);

		sum = _mm512_add_epi32(sum, prod_lo);
		sum = _mm512_add_epi32(sum, prod_hi);
	}

	PORTABLE_ALIGN64 uint32_t tmp[16];
	_mm512_store_si512(reinterpret_cast<__m512i*>(tmp), sum);
	float result = 0;
	for (int i = 0; i < 16; ++i) {
		result += tmp[i];
	}

	return result + InnerProduct(pVect1 + simdEnd, pVect2 + simdEnd, qty - simdEnd);
}
#ifndef _MSC_VER
#ifndef __clang__
#pragma GCC diagnostic pop
#endif
#pragma GCC diagnostic pop
#endif

RX_AVX_TARGET_ATTR static float InnerProductAVX(const uint8_t* pVect1, const uint8_t* pVect2, size_t qty) noexcept {
	size_t simdEnd = qty & ~15;
	__m128i sum = _mm_setzero_si128();

	for (size_t i = 0; i < simdEnd; i += 16) {
		__m128i va = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1 + i));
		__m128i vb = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2 + i));

		__m128i va_lo = _mm_cvtepu8_epi16(va);
		__m128i vb_lo = _mm_cvtepu8_epi16(vb);

		__m128i va_hi = _mm_cvtepu8_epi16(_mm_srli_si128(va, 8));
		__m128i vb_hi = _mm_cvtepu8_epi16(_mm_srli_si128(vb, 8));

		sum = _mm_add_epi32(sum, _mm_madd_epi16(va_lo, vb_lo));
		sum = _mm_add_epi32(sum, _mm_madd_epi16(va_hi, vb_hi));
	}

	PORTABLE_ALIGN16 uint32_t tmp[4];
	_mm_store_si128(reinterpret_cast<__m128i*>(tmp), sum);
	uint64_t result = static_cast<uint64_t>(tmp[0]) + tmp[1] + tmp[2] + tmp[3];
	return result + InnerProduct(pVect1 + simdEnd, pVect2 + simdEnd, qty - simdEnd);
}

static float InnerProductSSE(const uint8_t* pVect1, const uint8_t* pVect2, size_t qty) noexcept {
	size_t i = 0;

	__m128i acc = _mm_setzero_si128();
	const __m128i zero = _mm_setzero_si128();

	for (; i + 16 <= qty; i += 16) {
		__m128i va = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect1 + i));
		__m128i vb = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pVect2 + i));

		__m128i va_lo = _mm_unpacklo_epi8(va, zero);
		__m128i va_hi = _mm_unpackhi_epi8(va, zero);

		__m128i vb_lo = _mm_unpacklo_epi8(vb, zero);
		__m128i vb_hi = _mm_unpackhi_epi8(vb, zero);

		acc = _mm_add_epi32(acc, _mm_madd_epi16(va_lo, vb_lo));
		acc = _mm_add_epi32(acc, _mm_madd_epi16(va_hi, vb_hi));
	}

	PORTABLE_ALIGN16 uint32_t tmp[4];
	_mm_store_si128(reinterpret_cast<__m128i*>(tmp), acc);
	uint64_t result = static_cast<uint64_t>(tmp[0]) + tmp[1] + tmp[2] + tmp[3];
	return result + InnerProduct(pVect1 + i, pVect2 + i, qty - i);
}
#endif

namespace impl {

template <typename T>
static InnerProductPtrT<T> initInnerProductFn() noexcept {
	InnerProductPtrT<T> res = InnerProduct<T>;
#if REINDEXER_WITH_SSE
	if (reindexer::IsAVX512Allowed()) {
		res = InnerProductAVX512;
	} else if (reindexer::IsAVXAllowed()) {
		res = InnerProductAVX;
	} else {
		res = InnerProductSSE;
	}
#endif
	return res;
}

InnerProductPtrT<float> InnerProductPtrF = initInnerProductFn<float>();
InnerProductPtrT<uint8_t> InnerProductPtrU = initInnerProductFn<uint8_t>();

}  // namespace impl

}  // namespace reindexer::vector_dists

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
