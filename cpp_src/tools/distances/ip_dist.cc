#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#include "ip_dist.h"
#include "tools/distances/common.h"

namespace reindexer::vector_dists {

namespace impl {

using IPPtrT = float (*)(const float*, const float*, size_t);

#if REINDEXER_WITH_SSE
FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN

static float innerProductSSEResiduals16Ext(const float* pVect1, const float* pVect2, size_t qty) {
	size_t qty16 = qty >> 4 << 4;
	if (qty16 == qty) {
		return InnerProductSIMD16ExtSSE(pVect1, pVect2, qty16);
	}
	float res = qty16 ? InnerProductSIMD16ExtSSE(pVect1, pVect2, qty16) : 0.0;
	return res + InnerProduct(pVect1 + qty16, pVect2 + qty16, qty - qty16);
}

RX_AVX_TARGET_ATTR
static float innerProductAVXResiduals16Ext(const float* pVect1, const float* pVect2, size_t qty) {
	size_t qty16 = qty >> 4 << 4;
	if (qty16 == qty) {
		return InnerProductSIMD16ExtAVX(pVect1, pVect2, qty16);
	}
	float res = qty16 ? InnerProductSIMD16ExtAVX(pVect1, pVect2, qty16) : 0.0;
	return res + InnerProduct(pVect1 + qty16, pVect2 + qty16, qty - qty16);
}

RX_AVX512_TARGET_ATTR
static float innerProductAVX512Residuals16Ext(const float* pVect1, const float* pVect2, size_t qty) {
	size_t qty16 = qty >> 4 << 4;
	if (qty16 == qty) {
		return InnerProductSIMD16ExtAVX512(pVect1, pVect2, qty16);
	}
	float res = qty16 ? InnerProductSIMD16ExtAVX512(pVect1, pVect2, qty16) : 0.0;
	return res + InnerProduct(pVect1 + qty16, pVect2 + qty16, qty - qty16);
}

static float innerProductSSEResiduals4Ext(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	size_t qty4 = qty >> 2 << 2;
	if (qty4 == qty) {
		return InnerProductSIMD4ExtSSE(pVect1, pVect2, qty4);
	}
	float res = qty4 ? InnerProductSIMD4ExtSSE(pVect1, pVect2, qty4) : 0.0;
	return res + InnerProduct(pVect1 + qty4, pVect2 + qty4, qty - qty4);
}

RX_AVX_TARGET_ATTR
static float innerProductAVXResiduals4Ext(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	size_t qty4 = qty >> 2 << 2;
	if (qty4 == qty) {
		return InnerProductSIMD4ExtAVX(pVect1, pVect2, qty4);
	}
	float res = qty4 ? InnerProductSIMD4ExtAVX(pVect1, pVect2, qty4) : 0.0;
	return res + InnerProduct(pVect1 + qty4, pVect2 + qty4, qty - qty4);
}

FAISS_PRAGMA_IMPRECISE_FUNCTION_END
#endif	// REINDEXER_WITH_SSE

static IPPtrT initInnerProductSIMD16Ext() {
#if REINDEXER_WITH_SSE
	if (InnerProductWithAVX512()) {
		return innerProductAVX512Residuals16Ext;
	}
	if (InnerProductWithAVX()) {
		return innerProductAVXResiduals16Ext;
	}
	return innerProductSSEResiduals16Ext;
#else	// REINDEXER_WITH_SSE
	return InnerProduct;
#endif	// REINDEXER_WITH_SSE
}

static IPPtrT initInnerProductSIMD4Ext() {
#if REINDEXER_WITH_SSE
	if (InnerProductWithAVX()) {
		return innerProductAVXResiduals4Ext;
	}
	return innerProductSSEResiduals4Ext;
#else	// REINDEXER_WITH_SSE
	return InnerProduct;
#endif	// REINDEXER_WITH_SSE
}

IPPtrT InnerProductResiduals16ExtPtr = initInnerProductSIMD16Ext();
IPPtrT InnerProductResiduals4ExtPtr = initInnerProductSIMD4Ext();
}  // namespace impl

#if REINDEXER_WITH_SSE
FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
RX_AVX_TARGET_ATTR
float InnerProductSIMD4ExtAVX(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];

	size_t qty16 = qty / 16;
	size_t qty4 = qty / 4;

	const float* pEnd1 = pVect1 + 16 * qty16;
	const float* pEnd2 = pVect1 + 4 * qty4;

	__m256 sum256 = _mm256_set1_ps(0);

	while (pVect1 < pEnd1) {
		// TODO: Uncomment?
		//_mm_prefetch((char*)(pVect2 + 16), _MM_HINT_T0);

		__m256 v1 = _mm256_loadu_ps(pVect1);
		pVect1 += 8;
		__m256 v2 = _mm256_loadu_ps(pVect2);
		pVect2 += 8;
		sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));

		v1 = _mm256_loadu_ps(pVect1);
		pVect1 += 8;
		v2 = _mm256_loadu_ps(pVect2);
		pVect2 += 8;
		sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));
	}

	__m128 v1, v2;
	__m128 sum_prod = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));

	while (pVect1 < pEnd2) {
		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
	}

	_mm_store_ps(TmpRes, sum_prod);
	float sum = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
	return sum;
}

float InnerProductSIMD4ExtSSE(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];

	size_t qty16 = qty / 16;
	size_t qty4 = qty / 4;

	const float* pEnd1 = pVect1 + 16 * qty16;
	const float* pEnd2 = pVect1 + 4 * qty4;

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

	while (pVect1 < pEnd2) {
		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		sum_prod = _mm_add_ps(sum_prod, _mm_mul_ps(v1, v2));
	}

	_mm_store_ps(TmpRes, sum_prod);
	float sum = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

	return sum;
}

#ifndef _MSC_VER
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#endif
RX_AVX512_TARGET_ATTR
float InnerProductSIMD16ExtAVX512(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	size_t qty16 = qty / 16;

	const float* pEnd1 = pVect1 + 16 * qty16;

	__m512 sum512 = _mm512_set1_ps(0);

	size_t loop = qty16 / 4;

	while (loop--) {
		__m512 v1 = _mm512_loadu_ps(pVect1);
		__m512 v2 = _mm512_loadu_ps(pVect2);
		pVect1 += 16;
		pVect2 += 16;

		__m512 v3 = _mm512_loadu_ps(pVect1);
		__m512 v4 = _mm512_loadu_ps(pVect2);
		pVect1 += 16;
		pVect2 += 16;

		__m512 v5 = _mm512_loadu_ps(pVect1);
		__m512 v6 = _mm512_loadu_ps(pVect2);
		pVect1 += 16;
		pVect2 += 16;

		__m512 v7 = _mm512_loadu_ps(pVect1);
		__m512 v8 = _mm512_loadu_ps(pVect2);
		pVect1 += 16;
		pVect2 += 16;

		sum512 = _mm512_fmadd_ps(v1, v2, sum512);
		sum512 = _mm512_fmadd_ps(v3, v4, sum512);
		sum512 = _mm512_fmadd_ps(v5, v6, sum512);
		sum512 = _mm512_fmadd_ps(v7, v8, sum512);
	}

	while (pVect1 < pEnd1) {
		__m512 v1 = _mm512_loadu_ps(pVect1);
		__m512 v2 = _mm512_loadu_ps(pVect2);
		pVect1 += 16;
		pVect2 += 16;
		sum512 = _mm512_fmadd_ps(v1, v2, sum512);
	}

	float sum = _mm512_reduce_add_ps(sum512);
	return sum;
}
#ifndef _MSC_VER
#ifndef __clang__
#pragma GCC diagnostic pop
#endif
#pragma GCC diagnostic pop
#endif

RX_AVX_TARGET_ATTR
float InnerProductSIMD16ExtAVX(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];

	size_t qty16 = qty / 16;

	const float* pEnd1 = pVect1 + 16 * qty16;

	__m256 sum256 = _mm256_set1_ps(0);

	while (pVect1 < pEnd1) {
		//_mm_prefetch((char*)(pVect2 + 16), _MM_HINT_T0);

		__m256 v1 = _mm256_loadu_ps(pVect1);
		pVect1 += 8;
		__m256 v2 = _mm256_loadu_ps(pVect2);
		pVect2 += 8;
		sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));

		v1 = _mm256_loadu_ps(pVect1);
		pVect1 += 8;
		v2 = _mm256_loadu_ps(pVect2);
		pVect2 += 8;
		sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v1, v2));
	}

	_mm256_store_ps(TmpRes, sum256);
	float sum = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7];

	return sum;
}

float InnerProductSIMD16ExtSSE(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];

	size_t qty16 = qty / 16;

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
	float sum = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];

	return sum;
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END
#endif	// REINDEXER_WITH_SSE

}  // namespace reindexer::vector_dists

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
