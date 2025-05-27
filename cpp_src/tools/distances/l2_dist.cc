#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#include "l2_dist.h"
#include "tools/distances/common.h"

namespace reindexer::vector_dists {

namespace impl {

using L2SqrPtrT = float (*)(const float*, const float*, size_t);

#if REINDEXER_WITH_SSE

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
static float l2SqrSSEResiduals16Ext(const float* pVect1, const float* pVect2, size_t qty) {
	size_t qty16 = qty >> 4 << 4;
	if (qty16 == qty) {
		return L2SqrSIMD16ExtSSE(pVect1, pVect2, qty16);
	}
	float res = qty16 ? L2SqrSIMD16ExtSSE(pVect1, pVect2, qty16) : 0.0;
	return res + L2Sqr(pVect1 + qty16, pVect2 + qty16, qty - qty16);
}

RX_AVX_TARGET_ATTR
static float l2SqrAVXResiduals16Ext(const float* pVect1, const float* pVect2, size_t qty) {
	size_t qty16 = qty >> 4 << 4;
	if (qty16 == qty) {
		return L2SqrSIMD16ExtAVX(pVect1, pVect2, qty16);
	}
	float res = qty16 ? L2SqrSIMD16ExtAVX(pVect1, pVect2, qty16) : 0.0;
	return res + L2Sqr(pVect1 + qty16, pVect2 + qty16, qty - qty16);
}

RX_AVX512_TARGET_ATTR
static float l2SqrAVX512Residuals16Ext(const float* pVect1, const float* pVect2, size_t qty) {
	size_t qty16 = qty >> 4 << 4;
	if (qty16 == qty) {
		return L2SqrSIMD16ExtAVX512(pVect1, pVect2, qty16);
	}
	float res = qty16 ? L2SqrSIMD16ExtAVX512(pVect1, pVect2, qty16) : 0.0;
	return res + L2Sqr(pVect1 + qty16, pVect2 + qty16, qty - qty16);
}

static float l2SqrSSEResiduals4Ext(const float* pVect1, const float* pVect2, size_t qty) {
	size_t qty4 = qty >> 2 << 2;
	if (qty4 == qty) {
		return L2SqrSIMD4Ext(pVect1, pVect2, qty4);
	}
	float res = qty4 ? L2SqrSIMD4Ext(pVect1, pVect2, qty4) : 0.0;
	return res + L2Sqr(pVect1 + qty4, pVect2 + qty4, qty - qty4);
}

FAISS_PRAGMA_IMPRECISE_FUNCTION_END
#endif	// REINDEXER_WITH_SSE

static L2SqrPtrT initL2SqrSIMD16Ext() {
#if REINDEXER_WITH_SSE
	if (L2WithAVX512()) {
		return l2SqrAVX512Residuals16Ext;
	}
	if (L2WithAVX()) {
		return l2SqrAVXResiduals16Ext;
	}
	return l2SqrSSEResiduals16Ext;
#else	// REINDEXER_WITH_SSE
	return L2Sqr;
#endif	// REINDEXER_WITH_SSE
}

static L2SqrPtrT initL2SqrSIMD4Ext() {
#if REINDEXER_WITH_SSE
	return l2SqrSSEResiduals4Ext;
#else	// REINDEXER_WITH_SSE
	return L2Sqr;
#endif	// REINDEXER_WITH_SSE
}

L2SqrPtrT L2SqrResiduals16ExtPtr = initL2SqrSIMD16Ext();
L2SqrPtrT L2SqrResiduals4ExtPtr = initL2SqrSIMD4Ext();
}  // namespace impl

#if REINDEXER_WITH_SSE

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
RX_AVX512_TARGET_ATTR
float L2SqrSIMD16ExtAVX512(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN64 TmpRes[16];
	size_t qty16 = qty >> 4;

	const float* pEnd1 = pVect1 + (qty16 << 4);

	__m512 diff, v1, v2;
	__m512 sum = _mm512_set1_ps(0);

	while (pVect1 < pEnd1) {
		v1 = _mm512_loadu_ps(pVect1);
		pVect1 += 16;
		v2 = _mm512_loadu_ps(pVect2);
		pVect2 += 16;
		diff = _mm512_sub_ps(v1, v2);
		// sum = _mm512_fmadd_ps(diff, diff, sum);
		sum = _mm512_add_ps(sum, _mm512_mul_ps(diff, diff));
	}

	_mm512_store_ps(TmpRes, sum);
	float res = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7] + TmpRes[8] + TmpRes[9] +
				TmpRes[10] + TmpRes[11] + TmpRes[12] + TmpRes[13] + TmpRes[14] + TmpRes[15];

	return (res);
}

RX_AVX_TARGET_ATTR
float L2SqrSIMD16ExtAVX(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];
	size_t qty16 = qty >> 4;

	const float* pEnd1 = pVect1 + (qty16 << 4);

	__m256 diff, v1, v2;
	__m256 sum = _mm256_set1_ps(0);

	while (pVect1 < pEnd1) {
		v1 = _mm256_loadu_ps(pVect1);
		pVect1 += 8;
		v2 = _mm256_loadu_ps(pVect2);
		pVect2 += 8;
		diff = _mm256_sub_ps(v1, v2);
		sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));

		v1 = _mm256_loadu_ps(pVect1);
		pVect1 += 8;
		v2 = _mm256_loadu_ps(pVect2);
		pVect2 += 8;
		diff = _mm256_sub_ps(v1, v2);
		sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
	}

	_mm256_store_ps(TmpRes, sum);
	return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7];
}

float L2SqrSIMD16ExtSSE(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];
	size_t qty16 = qty >> 4;

	const float* pEnd1 = pVect1 + (qty16 << 4);

	__m128 diff, v1, v2;
	__m128 sum = _mm_set1_ps(0);

	while (pVect1 < pEnd1) {
		//_mm_prefetch((char*)(pVect2 + 16), _MM_HINT_T0);
		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		diff = _mm_sub_ps(v1, v2);
		sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));

		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		diff = _mm_sub_ps(v1, v2);
		sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));

		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		diff = _mm_sub_ps(v1, v2);
		sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));

		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		diff = _mm_sub_ps(v1, v2);
		sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
	}

	_mm_store_ps(TmpRes, sum);
	return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
}

float L2SqrSIMD4Ext(const float* pVect1, const float* pVect2, size_t qty) noexcept {
	float PORTABLE_ALIGN32 TmpRes[8];

	size_t qty4 = qty >> 2;

	const float* pEnd1 = pVect1 + (qty4 << 2);

	__m128 diff, v1, v2;
	__m128 sum = _mm_set1_ps(0);

	while (pVect1 < pEnd1) {
		v1 = _mm_loadu_ps(pVect1);
		pVect1 += 4;
		v2 = _mm_loadu_ps(pVect2);
		pVect2 += 4;
		diff = _mm_sub_ps(v1, v2);
		sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
	}
	_mm_store_ps(TmpRes, sum);
	return TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3];
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

#endif	// REINDEXER_WITH_SSE

}  // namespace reindexer::vector_dists

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
