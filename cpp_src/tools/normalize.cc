#include "normalize.h"
#include "cmath"
#include "estl/defines.h"
#include "faiss/impl/platform_macros.h"
#include "tools/cpucheck.h"

namespace reindexer::ann {

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
RX_ALWAYS_INLINE static float calculateL2Module(const float* x, int32_t d) noexcept {
	assertrx_dbg(d >= 0);
	float normL2Sqr = 0.0;
	FAISS_PRAGMA_IMPRECISE_LOOP
	for (auto cur = x, end = x + d; cur < end; ++cur) {
		normL2Sqr += (*cur) * (*cur);
	}
	float normL2K = 1.0;
	// Epsilon value should be different for non-float32 types
	if (normL2Sqr > 0.0 && std::abs(1.0f - normL2Sqr) > 0.00001f) [[likely]] {
		normL2K = 1.0 / std::sqrt(normL2Sqr);
	}
	return normL2K;
}

RX_ALWAYS_INLINE static float normalizeVector(float* x, int32_t d) noexcept {
	const auto normL2K = calculateL2Module(x, d);
	FAISS_PRAGMA_IMPRECISE_LOOP
	for (auto cur = x, end = x + d; cur < end; ++cur) {
		*cur *= normL2K;
	}
	return normL2K;
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

#if REINDEXER_WITH_SSE

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
RX_AVX_TARGET_ATTR static float calculateL2ModuleAVX(const float* x, int32_t d) noexcept { return calculateL2Module(x, d); }

RX_AVX512_TARGET_ATTR static float calculateL2ModuleAVX512(const float* x, int32_t d) noexcept { return calculateL2Module(x, d); }

RX_AVX_TARGET_ATTR static float normalizeVectorAVX(float* x, int32_t d) noexcept {
	const auto normL2K = calculateL2ModuleAVX(x, d);
	FAISS_PRAGMA_IMPRECISE_LOOP
	for (auto cur = x, end = x + d; cur < end; ++cur) {
		*cur *= normL2K;
	}
	return normL2K;
}

RX_AVX512_TARGET_ATTR static float normalizeVectorAVX512(float* x, int32_t d) noexcept {
	const auto normL2K = calculateL2ModuleAVX512(x, d);
	FAISS_PRAGMA_IMPRECISE_LOOP
	for (auto cur = x, end = x + d; cur < end; ++cur) {
		*cur *= normL2K;
	}
	return normL2K;
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

#endif	// REINDEXER_WITH_SSE

namespace impl {

using NormalizePtrT = float (*)(float*, int32_t) noexcept;
using CalculateL2ModulePtrT = float (*)(const float*, int32_t) noexcept;

static NormalizePtrT initNormalizeSIMD() {
#if REINDEXER_WITH_SSE
	if (IsAVX512Allowed()) {
		return normalizeVectorAVX512;
	}
	if (IsAVXAllowed()) {
		return normalizeVectorAVX;
	}
#endif	// REINDEXER_WITH_SSE
	return normalizeVector;
}

static CalculateL2ModulePtrT initCalculateL2Module() {
#if REINDEXER_WITH_SSE
	if (IsAVX512Allowed()) {
		return calculateL2ModuleAVX512;
	}
	if (IsAVXAllowed()) {
		return calculateL2ModuleAVX;
	}
#endif	// REINDEXER_WITH_SSE
	return calculateL2Module;
}

NormalizePtrT normalizeVectorPtr = initNormalizeSIMD();
CalculateL2ModulePtrT calculateL2ModulePtr = initCalculateL2Module();
}  // namespace impl

}  // namespace reindexer::ann
