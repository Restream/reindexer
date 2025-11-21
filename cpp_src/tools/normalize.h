#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include "estl/defines.h"
#include "tools/assertrx.h"

namespace reindexer::ann {

namespace impl {
extern float (*normalizeVectorPtr)(float*, int32_t) noexcept;
extern float (*calculateL2ModulePtr)(const float*, int32_t) noexcept;
}  // namespace impl

RX_ALWAYS_INLINE float CalculateL2Module(const float* x, int32_t d) noexcept { return impl::calculateL2ModulePtr(x, d); }
RX_ALWAYS_INLINE float NormalizeVector(float* x, int32_t d) noexcept { return impl::normalizeVectorPtr(x, d); }
RX_ALWAYS_INLINE void NormalizeCopyVector(const float* x, int32_t d, float* out) noexcept {
	assertrx_dbg(d >= 0);
	std::memcpy(out, x, d * sizeof(float));
	impl::normalizeVectorPtr(out, d);
}
RX_ALWAYS_INLINE std::unique_ptr<float[]> NormalizeCopyVector(const float* x, int32_t d) noexcept {
	auto ret = std::make_unique<float[]>(d);
	NormalizeCopyVector(x, d, ret.get());
	return ret;
}
RX_ALWAYS_INLINE std::unique_ptr<float[]> NormalizeCopyVectors(const float* x, size_t n, int32_t d) noexcept {
	assertrx_dbg(d >= 0);
	auto ret = std::make_unique<float[]>(n * d);
	std::memcpy(ret.get(), x, n * d * sizeof(float));
	auto ptr = ret.get();
	for (size_t i = 0; i < n; ++i, ptr += d) {
		impl::normalizeVectorPtr(ptr, d);
	}
	return ret;
}

}  // namespace reindexer::ann
