#pragma once

#include "estl/defines.h"

#include <cstdint>

#if defined(_MSC_VER) && !defined(__clang__)
#include <intrin.h>
#endif

namespace reindexer {

[[nodiscard]] RX_ALWAYS_INLINE bool AddOverflow(int64_t a, int64_t b, int64_t& result) noexcept {
#if defined(__GNUC__) || defined(__clang__)
	return __builtin_add_overflow(a, b, &result);
#else	// !defined(__GNUC__) && !defined(__clang__)
	const auto ur = static_cast<uint64_t>(a) + static_cast<uint64_t>(b);
	result = static_cast<int64_t>(ur);
	return ((a ^ result) & (b ^ result)) < 0;
#endif	// defined(__GNUC__) || defined(__clang__)
}

[[nodiscard]] RX_ALWAYS_INLINE bool SubOverflow(int64_t a, int64_t b, int64_t& result) noexcept {
#if defined(__GNUC__) || defined(__clang__)
	return __builtin_sub_overflow(a, b, &result);
#else	// !defined(__GNUC__) && !defined(__clang__)
	const auto ur = static_cast<uint64_t>(a) - static_cast<uint64_t>(b);
	result = static_cast<int64_t>(ur);
	return ((a ^ b) & (a ^ result)) < 0;
#endif	// defined(__GNUC__) || defined(__clang__)
}

[[nodiscard]] RX_ALWAYS_INLINE bool MulOverflow(int64_t a, int64_t b, int64_t& result) noexcept {
#if defined(__GNUC__) || defined(__clang__)
	return __builtin_mul_overflow(a, b, &result);
#elif defined(_M_X64)
	// _mul128 is x64-only (not available for Win32 / _M_IX86).
	int64_t hi = 0;
	result = _mul128(a, b, &hi);
	return hi != (result >> 63);
#else	// !defined(__GNUC__) && !defined(__clang__) && !defined(_M_X64)
	// Signed 64x64 -> 128 via 32-bit halves. Low word matches the unsigned product;
	// high word needs a two's-complement correction for each negative operand.
	const auto ua = uint64_t(a);
	const auto ub = uint64_t(b);
	const auto aLo = ua & 0xffffffffu;
	const auto aHi = ua >> 32;
	const auto bLo = ub & 0xffffffffu;
	const auto bHi = ub >> 32;
	const auto p00 = aLo * bLo;
	const auto p01 = aLo * bHi;
	const auto p10 = aHi * bLo;
	const auto p11 = aHi * bHi;
	const auto mid = (p00 >> 32) + (p01 & 0xffffffffu) + (p10 & 0xffffffffu);
	const auto uLo = (p00 & 0xffffffffu) | (mid << 32);
	auto uHi = p11 + (p01 >> 32) + (p10 >> 32) + (mid >> 32);
	if (a < 0) {
		uHi -= ub;
	}
	if (b < 0) {
		uHi -= ua;
	}
	result = int64_t(uLo);
	const auto hi = int64_t(uHi);
	return hi != (result >> 63);
#endif	// defined(__GNUC__) || defined(__clang__) || defined(_M_X64)
}

}  // namespace reindexer
