#pragma once

#include <cmath>
#include "estl/defines.h"

namespace reindexer::fp {

constexpr size_t kDefaultMaxULPs = 8;  // Value is empirical, based on possibe sort expression values difference
template <std::floating_point T>
RX_ALWAYS_INLINE bool EqualWithinULPs(T x, T y, size_t n = kDefaultMaxULPs) {
	// Implementation from https://en.cppreference.com/w/cpp/types/numeric_limits/epsilon.html
	const auto m = std::min(std::fabs(x), std::fabs(y));
	const int exp = m < std::numeric_limits<T>::min() ? (std::numeric_limits<T>::min_exponent - 1) : std::ilogb(m);
	return std::fabs(x - y) <= n * std::ldexp(std::numeric_limits<T>::epsilon(), exp);
}

#ifndef _MSC_VER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfloat-equal"
#endif

// Explicit floats equality. Used for special cases, when we have to preserve consistency
// (for example, in comparators - they have to return the same results, as hash/tree-index search)
template <std::floating_point T>
RX_ALWAYS_INLINE constexpr bool ExactlyEqual(T x, T y) noexcept {
	return x == y;
}

// Comparison with zero is safe
template <std::floating_point T>
RX_ALWAYS_INLINE constexpr bool IsZero(T x) noexcept {
	return x == T(0.);
}

#ifndef _MSC_VER
#pragma GCC diagnostic pop
#endif

}  // namespace reindexer::fp
