#pragma once

#include <algorithm>
#include <iterator>
#include "estl/defines.h"

namespace reindexer {
/// Adaptive implementation of upper_bound with exponential growth of step for walking through ordered container in forward direction
/// First element strictly greater than @p bound in [@p it, @p end), or @p end.
template <std::random_access_iterator IteratorT, typename BoundT, ptrdiff_t kLinearSteps = 8>
RX_ALWAYS_INLINE static IteratorT GallopUpperBound(IteratorT it, IteratorT end, BoundT bound) noexcept {
	for (ptrdiff_t i = 0; i < kLinearSteps && it != end && *it <= bound; ++i) {
		++it;
	}
	if (it == end || *it > bound) {
		return it;
	}
	auto lower = it;
	ptrdiff_t step = 1;
	while (true) {
		const auto remaining = end - lower;
		if (step >= remaining) {
			return std::upper_bound(lower, end, bound);
		}
		const auto probe = lower + step;
		if (*probe > bound) {
			return std::upper_bound(lower, probe + 1, bound);
		}
		lower = probe + 1;
		step <<= 1;
		if (lower == end || *lower > bound) {
			return lower;
		}
	}
}

/// Adaptive implementation of lower_bound with exponential growth of step for walking through ordered container in reverse direction
/// Reverse walk: first element strictly less than @p bound, or @p rend.
template <std::random_access_iterator ForwardIt, typename BoundT, ptrdiff_t kLinearSteps = 8>
RX_ALWAYS_INLINE static std::reverse_iterator<ForwardIt> GallopLowerBound(std::reverse_iterator<ForwardIt> rit,
																		  std::reverse_iterator<ForwardIt> rend, BoundT bound) noexcept {
	for (ptrdiff_t i = 0; i < kLinearSteps && rit != rend && *rit >= bound; ++i) {
		++rit;
	}

	if (rit == rend || *rit < bound) {
		return rit;
	}
	auto upper = std::prev(rit.base());
	const auto lo = rend.base();
	ptrdiff_t step = 1;
	while (true) {
		const auto remaining = upper - lo;
		if (step >= remaining) {
			const auto lb = std::lower_bound(lo, std::next(upper), bound);
			return (lb == lo) ? rend : std::make_reverse_iterator(lb);
		}
		const auto probe = upper - step;
		if (*probe < bound) {
			const auto lb = std::lower_bound(probe + 1, std::next(upper), bound);
			return std::make_reverse_iterator(lb);
		}
		upper = probe - 1;
		step <<= 1;
		if (upper == lo || *upper < bound) {
			return (*upper < bound) ? std::make_reverse_iterator(std::next(upper)) : rend;
		}
	}
}
}  // namespace reindexer