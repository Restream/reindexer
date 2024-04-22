#pragma once

#include <limits>
#include <type_traits>
#include "estl/comparation_result.h"

namespace reindexer {

template <typename LHS, typename RHS>
RX_ALWAYS_INLINE constexpr std::enable_if_t<std::is_integral_v<LHS> && std::is_floating_point_v<RHS>, ComparationResult> compare(
	LHS lhs, RHS rhs) noexcept {
	static_assert(std::numeric_limits<LHS>::radix == std::numeric_limits<RHS>::radix);
	static_assert(std::numeric_limits<RHS>::digits <= std::numeric_limits<long double>::digits);

	if constexpr (std::numeric_limits<LHS>::digits <= std::numeric_limits<RHS>::digits) {
		const RHS fLhs = lhs;
		if (fLhs < rhs) {
			return ComparationResult::Lt;
		} else if (fLhs > rhs) {
			return ComparationResult::Gt;
		} else {
			return ComparationResult::Eq;
		}
	} else if constexpr (std::numeric_limits<RHS>::digits <= std::numeric_limits<double>::digits &&
						 std::numeric_limits<LHS>::digits <= std::numeric_limits<double>::digits) {
		const double dLhs = lhs;
		const double dRhs = rhs;
		if (dLhs < dRhs) {
			return ComparationResult::Lt;
		} else if (dLhs > dRhs) {
			return ComparationResult::Gt;
		} else {
			return ComparationResult::Eq;
		}
	} else if constexpr (std::numeric_limits<LHS>::digits <= std::numeric_limits<long double>::digits) {
		const long double ldLhs = lhs;
		const long double ldRhs = rhs;
		if (ldLhs < ldRhs) {
			return ComparationResult::Lt;
		} else if (ldLhs > ldRhs) {
			return ComparationResult::Gt;
		} else {
			return ComparationResult::Eq;
		}
	} else {
		if (lhs == 0) {
			if (rhs > 0.0) {
				return ComparationResult::Lt;
			} else if (rhs < 0.0) {
				return ComparationResult::Gt;
			} else {
				return ComparationResult::Eq;
			}
		} else {
			const long double ldLhs = lhs;
			const long double ldRhs = rhs;
			if (ldLhs < ldRhs) {
				return ComparationResult::Lt;
			} else if (ldLhs > ldRhs) {
				return ComparationResult::Gt;
			} else {
				if (lhs > 0) {
					return static_cast<LHS>(ldLhs) < lhs ? ComparationResult::Gt : ComparationResult::Eq;
				} else {
					return static_cast<LHS>(ldLhs) > lhs ? ComparationResult::Lt : ComparationResult::Eq;
				}
			}
		}
	}
}

template <typename LHS, typename RHS>
RX_ALWAYS_INLINE constexpr std::enable_if_t<std::is_integral_v<RHS> && std::is_floating_point_v<LHS>, ComparationResult> compare(
	LHS lhs, RHS rhs) noexcept {
	return -compare(rhs, lhs);
}

template <typename LHS, typename RHS>
RX_ALWAYS_INLINE constexpr std::enable_if_t<(std::is_integral_v<LHS> && std::is_integral_v<RHS>) ||
												(std::is_floating_point_v<LHS> && std::is_floating_point_v<RHS>),
											ComparationResult>
compare(LHS lhs, RHS rhs) noexcept {
	if (lhs < rhs) {
		return ComparationResult::Lt;
	} else if (lhs > rhs) {
		return ComparationResult::Gt;
	} else {
		return ComparationResult::Eq;
	}
}

}  // namespace reindexer
