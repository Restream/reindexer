#pragma once

#include <limits>
#include <type_traits>

namespace reindexer {

template <typename LHS, typename RHS>
inline constexpr std::enable_if_t<std::is_integral_v<LHS> && std::is_floating_point_v<RHS>, int> compare(LHS lhs, RHS rhs) noexcept {
	static_assert(std::numeric_limits<LHS>::radix == std::numeric_limits<RHS>::radix);
	static_assert(std::numeric_limits<RHS>::digits <= std::numeric_limits<long double>::digits);

	if constexpr (std::numeric_limits<LHS>::digits <= std::numeric_limits<RHS>::digits) {
		const RHS fLhs = lhs;
		if (fLhs < rhs) {
			return -1;
		} else if (fLhs > rhs) {
			return 1;
		} else {
			return 0;
		}
	} else if constexpr (std::numeric_limits<RHS>::digits <= std::numeric_limits<double>::digits &&
						 std::numeric_limits<LHS>::digits <= std::numeric_limits<double>::digits) {
		const double dLhs = lhs;
		const double dRhs = rhs;
		if (dLhs < dRhs) {
			return -1;
		} else if (dLhs > dRhs) {
			return 1;
		} else {
			return 0;
		}
	} else if constexpr (std::numeric_limits<LHS>::digits <= std::numeric_limits<long double>::digits) {
		const long double ldLhs = lhs;
		const long double ldRhs = rhs;
		if (ldLhs < ldRhs) {
			return -1;
		} else if (ldLhs > ldRhs) {
			return 1;
		} else {
			return 0;
		}
	} else {
		if (lhs == 0) {
			if (rhs > 0.0) {
				return -1;
			} else if (rhs < 0.0) {
				return 1;
			} else {
				return 0;
			}
		} else {
			const long double ldLhs = lhs;
			const long double ldRhs = rhs;
			if (ldLhs < ldRhs) {
				return -1;
			} else if (ldLhs > ldRhs) {
				return 1;
			} else {
				if (lhs > 0) {
					return static_cast<LHS>(ldLhs) < lhs ? 1 : 0;
				} else {
					return static_cast<LHS>(ldLhs) > lhs ? -1 : 0;
				}
			}
		}
	}
}

template <typename LHS, typename RHS>
inline constexpr std::enable_if_t<std::is_integral_v<RHS> && std::is_floating_point_v<LHS>, int> compare(LHS lhs, RHS rhs) noexcept {
	return -compare(rhs, lhs);
}

template <typename LHS, typename RHS>
inline constexpr std::enable_if_t<
	(std::is_integral_v<LHS> && std::is_integral_v<RHS>) || (std::is_floating_point_v<LHS> && std::is_floating_point_v<RHS>), int>
compare(LHS lhs, RHS rhs) noexcept {
	if (lhs < rhs) {
		return -1;
	} else if (lhs > rhs) {
		return 1;
	} else {
		return 0;
	}
}

}  // namespace reindexer
