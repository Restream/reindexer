#pragma once

#include <cassert>
#include <type_traits>
#include "defines.h"

namespace reindexer {

enum class [[nodiscard]] ComparationResult : unsigned { Eq = 1, Lt = 2, Le = Eq | Lt, Gt = 4, Ge = Eq | Gt, NotComparable = 8 };

RX_ALWAYS_INLINE bool IsValid(ComparationResult r) noexcept {
	using UnderlyingType = std::underlying_type_t<ComparationResult>;
	const auto res = static_cast<UnderlyingType>(r);
	return (res == static_cast<UnderlyingType>(ComparationResult::Eq)) || (res == static_cast<UnderlyingType>(ComparationResult::Lt)) ||
		   (res == static_cast<UnderlyingType>(ComparationResult::Le)) || (res == static_cast<UnderlyingType>(ComparationResult::Gt)) ||
		   (res == static_cast<UnderlyingType>(ComparationResult::Ge)) ||
		   (res == static_cast<UnderlyingType>(ComparationResult::NotComparable));
}

RX_ALWAYS_INLINE bool operator&(ComparationResult lhs, ComparationResult rhs) noexcept {
	using UnderlyingType = std::underlying_type_t<ComparationResult>;
#ifdef RX_WITH_STDLIB_DEBUG
	assert(IsValid(lhs));
	assert(IsValid(rhs));
#endif	// RX_WITH_STDLIB_DEBUG
	return static_cast<UnderlyingType>(lhs) & static_cast<UnderlyingType>(rhs);
}

// convert Lt <-> Gt and Le <-> Ge
// others values does not change
RX_ALWAYS_INLINE ComparationResult operator-(ComparationResult v) noexcept {
	using UnderlyingType = std::underlying_type_t<ComparationResult>;
	static constexpr auto ltOrGt = static_cast<UnderlyingType>(ComparationResult::Lt) | static_cast<UnderlyingType>(ComparationResult::Gt);
	const auto res = static_cast<UnderlyingType>(v);
#ifdef RX_WITH_STDLIB_DEBUG
	assert(IsValid(v));
#endif	// RX_WITH_STDLIB_DEBUG
	return (res & ltOrGt) ? static_cast<ComparationResult>(res ^ ltOrGt) : v;
}

}  // namespace reindexer
