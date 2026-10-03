#pragma once

#include <type_traits>
#include <utility>

namespace reindexer {

template <typename T, typename U>
constexpr decltype(auto) forward_like(U&& x) noexcept {
	constexpr bool as_rval = std::is_rvalue_reference_v<T&&>;

	if constexpr (std::is_const_v<std::remove_reference_t<T>>) {
		using U2 = std::remove_reference_t<U>;
		if constexpr (as_rval) {
			return static_cast<const U2&&>(x);
		} else {
			return static_cast<const U2&>(x);
		}
	} else {
		if constexpr (as_rval) {
			return static_cast<std::remove_reference_t<U>&&>(x);
		} else {
			return static_cast<U&>(x);
		}
	}
}

}  // namespace reindexer
