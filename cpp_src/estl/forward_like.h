#pragma once

#include <type_traits>
#include <utility>

namespace reindexer {

template <class T, class U>
constexpr auto&& forward_like(U&& x) noexcept {
	constexpr bool is_adding_const = std::is_const_v<std::remove_reference_t<T>>;
	if constexpr (std::is_lvalue_reference_v<T&&>) {
		if constexpr (is_adding_const) {
			return std::as_const(x);
		} else {
			return static_cast<U&>(x);
		}
	} else {
		if constexpr (is_adding_const) {
			return std::move(std::as_const(x));
		} else {
			return std::move(x);
		}
	}
}

}  // namespace reindexer
