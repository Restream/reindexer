#pragma once

#include <type_traits>
#include "estl/template.h"

namespace reindexer {

template <typename T, typename... Ts>
class [[nodiscard]] OneOf : private OneOf<T>, private OneOf<Ts...> {
public:
	using OneOf<T>::OneOf;
	using OneOf<Ts...>::OneOf;
};

template <typename T>
class [[nodiscard]] OneOf<T> {
protected:
	OneOf() noexcept = default;

public:
	OneOf(const T&) noexcept {}
	template <typename U, typename = std::enable_if_t<std::is_convertible_v<U, T>>>
	OneOf(const U&) noexcept {}
};

template <template <typename> typename Templ, typename... Ts>
class [[nodiscard]] OneOf<Template<Templ, Ts...>> : private OneOf<Templ<Ts>...> {
public:
	using OneOf<Templ<Ts>...>::OneOf;
};

}  // namespace reindexer
