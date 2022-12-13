#pragma once

namespace reindexer {

template <typename T, typename... Ts>
class OneOf : private OneOf<Ts...> {
protected:
	OneOf() noexcept = default;

public:
	OneOf(const T&) noexcept {}
	using OneOf<Ts...>::OneOf;
};

template <typename T>
class OneOf<T> {
protected:
	OneOf() noexcept = default;

public:
	OneOf(const T&) noexcept {}
};

}  // namespace reindexer
