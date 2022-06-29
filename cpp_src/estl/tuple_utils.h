#pragma once

namespace reindexer {

template <typename T, size_t... I>
auto tail_impl(T&& v, std::index_sequence<I...>) {
	return std::make_tuple(std::move(std::get<I + 1>(v))...);
}

template <typename T, size_t... I>
auto tail_impl(const T& v, std::index_sequence<I...>) {
	return std::make_tuple(std::get<I + 1>(v)...);
}

template <typename T, typename... Ts>
std::tuple<Ts...> tail(std::tuple<T, Ts...>&& v) {
	return tail_impl(std::move(v), std::index_sequence_for<Ts...>{});
}

template <typename T, typename... Ts>
std::tuple<Ts...> tail(const std::tuple<T, Ts...>& v) {
	return tail_impl(v, std::index_sequence_for<Ts...>{});
}

}  // namespace reindexer
