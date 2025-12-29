#pragma once

#include <span>
#include <string>
#include <type_traits>
#include "estl/types_pack.h"

namespace reindexer {

template <template <typename> typename Templ, typename... Ts>
struct Template;

namespace concepts::impl {
template <typename T, typename U>
struct ContainsOrSameT {
	constexpr static bool value = std::is_same_v<T, U>;
};

template <typename T, typename... Us>
struct ContainsOrSameT<T, TypesPack<Us...>> {
	constexpr static bool value = (ContainsOrSameT<T, Us>::value || ...);
};

template <typename T, template <typename> typename Templ, typename... Us>
struct ContainsOrSameT<T, Template<Templ, Us...>> {
	constexpr static bool value = (ContainsOrSameT<T, Templ<Us>>::value || ...);
};
}  // namespace concepts::impl

// Concepts signatures
namespace concepts {

template <typename T, typename... Us>
concept OneOf = (impl::ContainsOrSameT<T, Us>::value || ...);

template <typename T>
concept ConvertibleToString = std::is_constructible_v<std::string, T>;

template <typename T>
concept StringContainer = requires(T t) {
	{ std::ranges::begin(t) } -> std::forward_iterator;
	{ std::ranges::end(t) } -> std::sentinel_for<decltype(std::ranges::begin(t))>;
	{ *std::ranges::begin(t) } -> ConvertibleToString;
};

template <typename T, typename... Us>
concept SpanFromOneOf = OneOf<T, std::span<Us>...>;

template <typename T>
concept Iterable = requires(T a) {
	std::begin(a);
	std::end(a);
};

template <typename T>
concept HasSize = requires(T a) {
	{ a.size() } -> std::convertible_to<unsigned>;	// Using 'unsigned' for h_vector/VariantsArray compatibility
};

template <typename T>
concept IsEnum = std::is_enum_v<T>;

}  // namespace concepts
}  // namespace reindexer
