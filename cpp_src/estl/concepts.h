#pragma once

#include <span>
#include <string>
#include <type_traits>

namespace reindexer::concepts {

template <typename T, typename... Us>
concept OneOf = (std::same_as<T, Us> || ...);

template <typename T>
concept ConvertibleToString = std::is_constructible_v<std::string, T>;

template <typename T, typename... Us>
concept SpanFromOneOf = OneOf<T, std::span<Us>...>;

}  // namespace reindexer::concepts
