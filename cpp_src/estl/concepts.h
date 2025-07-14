#pragma once

#include <string>
#include <type_traits>

namespace reindexer::concepts {

template <typename T, typename... Us>
concept OneOf = (std::same_as<T, Us> || ...);

template <typename T>
concept ConvertibleToString = std::is_constructible_v<std::string, T>;

}  // namespace reindexer::concepts
