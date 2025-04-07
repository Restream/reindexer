#pragma once

#include <string>
#include <type_traits>

namespace reindexer::concepts {

template <typename T>
concept ConvertibleToString = std::is_constructible_v<std::string, T>;

} // namespace reindexer::concepts
