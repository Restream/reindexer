#pragma once

#include <string_view>

namespace reindexer {

/// Determines if SQL LIKE utf8Pattern matches utf8Str.
/// @param utf8Str - Checked string in utf8.
/// @param utf8Pattern - SQL LIKE pattern in utf8.
bool matchLikePattern(std::string_view utf8Str, std::string_view utf8Pattern);

}  // namespace reindexer
