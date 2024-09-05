#pragma once

#include <string>
#include <string_view>

namespace reindexer {

/// Converts SQL LIKE pattern to regular expression in ECMAScript grammar.
std::string sqlLikePattern2ECMAScript(std::string pattern);

/// Makes random SQL LIKE pattern matches the given string.
std::string makeLikePattern(std::string_view utf8Str);

/// Determines if SQL LIKE utf8Pattern matches utf8Str.
/// @param utf8Str - Checked string in utf8.
/// @param utf8Pattern - SQL LIKE pattern in utf8.
bool matchLikePattern(std::string_view utf8Str, std::string_view utf8Pattern);

}  // namespace reindexer
