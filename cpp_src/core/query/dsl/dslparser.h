#pragma once

#include <string_view>

namespace reindexer {

class QueryImpl;

namespace dsl {

void Parse(std::string_view dsl, QueryImpl q);

}  // namespace dsl
}  // namespace reindexer
