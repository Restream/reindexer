#pragma once

#include <string_view>

namespace reindexer {

class Query;

namespace dsl {

void Parse(std::string_view dsl, Query& q);

}  // namespace dsl
}  // namespace reindexer
