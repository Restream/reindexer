#pragma once

#include <string_view>

namespace reindexer {

namespace impl {

class Query;

}  // namespace impl

namespace dsl {

void Parse(std::string_view dsl, impl::Query& q);

}  // namespace dsl
}  // namespace reindexer
