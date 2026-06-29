#pragma once

#include <string>

namespace reindexer {

namespace impl {
class Query;
}  // namespace impl

namespace dsl {
std::string toDsl(const impl::Query& query);
}  // namespace dsl

}  // namespace reindexer
