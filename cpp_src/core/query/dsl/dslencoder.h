#pragma once

#include <string>

namespace reindexer {

class Query;

namespace dsl {
std::string toDsl(const Query& query);
}  // namespace dsl

}  // namespace reindexer
