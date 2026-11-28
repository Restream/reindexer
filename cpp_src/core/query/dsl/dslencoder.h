#pragma once
#include "core/query/query_impl.h"

namespace reindexer::dsl {

std::string toDsl(ConstQueryImpl query);

}  // namespace reindexer::dsl
