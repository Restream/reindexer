#pragma once

#include <optional>
#include "core/query/query.h"
namespace reindexer {

namespace functions {
class PrecomputedValues;
}

template <concepts::OneOf<Query, JoinedQuery> Q>
void OptimizeFunctionEntries(const Q& query, std::optional<Q>& queryCopy, functions::PrecomputedValues& precomputedValues);

}  // namespace reindexer
