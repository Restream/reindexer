#pragma once

#include <optional>
#include "estl/concepts.h"

namespace reindexer {

class Query;
class JoinedQuery;

namespace functions {
class PrecomputedValues;
}

template <concepts::OneOf<Query, JoinedQuery> Q>
void OptimizeFunctionEntries(const Q& query, std::optional<Q>& queryCopy, functions::PrecomputedValues& precomputedValues);

}  // namespace reindexer
