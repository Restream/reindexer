#pragma once

#include <optional>
#include "estl/concepts.h"

namespace reindexer {

namespace impl {
class Query;
class JoinedQuery;
}  // namespace impl

namespace functions {
class PrecomputedValues;
}

template <concepts::OneOf<impl::Query, impl::JoinedQuery> Q>
void OptimizeFunctionEntries(const Q& query, std::optional<Q>& queryCopy, functions::PrecomputedValues& precomputedValues);

}  // namespace reindexer
