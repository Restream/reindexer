#pragma once

#include "core/query/query.h"
#include "core/type_consts.h"
#include "gason/gason.h"

namespace reindexer {
namespace dsl {
void parse(JsonValue& value, Query& q);
}  // namespace dsl
}  // namespace reindexer
