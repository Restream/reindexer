#pragma once

#include <string_view>
#include "tools/errors.h"

namespace reindexer {

class Query;

namespace dsl {

[[nodiscard]] Error Parse(std::string_view dsl, Query& q);

}  // namespace dsl
}  // namespace reindexer
