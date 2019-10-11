#pragma once

#include <string>
#include "core/type_consts.h"
#include "tools/errors.h"

namespace reindexer {
class Query;
namespace dsl {
Error Parse(const std::string& dsl, Query& q);
}  // namespace dsl
}  // namespace reindexer
