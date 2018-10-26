#pragma once

#include "core/keyvalue/variant.h"
#include "gason/gason.h"
#include "tools/errors.h"

namespace reindexer {

Variant jsonValue2Variant(JsonValue &v, KeyValueType t, const char *fieldName = "");

}  // namespace reindexer
