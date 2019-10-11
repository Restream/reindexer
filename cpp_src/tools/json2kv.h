#pragma once

#include "core/keyvalue/variant.h"
#include "gason/gason.h"
#include "tools/errors.h"

namespace reindexer {

Variant jsonValue2Variant(gason::JsonValue &v, KeyValueType t, string_view fieldName = string_view());

}  // namespace reindexer
