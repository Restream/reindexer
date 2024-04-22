#pragma once

#include "core/keyvalue/variant.h"
#include "gason/gason.h"

namespace reindexer {

Variant jsonValue2Variant(const gason::JsonValue& v, KeyValueType t, std::string_view fieldName = std::string_view());

}  // namespace reindexer
