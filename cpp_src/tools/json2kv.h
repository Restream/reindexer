#pragma once

#include "core/keyvalue/keyvalue.h"
#include "gason/gason.h"
#include "tools/errors.h"

namespace reindexer {

KeyRef jsonValue2KeyRef(JsonValue &v, KeyValueType t, const char *fieldName = "");
KeyValue jsonValue2KeyValue(JsonValue &values);

}  // namespace reindexer
