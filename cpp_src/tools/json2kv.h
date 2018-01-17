#pragma once

#include "core/keyvalue/keyvalue.h"
#include "gason/gason.h"
#include "tools/errors.h"

namespace reindexer {

KeyRef jsonValue2KeyRef(JsonValue &v, KeyValueType t);

}  // namespace reindexer
