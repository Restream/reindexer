#pragma once

#include "core/keyvalue/float_vectors_holder.h"
#include "core/keyvalue/variant.h"
#include "gason/gason.h"

namespace reindexer {

constexpr size_t kMaxThreadLocalJSONVector = 8 * 1024;

Variant jsonValue2Variant(const gason::JsonValue& v, KeyValueType t, std::string_view fieldName, FloatVectorsHolderVector*);

}  // namespace reindexer
