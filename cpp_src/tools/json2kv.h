#pragma once

#include "gason/gason.h"

namespace reindexer {

class KeyValueType;
class Variant;
class FloatVectorsHolderVector;
constexpr size_t kMaxThreadLocalJSONVector = 8 * 1024;

Variant jsonValue2Variant(const gason::JsonValue&, KeyValueType, std::string_view fieldName, FloatVectorsHolderVector*, ConvertToString,
						  ConvertNull);

}  // namespace reindexer
