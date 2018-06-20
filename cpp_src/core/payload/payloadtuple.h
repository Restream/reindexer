#pragma once

#include "core/keyvalue/key_string.h"

class ConstPayload;

namespace reindexer {
class TagsMatcher;
key_string BuildPayloadTuple(ConstPayload &pl, const TagsMatcher &tagsMatcher);
}  // namespace reindexer
