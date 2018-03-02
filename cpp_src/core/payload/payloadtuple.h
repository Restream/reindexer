#pragma once

#include "core/keyvalue/key_string.h"

class ConstPayload;
class TagsMatcher;

namespace reindexer {
key_string BuildPayloadTuple(ConstPayload &pl, const TagsMatcher &tagsMatcher);
}
