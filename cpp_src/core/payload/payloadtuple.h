#pragma once

#include "core/keyvalue/key_string.h"

class ConstPayload;

namespace reindexer {

class TagsMatcher;

/// Builds a tuple for Payload value.
/// @param pl - Payload value.
/// @param tagsMatcher - tags map for payload value.
key_string BuildPayloadTuple(ConstPayload &pl, const TagsMatcher &tagsMatcher);
}  // namespace reindexer
