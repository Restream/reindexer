#pragma once

#include <tools/errors.h>
#include <string>
#include <vector>
#include "gason/gason.h"
#include "tools/serializer.h"

namespace reindexer {

const int kJsonShiftWidth = 4;

void jsonValueToString(gason::JsonValue o, WrSerializer &ser, int shift = kJsonShiftWidth, int indent = 0, bool escapeStrings = true);
void prettyPrintJSON(span<char> json, WrSerializer &ser, int shift = kJsonShiftWidth);

string stringifyJson(const gason::JsonNode &elem);

}  // namespace reindexer
