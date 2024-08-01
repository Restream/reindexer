#pragma once

#include <string>
#include "gason/gason.h"

namespace reindexer {

class WrSerializer;

constexpr int kJsonShiftWidth = 4;

void jsonValueToString(gason::JsonValue o, WrSerializer& ser, int shift = kJsonShiftWidth, int indent = 0, bool escapeStrings = true);
void prettyPrintJSON(span<char> json, WrSerializer& ser, int shift = kJsonShiftWidth);

std::string stringifyJson(const gason::JsonNode& elem);

}  // namespace reindexer
