#pragma once

#include <tools/errors.h>
#include <string>
#include <vector>
#include "gason/gason.h"
#include "tools/serializer.h"

namespace reindexer {

const int kJsonShiftWidth = 4;

void jsonValueToString(JsonValue o, WrSerializer &ser, int shift = kJsonShiftWidth, int indent = 0);
void prettyPrintJSON(string json, WrSerializer &ser, int shift = kJsonShiftWidth);

string stringifyJson(const JsonNode *elem);

void parseJsonField(const char *name, string &ref, const JsonNode *elem);
void parseJsonField(const char *name, bool &ref, const JsonNode *elem);
template <typename T>
void parseJsonField(const char *name, T &ref, const JsonNode *elem, double min, double max);
template <typename T>
void parseJsonField(const char *name, T &ref, const JsonNode *elem);

}  // namespace reindexer
