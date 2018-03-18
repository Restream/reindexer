#pragma once

#include <string.h>
#include <tools/errors.h>
#include "gason/gason.h"

namespace reindexer {

void parseJsonField(const char *name, string &ref, JsonNode *elem);
void parseJsonField(const char *name, bool &ref, const JsonNode *elem);
template <typename T>
void parseJsonField(const char *name, T &ref, const JsonNode *elem, double min = 0, double max = 0);

}  // namespace reindexer
