#pragma once

#include <tools/errors.h>
#include <string>
#include <vector>
#include "gason/gason.h"

namespace reindexer {

void parseJsonField(const char *name, string &ref, const JsonNode *elem);
void parseJsonField(const char *name, bool &ref, const JsonNode *elem);
template <typename T>
void parseJsonField(const char *name, T &ref, const JsonNode *elem, double min, double max);

}  // namespace reindexer
