#pragma once

#include <string>

using std::string;

namespace reindexer {

int MkDirAll(const string &path);
int RmDirAll(const string &path);
}  // namespace reindexer
