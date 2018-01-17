#pragma once

#include <string>
#include <vector>

namespace reindexer {
using std::string;
using std::vector;

struct DirEntry {
	string name;
	bool isDir;
};

int MkDirAll(const string &path);
int RmDirAll(const string &path);
int ReadFile(const string &path, string &content);
int ReadDir(const string &path, vector<DirEntry> &content);

}  // namespace reindexer
