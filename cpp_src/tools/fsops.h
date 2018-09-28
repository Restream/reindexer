#pragma once

#include <string>
#include <vector>

#include "errors.h"

namespace reindexer {
namespace fs {
using std::string;
using std::vector;

struct DirEntry {
	string name;
	bool isDir;
};

enum FileStatus {
	StatError = -1,
	StatFile = 1,
	StatDir = 2,
};

int MkDirAll(const string &path);
int RmDirAll(const string &path);
int ReadFile(const string &path, string &content);
int ReadDir(const string &path, vector<DirEntry> &content);
bool DirectoryExists(const string &directory);
FileStatus Stat(const string &path);
string GetCwd();
string GetDirPath(const string &path);
string GetTempDir();
string GetHomeDir();

Error TryCreateDirectory(const string &dir);
Error ChangeUser(const char *userName);
Error ChownDir(const string &path, const string &user);

inline static string JoinPath(string base, string name) { return base + ((!base.empty() && base.back() != '/') ? "/" : "") + name; }
}  // namespace fs
}  // namespace reindexer
