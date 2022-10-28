#pragma once

#include <string>
#include <vector>

#include "errors.h"

namespace reindexer {
namespace fs {

struct DirEntry {
	std::string name;
	bool isDir;
};

enum FileStatus {
	StatError = -1,
	StatFile = 1,
	StatDir = 2,
};

struct TimeStats {
	int64_t atime;
	int64_t ctime;
	int64_t mtime;
};

int MkDirAll(const std::string &path);
int RmDirAll(const std::string &path);
int ReadFile(const std::string &path, std::string &content);
int64_t WriteFile(const std::string &path, std::string_view content);
int ReadDir(const std::string &path, std::vector<DirEntry> &content);
bool DirectoryExists(const std::string &directory);
FileStatus Stat(const std::string &path);
TimeStats StatTime(const std::string &path);
std::string GetCwd();
std::string GetDirPath(const std::string &path);
std::string GetTempDir();
std::string GetHomeDir();
std::string GetRelativePath(const std::string &path, unsigned maxUp = 1024);
inline static int Rename(const std::string &from, const std::string &to) { return rename(from.c_str(), to.c_str()); }

Error TryCreateDirectory(const std::string &dir);
Error ChangeUser(const char *userName);
Error ChownDir(const std::string &path, const std::string &user);

inline static std::string JoinPath(const std::string &base, const std::string &name) {
	return base + ((!base.empty() && base.back() != '/') ? "/" : "") + name;
}
}  // namespace fs
}  // namespace reindexer
