#pragma once

#include <string>
#include <vector>

#include "errors.h"

namespace reindexer {
namespace fs {

struct DirEntry {
	std::string name;
	bool isDir;
	unsigned internalFilesCount;
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

[[nodiscard]] int MkDirAll(const std::string &path) noexcept;
int RmDirAll(const std::string &path) noexcept;
[[nodiscard]] int ReadFile(const std::string &path, std::string &content) noexcept;
[[nodiscard]] int64_t WriteFile(const std::string &path, std::string_view content) noexcept;
[[nodiscard]] int ReadDir(const std::string &path, std::vector<DirEntry> &content) noexcept;
[[nodiscard]] bool DirectoryExists(const std::string &directory) noexcept;
FileStatus Stat(const std::string &path);
TimeStats StatTime(const std::string &path);
std::string GetCwd();
std::string GetDirPath(const std::string &path);
std::string GetTempDir();
std::string GetHomeDir();
std::string GetRelativePath(const std::string &path, unsigned maxUp = 1024);
inline int Rename(const std::string &from, const std::string &to) { return rename(from.c_str(), to.c_str()); }

Error TryCreateDirectory(const std::string &dir);
Error ChangeUser(const char *userName);
Error ChownDir(const std::string &path, const std::string &user);

inline std::string JoinPath(const std::string &base, const std::string &name) {
	return base + ((!base.empty() && base.back() != '/') ? "/" : "") + name;
}
}  // namespace fs
}  // namespace reindexer
