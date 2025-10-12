#pragma once

#include <string>
#include <vector>

#include "errors.h"
#include "estl/concepts.h"

namespace reindexer {
namespace fs {

struct [[nodiscard]] DirEntry {
	std::string name;
	bool isDir;
	unsigned internalFilesCount;
};

enum [[nodiscard]] FileStatus {
	StatError = -1,
	StatFile = 1,
	StatDir = 2,
};

struct [[nodiscard]] TimeStats {
	int64_t atime;
	int64_t ctime;
	int64_t mtime;
};

int MkDirAll(const std::string& path) noexcept;
int RmDirAll(const std::string& path) noexcept;
int ReadFile(const std::string& path, std::string& content) noexcept;
int64_t WriteFile(const std::string& path, std::string_view content) noexcept;
int ReadDir(const std::string& path, std::vector<DirEntry>& content) noexcept;
bool DirectoryExists(const std::string& directory) noexcept;
FileStatus Stat(const std::string& path);
TimeStats StatTime(const std::string& path);
std::string GetCwd();
std::string GetDirPath(const std::string& path);
std::string GetTempDir();
void SetTempDir(std::string&& dir) noexcept;
std::string GetHomeDir();
std::string GetRelativePath(const std::string& path, unsigned maxUp = 1024);
inline int Rename(const std::string& from, const std::string& to) { return rename(from.c_str(), to.c_str()); }

Error TryCreateDirectory(const std::string& dir);
Error ChangeUser(const char* userName);
Error ChownDir(const std::string& path, const std::string& user);

inline std::string JoinPath(std::string base, concepts::ConvertibleToString auto&& name) {
	return base.append((!base.empty() && base.back() != '/') ? "/" : "").append(name);
}

template <concepts::ConvertibleToString... Str>
inline std::string JoinPath(std::string base, concepts::ConvertibleToString auto&& name, Str&&... names) {
	return JoinPath(JoinPath(base, name), names...);
}

}  // namespace fs
}  // namespace reindexer
