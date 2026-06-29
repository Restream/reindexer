#include "fsops.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include "errors.h"
#include "estl/lock.h"
#include "estl/shared_mutex.h"
#include "tools/oscompat.h"

namespace reindexer {
namespace fs {

int MkDirAll(const std::string& path) noexcept {
	try {
		std::string tmpStr = path;
		char *p = nullptr, *tmp = tmpStr.data();
		int err;
		const auto len = tmpStr.size();
		if (tmp[len - 1] == '/' || tmp[len - 1] == '\\') {
			tmp[len - 1] = 0;
		}
		for (p = tmp + 1; *p; p++) {
			if (*p == '/' || *p == '\\') {
				*p = 0;
				err = mkdir(tmp, S_IRWXU);
				if ((err < 0) && (errno != EEXIST)) {
					return err;
				}
				*p = '/';
			}
		}
		return ((mkdir(tmp, S_IRWXU) < 0) && errno != EEXIST) ? -1 : 0;
	} catch (std::exception&) {
		return -1;
	}
}

int RmDirAll(const std::string& path) noexcept {
#ifndef _WIN32
	return nftw(
		path.c_str(), [](const char* fpath, const struct stat*, int, struct FTW*) { return ::remove(fpath); }, 64, FTW_DEPTH | FTW_PHYS);
#else
	WIN32_FIND_DATA entry;
	if (HANDLE hFind = FindFirstFile((path + "/*.*").c_str(), &entry); hFind != INVALID_HANDLE_VALUE) {
		std::string dirPath;
		do {
			if (strncmp(entry.cFileName, ".", 2) == 0 || strncmp(entry.cFileName, "..", 3) == 0) {
				continue;
			}
			const bool isDir = entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY;
			dirPath.clear();
			dirPath.append(path).append("/").append(entry.cFileName);
			if (isDir) {
				if (int ret = RmDirAll(dirPath); ret < 0) {
					FindClose(hFind);
					return ret;
				}
			} else {
				if (!DeleteFile(dirPath.c_str())) {
					FindClose(hFind);
					fprintf(stderr, "reindexer error: unable to remove file '%s'\n", dirPath.c_str());
					return -1;
				}
			}
		} while (FindNextFile(hFind, &entry));
		FindClose(hFind);
		if (!RemoveDirectory(path.c_str())) {
			fprintf(stderr, "reindexer error: unable to remove directory '%s'\n", path.c_str());
			return -1;
		}
	}

	return 0;
#endif
}

int ReadFile(const std::string& path, std::string& content) noexcept {
	try {
		FILE* f = fopen(path.c_str(), "rb");
		if (!f) {
			return -1;
		}
		fseek(f, 0, SEEK_END);
		size_t sz = ftell(f);
		content.resize(sz);
		fseek(f, 0, SEEK_SET);
		auto nread = fread(&content[0], 1, sz, f);
		fclose(f);
		return nread;
	} catch (std::exception&) {
		return -1;
	}
}

int64_t WriteFile(const std::string& path, std::string_view content) noexcept {
	FILE* f = fopen(path.c_str(), "w");
	if (!f) {
		return -1;
	}
	auto written = fwrite(content.data(), content.size(), 1, f);
	fflush(f);
	fclose(f);
	return static_cast<int64_t>((written > 0) ? content.size() : written);
}

int ReadDir(const std::string& path, std::vector<DirEntry>& content) noexcept {
#ifndef _WIN32
	struct dirent* entry;
	auto dir = opendir(path.c_str());

	if (!dir) {
		return -1;
	}

	std::string dirPath;
	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] == '.') {
			continue;
		}
		bool isDir = entry->d_type == DT_DIR;
		unsigned internalFiles = 0;
		dirPath.clear();
		if (isDir) {
			dirPath.append(path).append("/").append(entry->d_name);
		} else if (entry->d_type == DT_UNKNOWN) {
			struct stat stat;
			dirPath.append(path).append("/").append(entry->d_name);
			if (lstat(dirPath.c_str(), &stat) >= 0 && S_ISDIR(stat.st_mode)) {
				isDir = true;
			}
		}
		if (isDir) {
			auto internalDir = opendir(dirPath.c_str());
			if (internalDir) {
				while (readdir(internalDir) != NULL) {
					++internalFiles;
				}
				closedir(internalDir);
			}
		}
		content.push_back({entry->d_name, isDir, internalFiles});
	}

	closedir(dir);
#else
	WIN32_FIND_DATA entry;

	if (HANDLE hFind = FindFirstFile((path + "/*.*").c_str(), &entry); hFind != INVALID_HANDLE_VALUE) {
		std::string dirPath;
		do {
			if (entry.cFileName[0] == '.') {
				continue;
			}
			const bool isDir = entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY;
			unsigned internalFiles = 0;
			if (isDir) {
				WIN32_FIND_DATA internalEntry;
				dirPath.clear();
				dirPath.append(path).append("/").append(entry.cFileName).append("/*.*");
				if (HANDLE hInternal = FindFirstFile((path + "/*.*").c_str(), &internalEntry); hInternal != INVALID_HANDLE_VALUE) {
					do {
						++internalFiles;
					} while (FindNextFile(hInternal, &internalEntry));
					FindClose(hInternal);
				}
			}
			content.push_back({entry.cFileName, isDir, internalFiles});
		} while (FindNextFile(hFind, &entry));
		FindClose(hFind);
	}

#endif
	return 0;
}

std::string GetCwd() {
	char buff[FILENAME_MAX];
	auto cwd = getcwd(buff, FILENAME_MAX);
	return cwd ? std::string(cwd) : std::string();
}

static std::string tmpDir;
static shared_timed_mutex tmpDirMtx;

std::string GetTempDir() RX_REQUIRES(!tmpDirMtx) {
	{
		shared_lock lck(tmpDirMtx);
		if (!tmpDir.empty()) {
			return tmpDir;
		}
	}
#ifdef _WIN32
	char tmpBuf[512];
	*tmpBuf = 0;
	::GetTempPathA(sizeof(tmpBuf), tmpBuf);
	return tmpBuf;
#else
	const char* tmpDir = getenv("TMPDIR");
	if (tmpDir && *tmpDir) {
		return tmpDir;
	}
	return "/tmp";
#endif
}

void SetTempDir(std::string&& dir) noexcept {
	lock_guard lck(tmpDirMtx);
	tmpDir = std::move(dir);
}

std::string GetHomeDir() {
	const char* homeDir = getenv("HOME");
	if (homeDir && *homeDir) {
		return homeDir;
	}
	return ".";
}

FileStatus Stat(const std::string& path) {
#ifdef _WIN32
	struct _stat state;
	if (_stat(path.c_str(), &state) < 0) {
		return StatError;
	}
	return (state.st_mode & _S_IFDIR) ? StatDir : StatFile;
#else
	struct stat state;
	if (stat(path.c_str(), &state) < 0) {
		return StatError;
	}
	return S_ISDIR(state.st_mode) ? StatDir : StatFile;
#endif
}

TimeStats StatTime(const std::string& path) {
#ifdef _WIN32
	FILETIME ftCreate, ftAccess, ftWrite;
	HANDLE hFile = CreateFile(path.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, 0, NULL);

	if (hFile != INVALID_HANDLE_VALUE) {
		if (GetFileTime(hFile, &ftCreate, &ftAccess, &ftWrite)) {
			// https://docs.microsoft.com/en-us/windows/win32/sysinfo/file-times
			// A file time is a 64-bit value that represents the number of 100-nanosecond intervals...
			return {((int64_t(ftAccess.dwHighDateTime) << 32) + ftAccess.dwLowDateTime) * 100,
					((int64_t(ftCreate.dwHighDateTime) << 32) + ftCreate.dwLowDateTime) * 100,
					((int64_t(ftWrite.dwHighDateTime) << 32) + ftWrite.dwLowDateTime) * 100};
		}
		CloseHandle(hFile);
	}
#else
	struct stat st;
	if (stat(path.c_str(), &st) == 0) {
#if defined(__APPLE__)
		return {int64_t(st.st_atimespec.tv_sec) * 1000000000 + st.st_atimespec.tv_nsec,
				int64_t(st.st_ctimespec.tv_sec) * 1000000000 + st.st_ctimespec.tv_nsec,
				int64_t(st.st_mtimespec.tv_sec) * 1000000000 + st.st_mtimespec.tv_nsec};
#elif defined(st_mtime)
		return {int64_t(st.st_atim.tv_sec) * 1000000000 + st.st_atim.tv_nsec, int64_t(st.st_ctim.tv_sec) * 1000000000 + st.st_ctim.tv_nsec,
				int64_t(st.st_mtim.tv_sec) * 1000000000 + st.st_mtim.tv_nsec};
#else
		return {int64_t(st.st_atime) * 1000000000 + st.st_atimensec, int64_t(st.st_ctime) * 1000000000 + st.st_ctimensec,
				int64_t(st.st_mtime) * 1000000000 + st.st_mtimensec};
#endif	// defined(__APPLE__)
	}
#endif	// _WIN32
	return {-1, -1, -1};
}

bool DirectoryExists(const std::string& directory) noexcept {
	if (!directory.empty()) {
#ifdef _WIN32
		if (_access(directory.c_str(), 0) == 0) {
			struct _stat status;
			_stat(directory.c_str(), &status);
			if (status.st_mode & _S_IFDIR) {
				return true;
			}
		}
#else
		if (access(directory.c_str(), F_OK) == 0) {
			struct stat status;
			stat(directory.c_str(), &status);
			if (status.st_mode & S_IFDIR) {
				return true;
			}
		}
#endif
	}
	return false;
}

Error TryCreateDirectory(const std::string& dir) RX_REQUIRES(!tmpDirMtx) {
	using reindexer::fs::MkDirAll;
	using reindexer::fs::DirectoryExists;
	using reindexer::fs::GetTempDir;
	if (!dir.empty()) {
		if (!DirectoryExists(dir) && dir != GetTempDir()) {
			if (MkDirAll(dir) < 0) {
				return Error(errLogic, "Could not create '{}'. Reason: {}\n", dir.c_str(), strerror(errno));
			}
#ifdef _WIN32
		} else if (_access(dir.c_str(), 6) < 0) {
#else
		} else if (access(dir.c_str(), R_OK | W_OK) < 0) {
#endif
			return Error(errLogic, "Could not access dir '{}'. Reason: {}\n", dir.c_str(), strerror(errno));
		}
	}
	return {};
}

std::string GetDirPath(const std::string& path) {
	size_t lastSlashPos = path.find_last_of("/\\");
	return lastSlashPos == std::string::npos ? std::string() : path.substr(0, lastSlashPos + 1);
}

Error ChownDir(const std::string& path, const std::string& user) {
#ifndef _WIN32
	if (!user.empty() && !path.empty()) {
		struct passwd pwd, *usr;
		char buf[0x4000];

		int res = getpwnam_r(user.c_str(), &pwd, buf, sizeof(buf), &usr);
		if (usr == nullptr) {
			if (res == 0) {
				return Error(errLogic, "Could get uid of user and gid for user `{}`. Reason: user `{}` not found", user.c_str(),
							 user.c_str());
			} else {
				return Error(errLogic, "Could not change user to `{}`. Reason: {}", user.c_str(), strerror(errno));
			}
		}

		if (getuid() != usr->pw_uid || getgid() != usr->pw_gid) {
			if (chown(path.c_str(), usr->pw_uid, usr->pw_gid) < 0) {
				return Error(errLogic, "Could not change ownership for directory '{}'. Reason: {}\n", path.c_str(), strerror(errno));
			}
		}
	}
#else
	(void)path;
	(void)user;
#endif
	return {};
}

Error ChangeUser(const char* userName) {
#ifndef _WIN32
	struct passwd pwd, *result;
	char buf[0x4000];

	int res = getpwnam_r(userName, &pwd, buf, sizeof(buf), &result);
	if (result == nullptr) {
		if (res == 0) {
			return Error(errLogic, "Could not change user to `{}`. Reason: user `{}` not found", userName, userName);
		} else {
			errno = res;
			return Error(errLogic, "Could not change user to `{}`. Reason: {}", userName, strerror(errno));
		}
	}

	if (setgid(pwd.pw_gid) != 0) {
		return Error(errLogic, "Could not change user to `{}`. Reason: {}", userName, strerror(errno));
	}
	if (setuid(pwd.pw_uid) != 0) {
		return Error(errLogic, "Could not change user to `{}`. Reason: {}", userName, strerror(errno));
	}
#else
	(void)userName;
#endif
	return {};
}

std::string GetRelativePath(const std::string& path, unsigned maxUp) {
	std::string cwd = GetCwd();

	unsigned same = 0, slashes = 0;
	for (; same < std::min(cwd.size(), path.size()) && cwd[same] == path[same]; ++same) {
	}
	for (unsigned i = same; i < cwd.size(); ++i) {
		if (cwd[i] == '/' || i == same) {
			slashes++;
		}
	}
	if (!slashes && same < path.size()) {
		same++;
	}

	if (same < 2 || (slashes > maxUp)) {
		return path;
	}

	std::string rpath;
	rpath.reserve(slashes * 3 + path.size() - same + 1);
	while (slashes--) {
		rpath += "../";
	}
	rpath.append(path.begin() + same, path.end());
	return rpath;
}

}  // namespace fs
}  // namespace reindexer
