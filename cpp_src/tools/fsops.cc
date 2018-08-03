
#include "fsops.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include "tools/oscompat.h"

namespace reindexer {
namespace fs {

int MkDirAll(const string &path) {
	char tmp[0x1000], *p = nullptr;
	size_t len;
	int err;

	snprintf(tmp, sizeof(tmp), "%s", path.c_str());
	len = strlen(tmp);
	if (tmp[len - 1] == '/' || tmp[len - 1] == '\\') tmp[len - 1] = 0;
	for (p = tmp + 1; *p; p++) {
		if (*p == '/' || *p == '\\') {
			*p = 0;
			err = mkdir(tmp, S_IRWXU);
			if ((err < 0) && (errno != EEXIST)) return err;
			*p = '/';
		}
	}
	return ((mkdir(tmp, S_IRWXU) < 0) && errno != EEXIST) ? -1 : 0;
}

int RmDirAll(const string &path) {
#ifndef _WIN32
	return nftw(path.c_str(), [](const char *fpath, const struct stat *, int, struct FTW *) { return ::remove(fpath); }, 64,
				FTW_DEPTH | FTW_PHYS);
#else
	(void)path;
	return 0;
#endif
}

int ReadFile(const string &path, string &content) {
	FILE *f = fopen(path.c_str(), "r");
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
}

int ReadDir(const string &path, vector<DirEntry> &content) {
#ifndef _WIN32
	struct dirent *entry;
	auto dir = opendir(path.c_str());

	if (!dir) {
		return -1;
	}

	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] == '.') {
			continue;
		}
		bool isDir = entry->d_type == DT_DIR;
		if (entry->d_type == DT_UNKNOWN) {
			struct stat stat;
			if (lstat((path + "/" + entry->d_name).c_str(), &stat) >= 0 && S_ISDIR(stat.st_mode)) {
				isDir = true;
			}
		}
		content.push_back({entry->d_name, isDir});
	};

	closedir(dir);
#else
	HANDLE hFind;
	WIN32_FIND_DATA entry;

	if ((hFind = FindFirstFile((path + "/*.*").c_str(), &entry)) != INVALID_HANDLE_VALUE) {
		do {
			if (entry.cFileName[0] == '.') {
				continue;
			}
			bool isDir = entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY;
			content.push_back({entry.cFileName, isDir});
		} while (FindNextFile(hFind, &entry));
		FindClose(hFind);
	}

#endif
	return 0;
}

string GetCwd() {
	char buff[FILENAME_MAX];
	return std::string(getcwd(buff, FILENAME_MAX));
}

string GetTempDir() {
#ifdef _WIN32
	char tmpBuf[512];
	*tmpBuf = 0;
	::GetTempPathA(sizeof(tmpBuf), tmpBuf);
	return tmpBuf;
#else
	if (const char *tmpDir = getenv("TMPDIR")) {
		if (tmpDir && *tmpDir) return tmpDir;
	}
	return "/tmp";
#endif
}

string GetHomeDir() {
	if (const char *homeDir = getenv("HOME")) {
		if (homeDir && *homeDir) return homeDir;
	}
	return ".";
}

FileStatus Stat(const string &path) {
#ifdef _WIN32
	struct _stat state;
	if (_stat(path.c_str(), &state) < 0) return StatError;
	return (state.st_mode & _S_IFDIR) ? StatDir : StatFile;
#else
	struct stat state;
	if (stat(path.c_str(), &state) < 0) return StatError;
	return S_ISDIR(state.st_mode) ? StatDir : StatFile;
#endif
}

bool DirectoryExists(const std::string &directory) {
	if (!directory.empty()) {
#ifdef _WIN32
		if (_access(directory.c_str(), 0) == 0) {
			struct _stat status;
			_stat(directory.c_str(), &status);
			if (status.st_mode & _S_IFDIR) return true;
		}
#else
		if (access(directory.c_str(), F_OK) == 0) {
			struct stat status;
			stat(directory.c_str(), &status);
			if (status.st_mode & S_IFDIR) return true;
		}
#endif
	}
	return false;
}

}  // namespace fs
}  // namespace reindexer
