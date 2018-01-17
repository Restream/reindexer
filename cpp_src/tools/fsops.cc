
#include "fsops.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

namespace reindexer {
int MkDirAll(const string &path) {
	char tmp[0x8000], *p = nullptr;
	size_t len;
	int err;

	snprintf(tmp, sizeof(tmp), "%s", path.c_str());
	len = strlen(tmp);
	if (tmp[len - 1] == '/') tmp[len - 1] = 0;
	for (p = tmp + 1; *p; p++) {
		if (*p == '/') {
			*p = 0;
			err = mkdir(tmp, S_IRWXU);
			if ((err < 0) && (errno != EEXIST)) return err;
			*p = '/';
		}
	}
	return ((mkdir(tmp, S_IRWXU) < 0) && errno != EEXIST) ? -1 : 0;
}

int RmDirAll(const string &path) {
	return nftw(path.c_str(), [](const char *fpath, const struct stat *, int, struct FTW *) { return ::remove(fpath); }, 64,
				FTW_DEPTH | FTW_PHYS);
}

int ReadFile(const string &path, string &content) {
	int fd = open(path.c_str(), O_RDONLY);
	if (fd < 0) {
		return -1;
	}
	size_t sz = lseek(fd, 0, SEEK_END);
	content.resize(sz);
	lseek(fd, 0, SEEK_SET);
	auto nread = read(fd, &content[0], sz);
	close(fd);
	return nread;
}

int ReadDir(const string &path, vector<DirEntry> &content) {
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
	return 0;
}

}  // namespace reindexer
