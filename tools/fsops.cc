
#include "fsops.h"
#include <dirent.h>
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

	snprintf(tmp, sizeof(tmp), "%s", path.c_str());
	len = strlen(tmp);
	if (tmp[len - 1] == '/') tmp[len - 1] = 0;
	for (p = tmp + 1; *p; p++) {
		if (*p == '/') {
			*p = 0;
			mkdir(tmp, S_IRWXU);
			*p = '/';
		}
	}
	mkdir(tmp, S_IRWXU);
	return 0;
}

int RmDirAll(const string &path) {
	return nftw(path.c_str(), [](const char *fpath, const struct stat *, int, struct FTW *) { return ::remove(fpath); }, 64,
				FTW_DEPTH | FTW_PHYS);
}
}  // namespace reindexer
