#include "terminalutils.h"

#include <wchar.h>
#include <cstring>
#include "oscompat.h"
#include "tools/errors.h"
#include "utf8cpp/utf8.h"
#include "vendor/wcwidth/wcwidth.h"

namespace reindexer {

bool isStdoutRedirected() { return (!isatty(fileno(stdout))); }
bool isStdinRedirected() { return (!isatty(fileno(stdin))); }

Error getTerminalSize(int fd, int& columns, int& lines) {
	int retCode = -1;

	do {
#ifdef _WIN32
		CONSOLE_SCREEN_BUFFER_INFO csbi;
		GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
		columns = csbi.srWindow.Right - csbi.srWindow.Left + 1;
		lines = csbi.srWindow.Bottom - csbi.srWindow.Top + 1;
		retCode = 1;
		(void)fd;
		(void)columns;
		(void)lines;
#elif defined TIOCGSIZE
		struct ttysize sz;
		retCode = ioctl(fd, TIOCGSIZE, &sz);
		lines = sz.ts_lines;
		columns = sz.ts_cols;
#elif defined(TIOCGWINSZ)
		struct winsize sz;
		retCode = ioctl(fd, TIOCGWINSZ, &sz);
		lines = sz.ws_row;
		columns = sz.ws_col;
#endif
	} while ((retCode == -1) && (errno == EINTR));

	if (retCode == -1) {
		Error err(errLogic, std::strerror(errno));
		return err;
	}
	return errOK;
}

TerminalSize getTerminalSize() {
	TerminalSize size;

	Error err;
#ifdef _WIN32
	err = getTerminalSize(0, size.width, size.height);
#else
	const int fds[] = {STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO};
	for (const int fd : fds) {
		err = getTerminalSize(fd, size.width, size.height);
		if (err.ok()) {
			break;
		}
	}
#endif
	if (!err.ok()) {
		size.height = 24;
		size.width = 80;
	}

	return size;
}

int getStringTerminalWidth(std::string_view str) {
	int width = 0;
	try {
		for (auto it = str.begin(); it != str.end() && utf8::internal::sequence_length(it) > 0;) {
			width += mk_wcwidth(utf8::next(it, str.end()));
		}
	} catch (const std::exception&) {
		return str.length();
	}
	return width;
}

}  // namespace reindexer
