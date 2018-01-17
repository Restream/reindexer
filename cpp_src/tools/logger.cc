#include "tools/logger.h"
#include <stdarg.h>
#include <cstdio>

namespace reindexer {

LogWriter g_logWriter = nullptr;

void logPrintf(int level, const char *fmt, ...) {
	if (!g_logWriter) return;
	char buf[0x5000];
	va_list args;
	va_start(args, fmt);
	vsnprintf(buf, sizeof(buf), fmt, args);
	va_end(args);
	g_logWriter(level, buf);
}

void logInstallWriter(LogWriter writer) { g_logWriter = writer; }

}  // namespace reindexer
