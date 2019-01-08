#include "tools/logger.h"
#include <stdarg.h>
#include <cstdio>

namespace reindexer {

LogWriter g_logWriter = nullptr;

void logPrint(int level, char *buf) {
	if (!g_logWriter) return;
	g_logWriter(level, buf);
}

void logInstallWriter(LogWriter writer) { g_logWriter = writer; }

}  // namespace reindexer
