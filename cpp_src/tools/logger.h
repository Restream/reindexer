#pragma once

typedef void (*LogWriter)(int level, char *msg);

namespace reindexer {

void logPrintf(int level, const char *fmt, ...)
#ifndef _MSC_VER
	__attribute__((format(printf, 2, 3)))
#endif
	;
void logInstallWriter(LogWriter writer);

}  // namespace reindexer
