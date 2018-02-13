#pragma once

typedef void (*LogWriter)(int level, char *msg);

namespace reindexer {

void logPrintf(int level, const char *fmt, ...);
void logInstallWriter(LogWriter writer);

}  // namespace reindexer
