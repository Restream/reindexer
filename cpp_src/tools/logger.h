#pragma once

#include "spdlog/fmt/bundled/printf.h"
#include "spdlog/fmt/fmt.h"
typedef void (*LogWriter)(int level, char *msg);

namespace reindexer {

void logPrint(int level, char *buf);
template <typename... Args>
void logPrintf(int level, const char *fmt, const Args &... args) {
	auto str = fmt::sprintf(fmt, args...);
	logPrint(level, &str[0]);
}

void logInstallWriter(LogWriter writer);

}  // namespace reindexer
