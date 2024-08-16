#pragma once

#include <atomic>
#include <functional>
#include "fmt/printf.h"

typedef std::function<void(int level, char* msg)> LogWriter;

namespace reindexer {

namespace logger_details {

extern std::atomic<int> g_LogLevel;
void logPrintImpl(int level, char* buf);

}  // namespace logger_details

inline void logPrint(int level, char* buf) {
	if (logger_details::g_LogLevel.load(std::memory_order_relaxed) >= level) {
		logger_details::logPrintImpl(level, buf);
	}
}

#if REINDEX_CORE_BUILD
// Core build should not contain macro duplication
#ifdef logPrintf
static_assert(false, "Macro conflict");
#endif	// logPrintf
#ifdef logFmt
static_assert(false, "Macro conflict");
#endif	// logFmt
#endif	// REINDEX_CORE_BUILD

#ifndef logPrintf
// Using macro to avoid arguments calculation before global log level check
#define logPrintf(__level, __fmt, ...)                                                      \
	if (reindexer::logger_details::g_LogLevel.load(std::memory_order_relaxed) >= __level) { \
		auto __str = fmt::sprintf(__fmt, ##__VA_ARGS__);                                    \
		reindexer::logger_details::logPrintImpl(__level, &__str[0]);                        \
	}
#endif	// logPrintf

#ifndef logFmt
// Using macro to avoid arguments calculation before global log level check
#define logFmt(__level, __fmt, ...)                                                         \
	if (reindexer::logger_details::g_LogLevel.load(std::memory_order_relaxed) >= __level) { \
		auto __str = fmt::format(__fmt, ##__VA_ARGS__);                                     \
		reindexer::logger_details::logPrintImpl(__level, &__str[0]);                        \
	}
#endif	// logFmt

enum class LoggerPolicy : int { NotInit, WithLocks, WithoutLocks };
void logInstallWriter(LogWriter writer, LoggerPolicy mode, int globalLogLevel);

}  // namespace reindexer
