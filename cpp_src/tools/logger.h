#pragma once

#include <atomic>
#include <functional>
#include "fmt/format.h"
#include "tools/assertrx.h"

typedef std::function<void(int level, char* msg)> LogWriter;

namespace reindexer {

namespace logger_details {

extern std::atomic<int> g_LogLevel;
// Non-const char* is required for cbinding to interact with CGO
void logPrintImpl(int level, char* buf);

}  // namespace logger_details

inline void logPrint(int level, char* buf) {
	if (logger_details::g_LogLevel.load(std::memory_order_relaxed) >= level) {
		logger_details::logPrintImpl(level, buf);
	}
}

inline void logPrint(int level, std::string_view buf) {
	if (logger_details::g_LogLevel.load(std::memory_order_relaxed) >= level) {
		std::string str(buf);
		logger_details::logPrintImpl(level, &str[0]);
	}
}

#if REINDEX_CORE_BUILD
// Core build should not contain macro duplication
#ifdef logFmt
static_assert(false, "Macro conflict");
#endif	// logFmt
#endif	// REINDEX_CORE_BUILD

#ifndef logFmt
// Using macro to avoid arguments calculation before global log level check
#define logFmt(__level, __fmt, ...)                                                                                                \
	if (reindexer::logger_details::g_LogLevel.load(std::memory_order_relaxed) >= __level) {                                        \
		try {                                                                                                                      \
			auto __str = fmt::format(__fmt, ##__VA_ARGS__);                                                                        \
			reindexer::logger_details::logPrintImpl(__level, &__str[0]);                                                           \
		} catch (std::exception & e) {                                                                                             \
			fprintf(stderr, "reindexer error: unexpected exception in logFmt(%s:%s:%d): '%s'\n", __FILE__, __FUNCTION__, __LINE__, \
					e.what());                                                                                                     \
			assertrx_dbg(false);                                                                                                   \
		}                                                                                                                          \
	}
#endif	// logFmt

enum class [[nodiscard]] LoggerPolicy : int { NotInit, WithLocks, WithoutLocks };
void logInstallWriter(LogWriter writer, LoggerPolicy mode, int globalLogLevel);

}  // namespace reindexer
