#include "tools/logger.h"
#include "core/type_consts.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "estl/spin_lock.h"

namespace reindexer {

namespace logger_details {

static LogWriter g_logWriter;
static read_write_spinlock g_LoggerMtx;
static std::atomic<LoggerPolicy> g_MtLogger = {LoggerPolicy::NotInit};
std::atomic<int> g_LogLevel = LogTrace;

RX_ALWAYS_INLINE void write(int level, char* buf) {
	if (g_logWriter) {
		g_logWriter(level, buf);
	}
}

void logPrintImpl(int level, char* buf) RX_REQUIRES(!g_LoggerMtx) {
	switch (g_MtLogger.load(std::memory_order_relaxed)) {
		case LoggerPolicy::NotInit:
		case LoggerPolicy::WithLocks: {
			shared_lock lck(g_LoggerMtx);
			write(level, buf);
		} break;
		case LoggerPolicy::WithoutLocks:
			write(level, buf);
			break;
	}
}
}  // namespace logger_details

void logInstallWriter(LogWriter writer, LoggerPolicy policy, int globalLogLevel) {
	std::string errorText;

	static mutex g_LoggerPolicyMtx;
	unique_lock lck(g_LoggerPolicyMtx);

	const auto curPolicy = logger_details::g_MtLogger.load(std::memory_order_relaxed);
	if (curPolicy != LoggerPolicy::NotInit && policy != curPolicy) {
		errorText =
			fmt::format("Attempt to switch logger's lock policy, which was previously set. Current: {}; new: {}. Logger was not changed",
						int(curPolicy), int(policy));
		fputs(errorText.c_str(), stderr);
		fputs("\n", stderr);
		fflush(stderr);
#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN) || defined(RX_WITH_STDLIB_DEBUG)
		std::abort();
#else
		return;
#endif
	}

	logger_details::g_MtLogger.store(policy, std::memory_order_relaxed);
	logger_details::g_LogLevel.store(globalLogLevel, std::memory_order_relaxed);

	if (curPolicy == LoggerPolicy::WithLocks || policy == LoggerPolicy::WithLocks) {
		lock_guard logLck(logger_details::g_LoggerMtx);
		logger_details::g_logWriter = std::move(writer);
	} else {
		logger_details::g_logWriter = std::move(writer);
	}
}

}  // namespace reindexer
