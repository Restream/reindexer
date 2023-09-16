#include "tools/logger.h"
#include <mutex>
#include "estl/mutex.h"
#include "estl/shared_mutex.h"

namespace reindexer {

static LogWriter g_logWriter;
static read_write_spinlock g_LoggerMtx;
static std::atomic<LoggerPolicy> g_MtLogger = {LoggerPolicy::NotInit};

RX_ALWAYS_INLINE void write(int level, char *buf) {
	if (g_logWriter) {
		g_logWriter(level, buf);
	}
}

void logPrint(int level, char *buf) {
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

void logInstallWriter(LogWriter writer, LoggerPolicy policy) {
	std::string errorText;

	static std::mutex g_LoggerPolicyMtx;
	std::unique_lock lck(g_LoggerPolicyMtx);

	const auto curPolicy = g_MtLogger.load(std::memory_order_relaxed);
	if (curPolicy != LoggerPolicy::NotInit && policy != curPolicy) {
		errorText =
			fmt::sprintf("Attempt to switch logger's lock policy, which was previously set. Current: %d; new: %d. Logger was not changed",
						 int(curPolicy), int(policy));
		fputs(errorText.c_str(), stderr);
		fputs("\n", stderr);
		fflush(stderr);
#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN) || defined(RX_WITH_STDLIB_DEBUG)
		std::abort();
#else
		lck.unlock();
		if (writer) {
			errorText.append(". THIS logger is not active");
			writer(LogError, &errorText[0]);
		}
		return;
#endif
	}

	g_MtLogger.store(policy, std::memory_order_relaxed);

	if (curPolicy == LoggerPolicy::WithLocks || policy == LoggerPolicy::WithLocks) {
		std::lock_guard logLck(g_LoggerMtx);
		g_logWriter = std::move(writer);
	} else {
		g_logWriter = std::move(writer);
	}
}

}  // namespace reindexer
