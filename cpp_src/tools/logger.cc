#include "tools/logger.h"
#include <atomic>
#include "estl/shared_mutex.h"
#include "estl/smart_lock.h"

namespace reindexer {

LogWriter g_logWriter;
shared_timed_mutex g_LoggerLock;
std::atomic_bool g_MtLogger = {true};

void write(int level, char *buf) {
	if (!g_logWriter) return;
	g_logWriter(level, buf);
}

void logPrint(int level, char *buf) {
	if (g_MtLogger) {
		smart_lock<shared_timed_mutex> lk(g_LoggerLock, false);
		write(level, buf);
	} else {
		write(level, buf);
	}
}

void logInstallWriter(LogWriter writer, bool multithreaded) {
	if (g_MtLogger || multithreaded) {
		smart_lock<shared_timed_mutex> lk(g_LoggerLock, true);
		g_logWriter = std::move(writer);
		g_MtLogger = multithreaded;
	} else {
		g_logWriter = std::move(writer);
		g_MtLogger = multithreaded;
	}
}

}  // namespace reindexer
