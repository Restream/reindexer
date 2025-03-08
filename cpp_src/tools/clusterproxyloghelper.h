#pragma once

#if RX_ENABLE_CLUSTERPROXY_LOGS
#include "tools/logger.h"
#endif

namespace reindexer {

template <typename... Args>
void clusterProxyLog([[maybe_unused]] int level, [[maybe_unused]] const char* fmt, [[maybe_unused]] const Args&... args) {
#if RX_ENABLE_CLUSTERPROXY_LOGS
	logPrintf(level, fmt, args...);
#endif
}

}  // namespace reindexer
