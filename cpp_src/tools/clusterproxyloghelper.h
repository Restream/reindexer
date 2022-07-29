#pragma once

#include "tools/logger.h"

namespace reindexer {

struct sinkArgs {
	template <typename... Args>
	sinkArgs(Args const &...) {}
};

template <typename... Args>
void clusterProxyLog(int level, const char *fmt, const Args &...args) {
#if RX_ENABLE_CLUSTERPROXY_LOGS
	auto str = fmt::sprintf(fmt, args...);
	logPrint(level, &str[0]);
#else
	sinkArgs{level, fmt, args...};	// -V607
#endif
}

}
