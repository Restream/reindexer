#pragma once

#include "vendor/fmt/base.h"

#if RX_ENABLE_CLUSTERPROXY_LOGS
#include "tools/logger.h"
#endif

namespace reindexer {

template <typename... Args>
void clusterProxyLog([[maybe_unused]] int level, [[maybe_unused]] fmt::format_string<Args...> fmt, [[maybe_unused]] Args&&... args) {
#if RX_ENABLE_CLUSTERPROXY_LOGS
	logFmt(level, fmt, std::forward<Args>(args)...);
#endif
}

}  // namespace reindexer
