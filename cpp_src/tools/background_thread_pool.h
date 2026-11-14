#pragma once

#include <cstddef>
#include <tuple>
#include "core/type_consts.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"
#include "vendor/thread-pool/BS_thread_pool.hpp"

namespace reindexer {

// Returns the process-wide background pool. The first request defines its size;
// subsequent requests with a different size continue using the existing pool.
inline BS::thread_pool<>& GetBackgroundThreadPool(size_t configuredThreads = 0) {
	const unsigned threadCount = ResolveBackgroundThreads(configuredThreads);
	struct [[nodiscard]] Pool {
		explicit Pool(unsigned threads)
			: threadCount(threads),
			  pool(threads, [] { std::ignore = BS::this_thread::set_os_thread_priority(BS::os_thread_priority::below_normal); }) {}

		const unsigned threadCount;
		BS::thread_pool<> pool;
	};

	static Pool& instance = *new Pool(threadCount);
	if (instance.threadCount != threadCount) {
		logFmt(LogWarning, "Background thread pool is already initialized with {} threads. Requested {} threads are ignored.",
			   instance.threadCount, threadCount);
	}
	return instance.pool;
}

}  // namespace reindexer
