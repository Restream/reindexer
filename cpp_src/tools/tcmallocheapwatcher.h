#pragma once

#if REINDEX_WITH_GPERFTOOLS

#include "spdlog/logger.h"
#include "tools/clock.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

class MallocExtension;

namespace reindexer {

class [[nodiscard]] TCMallocHeapWatcher {
public:
	TCMallocHeapWatcher();
	explicit TCMallocHeapWatcher(MallocExtension* mallocExtention, int64_t cacheLimit, float maxCacheRatio);
	explicit TCMallocHeapWatcher(MallocExtension* mallocExtention, int64_t cacheLimit, float maxCacheRatio,
								  std::shared_ptr<spdlog::logger> logger);

	TCMallocHeapWatcher(const TCMallocHeapWatcher&) = delete;
	TCMallocHeapWatcher& operator=(const TCMallocHeapWatcher&) = delete;
	TCMallocHeapWatcher(TCMallocHeapWatcher&&) = default;
	TCMallocHeapWatcher& operator=(TCMallocHeapWatcher&&) = default;
	~TCMallocHeapWatcher();

	void CheckHeapUsagePeriodic();

private:
	using ClockT = steady_clock_w;

	MallocExtension* mallocExtention_;
	int64_t cacheLimit_;
	float maxCacheRatio_;
	std::chrono::microseconds heapInspectionPeriod_;
	std::chrono::microseconds heapChunkReleasePeriod_;
	std::shared_ptr<spdlog::logger> logger_;
	ClockT::time_point deadline_;

	template <typename... Args>
	void logDebug(spdlog::format_string_t<Args...>, Args&&... args);
};

}  // namespace reindexer

#endif
