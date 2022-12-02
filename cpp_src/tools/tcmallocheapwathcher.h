#pragma once

#if REINDEX_WITH_GPERFTOOLS

#include "spdlog/spdlog.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

class MallocExtension;

namespace reindexer {

class TCMallocHeapWathcher {
public:
	TCMallocHeapWathcher();
	explicit TCMallocHeapWathcher(MallocExtension *mallocExtention, int64_t cacheLimit, float maxCacheRatio);
	explicit TCMallocHeapWathcher(MallocExtension *mallocExtention, int64_t cacheLimit, float maxCacheRatio,
								  std::shared_ptr<spdlog::logger> logger);

	TCMallocHeapWathcher(const TCMallocHeapWathcher &) = delete;
	TCMallocHeapWathcher &operator=(const TCMallocHeapWathcher &) = delete;
	TCMallocHeapWathcher(TCMallocHeapWathcher &&) = default;
	TCMallocHeapWathcher &operator=(TCMallocHeapWathcher &&) = default;
	~TCMallocHeapWathcher();

	void CheckHeapUsagePeriodic();

private:
	MallocExtension *mallocExtention_;
	int64_t cacheLimit_;
	float maxCacheRatio_;
	std::chrono::microseconds heapInspectionPeriod_;
	std::chrono::microseconds heapChunkReleasePeriod_;
	std::shared_ptr<spdlog::logger> logger_;
	std::chrono::time_point<std::chrono::steady_clock> deadline_;

	template <typename... Args>
	void logDebug(Args &&...args);
};

}  // namespace reindexer

#endif
