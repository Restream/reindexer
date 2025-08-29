#if REINDEX_WITH_GPERFTOOLS
#include "tcmallocheapwathcher.h"

#include <gperftools/malloc_extension.h>
#include <cmath>

namespace reindexer {

static const size_t kReleaseChunkSize = 1024 * 1048576;		///< Max size in bytes to release in one interation
static const size_t kHeapFreeMinThreshold = 200 * 1048576;	///< Do not release parts if pageHeapFree less than that

TCMallocHeapWathcher::TCMallocHeapWathcher() : TCMallocHeapWathcher(nullptr, -1, -1.0) {}

TCMallocHeapWathcher::TCMallocHeapWathcher(MallocExtension* mallocExtention, int64_t cacheLimit, float maxCacheRatio,
										   std::shared_ptr<spdlog::logger> logger)
	: mallocExtention_(mallocExtention),
	  cacheLimit_(cacheLimit),
	  maxCacheRatio_(maxCacheRatio),
	  heapInspectionPeriod_(std::chrono::seconds(10)),
	  heapChunkReleasePeriod_(std::chrono::milliseconds(100)),
	  logger_(std::move(logger)) {}

TCMallocHeapWathcher::TCMallocHeapWathcher(MallocExtension* mallocExtention, int64_t cacheLimit, float maxCacheRatio)
	: TCMallocHeapWathcher(mallocExtention, cacheLimit, maxCacheRatio, nullptr) {}

template <typename... Args>
void TCMallocHeapWathcher::logDebug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
	if (logger_) {
		logger_->debug(fmt, std::forward<Args>(args)...);
	}
}

TCMallocHeapWathcher::~TCMallocHeapWathcher() { logDebug("Heap watcher destructed"); }

void TCMallocHeapWathcher::CheckHeapUsagePeriodic() {
	static std::once_flag startupFirstCallFlag;
	std::call_once(startupFirstCallFlag, [&]() {
		logDebug(
			"Heap Watcher: initial inspection. \n (AllocatorCacheLimit {0}\n, AllocatorCachePart {1},\n "
			"HeapInspectionPeriod(msec) {2},\nHeapChunkReleaseInterval(msec) {3},\n tc_mallocAvailable {4}), "
			"now: {5},\n first deadline: {6}",
			cacheLimit_, maxCacheRatio_, std::chrono::duration_cast<std::chrono::milliseconds>(heapInspectionPeriod_).count(),
			std::chrono::duration_cast<std::chrono::milliseconds>(heapChunkReleasePeriod_).count(), mallocExtention_ ? "true" : "false",
			std::chrono::duration_cast<std::chrono::milliseconds>(ClockT::now_coarse().time_since_epoch()).count(),
			std::chrono::duration_cast<std::chrono::milliseconds>(deadline_.time_since_epoch()).count());
	});

	if (!mallocExtention_) {
		return;
	}

	if ((cacheLimit_ > 0) || (maxCacheRatio_ > 0)) {
		if (ClockT::now_coarse() < deadline_) {
			return;
		}

		size_t allocated = 0;
		mallocExtention_->GetNumericProperty("generic.current_allocated_bytes", &allocated);

		size_t pageHeapFree = 0;
		mallocExtention_->GetNumericProperty("tcmalloc.pageheap_free_bytes", &pageHeapFree);

		int64_t sizeToRelease = 0;
		if (cacheLimit_ > 0) {
			sizeToRelease = pageHeapFree - cacheLimit_;
		}

		if ((sizeToRelease <= 0) && (maxCacheRatio_ > 0)) {
			if ((pageHeapFree > kHeapFreeMinThreshold) && float(double(pageHeapFree) / double(allocated)) > maxCacheRatio_) {
				sizeToRelease = std::llround(std::ceil(pageHeapFree - allocated * maxCacheRatio_));
			}
		}

		auto delayDuration = heapInspectionPeriod_;
		if (sizeToRelease > 0) {
			// cast is safe, since (sizeToRelease > 0)
			if (static_cast<size_t>(sizeToRelease) > kReleaseChunkSize) {
				sizeToRelease = kReleaseChunkSize;
				// release rest in chunks every 100milliseconds
				delayDuration = heapChunkReleasePeriod_;
			}
			logDebug("Heap Watcher: Releasing to system (bytes): {0} ({1})", sizeToRelease, static_cast<size_t>(sizeToRelease));
			mallocExtention_->ReleaseToSystem(static_cast<size_t>(sizeToRelease));
		}

		deadline_ = ClockT::now_coarse() + delayDuration;
	}
}

}  // namespace reindexer

#endif	// REINDEX_WITH_GPERFTOOLS
