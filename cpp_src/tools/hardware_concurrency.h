#pragma once

#include <algorithm>
#include <cstddef>
#include <thread>

namespace reindexer {

// Wrapper to handle situation, when std::thread::hardware_concurrency returns 0.
inline unsigned hardware_concurrency() noexcept { return std::max(std::thread::hardware_concurrency(), 1u); }

// Hard upper bound for the process-wide background thread pool (config cannot exceed this).
inline constexpr unsigned kMaxBackgroundThreads = 128;

// Auto-select background pool size from CPU core count (same caps as former getMaxBuildWorkers).
// Uses std::thread::hardware_concurrency directly (not reindexer's wrapper).
inline unsigned GetMaxBackgroundThreads() noexcept {
	const auto hw = std::thread::hardware_concurrency();
	if (!hw) {
		return 4;
	} else if (hw > 32) {
		return 16;
	} else if (hw > 24) {
		return 12;
	} else if (hw > 8) {
		return 8;
	}
	return hw;
}

// Resolve configured background thread count.
// 0 means the thread count is chosen automatically based on the number of CPU cores.
// Values above kMaxBackgroundThreads are clamped.
inline unsigned ResolveBackgroundThreads(size_t configured) noexcept {
	if (!configured) {
		return GetMaxBackgroundThreads();
	}
	return unsigned(std::min(configured, size_t(kMaxBackgroundThreads)));
}

}  // namespace reindexer
