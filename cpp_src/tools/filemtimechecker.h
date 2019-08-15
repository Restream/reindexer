#pragma once

#include <chrono>
#include "fsops.h"

namespace reindexer {

class FileMTimeChecker {
public:
	void SetFilepath(std::string filepath, bool enable = false) noexcept {
		assert(!hasFilepath_.load(std::memory_order_acquire));
		filepath_ = std::move(filepath);
		auto stat = fs::StatTime(filepath_);
		lastReplConfMTime_.store(stat.mtime, std::memory_order_relaxed);
		hasFilepath_.store(true, std::memory_order_release);
		if (enable) {
			isEnabled_.store(true, std::memory_order_release);
		}
	}

	Error Enable() noexcept {
		if (!hasFilepath_.load(std::memory_order_acquire)) {
			return Error(errLogic, "Filepath for FileMTimeChecker is not set");
		}
		isEnabled_.store(true, std::memory_order_release);
		return errOK;
	}

	bool FileWasModified() {
		if (!isEnabled_.load(std::memory_order_acquire) || !hasFilepath_.load(std::memory_order_acquire)) {
			return false;
		}
		auto stat = fs::StatTime(filepath_);
		if (stat.mtime > 0) {
			auto lastReplConfMTime = lastReplConfMTime_.load(std::memory_order_acquire);
			if (lastReplConfMTime != stat.mtime) {
				return lastReplConfMTime_.compare_exchange_strong(lastReplConfMTime, stat.mtime, std::memory_order_acq_rel);
			}
		}
		return false;
	}

private:
	std::string filepath_;
	std::atomic<bool> hasFilepath_{false};
	std::atomic<bool> isEnabled_{false};
	std::atomic<int64_t> lastReplConfMTime_{-1};
};

}  // namespace reindexer
