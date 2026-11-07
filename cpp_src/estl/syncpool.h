#pragma once

#include <memory>
#include <vector>
#include "estl/lock.h"
#include "estl/mutex.h"

namespace reindexer {

template <typename T, size_t maxPoolSize, size_t maxAllocSize = std::numeric_limits<size_t>::max()>
class [[nodiscard]] sync_pool {
public:
	void put(std::unique_ptr<T> obj) {
		lock_guard lck(lck_);
		if (pool_.size() < maxPoolSize) {
			pool_.emplace_back(std::move(obj));
		}
		alloced_.fetch_sub(1, std::memory_order_relaxed);
	}

	template <typename... Args>
	std::unique_ptr<T> get(int usedCount, Args&&... args) {
		unique_lock lck(lck_);
		if (alloced_.load(std::memory_order_relaxed) > maxAllocSize + usedCount) {
			return nullptr;
		}
		alloced_.fetch_add(1, std::memory_order_relaxed);
		if (pool_.empty()) {
			lck.unlock();
			return std::unique_ptr<T>{new T(std::forward<Args>(args)...)};
		} else {
			auto res = std::move(pool_.back());
			pool_.pop_back();
			return res;
		}
	}
	void clear() {
		lock_guard lck(lck_);
		pool_.clear();
	}
	size_t Alloced() const noexcept { return alloced_.load(std::memory_order_relaxed); }

protected:
	std::atomic<size_t> alloced_ = 0;
	std::vector<std::unique_ptr<T>> pool_;
	mutex lck_;
};

}  // namespace reindexer
