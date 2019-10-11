#pragma once
#include <memory>
#include <mutex>
#include <vector>

namespace reindexer {
template <typename T, size_t maxPoolSize, size_t maxAllocSize = std::numeric_limits<size_t>::max()>
class sync_pool {
public:
	void put(T* obj) {
		std::unique_lock<std::mutex> lck(lck_);
		if (pool_.size() < maxPoolSize)
			pool_.push_back(std::unique_ptr<T>(obj));
		else
			delete obj;
		alloced_--;
	}

	template <typename... Args>
	T* get(Args&&... args) {
		std::unique_lock<std::mutex> lck(lck_);
		if (alloced_ > maxAllocSize) {
			return nullptr;
		}
		alloced_++;
		if (pool_.empty()) {
			return new T(std::forward<Args>(args)...);
		} else {
			auto res = pool_.back().release();
			pool_.pop_back();
			return res;
		}
	}
	void clear() {
		std::unique_lock<std::mutex> lck(lck_);
		pool_.clear();
	}

protected:
	size_t alloced_ = 0;
	std::vector<std::unique_ptr<T>> pool_;
	std::mutex lck_;
};
}  // namespace reindexer
