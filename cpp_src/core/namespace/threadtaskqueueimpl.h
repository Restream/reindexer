#pragma once

#include <atomic>
#include "sparse-map/sparse_hash.h"

namespace reindexer {
class [[nodiscard]] ThreadTaskQueueImpl : public tsl::detail_sparse_hash::ThreadTaskQueue {
public:
	virtual void AddTask(std::function<void()> f) override { queue_.emplace_back(std::move(f)); }
	std::function<void()> GetTask() {
		unsigned i = index_.fetch_add(1);
		if (i >= queue_.size()) {
			return nullptr;
		}
		return std::move(queue_[i]);
	}

private:
	std::vector<std::function<void()>> queue_;
	std::atomic<unsigned> index_{0};
};

}  // namespace reindexer
