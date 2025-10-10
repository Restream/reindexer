#pragma once

#include <deque>
#include <set>
#include "coroutine.h"

namespace reindexer {
namespace coroutine {

/// @class Mutex for coroutines
class [[nodiscard]] mutex {
public:
	void lock() {
		bool await = false;
		const auto id = current();
		assert(id);
		assert(id != owner_);
		while (owner_) {
			await = true;
			// waiters_.insert(id);
			waiters_.emplace_back(id);
			suspend();
		}
		if (await) {
			assert(owner_ != id);
			// assert(waiters_.count(id) == 0);
		}
		owner_ = id;
	}
	void unlock() {
		assert(owner_);
		owner_ = 0;
		// auto waiters = std::move(waiters_);
		// waiters_.clear();
		if (!waiters_.empty()) {
			auto next = waiters_.front();
			waiters_.pop_front();
			resume(next);
		}
		// for (auto id : waiters) {
		//	resume(id);
		//}
	}

private:
	// std::set<routine_t> waiters_;
	std::deque<routine_t> waiters_;
	routine_t owner_ = 0;
};

}  // namespace coroutine
}  // namespace reindexer
