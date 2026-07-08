#pragma once

#include <cassert>
#include <exception>
#include "coroutine.h"
#include "waiters_queue.h"

namespace reindexer::coroutine {

/// @class Mutex for coroutines
class [[nodiscard]] mutex {
public:
	void lock() {
		const auto id = current();
		waiters_queue::hook waiter(id);
		assert(id);
		assert(id != owner_);
		while (owner_) {
			if (!waiter.linked()) {
				waiters_.push_back(waiter);
			}
			suspend();
		}
		// On wake the unlocking side has normally already removed us from the queue (waker-unlink); erase defensively in
		// case we leave the wait for any other reason.
		if (waiter.linked()) {
			waiters_.erase(waiter);
		}
		owner_ = id;
	}
	void unlock() {
		assert(owner_);
		owner_ = 0;
		if (!waiters_.empty()) {
			waiters_queue::hook& waiter = waiters_.front_hook();
			const auto next = waiter.id;
			waiters_.erase(waiter);
			if (std::uncaught_exceptions() > 0) {
				defer_resume(next);
			} else {
				[[maybe_unused]] int res = resume(next);
				assertrx_dbg(res == 0);
			}
		}
	}

private:
	waiters_queue waiters_;
	routine_t owner_ = 0;
};

}  // namespace reindexer::coroutine
