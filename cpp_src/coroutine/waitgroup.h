#pragma once

#include "coroutine.h"

namespace reindexer {
namespace coroutine {

/// @class Allows to await specified number of coroutines
class [[nodiscard]] wait_group {
public:
	/// Add specified number of coroutines to wait
	void add(size_t cnt) noexcept { wait_cnt_ += cnt; }
	/// Should be called on coroutine's exit
	void done() {
		assertrx(wait_cnt_);
		if (--wait_cnt_ == 0 && waiter_) {
			std::ignore = resume(waiter_);
		}
	}
	/// Await coroutines
	void wait() {
		assertrx(waiter_ == 0);
		waiter_ = current();
		while (wait_cnt_) {
			assertrx(waiter_);
			suspend();
		}
		waiter_ = 0;
	}
	/// Await next coroutine
	void wait_next() {
		assertrx(waiter_ == 0);
		waiter_ = current();
		if (wait_cnt_) {
			assertrx(waiter_);
			suspend();
		}
		waiter_ = 0;
	}
	/// Get await count
	size_t wait_count() const noexcept { return wait_cnt_; }

private:
	size_t wait_cnt_ = 0;
	routine_t waiter_ = 0;
};

/// @class Allows to call done() method for wait_group on guards destruction
class [[nodiscard]] wait_group_guard {
public:
	wait_group_guard(wait_group& wg) noexcept : wg_(wg) {}
	~wait_group_guard() { wg_.done(); }

private:
	wait_group& wg_;
};

}  // namespace coroutine
}  // namespace reindexer
