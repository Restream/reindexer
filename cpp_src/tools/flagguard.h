#pragma once

#include <assert.h>
#include <atomic>

namespace reindexer {

template <bool GuardValue>
class FlagGuard {
public:
	FlagGuard(bool& flag) : flag_(flag) { flag_ = GuardValue; }

	~FlagGuard() {
		assertrx(flag_ == GuardValue);
		flag_ = !GuardValue;
	}

private:
	bool& flag_;
};

template <typename CounterT, std::memory_order MemoryOrdering>
class CounterGuard {
public:
	CounterGuard(std::atomic<CounterT>& counter) noexcept : counter_(counter), owns_(true) { counter_.fetch_add(1, MemoryOrdering); }
	void Reset() noexcept {
		if (owns_) {
			owns_ = false;
			counter_.fetch_sub(1, MemoryOrdering);
		}
	}
	~CounterGuard() {
		if (owns_) {
			counter_.fetch_sub(1, MemoryOrdering);
			assertrx(counter_ >= 0);
		}
	}

private:
	std::atomic<CounterT>& counter_;
	bool owns_ = false;
};

using CounterGuardAIR32 = CounterGuard<int32_t, std::memory_order_relaxed>;
using FlagGuardT = FlagGuard<true>;
using FlagGuardF = FlagGuard<false>;

}  // namespace reindexer
