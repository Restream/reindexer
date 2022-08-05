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
	CounterGuard() = default;
	CounterGuard(const CounterGuard&) = delete;
	CounterGuard(CounterGuard&& o) noexcept : counter_(o.counter_) { o.counter_ = nullptr; }
	CounterGuard(std::atomic<CounterT>& counter) noexcept : counter_(&counter) { counter_->fetch_add(1, MemoryOrdering); }
	CounterGuard& operator=(const CounterGuard&) = delete;
	CounterGuard& operator=(CounterGuard&& o) noexcept {
		if (this != &o) {
			Reset();
			counter_ = o.counter_;
			o.counter_ = nullptr;
		}
		return *this;
	}
	void Reset() noexcept {
		if (counter_) {
			counter_->fetch_sub(1, MemoryOrdering);
			counter_ = nullptr;
		}
	}
	~CounterGuard() {
		if (counter_) {
			counter_->fetch_sub(1, MemoryOrdering);
			assertrx(*counter_ >= 0);
		}
	}

private:
	std::atomic<CounterT>* counter_ = nullptr;
};

using CounterGuardAIR32 = CounterGuard<int32_t, std::memory_order_relaxed>;
using CounterGuardAIRL32 = CounterGuard<int32_t, std::memory_order_release>;
using FlagGuardT = FlagGuard<true>;
using FlagGuardF = FlagGuard<false>;

}  // namespace reindexer
