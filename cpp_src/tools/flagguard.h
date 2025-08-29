#pragma once

#include <assert.h>
#include <atomic>

namespace reindexer {

template <bool GuardValue>
class [[nodiscard]] FlagGuard {
public:
	FlagGuard(bool& flag) noexcept : flag_(flag) { flag_ = GuardValue; }
	~FlagGuard() {
		assertrx(flag_ == GuardValue);
		flag_ = !GuardValue;
	}

private:
	bool& flag_;
};

template <typename CounterT>
class [[nodiscard]] NACounterGuard {
public:
	NACounterGuard() = default;
	NACounterGuard(const NACounterGuard&) = delete;
	NACounterGuard(NACounterGuard&& o) noexcept : counter_(o.counter_) { o.counter_ = nullptr; }
	NACounterGuard(CounterT& counter) noexcept : counter_(&counter) { ++counter; }
	NACounterGuard& operator=(const NACounterGuard&) = delete;
	NACounterGuard& operator=(NACounterGuard&& o) noexcept {
		if (this != &o) {
			Reset();
			counter_ = o.counter_;
			o.counter_ = nullptr;
		}
		return *this;
	}
	void Reset() noexcept {
		if (counter_) {
			--(*counter_);
			counter_ = nullptr;
		}
	}
	~NACounterGuard() {
		if (counter_) {
			--(*counter_);
			assertrx(*counter_ >= 0);
		}
	}

private:
	CounterT* counter_ = nullptr;
};

template <typename CounterT, std::memory_order MemoryOrdering>
class [[nodiscard]] CounterGuard {
public:
	CounterGuard() = default;
	CounterGuard(const CounterGuard&) = delete;
	CounterGuard(CounterGuard&& o) noexcept : counter_(o.counter_) { o.counter_ = nullptr; }
	CounterGuard(std::atomic<CounterT>& counter, bool increment = true) noexcept : counter_(&counter) {
		if (increment) {
			counter_->fetch_add(1, MemoryOrdering);
		}
	}
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
using CounterGuardIR32 = NACounterGuard<int32_t>;
using FlagGuardT = FlagGuard<true>;
using FlagGuardF = FlagGuard<false>;

}  // namespace reindexer
