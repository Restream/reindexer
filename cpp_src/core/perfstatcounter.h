#pragma once

#include <cstdlib>
#include <vector>
#include "estl/dummy_mutex.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "tools/clock.h"

namespace reindexer {

template <typename Mutex>
class [[nodiscard]] PerfStatCounter {
public:
	PerfStatCounter();
	void Hit(std::chrono::microseconds time) noexcept;
	void LockHit(std::chrono::microseconds time) noexcept;
	std::chrono::microseconds MaxTime() const noexcept { return maxTime_; }
	void Reset() noexcept;
	template <class T>
	T Get() noexcept {
		lock_guard lck(mtx_);
		lap();
		return T{.totalHitCount = totalHitCount_,
				 .totalAvgTimeUs = size_t(totalTime_.count() / (totalHitCount_ ? totalHitCount_ : 1)),
				 .totalAvgLockTimeUs = size_t(totalLockTime_.count() / (totalHitCount_ ? totalHitCount_ : 1)),
				 .lastSecHitCount = lastSecHitCount_,
				 .lastSecAvgTimeUs = size_t(lastSecTotalTime_.count() / (lastSecHitCount_ ? lastSecHitCount_ : 1)),
				 .lastSecAvgLockTimeUs = size_t(lastSecTotalLockTime_.count() / (lastSecHitCount_ ? lastSecHitCount_ : 1)),
				 .stddev = stddev_,
				 .minTimeUs = size_t(minTime_ == std::chrono::microseconds::max() ? 0 : minTime_.count()),
				 .maxTimeUs = size_t(maxTime_.count())};
	}

private:
	void lap() noexcept;

	size_t totalHitCount_ = 0;
	std::chrono::microseconds totalTime_ = std::chrono::microseconds(0);
	std::chrono::microseconds totalLockTime_ = std::chrono::microseconds(0);
	size_t lastSecHitCount_ = 0;
	std::chrono::microseconds lastSecTotalTime_ = std::chrono::microseconds(0);
	std::chrono::microseconds lastSecTotalLockTime_ = std::chrono::microseconds(0);
	size_t calcHitCount_ = 0;
	std::chrono::microseconds calcTime_ = std::chrono::microseconds(0);
	std::chrono::microseconds calcLockTime_ = std::chrono::microseconds(0);
	system_clock_w::time_point calcStartTime_ = system_clock_w::now_coarse();
	double stddev_ = 0.0;
	std::chrono::microseconds minTime_ = std::chrono::microseconds::max();
	std::chrono::microseconds maxTime_ = std::chrono::microseconds(0);
	std::vector<size_t> lastValuesUs_;
	size_t posInValuesUs_ = 0;
	mutable Mutex mtx_;
};

using PerfStatCounterMT = PerfStatCounter<mutex>;
using PerfStatCounterST = PerfStatCounter<DummyMutex>;

template <typename Mutex>
class [[nodiscard]] PerfStatCounterCountAvg {
public:
	PerfStatCounterCountAvg() = default;
	void Hit(size_t val) noexcept;
	void Reset() noexcept;
	float Get() noexcept {
		lock_guard lck(mtx_);
		lap();
		return lastSecondAvgValue;
	}

private:
	void lap() noexcept;

	size_t hitCount_ = 0;
	size_t valueCount = 0;
	float lastSecondAvgValue = 0.0;
	system_clock_w::time_point calcStartTime_ = system_clock_w::now_coarse();
	mutable Mutex mtx_;
};

using PerfStatCounterCountAvgMT = PerfStatCounterCountAvg<mutex>;
using PerfStatCounterCountAvgST = PerfStatCounterCountAvg<DummyMutex>;

template <typename Mutex>
class [[nodiscard]] PerfStatCalculator {
public:
	PerfStatCalculator(PerfStatCounter<Mutex>& counter, bool enable) noexcept : counter_(&counter), enable_(enable) {
		if (enable_) {
			tmStart_ = system_clock_w::now();
		}
	}
	PerfStatCalculator(PerfStatCounter<Mutex>& counter, system_clock_w::time_point tmStart, bool enable) noexcept
		: counter_(&counter), enable_(enable) {
		if (enable_) {
			tmStart_ = tmStart;
		}
	}

	~PerfStatCalculator() {
		if (enable_) {
			counter_->Hit(std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - tmStart_));
		}
	}
	void SetCounter(PerfStatCounter<Mutex>& counter) noexcept { counter_ = &counter; }
	void Disable() noexcept { enable_ = false; }
	void LockHit() noexcept {
		if (enable_) {
			counter_->LockHit(std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - tmStart_));
		}
	}
	void HitManualy() noexcept {
		if (enable_) {
			enable_ = false;
			counter_->Hit(std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - tmStart_));
		}
	}

private:
	system_clock_w::time_point tmStart_;
	PerfStatCounter<Mutex>* counter_;
	bool enable_;
};
using PerfStatCalculatorMT = PerfStatCalculator<mutex>;
using PerfStatCalculatorST = PerfStatCalculator<DummyMutex>;

template <typename IntT, typename Mutex>
class [[nodiscard]] QuantityCounter {
public:
	struct [[nodiscard]] Stats {
		double avg = 0.0;
		IntT minValue = 0;
		IntT maxValue = 0;
		size_t hitsCount = 0;
	};

	void Count(IntT quantity) noexcept {
		unique_lock lck(mtx_);
		stats_.avg = (stats_.avg * stats_.hitsCount + quantity) / (stats_.hitsCount + 1);
		if (stats_.hitsCount++) {
			if (quantity < stats_.minValue) {
				stats_.minValue = quantity;
			} else if (quantity > stats_.maxValue) {
				stats_.maxValue = quantity;
			}
		} else {
			stats_.maxValue = stats_.minValue = quantity;
		}
	}
	Stats Get() const noexcept {
		unique_lock lck(mtx_);
		return stats_;
	}
	void Reset() noexcept {
		unique_lock lck(mtx_);
		stats_ = Stats();
	}

private:
	mutable Mutex mtx_;
	Stats stats_;
};
template <typename IntT>
using QuantityCounterMT = QuantityCounter<IntT, mutex>;
template <typename IntT>
using QuantityCounterST = QuantityCounter<IntT, DummyMutex>;

}  // namespace reindexer
