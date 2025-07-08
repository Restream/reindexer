#pragma once

#include <cstdlib>
#include <mutex>
#include <vector>
#include "estl/mutex.h"
#include "tools/clock.h"

namespace reindexer {

template <typename Mutex>
class PerfStatCounter {
public:
	PerfStatCounter();
	void Hit(std::chrono::microseconds time) noexcept;
	void LockHit(std::chrono::microseconds time) noexcept;
	std::chrono::microseconds MaxTime() const noexcept { return maxTime_; }
	void Reset() noexcept;
	template <class T>
	T Get() noexcept {
		std::lock_guard<Mutex> lck(mtx_);
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

using PerfStatCounterMT = PerfStatCounter<std::mutex>;
using PerfStatCounterST = PerfStatCounter<dummy_mutex>;

template <typename Mutex>
class PerfStatCalculator {
public:
	PerfStatCalculator(PerfStatCounter<Mutex>& counter, bool enable) noexcept : counter_(&counter), enable_(enable) {
		if (enable_) {
			tmStart_ = system_clock_w::now();
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
using PerfStatCalculatorMT = PerfStatCalculator<std::mutex>;
using PerfStatCalculatorST = PerfStatCalculator<dummy_mutex>;

template <typename IntT, typename Mutex>
class QuantityCounter {
public:
	struct Stats {
		double avg = 0.0;
		IntT minValue = 0;
		IntT maxValue = 0;
		size_t hitsCount = 0;
	};

	void Count(IntT quantity) noexcept {
		std::unique_lock<Mutex> lck(mtx_);
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
		std::unique_lock<Mutex> lck(mtx_);
		return stats_;
	}
	void Reset() noexcept {
		std::unique_lock<Mutex> lck(mtx_);
		stats_ = Stats();
	}

private:
	mutable Mutex mtx_;
	Stats stats_;
};
template <typename IntT>
using QuantityCounterMT = QuantityCounter<IntT, std::mutex>;
template <typename IntT>
using QuantityCounterST = QuantityCounter<IntT, dummy_mutex>;

}  // namespace reindexer
