#pragma once

#include <stdlib.h>
#include <chrono>
#include <mutex>
#include <vector>
#include "estl/mutex.h"

namespace reindexer {

template <typename Mutex>
class PerfStatCounter {
public:
	PerfStatCounter();
	void Hit(std::chrono::microseconds time);
	void LockHit(std::chrono::microseconds time);
	std::chrono::microseconds MaxTime() const { return maxTime; }
	void Reset();
	template <class T>
	T Get() {
		std::unique_lock<Mutex> lck(mtx_);
		lap();
		return T{totalHitCount,
				 size_t(totalTime.count() / (totalHitCount ? totalHitCount : 1)),
				 size_t(totalLockTime.count() / (totalHitCount ? totalHitCount : 1)),
				 avgHitCount,
				 size_t(avgTime.count() / (avgHitCount ? avgHitCount : 1)),
				 size_t(avgLockTime.count() / (avgHitCount ? avgHitCount : 1)),
				 stddev,
				 size_t(minTime == defaultMinTime() ? 0 : minTime.count()),
				 size_t(maxTime.count())};
	}

protected:
	static std::chrono::microseconds defaultMinTime() {
		static constexpr std::chrono::microseconds defaultValue{std::numeric_limits<size_t>::max() / 2};
		return defaultValue;
	}

	void lap();
	void doCalculations();

	size_t totalHitCount = 0;
	std::chrono::microseconds totalTime = std::chrono::microseconds(0);
	std::chrono::microseconds totalLockTime = std::chrono::microseconds(0);
	size_t avgHitCount = 0;
	std::chrono::microseconds avgTime = std::chrono::microseconds(0);
	std::chrono::microseconds avgLockTime = std::chrono::microseconds(0);
	size_t calcHitCount = 0;
	std::chrono::microseconds calcTime = std::chrono::microseconds(0);
	std::chrono::microseconds calcLockTime = std::chrono::microseconds(0);
	std::chrono::time_point<std::chrono::high_resolution_clock> calcStartTime;
	double stddev = 0.0;
	std::chrono::microseconds minTime = defaultMinTime();
	std::chrono::microseconds maxTime = std::chrono::microseconds(std::numeric_limits<size_t>::min());
	std::vector<size_t> lastValuesUs;
	size_t posInValuesUs = 0;
	Mutex mtx_;
};

using PerfStatCounterMT = PerfStatCounter<std::mutex>;
using PerfStatCounterST = PerfStatCounter<dummy_mutex>;

template <typename Mutex>
class PerfStatCalculator {
public:
	PerfStatCalculator(PerfStatCounter<Mutex> &counter, bool enable) : counter_(&counter), enable_(enable) {
		if (enable_) tmStart = std::chrono::high_resolution_clock::now();
	}
	~PerfStatCalculator() {
		if (enable_)
			counter_->Hit(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart));
	}
	void SetCounter(PerfStatCounter<Mutex> &counter) { counter_ = &counter; }
	void LockHit() {
		if (enable_)
			counter_->LockHit(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart));
	}
	void HitManualy() {
		if (enable_) {
			enable_ = false;
			counter_->Hit(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart));
		}
	}

	std::chrono::time_point<std::chrono::high_resolution_clock> tmStart;
	PerfStatCounter<Mutex> *counter_;
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

	void Count(IntT quantity) {
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
	Stats Get() const {
		std::unique_lock<Mutex> lck(mtx_);
		return stats_;
	}
	void Reset() {
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
