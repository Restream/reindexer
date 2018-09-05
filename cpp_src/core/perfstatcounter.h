#pragma once

#include <stdlib.h>
#include <chrono>
#include <mutex>
#include "estl/shared_mutex.h"

namespace reindexer {

template <typename Mutex>
class PerfStatCounter {
public:
	PerfStatCounter() {}
	void Hit(std::chrono::microseconds time);
	void LockHit(std::chrono::microseconds time);
	void lap();
	template <class T>
	T Get() {
		std::unique_lock<Mutex> lck(mtx_);
		lap();
		return T{totalHitCount,
				 size_t(totalTime.count() / (totalHitCount ? totalHitCount : 1)),
				 size_t(totalLockTime.count() / (totalHitCount ? totalHitCount : 1)),
				 avgHitCount,
				 size_t(avgTime.count() / (avgHitCount ? avgHitCount : 1)),
				 size_t(avgLockTime.count() / (avgHitCount ? avgHitCount : 1))};
	}

protected:
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
	Mutex mtx_;
};

using PerfStatCounterMT = PerfStatCounter<std::mutex>;
using PerfStatCounterST = PerfStatCounter<dummy_mutex>;

template <typename Mutex>
class PerfStatCalculator {
public:
	PerfStatCalculator(PerfStatCounter<Mutex> &counter, bool enable) : counter_(counter), enable_(enable) {
		if (enable_) tmStart = std::chrono::high_resolution_clock::now();
	}
	~PerfStatCalculator() {
		if (enable_)
			counter_.Hit(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart));
	}
	void LockHit() {
		if (enable_)
			counter_.LockHit(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart));
	}

	std::chrono::time_point<std::chrono::high_resolution_clock> tmStart;
	PerfStatCounter<Mutex> &counter_;
	bool enable_;
};
using PerfStatCalculatorMT = PerfStatCalculator<std::mutex>;
using PerfStatCalculatorST = PerfStatCalculator<dummy_mutex>;
}  // namespace reindexer
