
#include "perfstatcounter.h"
#include "estl/shared_mutex.h"

#include <math.h>
#include <numeric>

static const size_t kMaxValuesCountForStddev = 100;

namespace reindexer {

template <typename Mutex>
PerfStatCounter<Mutex>::PerfStatCounter() {
	lastValuesUs.reserve(kMaxValuesCountForStddev);
}

template <typename Mutex>
void PerfStatCounter<Mutex>::Hit(std::chrono::microseconds time) {
	std::unique_lock<Mutex> lck(mtx_);
	totalTime += time;
	calcTime += time;
	calcHitCount++;
	totalHitCount++;
	lastValuesUs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(calcTime).count());
	doCalculations();
	lap();
}

template <typename Mutex>
void PerfStatCounter<Mutex>::LockHit(std::chrono::microseconds time) {
	std::unique_lock<Mutex> lck(mtx_);
	calcLockTime += time;
	totalLockTime += time;
	doCalculations();
}

template <typename Mutex>
void PerfStatCounter<Mutex>::Reset() {
	static const PerfStatCounter<Mutex> defaultCounter;
	std::unique_lock<Mutex> lck(mtx_);
	totalHitCount = defaultCounter.totalHitCount;
	totalTime = defaultCounter.totalTime;
	totalLockTime = defaultCounter.totalLockTime;
	avgHitCount = defaultCounter.avgHitCount;
	avgTime = defaultCounter.avgTime;
	avgLockTime = defaultCounter.avgLockTime;
	calcHitCount = defaultCounter.calcHitCount;
	calcTime = defaultCounter.calcTime;
	calcLockTime = defaultCounter.calcLockTime;
	calcStartTime = defaultCounter.calcStartTime;
	stddev = defaultCounter.stddev;
	minTime = defaultCounter.minTime;
	maxTime = defaultCounter.maxTime;
}

template <typename Mutex>
void PerfStatCounter<Mutex>::doCalculations() {
	if (calcTime > maxTime) {
		maxTime = calcTime;
	}
	if (calcTime < minTime) {
		minTime = calcTime;
	}
	if (lastValuesUs.size() > 1) {
		double avg = std::accumulate(lastValuesUs.begin(), lastValuesUs.end(), 0.0) / lastValuesUs.size();
		double dispersion = 0.0;
		for (size_t i = 0; i < lastValuesUs.size(); ++i) {
			dispersion += pow(lastValuesUs[i] - avg, 2);
		}
		dispersion /= lastValuesUs.size();
		stddev = sqrt(dispersion);
	}
	if (lastValuesUs.size() > kMaxValuesCountForStddev) {
		lastValuesUs.clear();
		lastValuesUs.reserve(kMaxValuesCountForStddev);
	}
}

template <typename Mutex>
void PerfStatCounter<Mutex>::lap() {
	auto now = std::chrono::high_resolution_clock::now();
	std::chrono::microseconds elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - calcStartTime);
	if (elapsed < std::chrono::microseconds(1000000)) return;
	avgHitCount = calcHitCount;
	avgTime = calcTime;
	calcTime = std::chrono::microseconds(0);
	avgLockTime = calcLockTime;
	calcLockTime = std::chrono::microseconds(0);
	calcHitCount = 0;
	calcStartTime = now;
	lastValuesUs.clear();
	lastValuesUs.reserve(kMaxValuesCountForStddev);
}

template class PerfStatCounter<std::mutex>;
template class PerfStatCounter<dummy_mutex>;

}  // namespace reindexer
