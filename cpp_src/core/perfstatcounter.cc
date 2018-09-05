
#include "perfstatcounter.h"
#include "estl/shared_mutex.h"

namespace reindexer {

template <typename Mutex>
void PerfStatCounter<Mutex>::Hit(std::chrono::microseconds time) {
	std::unique_lock<Mutex> lck(mtx_);
	totalTime += time;
	calcTime += time;
	calcHitCount++;
	totalHitCount++;
	lap();
}

template <typename Mutex>
void PerfStatCounter<Mutex>::LockHit(std::chrono::microseconds time) {
	std::unique_lock<Mutex> lck(mtx_);
	calcLockTime += time;
	totalLockTime += time;
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
}

template class PerfStatCounter<std::mutex>;
template class PerfStatCounter<dummy_mutex>;

}  // namespace reindexer
