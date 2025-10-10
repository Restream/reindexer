#include "perfstatcounter.h"
#include <cmath>
#include <numeric>
#include "estl/defines.h"
#include "estl/mutex.h"

namespace reindexer {

static constexpr size_t kMaxValuesCountForStddev = 100;

template <typename Mutex>
PerfStatCounter<Mutex>::PerfStatCounter() {
	lastValuesUs_.reserve(kMaxValuesCountForStddev);
}

template <typename Mutex>
void PerfStatCounter<Mutex>::Hit(std::chrono::microseconds time) noexcept {
	lock_guard lck(mtx_);
	totalTime_ += time;
	calcTime_ += time;
	++calcHitCount_;
	++totalHitCount_;
	if (lastValuesUs_.size() < kMaxValuesCountForStddev) {
		lastValuesUs_.emplace_back(time.count());
		posInValuesUs_ = kMaxValuesCountForStddev - 1;
	} else {
		posInValuesUs_ = (posInValuesUs_ + 1) % kMaxValuesCountForStddev;
		lastValuesUs_[posInValuesUs_] = time.count();
	}

	maxTime_ = (time > maxTime_) ? time : maxTime_;
	minTime_ = (time < minTime_) ? time : minTime_;
	if (lastValuesUs_.size() > 1) {
		double avg = std::accumulate(lastValuesUs_.begin(), lastValuesUs_.end(), 0.0) / lastValuesUs_.size();
		double dispersion = 0.0;
		for (size_t i = 0; i < lastValuesUs_.size(); ++i) {
			dispersion += pow(lastValuesUs_[i] - avg, 2);
		}
		dispersion /= lastValuesUs_.size();
		stddev_ = sqrt(dispersion);
	}

	lap();
}

template <typename Mutex>
void PerfStatCounter<Mutex>::LockHit(std::chrono::microseconds time) noexcept {
	lock_guard lck(mtx_);
	calcLockTime_ += time;
	totalLockTime_ += time;
}

template <typename Mutex>
void PerfStatCounter<Mutex>::Reset() noexcept {
	static const PerfStatCounter<Mutex> defaultCounter;
	lock_guard lck(mtx_);
	totalHitCount_ = defaultCounter.totalHitCount_;
	totalTime_ = defaultCounter.totalTime_;
	totalLockTime_ = defaultCounter.totalLockTime_;
	lastSecHitCount_ = defaultCounter.lastSecHitCount_;
	lastSecTotalTime_ = defaultCounter.lastSecTotalTime_;
	lastSecTotalLockTime_ = defaultCounter.lastSecTotalLockTime_;
	calcHitCount_ = defaultCounter.calcHitCount_;
	calcTime_ = defaultCounter.calcTime_;
	calcLockTime_ = defaultCounter.calcLockTime_;
	calcStartTime_ = defaultCounter.calcStartTime_;
	stddev_ = defaultCounter.stddev_;
	minTime_ = defaultCounter.minTime_;
	maxTime_ = defaultCounter.maxTime_;
	lastValuesUs_.resize(0);
	posInValuesUs_ = defaultCounter.posInValuesUs_;
}

template <typename Mutex>
void PerfStatCounter<Mutex>::lap() noexcept {
	const auto now = system_clock_w::now_coarse();
	std::chrono::microseconds elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - calcStartTime_);
	constexpr static auto kPeriod = std::chrono::microseconds(1000000);
	if (elapsed < kPeriod) {
		return;
	}
	if rx_likely (elapsed < 2 * kPeriod) {
		lastSecHitCount_ = calcHitCount_;
		lastSecTotalTime_ = calcTime_;
		lastSecTotalLockTime_ = calcLockTime_;
	} else {
		lastSecHitCount_ = 0;
		lastSecTotalTime_ = std::chrono::microseconds(0);
		lastSecTotalLockTime_ = std::chrono::microseconds(0);
	}
	calcStartTime_ = now;
	calcHitCount_ = 0;
	calcTime_ = std::chrono::microseconds(0);
	calcLockTime_ = std::chrono::microseconds(0);
	lastValuesUs_.resize(0);
}

template class PerfStatCounter<reindexer::mutex>;
template class PerfStatCounter<DummyMutex>;

}  // namespace reindexer
