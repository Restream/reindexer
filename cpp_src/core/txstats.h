#pragma once

#include "core/transaction.h"
#include "namespace/namespacestat.h"
#include "perfstatcounter.h"
#include "tools/stringstools.h"

namespace reindexer {

class TxStatCounter {
	using QuantityCounter = QuantityCounterST<size_t>;

public:
	TxStatCounter() {}

	void Count(const Transaction& tx) {
		using std::chrono::duration_cast;
		using std::chrono::microseconds;
		using std::chrono::high_resolution_clock;
		std::unique_lock<std::mutex> lck(mtx_);
		prepCounter_.Count(duration_cast<microseconds>(high_resolution_clock::now() - tx.GetStartTime()).count());
		stepsCounter_.Count(tx.GetSteps().size());
	}

	TxPerfStat Get() const {
		TxPerfStat stats;
		QuantityCounter::Stats stepsStats;
		QuantityCounter::Stats prepStats;
		{
			std::unique_lock<std::mutex> lck(mtx_);
			stepsStats = stepsCounter_.Get();
			prepStats = prepCounter_.Get();
		}
		stats.minStepsCount = stepsStats.minValue;
		stats.maxStepsCount = stepsStats.maxValue;
		stats.avgStepsCount = static_cast<size_t>(stepsStats.avg);
		stats.minPrepareTimeUs = prepStats.minValue;
		stats.maxPrepareTimeUs = prepStats.maxValue;
		stats.avgPrepareTimeUs = static_cast<size_t>(prepStats.avg);
		return stats;
	}

	void Reset() {
		std::unique_lock<std::mutex> lck(mtx_);
		stepsCounter_.Reset();
		prepCounter_.Reset();
	}

private:
	QuantityCounter stepsCounter_;
	QuantityCounter prepCounter_;
	mutable std::mutex mtx_;
};

}  // namespace reindexer
