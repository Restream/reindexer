#pragma once

#include "core/namespace/namespacestat.h"
#include "core/perfstatcounter.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "localtransaction.h"

namespace reindexer {

class [[nodiscard]] TxStatCounter {
	using QuantityCounter = QuantityCounterST<size_t>;

public:
	void Count(const LocalTransaction& tx) {
		using std::chrono::duration_cast;
		using std::chrono::microseconds;
		unique_lock lck(mtx_);
		prepCounter_.Count(duration_cast<microseconds>(Transaction::ClockT::now() - tx.GetStartTime()).count());
		stepsCounter_.Count(tx.GetSteps().size());
	}

	TxPerfStat Get() const {
		TxPerfStat stats;
		QuantityCounter::Stats stepsStats;
		QuantityCounter::Stats prepStats;
		{
			unique_lock lck(mtx_);
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
		unique_lock lck(mtx_);
		stepsCounter_.Reset();
		prepCounter_.Reset();
	}

private:
	QuantityCounter stepsCounter_;
	QuantityCounter prepCounter_;
	mutable mutex mtx_;
};

}  // namespace reindexer
