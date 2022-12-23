#include "logginglongqueries.h"
#include "core/query/query.h"
#include "core/transactionimpl.h"
#include "logger.h"

namespace reindexer::long_actions {

template <typename T>
void Logger<T>::Dump(std::chrono::microseconds time) {
	if constexpr (std::is_same_v<T, Query>)
		if (wrapper_.loggingParams.thresholdUs >= 0 && time.count() > wrapper_.loggingParams.thresholdUs)
			logPrintf(LogWarning, "[slowlog] Long execution query: sql - %s; (%dus)",
					  wrapper_.query.GetSQL(wrapper_.loggingParams.normalized), time.count());
	if constexpr (std::is_same_v<T, Transaction>) {
		int64_t avg_time;
		bool longAvgStep =
			wrapper_.thresholds.avgTxStepThresholdUs >= 0
				? ((avg_time = wrapper_.tx.GetSteps().size() > 0 ? (time.count() / wrapper_.tx.GetSteps().size()) : time.count()),
				   avg_time > wrapper_.thresholds.avgTxStepThresholdUs)
				: false;

		bool longTotal = wrapper_.thresholds.thresholdUs >= 0 && time.count() > wrapper_.thresholds.thresholdUs;

		if (longAvgStep || longTotal) {
			logPrintf(LogWarning, "[slowlog] Long tx apply: namespace - %s; was%scopied; %d steps;%s%s", wrapper_.tx.GetName(),
					  wrapper_.wasCopied ? " " : " not ", wrapper_.tx.GetSteps().size(),
					  longAvgStep ? fmt::sprintf(" Exceeded the average step execution time limit (%dus);", avg_time) : "",
					  longTotal ? fmt::sprintf(" Exceeded the total time limit (%dus);", time.count()) : "");
		}
	}
}

template struct Logger<Query>;
template struct Logger<Transaction>;
}  // namespace reindexer::long_actions
