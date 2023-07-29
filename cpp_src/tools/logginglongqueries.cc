#include "logginglongqueries.h"
#include "core/nsselecter/explaincalc.h"
#include "core/query/query.h"
#include "core/transactionimpl.h"
#include "logger.h"

namespace reindexer::long_actions {

static std::string_view describeExplainDuration(ExplainDuration mark) {
	using namespace std::string_view_literals;
	switch (mark) {
		case ExplainDuration::Total:
			return "Total"sv;
		case ExplainDuration::Prepare:
			return "Prepare"sv;
		case ExplainDuration::Indexes:
			return "Indexes"sv;
		case ExplainDuration::Postprocess:
			return "Postprocess"sv;
		case ExplainDuration::Loop:
			return "Loop"sv;
		case ExplainDuration::Sort:
			return "Sort"sv;
		case ExplainDuration::ExplainDurationSize:
			break;
	}
	throw Error(errLogic, "Unknown explain duration value");
}

static MutexMark mutexMarkCast(DurationStorageIdx mark) {
	switch (mark) {
		case DurationStorageIdx::DbManager:
			return MutexMark::DbManager;
		case DurationStorageIdx::IndexText:
			return MutexMark::IndexText;
		case DurationStorageIdx::Namespace:
			return MutexMark::Namespace;
		case DurationStorageIdx::Reindexer:
			return MutexMark::Reindexer;
		case DurationStorageIdx::ReindexerStats:
			return MutexMark::ReindexerStats;
		case DurationStorageIdx::CloneNs:
			return MutexMark::CloneNs;
		case DurationStorageIdx::AsyncStorage:
			return MutexMark::AsyncStorage;
		case DurationStorageIdx::StorageSize:
		case DurationStorageIdx::DataFlush:
			break;
	}
	throw Error(errLogic, "Unknown duration storage index");
}

static std::string_view describeDurationStorageIdx(DurationStorageIdx idx) {
	using namespace std::string_view_literals;
	switch (idx) {
		case DurationStorageIdx::DbManager:
		case DurationStorageIdx::IndexText:
		case DurationStorageIdx::Namespace:
		case DurationStorageIdx::Reindexer:
		case DurationStorageIdx::ReindexerStats:
		case DurationStorageIdx::CloneNs:
		case DurationStorageIdx::AsyncStorage:
			return DescribeMutexMark(mutexMarkCast(idx));
		case DurationStorageIdx::DataFlush:
			return "Data flush"sv;
		case DurationStorageIdx::StorageSize:
			break;
	}
	throw Error(errLogic, "Unknown duration storage index");
}

template <>
void Logger<Query>::Dump(std::chrono::microseconds time) {
	if (wrapper_.loggingParams.thresholdUs >= 0 && time.count() > wrapper_.loggingParams.thresholdUs) {
		std::ostringstream os;
		os << fmt::sprintf("[slowlog] Long execution query: sql - %s; (%dus)\n", wrapper_.query.GetSQL(wrapper_.loggingParams.normalized),
						   time.count());

		if (wrapper_.durationStorage) {
			os << "[slowlog] Explain statistics:\n";

			for (int i = 0; i < static_cast<int>(wrapper_.durationStorage->size()); ++i) {
				os << describeExplainDuration(ExplainDuration{i}) << ": " << (*wrapper_.durationStorage)[i].count() << "us" << std::endl;
			}
		}

		logPrint(LogWarning, os.str().data());
	}
}

template <>
void Logger<Transaction>::Dump(std::chrono::microseconds time) {
	int64_t avg_time = 0;
	bool longAvgStep = false;
	if (wrapper_.thresholds.avgTxStepThresholdUs >= 0) {
		avg_time = (wrapper_.tx.GetSteps().size() > 1) ? (time.count() / wrapper_.tx.GetSteps().size()) : time.count();
		longAvgStep = avg_time > wrapper_.thresholds.avgTxStepThresholdUs;
	}

	const bool longTotal = wrapper_.thresholds.thresholdUs >= 0 && time.count() > wrapper_.thresholds.thresholdUs;

	if (longAvgStep || longTotal) {
		std::ostringstream os;
		os << "[slowlog] Waiting for a mutex lock:" << std::endl;
		for (size_t i = 0; i < wrapper_.durationStorage.size(); ++i) {
			if (wrapper_.durationStorage[i]) {
				os << describeDurationStorageIdx(DurationStorageIdx{static_cast<unsigned>(i)}) << ": "
				   << wrapper_.durationStorage[i]->count() << "us" << std::endl;  // NOLINT(bugprone-unchecked-optional-access)
			}
		}

		logPrintf(LogWarning, "[slowlog] Long tx apply: namespace - %s; was%scopied; %d steps;%s%s\n%s", wrapper_.tx.GetName(),
				  wrapper_.wasCopied ? " " : " not ", wrapper_.tx.GetSteps().size(),
				  longAvgStep ? fmt::sprintf(" Exceeded the average step execution time limit (%dus);", avg_time) : "",
				  longTotal ? fmt::sprintf(" Exceeded the total time limit (%dus);", time.count()) : "", os.str());
	}
}

template <ActionWrapper<Query>::ExplainMethodType... methods>
void ActionWrapper<Query>::add(const ExplainCalc& explain) {
	durationStorage = {std::chrono::duration_cast<std::chrono::microseconds>((explain.*methods)())...};
}

void ActionWrapper<Query>::Add(const ExplainCalc& explain) {
	add<&ExplainCalc::Total, &ExplainCalc::Prepare, &ExplainCalc::Indexes, &ExplainCalc::Postprocess, &ExplainCalc::Loop,
		&ExplainCalc::Sort>(explain);
}

template struct Logger<Query>;
template struct Logger<Transaction>;
}  // namespace reindexer::long_actions
