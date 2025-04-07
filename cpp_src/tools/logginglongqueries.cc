#include "logginglongqueries.h"
#include <sstream>
#include "core/nsselecter/explaincalc.h"
#include "core/query/query.h"
#include "core/transaction/localtransaction.h"
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
		case DurationStorageIdx::StorageDirOps:
			return MutexMark::StorageDirOps;
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
		case DurationStorageIdx::StorageDirOps:
			return DescribeMutexMark(mutexMarkCast(idx));
		case DurationStorageIdx::DataFlush:
			return "Data flush"sv;
		case DurationStorageIdx::StorageSize:
			break;
	}
	throw Error(errLogic, "Unknown duration storage index");
}

template <typename Storage>
static auto fillStorageInfo(std::ostringstream& os, const Storage& storage) {
	os << "[slowlog] Waiting for a mutex lock:" << std::endl;
	for (size_t i = 0; i < storage.size(); ++i) {
		if (!storage.at(i)) {
			continue;
		}
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		os << describeDurationStorageIdx(DurationStorageIdx{static_cast<unsigned>(i)}) << ": " << storage.at(i)->count() << "us"
		   << std::endl;
	}
}

template <>
void Logger<QueryEnum2Type<QueryType::QuerySelect>>::Dump(std::chrono::microseconds time) {
	if (wrapper_.loggingParams.thresholdUs >= 0 && time.count() > wrapper_.loggingParams.thresholdUs) {
		std::ostringstream os;
		os << fmt::format("[slowlog] Long execution query: sql - {}; ({}us)\n", wrapper_.query.GetSQL(wrapper_.loggingParams.normalized),
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
void Logger<QueryEnum2Type<QueryType::QueryUpdate>>::Dump(std::chrono::microseconds time) {
	if (wrapper_.loggingParams.thresholdUs >= 0 && time.count() > wrapper_.loggingParams.thresholdUs) {
		std::ostringstream os;
		os << fmt::format("[slowlog] Long execution query: sql - {}; ({}us)\n", wrapper_.query.GetSQL(wrapper_.loggingParams.normalized),
						  time.count());
		fillStorageInfo(os, wrapper_.durationStorage);
		logPrint(LogWarning, os.str().data());
	}
}
template <>
void Logger<QueryEnum2Type<QueryType::QueryDelete>>::Dump(std::chrono::microseconds time) {
	if (wrapper_.loggingParams.thresholdUs >= 0 && time.count() > wrapper_.loggingParams.thresholdUs) {
		std::ostringstream os;
		os << fmt::format("[slowlog] Long execution query: sql - {}; ({}us)\n", wrapper_.query.GetSQL(wrapper_.loggingParams.normalized),
						  time.count());
		fillStorageInfo(os, wrapper_.durationStorage);
		logPrint(LogWarning, os.str().data());
	}
}

template <>
void Logger<LocalTransaction>::Dump(std::chrono::microseconds time) {
	int64_t avg_time = 0;
	bool longAvgStep = false;
	if (wrapper_.thresholds.avgTxStepThresholdUs >= 0) {
		avg_time = (wrapper_.tx.GetSteps().size() > 1) ? (time.count() / wrapper_.tx.GetSteps().size()) : time.count();
		longAvgStep = avg_time > wrapper_.thresholds.avgTxStepThresholdUs;
	}

	const bool longTotal = wrapper_.thresholds.thresholdUs >= 0 && time.count() > wrapper_.thresholds.thresholdUs;

	if (longAvgStep || longTotal) {
		std::ostringstream os;
		fillStorageInfo(os, wrapper_.durationStorage);

		logFmt(LogWarning, "[slowlog] Long tx apply: namespace - {}; was{}copied; {} steps;{}{}\n{}", wrapper_.tx.GetNsName(),
			   wrapper_.wasCopied ? " " : " not ", wrapper_.tx.GetSteps().size(),
			   longAvgStep ? fmt::format(" Exceeded the average step execution time limit ({}us);", avg_time) : "",
			   longTotal ? fmt::format(" Exceeded the total time limit ({}us);", time.count()) : "", os.str());
	}
}

template <ActionWrapper<QueryEnum2Type<QueryType::QuerySelect>>::ExplainMethodType... methods>
void ActionWrapper<QueryEnum2Type<QueryType::QuerySelect>>::add(const ExplainCalc& explain) {
	durationStorage = {std::chrono::duration_cast<std::chrono::microseconds>((explain.*methods)())...};
}

void ActionWrapper<QueryEnum2Type<QueryType::QuerySelect>>::Add(const ExplainCalc& explain) {
	add<&ExplainCalc::Total, &ExplainCalc::Prepare, &ExplainCalc::Indexes, &ExplainCalc::Postprocess, &ExplainCalc::Loop,
		&ExplainCalc::Sort>(explain);
}

template struct Logger<LocalTransaction>;
template struct Logger<QueryEnum2Type<QueryType::QuerySelect>>;
template struct Logger<QueryEnum2Type<QueryType::QueryUpdate>>;
template struct Logger<QueryEnum2Type<QueryType::QueryDelete>>;
}  // namespace reindexer::long_actions
