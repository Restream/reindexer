#pragma once
#include <chrono>
#include "core/dbconfig.h"
#include "estl/mutex.h"
#include "optional"

namespace reindexer {

class Query;
class Transaction;

class ExplainCalc;

namespace long_actions {

template <typename T>
struct ActionWrapper {};

// to store durations for different MutexMark types and methods calls in one array
enum class DurationStorageIdx : unsigned {
	DbManager = 0u,
	IndexText,
	Namespace,
	Reindexer,
	ReindexerStats,
	CloneNs,
	AsyncStorage,
	DataFlush,
	StorageSize
};

constexpr DurationStorageIdx DurationStorageIdxCast(MutexMark mark) {
	switch (mark) {
		case MutexMark::DbManager:
			return DurationStorageIdx::DbManager;
		case MutexMark::IndexText:
			return DurationStorageIdx::IndexText;
		case MutexMark::Namespace:
			return DurationStorageIdx::Namespace;
		case MutexMark::Reindexer:
			return DurationStorageIdx::Reindexer;
		case MutexMark::ReindexerStats:
			return DurationStorageIdx::ReindexerStats;
		case MutexMark::CloneNs:
			return DurationStorageIdx::CloneNs;
		case MutexMark::AsyncStorage:
			return DurationStorageIdx::AsyncStorage;
	}
}

template <>
struct ActionWrapper<Transaction> {
	const Transaction& tx;
	LongTxLoggingParams thresholds;
	const bool& wasCopied;

	using ArrayT = std::array<std::optional<std::chrono::microseconds>, size_t(DurationStorageIdx::StorageSize)>;
	ArrayT durationStorage = {};

	void Add(DurationStorageIdx idx, std::chrono::microseconds time) {
		auto& val = durationStorage[static_cast<int>(idx)];
		val ? * val += time : val = time;
	}
};

enum class ExplainDuration {
	Total,
	Prepare,
	Indexes,
	Postprocess,
	Loop,
	Sort,
	ExplainDurationSize,
};

template <>
struct ActionWrapper<Query> {
	const Query& query;
	LongQueriesLoggingParams loggingParams;
	using ArrayT = std::array<std::chrono::microseconds, size_t(ExplainDuration::ExplainDurationSize)>;
	std::optional<ArrayT> durationStorage = std::nullopt;

	void Add(const ExplainCalc&);

private:
	using ExplainMethodType = std::chrono::high_resolution_clock::duration (ExplainCalc::*)() const noexcept;
	template <ExplainMethodType... methods>
	void add(const ExplainCalc&);
};

template <typename T = void>
struct Logger {
	static constexpr bool isEnabled = !std::is_empty_v<ActionWrapper<T>>;
	template <typename ActionType, typename... Args>
	Logger(const ActionType& action, Args&&... args) : wrapper_(ActionWrapper<ActionType>{action, std::forward<Args>(args)...}) {}

	void Dump(std::chrono::microseconds time);
	template <typename... Args>
	void Add(Args&&... args) {
		wrapper_.Add(std::forward<Args>(args)...);
	}

private:
	ActionWrapper<T> wrapper_;
};

template <>
void Logger<Query>::Dump(std::chrono::microseconds);
template <>
void Logger<Transaction>::Dump(std::chrono::microseconds);

extern template struct Logger<Query>;
extern template struct Logger<Transaction>;
}  // namespace long_actions
}  // namespace reindexer