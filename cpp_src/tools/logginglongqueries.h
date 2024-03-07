#pragma once
#include <chrono>
#include <optional>
#include "core/dbconfig.h"
#include "estl/mutex.h"
#include "tools/clock.h"

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

enum class ExplainDuration {
	Total,
	Prepare,
	Indexes,
	Postprocess,
	Loop,
	Sort,
	ExplainDurationSize,
};

template <QueryType Enum>
struct QueryEnum2Type : std::integral_constant<QueryType, Enum> {};

struct LockDurationStorage {
	using ArrayT = std::array<std::optional<std::chrono::microseconds>, size_t(DurationStorageIdx::StorageSize)>;
	ArrayT durationStorage = {};
	void Add(DurationStorageIdx idx, std::chrono::microseconds time) {
		auto& val = durationStorage[static_cast<int>(idx)];
		val ? * val += time : val = time;
	}
};

struct QueryParams {
	const Query& query;
	LongQueriesLoggingParams loggingParams;
};

struct TransactionParams {
	const Transaction& tx;
	LongTxLoggingParams thresholds;
	const bool& wasCopied;
};

template <>
struct ActionWrapper<Transaction> : TransactionParams, LockDurationStorage {
	template <typename... Args>
	ActionWrapper(Args&&... args) : TransactionParams{std::forward<Args>(args)...}, LockDurationStorage() {}
};

template <>
struct ActionWrapper<QueryEnum2Type<QueryType::QuerySelect>> : QueryParams {
	template <typename... Args>
	ActionWrapper(Args&&... args) : QueryParams{std::forward<Args>(args)...} {}

	using ArrayT = std::array<std::chrono::microseconds, size_t(ExplainDuration::ExplainDurationSize)>;
	std::optional<ArrayT> durationStorage = std::nullopt;

	void Add(const ExplainCalc&);

private:
	using ExplainMethodType = system_clock_w::duration (ExplainCalc::*)() const noexcept;
	template <ExplainMethodType... methods>
	void add(const ExplainCalc&);
};

template <>
struct ActionWrapper<QueryEnum2Type<QueryType::QueryUpdate>> : QueryParams, LockDurationStorage {
	template <typename... Args>
	ActionWrapper(Args&&... args) : QueryParams{std::forward<Args>(args)...}, LockDurationStorage{} {}
};

template <>
struct ActionWrapper<QueryEnum2Type<QueryType::QueryDelete>> : QueryParams, LockDurationStorage {
	template <typename... Args>
	ActionWrapper(Args&&... args) : QueryParams{std::forward<Args>(args)...}, LockDurationStorage{} {}
};

template <typename T>
struct Logger {
	Logger() = default;
	static constexpr bool isEnabled = !std::is_empty_v<ActionWrapper<T>>;

	void Dump(std::chrono::microseconds time);
	template <typename... Args>
	void Add(Args&&... args) {
		wrapper_.Add(std::forward<Args>(args)...);
	}

private:
	template <typename... Args>
	friend auto MakeLogger(Args&&... args);

	template <QueryType queryType, typename... Args>
	friend auto MakeLogger(Args&&... args);

	template <typename ActionType, typename... Args>
	Logger(const ActionType& action, Args&&... args) : wrapper_(ActionWrapper<T>{action, std::forward<Args>(args)...}) {}
	ActionWrapper<T> wrapper_;
};

template <typename... Args>
auto MakeLogger(Args&&... args) {
	return Logger<Transaction>{std::forward<Args>(args)...};
}

template <QueryType queryType, typename... Args>
auto MakeLogger(Args&&... args) {
	return Logger<QueryEnum2Type<queryType>>{std::forward<Args>(args)...};
}

template <>
void Logger<QueryEnum2Type<QueryType::QuerySelect>>::Dump(std::chrono::microseconds);
template <>
void Logger<QueryEnum2Type<QueryType::QueryUpdate>>::Dump(std::chrono::microseconds);
template <>
void Logger<QueryEnum2Type<QueryType::QueryDelete>>::Dump(std::chrono::microseconds);
template <>
void Logger<Transaction>::Dump(std::chrono::microseconds);

extern template struct Logger<Transaction>;
extern template struct Logger<QueryEnum2Type<QueryType::QuerySelect>>;
extern template struct Logger<QueryEnum2Type<QueryType::QueryUpdate>>;
extern template struct Logger<QueryEnum2Type<QueryType::QueryDelete>>;

}  // namespace long_actions
}  // namespace reindexer
