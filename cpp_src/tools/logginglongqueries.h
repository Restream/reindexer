#pragma once
#include "core/dbconfig.h"

namespace reindexer {

class Query;
class LocalTransaction;

namespace long_actions {

template <typename T>
struct ActionWrapper {};

template <>
struct ActionWrapper<LocalTransaction> {
	const LocalTransaction& tx;
	LongTxLoggingParams thresholds;
	const bool& wasCopied;
};

template <>
struct ActionWrapper<Query> {
	const Query& query;
	LongQueriesLoggingParams loggingParams;
};

template <typename T = void>
struct Logger {
	static constexpr bool isEnabled = !std::is_empty_v<ActionWrapper<T>>;
	template <typename ActionType, typename... Args>
	Logger(const ActionType& action, Args&&... args) : wrapper_(ActionWrapper<ActionType>{action, std::forward<Args>(args)...}) {}

	void Dump(std::chrono::microseconds time);

private:
	ActionWrapper<T> wrapper_;
};

template <typename T, typename... Args>
Logger(const T&, Args&&...) -> Logger<T>;

extern template struct Logger<Query>;
extern template struct Logger<LocalTransaction>;
}  // namespace long_actions
}  // namespace reindexer
