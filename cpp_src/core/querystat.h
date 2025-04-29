#pragma once

#include <string>
#include "core/nsselecter/explaincalc.h"
#include "estl/fast_hash_map.h"
#include "namespace/namespacestat.h"
#include "perfstatcounter.h"
#include "tools/logginglongqueries.h"
#include "tools/stringstools.h"

namespace reindexer {

class WrSerializer;

struct QueryPerfStat {
	void GetJSON(WrSerializer& ser) const;
	std::string query;
	PerfStat perf;
	std::string longestQuery;
};

class QueriesStatTracer {
public:
	struct QuerySQL {
		std::string_view normalized;
		std::string_view nonNormalized;
	};

	void Hit(const QuerySQL& sql, std::chrono::microseconds time) { hit<&PerfStatCounterST::Hit>(sql, time); }
	void LockHit(const QuerySQL& sql, std::chrono::microseconds time) { hit<&PerfStatCounterST::LockHit>(sql, time); }
	std::vector<QueryPerfStat> Data();
	void Reset() {
		std::unique_lock<std::mutex> lck(mtx_);
		stat_.clear();
	}

private:
	struct Stat : public PerfStatCounterST {
		Stat(std::string_view q) : longestQuery(q) {}
		std::string longestQuery;
	};

	template <void (PerfStatCounterST::*hitFunc)(std::chrono::microseconds)>
	void hit(const QuerySQL&, std::chrono::microseconds);

	mutable std::mutex mtx_;
	fast_hash_map<std::string, Stat, hash_str, equal_str, less_str> stat_;
};
extern template void QueriesStatTracer::hit<&PerfStatCounterST::Hit>(const QuerySQL&, std::chrono::microseconds);
extern template void QueriesStatTracer::hit<&PerfStatCounterST::LockHit>(const QuerySQL&, std::chrono::microseconds);

template <typename T = void, template <typename> class Logger = long_actions::Logger>
class QueryStatCalculator {
public:
	QueryStatCalculator(std::function<void(bool, std::chrono::microseconds)> hitter, std::chrono::microseconds threshold, bool enable,
						Logger<T> logger = Logger{})
		: hitter_(std::move(hitter)), threshold_(threshold), enable_(enable), logger_(std::move(logger)) {
		if (enable_) {
			tmStart = system_clock_w::now();
		}
	}

	QueryStatCalculator(Logger<T> logger, bool enable = true) : enable_(enable), logger_(std::move(logger)) {
		if (enable_) {
			tmStart = system_clock_w::now();
		}
	}
	~QueryStatCalculator() {
		if (enable_) {
			auto time = std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - tmStart);
			if (hitter_ && time >= threshold_) {
				hitter_(false, time);
			}

			if constexpr (Logger<T>::isEnabled) {
				logger_.Dump(time);
			}
		}
	}
	void LockHit() {
		if (enable_ && hitter_) {
			auto time = std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - tmStart);
			if (time >= threshold_) {
				hitter_(true, time);
			}
		}
	}

	template <long_actions::DurationStorageIdx index, typename Type, typename Method, typename... Args>
	auto LogDuration(Type& var, Method method, Args&&... args) {
		return exec<index>([&var, &method](Args&&... aa) { return (var.*method)(std::forward<Args>(aa)...); }, std::forward<Args>(args)...);
	}

	template <typename Type, typename Method, typename... Args>
	auto LogFlushDuration(Type& var, Method method, Args&&... args) {
		return LogDuration<long_actions::DurationStorageIdx::DataFlush>(var, method, std::forward<Args>(args)...);
	}

	template <typename Type, typename Method, typename... Args>
	auto CreateLock(Type& var, Method method, Args&&... args) {
		return LogDuration<long_actions::DurationStorageIdxCast(decltype((var.*method)(std::forward<Args>(args)...))::MutexType::mark)>(
			var, method, std::forward<Args>(args)...);
	}

	template <template <typename...> class MutexType, typename... Args>
	auto CreateLock(Args&&... args) {
		return exec<long_actions::DurationStorageIdxCast(decltype(MutexType(std::forward<Args>(args)...))::MutexType::mark)>(
			[](Args&&... aa) { return MutexType(std::forward<Args>(aa)...); }, std::forward<Args>(args)...);
	}

	void AddExplain(const ExplainCalc& explain) { logger_.Add(explain); }

private:
	template <long_actions::DurationStorageIdx index, typename Callable, typename... Args>
	auto exec(Callable&& callable, Args&&... args) {
		if (enable_) {
			class LogGuard {
			public:
				LogGuard(Logger<T>& logger) : logger_{logger}, start_{system_clock_w::now()} {}
				~LogGuard() { logger_.Add(index, std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - start_)); }

			private:
				Logger<T>& logger_;
				system_clock_w::time_point start_;
			} logGuard{logger_};
			return callable(std::forward<Args>(args)...);

		} else {
			return callable(std::forward<Args>(args)...);
		}
	}

	system_clock_w::time_point tmStart;
	std::function<void(bool, std::chrono::microseconds)> hitter_;
	std::chrono::microseconds threshold_;
	bool enable_;
	Logger<T> logger_;
};

}  // namespace reindexer
