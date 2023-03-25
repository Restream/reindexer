#pragma once

#include <string>
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
	const std::vector<QueryPerfStat> Data();
	void Reset() {
		std::unique_lock<std::mutex> lck(mtx_);
		stat_.clear();
	}

protected:
	struct Stat : public PerfStatCounterST {
		Stat(std::string_view q) : longestQuery(q) {}
		std::string longestQuery;
	};

	template <void (PerfStatCounterST::*hitFunc)(std::chrono::microseconds)>
	void hit(const QuerySQL&, std::chrono::microseconds);

	std::mutex mtx_;
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
		if (enable_) tmStart = std::chrono::high_resolution_clock::now();
	}

	QueryStatCalculator(Logger<T> logger) : enable_(true), logger_(std::move(logger)) {
		if (enable_) tmStart = std::chrono::high_resolution_clock::now();
	}
	~QueryStatCalculator() {
		if (enable_) {
			auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart);
			if (hitter_ && time >= threshold_) hitter_(false, time);

			if constexpr (Logger<T>::isEnabled) {
				logger_.Dump(time);
			}
		}
	}
	void LockHit() {
		if (enable_ && hitter_) {
			auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart);
			if (time >= threshold_) hitter_(true, time);
		}
	}

	std::chrono::time_point<std::chrono::high_resolution_clock> tmStart;
	std::function<void(bool, std::chrono::microseconds)> hitter_;
	std::chrono::microseconds threshold_;
	bool enable_;

private:
	Logger<T> logger_;
};

}  // namespace reindexer
