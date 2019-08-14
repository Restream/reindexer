#pragma once

#include <string>
#include "estl/fast_hash_map.h"
#include "namespacestat.h"
#include "perfstatcounter.h"
#include "tools/stringstools.h"

namespace reindexer {

class WrSerializer;
class Query;

struct QueryPerfStat {
	void GetJSON(WrSerializer &ser) const;
	std::string query;
	PerfStat perf;
	std::string longestQuery;
};

class QueriesStatTracer {
public:
	void Hit(const Query &q, std::chrono::microseconds time) { hit<&PerfStatCounterST::Hit>(q, time); }
	void LockHit(const Query &q, std::chrono::microseconds time) { hit<&PerfStatCounterST::LockHit>(q, time); }
	const std::vector<QueryPerfStat> Data();
	void Reset() {
		std::unique_lock<std::mutex> lck(mtx_);
		stat_.clear();
	}

protected:
	struct Stat : public PerfStatCounterST {
		Stat(string_view q) : longestQuery(q) {}
		std::string longestQuery;
	};

	template <void (PerfStatCounterST::*hitFunc)(std::chrono::microseconds)>
	void hit(const Query &, std::chrono::microseconds);

	std::mutex mtx_;
	fast_hash_map<std::string, Stat, hash_str, equal_str> stat_;
};
extern template void QueriesStatTracer::hit<&PerfStatCounterST::Hit>(const Query &, std::chrono::microseconds);
extern template void QueriesStatTracer::hit<&PerfStatCounterST::LockHit>(const Query &, std::chrono::microseconds);

class QueryStatCalculator {
public:
	QueryStatCalculator(std::function<void(bool, std::chrono::microseconds)> hitter, std::chrono::microseconds threshold, bool enable)
		: hitter_(hitter), threshold_(threshold), enable_(enable) {
		if (enable_) tmStart = std::chrono::high_resolution_clock::now();
	}
	~QueryStatCalculator() {
		if (enable_) {
			auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart);
			if (time >= threshold_) hitter_(false, time);
		}
	}
	void LockHit() {
		if (enable_) {
			auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart);
			if (time >= threshold_) hitter_(true, time);
		}
	}

	std::chrono::time_point<std::chrono::high_resolution_clock> tmStart;
	std::function<void(bool, std::chrono::microseconds)> hitter_;
	std::chrono::microseconds threshold_;
	bool enable_;
};

}  // namespace reindexer
