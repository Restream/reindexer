#pragma once

#include <stdlib.h>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include "estl/shared_mutex.h"
#include "namespacestat.h"
#include "perfstatcounter.h"
#include "query/query.h"
#include "tools/serializer.h"

namespace reindexer {

struct QueryPerfStat {
	void GetJSON(WrSerializer &ser) const;
	std::string query;
	PerfStat perf;
};

class QueriesStatTracer {
public:
	void Hit(const Query &q, std::chrono::microseconds time);
	void LockHit(const Query &q, std::chrono::microseconds time);
	const std::vector<QueryPerfStat> Data();

protected:
	std::mutex mtx_;
	std::unordered_map<std::string, PerfStatCounterST> stat_;
};

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
