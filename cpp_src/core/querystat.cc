
#include "querystat.h"

#ifdef _WIN32
#define PRI_SIZE_T "Iu"
#else
#define PRI_SIZE_T "zu"
#endif

namespace reindexer {

void QueriesStatTracer::Hit(const Query &q, std::chrono::microseconds time) {
	auto sqlq = q.Dump(true);
	std::unique_lock<std::mutex> lck(mtx_);
	stat_.emplace(sqlq, PerfStatCounterST()).first->second.Hit(time);
};

void QueriesStatTracer::LockHit(const Query &q, std::chrono::microseconds time) {
	auto sqlq = q.Dump(true);
	std::unique_lock<std::mutex> lck(mtx_);
	stat_.emplace(sqlq, PerfStatCounterST()).first->second.LockHit(time);
};

const std::vector<QueryPerfStat> QueriesStatTracer::Data() {
	std::unique_lock<std::mutex> lck(mtx_);

	std::vector<QueryPerfStat> ret;
	ret.reserve(stat_.size());
	for (auto &stat : stat_) ret.push_back({stat.first, stat.second.Get<PerfStat>()});
	return ret;
}

void QueryPerfStat::GetJSON(WrSerializer &ser) const {
	ser.PutChar('{');
	ser.Printf("\"query\":\"%s\",", query.c_str());
	ser.Printf("\"total_queries_count\":%" PRI_SIZE_T ",", perf.totalHitCount);
	ser.Printf("\"total_avg_lock_time_us\":%" PRI_SIZE_T ",", perf.totalLockTimeUs);
	ser.Printf("\"total_avg_latency_us\":%" PRI_SIZE_T ",", perf.totalTimeUs);
	ser.Printf("\"last_sec_qps\":%" PRI_SIZE_T ",", perf.avgHitCount);
	ser.Printf("\"last_sec_avg_lock_time_us\":%" PRI_SIZE_T ",", perf.avgLockTimeUs);
	ser.Printf("\"last_sec_avg_latency_us\":%" PRI_SIZE_T "", perf.avgTimeUs);
	ser.PutChar('}');
}

}  // namespace reindexer