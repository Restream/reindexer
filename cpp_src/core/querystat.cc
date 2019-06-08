
#include "querystat.h"

#include "core/cjson/jsonbuilder.h"

namespace reindexer {

void QueriesStatTracer::Hit(const Query &q, std::chrono::microseconds time) {
	WrSerializer ser;
	string sqlq(q.GetSQL(ser, true).Slice());
	std::unique_lock<std::mutex> lck(mtx_);
	stat_.emplace(sqlq, PerfStatCounterST()).first->second.Hit(time);
};

void QueriesStatTracer::LockHit(const Query &q, std::chrono::microseconds time) {
	WrSerializer ser;
	string sqlq(q.GetSQL(ser, true).Slice());
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
	JsonBuilder builder(ser);

	builder.Put("query", query);
	builder.Put("total_queries_count", perf.totalHitCount);
	builder.Put("total_avg_lock_time_us", perf.totalLockTimeUs);
	builder.Put("total_avg_latency_us", perf.totalTimeUs);
	builder.Put("last_sec_qps", perf.avgHitCount);
	builder.Put("last_sec_avg_lock_time_us", perf.avgLockTimeUs);
	builder.Put("last_sec_avg_latency_us", perf.avgTimeUs);
	builder.Put("latency_stddev", perf.stddev);
	builder.Put("min_latency_us", perf.minTimeUs);
	builder.Put("max_latency_us", perf.maxTimeUs);
}

}  // namespace reindexer
