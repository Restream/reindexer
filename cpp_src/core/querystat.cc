
#include "querystat.h"

#include "core/cjson/jsonbuilder.h"
#include "query/query.h"

namespace reindexer {

template <void (PerfStatCounterST::*hitFunc)(std::chrono::microseconds)>
void QueriesStatTracer::hit(const QuerySQL& sql, std::chrono::microseconds time) {
	unique_lock lck(mtx_);
	const auto it = stat_.find(sql.normalized);
	if (it == stat_.end()) {
		(stat_.emplace(std::string(sql.normalized), Stat(sql.nonNormalized)).first->second.*hitFunc)(time);
	} else {
		const auto maxTime = it->second.MaxTime();
		(it->second.*hitFunc)(time);
		if (it->second.MaxTime() > maxTime) {
			it->second.longestQuery = std::string(sql.nonNormalized);
		}
	}
}
template void QueriesStatTracer::hit<&PerfStatCounterST::Hit>(const QuerySQL&, std::chrono::microseconds);
template void QueriesStatTracer::hit<&PerfStatCounterST::LockHit>(const QuerySQL&, std::chrono::microseconds);

std::vector<QueryPerfStat> QueriesStatTracer::Data() {
	unique_lock lck(mtx_);

	std::vector<QueryPerfStat> ret;
	ret.reserve(stat_.size());
	for (auto& stat : stat_) {
		ret.push_back({stat.first, stat.second.Get<PerfStat>(), stat.second.longestQuery});
	}
	return ret;
}

void QueryPerfStat::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);

	builder.Put("query", query);
	builder.Put("total_queries_count", perf.totalHitCount);
	builder.Put("total_avg_lock_time_us", perf.totalAvgLockTimeUs);
	builder.Put("total_avg_latency_us", perf.totalAvgTimeUs);
	builder.Put("last_sec_qps", perf.lastSecHitCount);
	builder.Put("last_sec_avg_lock_time_us", perf.lastSecAvgLockTimeUs);
	builder.Put("last_sec_avg_latency_us", perf.lastSecAvgTimeUs);
	builder.Put("latency_stddev", perf.stddev);
	builder.Put("min_latency_us", perf.minTimeUs);
	builder.Put("max_latency_us", perf.maxTimeUs);
	builder.Put("longest_query", longestQuery);
}

}  // namespace reindexer
