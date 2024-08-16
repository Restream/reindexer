#include "activity.h"
#include <iomanip>
#include <sstream>
#include <string>
#include "cjson/jsonbuilder.h"
#include "tools/timetools.h"

namespace reindexer {

std::string_view Activity::DescribeState(State st) noexcept {
	switch (st) {
		case InProgress:
			return "in_progress";
		case WaitLock:
			return "wait_lock";
		case Sending:
			return "sending";
		case IndexesLookup:
			return "indexes_lookup";
		case SelectLoop:
			return "select_loop";
		case ProxiedViaClusterProxy:
			return "proxied_via_cluster_proxy";
		case ProxiedViaShardingProxy:
			return "proxied_via_sharding_proxy";
		default:
			abort();
	}
}

void Activity::GetJSON(WrSerializer& ser) const {
	using namespace std::chrono;
	JsonBuilder builder(ser);
	builder.Put("client", activityTracer);
	if (!user.empty()) {
		builder.Put("user", user);
	}
	builder.Put("query", query);
	builder.Put("query_id", id);
	std::time_t t = system_clock_w::to_time_t(startTime);
	char buffer[80];
	std::tm tm = reindexer::localtime(t);
	std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm);
	std::stringstream ss;
	ss << buffer << '.' << std::setw(3) << std::setfill('0') << (duration_cast<milliseconds>(startTime.time_since_epoch()).count() % 1000);
	builder.Put("query_start", ss.str());
	builder.Put("state", DescribeState(state));
	if (state == WaitLock) {
		builder.Put("lock_description", "Wait lock for " + std::string(description));
	}
	builder.End();
}

}  // namespace reindexer
