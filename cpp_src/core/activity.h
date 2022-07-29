#pragma once
#include <string>
#include <chrono>
namespace reindexer {

class WrSerializer;

struct Activity {
	unsigned id;
	std::string activityTracer;
	std::string user;
	std::string query;
	int connectionId;
	std::chrono::system_clock::time_point startTime;
	enum State : unsigned {
		InProgress = 0,
		WaitLock,
		Sending,
		IndexesLookup,
		SelectLoop,
		ProxiedViaClusterProxy,
		ProxiedViaShardingProxy
	} state;
	std::string_view description;
	void GetJSON(WrSerializer&) const;
	static std::string_view DescribeState(State) noexcept;
};

}  // namespace reindexer
