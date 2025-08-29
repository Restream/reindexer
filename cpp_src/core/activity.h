#pragma once

#include <string>
#include "tools/clock.h"

namespace reindexer {

class WrSerializer;

struct [[nodiscard]] Activity {
	unsigned id;
	int connectionId;
	std::string activityTracer;
	std::string user;
	std::string query;
	system_clock_w::time_point startTime;
	enum [[nodiscard]] State : unsigned {
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
