#pragma once

#include <string>
#include <unordered_map>
#include "tools/errors.h"

union JsonValue;

namespace reindexer {

struct DBProfilingConfig {
	Error FromJSON(JsonValue &v);
	bool queriesPerfStats = false;
	size_t queriedThresholdUS = 10;
	bool perfStats = false;
	bool memStats = false;
};

struct DBLoggingConfig {
	Error FromJSON(JsonValue &v);
	std::unordered_map<std::string, int> logQueries;
};

}  // namespace reindexer
