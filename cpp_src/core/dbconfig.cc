#include "dbconfig.h"
#include <limits.h>
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/stringstools.h"

namespace reindexer {

Error DBProfilingConfig::FromJSON(JsonValue &jvalue) {
	try {
		if (jvalue.getTag() == JSON_NULL) return errOK;
		if (jvalue.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected object in 'profiling' key");

		for (auto elem : jvalue) {
			parseJsonField("queriesperfstats", queriesPerfStats, elem);
			parseJsonField("queries_threshold_us", queriedThresholdUS, elem, 0, INT_MAX);
			parseJsonField("perfstats", perfStats, elem);
			parseJsonField("memstats", memStats, elem);
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error DBLoggingConfig::FromJSON(JsonValue &jvalue) {
	try {
		if (jvalue.getTag() == JSON_NULL) return errOK;
		if (jvalue.getTag() != JSON_ARRAY) return Error(errParseJson, "Expected array in 'log_queries' key");

		for (auto elem : jvalue) {
			auto &subv = elem->value;
			if (subv.getTag() != JSON_OBJECT) {
				return Error(errParseJson, "Expected object in 'log_queries' array element");
			}

			string name, logLevel;
			for (auto subelem : subv) {
				parseJsonField("namespace", name, subelem);
				parseJsonField("log_level", logLevel, subelem);
			}
			logQueries.insert({name, logLevelFromString(logLevel)});
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

}  // namespace reindexer
