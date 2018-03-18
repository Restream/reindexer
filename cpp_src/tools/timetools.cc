#include "tools/timetools.h"
#include <chrono>
#include <string>
#include "tools/errors.h"
#include "tools/stringstools.h"

using reindexer::lower;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
using std::string;

namespace reindexer {

int64_t getTimeNow(string mode = "sec") {
	auto tm = system_clock::now();
	auto duration = tm.time_since_epoch();

	mode = lower(mode);

	if (mode == "sec") {
		auto cnt = duration_cast<seconds>(duration).count();
		return static_cast<int64_t>(cnt);
	} else if (mode == "msec") {
		auto cnt = duration_cast<milliseconds>(duration).count();
		return static_cast<int64_t>(cnt);
	} else if (mode == "usec") {
		auto cnt = duration_cast<microseconds>(duration).count();
		return static_cast<int64_t>(cnt);
	} else if (mode == "nsec") {
		auto cnt = duration_cast<nanoseconds>(duration).count();
		return static_cast<int64_t>(cnt);
	}

	throw Error(errParams, "Unknown parameter in getTimeNow function.");
}

}  // namespace reindexer
