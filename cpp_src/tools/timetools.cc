#include "tools/timetools.h"
#include <chrono>
#include "tools/clock.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;
using std::chrono::seconds;
using namespace std::string_view_literals;

namespace reindexer {

int64_t getTimeNow(std::string_view mode) {
	const auto tm = system_clock_w::now();
	const auto duration = tm.time_since_epoch();

	if (iequals(mode, "sec"sv)) {
		return duration_cast<seconds>(duration).count();
	} else if (iequals(mode, "msec"sv)) {
		return duration_cast<milliseconds>(duration).count();
	} else if (iequals(mode, "usec"sv)) {
		return duration_cast<microseconds>(duration).count();
	} else if (iequals(mode, "nsec"sv)) {
		return duration_cast<nanoseconds>(duration).count();
	}

	throw Error(errParams, "Unknown parameter '%s' in getTimeNow function.", mode);
}

}  // namespace reindexer
