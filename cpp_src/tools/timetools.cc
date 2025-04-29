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

TimeUnit ToTimeUnit(std::string_view unit) {
	if (iequals(unit, "sec"sv)) {
		return TimeUnit::sec;
	} else if (iequals(unit, "msec"sv)) {
		return TimeUnit::msec;
	} else if (iequals(unit, "usec"sv)) {
		return TimeUnit::usec;
	} else if (iequals(unit, "nsec"sv)) {
		return TimeUnit::nsec;
	}
	throw Error(errParams, "Unknown time unit parameter '{}'", unit);
}

int64_t getTimeNow(TimeUnit unit) {
	const auto tm = system_clock_w::now();
	const auto duration = tm.time_since_epoch();

	switch (unit) {
		case TimeUnit::sec:
			return duration_cast<seconds>(duration).count();
		case TimeUnit::msec:
			return duration_cast<milliseconds>(duration).count();
		case TimeUnit::usec:
			return duration_cast<microseconds>(duration).count();
		case TimeUnit::nsec:
			return duration_cast<nanoseconds>(duration).count();
		default:
			throw_as_assert;
	}
}

std::tm localtime(const std::time_t& time_tt) {
	std::tm tm;
#ifdef _WIN32
	localtime_s(&tm, &time_tt);
#else
	localtime_r(&time_tt, &tm);
#endif
	return tm;
}

}  // namespace reindexer
