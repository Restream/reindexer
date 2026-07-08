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

std::string_view TimeUnitToString(TimeUnit timeUnit) {
	switch (timeUnit) {
		case TimeUnit::sec:
			return "sec";
		case TimeUnit::msec:
			return "msec";
		case TimeUnit::usec:
			return "usec";
		case TimeUnit::nsec:
			return "nsec";
	}
	throw Error(errParams, "Unknown time unit parameter '{}'", static_cast<uint8_t>(timeUnit));
}

int64_t ConvertTime(int64_t t, TimeUnit from, TimeUnit to) {
	// Conversion factors to nanoseconds for each unit:
	// sec: 1 sec = 1,000,000,000 nsec
	// msec: 1 msec = 1,000,000 nsec
	// usec: 1 usec = 1,000 nsec
	// nsec: 1 nsec = 1 nsec

	auto toNanoseconds = [](TimeUnit unit, int64_t t) -> int64_t {
		switch (unit) {
			case TimeUnit::sec:
				return t * 1'000'000'000LL;
			case TimeUnit::msec:
				return t * 1'000'000LL;
			case TimeUnit::usec:
				return t * 1000LL;
			case TimeUnit::nsec:
				return t;
		}
		throw Error(errParams, "Unknown time unit parameter '{}'", static_cast<uint8_t>(unit));
	};

	auto fromNanoseconds = [](TimeUnit unit, int64_t t) -> int64_t {
		switch (unit) {
			case TimeUnit::sec:
				return t / 1'000'000'000LL;
			case TimeUnit::msec:
				return t / 1'000'000LL;
			case TimeUnit::usec:
				return t / 1'000LL;
			case TimeUnit::nsec:
				return t;
		}
		throw Error(errParams, "Unknown time unit parameter '{}'", static_cast<uint8_t>(unit));
	};

	const int64_t nanoseconds{toNanoseconds(from, t)};
	return fromNanoseconds(to, nanoseconds);
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
