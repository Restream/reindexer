#include "tools/timetools.h"
#include <chrono>
#include <string>
#include "tools/errors.h"
#include "tools/stringstools.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
using namespace std::string_view_literals;

namespace reindexer {

int64_t getTimeNow(std::string_view mode) {
	const auto tm = system_clock::now();
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
