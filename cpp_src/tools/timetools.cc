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
	auto tm = system_clock::now();
	auto duration = tm.time_since_epoch();

	if (iequals(mode, "sec"sv)) {
		auto cnt = duration_cast<seconds>(duration).count();
		return static_cast<int64_t>(cnt);
	} else if (iequals(mode, "msec"sv)) {
		auto cnt = duration_cast<milliseconds>(duration).count();
		return static_cast<int64_t>(cnt);
	} else if (iequals(mode, "usec"sv)) {
		auto cnt = duration_cast<microseconds>(duration).count();
		return static_cast<int64_t>(cnt);
	} else if (iequals(mode, "nsec"sv)) {
		auto cnt = duration_cast<nanoseconds>(duration).count();
		return static_cast<int64_t>(cnt);
	}

	throw Error(errParams, "Unknown parameter '%s' in getTimeNow function.", mode);
}

}  // namespace reindexer
