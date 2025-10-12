#pragma once

#include <chrono>

#if defined(__linux__) && !defined(__APPLE__)
#define REINDEXER_OVERRIDES_STD_NOW
#endif	// !defined(__linux__) && !defined(__APPLE__)

#ifdef REINDEXER_OVERRIDES_STD_NOW
#include <time.h>
#endif	// REINDEXER_OVERRIDES_STD_NOW

namespace reindexer {

class [[nodiscard]] system_clock_w : public std::chrono::system_clock {
public:
	using std::chrono::system_clock::duration;
	using std::chrono::system_clock::rep;
	using std::chrono::system_clock::period;
	using std::chrono::system_clock::time_point;

	// Overload for the stdlib function. In some versions of stdlib (for example, version for centos7) default now()-call
	// does not use vDSO (https://manpages.org/vdso/7) and runs ~8 times slower than direct clock_gettime()-call.
	static time_point now() noexcept {
#ifdef REINDEXER_OVERRIDES_STD_NOW
		timespec t;
		// No return code handling for clock_gettime. Stdlib does not check it either.
		clock_gettime(CLOCK_REALTIME, &t);
		return time_point(duration(std::chrono::seconds(t.tv_sec) + std::chrono::nanoseconds(t.tv_nsec)));
#else	// !REINDEXER_OVERRIDES_STD_NOW
		return std::chrono::system_clock::now();
#endif	// !REINDEXER_OVERRIDES_STD_NOW
	}
	// Coarse version of the clock gets time ~2-3 times faster, but has worse granularity. Expected granularity is about 2-4 ms.
	static time_point now_coarse() noexcept {
#if defined(CLOCK_REALTIME_COARSE)
		timespec t;
		clock_gettime(CLOCK_REALTIME_COARSE, &t);
		return time_point(duration(std::chrono::seconds(t.tv_sec) + std::chrono::nanoseconds(t.tv_nsec)));
#else	// !defined(CLOCK_REALTIME_COARSE)
		return now();
#endif	// !defined(CLOCK_REALTIME_COARSE)
	}
};

class [[nodiscard]] steady_clock_w : public std::chrono::steady_clock {
public:
	using std::chrono::steady_clock::duration;
	using std::chrono::steady_clock::rep;
	using std::chrono::steady_clock::period;
	using std::chrono::steady_clock::time_point;

	// Overload for the stdlib function. In some versions of stdlib (for example, version for centos7) default now()-call
	// does not use vDSO (https://manpages.org/vdso/7) and runs ~8 times slower than direct clock_gettime()-call.
	static time_point now() noexcept {
#ifdef REINDEXER_OVERRIDES_STD_NOW
		timespec t;
		// No return code handling for clock_gettime. Stdlib does not check it either.
		clock_gettime(CLOCK_MONOTONIC, &t);
		return time_point(duration(std::chrono::seconds(t.tv_sec) + std::chrono::nanoseconds(t.tv_nsec)));
#else	// !REINDEXER_OVERRIDES_STD_NOW
		return std::chrono::steady_clock::now();
#endif	// !REINDEXER_OVERRIDES_STD_NOW
	}
	// Coarse version of the clock gets time ~2-3 times faster, but has worse granularity. Expected granularity is about 2-4 ms.
	static time_point now_coarse() noexcept {
#if defined(CLOCK_MONOTONIC_COARSE)
		timespec t;
		clock_gettime(CLOCK_MONOTONIC_COARSE, &t);
		return time_point(duration(std::chrono::seconds(t.tv_sec) + std::chrono::nanoseconds(t.tv_nsec)));
#else	// !defined(CLOCK_REALTIME_COARSE)
		return now();
#endif	// !defined(CLOCK_REALTIME_COARSE)
	}
};

}  // namespace reindexer
