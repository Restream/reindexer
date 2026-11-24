#ifndef _WIN32

#include <gtest/gtest.h>
#include <chrono>
#include <csignal>

#include "net/ev/ev.h"

namespace reindexer_tests {

using namespace reindexer::net;

// NOLINTBEGIN(rx-perf-lambda-to-std-function-allocation)

// #2033: reindexer_server installs its signal handlers before the main loop starts. A signal caught at that point must be
// handled on the first loop iteration instead of waiting for an unrelated I/O event or timer.
TEST(EvLoop, SignalCaughtBeforeRunIsHandledImmediately) {
	constexpr auto kWatchdogPeriod = std::chrono::seconds(5);
	ev::dynamic_loop loop;
	bool handled = false;
	bool watchdogFired = false;

	ev::sig sigWatcher;
	sigWatcher.set(loop);
	sigWatcher.set([&handled](ev::sig& s) {
		handled = true;
		s.loop.break_loop();
	});
	sigWatcher.start(SIGUSR1);

	// The only other wakeup source: if a pending signal does not prevent blocking, the loop waits for this timer.
	ev::timer watchdog;
	watchdog.set(loop);
	watchdog.set([&watchdogFired](ev::timer& t, int) {
		watchdogFired = true;
		t.loop.break_loop();
	});
	watchdog.start(std::chrono::duration<double>(kWatchdogPeriod).count());

	// The signal is delivered to the calling thread before raise() returns, i.e. before the loop runs
	ASSERT_EQ(raise(SIGUSR1), 0);

	const auto start = std::chrono::steady_clock::now();
	loop.run();
	const auto elapsed = std::chrono::steady_clock::now() - start;

	EXPECT_TRUE(handled);
	EXPECT_FALSE(watchdogFired);
	EXPECT_LT(elapsed, kWatchdogPeriod / 2);
}

// NOLINTEND(rx-perf-lambda-to-std-function-allocation)

}  // namespace reindexer_tests

#endif	// _WIN32
