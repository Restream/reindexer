#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "core/embedding/circuitbreaker.h"
#include "tools/clock.h"
#include "tools/errors.h"

namespace {

using reindexer::EmbedderCircuitBreaker;
using reindexer::EmbedderCircuitBreakerDefaults;
using reindexer::Error;
using reindexer::steady_clock_w;
using namespace std::chrono_literals;

EmbedderCircuitBreaker::Config SmallConfig() {
	return EmbedderCircuitBreaker::Config{
		.threshold = 3,
		.thresholdTimeout = 15s,
		.cooldown = 7500ms,
	};
}

void FailOnce(EmbedderCircuitBreaker& breaker, steady_clock_w::time_point now) {
	auto permit = breaker.Acquire("vec", now);
	permit.ReportFailure(now);
}

void SucceedOnce(EmbedderCircuitBreaker& breaker, steady_clock_w::time_point now) {
	auto permit = breaker.Acquire("vec", now);
	permit.ReportSuccess(now);
}

TEST(EmbedderCircuitBreaker, OpensAfterConsecutiveErrors) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	EXPECT_FALSE(breaker.IsOpen());
	FailOnce(breaker, t0 + 2ms);
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_THROW(
		{
			try {
				[[maybe_unused]] auto p = breaker.Acquire("vec", t0 + 3ms);
			} catch (const Error& e) {
				EXPECT_EQ(e.code(), errNetwork);
				EXPECT_NE(std::string_view{e.what()}.find("retry in"), std::string_view::npos);
				throw;
			}
		},
		Error);
}

TEST(EmbedderCircuitBreaker, SuccessResetsErrorCounter) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	SucceedOnce(breaker, t0 + 2ms);
	EXPECT_EQ(breaker.ConsecutiveErrors(), 0u);

	FailOnce(breaker, t0 + 3ms);
	FailOnce(breaker, t0 + 4ms);
	EXPECT_FALSE(breaker.IsOpen());
	FailOnce(breaker, t0 + 5ms);
	EXPECT_TRUE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, IdleWindowResetsErrorCounter) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	// Gap == thresholdTimeout resets before counting the new failure.
	FailOnce(breaker, t0 + 1ms + 15s);
	EXPECT_FALSE(breaker.IsOpen());
	EXPECT_EQ(breaker.ConsecutiveErrors(), 1u);
}

TEST(EmbedderCircuitBreaker, IdleWindowJustBelowTimeoutKeepsStreak) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	// Gap just below thresholdTimeout keeps the streak.
	FailOnce(breaker, t0 + 1ms + 15s - 1ms);
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_EQ(breaker.ConsecutiveErrors(), 3u);
}

TEST(EmbedderCircuitBreaker, OutOfOrderReportsDoNotMoveLastRequestBackwards) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0 + 10ms);
	FailOnce(breaker, t0 + 5ms);
	// The second report is older. Idle timeout is measured from the latest report
	// timestamp (10ms), so this failure remains in the same streak.
	FailOnce(breaker, t0 + 10ms + 15s - 1ms);
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_EQ(breaker.ConsecutiveErrors(), 3u);
}

TEST(EmbedderCircuitBreaker, IdleResetDisabledWhenTimeoutZero) {
	EmbedderCircuitBreaker::Config cfg{
		.threshold = 3,
		.thresholdTimeout = 0ms,
		.cooldown = 7500ms,
	};
	EmbedderCircuitBreaker breaker{cfg};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	// Large idle gap must not reset the streak when timeout is disabled.
	FailOnce(breaker, t0 + 1h);
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_EQ(breaker.ConsecutiveErrors(), 3u);
}

TEST(EmbedderCircuitBreaker, PermitDestructorCountsAsFailure) {
	// Destructor always reports failure with steady_clock_w::now(); keep Acquire on the
	// same clock domain (no virtual timeline) so idle reset does not interfere.
	EmbedderCircuitBreaker breaker{SmallConfig()};

	{
		[[maybe_unused]] auto permit = breaker.Acquire("vec");
	}
	{
		[[maybe_unused]] auto permit = breaker.Acquire("vec");
	}
	EXPECT_FALSE(breaker.IsOpen());
	EXPECT_EQ(breaker.ConsecutiveErrors(), 2u);
	{
		[[maybe_unused]] auto permit = breaker.Acquire("vec");
	}
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_EQ(breaker.ConsecutiveErrors(), 3u);
}

TEST(EmbedderCircuitBreaker, ProbeDestructorRestartsCooldown) {
	EmbedderCircuitBreaker::Config cfg{
		.threshold = 1,
		.thresholdTimeout = 15s,
		.cooldown = 7500ms,
	};
	EmbedderCircuitBreaker breaker{cfg};
	const auto openedAt = steady_clock_w::now() - cfg.cooldown;

	FailOnce(breaker, openedAt);
	ASSERT_TRUE(breaker.IsOpen());

	{
		// Cooldown has elapsed, so this is the HalfOpen probe. Dropping its permit
		// reports a failure at the current time and must restart cooldown.
		[[maybe_unused]] auto probe = breaker.Acquire("vec");
	}

	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_THROW({ [[maybe_unused]] auto probe = breaker.Acquire("vec"); }, Error);
}

TEST(EmbedderCircuitBreaker, InFlightSuccessDoesNotCloseAfterOpen) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	auto inFlight = breaker.Acquire("vec", t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	FailOnce(breaker, t0 + 3ms);
	ASSERT_TRUE(breaker.IsOpen());

	inFlight.ReportSuccess(t0 + 4ms);
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_THROW({ [[maybe_unused]] auto p = breaker.Acquire("vec", t0 + 5ms); }, Error);
}

TEST(EmbedderCircuitBreaker, InFlightFailureDoesNotRestartCooldown) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	auto inFlight = breaker.Acquire("vec", t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	FailOnce(breaker, t0 + 3ms);
	ASSERT_TRUE(breaker.IsOpen());

	// Opened at t0+3ms; without restart, probe is allowed at t0+3ms+7500ms.
	inFlight.ReportFailure(t0 + 3ms + 1s);
	EXPECT_TRUE(breaker.IsOpen());

	auto probe = breaker.Acquire("vec", t0 + 3ms + 7500ms);
	probe.ReportSuccess(t0 + 3ms + 7500ms);
	EXPECT_FALSE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, InFlightClosedDoesNotActAsProbe) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	auto stale = breaker.Acquire("vec", t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	FailOnce(breaker, t0 + 3ms);
	ASSERT_TRUE(breaker.IsOpen());

	auto probe = breaker.Acquire("vec", t0 + 3ms + 7500ms);
	// Stale Closed permit must not close HalfOpen or restart cooldown.
	stale.ReportSuccess(t0 + 3ms + 7500ms + 1ms);
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_THROW({ [[maybe_unused]] auto p = breaker.Acquire("vec", t0 + 3ms + 7500ms + 2ms); }, Error);

	probe.ReportSuccess(t0 + 3ms + 7500ms + 3ms);
	EXPECT_FALSE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, StalePermitAfterRecoveryDoesNotResetNewStreak) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	auto stale = breaker.Acquire("vec", t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	FailOnce(breaker, t0 + 3ms);
	ASSERT_TRUE(breaker.IsOpen());

	// Full recovery: cooldown → probe success → Closed again.
	auto probe = breaker.Acquire("vec", t0 + 3ms + 7500ms);
	probe.ReportSuccess(t0 + 3ms + 7500ms);
	ASSERT_FALSE(breaker.IsOpen());

	const auto t1 = t0 + 3ms + 7500ms + 1s;
	FailOnce(breaker, t1);
	FailOnce(breaker, t1 + 1ms);
	EXPECT_EQ(breaker.ConsecutiveErrors(), 2u);
	EXPECT_FALSE(breaker.IsOpen());

	// Stale Closed permit from the previous generation must not clear the new streak.
	stale.ReportSuccess(t1 + 2ms);
	EXPECT_EQ(breaker.ConsecutiveErrors(), 2u);
	EXPECT_FALSE(breaker.IsOpen());

	FailOnce(breaker, t1 + 3ms);
	EXPECT_TRUE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, StaleFailureAfterRecoveryDoesNotExtendNewStreak) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	auto stale = breaker.Acquire("vec", t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	FailOnce(breaker, t0 + 3ms);
	ASSERT_TRUE(breaker.IsOpen());

	auto probe = breaker.Acquire("vec", t0 + 3ms + 7500ms);
	probe.ReportSuccess(t0 + 3ms + 7500ms);
	ASSERT_FALSE(breaker.IsOpen());

	const auto t1 = t0 + 3ms + 7500ms + 1s;
	FailOnce(breaker, t1);
	EXPECT_EQ(breaker.ConsecutiveErrors(), 1u);

	// Stale Closed permit from the previous generation must not increment
	// the new generation's error streak.
	stale.ReportFailure(t1 + 1ms);
	EXPECT_EQ(breaker.ConsecutiveErrors(), 1u);
	EXPECT_FALSE(breaker.IsOpen());

	FailOnce(breaker, t1 + 2ms);
	EXPECT_FALSE(breaker.IsOpen());
	FailOnce(breaker, t1 + 3ms);
	EXPECT_TRUE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, ThresholdOneOpensOnFirstFailure) {
	EmbedderCircuitBreaker::Config cfg{
		.threshold = 1,
		.thresholdTimeout = 15s,
		.cooldown = 7500ms,
	};
	EmbedderCircuitBreaker breaker{cfg};
	const auto t0 = steady_clock_w::time_point{};

	EXPECT_FALSE(breaker.IsOpen());
	FailOnce(breaker, t0);
	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_EQ(breaker.ConsecutiveErrors(), 1u);
}

TEST(EmbedderCircuitBreaker, CooldownRejectsThenAllowsProbe) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	ASSERT_TRUE(breaker.IsOpen());

	EXPECT_THROW({ [[maybe_unused]] auto p = breaker.Acquire("vec", t0 + 2ms + 7s); }, Error);

	const auto probeAt = t0 + 2ms + 7500ms;
	auto probe = breaker.Acquire("vec", probeAt);
	EXPECT_THROW(
		{
			try {
				[[maybe_unused]] auto p = breaker.Acquire("vec", probeAt);
			} catch (const Error& e) {
				EXPECT_NE(std::string_view{e.what()}.find("probe request is in progress"), std::string_view::npos);
				throw;
			}
		},
		Error);

	probe.ReportSuccess(probeAt);
	EXPECT_FALSE(breaker.IsOpen());
	SucceedOnce(breaker, probeAt + 1s);
}

TEST(EmbedderCircuitBreaker, FailedProbeRestartsCooldown) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);

	const auto probeAt = t0 + 2ms + 7500ms;
	FailOnce(breaker, probeAt);	 // half-open probe fails → cooldown restarts
	EXPECT_TRUE(breaker.IsOpen());

	EXPECT_THROW({ [[maybe_unused]] auto p = breaker.Acquire("vec", probeAt + 7s); }, Error);

	auto probe = breaker.Acquire("vec", probeAt + 7500ms);
	probe.ReportSuccess(probeAt + 7500ms);
	EXPECT_FALSE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, DefaultThresholdOpensAt16) {
	EmbedderCircuitBreaker breaker;
	const auto t0 = steady_clock_w::time_point{};

	for (size_t i = 0; i < EmbedderCircuitBreakerDefaults::kThreshold - 1; ++i) {
		FailOnce(breaker, t0 + i * 1ms);
		EXPECT_FALSE(breaker.IsOpen()) << i;
	}
	FailOnce(breaker, t0 + EmbedderCircuitBreakerDefaults::kThreshold * 1ms);
	EXPECT_TRUE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, ErrorMessageIncludesRemainingCooldown) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);

	try {
		[[maybe_unused]] auto p = breaker.Acquire("vec", t0 + 2ms + 2500ms);
		FAIL() << "expected circuit breaker error";
	} catch (const Error& e) {
		// 7.5s - 2.5s = 5.0s remaining
		EXPECT_NE(std::string_view{e.what()}.find("retry in 5.0s"), std::string_view::npos) << e.what();
	}
}

TEST(EmbedderCircuitBreaker, ConcurrentExclusiveProbe) {
	EmbedderCircuitBreaker breaker{SmallConfig()};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	ASSERT_TRUE(breaker.IsOpen());

	const auto probeAt = t0 + 2ms + EmbedderCircuitBreakerDefaults::kCooldown;
	constexpr int kThreads = 32;
	std::atomic<int> acquireDone{0};
	std::atomic<int> probeAcquired{0};
	std::atomic<int> rejected{0};

	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (int i = 0; i < kThreads; ++i) {
		threads.emplace_back([&] {
			try {
				auto permit = breaker.Acquire("vec", probeAt);
				probeAcquired.fetch_add(1, std::memory_order_relaxed);
				acquireDone.fetch_add(1, std::memory_order_relaxed);
				// Hold Half-Open until every thread has finished Acquire.
				while (acquireDone.load(std::memory_order_relaxed) < kThreads) {
					std::this_thread::yield();
				}
				permit.ReportSuccess(probeAt);
			} catch (const Error&) {
				rejected.fetch_add(1, std::memory_order_relaxed);
				acquireDone.fetch_add(1, std::memory_order_relaxed);
			}
		});
	}
	for (auto& th : threads) {
		th.join();
	}

	EXPECT_EQ(acquireDone.load(std::memory_order_relaxed), kThreads);
	EXPECT_EQ(probeAcquired.load(std::memory_order_relaxed), 1);
	EXPECT_EQ(rejected.load(std::memory_order_relaxed), kThreads - 1);
	EXPECT_EQ(probeAcquired.load(std::memory_order_relaxed) + rejected.load(std::memory_order_relaxed), kThreads);
	EXPECT_FALSE(breaker.IsOpen());
}

TEST(EmbedderCircuitBreaker, ConcurrentFailuresOpenBreaker) {
	EmbedderCircuitBreaker::Config cfg{
		.threshold = 16,
		.thresholdTimeout = 15s,
		.cooldown = 7500ms,
	};
	EmbedderCircuitBreaker breaker{cfg};

	constexpr int kThreads = 8;
	constexpr int kFailsPerThread = 8;
	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (int t = 0; t < kThreads; ++t) {
		threads.emplace_back([&, t] {
			for (int i = 0; i < kFailsPerThread; ++i) {
				try {
					FailOnce(breaker, steady_clock_w::time_point{} + std::chrono::milliseconds(t * kFailsPerThread + i));
				} catch (const Error& e) {
					// Already open — expected under contention.
					EXPECT_EQ(e.code(), errNetwork);
				}
			}
		});
	}
	for (auto& th : threads) {
		th.join();
	}

	EXPECT_TRUE(breaker.IsOpen());
	EXPECT_GE(breaker.ConsecutiveErrors(), static_cast<size_t>(cfg.threshold));
}

TEST(EmbedderCircuitBreaker, ConcurrentMixedTrafficStress) {
	EmbedderCircuitBreaker::Config cfg{
		.threshold = 8,
		.thresholdTimeout = 15s,
		.cooldown = 7500ms,
	};
	EmbedderCircuitBreaker breaker{cfg};

	constexpr int kThreads = 8;
	constexpr int kIters = 2'000;
	constexpr uint64_t kTotal = static_cast<uint64_t>(kThreads) * kIters;
	std::atomic<uint64_t> allowed{0};
	std::atomic<uint64_t> blocked{0};
	std::atomic<uint64_t> successes{0};
	std::atomic<uint64_t> failures{0};

	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (int t = 0; t < kThreads; ++t) {
		threads.emplace_back([&, t] {
			for (int i = 0; i < kIters; ++i) {
				try {
					auto permit = breaker.Acquire("vec");
					allowed.fetch_add(1, std::memory_order_relaxed);
					// Deterministic mix: 80% of failures to exercise open/half-open paths.
					if (((t + i) % 5) == 0) {
						permit.ReportSuccess();
						successes.fetch_add(1, std::memory_order_relaxed);
					} else {
						permit.ReportFailure();
						failures.fetch_add(1, std::memory_order_relaxed);
					}
				} catch (const Error& e) {
					EXPECT_EQ(e.code(), errNetwork);
					blocked.fetch_add(1, std::memory_order_relaxed);
				}
			}
		});
	}
	for (auto& th : threads) {
		th.join();
	}

	const auto allowedN = allowed.load(std::memory_order_relaxed);
	const auto blockedN = blocked.load(std::memory_order_relaxed);
	const auto successesN = successes.load(std::memory_order_relaxed);
	const auto failuresN = failures.load(std::memory_order_relaxed);

	// Every attempt is either admitted or rejected; every admission reports exactly once.
	EXPECT_EQ(allowedN + blockedN, kTotal);
	EXPECT_EQ(allowedN, successesN + failuresN);
	EXPECT_GT(allowedN, 0u);
	EXPECT_GT(blockedN, 0u);
	EXPECT_GT(successesN, 0u);
	EXPECT_GT(failuresN, 0u);

	const auto errors = breaker.ConsecutiveErrors();
	EXPECT_LE(errors, cfg.threshold);
	if (!breaker.IsOpen()) {
		// Closed ⇒ counter must be strictly below the trip threshold.
		EXPECT_LT(errors, cfg.threshold);
	}
}

TEST(EmbedderCircuitBreaker, DisabledWhenThresholdNonPositive) {
	EmbedderCircuitBreaker::Config cfg{
		.threshold = 0,
		.thresholdTimeout = 15s,
		.cooldown = 7500ms,
	};
	EmbedderCircuitBreaker breaker{cfg};
	const auto t0 = steady_clock_w::time_point{};

	for (int i = 0; i < 32; ++i) {
		FailOnce(breaker, t0 + i * 1ms);
	}
	EXPECT_FALSE(breaker.IsOpen());
	SucceedOnce(breaker, t0 + 1s);
}

TEST(EmbedderCircuitBreaker, DisabledWhenCooldownNonPositive) {
	EmbedderCircuitBreaker::Config cfg{
		.threshold = 3,
		.thresholdTimeout = 15s,
		.cooldown = 0ms,
	};
	EmbedderCircuitBreaker breaker{cfg};
	const auto t0 = steady_clock_w::time_point{};

	FailOnce(breaker, t0);
	FailOnce(breaker, t0 + 1ms);
	FailOnce(breaker, t0 + 2ms);
	EXPECT_FALSE(breaker.IsOpen());
	SucceedOnce(breaker, t0 + 3ms);
}

}  // namespace
