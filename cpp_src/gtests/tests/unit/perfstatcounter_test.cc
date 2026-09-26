#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "core/perfstatcounter.h"

namespace reindexer_tests {

using reindexer::PerfStatCounterCountAvgST;
using reindexer::PerfStatCounterST;
using namespace std::chrono_literals;

namespace {

struct [[nodiscard]] Stat {
	size_t totalHitCount;
	size_t totalAvgTimeUs;
	size_t totalAvgLockTimeUs;
	size_t lastSecHitCount;
	size_t lastSecAvgTimeUs;
	size_t lastSecAvgLockTimeUs;
	double stddev;
	size_t minTimeUs;
	size_t maxTimeUs;
};

constexpr auto kHitTime = 42us;
constexpr auto kIdleGap = 2100ms;
constexpr auto kPublishGap = 1100ms;

}  // namespace

TEST(PerfStatCounter, HitAfterIdleKeepsSampleForNextBucket) {
	PerfStatCounterST counter;
	counter.Hit(kHitTime);
	std::this_thread::sleep_for(kIdleGap);

	counter.Hit(kHitTime);
	auto stat = counter.Get<Stat>();
	EXPECT_EQ(stat.totalHitCount, 2);
	EXPECT_EQ(stat.lastSecHitCount, 0);

	std::this_thread::sleep_for(kPublishGap);
	stat = counter.Get<Stat>();
	EXPECT_EQ(stat.lastSecHitCount, 1);
}

TEST(PerfStatCounter, HitAfterIdleKeepsPairedLockHit) {
	PerfStatCounterST counter;
	counter.LockHit(100us);
	counter.Hit(kHitTime);
	std::this_thread::sleep_for(kIdleGap);

	constexpr auto kLock = 7us;
	counter.LockHit(kLock);
	counter.Hit(kHitTime);
	auto stat = counter.Get<Stat>();
	EXPECT_EQ(stat.lastSecHitCount, 0);

	std::this_thread::sleep_for(kPublishGap);
	stat = counter.Get<Stat>();
	EXPECT_EQ(stat.lastSecHitCount, 1);
	EXPECT_EQ(stat.lastSecAvgLockTimeUs, size_t(kLock.count()));
}

TEST(PerfStatCounter, GetAfterIdleDoesNotInventHits) {
	PerfStatCounterST counter;
	counter.Hit(kHitTime);
	std::this_thread::sleep_for(kIdleGap);

	auto stat = counter.Get<Stat>();
	EXPECT_EQ(stat.totalHitCount, 1);
	EXPECT_EQ(stat.lastSecHitCount, 0);

	std::this_thread::sleep_for(kPublishGap);
	stat = counter.Get<Stat>();
	EXPECT_EQ(stat.lastSecHitCount, 0);
}

TEST(PerfStatCounterCountAvg, HitAfterIdleKeepsSampleForNextBucket) {
	PerfStatCounterCountAvgST counter;
	counter.Hit(10);
	std::this_thread::sleep_for(kIdleGap);

	counter.Hit(20);
	EXPECT_FLOAT_EQ(counter.Get(), 0.0f);

	std::this_thread::sleep_for(kPublishGap);
	EXPECT_FLOAT_EQ(counter.Get(), 20.0f);
}

}  // namespace reindexer_tests
