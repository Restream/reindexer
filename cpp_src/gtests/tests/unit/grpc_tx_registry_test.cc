#if defined(WITH_GRPC)

#include "gtest/gtest.h"
#include "server/grpc/txregistry.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_set>
#include <vector>

namespace reindexer_tests {

using reindexer::grpc::TxRegistry;
using namespace std::chrono_literals;

class [[nodiscard]] GrpcTxRegistry : public ::testing::Test {
public:
	static constexpr std::chrono::seconds kIdleTimeout{5};
	static constexpr TxRegistry::TimePoint kStart{};

	static TxRegistry::TxData MakeTx() {
		TxRegistry::TxData data;
		data.dbName = "db";
		data.nsName = "ns";
		return data;
	}

	TxRegistry registry_{kIdleTimeout};
};

TEST_F(GrpcTxRegistry, ExpiresAfterIdleTimeout) {
	const uint64_t id = registry_.Add(MakeTx(), kStart);
	ASSERT_TRUE(registry_.ExtractExpired(kStart + kIdleTimeout - 1ms).empty());

	const auto expired = registry_.ExtractExpired(kStart + kIdleTimeout);
	ASSERT_EQ(expired.size(), 1u);
	ASSERT_EQ(expired[0].first, id);
	ASSERT_EQ(expired[0].second.nsName, "ns");
	ASSERT_FALSE(registry_.Acquire(id).has_value());
}

TEST_F(GrpcTxRegistry, ActiveRequestSuspendsExpiration) {
	const uint64_t id = registry_.Add(MakeTx(), kStart);
	ASSERT_TRUE(registry_.Acquire(id).has_value());

	const auto requestEnd = kStart + 10 * kIdleTimeout;
	ASSERT_TRUE(registry_.ExtractExpired(requestEnd).empty());
	registry_.Release(id, requestEnd);

	ASSERT_TRUE(registry_.ExtractExpired(requestEnd + kIdleTimeout - 1ms).empty());
	const auto expired = registry_.ExtractExpired(requestEnd + kIdleTimeout);
	ASSERT_EQ(expired.size(), 1u);
	ASSERT_EQ(expired[0].first, id);
}

TEST_F(GrpcTxRegistry, ExpiresOnlyAfterAllRequestsReleased) {
	const uint64_t id = registry_.Add(MakeTx(), kStart);
	ASSERT_TRUE(registry_.Acquire(id).has_value());
	ASSERT_TRUE(registry_.Acquire(id).has_value());

	const auto firstEnd = kStart + kIdleTimeout;
	registry_.Release(id, firstEnd);
	ASSERT_TRUE(registry_.ExtractExpired(firstEnd + 10 * kIdleTimeout).empty());

	const auto secondEnd = firstEnd + 10 * kIdleTimeout;
	registry_.Release(id, secondEnd);
	ASSERT_TRUE(registry_.ExtractExpired(secondEnd + kIdleTimeout - 1ms).empty());
	ASSERT_EQ(registry_.ExtractExpired(secondEnd + kIdleTimeout).size(), 1u);
}

TEST_F(GrpcTxRegistry, StaleReleaseKeepsLaterDeadline) {
	const uint64_t id = registry_.Add(MakeTx(), kStart);
	ASSERT_TRUE(registry_.Acquire(id).has_value());
	ASSERT_TRUE(registry_.Acquire(id).has_value());

	const auto laterEnd = kStart + 10 * kIdleTimeout;
	const auto earlierEnd = kStart + kIdleTimeout;
	registry_.Release(id, laterEnd);
	registry_.Release(id, earlierEnd);

	ASSERT_TRUE(registry_.ExtractExpired(laterEnd + kIdleTimeout - 1ms).empty());
	ASSERT_EQ(registry_.ExtractExpired(laterEnd + kIdleTimeout).size(), 1u);
}

TEST_F(GrpcTxRegistry, ReleaseAfterRemoveIsNoop) {
	const uint64_t id = registry_.Add(MakeTx(), kStart);
	ASSERT_TRUE(registry_.Acquire(id).has_value());
	ASSERT_TRUE(registry_.Remove(id));
	registry_.Release(id, kStart);

	ASSERT_FALSE(registry_.Acquire(id).has_value());
	ASSERT_TRUE(registry_.ExtractExpired(kStart + 10 * kIdleTimeout).empty());
}

TEST_F(GrpcTxRegistry, RemoveSucceedsOnlyOnce) {
	const uint64_t id = registry_.Add(MakeTx(), kStart);
	ASSERT_TRUE(registry_.Acquire(id).has_value());
	ASSERT_TRUE(registry_.Acquire(id).has_value());

	ASSERT_TRUE(registry_.Remove(id));
	ASSERT_FALSE(registry_.Remove(id));
}

TEST_F(GrpcTxRegistry, ConcurrentRemoveHasSingleWinner) {
	constexpr size_t kThreads = 8;
	constexpr size_t kIterations = 1000;
	for (size_t i = 0; i < kIterations; ++i) {
		const uint64_t id = registry_.Add(MakeTx(), kStart);
		std::atomic<size_t> winners{0};
		std::vector<std::thread> threads;
		threads.reserve(kThreads);
		for (size_t t = 0; t < kThreads; ++t) {
			threads.emplace_back([this, &winners, id] {
				if (registry_.Remove(id)) {
					++winners;
				}
			});
		}
		for (auto& th : threads) {
			th.join();
		}
		ASSERT_EQ(winners.load(), 1u);
	}
}

TEST_F(GrpcTxRegistry, ConcurrentAddGivesUniqueIds) {
	constexpr size_t kThreads = 8;
	constexpr size_t kTxPerThread = 1000;
	std::vector<std::vector<uint64_t>> ids(kThreads);
	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (size_t t = 0; t < kThreads; ++t) {
		threads.emplace_back([this, &ids, t] {
			ids[t].reserve(kTxPerThread);
			for (size_t i = 0; i < kTxPerThread; ++i) {
				ids[t].push_back(registry_.Add(MakeTx(), kStart));
			}
		});
	}
	for (auto& th : threads) {
		th.join();
	}

	std::unordered_set<uint64_t> unique;
	for (const auto& threadIds : ids) {
		unique.insert(threadIds.begin(), threadIds.end());
	}
	ASSERT_EQ(unique.size(), kThreads * kTxPerThread);
	ASSERT_EQ(registry_.ExtractExpired(kStart + kIdleTimeout).size(), kThreads * kTxPerThread);
}

}  // namespace reindexer_tests

#endif
