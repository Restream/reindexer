#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "core/dbconfig.h"
#include "core/query/query.h"
#include "core/querycache.h"
#include "debug/allocdebug.h"
#include "gtests/tests/gtest_cout.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/serializer.h"

using reindexer::Query;
using reindexer::WrSerializer;
using reindexer::Serializer;
using reindexer::QueryCountCache;
using reindexer::QueryCacheKey;
using reindexer::QueryCountCacheVal;
using reindexer::EqQueryCacheKey;
using reindexer::kCountCachedKeyMode;

struct [[nodiscard]] CacheJoinedSelectorMock {
	std::string_view RightNsName() const noexcept { return rightNsName; }
	int64_t LastUpdateTime() const noexcept { return lastUpdateTime; }

	std::string rightNsName;
	int64_t lastUpdateTime;
};
using CacheJoinedSelectorsMock = std::vector<CacheJoinedSelectorMock>;

TEST(LruCache, SimpleTest) {
	constexpr int kNsCount = 10;
	constexpr int kSingleJoinNsCount = 5;
	constexpr int kDoubleJoinNsCount = 5;
	constexpr int kIterCount = 3000;

	struct [[nodiscard]] QueryCacheData {
		const CacheJoinedSelectorsMock* JoinedSelectorsPtr() const noexcept { return joinedSelectors.size() ? &joinedSelectors : nullptr; }

		Query q;
		CacheJoinedSelectorsMock joinedSelectors = {};
		bool cached = false;
		int64_t expectedTotal = -1;
	};
	std::vector<QueryCacheData> qs;

	PRINTF("preparing queries for caching ...\n");
	int i = 0;
	for (int j = 0; j < kNsCount; ++j, ++i) {
		qs.emplace_back(QueryCacheData{.q = Query(fmt::format("namespace_{}", i))});
	}
	for (int j = 0; j < kSingleJoinNsCount; ++j, ++i) {
		const std::string kJoinedNsName = fmt::format("joined_namespace_{}", j);
		qs.emplace_back(QueryCacheData{
			.q = Query(fmt::format("namespace_{}", i))
					 .InnerJoin(fmt::format("joined_field_{}", j), fmt::format("main_field_{}", j % 2), CondEq, Query(kJoinedNsName)),
			.joinedSelectors = {CacheJoinedSelectorMock{kJoinedNsName, 123}}});
	}
	for (int j = 0; j < kDoubleJoinNsCount; ++j, ++i) {
		const std::string kJoinedNsName1 = fmt::format("second_joined_namespace_{}", j);
		const std::string kJoinedNsName2 = fmt::format("third_joined_namespace_{}", j);
		constexpr int64_t kUpdateTime1 = 123;
		constexpr int64_t kUpdateTime2 = 321;
		if (j % 3 == 0) {
			qs.emplace_back(QueryCacheData{
				.q =
					Query(fmt::format("namespace_{}", i))
						.InnerJoin(fmt::format("joined_field_{}", j), fmt::format("main_field_{}", j % 2), CondEq, Query(kJoinedNsName1))
						.OrInnerJoin(fmt::format("joined_field_{}", j), fmt::format("main_field_{}", j % 2), CondEq, Query(kJoinedNsName2)),
				.joinedSelectors = {CacheJoinedSelectorMock{kJoinedNsName1, kUpdateTime1},
									CacheJoinedSelectorMock{kJoinedNsName2, kUpdateTime2}}});
		} else {
			qs.emplace_back(QueryCacheData{
				.q = Query(fmt::format("namespace_{}", i))
						 .InnerJoin(fmt::format("joined_field_{}", j), fmt::format("main_field_{}", j % 2), CondEq, Query(kJoinedNsName1))
						 .InnerJoin(fmt::format("joined_field_{}", j), fmt::format("main_field_{}", j % 2), CondEq, Query(kJoinedNsName2)),
				.joinedSelectors = {CacheJoinedSelectorMock{kJoinedNsName1, kUpdateTime1},
									CacheJoinedSelectorMock{kJoinedNsName2, kUpdateTime2}}});
		}
	}

	QueryCountCache cache(reindexer::kDefaultCacheSizeLimit, reindexer::kDefaultHitCountToCache);

	PRINTF("checking query cache...\n");
	for (i = 0; i < kIterCount; i++) {
		auto idx = rand() % qs.size();
		auto& qce = qs.at(idx);
		QueryCacheKey ckey{qce.q, kCountCachedKeyMode, qce.JoinedSelectorsPtr()};
		auto cached = cache.Get(ckey);
		bool exist = qce.cached;

		if (cached.valid) {
			ASSERT_TRUE(exist) << "query missing in query cache";
			ASSERT_EQ(cached.val.totalCount, qce.expectedTotal) << "cached data are not valid";
		} else {
			size_t total = static_cast<size_t>(rand() % 10000);
			cache.Put(ckey, QueryCountCacheVal{total});
			qce.cached = true;
			qce.expectedTotal = total;
		}
	}
	PRINTF("checking query update time change...\n");
	auto& qce = qs.back();
	if (!qce.cached) {
		QueryCacheKey ckey{qce.q, kCountCachedKeyMode, qce.JoinedSelectorsPtr()};
		auto cached = cache.Get(ckey);
		ASSERT_FALSE(cached.valid) << "query missing in query cache";
		cache.Put(ckey, QueryCountCacheVal{static_cast<size_t>(rand() % 10000)});
	}
	qce.joinedSelectors.back().lastUpdateTime += 100;
	QueryCacheKey ckey{qce.q, kCountCachedKeyMode, qce.JoinedSelectorsPtr()};
	auto cached = cache.Get(ckey);
	ASSERT_FALSE(cached.valid) << "update time change did not affected the key";
}

TEST(LruCache, StressTest) {
	const int nsCount = 10;
	const int iterCount = 10000;
	const int cacheSize = 1024 * 1024;

	std::vector<Query> qs;

	bool gperfEnabled = false;
#if REINDEX_WITH_GPERFTOOLS
	gperfEnabled = reindexer::alloc_ext::TCMallocIsAvailable();
#endif

	allocdebug_init_mt();
	size_t memoryCheckpoint = get_alloc_size();

	QueryCountCache cache(cacheSize, reindexer::kDefaultHitCountToCache);

	PRINTF("preparing queries for caching ...\n");
	for (auto i = 0; i < nsCount; i++) {
		auto idx = std::to_string(i);
		qs.emplace_back(Query("namespace" + idx));
	}

	size_t threadsCount = 8;
	std::vector<std::thread> threads;
	threads.reserve(threadsCount);

	PRINTF("checking query cache ...\n");
	for (size_t i = 0; i < threadsCount; ++i) {
		threads.emplace_back([&]() {
			for (auto i = 0; i < iterCount; i++) {
				auto idx = rand() % qs.size();
				const auto& qce = qs.at(idx);
				QueryCacheKey ckey{qce, kCountCachedKeyMode, static_cast<const CacheJoinedSelectorsMock*>(nullptr)};
				auto cached = cache.Get(ckey);

				if (cached.valid) {
					ASSERT_TRUE(EqQueryCacheKey()(
						QueryCacheKey{qs[idx], kCountCachedKeyMode, static_cast<const CacheJoinedSelectorsMock*>(nullptr)}, ckey))
						<< "queries are not EQUAL!\n";
				} else {
					size_t total = static_cast<size_t>(rand() % 1000);
					cache.Put(ckey, QueryCountCacheVal{total});
				}
			}
		});
	}

	for (size_t i = 0; i < threads.size(); ++i) {
		threads[i].join();
	}

	if (gperfEnabled) {
		size_t memoryConsumed = get_alloc_size() - memoryCheckpoint;
		EXPECT_TRUE(memoryConsumed <= cacheSize);
	}
}
