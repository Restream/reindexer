#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "core/query/query.h"
#include "core/querycache.h"
#include "debug/allocdebug.h"
#include "gtests/tests/gtest_cout.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/serializer.h"

using reindexer::Query;
using reindexer::WrSerializer;
using reindexer::Serializer;
using reindexer::QueryTotalCountCache;
using reindexer::QueryCacheKey;
using reindexer::QueryTotalCountCacheVal;
using reindexer::EqQueryCacheKey;

TEST(LruCache, SimpleTest) {
	const int nsCount = 10;
	const int iterCount = 1000;

	typedef std::pair<Query, bool> QueryCachePair;

	std::vector<QueryCachePair> qs;

	PRINTF("preparing queries for caching ...\n");
	for (auto i = 0; i < nsCount; i++) {
		auto idx = std::to_string(i);
		qs.emplace_back(Query("namespace" + idx), false);
	}

	QueryTotalCountCache cache;
	auto keyComparator = EqQueryCacheKey();

	PRINTF("checking query cache ...\n");
	for (auto i = 0; i < iterCount; i++) {
		auto idx = rand() % qs.size();
		auto const& qce = qs.at(idx);
		QueryCacheKey ckey{qce.first};
		auto cached = cache.Get(ckey);
		bool exist = qce.second;

		if (cached.valid) {
			ASSERT_TRUE(exist) << "query missing in query cache!\n";
			QueryCacheKey k(qs[idx].first);
			ASSERT_TRUE(keyComparator(k, ckey)) << "queries are not EQUAL!\n";
		} else {
			size_t total = static_cast<size_t>(rand() % 1000);
			cache.Put(ckey, QueryTotalCountCacheVal{total});
			qs[idx].second = true;
		}
	}
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

	QueryTotalCountCache cache(cacheSize);

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
				auto const& qce = qs.at(idx);
				QueryCacheKey ckey{qce};
				auto cached = cache.Get(ckey);

				if (cached.valid) {
					ASSERT_TRUE(EqQueryCacheKey()(qs[idx], ckey)) << "queries are not EQUAL!\n";
				} else {
					size_t total = static_cast<size_t>(rand() % 1000);
					cache.Put(ckey, QueryTotalCountCacheVal{total});
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
