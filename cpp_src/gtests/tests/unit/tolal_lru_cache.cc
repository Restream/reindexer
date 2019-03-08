#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "core/query/query.h"
#include "core/query/querycache.h"
#include "debug/allocdebug.h"
#include "gtests/tests/gtest_cout.h"
#include "tools/serializer.h"

using std::vector;
using reindexer::Query;
using reindexer::WrSerializer;
using reindexer::Serializer;
using reindexer::QueryCache;
using reindexer::QueryCacheKey;
using reindexer::QueryCacheVal;
using reindexer::EqQueryCacheKey;

TEST(LruCache, SimpleTest) {
	const int nsCount = 10;
	const int iterCount = 1000;

	typedef std::pair<Query, bool> QueryCachePair;

	vector<QueryCachePair> qs;

	PRINTF("preparing queries for caching ...\n");
	for (auto i = 0; i < nsCount; i++) {
		auto idx = std::to_string(i);
		qs.emplace_back(Query("namespace" + idx), false);
	}

	QueryCache cache;
	auto keyComparator = EqQueryCacheKey();

	PRINTF("checking query cache ...\n");
	for (auto i = 0; i < iterCount; i++) {
		auto idx = rand() % qs.size();
		auto const& qce = qs.at(idx);
		auto cached = cache.Get({qce.first});
		bool exist = qce.second;

		if (cached.key) {
			ASSERT_TRUE(exist) << "query missing in query cache!\n";
			QueryCacheKey k(qs[idx].first);
			ASSERT_TRUE(keyComparator(k, *cached.key)) << "queries are not EQUAL!\n";
		} else {
			size_t total = static_cast<size_t>(rand() % 1000);
			cache.Put({qce.first}, QueryCacheVal{total});
			qs[idx].second = true;
		}
	}
}

TEST(LruCache, StressTest) {
	const int nsCount = 10;
	const int iterCount = 10000;
	const int cacheSize = 1024 * 1024;

	vector<Query> qs;

	bool gperfEnabled = false;
#ifdef REINDEX_WITH_GPERFTOOLS
	gperfEnabled = true;
#else
	gperfEnabled = false;
#endif

	allocdebug_init_mt();
	size_t memoryCheckpoint = get_alloc_size();

	QueryCache cache(cacheSize);

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
				auto cached = cache.Get({qce});

				if (cached.key) {
					Query k(qs[idx]);
					ASSERT_TRUE(EqQueryCacheKey()(k, *cached.key)) << "queries are not EQUAL!\n";
				} else {
					size_t total = static_cast<size_t>(rand() % 1000);
					cache.Put({qce}, QueryCacheVal{total});
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
