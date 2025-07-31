#pragma once

#include <unordered_map>

#include "base_fixture.h"
#include "core/system_ns_names.h"

class ApiTvSimple : private BaseFixture {
public:
	virtual ~ApiTvSimple() {}
	ApiTvSimple(Reindexer* db, std::string_view name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("genre", "tree", "int64", IndexOpts())
			.AddIndex("year", "tree", "int", IndexOpts())
			.AddIndex("packages", "hash", "int", IndexOpts().Array())
			.AddIndex("countries", "tree", "string", IndexOpts().Array())
			.AddIndex("age", "hash", "int", IndexOpts())
			.AddIndex("price_id", "hash", "int", IndexOpts().Array())
			.AddIndex("location", "hash", "string", IndexOpts())
			.AddIndex("end_time", "hash", "int", IndexOpts())
			.AddIndex("start_time", "tree", "int", IndexOpts())
			.AddIndex("uuid", "hash", "uuid", IndexOpts())
			.AddIndex("uuid_str", "hash", "string", IndexOpts());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	class IndexCacheSetter {
	public:
		constexpr static unsigned kVeryLargeHitsValue = 1000000;

		IndexCacheSetter(reindexer::Reindexer& db, unsigned hitsCount = kVeryLargeHitsValue) : db_(db) {
			shrinkCache();
			setHitsCount(hitsCount);
		}
		~IndexCacheSetter() { setHitsCount(kDefaultCacheHits); }

	private:
		constexpr static int64_t kDefaultCacheSize = 134217728;
		constexpr static int64_t kDefaultCacheHits = 2;

		void shrinkCache() {
			// Shrink cache size to force cache invalidation
			auto q = reindexer::Query(reindexer::kConfigNamespace)
						 .Set("namespaces.cache.index_idset_cache_size", 1024)
						 .Where("type", CondEq, "namespaces");
			reindexer::QueryResults qr;
			auto err = db_.Update(q, qr);
			assertrx(err.ok());
			assertrx(qr.Count() == 1);
		}
		void setHitsCount(unsigned hitsCount) {
			// Set required hits count and default cache size
			auto q = reindexer::Query(reindexer::kConfigNamespace)
						 .Set("namespaces.cache.index_idset_cache_size", kDefaultCacheSize)
						 .Set("namespaces.cache.index_idset_hits_to_cache", int64_t(hitsCount))
						 .Where("type", CondEq, "namespaces");
			reindexer::QueryResults qr;
			auto err = db_.Update(q, qr);
			assertrx(err.ok());
			assertrx(qr.Count() == 1);
		}

		reindexer::Reindexer& db_;
	};

	reindexer::Item MakeItem(benchmark::State&) override;
	reindexer::Item MakeStrItem();

	void WarmUpIndexes(State& state);

	void StringsSelect(State& state);
	void GetByID(State& state);
	void GetByIDInBrackets(State& state);
	void GetEqInt(State& state);
	void GetEqArrayInt(State& state);
	void GetEqString(State& state);
	void GetLikeString(State& state);
	void GetByRangeIDAndSortByHash(State& state);
	void GetByRangeIDAndSortByTree(State& state);
	void GetUuid(State&);
	void GetUuidStr(State&);

	void Query1Cond(State& state);
	void Query1CondTotal(State& state);
	void Query1CondCachedTotal(State& state);
	void Query2CondIdSet10(State& state);
	void Query2CondIdSet100(State& state);
	void Query2CondIdSet500(State& state);
	void Query2CondIdSet2000(State& state);
	void Query2CondIdSet20000(State& state);

	void Query2Cond(State& state);
	void Query2CondTotal(State& state);
	void Query2CondCachedTotal(State& state);
	void Query2CondLeftJoin2Cond(State& state);
	void Query2CondLeftJoin2CondTotal(State& state);
	void Query2CondLeftJoin2CondCachedTotal(State& state);
	void Query2CondLeftJoin3Cond(State& state);
	void Query2CondLeftJoin3CondTotal(State& state);
	void Query2CondLeftJoin3CondCachedTotal(State& state);
	void Query0CondInnerJoinUnlimit(State& state);
	void Query0CondInnerJoinUnlimitLowSelectivity(State& state);
	void Query0CondInnerJoinPreResultStoreValues(State& state);
	void Query2CondInnerJoin2Cond(State& state);
	void Query2CondInnerJoin2CondTotal(State& state);
	void Query2CondInnerJoin2CondCachedTotal(State& state);
	void Query2CondInnerJoin3Cond(State& state);
	void Query2CondInnerJoin3CondTotal(State& state);
	void Query2CondInnerJoin3CondCachedTotal(State& state);
	void InnerJoinInjectConditionFromMain(benchmark::State&);
	void InnerJoinRejectInjection(benchmark::State&);

	void Query3Cond(State& state);
	void Query3CondTotal(State& state);
	void Query3CondCachedTotal(State& state);
	void Query3CondKillIdsCache(State& state);
	void Query3CondRestoreIdsCache(State& state);

	void Query4Cond(State& state);
	void Query4CondTotal(State& state);
	void Query4CondCachedTotal(State& state);
	void Query4CondRange(State& state);
	void Query4CondRangeTotal(State& state);
	void Query4CondRangeCachedTotal(State& state);
	void Query4CondRangeDropCache(State& state);
	void Query4CondRangeDropCacheTotal(State& state);
	void Query4CondRangeDropCacheCachedTotal(State& state);
	void SubQueryEq(State&);
	void SubQuerySet(State&);
	void SubQueryAggregate(State&);

	void query2CondIdSet(State& state, const std::vector<std::vector<int>>& idsets);

	std::vector<std::string> countries_;
	std::vector<std::string> countryLikePatterns_;
	std::vector<std::string> locations_;
	std::vector<std::string> devices_;
	std::vector<int> start_times_;
	std::vector<std::vector<int>> packages_;
	std::vector<std::vector<int>> priceIDs_;
	std::vector<std::string> uuids_;
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	constexpr static unsigned kTotalItemsStringSelectNs = 100'000;
	constexpr static unsigned kTotalItemsMainJoinNs = 1'000'000;
	constexpr static unsigned idsetsSz_[] = {10, 100, 500, 2000, 20000};
#else	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	constexpr static unsigned kTotalItemsStringSelectNs = 20'000;
	constexpr static unsigned kTotalItemsMainJoinNs = 50'000;
	constexpr static unsigned idsetsSz_[] = {100, 500};
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	std::unordered_map<unsigned, std::vector<std::vector<int>>> idsets_;
	reindexer::WrSerializer wrSer_;
	std::string stringSelectNs_{"string_select_ns"};
	std::string mainNs_{"main_ns"};
	std::string rightNs_{"right_ns"};
};
