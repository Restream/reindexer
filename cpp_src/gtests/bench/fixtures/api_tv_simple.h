#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "base_fixture.h"

class ApiTvSimple : protected BaseFixture {
public:
	virtual ~ApiTvSimple() {}
	ApiTvSimple(Reindexer* db, const std::string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("genre", "tree", "int64", IndexOpts())
			.AddIndex("year", "tree", "int", IndexOpts())
			.AddIndex("packages", "hash", "int", IndexOpts().Array())
			.AddIndex("countries", "tree", "string", IndexOpts().Array())
			.AddIndex("age", "hash", "int", IndexOpts())
			.AddIndex("price_id", "hash", "int", IndexOpts().Array())
			.AddIndex("location", "hash", "string", IndexOpts())
			.AddIndex("end_time", "hash", "int", IndexOpts())
			.AddIndex("start_time", "tree", "int", IndexOpts());
	}

	virtual void RegisterAllCases();
	virtual reindexer::Error Initialize();

protected:
	virtual reindexer::Item MakeItem();
	reindexer::Item MakeStrItem();

protected:
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
	void Query2CondLeftJoin(State& state);
	void Query2CondLeftJoinTotal(State& state);
	void Query2CondLeftJoinCachedTotal(State& state);
	void Query0CondInnerJoinUnlimit(State& state);
	void Query0CondInnerJoinUnlimitLowSelectivity(State& state);
	void Query0CondInnerJoinPreResultStoreValues(State& state);
	void Query2CondInnerJoin(State& state);
	void Query2CondInnerJoinTotal(State& state);
	void Query2CondInnerJoinCachedTotal(State& state);

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

private:
	void query2CondIdSet(State& state, const std::vector<std::vector<int>>& idsets);

	constexpr static unsigned kTotalItemsMainJoinNs = 1000000;

	vector<std::string> countries_;
	vector<std::string> countryLikePatterns_;
	vector<std::string> locations_;
	vector<int> start_times_;
	vector<vector<int>> packages_;
	vector<vector<int>> priceIDs_;
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
	constexpr static unsigned idsetsSz_[] = {10, 100, 500, 2000, 20000};
#else	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
	constexpr static unsigned idsetsSz_[] = {100, 500};
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
	std::unordered_map<unsigned, std::vector<std::vector<int>>> idsets_;
	reindexer::WrSerializer wrSer_;
	std::string stringSelectNs_{"string_select_ns"};
	std::string innerJoinLowSelectivityMainNs_{"inner_join_low_selectivity_main_ns"};
	std::string innerJoinLowSelectivityRightNs_{"inner_join_low_selectivity_right_ns"};
};
