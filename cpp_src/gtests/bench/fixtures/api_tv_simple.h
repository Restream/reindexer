#pragma once

#include <string>
#include <vector>

#include "base_fixture.h"

using std::vector;
using std::string;

class ApiTvSimple : protected BaseFixture {
public:
	virtual ~ApiTvSimple() {}
	ApiTvSimple(Reindexer* db, const string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
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
	virtual Error Initialize();

protected:
	virtual Item MakeItem();

protected:
	void WarmUpIndexes(State& state);

	void GetByID(State& state);
	void GetEqInt(State& state);
	void GetEqArrayInt(State& state);
	void GetEqString(State& state);
	void GetLikeString(State& state);
	void GetByRangeIDAndSortByHash(State& state);
	void GetByRangeIDAndSortByTree(State& state);

	void Query1Cond(State& state);
	void Query1CondTotal(State& state);
	void Query1CondCachedTotal(State& state);

	void Query2Cond(State& state);
	void Query2CondTotal(State& state);
	void Query2CondCachedTotal(State& state);
	void Query2CondLeftJoin(State& state);
	void Query2CondLeftJoinTotal(State& state);
	void Query2CondLeftJoinCachedTotal(State& state);
	void Query0CondInnerJoinUnlimit(State& state);
	void Query0CondInnerJoinPreResultStoreValues(benchmark::State& state);
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
	vector<string> countries_;
	vector<string> countryLikePatterns_;
	vector<string> locations_;
	vector<int> start_times_;
	vector<vector<int>> packages_;
	vector<vector<int>> priceIDs_;
	reindexer::WrSerializer wrSer_;
};
