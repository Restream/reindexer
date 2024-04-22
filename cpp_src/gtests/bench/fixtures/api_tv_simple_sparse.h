#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "base_fixture.h"

class ApiTvSimpleSparse : private BaseFixture {
public:
	virtual ~ApiTvSimpleSparse() {}
	ApiTvSimpleSparse(Reindexer* db, const std::string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("genre", "tree", "int64", IndexOpts().Sparse())
			.AddIndex("year", "tree", "int", IndexOpts().Sparse())
			.AddIndex("packages", "hash", "int", IndexOpts().Array())
			.AddIndex("countries", "tree", "string", IndexOpts().Array())
			.AddIndex("age", "hash", "int", IndexOpts().Sparse())
			.AddIndex("price_id", "hash", "int", IndexOpts().Array())
			.AddIndex("location", "hash", "string", IndexOpts().Sparse())
			.AddIndex("end_time", "hash", "int", IndexOpts().Sparse())
			.AddIndex("start_time", "tree", "int", IndexOpts().Sparse())
			.AddIndex("uuid", "hash", "uuid", IndexOpts()) // can't be sparse
			.AddIndex("uuid_str", "hash", "string", IndexOpts().Sparse())
			.AddIndex("data10", "hash", "int", IndexOpts().Sparse())
			.AddIndex("data33", "hash", "int", IndexOpts().Sparse())
			.AddIndex("data66", "hash", "int", IndexOpts().Sparse())
			.AddIndex("limit", "hash", "int", IndexOpts());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	reindexer::Item MakeItem(benchmark::State&) override;

	void WarmUpIndexes(State& state);

	void GetByRangeIDAndSortByHash(State& state);
	void GetByRangeIDAndSortByTree(State& state);

	void Query1Cond(State& state);
	void Query1CondTotal(State& state);
	void Query1CondCachedTotal(State& state);

	void Query2Cond(State& state);
	void Query2CondTotal(State& state);
	void Query2CondCachedTotal(State& state);

	void Query3Cond(State& state);
	void Query3CondTotal(State& state);
	void Query3CondCachedTotal(State& state);

	void Query4Cond(State& state);
	void Query4CondTotal(State& state);
	void Query4CondCachedTotal(State& state);

	void QueryInnerJoinPreselectByValues(State& state);
	void QueryInnerJoinNoPreselect(State& state);

	void Query4CondIsNULL10(State& state);
	void Query4CondIsNULL33(State& state);
	void Query4CondIsNULL66(State& state);
	void Query4CondNotNULL10(State& state);
	void Query4CondNotNULL33(State& state);
	void Query4CondNotNULL66(State& state);

	void query4CondParameterizable(State& state, std::string_view targetIndexName, bool isNull);

	std::vector<std::string> countries_;
	std::vector<std::string> locations_;
	std::vector<std::string> devices_;
	std::vector<int> start_times_;
	std::vector<std::vector<int>> packages_;
	std::vector<std::vector<int>> priceIDs_;
	std::vector<std::string> uuids_;
	int counter_ = 0;
};
