#pragma once

#include <string>
#include <vector>

#include "allocs_tracker.h"
#include "base_fixture.h"

using std::string;
using std::vector;

using benchmark::AllocsTracker;

class ApiTvComposite : protected BaseFixture {
public:
	ApiTvComposite(Reindexer* db, const string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		AddIndex("id", "id", "hash", "int", IndexOpts().PK());
		AddIndex("sub_id", "sub_id", "tree", "string", IndexOpts().SetCollateMode(CollateNumeric).PK());
		AddIndex("name", "name", "hash", "string", IndexOpts().SetCollateMode(CollateUTF8));
		AddIndex("year", "year", "tree", "int", IndexOpts());
		AddIndex("genre", "genre", "hash", "string", IndexOpts().SetCollateMode(CollateNumeric));
		AddIndex("age", "age", "hash", "int", IndexOpts());
		AddIndex("rate", "rate", "tree", "double", IndexOpts());
		AddIndex("location", "location", "hash", "string", IndexOpts().SetCollateMode(CollateASCII));
		AddIndex("start_time", "start_time", "tree", "int64", IndexOpts());
		AddIndex("end_time", "end_time", "tree", "int64", IndexOpts());
		AddIndex("tmp", "tmp", "-", "string", IndexOpts());

		AddIndex("id+sub_id", "", "hash", "composite", IndexOpts());  // IntInt Tree
		AddIndex("id+year", "", "tree", "composite", IndexOpts());	// IntInt Tree
		AddIndex("id+name", "", "tree", "composite", IndexOpts());	// IntStr Tree

		AddIndex("id+start_time", "", "hash", "composite", IndexOpts());  // IntInt Hash
		AddIndex("id+genre", "", "hash", "composite", IndexOpts());		  // IntStr Hash

		//		AddIndex("sub_id+name", "", "tree", "composite", IndexOpts());
		//		AddIndex("genre+rate", "", "tree", "composite", IndexOpts());  // collate numeric and double
		//		AddIndex("year+rate", "", "tree", "composite", IndexOpts());   // tree int and double
	}

	Error Initialize();
	Item MakeItem();
	void RegisterAllCases();

protected:
	void Insert(State& state);
	void WarmUpIndexes(State& state);
	void GetByCompositePK(State& state);

	// Part I
	void RangeTreeInt(State& state);
	void RangeTreeStrCollateNumeric(State& state);
	void RangeTreeDouble(State& state);
	void RangeTreeCompositeIntInt(State& state);
	void RangeTreeCompositeIntStr(State& state);

	// Part II
	void RangeHashInt(State& state);
	void RangeHashStringCollateASCII(State& state);
	void RangeHashStringCollateUTF8(State& state);
	void RangeHashCompositeIntInt(State& state);
	void RangeHashCompositeIntStr(State& state);

	// Part III
	void RangeTreeIntSortByHashInt(State& state);
	void RangeTreeIntSortByTreeInt(State& state);
	void RangeTreeStrSortByHashInt(State& state);
	void RangeTreeStrSortByTreeInt(State& state);
	void RangeTreeDoubleSortByTreeInt(State& state);
	void RangeTreeDoubleSortByHashInt(State& state);
	void RangeTreeStrSortByHashStrCollateASCII(State& state);
	void RangeTreeStrSortByHashStrCollateUTF8(State& state);

	// Part IV
	void SortByHashInt(State& state);
	void SortByHashStrCollateASCII(State& state);
	void SortByHashStrCollateUTF8(State& state);
	void SortByHashCompositeIntInt(State& state);
	void SortByHashCompositeIntStr(State& state);
	void SortByTreeCompositeIntInt(State& state);
	void SortByTreeCompositeIntStrCollateUTF8(State& state);

private:
	vector<string> locations_;
	vector<string> names_;
};
