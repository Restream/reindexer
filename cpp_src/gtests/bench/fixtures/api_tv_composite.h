#pragma once

#include <string>
#include <vector>
#include "base_fixture.h"

class [[nodiscard]] ApiTvComposite : private BaseFixture {
public:
	ApiTvComposite(Reindexer* db, std::string_view name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts())
			.AddIndex("sub_id", "tree", "string", IndexOpts().SetCollateMode(CollateNumeric))
			.AddIndex("name", "hash", "string", IndexOpts().SetCollateMode(CollateUTF8))
			.AddIndex("year", "tree", "int", IndexOpts())
			.AddIndex("genre", "hash", "string", IndexOpts().SetCollateMode(CollateNumeric))
			.AddIndex("age", "hash", "int", IndexOpts())
			.AddIndex("rate", "tree", "double", IndexOpts())
			.AddIndex("location", "hash", "string", IndexOpts().SetCollateMode(CollateASCII))
			.AddIndex("start_time", "tree", "int64", IndexOpts())
			.AddIndex("end_time", "tree", "int64", IndexOpts())
			.AddIndex("tmp", "-", "string", IndexOpts())
			.AddIndex("id+sub_id", {"id", "sub_id"}, "hash", "composite", IndexOpts().PK())
			.AddIndex("id+year", {"id", "year"}, "tree", "composite", IndexOpts())
			.AddIndex("id+name", {"id", "name"}, "tree", "composite", IndexOpts())
			.AddIndex("id+start_time", {"id", "start_time"}, "hash", "composite", IndexOpts())
			.AddIndex("id+genre", {"id", "genre"}, "hash", "composite", IndexOpts())
			.AddIndex("field1", "hash", "int", IndexOpts())
			.AddIndex("field2", "hash", "int", IndexOpts())
			.AddIndex("field1+field2", {"field1", "field2"}, "hash", "composite", IndexOpts());

		//		AddIndex("sub_id+name", "", "tree", "composite", IndexOpts());
		//		AddIndex("genre+rate", "", "tree", "composite", IndexOpts());  // collate numeric and double
		//		AddIndex("year+rate", "", "tree", "composite", IndexOpts());   // tree int and double
	}

	reindexer::Error Initialize() override;
	reindexer::Item MakeItem(benchmark::State&) override;
	void RegisterAllCases();

private:
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
	void ForcedSortByHashInt(State& state);
	void ForcedSortWithSecondCondition(State& state);
	void Query2CondIdSetComposite(State& state);

	std::vector<VariantArray> compositeIdSet_;
	std::vector<std::string> locations_;
	std::vector<std::string> names_;
};
