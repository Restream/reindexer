#pragma once

#include <string>
#include "base_fixture.h"

class [[nodiscard]] EqualPositions : protected BaseFixture {
public:
	~EqualPositions() override = default;
	EqualPositions(Reindexer* db, std::string_view name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
		nsdef_.AddIndex("int_array_index1", "hash", "int", IndexOpts().Array());
		nsdef_.AddIndex("int_array_index2", "hash", "int", IndexOpts().Array());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	class FacetNotEmptyChecker;

	reindexer::Item MakeItem(benchmark::State&) override;

	void Insert(State& state);
	void EqPos2Index(State& state);
	void EqPos2(State& state);
	void EqPos2Grouping(State& state);
	void EqPos4(State& state);
	void EqPos2GroupingSubArray(State& state);

	reindexer::WrSerializer wrSer_;
	int id_ = 0;
	static const unsigned kRowCount = 1000;
	static const unsigned kArray1Count = 10;
	static const unsigned kArray1Start = 0;
	static const unsigned kArray1End = 10;

	static const unsigned kArray2Count = 20;
	static const unsigned kArray2Start = 10;
	static const unsigned kArray2End = 30;
};
