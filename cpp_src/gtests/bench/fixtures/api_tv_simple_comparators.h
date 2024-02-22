#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "base_fixture.h"

class ApiTvSimpleComparators : private BaseFixture {
public:
	virtual ~ApiTvSimpleComparators() {}
	ApiTvSimpleComparators(Reindexer* db, const std::string& name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("genre", "-", "int64", IndexOpts())
			.AddIndex("year", "-", "int", IndexOpts())
			.AddIndex("packages", "-", "int", IndexOpts().Array())
			.AddIndex("countries", "-", "string", IndexOpts().Array())
			.AddIndex("age", "-", "int", IndexOpts())
			.AddIndex("price_id", "-", "int", IndexOpts().Array())
			.AddIndex("location", "-", "string", IndexOpts())
			.AddIndex("end_time", "-", "int", IndexOpts())
			.AddIndex("start_time", "-", "int", IndexOpts())
			.AddIndex("uuid_str", "-", "string", IndexOpts());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	reindexer::Item MakeItem(benchmark::State&) override;
	reindexer::Item MakeStrItem();

	void WarmUpIndexes(State& state);

	void StringsSelect(State& state);
	void GetEqInt(State& state);
	void GetEqArrayInt(State& state);
	void GetEqString(State& state);
	void GetByRangeIDAndSort(State& state);
	void GetUuidStr(State& state);

	void Query1Cond(State& state);
	void Query1CondTotal(State& state);
	void Query1CondCachedTotal(State& state);
	void Query2Cond(State& state);
	void Query2CondTotal(State& state);
	void Query2CondCachedTotal(State& state);
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

	std::vector<std::string> countries_;
	std::vector<std::string> locations_;
	std::vector<int> start_times_;
	std::vector<std::vector<int>> packages_;
	std::vector<std::vector<int>> priceIDs_;
	std::vector<std::string> uuids_;
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	constexpr static unsigned kTotalItemsStringSelectNs = 100'000;
#else	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	constexpr static unsigned kTotalItemsStringSelectNs = 20'000;
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	reindexer::WrSerializer wrSer_;
	std::string stringSelectNs_{"string_select_ns_comparators"};
};
