#pragma once

#include <string>
#include "api_tv_simple_base.h"

using namespace std::string_view_literals;

class [[nodiscard]] ApiTvSimpleComparators : private ApiTvSimpleBase {
	using Base = ApiTvSimpleBase;

public:
	~ApiTvSimpleComparators() override = default;
	ApiTvSimpleComparators(Reindexer* db, std::string_view name, size_t maxItems)
		: Base(db, name, maxItems, "string_select_ns_comparators"sv) {
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

	void GetByRangeIDAndSort(State& state);

	void QueryDistinctOneField(State& state);
	void QueryDistinctTwoField(State& state);
	void QueryDistinctTwoFieldArray(State& state);

	void QueryDistinctOneFieldLimit(State& state);
	void QueryDistinctTwoFieldLimit(State& state);
	void QueryDistinctTwoFieldArrayLimit(State& state);

#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	constexpr static unsigned kTotalItemsStringSelectNs = 100'000;
#else	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	constexpr static unsigned kTotalItemsStringSelectNs = 20'000;
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	reindexer::WrSerializer wrSer_;
};
