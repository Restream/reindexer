#pragma once

#include <string>
#include "api_tv_simple_base.h"

using namespace std::string_view_literals;

class [[nodiscard]] ApiTvSimpleSparse : private ApiTvSimpleBase {
	using Base = ApiTvSimpleBase;

public:
	~ApiTvSimpleSparse() override = default;
	ApiTvSimpleSparse(Reindexer* db, std::string_view name, size_t maxItems) : Base(db, name, maxItems, ""sv) {
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
			.AddIndex("uuid", "hash", "uuid", IndexOpts())	// can't be sparse
			.AddIndex("uuid_str", "hash", "string", IndexOpts().Sparse())
			.AddIndex("data10", "hash", "int", IndexOpts().Sparse())
			.AddIndex("data33", "hash", "int", IndexOpts().Sparse())
			.AddIndex("data66", "hash", "int", IndexOpts().Sparse())
			.AddIndex("limit", "hash", "int", IndexOpts());
	}

	void RegisterAllCases();
	using Base::Initialize;

private:
	reindexer::Item MakeItem(benchmark::State&) override;

	void WarmUpIndexes(State& state);

	template <typename Total>
	void Query2Cond(State& state);
	template <typename Total>
	void Query3Cond(State& state);
	template <typename Total>
	void Query4Cond(State& state);

	void QueryInnerJoinPreselectByValues(State& state);
	void QueryInnerJoinNoPreselect(State& state);

	void Query4CondIsNULL10(State& state);
	void Query4CondIsNULL33(State& state);
	void Query4CondIsNULL66(State& state);
	void Query4CondNotNULL10(State& state);
	void Query4CondNotNULL33(State& state);
	void Query4CondNotNULL66(State& state);

	void query4CondParameterizable(State& state, std::string_view targetIndexName, bool isNull);

	int counter_ = 0;
};
