#include "api_tv_simple_sparse.h"
#include "allocs_tracker.h"
#include "helpers.h"

using benchmark::AllocsTracker;
using reindexer::Query;
using reindexer::QueryResults;

constexpr char kJoinNamespace[] = "JoinItems";

void ApiTvSimpleSparse::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	BaseFixture::RegisterAllCases();
	Register("WarmUpIndexes", &ApiTvSimpleSparse::WarmUpIndexes, this)->Iterations(1);	// once!!!

	Base::RegisterAllCases();
	// ToDo always fail  Register("GetByRangeIDAndSortByHash", &ApiTvSimpleSparse::GetByRangeIDAndSortByHash, BasePtr());
	Register("GetByRangeIDAndSortByTree", &ApiTvSimpleSparse::GetByRangeIDAndSortByTree, BasePtr());

	Register("Query2Cond", &ApiTvSimpleSparse::Query2Cond<NoTotal>, this);
	Register("Query2CondTotal", &ApiTvSimpleSparse::Query2Cond<ReqTotal>, this);
	Register("Query2CondCachedTotal", &ApiTvSimpleSparse::Query2Cond<CachedTotal>, this);

	Register("Query3Cond", &ApiTvSimpleSparse::Query3Cond<NoTotal>, this);
	Register("Query3CondTotal", &ApiTvSimpleSparse::Query3Cond<ReqTotal>, this);
	Register("Query3CondCachedTotal", &ApiTvSimpleSparse::Query3Cond<CachedTotal>, this);

	Register("Query4Cond", &ApiTvSimpleSparse::Query4Cond<NoTotal>, this);
	Register("Query4CondTotal", &ApiTvSimpleSparse::Query4Cond<ReqTotal>, this);
	Register("Query4CondCachedTotal", &ApiTvSimpleSparse::Query4Cond<CachedTotal>, this);

	Register("QueryJoinByValues", &ApiTvSimpleSparse::QueryInnerJoinPreselectByValues, this);
	Register("QueryInnerJoinNoPreselect", &ApiTvSimpleSparse::QueryInnerJoinNoPreselect, this);

	Register("Query4CondIsNULL10", &ApiTvSimpleSparse::Query4CondIsNULL10, this);
	Register("Query4CondIsNULL33", &ApiTvSimpleSparse::Query4CondIsNULL33, this);
	Register("Query4CondIsNULL66", &ApiTvSimpleSparse::Query4CondIsNULL66, this);
	Register("Query4CondNotNULL10", &ApiTvSimpleSparse::Query4CondNotNULL10, this);
	Register("Query4CondNotNULL33", &ApiTvSimpleSparse::Query4CondNotNULL33, this);
	Register("Query4CondNotNULL66", &ApiTvSimpleSparse::Query4CondNotNULL66, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Item ApiTvSimpleSparse::MakeItem(benchmark::State&) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	// All strings passed to item must be stored in app
	std::ignore = item.Unsafe();

	auto startTime = random<int>(0, 50000);

	// nullable: 10 - 10%, 5 - 20%, 4 - 25%, 3 - 33%, 2 - 50%
	auto id = id_seq_->Next();
	item["id"] = id;
	item["genre"] = ((id + 3) % 5) ? Variant(random<int64_t>(0, 49)) : Variant();  // 20%
	item["year"] = ((id + 1) % 4) ? Variant(random<int>(2000, 2049)) : Variant();  // 25%
	item["packages"] = packages_.at(random<size_t>(0, packages_.size() - 1));
	item["countries"] = countries_.at(random<size_t>(0, countries_.size() - 1));
	item["age"] = ((id + 0) % 5) ? Variant(random<int>(0, 4)) : Variant();	// 20%
	item["price_id"] = priceIDs_.at(random<size_t>(0, priceIDs_.size() - 1));
	item["location"] = ((id + 2) % 4) ? Variant(locations_.at(random<size_t>(0, locations_.size() - 1))) : Variant();		 // 25%
	item["start_time"] = ((id + 4) % 5) ? Variant(start_times_.at(random<size_t>(0, start_times_.size() - 1))) : Variant();	 // 20%
	item["end_time"] = ((id + 5) % 4) ? Variant(startTime + random<int>(1, 5) * 1000) : Variant();							 // 25%
	item["uuid"] = reindexer::Uuid{uuids_[rand() % uuids_.size()]};
	item["uuid_str"] = ((id + 6) % 5) ? Variant(uuids_[rand() % uuids_.size()]) : Variant();  // 20%

	item["data10"] = ((id + 1) % 10) ? Variant(random<int>(0, 5'000)) : Variant();		 // 10%
	item["data33"] = ((id + 2) % 3) ? Variant(random<int>(5'001, 10'000)) : Variant();	 // 33%
	item["data66"] = ((id + 0) % 3) ? Variant() : Variant(random<int>(10'001, 20'000));	 // 66%

	item["limit"] = counter_++;

	return item;
}

// FIXTURES

void ApiTvSimpleSparse::WarmUpIndexes(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		reindexer::Error err;

		// Ensure indexes complete build
		WaitForOptimization();
		for (size_t i = 0; i < packages_.size() * 2; ++i) {
			QueryResults qres;
			Query q(nsdef_.name);
			q.Where("packages", CondSet, packages_.at(i % packages_.size())).Limit(20).Sort("uuid", false);
			err = db_->Select(q, qres);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
			checkNotEmpty(qres, state);
		}

		for (size_t i = 0; i < packages_.size() * 2; ++i) {
			QueryResults qres;
			Query q(nsdef_.name);
			q.Where("packages", CondSet, packages_.at(i % packages_.size())).Limit(20).Sort("countries", false);
			err = db_->Select(q, qres);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
			checkNotEmpty(qres, state);
		}

		for (size_t i = 0; i < priceIDs_.size() * 3; ++i) {
			QueryResults qres;
			Query q(kJoinNamespace);
			q.Where("id", CondSet, priceIDs_.at(i % priceIDs_.size())).Limit(20);
			err = db_->Select(q, qres);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
			checkNotEmpty(qres, state);
		}
	}
}

template <typename Total>
void ApiTvSimpleSparse::Query2Cond(benchmark::State& state) {
	auto q = Query(nsdef_.name).Where("age", CondEq, 1).Where("year", CondRange, {2010, 2016}).Limit(20);
	Total::Apply(q);
	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimpleSparse::Query3Cond(benchmark::State& state) {
	const auto q = [&] {
		const size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		auto q = Query(nsdef_.name)
					 .Where("genre", CondEq, {1, 2, 5})
					 .Where("age", CondEq, {1, 2, 3})
					 .Where("packages", CondSet, packages_.at(randomIndex))
					 .Sort("year", false)
					 .Limit(20);
		Total::Apply(q);
		return q;
	};
	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimpleSparse::Query4Cond(benchmark::State& state) {
	const auto q = [&] {
		const size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		auto q = Query(nsdef_.name)
					 .Where("genre", CondEq, {1, 2, 5})
					 .Where("age", CondEq, {1, 2, 3})
					 .Where("year", CondRange, {2000, 2039})
					 .Where("packages", CondSet, packages_.at(randomIndex))
					 .Sort("year", false)
					 .Limit(20);
		Total::Apply(q);
		return q;
	};
	benchQuery(q, state);
}

void ApiTvSimpleSparse::QueryInnerJoinPreselectByValues(benchmark::State& state) {
	auto q4join = Query(kJoinNamespace).Where("device", CondSet, {"ottstb", "smarttv", "stb"});
	const auto q = Query(nsdef_.name)
					   .Where("limit", CondRange, {counter_ - 256, counter_ - 1})
					   .LeftJoin("location", "location", CondSet, std::move(q4join))
					   .Sort("year", false)
					   .Limit(20);
	benchQuery(q, state);
}

void ApiTvSimpleSparse::QueryInnerJoinNoPreselect(benchmark::State& state) {
	auto q4join = Query(kJoinNamespace).Where("device", CondSet, {"ottstb", "smarttv", "stb"});
	const auto q = Query(nsdef_.name)
					   .Where("year", CondRange, {2010, 2030})
					   .LeftJoin("location", "location", CondSet, std::move(q4join))
					   .Sort("year", false)
					   .Limit(20);
	benchQuery(q, state);
}

void ApiTvSimpleSparse::Query4CondIsNULL10(benchmark::State& state) { query4CondParameterizable(state, "data10", true); }
void ApiTvSimpleSparse::Query4CondIsNULL33(benchmark::State& state) { query4CondParameterizable(state, "data33", true); }
void ApiTvSimpleSparse::Query4CondIsNULL66(benchmark::State& state) { query4CondParameterizable(state, "data66", true); }
void ApiTvSimpleSparse::Query4CondNotNULL10(benchmark::State& state) { query4CondParameterizable(state, "data10", false); }
void ApiTvSimpleSparse::Query4CondNotNULL33(benchmark::State& state) { query4CondParameterizable(state, "data33", false); }
void ApiTvSimpleSparse::Query4CondNotNULL66(benchmark::State& state) { query4CondParameterizable(state, "data66", false); }

void ApiTvSimpleSparse::query4CondParameterizable(benchmark::State& state, std::string_view targetIndexName, bool isNull) {
	const CondType cond = isNull ? CondEmpty : CondAny;

	const auto q = Query(nsdef_.name)
					   .Where("genre", CondEq, 5)
					   .Where("age", CondSet, {1, 2})
					   .Where("year", CondRange, {2010, 2020})
					   .Where(targetIndexName, cond, VariantArray{})
					   .Sort("year", false)
					   .Limit(20);
	benchQuery(q, state);
}
