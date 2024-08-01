#include "api_tv_simple_sparse.h"
#include <thread>
#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"
#include "core/nsselecter/joinedselector.h"
#include "core/reindexer.h"
#include "gtests/tools.h"
#include "tools/string_regexp_functions.h"

#include "helpers.h"

using benchmark::AllocsTracker;
using reindexer::Query;
using reindexer::QueryResults;

constexpr char kJoinNamespace[] = "JoinItems";

void ApiTvSimpleSparse::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	BaseFixture::RegisterAllCases();
	Register("WarmUpIndexes", &ApiTvSimpleSparse::WarmUpIndexes, this)->Iterations(1);	// once!!!

	// ToDo always fail  Register("GetByRangeIDAndSortByHash", &ApiTvSimpleSparse::GetByRangeIDAndSortByHash, this);
	Register("GetByRangeIDAndSortByTree", &ApiTvSimpleSparse::GetByRangeIDAndSortByTree, this);

	Register("Query1Cond", &ApiTvSimpleSparse::Query1Cond, this);
	Register("Query1CondTotal", &ApiTvSimpleSparse::Query1CondTotal, this);
	Register("Query1CondCachedTotal", &ApiTvSimpleSparse::Query1CondCachedTotal, this);

	Register("Query2Cond", &ApiTvSimpleSparse::Query2Cond, this);
	Register("Query2CondTotal", &ApiTvSimpleSparse::Query2CondTotal, this);
	Register("Query2CondCachedTotal", &ApiTvSimpleSparse::Query2CondCachedTotal, this);

	Register("Query3Cond", &ApiTvSimpleSparse::Query3Cond, this);
	Register("Query3CondTotal", &ApiTvSimpleSparse::Query3CondTotal, this);
	Register("Query3CondCachedTotal", &ApiTvSimpleSparse::Query3CondCachedTotal, this);

	Register("Query4Cond", &ApiTvSimpleSparse::Query4Cond, this);
	Register("Query4CondTotal", &ApiTvSimpleSparse::Query4CondTotal, this);
	Register("Query4CondCachedTotal", &ApiTvSimpleSparse::Query4CondCachedTotal, this);

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

reindexer::Error ApiTvSimpleSparse::Initialize() {
	assertrx(db_);
	auto err = db_->AddNamespace(nsdef_);

	if (!err.ok()) {
		return err;
	}

	countries_ = {"Portugal",
				  "Afghanistan",
				  "El Salvador",
				  "Bolivia",
				  "Angola",
				  "Finland",
				  "Wales",
				  "Kosovo",
				  "Poland",
				  "Iraq",
				  "United States of America",
				  "Croatia"};

	uuids_.reserve(1000);
	for (size_t i = 0; i < 1000; ++i) {
		uuids_.emplace_back(randStrUuid());
	}

	devices_ = {"iphone", "android", "smarttv", "stb", "ottstb"};
	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};

	for (int i = 0; i < 10; ++i) {
		packages_.emplace_back(randomNumArray<int>(20, 10000, 10));
	}

	for (int i = 0; i < 20; ++i) {
		priceIDs_.emplace_back(randomNumArray<int>(10, 7000, 50));
	}

	start_times_.resize(20);
	for (int i = 0; i < 20; ++i) {
		start_times_[i] = random<int>(0, 50000);
	}

	return err;
}

reindexer::Item ApiTvSimpleSparse::MakeItem(benchmark::State&) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	// All strings passed to item must be stored in app
	item.Unsafe();

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

	item["data10"] = ((id + 0) % 10) ? Variant(random<int>(0, 5'000)) : Variant();		 // 10%
	item["data33"] = ((id + 1) % 3) ? Variant(random<int>(5'001, 10'000)) : Variant();	 // 33%
	item["data66"] = ((id + 2) % 3) ? Variant() : Variant(random<int>(10'001, 20'000));	 // 66%

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
				state.SkipWithError(err.what().c_str());
			}
		}

		for (size_t i = 0; i < packages_.size() * 2; ++i) {
			QueryResults qres;
			Query q(nsdef_.name);
			q.Where("packages", CondSet, packages_.at(i % packages_.size())).Limit(20).Sort("countries", false);
			err = db_->Select(q, qres);
			if (!err.ok()) {
				state.SkipWithError(err.what().c_str());
			}
		}

		for (size_t i = 0; i < priceIDs_.size() * 3; ++i) {
			QueryResults qres;
			Query q(kJoinNamespace);
			q.Where("id", CondSet, priceIDs_.at(i % priceIDs_.size())).Limit(20);
			err = db_->Select(q, qres);
			if (!err.ok()) {
				state.SkipWithError(err.what().c_str());
			}
		}
	}
}

void ApiTvSimpleSparse::GetByRangeIDAndSortByHash(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		Query q(nsdef_.name);
		q.Limit(20).Where("id", CondRange, {idRange.first, idRange.second}).Sort("age", false);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}

		if (!qres.Count()) {
			state.SkipWithError("Results does not contain any value");
		}
	}
}

void ApiTvSimpleSparse::GetByRangeIDAndSortByTree(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		Query q(nsdef_.name);
		q.Limit(20).Where("id", CondRange, {idRange.first, idRange.second}).Sort("genre", false);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}

		if (!qres.Count()) {
			state.SkipWithError("Results does not contain any value");
		}
	}
}

void ApiTvSimpleSparse::Query1Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Limit(20).Where("year", CondGe, 2020);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query1CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Limit(20).Where("year", CondGe, 2020).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query1CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Limit(20).Where("year", CondGe, 2020).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query2Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Limit(20).Where("age", CondEq, 1).Where("year", CondRange, {2010, 2016});

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query2CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Limit(20).Where("age", CondEq, 1).Where("year", CondRange, {2010, 2016}).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query2CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Limit(20).Where("age", CondEq, 1).Where("year", CondRange, {2010, 2016}).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query3Cond(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, {1, 2, 5})
			.Where("age", CondEq, {1, 2, 3})
			.Where("packages", CondSet, packages_.at(randomIndex));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query3CondTotal(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, {1, 2, 5})
			.Where("age", CondEq, {1, 2, 3})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query3CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, {1, 2, 5})
			.Where("age", CondEq, {1, 2, 3})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query4Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, {1, 2, 5})
			.Where("age", CondEq, {1, 2, 3})
			.Where("year", CondRange, {2000, 2039})
			.Where("packages", CondSet, packages_.at(randomIndex));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query4CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, {1, 2, 5})
			.Where("age", CondEq, {1, 2, 3})
			.Where("year", CondRange, {2000, 2039})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query4CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, {1, 2, 5})
			.Where("age", CondEq, {1, 2, 3})
			.Where("year", CondRange, {2000, 2039})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::QueryInnerJoinPreselectByValues(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join(kJoinNamespace);
		q4join.Where("device", CondSet, {"ottstb", "smarttv", "stb"});

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("limit", CondRange, {counter_ - 256, counter_ - 1})
			.Where("location", CondAny, VariantArray{})
			.LeftJoin("location", "location", CondSet, std::move(q4join));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::QueryInnerJoinNoPreselect(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join(kJoinNamespace);
		q4join.Where("device", CondSet, {"ottstb", "smarttv", "stb"});

		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("year", CondRange, {2010, 2030})
			.Where("location", CondAny, VariantArray{})
			.LeftJoin("location", "location", CondSet, std::move(q4join));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimpleSparse::Query4CondIsNULL10(benchmark::State& state) { query4CondParameterizable(state, "data10", true); }
void ApiTvSimpleSparse::Query4CondIsNULL33(benchmark::State& state) { query4CondParameterizable(state, "data33", true); }
void ApiTvSimpleSparse::Query4CondIsNULL66(benchmark::State& state) { query4CondParameterizable(state, "data66", true); }
void ApiTvSimpleSparse::Query4CondNotNULL10(benchmark::State& state) { query4CondParameterizable(state, "data10", false); }
void ApiTvSimpleSparse::Query4CondNotNULL33(benchmark::State& state) { query4CondParameterizable(state, "data33", false); }
void ApiTvSimpleSparse::Query4CondNotNULL66(benchmark::State& state) { query4CondParameterizable(state, "data66", false); }

void ApiTvSimpleSparse::query4CondParameterizable(benchmark::State& state, std::string_view targetIndexName, bool isNull) {
	const CondType cond = isNull ? CondEmpty : CondAny;

	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("age", CondSet, {1, 2})
			.Where("year", CondRange, {2010, 2020})
			.Where(targetIndexName, cond, VariantArray{});

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what().c_str());
		}
	}
}
