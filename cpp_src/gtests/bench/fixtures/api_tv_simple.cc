#include "api_tv_simple.h"
#include <thread>
#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"
#include "core/reindexer.h"
#include "tools/string_regexp_functions.h"

#include "helpers.h"

using std::bind;
using std::placeholders::_1;

using benchmark::RegisterBenchmark;
using benchmark::AllocsTracker;

using reindexer::Query;
using reindexer::QueryResults;

void ApiTvSimple::RegisterAllCases() {
	BaseFixture::RegisterAllCases();
	Register("WarmUpIndexes", &ApiTvSimple::WarmUpIndexes, this)->Iterations(1);  // Just 1 time!!!

	Register("GetByID", &ApiTvSimple::GetByID, this);
	Register("GetEqInt", &ApiTvSimple::GetEqInt, this);
	Register("GetEqArrayInt", &ApiTvSimple::GetEqArrayInt, this);
	Register("GetEqString", &ApiTvSimple::GetEqString, this);
	Register("GetLikeString", &ApiTvSimple::GetLikeString, this);
	Register("GetByRangeIDAndSortByHash", &ApiTvSimple::GetByRangeIDAndSortByHash, this);
	Register("GetByRangeIDAndSortByTree", &ApiTvSimple::GetByRangeIDAndSortByTree, this);

	Register("Query1Cond", &ApiTvSimple::Query1Cond, this);
	Register("Query1CondTotal", &ApiTvSimple::Query1CondTotal, this);
	Register("Query1CondCachedTotal", &ApiTvSimple::Query1CondCachedTotal, this);
	Register("Query2Cond", &ApiTvSimple::Query2Cond, this);
	Register("Query2CondTotal", &ApiTvSimple::Query2CondTotal, this);
	Register("Query2CondCachedTotal", &ApiTvSimple::Query2CondCachedTotal, this);
	Register("Query2CondLeftJoin", &ApiTvSimple::Query2CondLeftJoin, this);
	Register("Query2CondLeftJoinTotal", &ApiTvSimple::Query2CondLeftJoinTotal, this);
	Register("Query2CondLeftJoinCachedTotal", &ApiTvSimple::Query2CondLeftJoinCachedTotal, this);
	Register("Query0CondInnerJoinUnlimit", &ApiTvSimple::Query0CondInnerJoinUnlimit, this);
	Register("Query2CondInnerJoin", &ApiTvSimple::Query2CondInnerJoin, this);
	Register("Query2CondInnerJoinTotal", &ApiTvSimple::Query2CondInnerJoinTotal, this);
	Register("Query2CondInnerJoinCachedTotal", &ApiTvSimple::Query2CondInnerJoinCachedTotal, this);
	Register("Query3Cond", &ApiTvSimple::Query3Cond, this);
	Register("Query3CondTotal", &ApiTvSimple::Query3CondTotal, this);
	Register("Query3CondCachedTotal", &ApiTvSimple::Query3CondCachedTotal, this);
	Register("Query3CondKillIdsCache", &ApiTvSimple::Query3CondKillIdsCache, this);
	Register("Query3CondRestoreIdsCache", &ApiTvSimple::Query3CondRestoreIdsCache, this);
	Register("Query4Cond", &ApiTvSimple::Query4Cond, this);
	Register("Query4CondTotal", &ApiTvSimple::Query4CondTotal, this);
	Register("Query4CondCachedTotal", &ApiTvSimple::Query4CondCachedTotal, this);
	Register("Query4CondRange", &ApiTvSimple::Query4CondRange, this);
	Register("Query4CondRangeTotal", &ApiTvSimple::Query4CondRangeTotal, this);
	Register("Query4CondRangeCachedTotal", &ApiTvSimple::Query4CondRangeCachedTotal, this);
}

Error ApiTvSimple::Initialize() {
	assert(db_);
	auto err = db_->AddNamespace(nsdef_);

	if (!err.ok()) return err;

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

	countryLikePatterns_.reserve(countries_.size());
	for (const auto& country : countries_) {
		countryLikePatterns_.push_back(reindexer::makeLikePattern(country));
	}

	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};

	for (int i = 0; i < 10; i++) packages_.emplace_back(randomNumArray<int>(20, 10000, 10));

	for (int i = 0; i < 20; i++) priceIDs_.emplace_back(randomNumArray<int>(10, 7000, 50));

	start_times_.resize(20);
	for (int i = 0; i < 20; i++) start_times_[i] = random<int>(0, 50000);

	return 0;
}

reindexer::Item ApiTvSimple::MakeItem() {
	Item item = db_->NewItem(nsdef_.name);
	// All strings passed to item must be holded by app
	item.Unsafe();

	auto startTime = random<int>(0, 50000);

	item["id"] = id_seq_->Next();
	item["genre"] = random<int64_t>(0, 49);
	item["year"] = random<int>(2000, 2049);
	item["packages"] = packages_.at(random<size_t>(0, packages_.size() - 1));
	item["countries"] = countries_.at(random<size_t>(0, countries_.size() - 1));
	item["age"] = random<int>(0, 4);
	item["price_id"] = priceIDs_.at(random<size_t>(0, priceIDs_.size() - 1));
	item["location"] = locations_.at(random<size_t>(0, locations_.size() - 1));
	item["start_time"] = start_times_.at(random<size_t>(0, start_times_.size() - 1));
	item["end_time"] = startTime + random<int>(1, 5) * 1000;

	// wrSer_.Reset();
	// reindexer::JsonBuilder bld(wrSer_);
	// bld.Put("id", id_seq_->Next());
	// bld.Put("genre", random<int64_t>(0, 49));
	// bld.Put("year", random<int>(2000, 2049));
	// bld.Array("packages", reindexer::span<int>(packages_.at(random<size_t>(0, packages_.size() - 1))));
	// bld.Put("countries", countries_.at(random<size_t>(0, countries_.size() - 1)));
	// bld.Put("age", random<int>(0, 4));
	// bld.Array("price_id", reindexer::span<int>(priceIDs_.at(random<size_t>(0, priceIDs_.size() - 1))));
	// bld.Put("location", locations_.at(random<size_t>(0, locations_.size() - 1)));
	// bld.Put("start_time", start_times_.at(random<size_t>(0, start_times_.size() - 1)));
	// bld.Put("end_time", startTime + random<int>(1, 5) * 1000);
	// bld.End();
	// item.FromJSON(wrSer_.Slice());

	return item;
}

// FIXTURES

void ApiTvSimple::WarmUpIndexes(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Error err;

		// Ensure indexes complete build
		// In current implementation - just wait
		// Index build process is in background routine
		std::this_thread::sleep_for(std::chrono::milliseconds(2000));

		for (size_t i = 0; i < packages_.size() * 3; i++) {
			{
				QueryResults qres;
				Query q(nsdef_.name);
				auto randIdx = random<size_t>(0, packages_.size() - 1);
				q.Where("packages", CondSet, toArray<int>(packages_.at(randIdx))).Limit(20).Sort("start_time", false);
				err = db_->Select(q, qres);
				if (!err.ok()) state.SkipWithError(err.what().c_str());
			}

			{
				QueryResults qres;
				Query q(nsdef_.name);
				auto randIdx = random<size_t>(0, packages_.size() - 1);
				q.Where("packages", CondSet, toArray<int>(packages_.at(randIdx))).Limit(20).Sort("year", false);
				err = db_->Select(q, qres);
				if (!err.ok()) state.SkipWithError(err.what().c_str());
			}

			{
				QueryResults qres;
				Query q(nsdef_.name);
				q.Where("year", CondRange, {2010, 2016}).Limit(20);
				err = db_->Select(q, qres);
				if (!err.ok()) state.SkipWithError(err.what().c_str());
			}
		}

		for (size_t i = 0; i < priceIDs_.size() * 3; i++) {
			QueryResults qres;
			Query q("JoinItems");
			auto randIdx = random<size_t>(0, priceIDs_.size() - 1);
			q.Where("id", CondSet, toArray<int>(priceIDs_.at(randIdx))).Limit(20);
			err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}

		for (size_t i = 0; i < 1000; ++i) {
			Query q(nsdef_.name);
			q.Where("start_time", CondEq, start_times_.at(random<size_t>(0, start_times_.size() - 1)));
			QueryResults qres;
			auto err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}

		for (size_t i = 0; i < priceIDs_.size() * 3; ++i) {
			Query q(nsdef_.name);
			q.Where("price_id", CondEq, priceIDs_[random<size_t>(0, priceIDs_.size() - 1)]);
			QueryResults qres;
			auto err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}

		for (size_t i = 0; i < countries_.size() * 3; ++i) {
			Query q(nsdef_.name);
			q.Where("countries", CondEq, countries_[random<size_t>(0, countries_.size() - 1)]);
			QueryResults qres;
			auto err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}

		for (size_t i = 0; i < countryLikePatterns_.size() * 3; ++i) {
			Query q(nsdef_.name);
			q.Where("countries", CondLike, countryLikePatterns_[random<size_t>(0, countryLikePatterns_.size() - 1)]);
			QueryResults qres;
			auto err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimple::GetByID(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("id", CondEq, random<int>(id_seq_->Start(), id_seq_->End()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetEqInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("start_time", CondEq, start_times_.at(random<size_t>(0, start_times_.size() - 1)));
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetEqArrayInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("price_id", CondEq, priceIDs_[random<size_t>(0, priceIDs_.size() - 1)]);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetEqString(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("countries", CondEq, countries_[random<size_t>(0, countries_.size() - 1)]);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetLikeString(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("countries", CondLike, countryLikePatterns_[random<size_t>(0, countryLikePatterns_.size() - 1)]);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetByRangeIDAndSortByHash(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		Query q(nsdef_.name);
		q.Where("id", CondRange, {idRange.first, idRange.second}).Sort("age", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetByRangeIDAndSortByTree(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		Query q(nsdef_.name);
		q.Where("id", CondRange, {idRange.first, idRange.second}).Sort("genre", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::Query1Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query1CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query1CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondLeftJoin(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.LeftJoin("price_id", "id", CondSet, q4join);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondLeftJoinTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.LeftJoin("price_id", "id", CondSet, q4join)
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondLeftJoinCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.LeftJoin("price_id", "id", CondSet, q4join)
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query0CondInnerJoinUnlimit(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q4join("JoinItems");
		Query q(nsdef_.name);
		q.ReqTotal().Limit(1);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondEq, "mos");

		q.InnerJoin("price_id", "id", CondSet, q4join);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondInnerJoin(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.InnerJoin("price_id", "id", CondSet, q4join);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondInnerJoinTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.InnerJoin("price_id", "id", CondSet, q4join)
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondInnerJoinCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.InnerJoin("price_id", "id", CondSet, q4join)
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query3Cond(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		Query q(nsdef_.name);
		q.Sort("year", false)
			.Limit(20)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query3CondTotal(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		Query q(nsdef_.name);
		q.Sort("year", false)
			.Limit(20)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query3CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		Query q(nsdef_.name);
		q.Sort("year", false)
			.Limit(20)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query3CondKillIdsCache(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, randomNumArray<int>(20, 10000, 10));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query3CondRestoreIdsCache(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query4Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("age", CondEq, 2)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query4CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("age", CondEq, 2)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query4CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("age", CondEq, 2)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query4CondRange(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		int startTime = random<int>(0, 50000);
		int endTime = startTime + 10000;
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("start_time", CondGt, startTime)
			.Where("end_time", CondLt, endTime);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query4CondRangeTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		int startTime = random<int>(0, 50000);
		int endTime = startTime + 10000;
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("start_time", CondGt, startTime)
			.Where("end_time", CondLt, endTime)
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query4CondRangeCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		int startTime = random<int>(0, 50000);
		int endTime = startTime + 10000;
		Query q(nsdef_.name);
		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("start_time", CondGt, startTime)
			.Where("end_time", CondLt, endTime)
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}
