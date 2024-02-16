#include "api_tv_simple_comparators.h"
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

template <size_t L>
reindexer::span<bool> randBoolArray() {
	static bool ret[L];
	for (size_t i = 0; i < L; ++i) ret[i] = rand() % 2;
	return ret;
}

template <size_t L>
reindexer::span<int> randIntArray() {
	static int ret[L];
	for (size_t i = 0; i < L; ++i) ret[i] = rand();
	return ret;
}

template <size_t L>
reindexer::span<int64_t> randInt64Array() {
	static int64_t ret[L];
	for (size_t i = 0; i < L; ++i) ret[i] = rand();
	return ret;
}

template <size_t L>
reindexer::span<double> randDoubleArray() {
	static double ret[L];
	for (size_t i = 0; i < L; ++i) ret[i] = double(rand()) / (rand() + 1);
	return ret;
}

void ApiTvSimpleComparators::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	BaseFixture::RegisterAllCases();
	Register("WarmUpIndexes", &ApiTvSimpleComparators::WarmUpIndexes, this)->Iterations(1);	 // Just 1 time!!!

	Register("StringsSelect", &ApiTvSimpleComparators::StringsSelect, this);
	Register("GetEqInt", &ApiTvSimpleComparators::GetEqInt, this);
	Register("GetEqArrayInt", &ApiTvSimpleComparators::GetEqArrayInt, this);
	Register("GetEqString", &ApiTvSimpleComparators::GetEqString, this);
	Register("GetByRangeIDAndSort", &ApiTvSimpleComparators::GetByRangeIDAndSort, this);
	Register("GetUuidStr", &ApiTvSimpleComparators::GetUuidStr, this);

	Register("Query1Cond", &ApiTvSimpleComparators::Query1Cond, this);
	Register("Query1CondTotal", &ApiTvSimpleComparators::Query1CondTotal, this);
	Register("Query1CondCachedTotal", &ApiTvSimpleComparators::Query1CondCachedTotal, this);
	Register("Query2Cond", &ApiTvSimpleComparators::Query2Cond, this);
	Register("Query2CondTotal", &ApiTvSimpleComparators::Query2CondTotal, this);
	Register("Query2CondCachedTotal", &ApiTvSimpleComparators::Query2CondCachedTotal, this);
	Register("Query3Cond", &ApiTvSimpleComparators::Query3Cond, this);
	Register("Query3CondTotal", &ApiTvSimpleComparators::Query3CondTotal, this);
	Register("Query3CondCachedTotal", &ApiTvSimpleComparators::Query3CondCachedTotal, this);
	Register("Query3CondKillIdsCache", &ApiTvSimpleComparators::Query3CondKillIdsCache, this);
	Register("Query3CondRestoreIdsCache", &ApiTvSimpleComparators::Query3CondRestoreIdsCache, this);
	Register("Query4Cond", &ApiTvSimpleComparators::Query4Cond, this);
	Register("Query4CondTotal", &ApiTvSimpleComparators::Query4CondTotal, this);
	Register("Query4CondCachedTotal", &ApiTvSimpleComparators::Query4CondCachedTotal, this);
	Register("Query4CondRange", &ApiTvSimpleComparators::Query4CondRange, this);
	Register("Query4CondRangeTotal", &ApiTvSimpleComparators::Query4CondRangeTotal, this);
	Register("Query4CondRangeCachedTotal", &ApiTvSimpleComparators::Query4CondRangeCachedTotal, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Error ApiTvSimpleComparators::Initialize() {
	assertrx(db_);
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

	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};

	uuids_.reserve(1000);
	for (size_t i = 0; i < 1000; ++i) {
		uuids_.emplace_back(randStrUuid());
	}

	for (int i = 0; i < 10; i++) packages_.emplace_back(randomNumArray<int>(20, 10000, 10));

	for (int i = 0; i < 20; i++) priceIDs_.emplace_back(randomNumArray<int>(10, 7000, 50));

	start_times_.resize(20);
	for (int i = 0; i < 20; i++) start_times_[i] = random<int>(0, 50000);

	NamespaceDef strNsDef{stringSelectNs_};
	strNsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("str_hash_coll_none", "-", "string", IndexOpts())
		.AddIndex("str_hash_coll_ascii", "-", "string", IndexOpts(0, CollateASCII))
		.AddIndex("str_hash_coll_utf8", "-", "string", IndexOpts(0, CollateUTF8))
		.AddIndex("str_hash_coll_num", "-", "string", IndexOpts(0, CollateNumeric));

	err = db_->AddNamespace(strNsDef);
	if (!err.ok()) return err;

	for (size_t i = 0; i < kTotalItemsStringSelectNs; ++i) {
		auto item = MakeStrItem();
		if (!item.Status().ok()) return item.Status();
		err = db_->Insert(stringSelectNs_, item);
		if (!err.ok()) return err;
	}
	err = db_->Commit(stringSelectNs_);
	return err;
}

reindexer::Item ApiTvSimpleComparators::MakeStrItem() {
	static int id = 0;
	reindexer::Item item = db_->NewItem(stringSelectNs_);
	if (item.Status().ok()) {
		item.Unsafe();
		wrSer_.Reset();
		reindexer::JsonBuilder bld(wrSer_);
		bld.Put("id", id++);
		const std::string idStr = std::to_string(id);
		bld.Put("str_hash_coll_none", "h" + idStr);
		bld.Put("str_hash_coll_ascii", "ha" + idStr);
		bld.Put("str_hash_coll_utf8", "hu" + idStr);
		bld.Put("str_hash_coll_num", idStr + "hn");
		bld.Put("field_int", id);
		bld.Put("field_str", "value_" + idStr);
		bld.End();
		const auto err = item.FromJSON(wrSer_.Slice());
		if (!err.ok()) assert(!item.Status().ok());
	}
	return item;
}

reindexer::Item ApiTvSimpleComparators::MakeItem(benchmark::State&) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
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
	item["uuid_str"] = uuids_[rand() % uuids_.size()];

	return item;
}

// FIXTURES

void ApiTvSimpleComparators::WarmUpIndexes(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		reindexer::Error err;

		// Ensure indexes complete build
		WaitForOptimization();
	}
}

void ApiTvSimpleComparators::StringsSelect(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(stringSelectNs_);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimpleComparators::GetEqInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("start_time", CondEq, start_times_.at(random<size_t>(0, start_times_.size() - 1)));
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimpleComparators::GetEqArrayInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("price_id", CondEq, priceIDs_[random<size_t>(0, priceIDs_.size() - 1)]);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimpleComparators::GetEqString(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("countries", CondEq, countries_[random<size_t>(0, countries_.size() - 1)]);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimpleComparators::GetByRangeIDAndSort(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		Query q(nsdef_.name);
		q.Where("id", CondRange, {idRange.first, idRange.second}).Sort("age", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimpleComparators::GetUuidStr(benchmark::State& state) {
	const auto& uuid = uuids_[rand() % uuids_.size()];
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("uuid_str", CondEq, uuid);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimpleComparators::Query1Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimpleComparators::Query1CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimpleComparators::Query1CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimpleComparators::Query2Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimpleComparators::Query2CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimpleComparators::Query2CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimpleComparators::Query3Cond(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query3CondTotal(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query3CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocksTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query3CondKillIdsCache(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query3CondRestoreIdsCache(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query4Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query4CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query4CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimpleComparators::Query4CondRange(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		int startTime = random<int>(0, 30000);
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

void ApiTvSimpleComparators::Query4CondRangeTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		int startTime = random<int>(0, 30000);
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

void ApiTvSimpleComparators::Query4CondRangeCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		int startTime = random<int>(0, 30000);
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
