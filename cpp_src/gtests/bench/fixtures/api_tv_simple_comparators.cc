#include "api_tv_simple_comparators.h"
#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"
#include "helpers.h"

using benchmark::AllocsTracker;
using reindexer::Query;
using reindexer::QueryResults;

void ApiTvSimpleComparators::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	BaseFixture::RegisterAllCases();
	Register("WarmUpIndexes", &ApiTvSimpleComparators::WarmUpIndexes, this)->Iterations(1);	 // Just 1 time!!!
	Base::RegisterAllCases();

	Register("StringsSelect", &ApiTvSimpleComparators::StringsSelect, BasePtr());
	Register("GetEqInt", &ApiTvSimpleComparators::GetEqInt, BasePtr());
	Register("GetEqArrayInt", &ApiTvSimpleComparators::GetEqArrayInt, BasePtr());
	Register("GetEqString", &ApiTvSimpleComparators::GetEqString, BasePtr());
	Register("GetByRangeIDAndSort", &ApiTvSimpleComparators::GetByRangeIDAndSort, this);
	Register("GetUuidStr", &ApiTvSimpleComparators::GetUuidStr, BasePtr());

	Register("Query2Cond", &ApiTvSimpleComparators::Query2Cond<NoTotal>, BasePtr());
	Register("Query2CondTotal", &ApiTvSimpleComparators::Query2Cond<ReqTotal>, BasePtr());
	Register("Query2CondCachedTotal", &ApiTvSimpleComparators::Query2Cond<CachedTotal>, BasePtr());
	Register("Query3Cond", &ApiTvSimpleComparators::Query3Cond<NoTotal>, BasePtr());
	Register("Query3CondTotal", &ApiTvSimpleComparators::Query3Cond<ReqTotal>, BasePtr());
	Register("Query3CondCachedTotal", &ApiTvSimpleComparators::Query3Cond<CachedTotal>, BasePtr());
	Register("Query3CondKillIdsCache", &ApiTvSimpleComparators::Query3CondKillIdsCache, BasePtr());
	Register("Query3CondRestoreIdsCache", &ApiTvSimpleComparators::Query3CondRestoreIdsCache, BasePtr());
	Register("Query4Cond", &ApiTvSimpleComparators::Query4Cond<NoTotal>, BasePtr());
	Register("Query4CondTotal", &ApiTvSimpleComparators::Query4Cond<ReqTotal>, BasePtr());
	Register("Query4CondCachedTotal", &ApiTvSimpleComparators::Query4Cond<CachedTotal>, BasePtr());
	Register("Query4CondRange", &ApiTvSimpleComparators::Query4CondRange<NoTotal>, BasePtr());
	Register("Query4CondRangeTotal", &ApiTvSimpleComparators::Query4CondRange<ReqTotal>, BasePtr());
	Register("Query4CondRangeCachedTotal", &ApiTvSimpleComparators::Query4CondRange<CachedTotal>, BasePtr());

	Register("QueryDistinctOneField", &ApiTvSimpleComparators::QueryDistinctOneField, this);
	Register("QueryDistinctTwoField", &ApiTvSimpleComparators::QueryDistinctTwoField, this);
	Register("QueryDistinctTwoFieldArray", &ApiTvSimpleComparators::QueryDistinctTwoFieldArray, this);

	Register("QueryDistinctOneFieldLimit", &ApiTvSimpleComparators::QueryDistinctOneFieldLimit, this);
	Register("QueryDistinctTwoFieldLimit", &ApiTvSimpleComparators::QueryDistinctTwoFieldLimit, this);
	Register("QueryDistinctTwoFieldArrayLimit", &ApiTvSimpleComparators::QueryDistinctTwoFieldArrayLimit, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Error ApiTvSimpleComparators::Initialize() {
	auto err = Base::Initialize();
	if (!err.ok()) {
		return err;
	}

	NamespaceDef strNsDef{stringSelectNs_};
	strNsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("str_hash_coll_none", "-", "string", IndexOpts())
		.AddIndex("str_hash_coll_ascii", "-", "string", IndexOpts(0, CollateASCII))
		.AddIndex("str_hash_coll_utf8", "-", "string", IndexOpts(0, CollateUTF8))
		.AddIndex("str_hash_coll_num", "-", "string", IndexOpts(0, CollateNumeric));

	err = db_->AddNamespace(strNsDef);
	if (!err.ok()) {
		return err;
	}

	for (size_t i = 0; i < kTotalItemsStringSelectNs; ++i) {
		auto item = MakeStrItem();
		if (!item.Status().ok()) {
			return item.Status();
		}
		err = db_->Insert(stringSelectNs_, item);
		if (!err.ok()) {
			return err;
		}
	}
	return err;
}

reindexer::Item ApiTvSimpleComparators::MakeStrItem() {
	static int id = 0;
	reindexer::Item item = db_->NewItem(stringSelectNs_);
	if (item.Status().ok()) {
		std::ignore = item.Unsafe();
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
		if (!err.ok()) {
			assert(!item.Status().ok());
		}
	}
	return item;
}

reindexer::Item ApiTvSimpleComparators::MakeItem(benchmark::State&) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	// All strings passed to item must be holded by app
	std::ignore = item.Unsafe();

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

void ApiTvSimpleComparators::GetByRangeIDAndSort(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		Query q(nsdef_.name);
		q.Where("id", CondRange, {idRange.first, idRange.second}).Sort("age", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}

		if (!qres.Count()) {
			state.SkipWithError("Results does not contain any value");
		}
	}
}

void ApiTvSimpleComparators::QueryDistinctOneField(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Distinct("year");
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void ApiTvSimpleComparators::QueryDistinctTwoField(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Distinct("year", "location");
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void ApiTvSimpleComparators::QueryDistinctTwoFieldArray(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Distinct("packages", "countries");
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void ApiTvSimpleComparators::QueryDistinctOneFieldLimit(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Distinct("year").Limit(20);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void ApiTvSimpleComparators::QueryDistinctTwoFieldLimit(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Distinct("year", "location").Limit(20);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void ApiTvSimpleComparators::QueryDistinctTwoFieldArrayLimit(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Distinct("packages", "countries").Limit(20);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}
