#include "api_tv_simple.h"
#include <thread>
#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"
#include "core/nsselecter/joinedselector.h"
#include "core/queryresults/queryresults.h"
#include "helpers.h"
#include "tools/string_regexp_functions.h"

using reindexer::Query;

constexpr char kJoinNamespace[] = "JoinItems";

#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN) || defined(RX_WITH_STDLIB_DEBUG)
constexpr benchmark::IterationCount k0CondJoinIters = 8;
constexpr benchmark::IterationCount kQuery4CondIters = 80;
#else	// defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN) || defined(RX_WITH_STDLIB_DEBUG)
constexpr benchmark::IterationCount k0CondJoinIters = 500;
constexpr benchmark::IterationCount kQuery4CondIters = 1000;
#endif	// defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN) || defined(RX_WITH_STDLIB_DEBUG)

void ApiTvSimple::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	BaseFixture::RegisterAllCases();
	Register("WarmUpIndexes", &ApiTvSimple::WarmUpIndexes, this)->Iterations(1);  // Just 1 time!!!

	Base::RegisterAllCases();

	Register("StringsSelect", &ApiTvSimple::StringsSelect, BasePtr());
	Register("GetByID", &ApiTvSimple::GetByID, this);
	Register("GetByIDInBrackets", &ApiTvSimple::GetByIDInBrackets, this);
	Register("GetEqInt", &ApiTvSimple::GetEqInt, BasePtr());
	Register("GetEqArrayInt", &ApiTvSimple::GetEqArrayInt, BasePtr());
	Register("GetEqString", &ApiTvSimple::GetEqString, BasePtr());
	Register("GetLikeString", &ApiTvSimple::GetLikeString, this);
	Register("GetByRangeIDAndSortByHash", &ApiTvSimple::GetByRangeIDAndSortByHash, BasePtr());
	Register("GetByRangeIDAndSortByTree", &ApiTvSimple::GetByRangeIDAndSortByTree, BasePtr());
	Register("GetUuid", &ApiTvSimple::GetUuid, this);
	Register("GetUuidStr", &ApiTvSimple::GetUuidStr, BasePtr());

	Register("Query2Cond", &ApiTvSimple::Query2Cond<NoTotal>, BasePtr());
	Register("Query2CondTotal", &ApiTvSimple::Query2Cond<ReqTotal>, BasePtr());
	Register("Query2CondCachedTotal", &ApiTvSimple::Query2Cond<CachedTotal>, BasePtr());
	Register("Query2CondLeftJoin2Cond", &ApiTvSimple::Query2CondLeftJoin2Cond<NoTotal>, this);
	Register("Query2CondLeftJoin2CondTotal", &ApiTvSimple::Query2CondLeftJoin2Cond<ReqTotal>, this);
	Register("Query2CondLeftJoin2CondCachedTotal", &ApiTvSimple::Query2CondLeftJoin2Cond<CachedTotal>, this);
	Register("Query2CondLeftJoin3Cond", &ApiTvSimple::Query2CondLeftJoin3Cond<NoTotal>, this);
	Register("Query2CondLeftJoin3CondTotal", &ApiTvSimple::Query2CondLeftJoin3Cond<ReqTotal>, this);
	Register("Query2CondLeftJoin3CondCachedTotal", &ApiTvSimple::Query2CondLeftJoin3Cond<CachedTotal>, this);

	Register("Query0CondInnerJoinUnlimit", &ApiTvSimple::Query0CondInnerJoinUnlimit, this)->Iterations(k0CondJoinIters);
	Register("Query0CondInnerJoinUnlimitLowSelectivity", &ApiTvSimple::Query0CondInnerJoinUnlimitLowSelectivity, this)
		->Iterations(k0CondJoinIters);
	Register("Query0CondInnerJoinPreResultStoreValues", &ApiTvSimple::Query0CondInnerJoinPreResultStoreValues, this)
		->Iterations(k0CondJoinIters);
	Register("InnerJoinInjectConditionFromMain", &ApiTvSimple::InnerJoinInjectConditionFromMain, this);
	Register("InnerJoinRejectInjection", &ApiTvSimple::InnerJoinRejectInjection, this);

	Register("Query2CondInnerJoin2Cond", &ApiTvSimple::Query2CondInnerJoin2Cond<NoTotal>, this);
	Register("Query2CondInnerJoin2CondTotal", &ApiTvSimple::Query2CondInnerJoin2Cond<ReqTotal>, this);
	Register("Query2CondInnerJoin2CondCachedTotal", &ApiTvSimple::Query2CondInnerJoin2Cond<CachedTotal>, this);
	Register("Query2CondInnerJoin3Cond", &ApiTvSimple::Query2CondInnerJoin3Cond<NoTotal>, this);
	Register("Query2CondInnerJoin3CondTotal", &ApiTvSimple::Query2CondInnerJoin3Cond<ReqTotal>, this);
	Register("Query2CondInnerJoin3CondCachedTotal", &ApiTvSimple::Query2CondInnerJoin3Cond<CachedTotal>, this);

	Register("Query3Cond", &ApiTvSimple::Query3Cond<NoTotal>, BasePtr());
	Register("Query3CondTotal", &ApiTvSimple::Query3Cond<ReqTotal>, BasePtr());
	Register("Query3CondCachedTotal", &ApiTvSimple::Query3Cond<CachedTotal>, BasePtr());
	Register("Query3CondKillIdsCache", &ApiTvSimple::Query3CondKillIdsCache, BasePtr());
	Register("Query3CondRestoreIdsCache", &ApiTvSimple::Query3CondRestoreIdsCache, BasePtr());

	Register("Query4Cond", &ApiTvSimple::Query4Cond<NoTotal>, BasePtr());
	Register("Query4CondTotal", &ApiTvSimple::Query4Cond<ReqTotal>, BasePtr());
	Register("Query4CondCachedTotal", &ApiTvSimple::Query4Cond<CachedTotal>, BasePtr());
	Register("Query4CondRange", &ApiTvSimple::Query4CondRange<NoTotal>, BasePtr())->Iterations(kQuery4CondIters);
	Register("Query4CondRangeTotal", &ApiTvSimple::Query4CondRange<ReqTotal>, BasePtr())->Iterations(kQuery4CondIters);
	Register("Query4CondRangeCachedTotal", &ApiTvSimple::Query4CondRange<CachedTotal>, BasePtr())->Iterations(kQuery4CondIters);

	Register("QueryForcedSortHash", &ApiTvSimple::QueryForcedSortHash, this);
	Register("QueryForcedSortTree", &ApiTvSimple::QueryForcedSortTree, this);
	Register("QueryForcedSortDistinctHash", &ApiTvSimple::QueryForcedSortDistinctHash, this);
	Register("QueryForcedSortDistinctLowSelectivityHash", &ApiTvSimple::QueryForcedSortDistinctLowSelectivityHash, this);
	Register("QueryForcedSortDistinctTree", &ApiTvSimple::QueryForcedSortDistinctTree, this);

#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	Register("Query2CondIdSet10", &ApiTvSimple::Query2CondIdSet10, this);
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
	Register("Query2CondIdSet100", &ApiTvSimple::Query2CondIdSet100, this);
	Register("Query2CondIdSet500", &ApiTvSimple::Query2CondIdSet500, this);
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	Register("Query2CondIdSet2000", &ApiTvSimple::Query2CondIdSet2000, this);
	Register("Query2CondIdSet20000", &ApiTvSimple::Query2CondIdSet20000, this);
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	Register("SubQueryEq", &ApiTvSimple::SubQueryEq, this);
	Register("SubQuerySet", &ApiTvSimple::SubQuerySet, this);
	Register("SubQueryAggregate", &ApiTvSimple::SubQueryAggregate, this);

	// Those benches should be last, because they are recreating indexes cache
	Register("Query4CondRangeDropCache", &ApiTvSimple::Query4CondRangeDropCache<NoTotal>, this)->Iterations(kQuery4CondIters);
	Register("Query4CondRangeDropCacheTotal", &ApiTvSimple::Query4CondRangeDropCache<ReqTotal>, this)->Iterations(kQuery4CondIters);
	Register("Query4CondRangeDropCacheCachedTotal", &ApiTvSimple::Query4CondRangeDropCache<CachedTotal>, this)
		->Iterations(kQuery4CondIters);
	//  NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Error ApiTvSimple::Initialize() {
	auto err = Base::Initialize();
	if (!err.ok()) {
		return err;
	}

	countryLikePatterns_.reserve(countries_.size());
	for (const auto& country : countries_) {
		countryLikePatterns_.emplace_back(reindexer::makeLikePattern(country));
	}

	for (auto sz : idsetsSz_) {
		constexpr unsigned kTotalIdSets = 1000;
		auto& idsets = idsets_[sz];
		idsets.reserve(kTotalIdSets);
		for (unsigned i = 0; i < kTotalIdSets; ++i) {
			idsets.emplace_back(randomNumArray<int>(sz, 0, kTotalItemsMainJoinNs));
		}
	}

	NamespaceDef strNsDef{stringSelectNs_};
	strNsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("str_hash_coll_none", "hash", "string", IndexOpts())
		.AddIndex("str_hash_coll_ascii", "hash", "string", IndexOpts(0, CollateASCII))
		.AddIndex("str_hash_coll_utf8", "hash", "string", IndexOpts(0, CollateUTF8))
		.AddIndex("str_hash_coll_num", "hash", "string", IndexOpts(0, CollateNumeric))
		.AddIndex("str_tree_coll_none", "tree", "string", IndexOpts())
		.AddIndex("str_tree_coll_ascii", "tree", "string", IndexOpts(0, CollateASCII))
		.AddIndex("str_tree_coll_utf8", "tree", "string", IndexOpts(0, CollateUTF8))
		.AddIndex("str_tree_coll_num", "tree", "string", IndexOpts(0, CollateNumeric))
		.AddIndex("str_ft_coll_none", "text", "string", IndexOpts())
		.AddIndex("str_ft_coll_ascii", "text", "string", IndexOpts(0, CollateASCII))
		.AddIndex("str_ft_coll_utf8", "text", "string", IndexOpts(0, CollateUTF8))
		.AddIndex("str_ft_coll_num", "text", "string", IndexOpts(0, CollateNumeric))
		.AddIndex("str_fuzzy_coll_none", "fuzzytext", "string", IndexOpts())
		.AddIndex("str_fuzzy_coll_ascii", "fuzzytext", "string", IndexOpts(0, CollateASCII))
		.AddIndex("str_fuzzy_coll_utf8", "fuzzytext", "string", IndexOpts(0, CollateUTF8))
		.AddIndex("str_fuzzy_coll_num", "fuzzytext", "string", IndexOpts(0, CollateNumeric));

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

	NamespaceDef mainNsDef{mainNs_};
	mainNsDef.AddIndex("id", "hash", "int", IndexOpts().PK()).AddIndex("field", "hash", "int", IndexOpts());
	err = db_->AddNamespace(mainNsDef);
	if (!err.ok()) {
		return err;
	}
	NamespaceDef rightNsDef{rightNs_};
	rightNsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("field", "hash", "int", IndexOpts())
		.AddIndex("id_tree", "tree", "int", IndexOpts());
	err = db_->AddNamespace(rightNsDef);
	if (!err.ok()) {
		return err;
	}

	for (size_t i = 0; i < kTotalItemsMainJoinNs; ++i) {
		reindexer::Item mItem = db_->NewItem(mainNsDef.name);
		if (!mItem.Status().ok()) {
			return mItem.Status();
		}
		std::ignore = mItem.Unsafe();
		wrSer_.Reset();
		reindexer::JsonBuilder bld(wrSer_);
		bld.Put("id", i);
		bld.Put("field", i);
		bld.End();
		err = mItem.FromJSON(wrSer_.Slice());
		if (!err.ok()) {
			return err;
		}
		err = db_->Insert(mainNsDef.name, mItem);
		if (!err.ok()) {
			return err;
		}

		reindexer::Item rItem = db_->NewItem(rightNsDef.name);
		if (!rItem.Status().ok()) {
			return rItem.Status();
		}
		std::ignore = rItem.Unsafe();
		wrSer_.Reset();
		reindexer::JsonBuilder bld2(wrSer_);
		bld2.Put("id", i);
		bld2.Put("field", i);
		bld2.Put("id_tree", i);
		bld2.End();
		err = rItem.FromJSON(wrSer_.Slice());
		if (!err.ok()) {
			return err;
		}
		err = db_->Insert(rightNsDef.name, rItem);
		if (!err.ok()) {
			return err;
		}
	}
	return {};
}

reindexer::Item ApiTvSimple::MakeStrItem() {
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
		bld.Put("str_tree_coll_none", "t" + idStr);
		bld.Put("str_tree_coll_ascii", "ta" + idStr);
		bld.Put("str_tree_coll_utf8", "tu" + idStr);
		bld.Put("str_tree_coll_num", idStr + "tn");
		bld.Put("str_ft_coll_none", "ft" + idStr);
		bld.Put("str_ft_coll_ascii", "fta" + idStr);
		bld.Put("str_ft_coll_utf8", "ftu" + idStr);
		bld.Put("str_ft_coll_num", idStr + "ftn");
		bld.Put("str_fuzzy_coll_none", "fu" + idStr);
		bld.Put("str_fuzzy_coll_ascii", "fua" + idStr);
		bld.Put("str_fuzzy_coll_utf8", "fuu" + idStr);
		bld.Put("str_fuzzy_coll_num", idStr + "fun");
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

reindexer::Item ApiTvSimple::MakeItem(benchmark::State&) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	// All strings passed to item must be holded by app
	std::ignore = item.Unsafe();

	auto startTime = random<int>(0, 50000);

	item["id"] = id_seq_->Next();
	item["genre"] = random<int64_t>(0, 49);
	item["year"] = random<int>(kMinYear, kMaxYear);
	item["packages"] = packages_.at(random<size_t>(0, packages_.size() - 1));
	item["countries"] = countries_.at(random<size_t>(0, countries_.size() - 1));
	item["age"] = random<int>(0, 4);
	item["price_id"] = priceIDs_.at(random<size_t>(0, priceIDs_.size() - 1));
	item["location"] = locations_.at(random<size_t>(0, locations_.size() - 1));
	item["start_time"] = start_times_.at(random<size_t>(0, start_times_.size() - 1));
	item["end_time"] = startTime + random<int>(1, 5) * 1000;
	item["uuid"] = reindexer::Uuid{uuids_[rand() % uuids_.size()]};
	item["uuid_str"] = uuids_[rand() % uuids_.size()];

	return item;
}

// FIXTURES

void ApiTvSimple::WarmUpIndexes(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		reindexer::Error err;

		// Ensure indexes complete build
		WaitForOptimization();
		for (size_t i = 0; i < packages_.size() * 2; ++i) {
			reindexer::QueryResults qres;
			Query q(nsdef_.name);
			q.Where("packages", CondSet, packages_.at(i % packages_.size())).Limit(20).Sort("start_time", false);
			err = db_->Select(q, qres);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
			checkNotEmpty(qres, state);
		}

		for (size_t i = 0; i < packages_.size() * 2; ++i) {
			reindexer::QueryResults qres;
			Query q(nsdef_.name);
			q.Where("packages", CondSet, packages_.at(i % packages_.size())).Limit(20).Sort("year", false);
			err = db_->Select(q, qres);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
			checkNotEmpty(qres, state);
		}

		for (size_t i = 0; i < priceIDs_.size() * 3; ++i) {
			reindexer::QueryResults qres;
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

void ApiTvSimple::GetByID(benchmark::State& state) {
	const auto q = [&] { return Query(nsdef_.name).Where("id", CondEq, random<int>(id_seq_->Start(), id_seq_->End())); };
	benchQuery(q, state);
}

void ApiTvSimple::GetByIDInBrackets(benchmark::State& state) {
	const auto q = [&] {
		return Query(nsdef_.name).OpenBracket().Where("id", CondEq, random<int>(id_seq_->Start(), id_seq_->End())).CloseBracket();
	};
	benchQuery(q, state);
}

void ApiTvSimple::GetUuid(benchmark::State& state) {
	const reindexer::Uuid uuid{uuids_[rand() % uuids_.size()]};
	const auto q = Query(nsdef_.name).Where("uuid", CondEq, uuid);
	benchQuery(q, state);
}

void ApiTvSimple::GetLikeString(benchmark::State& state) {
	const auto q = [&] {
		return Query(nsdef_.name).Where("countries", CondLike, countryLikePatterns_[random<size_t>(0, countryLikePatterns_.size() - 1)]);
	};
	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimple::Query2CondLeftJoin2Cond(benchmark::State& state) {
	auto q4join = Query(kJoinNamespace)
					  .Where("device", CondEq, {"ottstb", "smarttv", "stb"})
					  .Where("location", CondSet, {"mos", "dv", "sib", "ural"});

	auto q = Query(nsdef_.name)
				 .Where("genre", CondEq, 5)
				 .Where("year", CondRange, {2010, 2016})
				 .LeftJoin("price_id", "id", CondSet, std::move(q4join))
				 .Sort("year", false)
				 .Limit(20);
	Total::Apply(q);
	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimple::Query2CondLeftJoin3Cond(benchmark::State& state) {
	auto q4join = Query(kJoinNamespace).Where("device", CondEq, "ottstb").Where("location", CondSet, "mos").Where("id", CondLt, 500);
	auto q = Query(nsdef_.name)
				 .Where("genre", CondEq, 5)
				 .Where("year", CondRange, {2010, 2016})
				 .LeftJoin("price_id", "id", CondSet, std::move(q4join))
				 .Sort("year", false)
				 .Limit(20);
	Total::Apply(q);
	benchQuery(q, state);
}

void ApiTvSimple::Query0CondInnerJoinUnlimit(benchmark::State& state) {
	const auto q = [&] {
		auto q4join = Query(rightNs_).Where("id", CondSet, randomNumArray<int>(10'000, 0, kTotalItemsMainJoinNs));
		return Query(nsdef_.name).InnerJoin("id", "id", CondSet, std::move(q4join)).ReqTotal();
	};
	benchQuery(q, state);
}

void ApiTvSimple::Query0CondInnerJoinUnlimitLowSelectivity(benchmark::State& state) {
	auto q4join = Query(rightNs_).Where("id", CondLe, 250);
	const auto q = Query(mainNs_).InnerJoin("id", "id", CondEq, std::move(q4join)).ReqTotal();
	benchQuery(q, state);
}

void ApiTvSimple::SubQueryEq(benchmark::State& state) {
	const auto q = [&] {
		auto subQuery = Query(rightNs_).Select({"field"}).Where("id", CondEq, VariantArray::Create(int(rand() % kTotalItemsMainJoinNs)));
		return Query(mainNs_).Where("id", CondEq, std::move(subQuery));
	};
	benchQuery(q, state);
}

void ApiTvSimple::SubQuerySet(benchmark::State& state) {
	const auto q = [&] {
		const int rangeMin = rand() % (kTotalItemsMainJoinNs - 500);
		auto subQuery = Query(rightNs_).Select({"id"}).Where("id_tree", CondRange, VariantArray::Create(rangeMin, rangeMin + 500));
		return Query(mainNs_).Where("id", CondSet, std::move(subQuery));
	};
	benchQuery(q, state);
}

void ApiTvSimple::SubQueryAggregate(benchmark::State& state) {
	const auto q = [&] {
		auto subQuery = Query(rightNs_)
							.Aggregate(AggAvg, {"id"})
							.Where("id", CondLt, VariantArray::Create(int(rand() % kTotalItemsMainJoinNs)))
							.Limit(500);
		return Query(mainNs_).Where("id", CondEq, std::move(subQuery));
	};
	benchQuery(q, state);
}

void ApiTvSimple::QueryForcedSortHash(State& state) {
	const auto forcedSort = generateForcedSort(id_seq_->Start(), id_seq_->Current(), 10);
	const auto q = Query(nsdef_.name).Where("age", CondLt, 2).Sort("id", false, forcedSort).Limit(20);
	benchQuery(q, state);
}

void ApiTvSimple::QueryForcedSortTree(State& state) {
	// 'year' has low selectivity, so we are not getting any benefits from tree-index here.
	// Hovewer, 'year'-index shows much better performance in the distinct-version of this benchmark (QueryForcedSortDistinctTree)
	const auto forcedSort = generateForcedSort(kMinYear, kMaxYear, 5);
	const auto q = Query(nsdef_.name).Where("age", CondLt, 2).Sort("year", false, forcedSort).Limit(20);
	benchQuery(q, state);
}

void ApiTvSimple::QueryForcedSortDistinctHash(State& state) {
	const auto forcedSort = generateForcedSort(id_seq_->Start(), id_seq_->Current(), 10);
	const auto q = Query(nsdef_.name).Distinct("uuid_str").Where("age", CondLt, 2).Sort("id", false, forcedSort).Limit(20);
	benchQuery(q, state);
}

void ApiTvSimple::QueryForcedSortDistinctLowSelectivityHash(State& state) {
	const auto forcedSort = generateForcedSort(id_seq_->Start(), id_seq_->Current(), 10);
	const auto q = Query(nsdef_.name).Distinct("start_time").Where("age", CondLt, 2).Sort("id", false, forcedSort).Limit(20);
	benchQuery(q, state);
}

void ApiTvSimple::QueryForcedSortDistinctTree(State& state) {
	const auto forcedSort = generateForcedSort(kMinYear, kMaxYear, 5);
	const auto q = Query(nsdef_.name).Distinct("uuid_str").Where("age", CondLt, 2).Sort("year", false, forcedSort).Limit(20);
	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimple::Query2CondInnerJoin2Cond(benchmark::State& state) {
	auto q4join = Query(kJoinNamespace)
					  .Where("device", CondSet, {"ottstb", "smarttv", "stb"})
					  .Where("location", CondSet, {"mos", "dv", "sib", "ural"});

	auto q = Query(nsdef_.name)
				 .Where("genre", CondEq, 5)
				 .Where("year", CondRange, {2010, 2016})
				 .InnerJoin("price_id", "id", CondSet, std::move(q4join))
				 .Sort("year", false)
				 .Limit(20);
	Total::Apply(q);

	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimple::Query2CondInnerJoin3Cond(benchmark::State& state) {
	auto q4join = Query(kJoinNamespace)
					  .Where("device", CondSet, {"ottstb", "smarttv", "stb"})
					  .Where("location", CondSet, {"mos", "dv", "sib", "ural"})
					  .Where("id", CondLt, kPriceIdStart + kPriceIdRegion);

	auto q = Query(nsdef_.name)
				 .Where("genre", CondEq, 5)
				 .Where("year", CondRange, {2010, 2016})
				 .InnerJoin("price_id", "id", CondSet, std::move(q4join))
				 .Sort("year", false)
				 .Limit(20);
	Total::Apply(q);

	benchQuery(q, state);
}

void ApiTvSimple::InnerJoinInjectConditionFromMain(benchmark::State& state) {
	constexpr size_t step = 7;
	size_t i = 4;
	const auto q = [&] {
		i += step;
		return Query(nsdef_.name)
			.Where("price_id", CondSet, priceIDs_.at(i % priceIDs_.size()))
			.InnerJoin("price_id", "id", CondSet, Query(kJoinNamespace))
			.Limit(100);
	};
	benchQuery(q, state);
}

void ApiTvSimple::InnerJoinRejectInjection(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Where("id", CondEq, {-100}).InnerJoin("price_id", "id", CondSet, Query{kJoinNamespace});
	benchQuery(q, state, allowEmptyResult);
}

void ApiTvSimple::Query0CondInnerJoinPreResultStoreValues(benchmark::State& state) {
	using reindexer::JoinedSelector;
	static const std::string rightNs = "rightNs";
	static const std::vector<std::string> leftNs = {"leftNs1", "leftNs2", "leftNs3", "leftNs4"};
	static constexpr const char* id = "id";
	static constexpr const char* data = "data";
	static constexpr int maxDataValue = 10;
	static constexpr int maxRightNsRowCount = maxDataValue * (JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization() - 1);
	static constexpr int maxLeftNsRowCount = 10000;

	const auto createNs = [this, &state](const std::string& ns) {
		reindexer::Error err = db_->OpenNamespace(ns);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		err = db_->AddIndex(ns, {id, "hash", "int", IndexOpts().PK()});
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		err = db_->AddIndex(ns, {data, "hash", "int", IndexOpts()});
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	};
	const auto fill = [this, &state](const std::string& ns, int startId, int endId) {
		reindexer::Error err;
		for (int i = startId; i < endId; ++i) {
			reindexer::Item item = db_->NewItem(ns);
			item[id] = i;
			item[data] = i % maxDataValue;
			err = db_->Upsert(ns, item);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
	};

	createNs(rightNs);
	fill(rightNs, 0, maxRightNsRowCount);
	for (size_t i = 0; i < leftNs.size(); ++i) {
		createNs(leftNs[i]);
		fill(leftNs[i], 0, maxLeftNsRowCount);
	}
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		std::vector<std::thread> threads;
		threads.reserve(leftNs.size());
		for (size_t i = 0; i < leftNs.size(); ++i) {
			threads.emplace_back([this, i, &state]() {
				Query q{leftNs[i]};
				q.InnerJoin(data, data, CondEq, Query(rightNs).Where(data, CondEq, rand() % maxDataValue));

				reindexer::QueryResults qres;
				reindexer::Error err = db_->Select(q, qres);
				if (!err.ok()) {
					state.SkipWithError(err.what());
				}
				checkNotEmpty(qres, state);
			});
		}
		for (auto& th : threads) {
			th.join();
		}
	}
}

template <typename Total>
void ApiTvSimple::Query4CondRangeDropCache(benchmark::State& state) {
	IndexCacheSetter cacheDropper(*db_);
	Query4CondRange<Total>(state);
}

void ApiTvSimple::query2CondIdSet(benchmark::State& state, const std::vector<std::vector<int>>& idsets) {
	unsigned counter = 0;
	const auto q = [&] {
		return Query(rightNs_)
			.Where("id", CondSet, idsets[counter++ % idsets.size()])
			.Where("field", CondGt, int(kTotalItemsMainJoinNs / 2))
			.Limit(20);
	};
	LowSelectivityItemsCounter itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

std::vector<Variant> ApiTvSimple::generateForcedSort(int minVal, int maxVal, unsigned int cnt) {
	std::vector<Variant> res;
	res.reserve(cnt);
	reindexer::fast_hash_set<int> uvals;
	assertrx(maxVal >= minVal);
	unsigned diff = maxVal - minVal;
	assertrx(diff > cnt);
	while (uvals.size() < cnt) {
		uvals.emplace(minVal + rand() % diff);
	}
	for (auto val : uvals) {
		res.emplace_back(val);
	}
	return res;
}

void ApiTvSimple::Query2CondIdSet10(benchmark::State& state) { query2CondIdSet(state, idsets_.at(10)); }
void ApiTvSimple::Query2CondIdSet100(benchmark::State& state) { query2CondIdSet(state, idsets_.at(100)); }
void ApiTvSimple::Query2CondIdSet500(benchmark::State& state) { query2CondIdSet(state, idsets_.at(500)); }
void ApiTvSimple::Query2CondIdSet2000(benchmark::State& state) { query2CondIdSet(state, idsets_.at(2000)); }
void ApiTvSimple::Query2CondIdSet20000(benchmark::State& state) { query2CondIdSet(state, idsets_.at(20000)); }
