#include "api_tv_simple.h"
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

static std::string randString(size_t size) {
	constexpr static std::string_view ch{"qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM"};
	std::string ret(size, ' ');
	for (size_t i = 0; i < size; ++i) ret[i] = ch[rand() % ch.size()];
	return ret;
}

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

template <size_t L>
reindexer::span<std::string> randStringArray() {
	static std::string ret[L];
	for (size_t i = 0; i < L; ++i) ret[i] = randString(L);
	return ret;
}

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

	Register("StringsSelect", &ApiTvSimple::StringsSelect, this);
	Register("GetByID", &ApiTvSimple::GetByID, this);
	Register("GetByIDInBrackets", &ApiTvSimple::GetByIDInBrackets, this);
	Register("GetEqInt", &ApiTvSimple::GetEqInt, this);
	Register("GetEqArrayInt", &ApiTvSimple::GetEqArrayInt, this);
	Register("GetEqString", &ApiTvSimple::GetEqString, this);
	Register("GetLikeString", &ApiTvSimple::GetLikeString, this);
	Register("GetByRangeIDAndSortByHash", &ApiTvSimple::GetByRangeIDAndSortByHash, this);
	Register("GetByRangeIDAndSortByTree", &ApiTvSimple::GetByRangeIDAndSortByTree, this);
	Register("GetUuid", &ApiTvSimple::GetUuid, this);
	Register("GetUuidStr", &ApiTvSimple::GetUuidStr, this);

	Register("Query1Cond", &ApiTvSimple::Query1Cond, this);
	Register("Query1CondTotal", &ApiTvSimple::Query1CondTotal, this);
	Register("Query1CondCachedTotal", &ApiTvSimple::Query1CondCachedTotal, this);
	Register("Query2Cond", &ApiTvSimple::Query2Cond, this);
	Register("Query2CondTotal", &ApiTvSimple::Query2CondTotal, this);
	Register("Query2CondCachedTotal", &ApiTvSimple::Query2CondCachedTotal, this);
	Register("Query2CondLeftJoin", &ApiTvSimple::Query2CondLeftJoin, this);
	Register("Query2CondLeftJoinTotal", &ApiTvSimple::Query2CondLeftJoinTotal, this);
	Register("Query2CondLeftJoinCachedTotal", &ApiTvSimple::Query2CondLeftJoinCachedTotal, this);
	Register("Query0CondInnerJoinUnlimit", &ApiTvSimple::Query0CondInnerJoinUnlimit, this)->Iterations(k0CondJoinIters);
	Register("Query0CondInnerJoinUnlimitLowSelectivity", &ApiTvSimple::Query0CondInnerJoinUnlimitLowSelectivity, this)
		->Iterations(k0CondJoinIters);
	Register("Query0CondInnerJoinPreResultStoreValues", &ApiTvSimple::Query0CondInnerJoinPreResultStoreValues, this)
		->Iterations(k0CondJoinIters);
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
	Register("Query4CondRange", &ApiTvSimple::Query4CondRange, this)->Iterations(kQuery4CondIters);
	Register("Query4CondRangeTotal", &ApiTvSimple::Query4CondRangeTotal, this)->Iterations(kQuery4CondIters);
	Register("Query4CondRangeCachedTotal", &ApiTvSimple::Query4CondRangeCachedTotal, this)->Iterations(kQuery4CondIters);
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	Register("Query2CondIdSet10", &ApiTvSimple::Query2CondIdSet10, this);
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
	Register("Query2CondIdSet100", &ApiTvSimple::Query2CondIdSet100, this);
	Register("Query2CondIdSet500", &ApiTvSimple::Query2CondIdSet500, this);
#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	Register("Query2CondIdSet2000", &ApiTvSimple::Query2CondIdSet2000, this);
	Register("Query2CondIdSet20000", &ApiTvSimple::Query2CondIdSet20000, this);
#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(RX_WITH_STDLIB_DEBUG)
	Register("FromCJSON", &ApiTvSimple::FromCJSON, this);
	Register("FromCJSONPKOnly", &ApiTvSimple::FromCJSONPKOnly, this);
	Register("GetCJSON", &ApiTvSimple::GetCJSON, this);
	Register("ExtractField", &ApiTvSimple::ExtractField, this);
	Register("SubQueryEq", &ApiTvSimple::SubQueryEq, this);
	Register("SubQuerySet", &ApiTvSimple::SubQuerySet, this);
	Register("SubQueryAggregate", &ApiTvSimple::SubQueryAggregate, this);

	// Those benches should be last, because they are recreating indexes cache
	Register("Query4CondRangeDropCache", &ApiTvSimple::Query4CondRangeDropCache, this)->Iterations(kQuery4CondIters);
	Register("Query4CondRangeDropCacheTotal", &ApiTvSimple::Query4CondRangeDropCacheTotal, this)->Iterations(kQuery4CondIters);
	Register("Query4CondRangeDropCacheCachedTotal", &ApiTvSimple::Query4CondRangeDropCacheCachedTotal, this)->Iterations(kQuery4CondIters);
	//  NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Error ApiTvSimple::Initialize() {
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

	countryLikePatterns_.reserve(countries_.size());
	for (const auto& country : countries_) {
		countryLikePatterns_.emplace_back(reindexer::makeLikePattern(country));
	}
	uuids_.reserve(1000);
	for (size_t i = 0; i < 1000; ++i) {
		uuids_.emplace_back(randStrUuid());
	}

	devices_ = {"iphone", "android", "smarttv", "stb", "ottstb"};
	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};

	for (int i = 0; i < 10; i++) packages_.emplace_back(randomNumArray<int>(20, 10000, 10));

	for (int i = 0; i < 20; i++) priceIDs_.emplace_back(randomNumArray<int>(10, 7000, 50));

	for (auto sz : idsetsSz_) {
		constexpr unsigned kTotalIdSets = 1000;
		auto& idsets = idsets_[sz];
		idsets.reserve(kTotalIdSets);
		for (unsigned i = 0; i < kTotalIdSets; ++i) {
			idsets.emplace_back(randomNumArray<int>(sz, 0, kTotalItemsMainJoinNs));
		}
	}

	start_times_.resize(20);
	for (int i = 0; i < 20; i++) start_times_[i] = random<int>(0, 50000);

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
	if (!err.ok()) return err;

	for (size_t i = 0; i < kTotalItemsStringSelectNs; ++i) {
		auto item = MakeStrItem();
		if (!item.Status().ok()) return item.Status();
		err = db_->Insert(stringSelectNs_, item);
		if (!err.ok()) return err;
	}
	err = db_->Commit(stringSelectNs_);
	if (!err.ok()) return err;

	NamespaceDef mainNsDef{mainNs_};
	mainNsDef.AddIndex("id", "hash", "int", IndexOpts().PK()).AddIndex("field", "hash", "int", IndexOpts());
	err = db_->AddNamespace(mainNsDef);
	if (!err.ok()) return err;
	NamespaceDef rightNsDef{rightNs_};
	rightNsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("field", "hash", "int", IndexOpts())
		.AddIndex("id_tree", "tree", "int", IndexOpts());
	err = db_->AddNamespace(rightNsDef);
	if (!err.ok()) return err;

	for (size_t i = 0; i < kTotalItemsMainJoinNs; ++i) {
		reindexer::Item mItem = db_->NewItem(mainNsDef.name);
		if (!mItem.Status().ok()) return mItem.Status();
		mItem.Unsafe();
		wrSer_.Reset();
		reindexer::JsonBuilder bld(wrSer_);
		bld.Put("id", i);
		bld.Put("field", i);
		bld.End();
		err = mItem.FromJSON(wrSer_.Slice());
		if (!err.ok()) return err;
		err = db_->Insert(mainNsDef.name, mItem);
		if (!err.ok()) return err;

		reindexer::Item rItem = db_->NewItem(rightNsDef.name);
		if (!rItem.Status().ok()) return rItem.Status();
		rItem.Unsafe();
		wrSer_.Reset();
		reindexer::JsonBuilder bld2(wrSer_);
		bld2.Put("id", i);
		bld2.Put("field", i);
		bld2.Put("id_tree", i);
		bld2.End();
		err = rItem.FromJSON(wrSer_.Slice());
		if (!err.ok()) return err;
		err = db_->Insert(rightNsDef.name, rItem);
		if (!err.ok()) return err;
	}

	err = prepareCJsonBench();
	if (!err.ok()) return err;

	return err;
}

reindexer::Error ApiTvSimple::prepareCJsonBench() {
	NamespaceDef cjsonNsDef{cjsonNsName_};
	cjsonNsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("bool_-_index", "-", "bool", IndexOpts())
		.AddIndex("int_-_index", "-", "int", IndexOpts())
		.AddIndex("int_hash_index", "hash", "int", IndexOpts())
		.AddIndex("int_tree_index", "tree", "int", IndexOpts())
		.AddIndex("int64_-_index", "-", "int64", IndexOpts())
		.AddIndex("int64_hash_index", "hash", "int64", IndexOpts())
		.AddIndex("int64_tree_index", "tree", "int64", IndexOpts())
		.AddIndex("double_-_index", "-", "double", IndexOpts())
		.AddIndex("double_tree_index", "tree", "double", IndexOpts())
		.AddIndex("string_-_index", "-", "string", IndexOpts())
		.AddIndex("string_hash_index", "hash", "string", IndexOpts())
		.AddIndex("string_tree_index", "tree", "string", IndexOpts())
		.AddIndex("string_text_index", "text", "string", IndexOpts())
		.AddIndex("bool_-_array_index", "-", "bool", IndexOpts().Array())
		.AddIndex("int_-_array_index", "-", "int", IndexOpts().Array())
		.AddIndex("int_hash_array_index", "hash", "int", IndexOpts().Array())
		.AddIndex("int_tree_array_index", "tree", "int", IndexOpts().Array())
		.AddIndex("int64_-_array_index", "-", "int64", IndexOpts().Array())
		.AddIndex("int64_hash_array_index", "hash", "int64", IndexOpts().Array())
		.AddIndex("int64_tree_array_index", "tree", "int64", IndexOpts().Array())
		.AddIndex("double_-_array_index", "-", "double", IndexOpts().Array())
		.AddIndex("double_tree_array_index", "tree", "double", IndexOpts().Array())
		.AddIndex("string_-_array_index", "-", "string", IndexOpts().Array())
		.AddIndex("string_hash_array_index", "hash", "string", IndexOpts().Array())
		.AddIndex("string_tree_array_index", "tree", "string", IndexOpts().Array());
	auto err = db_->AddNamespace(cjsonNsDef);
	if (!err.ok()) return err;

	fieldsToExtract_.clear();
	itemForCjsonBench_ = std::make_unique<reindexer::Item>(db_->NewItem(cjsonNsName_));
	if (!itemForCjsonBench_->Status().ok()) return itemForCjsonBench_->Status();
	wrSer_.Reset();
	reindexer::JsonBuilder bld(wrSer_);
	constexpr size_t len = 10;
	bld.Put("id", kCjsonBenchItemID);
	bld.Put("bool_-_index", rand() % 2);
	bld.Put("int_-_index", rand());
	bld.Put("int_hash_index", rand());
	bld.Put("int_tree_index", rand());
	bld.Put("int64_-_index", rand());
	bld.Put("int64_hash_index", rand());
	bld.Put("int64_tree_index", rand());
	bld.Put("double_-_index", rand() / double(rand() + 1));
	bld.Put("double_tree_index", rand() / double(rand() + 1));
	bld.Put("string_-_index", randString(len));
	bld.Put("string_hash_index", randString(len));
	bld.Put("string_tree_index", randString(len));
	bld.Put("string_text_index", randString(len));
	bld.Array("bool_-_array_index", randBoolArray<len>());
	bld.Array("int_-_array_index", randIntArray<len>());
	bld.Array("int_hash_array_index", randIntArray<len>());
	bld.Array("int_tree_array_index", randIntArray<len>());
	bld.Array("int64_-_array_index", randInt64Array<len>());
	bld.Array("int64_hash_array_index", randInt64Array<len>());
	bld.Array("int64_tree_array_index", randInt64Array<len>());
	bld.Array("double_-_array_index", randDoubleArray<len>());
	bld.Array("double_tree_array_index", randDoubleArray<len>());
	bld.Array("string_-_array_index", randStringArray<len>());
	bld.Array("string_hash_array_index", randStringArray<len>());
	bld.Array("string_tree_array_index", randStringArray<len>());
	for (size_t i = 0; i < 10; ++i) {
		const std::string i_str = std::to_string(i);
		fieldsToExtract_.emplace_back("bool_field_" + i_str);
		bld.Put("bool_field_" + i_str, rand() % 2);
		fieldsToExtract_.emplace_back("int_field_" + i_str);
		bld.Put("int_field_" + i_str, rand());
		fieldsToExtract_.emplace_back("double_field_" + i_str);
		bld.Put("double_field_" + i_str, rand() / double(rand() + 1));
		fieldsToExtract_.emplace_back("string_field_" + i_str);
		bld.Put("string_field_" + i_str, randString(len));
		bld.Array("bool_array_field_" + i_str, randBoolArray<len>());
		bld.Array("int_array_field_" + i_str, randIntArray<len>());
		bld.Array("double_array_field_" + i_str, randDoubleArray<len>());
		bld.Array("string_array_field_" + i_str, randStringArray<len>());
		{
			const std::string nestedBase("nested_obj_" + i_str);
			auto obj = bld.Object(nestedBase);
			obj.Put("bool_field", rand() % 2);
			fieldsToExtract_.emplace_back(nestedBase + ".bool_field");
			obj.Put("int_field", rand());
			fieldsToExtract_.emplace_back(nestedBase + ".int_field");
			obj.Put("double_field", rand() / double(rand() + 1));
			fieldsToExtract_.emplace_back(nestedBase + ".double_field");
			obj.Put("string_field", randString(len));
			fieldsToExtract_.emplace_back(nestedBase + ".string_field");
			obj.Array("bool_array_field", randBoolArray<len>());
			obj.Array("int_array_field", randIntArray<len>());
			obj.Array("double_array_field", randDoubleArray<len>());
			obj.Array("string_array_field", randStringArray<len>());
		}
		{
			auto arr = bld.Array("nested_arr_" + i_str);
			for (size_t j = 0; j < len; ++j) {
				auto obj = arr.Object();
				obj.Put("bool_field", rand() % 2);
				obj.Put("int_field", rand());
				obj.Put("double_field", rand() / double(rand() + 1));
				obj.Put("string_field", randString(len));
				obj.Array("bool_array_field", randBoolArray<len>());
				obj.Array("int_array_field", randIntArray<len>());
				obj.Array("double_array_field", randDoubleArray<len>());
				obj.Array("string_array_field", randStringArray<len>());
			}
		}
	}
	bld.End();
	err = itemForCjsonBench_->FromJSON(wrSer_.Slice());
	if (!err.ok()) return err;
	if (!itemForCjsonBench_->Status().ok()) return itemForCjsonBench_->Status();
	err = db_->Insert(cjsonNsName_, *itemForCjsonBench_);
	if (!err.ok()) return err;
	cjsonOfItem_ = itemForCjsonBench_->GetCJSON();
	err = db_->Commit(cjsonNsName_);
	if (!err.ok()) return err;

	return err;
}

reindexer::Item ApiTvSimple::MakeStrItem() {
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
		if (!err.ok()) assert(!item.Status().ok());
	}
	return item;
}

reindexer::Item ApiTvSimple::MakeItem(benchmark::State&) {
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
	item["uuid"] = reindexer::Uuid{uuids_[rand() % uuids_.size()]};
	item["uuid_str"] = uuids_[rand() % uuids_.size()];

	return item;
}

// FIXTURES

void ApiTvSimple::WarmUpIndexes(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		reindexer::Error err;

		// Ensure indexes complete build
		WaitForOptimization();
		for (size_t i = 0; i < packages_.size() * 2; i++) {
			QueryResults qres;
			Query q(nsdef_.name);
			q.Where("packages", CondSet, packages_.at(i % packages_.size())).Limit(20).Sort("start_time", false);
			err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}

		for (size_t i = 0; i < packages_.size() * 2; i++) {
			QueryResults qres;
			Query q(nsdef_.name);
			q.Where("packages", CondSet, packages_.at(i % packages_.size())).Limit(20).Sort("year", false);
			err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}

		for (size_t i = 0; i < priceIDs_.size() * 3; i++) {
			QueryResults qres;
			Query q("JoinItems");
			q.Where("id", CondSet, priceIDs_.at(i % priceIDs_.size())).Limit(20);
			err = db_->Select(q, qres);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}
	}
}

void ApiTvSimple::GetCJSON(benchmark::State& state) {
	assertrx(itemForCjsonBench_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		[[maybe_unused]] const auto ret = itemForCjsonBench_->GetCJSON();
	}
}

void ApiTvSimple::FromCJSON(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(cjsonNsName_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto err = item.FromCJSON(cjsonOfItem_);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!item.Status().ok()) state.SkipWithError(item.Status().what().c_str());
	}
}

void ApiTvSimple::FromCJSONPKOnly(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(cjsonNsName_);
	{
		AllocsTracker allocsTracker(state);
		for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
			const auto err = item.FromCJSON(cjsonOfItem_, true);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
			if (!item.Status().ok()) state.SkipWithError(item.Status().what().c_str());
		}
	}
	assertrx(item["id"].Get<int>() == kCjsonBenchItemID);
}

void ApiTvSimple::ExtractField(benchmark::State& state) {
	assertrx(itemForCjsonBench_);
	assertrx(fieldsToExtract_.size());
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto& fieldName = fieldsToExtract_[rand() % fieldsToExtract_.size()];
		const auto va = VariantArray((*itemForCjsonBench_)[fieldName]);
		if (va.size() != 1) state.SkipWithError(fmt::sprintf("Unexpected result size: %d", va.size()).c_str());
	}
}

void ApiTvSimple::StringsSelect(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(stringSelectNs_);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetByID(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("id", CondEq, random<int>(id_seq_->Start(), id_seq_->End()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetByIDInBrackets(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.OpenBracket().Where("id", CondEq, random<int>(id_seq_->Start(), id_seq_->End())).CloseBracket();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetEqInt(benchmark::State& state) {
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

void ApiTvSimple::GetUuid(benchmark::State& state) {
	reindexer::Uuid uuid{uuids_[rand() % uuids_.size()]};
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("uuid", CondEq, uuid);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvSimple::GetUuidStr(benchmark::State& state) {
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

void ApiTvSimple::GetEqArrayInt(benchmark::State& state) {
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

void ApiTvSimple::GetEqString(benchmark::State& state) {
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

void ApiTvSimple::GetLikeString(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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

void ApiTvSimple::GetByRangeIDAndSortByTree(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
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
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query1CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query1CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("year", CondGe, 2020).Limit(20).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondIdSet10(benchmark::State& state) { query2CondIdSet(state, idsets_.at(10)); }
void ApiTvSimple::Query2CondIdSet100(benchmark::State& state) { query2CondIdSet(state, idsets_.at(100)); }
void ApiTvSimple::Query2CondIdSet500(benchmark::State& state) { query2CondIdSet(state, idsets_.at(500)); }
void ApiTvSimple::Query2CondIdSet2000(benchmark::State& state) { query2CondIdSet(state, idsets_.at(2000)); }
void ApiTvSimple::Query2CondIdSet20000(benchmark::State& state) { query2CondIdSet(state, idsets_.at(20000)); }

void ApiTvSimple::Query2Cond(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20).CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondLeftJoin(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.LeftJoin("price_id", "id", CondSet, std::move(q4join));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondLeftJoinTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.LeftJoin("price_id", "id", CondSet, std::move(q4join))
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondLeftJoinCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.LeftJoin("price_id", "id", CondSet, std::move(q4join))
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query0CondInnerJoinUnlimit(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	size_t num = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join("JoinItems");
		Query q(nsdef_.name);
		q.ReqTotal().Limit(1);

		// Results depend on the joined query expected results size. If preselect is disabled, then
		// timing will be much higher. To stabilize benchmark result we are using all combinations of 'device' and 'location'
		size_t deviceId = num % devices_.size();
		size_t locationId = (num / devices_.size()) % locations_.size();

		q4join.Where("device", CondEq, devices_[deviceId]).Where("location", CondEq, locations_[locationId]);

		q.InnerJoin("price_id", "id", CondSet, std::move(q4join));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		++num;
	}
}

void ApiTvSimple::Query0CondInnerJoinUnlimitLowSelectivity(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join(rightNs_);
		q4join.Where("id", CondLe, 250);
		Query q(mainNs_);
		q.InnerJoin("id", "id", CondEq, std::move(q4join)).ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::SubQueryEq(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q = Query(mainNs_).Where(
			"id", CondEq, Query(rightNs_).Select({"field"}).Where("id", CondEq, VariantArray::Create(int(rand() % kTotalItemsMainJoinNs))));
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::SubQuerySet(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const int rangeMin = rand() % (kTotalItemsMainJoinNs - 500);
		Query q = Query(mainNs_).Where(
			"id", CondSet, Query(rightNs_).Select({"id"}).Where("id_tree", CondRange, VariantArray::Create(rangeMin, rangeMin + 500)));
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::SubQueryAggregate(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q = Query(mainNs_).Where("id", CondEq,
									   Query(rightNs_)
										   .Aggregate(AggAvg, {"id"})
										   .Where("id", CondLt, VariantArray::Create(int(rand() % kTotalItemsMainJoinNs)))
										   .Limit(500));
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondInnerJoin(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.InnerJoin("price_id", "id", CondSet, std::move(q4join));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query0CondInnerJoinPreResultStoreValues(benchmark::State& state) {
	using reindexer::JoinedSelector;
	static const std::string rightNs = "rightNs";
	static const std::vector<std::string> leftNs = {"leftNs1", "leftNs2", "leftNs3", "leftNs4"};
	static constexpr char const* id = "id";
	static constexpr char const* data = "data";
	static constexpr int maxDataValue = 10;
	static constexpr int maxRightNsRowCount = maxDataValue * (JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization() - 1);
	static constexpr int maxLeftNsRowCount = 10000;

	const auto createNs = [this, &state](const std::string& ns) {
		reindexer::Error err = db_->OpenNamespace(ns);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		err = db_->AddIndex(ns, {id, "hash", "int", IndexOpts().PK()});
		if (!err.ok()) state.SkipWithError(err.what().c_str());
		err = db_->AddIndex(ns, {data, "hash", "int", IndexOpts()});
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	};
	const auto fill = [this, &state](const std::string& ns, int startId, int endId) {
		reindexer::Error err;
		for (int i = startId; i < endId; ++i) {
			reindexer::Item item = db_->NewItem(ns);
			item[id] = i;
			item[data] = i % maxDataValue;
			err = db_->Upsert(ns, item);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
		}
		err = db_->Commit(ns);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	};

	createNs(rightNs);
	fill(rightNs, 0, maxRightNsRowCount);
	for (size_t i = 0; i < leftNs.size(); ++i) {
		createNs(leftNs[i]);
		fill(leftNs[i], 0, maxLeftNsRowCount);
	}
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		std::vector<std::thread> threads;
		threads.reserve(leftNs.size());
		for (size_t i = 0; i < leftNs.size(); ++i) {
			threads.emplace_back([this, i, &state]() {
				Query q{leftNs[i]};
				q.InnerJoin(data, data, CondEq, Query(rightNs).Where(data, CondEq, rand() % maxDataValue));
				QueryResults qres;
				reindexer::Error err = db_->Select(q, qres);
				if (!err.ok()) state.SkipWithError(err.what().c_str());
			});
		}
		for (auto& th : threads) th.join();
	}
}

void ApiTvSimple::Query2CondInnerJoinTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.InnerJoin("price_id", "id", CondSet, std::move(q4join))
			.ReqTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query2CondInnerJoinCachedTotal(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q4join("JoinItems");
		Query q(nsdef_.name);

		q4join.Where("device", CondEq, "ottstb").Where("location", CondSet, {"mos", "dv", "sib"});

		q.Limit(20)
			.Sort("year", false)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.InnerJoin("price_id", "id", CondSet, std::move(q4join))
			.CachedTotal();

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvSimple::Query3Cond(benchmark::State& state) {
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

void ApiTvSimple::Query3CondTotal(benchmark::State& state) {
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

void ApiTvSimple::Query3CondCachedTotal(benchmark::State& state) {
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

void ApiTvSimple::Query3CondKillIdsCache(benchmark::State& state) {
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

void ApiTvSimple::Query3CondRestoreIdsCache(benchmark::State& state) {
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

void ApiTvSimple::Query4Cond(benchmark::State& state) {
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

void ApiTvSimple::Query4CondTotal(benchmark::State& state) {
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

void ApiTvSimple::Query4CondCachedTotal(benchmark::State& state) {
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

void ApiTvSimple::Query4CondRange(benchmark::State& state) {
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

void ApiTvSimple::Query4CondRangeTotal(benchmark::State& state) {
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

void ApiTvSimple::Query4CondRangeCachedTotal(benchmark::State& state) {
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

void ApiTvSimple::Query4CondRangeDropCache(benchmark::State& state) {
	IndexCacheSetter cacheDropper(*db_);

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

void ApiTvSimple::Query4CondRangeDropCacheTotal(benchmark::State& state) {
	IndexCacheSetter cacheDropper(*db_);

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

void ApiTvSimple::Query4CondRangeDropCacheCachedTotal(benchmark::State& state) {
	IndexCacheSetter cacheDropper(*db_);

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

void ApiTvSimple::query2CondIdSet(benchmark::State& state, const std::vector<std::vector<int>>& idsets) {
	AllocsTracker allocsTracker(state);
	unsigned counter = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(rightNs_);
		q.Where("id", CondSet, idsets[counter++ % idsets.size()]).Where("field", CondGt, int(kTotalItemsMainJoinNs / 2)).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}
