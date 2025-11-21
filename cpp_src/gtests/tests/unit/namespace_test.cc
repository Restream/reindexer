#include <string_view>
#include <thread>
#include "core/cbinding/resultserializer.h"
#include "core/cjson/ctag.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"
#include "core/namespace/asyncstorage.h"
#include "core/system_ns_names.h"
#include "estl/fast_hash_set.h"
#include "gmock/gmock.h"
#include "gtests/tools.h"
#include "ns_api.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "tools/timetools.h"
#include "vendor/gason/gason.h"

using QueryResults = ReindexerApi::QueryResults;
using Item = ReindexerApi::Item;
using Reindexer = ReindexerApi::Reindexer;

TEST_F(NsApi, TupleColumnSize) {
	// Check, that -tuple index does not have column subindex
	constexpr auto kNoColumnIdx = "no_column_idx";
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"date", "-", "int64", IndexOpts(), 0},
											   IndexDeclaration{kNoColumnIdx, "-", "int64", IndexOpts().NoIndexColumn(), 0}});

	constexpr int kDataCount = 500;
	for (int i = 0; i < kDataCount; ++i) {
		rt.UpsertJSON(default_namespace, fmt::format(R"j({{"{}":{},"date":{},"{}":{}}})j", idIdxName, i, rand(), kNoColumnIdx, rand()));
	}

	auto memstats = getMemStat(*rt.reindexer, default_namespace);
	const VariantArray sizes(memstats["indexes.column_size"]);
	const VariantArray names(memstats["indexes.name"]);
	[[maybe_unused]] constexpr int kMaxEmptyColumnSize = 32;  // Platform dependant
#ifndef REINDEX_DEBUG_CONTAINERS
	ASSERT_EQ(sizes.size(), 4);
	EXPECT_LE(sizes[0].As<int>(), kMaxEmptyColumnSize);
	EXPECT_LE(sizes[3].As<int>(), kMaxEmptyColumnSize);
	EXPECT_GT(sizes[1].As<int>(), kDataCount);
	EXPECT_GT(sizes[2].As<int>(), kDataCount);
#endif	// REINDEX_DEBUG_CONTAINERS
	ASSERT_EQ(names.size(), 4);
	EXPECT_EQ(names[0].As<std::string>(), "-tuple");
	EXPECT_EQ(names[1].As<std::string>(), idIdxName);
	EXPECT_EQ(names[2].As<std::string>(), "date");
	EXPECT_EQ(names[3].As<std::string>(), kNoColumnIdx);
}

TEST_F(NsApi, IndexDrop) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(
		default_namespace,
		{IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"date", "", "int64", IndexOpts(), 0},
		 IndexDeclaration{"price", "", "int64", IndexOpts(), 0}, IndexDeclaration{"serialNumber", "", "int64", IndexOpts(), 0},
		 IndexDeclaration{"fileName", "", "string", IndexOpts(), 0}});
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{"ft11", "text", "string", IndexOpts(), 0},
											   IndexDeclaration{"ft12", "text", "string", IndexOpts(), 0},
											   IndexDeclaration{"ft11+ft12=ft13", "text", "composite", IndexOpts(), 0}});
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{"ft21", "text", "string", IndexOpts(), 0},
											   IndexDeclaration{"ft22", "text", "string", IndexOpts(), 0},
											   IndexDeclaration{"ft23", "text", "string", IndexOpts(), 0},
											   IndexDeclaration{"ft21+ft22+ft23=ft24", "text", "composite", IndexOpts(), 0}});

	for (int i = 0; i < 1000; ++i) {
		Item item = NewItem(default_namespace);
		item[idIdxName] = i;
		item["date"] = rand();
		item["price"] = rand();
		item["serialNumber"] = i * 100;
		item["fileName"] = "File" + std::to_string(i);
		item["ft11"] = RandString();
		item["ft12"] = RandString();
		item["ft21"] = RandString();
		item["ft22"] = RandString();
		item["ft23"] = RandString();
		rt.Insert(default_namespace, item);
	}

	rt.DropIndex(default_namespace, "price");
}

TEST_F(NsApi, AddTooManyIndexes) {
	constexpr size_t kHalfOfStartNotCompositeIndexesCount = 80;
	constexpr size_t kMaxCompositeIndexesCount = 100;
	static constexpr std::string_view ns = "too_many_indexes";
	rt.OpenNamespace(ns);

	size_t notCompositeIndexesCount = 0;
	size_t compositeIndexesCount = 0;
	while (notCompositeIndexesCount < kMaxIndexes - 1) {
		if (notCompositeIndexesCount < 2 * kHalfOfStartNotCompositeIndexesCount || rand() % 4 != 0 ||
			compositeIndexesCount >= kMaxCompositeIndexesCount) {
			const std::string indexName = "index_" + std::to_string(notCompositeIndexesCount);
			rt.AddIndex(ns, reindexer::IndexDef{indexName, {indexName}, "tree", "int", IndexOpts{}});
			++notCompositeIndexesCount;
		} else {
			const std::string firstSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount);
			const std::string secondSubIndex =
				"index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount + kHalfOfStartNotCompositeIndexesCount);
			const std::string indexName = std::string(firstSubIndex).append("+").append(secondSubIndex);
			rt.AddIndex(ns, reindexer::IndexDef{indexName, {firstSubIndex, secondSubIndex}, "tree", "composite", IndexOpts{}});
			++compositeIndexesCount;
		}
	}
	// Add composite index
	std::string firstSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount);
	std::string secondSubIndex =
		"index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount + kHalfOfStartNotCompositeIndexesCount);
	std::string indexName = std::string(firstSubIndex).append("+").append(secondSubIndex);
	rt.AddIndex(ns, reindexer::IndexDef{indexName, {firstSubIndex, secondSubIndex}, "tree", "composite", IndexOpts{}});

	// Add non-composite index
	indexName = "index_" + std::to_string(notCompositeIndexesCount);
	auto err = rt.reindexer->AddIndex(ns, reindexer::IndexDef{indexName, {indexName}, "tree", "int", IndexOpts{}});
	ASSERT_FALSE(err.ok());
	ASSERT_STREQ(
		err.what(),
		"Cannot add index 'too_many_indexes.index_255'. Too many non-composite indexes. 255 non-composite indexes are allowed only");

	// Add composite index
	firstSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount);
	secondSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount + kHalfOfStartNotCompositeIndexesCount);
	indexName = std::string(firstSubIndex).append("+").append(secondSubIndex);
	rt.AddIndex(ns, reindexer::IndexDef{indexName, {firstSubIndex, secondSubIndex}, "tree", "composite", IndexOpts{}});
}

TEST_F(NsApi, TruncateNamespace) {
	TruncateNamespace([&](const std::string& nsName) { return rt.reindexer->TruncateNamespace(nsName); });
	TruncateNamespace([&](const std::string& nsName) {
		QueryResults qr;
		return rt.reindexer->ExecSQL("TRUNCATE " + nsName, qr);
	});
}

TEST_F(NsApi, UpsertWithPrecepts) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{updatedTimeSecFieldName, "", "int64", IndexOpts(), 0},
											   IndexDeclaration{updatedTimeMSecFieldName, "", "int64", IndexOpts(), 0},
											   IndexDeclaration{updatedTimeUSecFieldName, "", "int64", IndexOpts(), 0},
											   IndexDeclaration{updatedTimeNSecFieldName, "", "int64", IndexOpts(), 0},
											   IndexDeclaration{serialFieldName, "", "int64", IndexOpts(), 0},
											   IndexDeclaration{stringField, "text", "string", IndexOpts(), 0}});

	Item item = NewItem(default_namespace);
	item[idIdxName] = idNum;

	{
		// Set precepts
		std::vector<std::string> precepts = {updatedTimeSecFieldName + "=NOW()",	  updatedTimeMSecFieldName + "=NOW(msec)",
											 updatedTimeUSecFieldName + "=NOW(usec)", updatedTimeNSecFieldName + "=NOW(nsec)",
											 serialFieldName + "=SERIAL()",			  stringField + "=SERIAL()"};
		item.SetPrecepts(std::move(precepts));
	}

	// Upsert item a few times
	for (int i = 0; i < upsertTimes; i++) {
		rt.Upsert(default_namespace, item);
	}

	// Get item
	auto res = rt.ExecSQL("SELECT * FROM " + default_namespace + " WHERE id=" + std::to_string(idNum));
	for (auto it : res) {
		item = it.GetItem(false);
		for (auto idx = 1; idx < item.NumFields(); idx++) {
			auto field = item[idx].Name();

			if (field == updatedTimeSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_LE(reindexer::getTimeNow(reindexer::TimeUnit::sec) - value, 1)
					<< "Precept function `now()/now(sec)` doesn't work properly";
			} else if (field == updatedTimeMSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_LT(reindexer::getTimeNow(reindexer::TimeUnit::msec) - value, 1000)
					<< "Precept function `now(msec)` doesn't work properly";
			} else if (field == updatedTimeUSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_LT(reindexer::getTimeNow(reindexer::TimeUnit::usec) - value, 1000000)
					<< "Precept function `now(usec)` doesn't work properly";
			} else if (field == updatedTimeNSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_LT(reindexer::getTimeNow(reindexer::TimeUnit::nsec) - value, 1000000000)
					<< "Precept function `now(nsec)` doesn't work properly";
			} else if (field == serialFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_EQ(value, upsertTimes) << "Precept function `serial()` didn't increment a value";
			} else if (field == stringField) {
				auto value = item[field].Get<std::string_view>();
				ASSERT_EQ(value, std::to_string(upsertTimes)) << "Precept function `serial()` didn't increment a value";
			}
		}
	}
}

TEST_F(NsApi, ReturnOfItemChange) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{updatedTimeNSecFieldName, "", "int64", IndexOpts(), 0},
											   IndexDeclaration{serialFieldName, "", "int64", IndexOpts(), 0}});

	Item item = NewItem(default_namespace);
	item[idIdxName] = idNum;

	{
		// Set precepts
		std::vector<std::string> precepts = {updatedTimeNSecFieldName + "=NOW(nsec)", serialFieldName + "=SERIAL()"};
		item.SetPrecepts(std::move(precepts));
	}

	// Check Insert
	rt.Insert(default_namespace, item);
	auto res1 = rt.ExecSQL("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum));
	ASSERT_EQ(res1.Count(), 1);
	Item selectedItem(res1.begin().GetItem(false));
	CheckItemsEqual(item, selectedItem);

	// Check Update
	rt.Update(default_namespace, item);
	auto res2 = rt.ExecSQL("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum));
	ASSERT_EQ(res2.Count(), 1);
	selectedItem = res2.begin().GetItem(false);
	CheckItemsEqual(item, selectedItem);

	// Check Delete
	rt.Delete(default_namespace, item);
	CheckItemsEqual(item, selectedItem);

	// Check Upsert
	item[idIdxName] = idNum;
	rt.Upsert(default_namespace, item);
	auto res3 = rt.ExecSQL("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum));
	ASSERT_EQ(res3.Count(), 1);
	selectedItem = res3.begin().GetItem(false);
	CheckItemsEqual(item, selectedItem);
}

TEST_F(NsApi, UpdateIndex) try {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0}});

	[[maybe_unused]] reindexer::IndexDef wrongIndexDef("");
	EXPECT_THROW(wrongIndexDef = reindexer::IndexDef(idIdxName, reindexer::JsonPaths{"wrongPath"}, "hash", "double", IndexOpts().PK()),
				 Error);
	try {
		std::ignore = reindexer::IndexDef(idIdxName, reindexer::JsonPaths{"wrongPath"}, "hash", "double", IndexOpts().PK());
	} catch (const Error& err) {
		EXPECT_STREQ(err.what(), "Unsupported combination of field 'id' type 'double' and index type 'hash'");
	}

	auto newIdx = reindexer::IndexDef(idIdxName, "tree", "int64", IndexOpts().PK().Dense());
	rt.UpdateIndex(default_namespace, newIdx);

	auto nsDefs = rt.EnumNamespaces(reindexer::EnumNamespacesOpts());
	auto nsDefIt =
		std::find_if(nsDefs.begin(), nsDefs.end(), [&](const reindexer::NamespaceDef& nsDef) { return nsDef.name == default_namespace; });

	ASSERT_TRUE(nsDefIt != nsDefs.end()) << "Namespace " + default_namespace + " is not found";

	auto& indexes = nsDefIt->indexes;
	auto receivedIdx =
		std::find_if(indexes.begin(), indexes.end(), [&](const reindexer::IndexDef& idx) { return idx.Name() == idIdxName; });
	ASSERT_TRUE(receivedIdx != indexes.end()) << "Expect index was created, but it wasn't";

	reindexer::WrSerializer newIdxSer;
	newIdx.GetJSON(newIdxSer);

	reindexer::WrSerializer receivedIdxSer;
	receivedIdx->GetJSON(receivedIdxSer);

	auto newIdxJson = newIdxSer.Slice();
	auto receivedIdxJson = receivedIdxSer.Slice();

	ASSERT_EQ(newIdxJson, receivedIdxJson);
}
CATCH_AND_ASSERT

TEST_F(NsApi, QueryperfstatsNsDummyTest) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0}});

	Item item = NewItem(reindexer::kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	constexpr std::string_view newConfig = R"json({
                        "type":"profiling",
                        "profiling":{
                            "queriesperfstats":true,
                            "queries_threshold_us":0,
                            "perfstats":true,
                            "memstats":true,
                            "long_queries_logging":{
                                "select":{
                                    "threshold_us": 1000000,
                                    "normalized": false
                                },
                                "update_delete":{
                                    "threshold_us": 1000000,
                                    "normalized": false
                                },
                                "transaction":{
                                    "threshold_us": 1000000,
                                    "avg_step_threshold_us": 1000
                                }
                            }
                        }
                    })json";
	rt.UpsertJSON(reindexer::kConfigNamespace, newConfig);

	struct [[nodiscard]] QueryPerformance {
		std::string query;
		double latencyStddev = 0;
		int64_t minLatencyUs = 0;
		int64_t maxLatencyUs = 0;
		void Dump() const {
			std::cout << "stddev: " << latencyStddev << std::endl;
			std::cout << "min: " << minLatencyUs << std::endl;
			std::cout << "max: " << maxLatencyUs << std::endl;
		}
	};

	Query testQuery = Query(default_namespace, 0, 0, ModeAccurateTotal);
	const std::string querySql(testQuery.GetSQL(true));

	auto performSimpleQuery = [&]() { auto qr = rt.Select(testQuery); };

	auto getPerformanceParams = [&](QueryPerformance& performanceRes) {
		auto qres = rt.Select(Query(reindexer::kQueriesPerfStatsNamespace).Where("query", CondEq, Variant(querySql)));
		if (qres.Count() == 0) {
			auto qr = rt.Select(Query(reindexer::kQueriesPerfStatsNamespace));
			ASSERT_GT(qr.Count(), 0) << "#queriesperfstats table is empty!";
			for (auto& it : qr) {
				std::cout << it.GetItem(false).GetJSON() << std::endl;
			}
		}
		ASSERT_EQ(qres.Count(), 1);
		Item item = qres.begin().GetItem(false);
		performanceRes.latencyStddev = item["latency_stddev"].As<double>();
		performanceRes.minLatencyUs = item["min_latency_us"].As<int64_t>();
		performanceRes.maxLatencyUs = item["max_latency_us"].As<int64_t>();
		performanceRes.query = item["query"].As<std::string>();
	};

	sleep(1);

	QueryPerformance prevQperf;
	for (size_t i = 0; i < 1000; ++i) {
		performSimpleQuery();
		QueryPerformance qperf;
		getPerformanceParams(qperf);
		if ((qperf.minLatencyUs > qperf.maxLatencyUs) || (qperf.latencyStddev < 0) || (qperf.latencyStddev > qperf.maxLatencyUs)) {
			qperf.Dump();
		}
		ASSERT_TRUE(qperf.minLatencyUs <= qperf.maxLatencyUs);
		ASSERT_TRUE((qperf.latencyStddev >= 0) && (qperf.latencyStddev <= qperf.maxLatencyUs));
		if (i > 0) {
			ASSERT_TRUE(qperf.minLatencyUs <= prevQperf.minLatencyUs);
			ASSERT_TRUE(qperf.maxLatencyUs >= prevQperf.maxLatencyUs);
		}
		ASSERT_TRUE(qperf.query == "SELECT COUNT(*) FROM test_namespace") << qperf.query;
		prevQperf = qperf;
	}
}

static void checkIfItemJSONValid(QueryResults::Iterator& it, bool print = false) {
	reindexer::WrSerializer wrser;
	Error err = it.GetJSON(wrser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	if (err.ok() && print) {
		std::cout << wrser.Slice() << std::endl;
	}
}

TEST_F(NsApi, TestUpdateIndexedField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	const Query updateQuery{Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500))).Set(stringField, "bingo!")};
	rt.Update(updateQuery);

	const auto qrAll = rt.Select(Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500))));
	ASSERT_GT(qrAll.Count(), 0);
	for (auto it : qrAll) {
		Item item = it.GetItem(false);
		Variant val = item[stringField];
		ASSERT_TRUE(val.Type().Is<reindexer::KeyValueType::String>());
		ASSERT_TRUE(val.As<std::string>() == "bingo!") << val.As<std::string>();
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, TestUpdateNonindexedField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	const Query updateQuery{Query(default_namespace).Where("id", CondGe, Variant("1500")).Set("nested.bonus", static_cast<int>(100500))};
	auto qrUpdate = rt.UpdateQR(updateQuery);
	ASSERT_EQ(qrUpdate.Count(), 500);

	const auto qrAll = rt.Select(Query(default_namespace).Where("id", CondGe, Variant("1500")));
	ASSERT_EQ(qrAll.Count(), 500);
	for (auto it : qrAll) {
		Item item = it.GetItem(false);
		Variant val = item["nested.bonus"];
		ASSERT_TRUE(val.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(val.As<int64_t>() == 100500);
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, TestUpdateSparseField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	const Query updateQuery{Query(default_namespace).Where("id", CondGe, Variant("1500")).Set("sparse_field", static_cast<int>(100500))};
	auto qrUpdate = rt.UpdateQR(updateQuery);
	ASSERT_EQ(qrUpdate.Count(), 500);

	const auto qrAll = rt.Select(Query(default_namespace).Where("id", CondGe, Variant("1500")));
	ASSERT_EQ(qrAll.Count(), 500);
	for (auto it : qrAll) {
		Item item = it.GetItem(false);
		Variant val = item["sparse_field"];
		ASSERT_TRUE(val.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(val.As<int>() == 100500);
		checkIfItemJSONValid(it);
	}
}

// Test of the curious case: https://github.com/restream/reindexer/-/issues/697
// Updating entire object field and some indexed field at once.
TEST_F(NsApi, TestUpdateTwoFields) {
	// Set and fill Database
	DefineDefaultNamespace();
	FillDefaultNamespace();

	// Try to update 2 fields at once: indexed field 'stringField'
	// + adding and setting a new object-field called 'very_nested'
	const Query updateQuery = Query(default_namespace)
								  .Where(idIdxName, CondEq, 1)
								  .Set(stringField, "Bingo!")
								  .SetObject("very_nested", R"({"id":111, "name":"successfully updated!"})");
	const auto qrUpdate = rt.UpdateQR(updateQuery);
	ASSERT_EQ(qrUpdate.Count(), 1);
	// Make sure:
	// 1. JSON of the item is correct
	// 2. every new set&updated field has a correct value
	for (auto it : qrUpdate) {
		checkIfItemJSONValid(it);

		Item item = it.GetItem(false);
		Variant strField = item[stringField];
		EXPECT_TRUE(strField.Type().Is<reindexer::KeyValueType::String>());
		EXPECT_TRUE(strField.As<std::string>() == "Bingo!");
		EXPECT_TRUE(item["very_nested.id"].As<int>() == 111);
		EXPECT_TRUE(item["very_nested.name"].As<std::string>() == "successfully updated!");
	}
}

TEST_F(NsApi, TestUpdateNewFieldCheckTmVersion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	auto check = [this](const Query& query, int tmVersion) {
		auto qrUpdate = rt.UpdateQR(query);
		ASSERT_EQ(qrUpdate.Count(), 1);
		ASSERT_EQ(qrUpdate.GetTagsMatcher(0).version(), tmVersion);
	};

	auto qr = rt.Select(Query(default_namespace).Where(idIdxName, CondEq, 1));
	ASSERT_EQ(qr.Count(), 1);
	auto tmVersion = qr.GetTagsMatcher(0).version();
	Query updateQuery = Query(default_namespace).Where(idIdxName, CondEq, 1).Set("some_new_field", "some_value");

	// Make sure the version increases by 1 when one new tag with non-object content is added
	check(updateQuery, ++tmVersion);

	// Make sure the version not change when the same update query applied
	check(updateQuery, tmVersion);

	updateQuery.SetObject("new_obj_field", R"({"id":111, "name":"successfully updated!"})");

	// Make sure that tm version updates correctly when new tags are added:
	// +1 by tag very_nested,
	// +1 by all new tags processed in ItemModifier::modifyCJSON for SetObject-method
	// version was not changed by the merge of the two compatible tagsmatchers
	check(updateQuery, tmVersion += 2);

	// Make sure that if no new tags were added to the tagsmatcher during the update,
	// then the version of the tagsmatcher will not change
	check(updateQuery, tmVersion);
}

static void checkUpdateArrayFieldResults(std::string_view updateFieldPath, const QueryResults& results, const VariantArray& values) {
	for (auto it : results) {
		Item item = it.GetItem(false);
		VariantArray val = item[updateFieldPath];
		if (values.empty()) {
			ASSERT_EQ(val.size(), 1);
			ASSERT_TRUE(val.IsNullValue()) << (val.empty() ? "<empty>" : val.front().Type().Name());
		} else {
			EXPECT_EQ(val.size(), values.size());
			if (val != values) {
				std::cerr << "val:\n";
				val.Dump(std::cout);
				std::cerr << std::endl;
				std::cerr << "values:\n";
				values.Dump(std::cout);
				std::cerr << std::endl;
				GTEST_FATAL_FAILURE_("");
			}
		}
		checkIfItemJSONValid(it);
	}
}

static void updateArrayField(const std::shared_ptr<reindexer::Reindexer>& reindexer, const std::string& ns,
							 std::string_view updateFieldPath, const VariantArray& values) {
	QueryResults qrUpdate;
	{
		SCOPED_TRACE("Checking array update");
		Query updateQuery{Query(ns).Where("id", CondGe, Variant("500")).Set(updateFieldPath, values)};
		Error err = reindexer->Update(updateQuery, qrUpdate);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_GT(qrUpdate.Count(), 0);
		SCOPED_TRACE("Checking array update results");
		checkUpdateArrayFieldResults(updateFieldPath, qrUpdate, values);
	}
	{
		SCOPED_TRACE("Checking selection after array update");
		QueryResults qrAll;
		Error err = reindexer->Select(Query(ns).Where("id", CondGe, Variant("500")), qrAll);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qrAll.Count(), qrUpdate.Count());
		checkUpdateArrayFieldResults(updateFieldPath, qrAll, values);
	}
}

TEST_F(NsApi, TestUpdateNonindexedArrayField) {
	DefineDefaultNamespace();
	AddUnindexedData();
	updateArrayField(rt.reindexer, default_namespace, "array_field", {});
	updateArrayField(rt.reindexer, default_namespace, "array_field",
					 {Variant(static_cast<int64_t>(3)), Variant(static_cast<int64_t>(4)), Variant(static_cast<int64_t>(5)),
					  Variant(static_cast<int64_t>(6))});
}

TEST_F(NsApi, TestUpdateNonindexedArrayField2) {
	DefineDefaultNamespace();
	AddUnindexedData();

	auto qr = rt.ExecSQL(R"(update test_namespace set nested.bonus=[{"first":1,"second":2,"third":3}] where id = 1000;)");
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr.begin().GetItem(false);
	std::string_view json = item.GetJSON();
	size_t pos = json.find(R"("nested":{"bonus":[{"first":1,"second":2,"third":3}])");
	ASSERT_TRUE(pos != std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField3) {
	DefineDefaultNamespace();
	AddUnindexedData();

	auto qr = rt.ExecSQL(R"(update test_namespace set nested.bonus=[{"id":1},{"id":2},{"id":3},{"id":4}] where id = 1000;)");
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr.begin().GetItem(false);
	ASSERT_EQ(VariantArray(item["nested.bonus"]).size(), 4);

	size_t length = 0;
	std::string_view json = item.GetJSON();
	gason::JsonParser jsonParser;
	ASSERT_NO_THROW(jsonParser.Parse(json, &length));
	ASSERT_GT(length, 0);

	size_t pos = json.find(R"("nested":{"bonus":[{"id":1},{"id":2},{"id":3},{"id":4}])");
	ASSERT_TRUE(pos != std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField4) {
	DefineDefaultNamespace();
	AddUnindexedData();

	auto qr = rt.ExecSQL(R"(update test_namespace set nested.bonus=[0] where id = 1000;)");
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr.begin().GetItem(false);
	std::string_view json = item.GetJSON();
	size_t pos = json.find(R"("nested":{"bonus":[0])");
	ASSERT_NE(pos, std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField5) {
	DefineDefaultNamespace();
	AddUnindexedData();
	updateArrayField(rt.reindexer, default_namespace, "string_array", {});
	updateArrayField(rt.reindexer, default_namespace, "string_array",
					 VariantArray::Create({std::string("one"), std::string("two"), std::string("three"), std::string("four")}));
	updateArrayField(rt.reindexer, default_namespace, "string_array", {Variant(std::string("single one"))});
}

TEST_F(NsApi, TestUpdateIndexedArrayField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();
	updateArrayField(rt.reindexer, default_namespace, indexedArrayField, VariantArray::Create({7, 8, 9, 10, 11, 12, 13}));
}

TEST_F(NsApi, TestUpdateIndexedArrayField2) {
	DefineDefaultNamespace();
	AddUnindexedData();

	VariantArray value;
	value.emplace_back(static_cast<int>(77));
	Query q{Query(default_namespace).Where(idIdxName, CondEq, static_cast<int>(1000)).Set(indexedArrayField, std::move(value.MarkArray()))};
	auto qr = rt.UpdateQR(q);
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr.begin().GetItem(false);
	std::string_view json = item.GetJSON();
	size_t pos = json.find(R"("indexed_array_field":[77])");
	ASSERT_NE(pos, std::string::npos) << "'indexed_array_field' was not updated properly" << json;
}

static void addAndSetNonindexedField(const std::shared_ptr<reindexer::Reindexer>& reindexer, const std::string& ns,
									 const std::string& updateFieldPath) {
	QueryResults qrUpdate;
	Query updateQuery{Query(ns).Where("nested.bonus", CondGe, Variant(500)).Set(updateFieldPath, static_cast<int64_t>(777))};
	Error err = reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qrAll;
	err = reindexer->Select(Query(ns).Where("nested.bonus", CondGe, Variant(500)), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

	for (QueryResults::Iterator it : qrAll) {
		Item item = it.GetItem(false);
		Variant val = item[updateFieldPath.c_str()];
		ASSERT_TRUE(val.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(val.As<int64_t>(), 777);
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, TestAddAndSetNonindexedField) {
	DefineDefaultNamespace();
	AddUnindexedData();
	addAndSetNonindexedField(rt.reindexer, default_namespace, "nested3.extrabonus");
}

TEST_F(NsApi, TestAddAndSetNonindexedField2) {
	DefineDefaultNamespace();
	AddUnindexedData();
	addAndSetNonindexedField(rt.reindexer, default_namespace, "nested2.nested3.extrabonus");
}

TEST_F(NsApi, TestAddAndSetNonindexedField3) {
	DefineDefaultNamespace();
	AddUnindexedData();
	addAndSetNonindexedField(rt.reindexer, default_namespace, "nested3.nested4.extrabonus");
}

static void setAndCheckArrayItem(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, std::string_view fullItemPath,
								 std::string_view jsonPath, std::string_view description, int i = IndexValueType::NotSet,
								 int j = IndexValueType::NotSet) {
	SCOPED_TRACE(description);

	// Set array item to 777
	QueryResults qrUpdate;
	Query updateQuery{Query(ns).Where("nested.bonus", CondGe, Variant(500)).Set(fullItemPath, static_cast<int64_t>(777))};
	Error err = reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	// Get all items for the same query
	QueryResults qrAll;
	err = reindexer->Select(Query(ns).Where("nested.bonus", CondGe, Variant(500)), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

	const int kPricesSize = 3;

	// Check if array item with appropriate index equals to 777 and
	// is a type of Int64.
	auto checkItem = [](const VariantArray& values, size_t index, std::string_view description) {
		SCOPED_TRACE(description);
		ASSERT_LT(index, values.size());
		ASSERT_TRUE(values[index].Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(values[index].As<int64_t>(), 777);
	};

	// Check every item according to it's index, where i is the index of parent's array
	// and j is the index of a nested array:
	// 1) objects[1].prices[0]: i = 1, j = 0
	// 2) objects[2].prices[*]: i = 2, j = IndexValueType::NotSet
	// etc.
	for (QueryResults::Iterator it : qrAll) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item[jsonPath];
		if (i == j && i == IndexValueType::NotSet) {
			for (size_t k = 0; k < values.size(); ++k) {
				checkItem(values, k, description);
			}
		} else if (i == IndexValueType::NotSet) {
			for (int k = 0; k < kPricesSize; ++k) {
				checkItem(values, k * kPricesSize + j, description);
			}
		} else if (j == IndexValueType::NotSet) {
			for (int k = 0; k < kPricesSize; ++k) {
				checkItem(values, i * kPricesSize + k, description);
			}
		} else {
			checkItem(values, i * kPricesSize + j, description);
		}
	}
}

TEST_F(NsApi, TestAddAndSetArrayField) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Set array item(s) value to 777 and check if it was set properly
	DefineDefaultNamespace();
	AddUnindexedData();
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[0].prices[2]", "nested.nested_array.prices",
						 "TestAddAndSetArrayField 1 ", 0, 2);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[2].nested.array[1]", "nested.nested_array.nested.array",
						 "TestAddAndSetArrayField 2 ", 0, 1);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[2].nested.array[*]", "nested.nested_array.nested.array",
						 "TestAddAndSetArrayField 3 ", 0);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[1].prices[*]", "nested.nested_array.prices",
						 "TestAddAndSetArrayField 4 ", 1);
}

TEST_F(NsApi, TestAddAndSetArrayField2) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Set array item(s) value to 777 and check if it was set properly
	DefineDefaultNamespace();
	AddUnindexedData();
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[*].prices[0]", "nested.nested_array.prices",
						 "TestAddAndSetArrayField2 1 ", IndexValueType::NotSet, 0);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[*].name", "nested.nested_array.name",
						 "TestAddAndSetArrayField2 2 ");
}

TEST_F(NsApi, TestAddAndSetArrayField3) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set array item(s) value to 777 and check if it was set properly
	Query updateQuery{
		Query(default_namespace).Where("nested.bonus", CondGe, Variant(500)).Set("indexed_array_field[0]", static_cast<int>(777))};
	const auto qrUpdate = rt.UpdateQR(updateQuery);
	ASSERT_GT(qrUpdate.Count(), 0);

	// 4. Make sure each item's indexed_array_field[0] is of type Int and equal to 777
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item[indexedArrayField];
		ASSERT_TRUE(values[0].Type().Is<reindexer::KeyValueType::Int>());
		ASSERT_EQ(values[0].As<int>(), 777);
	}
}

TEST_F(NsApi, TestAddAndSetArrayField4) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set array item(s) value to 777 and check if it was set properly
	Query updateQuery{
		Query(default_namespace).Where("nested.bonus", CondGe, Variant(500)).Set("indexed_array_field[*]", static_cast<int>(777))};
	const auto qrUpdate = rt.UpdateQR(updateQuery);
	ASSERT_GT(qrUpdate.Count(), 0);

	// 4. Make sure all items of indexed_array_field are of type Int and set to 777
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item[indexedArrayField];
		ASSERT_EQ(values.size(), 9);
		for (size_t i = 0; i < values.size(); ++i) {
			ASSERT_TRUE(values[i].Type().Is<reindexer::KeyValueType::Int>());
			ASSERT_EQ(values[i].As<int>(), 777);
		}
	}
}

static void dropArrayItem(const std::shared_ptr<reindexer::Reindexer>& reindexer, const std::string& ns, const std::string& fullItemPath,
						  const std::string& jsonPath, int i = IndexValueType::NotSet, int j = IndexValueType::NotSet) {
	// Drop item(s) with name = fullItemPath
	QueryResults qrUpdate;
	Query updateQuery{Query(ns).Where("nested.bonus", CondGe, Variant(500)).Drop(fullItemPath)};
	Error err = reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	// Get all items of the same query
	QueryResults qrAll;
	err = reindexer->Select(Query(ns).Where("nested.bonus", CondGe, Variant(500)), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

	const int kPricesSize = 3;

	// Check every item according to it's index, where i is the index of parent's array
	// and j is the index of a nested array:
	// 1) objects[1].prices[0]: i = 1, j = 0
	// 2) objects[2].prices[*]: i = 2, j = IndexValueType::NotSet
	// etc.
	// Approach is to check array size (because after removing some of its items
	// it should decrease).
	for (QueryResults::Iterator it : qrAll) {
		checkIfItemJSONValid(it);
		Item item = it.GetItem(false);
		VariantArray values = item[jsonPath];
		if (i == IndexValueType::NotSet && j == IndexValueType::NotSet) {
			ASSERT_EQ(values.size(), 0);
		} else if (i == IndexValueType::NotSet || j == IndexValueType::NotSet) {
			ASSERT_EQ(int(values.size()), kPricesSize * 2);
		} else {
			ASSERT_EQ(int(values.size()), kPricesSize * 3 - 1);
		}
	}
}

TEST_F(NsApi, DropArrayField1) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Drop array item(s) and check it was properly removed
	DefineDefaultNamespace();
	AddUnindexedData();
	dropArrayItem(rt.reindexer, default_namespace, "nested.nested_array[0].prices[0]", "nested.nested_array.prices", 0, 0);
}

TEST_F(NsApi, DropArrayField2) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Drop array item(s) and check it was properly removed
	DefineDefaultNamespace();
	AddUnindexedData();
	dropArrayItem(rt.reindexer, default_namespace, "nested.nested_array[1].prices[*]", "nested.nested_array.prices", 1);
}

TEST_F(NsApi, DropArrayField3) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Drop array item(s) and check it was properly removed
	DefineDefaultNamespace();
	AddUnindexedData();
	dropArrayItem(rt.reindexer, default_namespace, "nested.nested_array[*].prices[*]", "nested.nested_array.prices");
}

TEST_F(NsApi, SetArrayFieldWithSql) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set all items of array to 777
	Query updateQuery = Query::FromSQL("update test_namespace set nested.nested_array[1].prices[*] = 777");
	const auto qrUpdate = rt.UpdateQR(updateQuery);
	ASSERT_GT(qrUpdate.Count(), 0);

	constexpr int kElements = 3;
	// 4. Make sure all items of array nested.nested_array.prices are equal to 777 and of type Int
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		VariantArray values = item["nested.nested_array.prices"];
		for (int i = 0; i < kElements; ++i) {
			ASSERT_EQ(values[kElements + i].As<int>(), 777);
		}
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, DropArrayFieldWithSql) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Drop all items of array nested.nested_array[1].prices
	Query updateQuery = Query::FromSQL("update test_namespace drop nested.nested_array[1].prices[*]");
	const auto qrUpdate = rt.UpdateQR(updateQuery);

	constexpr int kElements = 3;
	// 4. Check if items were really removed
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		VariantArray values = item["nested.nested_array.prices"];
		ASSERT_TRUE(values.size() == kElements * 2);
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, ExtendArrayFromTopWithSql) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// Append the following items: [88, 88, 88] to the top of the array array_field
	Query updateQuery = Query::FromSQL("update test_namespace set array_field = [88,88,88] || array_field");
	const auto qrUpdate = rt.UpdateQR(updateQuery);

	constexpr int kElements = 3;
	// Check if these items were really added to array_field
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item["array_field"];
		ASSERT_TRUE(values.size() == kElements * 2);
		for (int i = 0; i < kElements; ++i) {
			ASSERT_TRUE(values[i].As<int>() == 88);
		}
	}
}

TEST_F(NsApi, AppendToArrayWithSql) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Extend array_field with expression substantially
	Query updateQuery =
		Query::FromSQL("update test_namespace set array_field = array_field || objects.more[1].array[4] || [22,22,22] || [11]");
	const auto qrUpdate = rt.UpdateQR(updateQuery);

	constexpr int kElements = 3;
	// 4. Make sure all items of array have proper values
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item["array_field"];
		int i = 0;
		ASSERT_EQ(values.size(), kElements * 2 + 1 + 1);
		for (; i < kElements; ++i) {
			ASSERT_EQ(values[i].As<int>(), i + 1);
		}
		ASSERT_EQ(values[i++].As<int>(), 0);
		for (; i < 7; ++i) {
			ASSERT_EQ(values[i].As<int>(), 22);
		}
		ASSERT_EQ(values[i].As<int>(), 11);
	}
}

TEST_F(NsApi, ExtendArrayWithExpressions) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Extend array_field with expression via Query builder
	Query updateQuery =
		Query(default_namespace)
			.Set("array_field",
				 Variant(std::string("[88,88,88] || array_field || [99, 99, 99] || indexed_array_field || objects.more[1].array[4]")),
				 true);
	const auto qrUpdate = rt.UpdateQR(updateQuery);

	constexpr int kElements = 3;
	// Check if array_field was modified properly
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item["array_field"];
		ASSERT_EQ(values.size(), kElements * 3 + 9 + 1);
		int i = 0;
		for (; i < kElements; ++i) {
			ASSERT_EQ(values[i].As<int>(), 88);
		}
		ASSERT_EQ(values[i++].As<int>(), 1);
		ASSERT_EQ(values[i++].As<int>(), 2);
		ASSERT_EQ(values[i++].As<int>(), 3);
		for (; i < 9; ++i) {
			ASSERT_EQ(values[i].As<int>(), 99);
		}
		for (int k = 1; k < 10; ++i, ++k) {
			ASSERT_EQ(values[i].As<int>(), k * 11);
		}
		ASSERT_EQ(values[i++].As<int>(), 0);
	}
}

static void validateResults(const std::shared_ptr<reindexer::Reindexer>& reindexer, const Query& baseQuery, const Query& testQuery,
							std::string_view ns, std::string_view pattern, std::string_view field, const VariantArray& expectedValues,
							std::string_view description, int resCount = 5) {
	SCOPED_TRACE(description);

	QueryResults qr;
	auto err = reindexer->Update(testQuery, qr);
	ASSERT_TRUE(err.ok()) << err.what();

	// Check initial result
	ASSERT_EQ(qr.Count(), resCount);
	std::vector<std::string> initialResults;
	initialResults.reserve(qr.Count());
	for (QueryResults::Iterator it : qr) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		const auto json = item.GetJSON();
		ASSERT_NE(json.find(pattern), std::string::npos) << "JSON: " << json << ";\npattern: " << pattern;
		initialResults.emplace_back(json);
		const VariantArray values = item[field];
		ASSERT_EQ(values.size(), expectedValues.size());
		ASSERT_EQ(values.IsArrayValue(), expectedValues.IsArrayValue());
		for (size_t i = 0; i < values.size(); ++i) {
			ASSERT_TRUE(values[i].Type().IsSame(expectedValues[i].Type()))
				<< values[i].Type().Name() << "!=" << expectedValues[i].Type().Name();
			if (values[i].Type().IsSame(reindexer::KeyValueType::Null())) {
				continue;
			}
			ASSERT_EQ(values[i], expectedValues[i]);
		}
	}
	// Check select results
	QueryResults qrSelect;
	const Query q = expectedValues.size() ? Query(ns).Where(std::string(field), CondAllSet, expectedValues) : baseQuery;
	err = reindexer->Select(q, qrSelect);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qrSelect.Count(), qr.Count());
	unsigned i = 0;
	for (QueryResults::Iterator it : qrSelect) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		const auto json = item.GetJSON();
		ASSERT_EQ(json, initialResults[i++]);
		const VariantArray values = item[field];
		ASSERT_EQ(values.size(), expectedValues.size());
		ASSERT_EQ(values.IsArrayValue(), expectedValues.IsArrayValue());
		for (size_t j = 0; j < values.size(); ++j) {
			ASSERT_TRUE(values[j].Type().IsSame(expectedValues[j].Type()))
				<< values[j].Type().Name() << "!=" << expectedValues[j].Type().Name();
			if (values[j].Type().IsSame(reindexer::KeyValueType::Null())) {
				continue;
			}
			ASSERT_EQ(values[j], expectedValues[j]);
		}
	}
}

// Check if it's possible to use append operation with empty arrays and null fields
TEST_F(NsApi, ExtendEmptyArrayWithExpressions) {
	constexpr std::string_view kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("indexed_array_field || [99, 99, 99]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, "append value to the empty indexed array");
	}
	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("indexed_array_field || []"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, "append empty array to the indexed array");
	}
	{
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || [88, 88]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88])", "non_indexed_array_field",
						{Variant(int64_t(88)), Variant(int64_t(88))}, "append value to the empty non-indexed array");
	}
	{
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || []"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88])", "non_indexed_array_field",
						{Variant(int64_t(88)), Variant(int64_t(88))}, "append empty array to the non-indexed array");
	}
	{
		const Query query = Query(kBaseQuery).Set("non_existing_field", Variant("non_existing_field || []"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88],"non_existing_field":[])",
						"non_existing_field", VariantArray().MarkArray(), "append empty array to the non-existing field");
	}
	{
		const Query query = Query(kBaseQuery).Set("non_existing_field1", Variant("non_existing_field1 || [546]"), true);
		validateResults(
			rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
			R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88],"non_existing_field":[],"non_existing_field1":[546])",
			"non_existing_field1", VariantArray{Variant(int64_t(546))}.MarkArray(), "append non-empty array to the non-existing field");
	}
}

TEST_F(NsApi, ArrayRemove) {
	constexpr std::string_view kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});
	const auto kEmptyArray = VariantArray().MarkArray();

	{
		// remove items from empty indexed non array field
		const Query query = Query(kBaseQuery).Set("id", Variant("array_remove(id, [1, 99])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_STREQ(err.what(), "Only an array field is expected as first parameter of command 'array_remove_once/array_remove'");
	}
	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", kEmptyArray, "remove empty array from empty indexed array");
	}
	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [1, 99]) || []"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", kEmptyArray, "remove all values from empty indexed array with append empty array");
	}
	{
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [1, 2, 3, 99]) || [99, 99, 99]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)},
						"remove non-used values from empty indexed array with append");
	}
	{
		const Query query =
			Query(kBaseQuery)
				.Set("indexed_array_field", Variant(std::string(R"(array_remove(indexed_array_field, ['test', '99']))")), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", kEmptyArray, "remove string values from numeric indexed array");
	}
	{
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [1]) || [4, 3, 3]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[4,3,3],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(4), Variant(3), Variant(3)},
						"remove all values from empty indexed array with append");
	}
	{
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [2, 5, 3]) || []"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[4],"non_indexed_array_field":[])",
						"indexed_array_field", VariantArray{Variant(4)}.MarkArray(),
						"remove used/non-used values from indexed array with append empty array");
	}
	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, 4)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", kEmptyArray, "remove items from indexed array by single value scalar");
	}
}

TEST_F(NsApi, ArrayRemoveExtra) {
	constexpr std::string_view kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || [99, 99, 99]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[99,99,99])",
						"non_indexed_array_field", {Variant(int64_t(99)), Variant(int64_t(99)), Variant(int64_t(99))},
						"add array to empty non-indexed array");
	}
	{
		const Query query = Query(kBaseQuery)
								.Set("indexed_array_field",
									 Variant("array_remove(indexed_array_field, indexed_array_field) || non_indexed_array_field"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[99,99,99])", "indexed_array_field",
						{Variant(99), Variant(99), Variant(99)},
						"remove from yourself indexed array field (empty) with append non-indexed field");
	}
	{
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, indexed_array_field) || [1,2]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
						R"("indexed_array_field":[1,2],"non_indexed_array_field":[99,99,99])", "indexed_array_field",
						{Variant(1), Variant(2)}, "remove from yourself indexed array field with append");
	}
	{
		const Query query =
			Query(kBaseQuery)
				.Set("indexed_array_field",
					 Variant("array_remove(indexed_array_field, 1) || array_remove_once(non_indexed_array_field, 99) || [3]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
						R"("indexed_array_field":[2,99,99,3],"non_indexed_array_field":[99,99,99])", "indexed_array_field",
						{Variant(2), Variant(99), Variant(99), Variant(3)},
						"mixed remove indexed array field with append remove in non-indexed field and append array (remove scalar)");
	}
}

TEST_F(NsApi, ArrayRemoveOnce) {
	constexpr std::string_view kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});
	const auto kEmptyArray = VariantArray().MarkArray();

	{
		// remove once value from empty indexed non array field
		const Query query = Query(kBaseQuery).Set("id", Variant("array_remove_once(id, [99])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_STREQ(err.what(), "Only an array field is expected as first parameter of command 'array_remove_once/array_remove'");
	}
	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", kEmptyArray, "remove once empty array from empty indexed array");
	}
	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [1, 99])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", kEmptyArray, "remove once values from empty indexed array");
	}
	{
		const Query query =
			Query(kBaseQuery)
				.Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [1, 2, 3, 99]) || [99, 99, 99]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)},
						"remove once non-used values from empty indexed array with append");
	}
	{
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant(std::string(R"(array_remove_once(indexed_array_field, 'Boo'))")), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)},
						"remove once string non-used values from numeric indexed array");
	}
	{
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)},
						"remove once empty array from non empty indexed array");
	}
	{
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [1, 2, 3])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, "remove once non-used values from indexed array");
	}
	{
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [99, 99]) || []"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[99],"non_indexed_array_field":[])",
						"indexed_array_field", VariantArray{Variant(99)}.MarkArray(),
						"remove one value twice from indexed array with duplicates and with append empty array");
	}
}

TEST_F(NsApi, ArrayRemoveNonIndexed) {
	constexpr std::string_view kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || [99, 99, 99]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[99,99,99])",
						"non_indexed_array_field", {Variant(int64_t(99)), Variant(int64_t(99)), Variant(int64_t(99))},
						"add array to empty non-indexed array");
	}
	{
		const Query query = Query(kBaseQuery)
								.Set("non_indexed_array_field",
									 Variant(std::string(R"(array_remove_once(non_indexed_array_field, '99') || [1, 2]))")), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs,
						R"("indexed_array_field":[],"non_indexed_array_field":[99,99,1,2])", "non_indexed_array_field",
						{Variant(int64_t(99)), Variant(int64_t(99)), Variant(int64_t(1)), Variant(int64_t(2))},
						"remove value from non-indexed array with append array");
	}
	{
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("array_remove(non_indexed_array_field, [99])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[1,2])",
						"non_indexed_array_field", {Variant(int64_t(1)), Variant(int64_t(2))},
						"remove with duplicates from non indexed array");
	}
}

TEST_F(NsApi, ArrayRemoveSparseStrings) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"str_h_field", "hash", "string", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"str_h_empty", "hash", "string", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"str_t_field", "tree", "string", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"str_t_empty", "tree", "string", IndexOpts().Array().Sparse(), 0}});
	constexpr std::string_view json =
		R"json({
				"id": 1,
				"str_h_field" : ["1", "2", "3", "3"],
				"str_t_field": ["11","22","33","33"],
			})json";
	rt.UpsertJSON(default_namespace, json);

	constexpr int resCount = 1;
	const Query kBaseQuery = Query(default_namespace).Where("id", CondEq, {resCount});
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty, [])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.1", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once([], str_h_empty)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.2", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty, ['1'])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.3", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty, str_h_field)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.4", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_field", Variant("array_remove_once(str_h_field, str_h_empty)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_field",
						{Variant("1"), Variant("2"), Variant("3"), Variant("3")}, "Step 1.5", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove(str_h_field, ['1','3'])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":["2"])", "str_h_empty",
						VariantArray{Variant("2")}.MarkArray(), "Step 1.6", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove(['1'], str_h_empty)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"])", "str_h_empty",
						VariantArray{Variant("1")}.MarkArray(), "Step 1.7", resCount);
	}
	{
		const Query query =
			Query(kBaseQuery).Set("str_h_field", Variant("array_remove(str_h_field, ['1','3','first']) || ['POCOMAXA']"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"])", "str_h_field",
						{Variant("2"), Variant("POCOMAXA")}, "Step 1.8", resCount);
	}

	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove_once(str_t_empty, [])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.1", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove_once(str_t_empty, ['1'])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.2", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove_once(str_t_empty, str_t_field)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.3", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove(str_t_empty, ['11','33','32'])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.4", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove(['7'], str_t_empty)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":["7"])",
						"str_t_empty", VariantArray{Variant("7")}.MarkArray(), "Step 2.5", resCount);
	}
	{
		const Query query =
			Query(kBaseQuery).Set("str_t_field", Variant("array_remove_once(str_t_field, ['11', '33',  'first']) || ['POCOMAXA']"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["22","33","POCOMAXA"],"str_h_empty":["1"],"str_t_empty":["7"])",
						"str_t_field", {Variant("22"), Variant("33"), Variant("POCOMAXA")}, "Step 2.6", resCount);
	}

	{
		const Query query =
			Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty,   str_t_empty) || ['007','XXX']"), true);
		validateResults(
			rt.reindexer, kBaseQuery, query, default_namespace,
			R"("str_h_field":["2","POCOMAXA"],"str_t_field":["22","33","POCOMAXA"],"str_h_empty":["1","007","XXX"],"str_t_empty":["7"])",
			"str_h_empty", {Variant("1"), Variant("007"), Variant("XXX")}, "Step 3.1", resCount);
	}
	{
		const Query query =
			Query(kBaseQuery).Set("str_t_field", Variant("[ '7', 'XXX' ]  ||  array_remove_once( str_t_field , str_h_field ) "), true);
		validateResults(
			rt.reindexer, kBaseQuery, query, default_namespace,
			R"("str_h_field":["2","POCOMAXA"],"str_t_field":["7","XXX","22","33"],"str_h_empty":["1","007","XXX"],"str_t_empty":["7"])",
			"str_t_field", {Variant("7"), Variant("XXX"), Variant("22"), Variant("33")}, "Step 3.2", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_field", Variant("array_remove_once( str_t_field , '22' \t) "), true);
		validateResults(
			rt.reindexer, kBaseQuery, query, default_namespace,
			R"("str_h_field":["2","POCOMAXA"],"str_t_field":["7","XXX","33"],"str_h_empty":["1","007","XXX"],"str_t_empty":["7"])",
			"str_t_field", {Variant("7"), Variant("XXX"), Variant("33")}, "Step 3.3", resCount);
	}
}

TEST_F(NsApi, ArrayRemoveSparseDoubles) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"double_field", "tree", "double", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"double_empty", "tree", "double", IndexOpts().Array().Sparse(), 0}});
	rt.UpsertJSON(default_namespace, R"json({"id": 1, "double_field": [1.11,2.22,3.33,3.33]})json");

	constexpr int resCount = 1;
	const Query kBaseQuery = Query(default_namespace).Where("id", CondEq, {resCount});
	{
		const Query query = Query(kBaseQuery).Set("double_empty", Variant("array_remove(double_empty, [])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace, R"("double_field":[1.11,2.22,3.33,3.33],"double_empty":[])",
						"double_empty", VariantArray{}.MarkArray(), "ArrayRemoveSparseDoubles Step 1.1", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("double_empty", Variant("array_remove_once(double_empty, double_field) || [0.07]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace, R"("double_field":[1.11,2.22,3.33,3.33],"double_empty":[0.07])",
						"double_empty", VariantArray{Variant(0.07)}.MarkArray(), "ArrayRemoveSparseDoubles Step 1.2", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("double_field", Variant("[7.77] || array_remove(double_field, double_empty)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace,
						R"("double_field":[7.77,1.11,2.22,3.33,3.33],"double_empty":[0.07])", "double_field",
						{Variant(7.77), Variant(1.11), Variant(2.22), Variant(3.33), Variant(3.33)}, "ArrayRemoveSparseDoubles Step 1.3",
						resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("double_field", Variant("array_remove_once(double_field, [3.33,3.33,1.11,99])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace, R"("double_field":[7.77,2.22],"double_empty":[0.07])",
						"double_field", {Variant(7.77), Variant(2.22)}, "ArrayRemoveSparseDoubles Step 1.4", resCount);
	}
}

TEST_F(NsApi, ArrayRemoveSparseBooleans) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"bool_field", "-", "bool", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"bool_empty", "-", "bool", IndexOpts().Array().Sparse(), 0}});
	rt.UpsertJSON(default_namespace, R"json({"id": 1, "bool_field": [true,true,false,false]})json");

	constexpr int resCount = 1;
	const Query kBaseQuery = Query(default_namespace).Where("id", CondEq, {resCount});
	{
		const Query query = Query(kBaseQuery).Set("bool_empty", Variant("array_remove(bool_empty, [])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace, R"("bool_field":[true,true,false,false],"bool_empty":[])",
						"bool_empty", VariantArray().MarkArray(), "ArrayRemoveSparseBooleans Step 1.1", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("bool_empty", Variant("array_remove_once(bool_empty, bool_field) || [1]"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace, R"("bool_field":[true,true,false,false],"bool_empty":[true])",
						"bool_empty", VariantArray{Variant(true)}.MarkArray(), "ArrayRemoveSparseBooleans Step 1.2", resCount);
	}
	{
		const Query query = Query(kBaseQuery).Set("bool_field", Variant("array_remove_once(bool_field, bool_empty)"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace, R"("bool_field":[true,false,false],"bool_empty":[true])",
						"bool_field", {Variant(true), Variant(false), Variant(false)}, "ArrayRemoveSparseBooleans Step 1.3", resCount);
	}
	{
		const Query query =
			Query(kBaseQuery)
				.Set("bool_field", Variant("[true] || array_remove(bool_field, [false]) || array_remove_once(bool_empty, [0])"), true);
		validateResults(rt.reindexer, kBaseQuery, query, default_namespace, R"("bool_field":[true,true,true],"bool_empty":[true])",
						"bool_field", {Variant(true), Variant(true), Variant(true)}, "ArrayRemoveSparseBooleans Step 1.4", resCount);
	}
}

TEST_F(NsApi, ArrayRemoveSeveralJsonPathsField) {
	constexpr std::string_view testNS = "remove_arrays_ns";
	const std::string intField0 = "int_field0";
	const std::string intField1 = "int_field1";
	const std::string multiPathArrayField = "array_index";
	const Query kBaseQuery = Query(testNS).Where("id", CondSet, {1, 5, 8, 3});

	rt.OpenNamespace(testNS);
	DefineNamespaceDataset(
		testNS, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
				 IndexDeclaration{intField0, "hash", "int", IndexOpts(), 0}, IndexDeclaration{intField1, "hash", "int", IndexOpts(), 0}});
	rt.AddIndex(testNS, {multiPathArrayField, reindexer::JsonPaths{intField0, intField1}, "hash", "composite", IndexOpts().Array()});

	static constexpr int sz = 10;
	for (int i = 0; i < sz; ++i) {
		Item item = NewItem(testNS);
		item[idIdxName] = i;
		item[intField0] = sz - i;
		item[intField1] = sz + i;
		Upsert(testNS, item);
	}

	const Query query =
		Query(kBaseQuery).Set(multiPathArrayField, Variant(fmt::format("array_remove_once({}, ['99'])", multiPathArrayField)), true);
	QueryResults qr;
	auto err = rt.reindexer->Update(query, qr);
	ASSERT_FALSE(err.ok());
	ASSERT_EQ(err.what(), fmt::format("Ambiguity when updating field with several json paths by index name: '{}'", multiPathArrayField));
}

TEST_F(NsApi, ArrayRemoveWithSql) {
	DefineDefaultNamespace();
	AddUnindexedData();

	// Remove from array_field with expression substantially
	{
		auto updateQuery = Query::FromSQL(
			"update test_namespace set array_field = [0] || array_remove(array_field, [3,2,1])"
			" || array_remove_once(indexed_array_field, 99) || [7,9]");
		const auto qrUpdate = rt.UpdateQR(updateQuery);

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["array_field"];
			ASSERT_EQ(values.size(), 1 + 8 + 2);  // [0] || [11,22,33,44,55,66,77,88] || [7,9] remove 99
			int i = 0;							  // 1
			for (; i < 9; ++i) {
				ASSERT_EQ(values[i].As<int>(), i * 11);	 // +8
			}
			ASSERT_EQ(values[i++].As<int>(), 7);
			ASSERT_EQ(values[i].As<int>(), 9);	// +2
		}
	}

	// Remove scalar value of an array_field
	{
		auto updateQuery = Query::FromSQL("update test_namespace set array_field = array_remove(array_field, 7)");
		const auto qrUpdate = rt.UpdateQR(updateQuery);

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["array_field"];
			ASSERT_EQ(values.size(), 1 + 8 + 1);  // [0,11,22,33,44,55,66,77,88,9] remove 7
			int i = 0;							  // 1
			for (; i < 9; ++i) {
				ASSERT_EQ(values[i].As<int>(), i * 11);	 // +8
			}
			ASSERT_EQ(values[i].As<int>(), 9);	// +1
		}
	}

	// Remove & concatenate for string array field
	{
		auto updateQuery =
			Query::FromSQL(R"(update test_namespace set string_array = array_remove(string_array, ['first']) || ['POCOMAXA'])");
		const auto qrUpdate = rt.UpdateQR(updateQuery);

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["string_array"];
			ASSERT_EQ(values.size(), 3);
			ASSERT_EQ(values[0].As<std::string>(), "second");
			ASSERT_EQ(values[1].As<std::string>(), "third");
			ASSERT_EQ(values[2].As<std::string>(), "POCOMAXA");
		}
	}

	// Remove scalar value from string array field
	{
		std::vector<std::string> words({"second", "third", "POCOMAXA"});
		for (size_t idx = 0, sz = words.size(); idx < sz; ++idx) {
			const auto& word = words[idx];
			auto updateQuery = Query::FromSQL(R"(update test_namespace set string_array = array_remove(string_array, ')" + word + R"('))");
			const auto qrUpdate = rt.UpdateQR(updateQuery);
			// Check if array_field was modified properly
			for (auto it : qrUpdate) {
				Item item = it.GetItem(false);
				checkIfItemJSONValid(it);
				VariantArray values = item["string_array"];
				ASSERT_EQ(values.size(), sz - 1 - idx);
				for (size_t i = 0, sz1 = values.size(); i < sz1; ++i) {
					ASSERT_EQ(values[i].As<std::string>(), words[i + idx + 1]);
				}
			}
		}
	}

	// Remove scalar value from bool array field
	{
		std::vector<std::string> boolVals({"false", "true"});
		for (size_t idx = 0, sz = boolVals.size(); idx < sz; ++idx) {
			const auto& boolVal = boolVals[idx];
			auto updateQuery = Query::FromSQL("update test_namespace set bool_array = array_remove(bool_array, " + boolVal + ")");
			const auto qrUpdate = rt.UpdateQR(updateQuery);
			// Check if array_field was modified properly
			for (auto it : qrUpdate) {
				Item item = it.GetItem(false);
				checkIfItemJSONValid(it);
				VariantArray values = item["bool_array"];
				ASSERT_EQ(values.size(), sz - 1 - idx);
				for (const auto& value : values) {
					ASSERT_EQ(value.As<bool>(), true);
				}
			}
		}
	}

	// Remove array value from bool array field
	{
		std::vector<std::string> boolVals({"true", "false"});
		for (size_t idx = 0, sz = boolVals.size(); idx < sz; ++idx) {
			const auto& boolVal = boolVals[idx];
			auto updateQuery = Query::FromSQL("update test_namespace set bool_array2 = array_remove(bool_array2, [" + boolVal + "])");
			const auto qrUpdate = rt.UpdateQR(updateQuery);
			// Check if array_field was modified properly
			for (auto it : qrUpdate) {
				Item item = it.GetItem(false);
				checkIfItemJSONValid(it);
				VariantArray values = item["bool_array2"];
				ASSERT_EQ(values.size(), sz - 1 - idx);
				for (const auto& value : values) {
					ASSERT_EQ(value.As<bool>(), false);
				}
			}
		}
	}

	// Remove value from array_field by index of an array_field
	{
		auto updateQuery = Query::FromSQL("update test_namespace set array_field = array_remove(array_field, array_field[0])");
		const auto qrUpdate = rt.UpdateQR(updateQuery);

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["array_field"];
			ASSERT_EQ(values.size(), 9);  // [11,22,33,44,55,66,77,88,9] remove 0
			int i = 0;
			for (; i < 8; ++i) {
				ASSERT_EQ(values[i].As<int>(), (i + 1) * 11);  // +8
			}
			ASSERT_EQ(values[i].As<int>(), 9);	// +1
		}
	}

	// Remove value from int_field2{88} of an array_field
	{
		auto updateQuery = Query::FromSQL("update test_namespace set array_field = array_remove(array_field, int_field2)");
		const auto qrUpdate = rt.UpdateQR(updateQuery);

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["array_field"];
			ASSERT_EQ(values.size(), 8);  // [11,22,33,44,55,66,77,9] remove 88
			int i = 0;
			for (; i < 7; ++i) {
				ASSERT_EQ(values[i].As<int>(), (i + 1) * 11);  // +7
			}
			ASSERT_EQ(values[i].As<int>(), 9);	// +1
		}
	}

	// Remove all elements from array_field. Destroy itself
	{
		auto updateQuery = Query::FromSQL("update test_namespace set array_field = array_remove(array_field, array_field)");
		const auto qrUpdate = rt.UpdateQR(updateQuery);

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["array_field"];
			ASSERT_TRUE(values.empty());
		}
	}
}

static void validateUpdateJSONResults(const std::shared_ptr<reindexer::Reindexer>& reindexer, const Query& updateQuery,
									  std::string_view expectation, std::string_view description) {
	SCOPED_TRACE(description);

	QueryResults qr;
	auto err = reindexer->Update(updateQuery, qr);
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<std::string> initialResults;
	initialResults.reserve(qr.Count());
	for (QueryResults::Iterator it : qr) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		const auto json = item.GetJSON();
		ASSERT_NE(json.find(expectation), std::string::npos) << "JSON: " << json << ";\nexpectation: " << expectation;
		initialResults.emplace_back(json);
	}

	// Check select results
	QueryResults qrSelect;
	err = reindexer->ExecSQL("SELECT * FROM test_namespace", qrSelect);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qrSelect.Count(), qr.Count());
	unsigned i = 0;
	for (QueryResults::Iterator it : qrSelect) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		const auto json = item.GetJSON();
		ASSERT_EQ(json, initialResults[i++]);
	}
}

TEST_F(NsApi, UpdateObjectsArray) {
	DefineDefaultNamespace();
	AddUnindexedData();

	Query updateQuery =
		Query::FromSQL(R"(update test_namespace set nested.nested_array[1] = {"id":1,"name":"modified", "prices":[4,5,6]})");
	validateUpdateJSONResults(rt.reindexer, updateQuery, R"({"id":1,"name":"modified","prices":[4,5,6]})",
							  "Make sure nested.nested_array[1] is set to a new value properly");
}

TEST_F(NsApi, UpdateObjectsArray2) {
	DefineDefaultNamespace();
	AddUnindexedData();

	Query updateQuery = Query::FromSQL(R"(update test_namespace set nested.nested_array[*] = {"ein":1,"zwei":2, "drei":3})");
	validateUpdateJSONResults(rt.reindexer, updateQuery,
							  R"("nested_array":[{"ein":1,"zwei":2,"drei":3},{"ein":1,"zwei":2,"drei":3},{"ein":1,"zwei":2,"drei":3}]})",
							  "Make sure all items of nested.nested_array are set to a new value correctly");
}

TEST_F(NsApi, UpdateHeterogeneousArray) {
	constexpr std::string_view kEmptyArraysNs = "empty_namespace";
	constexpr int resCount = 100;
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kQueryDummy;

	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET non_indexed_array_field = [1, null])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[1,null])",
						"non_indexed_array_field", {Variant(int64_t(1)), Variant()},
						"Checking set heterogeneous non-indexed array with null", resCount);
	}
	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET non_indexed_array_field = [1,-2,3])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[1,-2,3])",
						"non_indexed_array_field", {Variant(int64_t(1)), Variant(int64_t(-2)), Variant(int64_t(3))},
						"Set homogeneous non-indexed array", resCount);

		Query query2 = Query::FromSQL(R"(UPDATE empty_namespace SET non_indexed_array_field[1] = -505.6782)");
		validateResults(rt.reindexer, kQueryDummy, query2, kEmptyArraysNs,
						R"("indexed_array_field":[],"non_indexed_array_field":[1,-505.6782,3])", "non_indexed_array_field",
						{Variant(int64_t(1)), Variant(-505.6782), Variant(int64_t(3))},
						"Check the possibility of making a homogeneous indexed array heterogeneous", resCount);
	}
	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET non_indexed_array_field = ['hi',true,'bro'])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[],"non_indexed_array_field":["hi",true,"bro"])", "non_indexed_array_field",
						{Variant("hi"), Variant(true), Variant("bro")}, "Checking set heterogeneous non-indexed array", resCount);
	}
	{
		Query query = Query::FromSQL("UPDATE empty_namespace SET non_indexed_array_field[1] = 3");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[],"non_indexed_array_field":["hi",3,"bro"])", "non_indexed_array_field",
						{Variant("hi"), Variant(int64_t(3)), Variant("bro")},
						"Checking overwrite in heterogeneous array one item via scalar value (middle)", resCount);
	}
	{
		Query query = Query::FromSQL("UPDATE empty_namespace SET non_indexed_array_field[2] = 24");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[],"non_indexed_array_field":["hi",3,24])", "non_indexed_array_field",
						{Variant("hi"), Variant(int64_t(3)), Variant(int64_t(24))},
						"Checking overwrite in heterogeneous array one item via scalar value (last)", resCount);
	}
	{
		Query query = Query::FromSQL("UPDATE empty_namespace SET non_indexed_array_field[0] = 81");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":[81,3,24])",
						"non_indexed_array_field", {Variant(int64_t(81)), Variant(int64_t(3)), Variant(int64_t(24))},
						"Checking overwrite in heterogeneous array one item via scalar value (first)", resCount);
	}
	{
		Query query = Query::FromSQL("UPDATE empty_namespace SET non_indexed_array_field = 183042");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":183042)",
						"non_indexed_array_field", {Variant(int64_t(183042))},
						"Checking overwrite heterogeneous non-indexed array by single scalar value", resCount);
	}
	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET non_indexed_array_field = ['pocomaxa','forever',true])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[],"non_indexed_array_field":["pocomaxa","forever",true])", "non_indexed_array_field",
						{Variant("pocomaxa"), Variant("forever"), Variant(true)},
						"Checking overwrite non-indexed scalar with heterogeneous array", resCount);
	}
	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET non_indexed_array_field = [3.14,9811,'Boom'])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[],"non_indexed_array_field":[3.14,9811,"Boom"])", "non_indexed_array_field",
						{Variant(3.14), Variant(int64_t(9811)), Variant("Boom")},
						"Checking overwrite non-indexed array with heterogeneous array", resCount);
	}
	{
		Query query = Query::FromSQL("UPDATE empty_namespace SET non_indexed_array_field = 3.14");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs, R"("indexed_array_field":[],"non_indexed_array_field":3.14)",
						"non_indexed_array_field", {Variant(3.14)},
						"Checking overwrite heterogeneous non-indexed array with scalar value (double)", resCount);
	}

	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET indexed_array_field = ['2',3])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs, R"("indexed_array_field":[2,3],"non_indexed_array_field":3.14)",
						"indexed_array_field", {Variant(2), Variant(3)}, "Checking set heterogeneous indexed array with conversion",
						resCount);
	}
	{
		Query query = Query::FromSQL("UPDATE empty_namespace SET indexed_array_field = 4");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs, R"("indexed_array_field":4,"non_indexed_array_field":3.14)",
						"indexed_array_field", VariantArray{Variant(4)}.MarkArray(),
						"Checking set heterogeneous indexed array with scalar value (int)", resCount);
	}
	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET indexed_array_field = ['111',222,333])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[111,222,333],"non_indexed_array_field":3.14)", "indexed_array_field",
						{Variant(111), Variant(222), Variant(333)}, "Checking overwrite scalar value field with heterogeneous array",
						resCount);
	}
	{
		const auto description = "Checking update of heterogeneous indexed array with invalid element - expected failure";
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET indexed_array_field = [555,'BOO'])");
		QueryResults qr;
		auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok()) << description;
		ASSERT_STREQ(err.what(), "Can't convert 'BOO' to number") << description;
	}
	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET indexed_array_field[0] = '777')");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[777,222,333],"non_indexed_array_field":3.14)", "indexed_array_field",
						{Variant(777), Variant(222), Variant(333)},
						"Checking overwrite in heterogeneous indexed array one item via scalar value", resCount);
	}
	{
		Query query = Query::FromSQL(R"(UPDATE empty_namespace SET indexed_array_field = ['333', 33])");
		validateResults(rt.reindexer, kQueryDummy, query, kEmptyArraysNs,
						R"("indexed_array_field":[333,33],"non_indexed_array_field":3.14)", "indexed_array_field",
						{Variant(333), Variant(33)}, "Checking overwrite indexed array field with heterogeneous array", resCount);
	}
}

TEST_F(NsApi, UpdateObjectsArray3) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set all items of the object array to a new value via Query builder
	Query updateQuery =
		Query(default_namespace).SetObject("nested.nested_array[*]", Variant(std::string(R"({"ein":1,"zwei":2, "drei":3})")), false);
	const auto qrUpdate = rt.UpdateQR(updateQuery);
	ASSERT_GT(qrUpdate.Count(), 0);

	// 4. Make sure all items of nested.nested_array are set to a new value correctly
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		const auto json = item.GetJSON();
		ASSERT_NE(json.find(R"("nested_array":[{"ein":1,"zwei":2,"drei":3},{"ein":1,"zwei":2,"drei":3},{"ein":1,"zwei":2,"drei":3}]})"),
				  std::string::npos)
			<< json;
		ASSERT_NE(json.find(R"("objects":[{"more":[{"array":[9,8,7,6,5]},{"array":[4,3,2,1,0]}]}])"), std::string::npos) << json;
	}
}

TEST_F(NsApi, UpdateObjectsArray4) {
	// 1. Define NS
	DefineDefaultNamespace();
	const std::vector<std::string_view> indexTypes = {"regular", "sparse", "none"};
	constexpr char kIndexName[] = "objects.array.field";
	const Query kBaseQuery = Query(default_namespace).Where("id", CondSet, {1199, 1201, 1203, 1210, 1240});

	auto ValidateResults = [this, &kBaseQuery](const QueryResults& qr, std::string_view pattern, std::string_view description) {
		SCOPED_TRACE(description);
		// Check initial result
		ASSERT_EQ(qr.Count(), 5);
		std::vector<std::string> initialResults;
		initialResults.reserve(qr.Count());
		for (auto it : qr) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			const auto json = item.GetJSON();
			ASSERT_NE(json.find(pattern), std::string::npos) << "JSON: " << json << ";\npattern: " << pattern;
			initialResults.emplace_back(json);
		}
		// Check select results
		const auto qrSelect = rt.Select(kBaseQuery);
		ASSERT_EQ(qrSelect.Count(), qr.Count());
		unsigned i = 0;
		for (auto it : qrSelect) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			const auto json = item.GetJSON();
			ASSERT_EQ(json, initialResults[i++]);
		}
	};

	for (const auto& index : indexTypes) {
		rt.TruncateNamespace(default_namespace);
		// 2. Refill NS
		AddHeterogeneousNestedData();
		auto err = rt.reindexer->DropIndex(default_namespace, reindexer::IndexDef(kIndexName));
		(void)err;	// Error does not matter here
		if (index != "none") {
			rt.AddIndex(default_namespace,
						reindexer::IndexDef(kIndexName, {kIndexName}, "hash", "int64", IndexOpts().Array().Sparse(index == "sparse")));
		}

		SCOPED_TRACE(fmt::format("Index type is '{}' ", index));
		{
			const auto description = "Update array field, nested into objects array with explicit index (1 element)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[0].field[4]", {777}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":11},{"field":[4,3,2,1,0]},{"field":[99]}]}])",
							description);
		}
		{
			const auto description = "Update array field, nested into objects array with explicit index (1 element, different position)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[2].field[3]", {8387}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":11},{"field":[4,3,2,8387,0]},{"field":[99]}]}])",
							description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index with scalar type";
			// Make sure, that internal field's type ('scalar') was not changed
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", {537}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":537},{"field":[4,3,2,8387,0]},{"field":[99]}]}])",
							description);
		}
		{
			const auto description = "Update scalar field, nested into objects array with explicit index with array type";
			// Make sure, that internal field's type ('array') was not changed
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[3].field[0]", {999}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":537},{"field":[4,3,2,8387,0]},{"field":[999]}]}])",
							description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Change field type from array[1] to scalar";
			// Make sure, that internal field's type (array of 1 element) was changed to scalar
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[3].field", {837}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":537},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Change field type from array[4] to scalar";
			// Make sure, that internal field's type (array of 4 elements) was changed to scalar
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[0].field", {2345}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":537},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Change field type from scalar to array[1]";
			// Make sure, that internal field's type ('scalar') was changed to array
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{1847}}.MarkArray(), false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[1847]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index. Increase array size";
			Query updateQuery =
				Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{115}, Variant{1000}, Variant{501}}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[115,1000,501]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Reduce array size (to multiple elements)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{100}, Variant{999}}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[100,999]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Reduce array size (to single element)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{150}}.MarkArray(), false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Attempt to set array-value(1 element) by explicit index";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[0]", VariantArray{Variant{199}}.MarkArray(), false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << err.what();

			qr.Clear();
			qr = rt.Select(kBaseQuery);
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Attempt to set array-value(multiple elements) by explicit index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[0]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << err.what();

			qr = rt.Select(kBaseQuery);
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Attempt to set array-value(1 element) by *-index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[*]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << err.what();

			qr = rt.Select(kBaseQuery);
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Attempt to set array-value(multiple elements) by *-index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[*]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << err.what();

			qr = rt.Select(kBaseQuery);
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Update array field, nested into objects array with *-index";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[2].field[*]", {199}, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[199,199,199,199,199]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Attempt to update scalar value by *-index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[0].field[*]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << err.what();

			qr = rt.Select(kBaseQuery);
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[199,199,199,199,199]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index. Reduce array size to 0";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray().MarkArray(), false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[]},{"field":[199,199,199,199,199]},{"field":837}]}])",
							description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index. Increase array size from 0";
			VariantArray v{Variant{11199}, Variant{11200}, Variant{11300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", v, false);
			auto qr = rt.UpdateQR(updateQuery);
			ValidateResults(
				qr, R"("objects":[{"array":[{"field":2345},{"field":[11199,11200,11300]},{"field":[199,199,199,199,199]},{"field":837}]}])",
				description);
		}
	}
}

TEST_F(NsApi, UpdateArrayIndexFieldWithSeveralJsonPaths) {
	struct [[nodiscard]] Values {
		std::vector<std::string> valsList, newValsList;
	};
	const int fieldsCnt = 5;
	const int valsPerFieldCnt = 4;
	std::vector<Values> fieldsValues(fieldsCnt);
	for (int i = 0; i < fieldsCnt; ++i) {
		for (int j = 0; j < valsPerFieldCnt; ++j) {
			fieldsValues[i].valsList.emplace_back(fmt::format("data{}{}", i, j));
			fieldsValues[i].newValsList.emplace_back(fmt::format("data{}{}", i, j + i));
		}
	}

	enum class [[nodiscard]] OpT { Insert, Update };

	auto makeFieldsList = [&fieldsValues](const reindexer::fast_hash_set<int>& indexes, OpT type) {
		auto quote = type == OpT::Insert ? '"' : '\'';
		std::vector<std::string> Values::* list = type == OpT::Insert ? &Values::valsList : &Values::newValsList;
		const auto fieldsListTmplt = fmt::runtime(type == OpT::Insert ? R"({}"field{}": [{}])" : R"({}field{} = [{}])");
		std::string fieldsList;
		for (int idx : indexes) {
			std::string fieldList;
			for (const auto& data : fieldsValues[idx].*list) {
				fieldList += std::string(fieldList.empty() ? "" : ", ") + quote + data + quote;
			}
			fieldsList += fmt::format(fieldsListTmplt, fieldsList.empty() ? "" : ", ", idx, fieldList);
		}
		return fieldsList;
	};

	auto makeItem = [&makeFieldsList](int id, const reindexer::fast_hash_set<int>& indexes) {
		auto list = makeFieldsList(indexes, OpT::Insert);
		return fmt::format(R"({{"id": {}{}}})", id, (list.empty() ? "" : ", ") + list);
	};

	auto makeUpdate = [this, &makeFieldsList](int id, const reindexer::fast_hash_set<int>& indexes) {
		return fmt::format("UPDATE {} SET {} WHERE id = {}", default_namespace, makeFieldsList(indexes, OpT::Update), id);
	};

	struct [[nodiscard]] TestCase {
		reindexer::fast_hash_set<int> insertIdxs, updateIdxs;
		auto expected() const {
			auto res = insertIdxs;
			res.insert(updateIdxs.begin(), updateIdxs.end());
			return res;
		}
	};

	std::vector<TestCase> testCases{
		{{}, {0}},
		{{}, {2}},
		{{}, {0, 1, 2}},
		{{2, 3, 4}, {0}},
		{{3}, {0, 2, 4}},
		{{0, 3, 4}, {2, 1}},
		{{0, 1, 2, 3}, {4}},
		{{0, 1, 2}, {3, 4}},
		{{0, 2, 3}, {1, 4}},
		{{4}, {0, 1, 2, 3}},
		{{3, 4}, {0, 2, 1}},
		{{}, {0, 1, 2, 3, 4}},
		{{0, 1, 2, 3, 4}, {0}},
		{{0, 3, 4}, {0, 3, 4}},
		{{0, 1, 2}, {2, 3, 1}},
		{{0, 3, 4}, {2, 3, 4}},
		{{0, 1, 3}, {0, 1, 2, 3, 4}},
		{{0, 1, 2, 3, 4}, {0, 1, 2, 3, 4}},
	};

	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"array_index", reindexer::JsonPaths{"field0", "field1", "field2", "field3", "field4"}, "hash", "string",
									IndexOpts().Array()});
	for (size_t i = 0; i < testCases.size(); ++i) {
		rt.UpsertJSON(default_namespace, makeItem(i, testCases[i].insertIdxs));
		{
			auto qr = rt.ExecSQL(makeUpdate(i, testCases[i].updateIdxs));
			ASSERT_EQ(qr.Count(), 1);

			auto item = qr.begin().GetItem(false);
			for (auto idx : testCases[i].expected()) {
				int varArrCnt = 0;
				for (auto&& var : VariantArray(item[fmt::format("field{}", idx)])) {
					const auto& data = testCases[i].updateIdxs.count(idx) ? fieldsValues[idx].newValsList : fieldsValues[idx].valsList;
					ASSERT_EQ(var.As<std::string>(), data[varArrCnt++]);
				}
			}
		}
	}

	// Check that prohibited updating an index array field with several json paths by index name
	QueryResults qr;
	auto err =
		rt.reindexer->ExecSQL(fmt::format("UPDATE {} SET array_index = ['data0', 'data1', 'data2'] WHERE id = 0", default_namespace), qr);
	ASSERT_FALSE(err.ok());
	ASSERT_STREQ(err.what(), "Ambiguity when updating field with several json paths by index name: 'array_index'");
}

TEST_F(NsApi, UpdateWithObjectAndFieldsDuplication) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"nested", reindexer::JsonPaths{"n.idv"}, "hash", "string", IndexOpts()});

	std::vector<std::string_view> items = {R"({"id":0,"data":"data0","n":{"idv":"index_str_1","dat":"data1"}})",
										   R"({"id":5,"data":"data5","n":{"idv":"index_str_3","dat":"data2"}})"};

	rt.UpsertJSON(default_namespace, items[0]);
	rt.UpsertJSON(default_namespace, items[1]);
	{
		QueryResults qr;
		auto err =
			rt.reindexer->Update(Query(default_namespace)
									 .SetObject("n", R"({"idv":"index_str_3_modified","idv":"index_str_5_modified","dat":"data2_mod"})")
									 .Where("id", CondEq, 5),
								 qr);
		ASSERT_EQ(err.code(), errLogic) << err.what();
	}
	{
		// Check all the items
		auto qr = rt.Select(Query(default_namespace).Sort("id", false));
		ASSERT_EQ(qr.Count(), 2);
		unsigned i = 0;
		for (auto it : qr) {
			ASSERT_EQ(it.GetItem().GetJSON(), items[i]) << i;
			++i;
		}
	}
	{
		// Check old indexed value (have to exist)
		auto qr = rt.Select(Query(default_namespace).Where("nested", CondEq, std::string("index_str_3")));
		ASSERT_EQ(qr.Count(), 1);
		ASSERT_EQ(qr.begin().GetItem().GetJSON(), items[1]);
	}
	{
		// Check new indexed values (have to not exist)
		auto qr = rt.Select(
			Query(default_namespace).Where("nested", CondSet, {std::string("index_str_3_modified"), std::string("index_str_5_modified")}));
		ASSERT_EQ(qr.Count(), 0);
	}
}

TEST_F(NsApi, UpdateOutOfBoundsArrayField) {
	// Check, that item modifier does not allow to set value in the array without of bound index
	const int kTargetID = 1500;

	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	struct [[nodiscard]] Case {
		const std::string_view name;
		const std::string baseUpdateExpr;
		const std::vector<int> arrayIdx;
		const std::function<Query(const std::string&)> createQueryF;
	};
	const std::vector<Case> cases = {
		{.name = "update-index-array-field",
		 .baseUpdateExpr = "indexed_array_field[{}]",
		 .arrayIdx = {9, 10, 100, 10000, 5000000},
		 .createQueryF =
			 [&](const std::string& path) {
				 return Query(default_namespace).Where("id", CondEq, kTargetID).Set(path, static_cast<int>(777));
			 }},
		{.name = "update-non-indexed-array-field",
		 .baseUpdateExpr = "array_field[{}]",
		 .arrayIdx = {3, 4, 100, 10000, 5000000},
		 .createQueryF =
			 [&](const std::string& path) {
				 return Query(default_namespace).Where("id", CondEq, kTargetID).Set(path, static_cast<int>(777));
			 }},
		{.name = "update-object-array-field",
		 .baseUpdateExpr = "nested.nested_array[{}]",
		 .arrayIdx = {3, 4, 100, 10000, 5000000},
		 .createQueryF = [&](const std::string& path) {
			 return Query(default_namespace)
				 .Where("id", CondEq, kTargetID)
				 .SetObject(path, Variant(std::string(R"({"id":5,"name":"fifth", "prices":[3,5,5]})")), false);
		 }}};

	for (auto& c : cases) {
		SCOPED_TRACE(c.name);

		for (auto idx : c.arrayIdx) {
			// 3. Get initial array value
			std::string initialItemJSON;
			{
				auto qr = rt.Select(Query(default_namespace).Where("id", CondEq, kTargetID));
				ASSERT_EQ(qr.Count(), 1);
				reindexer::WrSerializer ser;
				auto err = qr.begin().GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				initialItemJSON = ser.Slice();
			}

			// 4. Set item without of bound index to specific value via Query builder
			const auto path = fmt::format(fmt::runtime(c.baseUpdateExpr), idx);
			SCOPED_TRACE(path);
			QueryResults qrUpdate;
			const auto updateQuery = c.createQueryF(path);
			Error err = rt.reindexer->Update(updateQuery, qrUpdate);
			EXPECT_FALSE(err.ok());

			{
				// 5. Make sure, that item was not changed
				auto qr = rt.Select(Query(default_namespace).Where("id", CondEq, kTargetID));
				ASSERT_EQ(qr.Count(), 1);
				reindexer::WrSerializer ser;
				err = qr.begin().GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(initialItemJSON, ser.Slice());
			}
		}
	}
}

TEST_F(NsApi, AccessForIndexedArrayItem) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set indexed_array_field[0] to 777
	const auto qr = rt.UpdateQR(Query(default_namespace).Set("indexed_array_field[0]", Variant(int(777))));
	ASSERT_GT(qr.Count(), 0);

	// 4. Try to access elements of different arrays with Item object functionality
	// to make sure if GetValueByJSONPath() works properly.
	for (auto it : qr) {
		checkIfItemJSONValid(it);

		reindexer::Item item = it.GetItem(false);

		Variant value1 = item["indexed_array_field[0]"];
		ASSERT_TRUE(value1.Type().Is<reindexer::KeyValueType::Int>());
		ASSERT_EQ(static_cast<int>(value1), 777);

		Variant value2 = item["objects[0].more[0].array[0]"];
		ASSERT_TRUE(value2.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value2), 9);

		Variant value3 = item["objects[0].more[0].array[1]"];
		ASSERT_TRUE(value3.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value3), 8);

		Variant value4 = item["objects[0].more[0].array[2]"];
		ASSERT_TRUE(value4.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value4), 7);

		Variant value5 = item["objects[0].more[0].array[3]"];
		ASSERT_TRUE(value5.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value5), 6);

		Variant value6 = item["objects[0].more[0].array[4]"];
		ASSERT_TRUE(value6.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value6), 5);

		Variant value7 = item["nested.nested_array[1].prices[1]"];
		ASSERT_TRUE(value7.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value7), 5);
	}
}

TEST_F(NsApi, UpdateComplexArrayItem) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set objects[0].more[1].array[1] to 777
	const auto qr = rt.UpdateQR(
		Query(default_namespace).Where(idIdxName, CondEq, Variant(1000)).Set("objects[0].more[1].array[1]", Variant(int64_t(777))));
	ASSERT_GT(qr.Count(), 0);

	// 4. Make sure the value of objects[0].more[1].array[1] which was updated above,
	// can be accesses correctly with no problems.
	for (auto it : qr) {
		checkIfItemJSONValid(it);

		reindexer::Item item = it.GetItem(false);

		Variant value = item["objects[0].more[1].array[1]"];
		ASSERT_TRUE(value.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value), 777);

		Variant value2 = item["objects[0].more[1].array[2]"];
		ASSERT_TRUE(value2.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value2), 2);
	}
}

TEST_F(NsApi, CheckIndexedArrayItem) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Select all items of the namespace
	const auto qr = rt.Select(Query(default_namespace));
	ASSERT_GT(qr.Count(), 0);

	// 4. Check if the value of indexed array objects[0].more[1].array[1]
	// can be accessed easily.
	for (auto it : qr) {
		checkIfItemJSONValid(it);

		reindexer::Item item = it.GetItem(false);

		Variant value = item["objects[0].more[1].array[1]"];
		ASSERT_TRUE(value.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value), 3);

		Variant value1 = item["objects[0].more[1].array[3]"];
		ASSERT_TRUE(value1.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value1), 1);

		Variant value2 = item["objects[0].more[0].array[4]"];
		ASSERT_TRUE(value2.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(static_cast<int64_t>(value2), 5);
	}
}

static void checkFieldConversion(const std::shared_ptr<reindexer::Reindexer>& reindexer, const std::string& ns,
								 const std::string& updateFieldPath, const VariantArray& newValue, const VariantArray& updatedValue,
								 reindexer::KeyValueType sourceType, bool expectFail) {
	const Query selectQuery{Query(ns).Where("id", CondGe, Variant("500"))};
	QueryResults qrUpdate;
	Query updateQuery = selectQuery;
	updateQuery.Set(updateFieldPath, newValue);
	Error err = reindexer->Update(updateQuery, qrUpdate);
	if (expectFail) {
		if (err.ok()) {
			for (QueryResults::Iterator it : qrUpdate) {
				checkIfItemJSONValid(it, true);
			}
		}
		ASSERT_TRUE(!err.ok());
	} else {
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_GT(qrUpdate.Count(), 0);

		QueryResults qrAll;
		err = reindexer->Select(selectQuery, qrAll);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qrAll.Count(), qrUpdate.Count());

		for (QueryResults::Iterator it : qrAll) {
			Item item = it.GetItem(false);
			VariantArray val = item[updateFieldPath];
			ASSERT_EQ(val.size(), updatedValue.size());
			if (sourceType.Is<reindexer::KeyValueType::Undefined>()) {
				ASSERT_TRUE(val == newValue) << "expected:" << val.Dump() << "\n;actual:" << newValue.Dump();
			} else {
				for (const Variant& v : val) {
					ASSERT_TRUE(v.Type().IsSame(sourceType)) << v.Type().Name();
				}
			}
			ASSERT_TRUE(val == updatedValue) << "expected:" << val.Dump() << "\n;actual:" << updatedValue.Dump();
			checkIfItemJSONValid(it);
		}
	}
}

TEST_F(NsApi, TestIntIndexedFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(static_cast<double>(13.33f))},
						 {Variant(static_cast<int>(13.33f))}, reindexer::KeyValueType::Int{}, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(static_cast<int64_t>(13))}, {Variant(static_cast<int>(13))},
						 reindexer::KeyValueType::Int{}, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(static_cast<bool>(false))}, {Variant(static_cast<int>(0))},
						 reindexer::KeyValueType::Int{}, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(static_cast<bool>(true))}, {Variant(static_cast<int>(1))},
						 reindexer::KeyValueType::Int{}, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(std::string("100500"))}, {Variant(static_cast<int>(100500))},
						 reindexer::KeyValueType::Int{}, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(std::string("Jesus Christ"))}, {Variant()},
						 reindexer::KeyValueType::Int{}, true);
}

TEST_F(NsApi, TestDoubleIndexedFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<int>(13333))},
						 {Variant(static_cast<double>(13333.0f))}, reindexer::KeyValueType::Double{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<int64_t>(13333))},
						 {Variant(static_cast<double>(13333.0f))}, reindexer::KeyValueType::Double{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<bool>(false))},
						 {Variant(static_cast<double>(0.0f))}, reindexer::KeyValueType::Double{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<bool>(true))},
						 {Variant(static_cast<double>(1.0f))}, reindexer::KeyValueType::Double{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(std::string("100500.1"))},
						 {Variant(static_cast<double>(100500.100000))}, reindexer::KeyValueType::Double{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(std::string("Jesus Christ"))}, {Variant()},
						 reindexer::KeyValueType::Double{}, true);
}

TEST_F(NsApi, TestBoolIndexedFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(static_cast<int>(100500))}, {Variant(true)},
						 reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(static_cast<int64_t>(100500))}, {Variant(true)},
						 reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(static_cast<double>(100500.1))}, {Variant(true)},
						 reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(std::string("1"))}, {Variant(true)},
						 reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(std::string("-70.0e-3"))}, {Variant(true)},
						 reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(std::string("0"))}, {Variant(false)},
						 reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(std::string("TrUe"))}, {Variant(true)},
						 reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(std::string("fALsE"))}, {Variant(false)},
						 reindexer::KeyValueType::Bool{}, false);
}

TEST_F(NsApi, TestStringIndexedFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(rt.reindexer, default_namespace, stringField, {Variant(static_cast<int>(100500))}, {Variant("100500")},
						 reindexer::KeyValueType::String{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, stringField, {Variant(true)}, {Variant(std::string("true"))},
						 reindexer::KeyValueType::String{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, stringField, {Variant(false)}, {Variant(std::string("false"))},
						 reindexer::KeyValueType::String{}, false);
}

TEST_F(NsApi, TestIntNonindexedFieldConversion) {
	DefineDefaultNamespace();
	AddUnindexedData();

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<double>(13.33f))},
						 {Variant(static_cast<double>(13.33f))}, reindexer::KeyValueType::Double{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<int>(13))},
						 {Variant(static_cast<int64_t>(13))}, reindexer::KeyValueType::Int64{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<bool>(false))},
						 {Variant(static_cast<bool>(false))}, reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<bool>(true))},
						 {Variant(static_cast<bool>(true))}, reindexer::KeyValueType::Bool{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(std::string("100500"))},
						 {Variant(std::string("100500"))}, reindexer::KeyValueType::String{}, false);
	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(std::string("Jesus Christ"))},
						 {Variant(std::string("Jesus Christ"))}, reindexer::KeyValueType::String{}, false);
}

TEST_F(NsApi, TestIndexedArrayFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(
		rt.reindexer, default_namespace, indexedArrayField,
		{Variant(static_cast<double>(1.33f)), Variant(static_cast<double>(2.33f)), Variant(static_cast<double>(3.33f)),
		 Variant(static_cast<double>(4.33f))},
		{Variant(static_cast<int>(1)), Variant(static_cast<int>(2)), Variant(static_cast<int>(3)), Variant(static_cast<int>(4))},
		reindexer::KeyValueType::Int{}, false);
}

TEST_F(NsApi, TestNonIndexedArrayFieldConversion) {
	DefineDefaultNamespace();
	AddUnindexedData();

	VariantArray value{Variant(3.33f), Variant(4.33), Variant(5.33), Variant(6.33)};
	checkFieldConversion(rt.reindexer, default_namespace, "array_field", value, value, reindexer::KeyValueType::Undefined{}, false);
}

TEST_F(NsApi, TestUpdatePkFieldNoConditions) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	auto qrCount = rt.ExecSQL("select count(*) from test_namespace");
	auto qr = rt.ExecSQL("update test_namespace set id = id + " + std::to_string(qrCount.TotalCount() + 100));
	ASSERT_GT(qr.Count(), 0);
	int i = 0;
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		Variant intFieldVal = item[idIdxName];
		ASSERT_EQ(static_cast<int>(intFieldVal), i + qrCount.TotalCount() + 100);
		i++;
	}
}

TEST_F(NsApi, TestUpdateIndexArrayWithNull) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	auto qr = rt.ExecSQL("update test_namespace set indexed_array_field = null where id = 1;");
	ASSERT_EQ(qr.Count(), 1);
	for (auto& it : qr) {
		Item item(it.GetItem(false));
		VariantArray fieldVal = item[indexedArrayField];
		ASSERT_TRUE(fieldVal.empty());
	}
}

TEST_F(NsApi, TestUpdateIndexToSparse) {
	rt.OpenNamespace(default_namespace);
	const std::string compIndexName = idIdxName + "+" + stringField;

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{intField, "hash", "int", IndexOpts(), 0},
											   IndexDeclaration{stringField, "hash", "string", IndexOpts(), 0},
											   IndexDeclaration{compIndexName, "hash", "composite", IndexOpts(), 0}});
	Item item(NewItem(default_namespace));
	const int i = rand() % 20;
	item[idIdxName] = i * 2;
	item[intField] = i;
	item[stringField] = "str_" + std::to_string(i * 5);
	Upsert(default_namespace, item);

	auto qr = rt.Select(Query(default_namespace).Where(intField, CondEq, i));
	ASSERT_EQ(qr.Count(), 1);

	qr = rt.Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}));
	ASSERT_EQ(qr.Count(), 1);

	auto newIdx = reindexer::IndexDef(intField, "hash", "int", IndexOpts().Sparse());
	rt.UpdateIndex(default_namespace, newIdx);

	qr = rt.Select(Query(default_namespace).Where(intField, CondEq, i));
	ASSERT_EQ(qr.Count(), 1);

	qr = rt.Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}));
	ASSERT_EQ(qr.Count(), 1);

	newIdx = reindexer::IndexDef(compIndexName, {idIdxName, stringField}, "hash", "composite", IndexOpts().Sparse());
	auto err = rt.reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_EQ(err.code(), errParams) << err.what();
	ASSERT_STREQ(err.what(), "Composite index cannot be sparse. Use non-sparse composite instead");
	// Sparse composite do not have any purpose, so just make sure this index was not affected by updateIndex

	qr = rt.Select(Query(default_namespace).Where(intField, CondEq, i));
	ASSERT_EQ(qr.Count(), 1);

	qr = rt.Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}));
	ASSERT_EQ(qr.Count(), 1);

	newIdx = reindexer::IndexDef(intField, "hash", "int", IndexOpts());
	rt.UpdateIndex(default_namespace, newIdx);

	qr = rt.Select(Query(default_namespace).Where(intField, CondEq, i));
	ASSERT_EQ(qr.Count(), 1);

	qr = rt.Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}));
	ASSERT_EQ(qr.Count(), 1);

	newIdx = reindexer::IndexDef(compIndexName, {idIdxName, stringField}, "hash", "composite", IndexOpts());
	rt.UpdateIndex(default_namespace, newIdx);

	qr = rt.Select(Query(default_namespace).Where(intField, CondEq, i));
	ASSERT_EQ(qr.Count(), 1);

	qr = rt.Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}));
	ASSERT_EQ(qr.Count(), 1);
}

TEST_F(NsApi, TestUpdateNonIndexFieldWithNull) {
	DefineDefaultNamespace();
	AddUnindexedData();

	auto qr = rt.ExecSQL("update test_namespace set extra = null where id = 1001;");
	ASSERT_EQ(qr.Count(), 1);

	for (auto& it : qr) {
		Item item(it.GetItem(false));
		Variant fieldVal = item["extra"];
		ASSERT_TRUE(fieldVal.Type().Is<reindexer::KeyValueType::Null>());
	}
}

TEST_F(NsApi, TestUpdateIndexedFieldWithNull) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qr;
	auto err = rt.reindexer->ExecSQL("update test_namespace set string_field = null where id = 1;", qr);
	EXPECT_FALSE(err.ok());
}

TEST_F(NsApi, TestUpdateEmptyArrayField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	auto qr = rt.ExecSQL("update test_namespace set indexed_array_field = [] where id = 1;");
	ASSERT_EQ(qr.Count(), 1);

	Item item(qr.begin().GetItem(false));
	ASSERT_EQ(item[idIdxName].As<int>(), 1);

	VariantArray arrayFieldVal = item[indexedArrayField];
	ASSERT_EQ(arrayFieldVal.size(), 0);
}

// Update 2 fields with one query in this order: object field, ordinary field of type String
// https://github.com/restream/reindexer/-/tree/issue_777
TEST_F(NsApi, TestUpdateObjectFieldWithScalar) {
	// Define namespace's schema and fill with data
	DefineDefaultNamespace();
	AddUnindexedData();

	// Prepare and execute Update query
	Query q = Query(default_namespace)
				  .Set("int_field", 7)
				  .Set("extra", 8)
				  .SetObject("nested2", Variant(std::string(R"({"bonus2":13,"extra2":"new"})")));
	auto qr = rt.UpdateQR(q);
	ASSERT_GT(qr.Count(), 0);

	// Check in the loop that all the updated fields have correct values
	for (auto it : qr) {
		reindexer::Item item = it.GetItem(false);

		Variant intVal = item["int_field"];
		ASSERT_TRUE(intVal.Type().Is<reindexer::KeyValueType::Int>());
		ASSERT_EQ(intVal.As<int>(), 7);
		Variant extraVal = item["extra"];
		ASSERT_TRUE(extraVal.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(extraVal.As<int>(), 8);

		std::string_view json = item.GetJSON();
		std::string_view::size_type pos = json.find(R"("nested2":{"bonus2":13,"extra2":"new"})");
		ASSERT_TRUE(pos != std::string_view::npos);

		Variant bonus2Val = item["nested2.bonus2"];
		ASSERT_TRUE(bonus2Val.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_EQ(bonus2Val.As<int>(), 13);
		Variant extra2Val = item["nested2.extra2"];
		ASSERT_TRUE(extra2Val.Type().Is<reindexer::KeyValueType::String>());
		ASSERT_EQ(extra2Val.As<std::string>(), "new");
	}
}

TEST_F(NsApi, TestUpdateEmptyIndexedField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	Query q = Query(default_namespace)
				  .Where("id", CondEq, Variant(1001))
				  .Set(emptyField, Variant("NEW GENERATION"))
				  .Set(indexedArrayField, {Variant(static_cast<int>(4)), Variant(static_cast<int>(5)), Variant(static_cast<int>(6))});
	auto cnt = rt.Update(q);
	ASSERT_EQ(cnt, 1);

	auto qr2 = rt.ExecSQL("select * from test_namespace where id = 1001;");
	ASSERT_EQ(qr2.Count(), 1);
	for (auto it : qr2) {
		Item item = it.GetItem(false);

		Variant val = item[emptyField];
		ASSERT_TRUE(val.As<std::string>() == "NEW GENERATION");

		std::string_view json = item.GetJSON();
		ASSERT_TRUE(json.find_first_of("\"empty_field\":\"NEW GENERATION\"") != std::string::npos);

		VariantArray arrayVals = item[indexedArrayField];
		ASSERT_EQ(arrayVals.size(), 3);
		ASSERT_EQ(arrayVals[0].As<int>(), 4);
		ASSERT_EQ(arrayVals[1].As<int>(), 5);
		ASSERT_EQ(arrayVals[2].As<int>(), 6);
	}
}

TEST_F(NsApi, TestDropField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	auto qr = rt.ExecSQL("update test_namespace drop extra where id >= 1000 and id < 1010;");
	ASSERT_EQ(qr.Count(), 10);
	for (auto it : qr) {
		Item item(it.GetItem(false));
		VariantArray val = item["extra"];
		EXPECT_TRUE(val.empty());
		EXPECT_TRUE(item.GetJSON().find("extra") == std::string::npos);
	}

	auto qr2 = rt.ExecSQL("update test_namespace drop nested.bonus where id >= 1005 and id < 1010;");
	ASSERT_EQ(qr2.Count(), 5);
	for (auto it : qr2) {
		Item item(it.GetItem(false));
		VariantArray val = item["nested.bonus"];
		EXPECT_TRUE(val.empty());
		EXPECT_TRUE(item.GetJSON().find("nested.bonus") == std::string::npos);
	}

	QueryResults qr3;
	auto err = rt.reindexer->ExecSQL("update test_namespace drop string_field where id >= 1000 and id < 1010;", qr3);
	ASSERT_FALSE(err.ok());

	auto qr4 = rt.ExecSQL("update test_namespace drop nested2 where id >= 1030 and id <= 1040;");
	for (auto it : qr4) {
		Item item(it.GetItem(false));
		EXPECT_TRUE(item.GetJSON().find("nested2") == std::string::npos);
	}
}

TEST_F(NsApi, TestUpdateFieldWithFunction) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	int64_t updateTime = std::chrono::duration_cast<std::chrono::milliseconds>(reindexer::system_clock_w::now().time_since_epoch()).count();

	auto qr = rt.ExecSQL("update test_namespace set int_field = SERIAL(), extra = SERIAL(), nested.timeField = NOW(msec) where id >= 0;");
	ASSERT_GT(qr.Count(), 0);

	int i = 1;
	for (auto& it : qr) {
		Item item(it.GetItem(false));
		Variant intFieldVal = item[intField];
		Variant extraFieldVal = item["extra"];
		Variant timeFieldVal = item["nested.timeField"];
		ASSERT_EQ(intFieldVal.As<int>(), i++);
		ASSERT_EQ(intFieldVal.As<int>(), extraFieldVal.As<int>());
		ASSERT_GE(timeFieldVal.As<int64_t>(), updateTime);
	}
}

TEST_F(NsApi, TestUpdateFieldWithExpressions) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	auto qr = rt.ExecSQL(
		"update test_namespace set int_field = ((7+8)*(4-3))/3, extra = (SERIAL() + 1)*3, nested.timeField = int_field - 1 where id >= "
		"0;");
	ASSERT_GT(qr.Count(), 0);

	int i = 1;
	for (auto& it : qr) {
		Item item(it.GetItem(false));
		Variant intFieldVal = item[intField];
		Variant extraFieldVal = item["extra"];
		Variant timeFieldVal = item["nested.timeField"];
		ASSERT_EQ(intFieldVal.As<int>(), 5);
		ASSERT_EQ(extraFieldVal.As<int>(), (i + 1) * 3);
		ASSERT_EQ(timeFieldVal.As<int>(), 4);
		++i;
	}
}

static void checkQueryDsl(const Query& src) {
	const std::string dsl = src.GetJSON();
	Query dst;
	EXPECT_NO_THROW(dst = Query::FromJSON(dsl));
	bool objectValues = false;
	if (src.UpdateFields().size() > 0) {
		EXPECT_TRUE(src.UpdateFields().size() == dst.UpdateFields().size());
		for (size_t i = 0; i < src.UpdateFields().size(); ++i) {
			if (src.UpdateFields()[i].Mode() == FieldModeSetJson) {
				ASSERT_EQ(src.UpdateFields()[i].Values().size(), 1);
				EXPECT_TRUE(src.UpdateFields()[i].Values().front().Type().Is<reindexer::KeyValueType::String>());
				ASSERT_EQ(dst.UpdateFields()[i].Values().size(), 1);
				EXPECT_TRUE(dst.UpdateFields()[i].Values().front().Type().Is<reindexer::KeyValueType::String>());
				reindexer::WrSerializer wrser1;
				reindexer::prettyPrintJSON(std::string_view(src.UpdateFields()[i].Values().front()), wrser1);
				reindexer::WrSerializer wrser2;
				reindexer::prettyPrintJSON(std::string_view(dst.UpdateFields()[i].Values().front()), wrser2);
				EXPECT_EQ(wrser1.Slice(), wrser2.Slice());
				objectValues = true;
			}
		}
	}
	if (objectValues) {
		EXPECT_EQ(src.Entries(), dst.Entries());
		EXPECT_EQ(src.aggregations_, dst.aggregations_);
		EXPECT_EQ(src.NsName(), dst.NsName());
		EXPECT_EQ(src.GetSortingEntries(), dst.GetSortingEntries());
		EXPECT_EQ(src.CalcTotal(), dst.CalcTotal());
		EXPECT_EQ(src.Offset(), dst.Offset());
		EXPECT_EQ(src.Limit(), dst.Limit());
		EXPECT_EQ(src.GetDebugLevel(), dst.GetDebugLevel());
		EXPECT_EQ(src.GetStrictMode(), dst.GetStrictMode());
		EXPECT_EQ(src.ForcedSortOrder(), dst.ForcedSortOrder());
		EXPECT_EQ(src.SelectFilters(), dst.SelectFilters());
		EXPECT_EQ(src.selectFunctions_, dst.selectFunctions_);
		EXPECT_EQ(src.GetJoinQueries(), dst.GetJoinQueries());
		EXPECT_EQ(src.GetMergeQueries(), dst.GetMergeQueries());
	} else {
		EXPECT_EQ(dst, src);
	}
}

TEST_F(NsApi, TestModifyQueriesSqlEncoder) {
	constexpr std::string_view sqlUpdate =
		"UPDATE ns SET field1 = 'mrf',field2 = field2+1,field3 = ['one','two','three','four','five'] WHERE a = true AND location = "
		"'msk'";
	Query q1 = Query::FromSQL(sqlUpdate);
	EXPECT_EQ(q1.GetSQL(), sqlUpdate);
	checkQueryDsl(q1);

	constexpr std::string_view sqlDrop = "UPDATE ns DROP field1,field2 WHERE a = true AND location = 'msk'";
	Query q2 = Query::FromSQL(sqlDrop);
	EXPECT_EQ(q2.GetSQL(), sqlDrop);
	checkQueryDsl(q2);

	constexpr std::string_view sqlUpdateWithObject =
		R"(UPDATE ns SET field = {"id":0,"name":"apple","price":1000,"nested":{"n_id":1,"desription":"good","array":[{"id":1,"description":"first"},{"id":2,"description":"second"},{"id":3,"description":"third"}]},"bonus":7} WHERE a = true AND location = 'msk')";
	Query q3 = Query::FromSQL(sqlUpdateWithObject);
	EXPECT_EQ(q3.GetSQL(), sqlUpdateWithObject);
	checkQueryDsl(q3);

	constexpr std::string_view sqlTruncate = R"(TRUNCATE ns)";
	Query q4 = Query::FromSQL(sqlTruncate);
	EXPECT_EQ(q4.GetSQL(), sqlTruncate);
	checkQueryDsl(q4);

	constexpr std::string_view sqlArrayAppend = R"(UPDATE ns SET array = array||[1,2,3]||array2||objects[0].nested.prices[0])";
	Query q5 = Query::FromSQL(sqlArrayAppend);
	EXPECT_EQ(q5.GetSQL(), sqlArrayAppend);
	checkQueryDsl(q5);

	constexpr std::string_view sqlIndexUpdate = R"(UPDATE ns SET objects[0].nested.prices[*] = 'NE DOROGO!')";
	Query q6 = Query::FromSQL(sqlIndexUpdate);
	EXPECT_EQ(q6.GetSQL(), sqlIndexUpdate);
	checkQueryDsl(q6);

	constexpr std::string_view sqlSpeccharsUpdate = R"(UPDATE ns SET f1 = 'HELLO\n\r\b\f',f2 = '\t',f3 = '\"')";
	Query q7 = Query::FromSQL(sqlSpeccharsUpdate);
	EXPECT_EQ(q7.GetSQL(), sqlSpeccharsUpdate);
	checkQueryDsl(q7);

	{
		// Check from #674
		Query q = Query::FromSQL(
			"explain select id, name, count(*) from ns where (a = 100 and b = 10 equal_position(a,b)) or (c < 10 and d = 77 "
			"equal_position(c,d)) inner join (select * from ns2 where not a == 0) on ns.id == ns2.id order by id limit 100 offset 10");
		q.Merge(Query("ns3"));
		q.Merge(Query("ns4"));
		Query dst;
		EXPECT_NO_THROW(dst = Query::FromJSON(q.GetJSON()));
		ASSERT_EQ(q.GetSQL(), dst.GetSQL());
	}
}

static void generateObject(reindexer::JsonBuilder& builder, const std::string& prefix, ReindexerApi* rtapi) {
	builder.Put(prefix + "ID", rand() % 1000);
	builder.Put(prefix + "Name", rtapi->RandString());
	builder.Put(prefix + "Rating", rtapi->RandString());
	builder.Put(prefix + "Description", rtapi->RandString());
	builder.Put(prefix + "Price", rand() % 1000 + 100);
	builder.Put(prefix + "IMDB", 7.77777777777f);
	builder.Put(prefix + "Subsription", bool(rand() % 100 > 50 ? 1 : 0));
	{
		auto idsArray = builder.Array(prefix + "IDS");
		for (auto id : rtapi->RandIntVector(10, 10, 1000)) {
			idsArray.Put(reindexer::TagName::Empty(), id);
		}
	}
	{
		auto homogeneousArray = builder.Array(prefix + "HomogeneousValues");
		for (int i = 0; i < 20; ++i) {
			if (i % 2 == 0) {
				homogeneousArray.Put(reindexer::TagName::Empty(), rand());
			} else {
				if (i % 5 == 0) {
					homogeneousArray.Put(reindexer::TagName::Empty(), 234.778f);
				} else {
					homogeneousArray.Put(reindexer::TagName::Empty(), rtapi->RandString());
				}
			}
		}
	}
}

void addObjectsArray(reindexer::JsonBuilder& builder, bool withInnerArray, ReindexerApi* rtapi) {
	size_t size = rand() % 10 + 5;
	reindexer::JsonBuilder array = builder.Array("object");
	for (size_t i = 0; i < size; ++i) {
		reindexer::JsonBuilder obj = array.Object();
		generateObject(obj, "item", rtapi);
		if (withInnerArray && i % 5 == 0) {
			addObjectsArray(obj, false, rtapi);
		}
	}
}

TEST_F(NsApi, MsgPackEncodingTest) {
	DefineDefaultNamespace();

	reindexer::WrSerializer wrSer1;
	std::vector<std::string> items;
	for (int i = 0; i < 100; ++i) {
		reindexer::WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser);
		jsonBuilder.Put("id", i);
		jsonBuilder.Put("sparse_field", rand() % 1000);
		jsonBuilder.Put("superID", i * 2);
		jsonBuilder.Put("superName", RandString());
		{
			auto priceArray = jsonBuilder.Array("superPrices");
			for (auto price : RandIntVector(10, 10, 1000)) {
				priceArray.Put(reindexer::TagName::Empty(), price);
			}
		}
		{
			reindexer::JsonBuilder objectBuilder = jsonBuilder.Object("nested1");
			generateObject(objectBuilder, "nested1", this);
			addObjectsArray(objectBuilder, true, this);
		}
		jsonBuilder.Put("superBonus", RuRandString());
		addObjectsArray(jsonBuilder, false, this);
		jsonBuilder.End();

		auto item = NewItem(default_namespace);
		auto err = item.FromJSON(wrser.Slice());
		ASSERT_TRUE(err.ok()) << err.what() << "; " << wrser.Slice();
		Upsert(default_namespace, item);

		reindexer::WrSerializer wrSer2;
		err = item.GetMsgPack(wrSer2);
		ASSERT_TRUE(err.ok()) << err.what();

		err = item.GetMsgPack(wrSer1);
		ASSERT_TRUE(err.ok()) << err.what();

		size_t offset = 0;
		Item item2 = NewItem(default_namespace);
		err = item2.FromMsgPack(std::string_view(reinterpret_cast<const char*>(wrSer2.Buf()), wrSer2.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		std::string_view json1(item.GetJSON());
		std::string_view json2(item2.GetJSON());
		ASSERT_EQ(json1, json2);
		items.emplace_back(json2);
	}

	QueryResults qr;
	int i = 0;
	size_t length = wrSer1.Len();
	size_t offset = 0;
	while (offset < length) {
		Item item(NewItem(default_namespace));
		auto err = item.FromMsgPack(std::string_view(reinterpret_cast<const char*>(wrSer1.Buf()), wrSer1.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		rt.Update(default_namespace, item, qr);
		std::string_view json(item.GetJSON());
		ASSERT_EQ(json, items[i++]);
	}

	reindexer::WrSerializer wrSer3;
	for (auto& it : qr) {
		const auto err = it.GetMsgPack(wrSer3, false);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	i = 0;
	offset = 0;
	while (offset < length) {
		Item item(NewItem(default_namespace));
		auto err = item.FromMsgPack(std::string_view(reinterpret_cast<const char*>(wrSer3.Buf()), wrSer3.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		std::string_view json(item.GetJSON());
		ASSERT_EQ(json, items[i++]);
	}
}

TEST_F(NsApi, MsgPackFromJson) {
	DefineDefaultNamespace();
	constexpr std::string_view json = R"xxx({
				"total_us": 100,
				"prepare_us": 12,
				"indexes_us": 48,
				"postprocess_us": 6,
				"loop_us": 32,
				"general_sort_us": 0,
				"sort_index": "-",
				"sort_by_uncommitted_index": false,
				"selectors": [
					{
						"field": "search",
						"keys": 1,
						"comparators": 0,
						"cost": 18446744073709552000,
						"matched": 90,
						"method": "index",
						"type": "Unsorted"
					}
				]
			})xxx";
	reindexer::WrSerializer msgpackSer;
	reindexer::MsgPackBuilder msgpackBuilder(msgpackSer, reindexer::ObjType::TypeObject, 1);
	msgpackBuilder.Json("my_json", json);
	msgpackBuilder.End();

	reindexer::WrSerializer jsonSer;
	reindexer::JsonBuilder jsonBuilder(jsonSer);
	jsonBuilder.Json("my_json", json);
	jsonBuilder.End();

	Item item1 = NewItem(default_namespace);
	size_t offset = 0;
	Error err = item1.FromMsgPack(msgpackSer.Slice(), offset);
	ASSERT_TRUE(err.ok()) << err.what();

	Item item2 = NewItem(default_namespace);
	err = item2.FromJSON(jsonSer.Slice());
	ASSERT_TRUE(err.ok()) << err.what();

	std::string_view json1(item1.GetJSON());
	std::string_view json2(item2.GetJSON());
	ASSERT_EQ(json1, json2);
}

TEST_F(NsApi, DeleteLastItems) {
	// Check for bug with memory access after items removing
	DefineDefaultNamespace();
	FillDefaultNamespace(2);
	auto cnt = rt.Delete(Query(default_namespace));
	ASSERT_EQ(cnt, 2);
}

TEST_F(NsApi, IncorrectNsName) {
	auto check = [&](const std::vector<std::string_view>& names, auto func) {
		for (const auto& v : names) {
			func(v);
		}
	};
	std::vector<std::string_view> variants = {"tes@t1", "@test1", "test1@",	 "tes#t1",	  "#test1",		  "test1#", "test 1",
											  " test1", "test1 ", "'test1'", "\"test1\"", "<a>test1</a>", "/test1", "test1,test2"};

	auto open = [&](std::string_view name) {
		auto err = rt.reindexer->OpenNamespace(name);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.whatStr(),
				  fmt::format("Namespace name '{}' contains invalid character. Only alphas, digits,'_','-', are allowed", name));
	};
	check(variants, open);

	variants.emplace_back(reindexer::kConfigNamespace);
	auto add = [&](std::string_view name) {
		reindexer::NamespaceDef nsDef(name);
		auto err = rt.reindexer->AddNamespace(nsDef);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.whatStr(),
				  fmt::format("Namespace name '{}' contains invalid character. Only alphas, digits,'_','-', are allowed", name));
	};
	check(variants, add);

	auto rename = [&](std::string_view name) {
		const std::string_view kNsName("test3");
		reindexer::NamespaceDef nsDef(kNsName);
		rt.AddNamespace(nsDef);
		auto err = rt.reindexer->RenameNamespace(kNsName, std::string(name));
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.whatStr(),
				  fmt::format("Namespace name '{}' contains invalid character. Only alphas, digits,'_','-', are allowed", name));
		rt.DropNamespace(kNsName);
	};
	check(variants, rename);
}

TEST_F(NsApi, TwistNullUpdate) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"array_idx", "hash", "int", IndexOpts().Array().Sparse(true), 0}});

	rt.UpsertJSON(default_namespace, R"json({"id": 3, "array_idx": [1,1]}})json");

	const Query query = Query::FromSQL("UPDATE test_namespace SET array_idx = [null, null, null] WHERE id=3");
	rt.Update(query);
	// second update - force read\parsing
	rt.Update(query);
}

TEST_F(NsApi, MultiDimensionalArrayQueryErrors) {
	DefineDefaultNamespace();
	FillDefaultNamespace(10);
	const std::string indexedSparseArrayField = "indexed_sparse_array_field";
	rt.AddIndex(default_namespace,
				reindexer::IndexDef{indexedSparseArrayField, {indexedSparseArrayField}, "tree", "int", IndexOpts().Array().Sparse()});

	auto testSet = [this](std::string_view field) {
		SCOPED_TRACE(fmt::format("Running tests for '{}'", field));

		constexpr std::string_view kTupleErrorText =
			"Unable to use 'tuple'-value (array of arrays, array of points, etc) in UPDATE-query. Only single dimensional arrays and "
			"arrays of objects are supported";
		constexpr std::string_view kCompositeErrorText =
			"Unable to use 'composite'-value (object, array of objects, etc) in UPDATE-query. Probably 'object'/'json' type was not "
			"explicitly set in the query";

		QueryResults qr;
		auto& rx = *rt.reindexer;

		// Set tuple to the field
		auto err = rx.Update(Query(default_namespace).Set(field, {Variant{VariantArray::Create({1, 2, 3})}}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kTupleErrorText);

		// Set another tuple to the field
		qr.Clear();
		err = rx.Update(Query(default_namespace).Set(field, {Variant{VariantArray::Create({1})}}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kTupleErrorText);

		// Set empty tuple to the field
		qr.Clear();
		err = rx.Update(Query(default_namespace).Set(field, {Variant{VariantArray::Create(std::initializer_list<int>{})}}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kTupleErrorText);

		// Set tuple to the field
		qr.Clear();
		err = rx.Update(
			Query(default_namespace).Set(field, {Variant{VariantArray::Create({1, 2, 3})}, Variant{VariantArray::Create({5, 2})}}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kTupleErrorText);

		// Set array of tuple and int to the field
		qr.Clear();
		err = rx.Update(Query(default_namespace).Set(field, {Variant{1}, Variant{VariantArray::Create({5, 2})}}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kTupleErrorText);

		// Set composite to the field
		qr.Clear();
		err = rx.Update(Query(default_namespace).Set(field, {reindexer::PayloadValue()}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kCompositeErrorText);

		// Set array of composite and int to the field
		qr.Clear();
		err = rx.Update(Query(default_namespace).Set(field, {Variant{1}, Variant{reindexer::PayloadValue()}}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kCompositeErrorText);
	};

	testSet("some_non_idx_field_112211");
	testSet(indexedArrayField);
	testSet(indexedSparseArrayField);
}

#define EXPECT_EXCEPTION(x, code_val, text_val)                          \
	try {                                                                \
		x;                                                               \
		EXPECT_TRUE(false) << "Expecting an exception during this call"; \
	} catch (Error & e) {                                                \
		EXPECT_EQ(e.code(), code_val);                                   \
		EXPECT_EQ(e.what(), text_val);                                   \
	} catch (...) {                                                      \
		EXPECT_TRUE(false) << "Unexpected exception";                    \
	}

TEST_F(NsApi, MultiDimensionalArrayItemsErrors) {
	const std::string indexedSparseArrayField = "indexed_sparse_array_field";

	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace,
						   {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
							IndexDeclaration{indexedArrayField, "tree", "double", IndexOpts().Array(), 0},
							IndexDeclaration{indexedSparseArrayField, "tree", "double", IndexOpts().Array().Sparse(), 0}});

	auto testSet = [this](std::string_view field) {
		SCOPED_TRACE(fmt::format("Running tests for '{}'", field));

		constexpr std::string_view kCompositeErrorText("Unable to use 'composite'-value (object, array of objects, etc) to modify item");
		constexpr std::string_view kTupleErrorText("Unable to use 'tuple'-value (array of arrays, array of points, etc) to modify item");

		Item item(rt.NewItem(default_namespace));
		// Set tuple to the field
		EXPECT_EXCEPTION(item[field] = Variant{VariantArray::Create({1, 2, 3})}, errParams, kTupleErrorText);
		// Set another tuple to the field
		EXPECT_EXCEPTION(item[field] = Variant{VariantArray::Create({1})}, errParams, kTupleErrorText);
		// Set empty tuple to the field
		EXPECT_EXCEPTION(item[field] = Variant{VariantArray::Create(std::initializer_list<int>{})}, errParams, kTupleErrorText);
		// Set tuple to the field
		EXPECT_EXCEPTION(
			item[field] = VariantArray::Create(Variant{VariantArray::Create({1, 2, 3})}, Variant{VariantArray::Create({5, 2})}), errParams,
			kTupleErrorText);
		// Set array of tuple and int to the field
		// EXPECT_THROW, Error);
		EXPECT_EXCEPTION(item[field] = VariantArray::Create(Variant{1}, Variant{VariantArray::Create({5, 2})}), errParams, kTupleErrorText);
		// Set composite to the field
		EXPECT_EXCEPTION(item[field] = reindexer::PayloadValue(), errParams, kCompositeErrorText);
		// Set array of composite and int to the field
		EXPECT_EXCEPTION(item[field] = VariantArray::Create(Variant{1}, Variant{reindexer::PayloadValue()}), errParams,
						 kCompositeErrorText);
		// Set point-value to the field
		const auto kTestPoint = reindexer::Point(1.0, 2.0);
		EXPECT_NO_THROW(item[field] = kTestPoint);
		EXPECT_EQ(item[field].As<reindexer::Point>(), kTestPoint);
	};

	testSet("some_non_idx_field_112211");
	testSet(indexedArrayField);
	testSet(indexedSparseArrayField);
}

TEST_F(NsApi, CompositeUpdateWithJSON) {
	const std::string kCompositeIdxName = "composite_idx";
	const std::string kIntFieldPath = "object.value";
	constexpr std::string_view kItem1JSON = R"json({"id":1,"object":{"value":10}})json";
	constexpr std::string_view kItem2JSON = R"json({"id":2,"object":{"value":5}})json";
	constexpr std::string_view kExpectedResultJSON = R"json({"id":1,"object":{"value":5,"new_field":"str"}})json";

	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0}});
	rt.AddIndex(default_namespace, reindexer::IndexDef(intField, {kIntFieldPath}, "hash", "int", IndexOpts()));
	rt.AddIndex(default_namespace, reindexer::IndexDef(kCompositeIdxName, {intField}, "hash", "composite", IndexOpts()));

	rt.UpsertJSON(default_namespace, kItem1JSON);
	rt.UpsertJSON(default_namespace, kItem2JSON);

	reindexer::WrSerializer ser;
	auto qr = rt.UpdateQR(
		Query(default_namespace).SetObject("object", R"json({ "value": 5, "new_field": "str" })json").Where(idIdxName, CondEq, 1));
	ASSERT_EQ(qr.Count(), 1);
	auto err = qr.begin().GetJSON(ser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(ser.Slice(), kExpectedResultJSON);

	qr = rt.Select(Query(default_namespace).Where(kCompositeIdxName, CondEq, {VariantArray::Create({10})}));
	EXPECT_EQ(qr.Count(), 0) << qr.ToLocalQr().Dump();
	qr = rt.Select(Query(default_namespace).Where(kCompositeIdxName, CondEq, {VariantArray::Create({5})}).Sort(idIdxName, false));
	ASSERT_EQ(qr.Count(), 2) << qr.ToLocalQr().Dump();
	ser.Reset();
	err = qr.begin().GetJSON(ser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(ser.Slice(), kExpectedResultJSON);
	ser.Reset();
	err = (qr.begin() + 1).GetJSON(ser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(ser.Slice(), kItem2JSON);
}

TEST_F(NsApi, TagsmatchersMerge) {
	using namespace reindexer;

	std::vector<TagsMatcher> tms;

	tms.emplace_back();	 // -V760
	auto _ = tms.back().path2tag("id", CanAddField_True);
	_ = tms.back().path2tag("string", CanAddField_True);
	_ = tms.back().path2tag("data", CanAddField_True);
	_ = tms.back().path2tag("data.value", CanAddField_True);

	tms.emplace_back();
	_ = tms.back().path2tag("id", CanAddField_True);
	_ = tms.back().path2tag("string", CanAddField_True);
	_ = tms.back().path2tag("data", CanAddField_True);
	_ = tms.back().path2tag("data.value", CanAddField_True);
	_ = tms.back().path2tag("additional_data", CanAddField_True);

	tms.emplace_back();
	_ = tms.back().path2tag("id", CanAddField_True);
	_ = tms.back().path2tag("something_else", CanAddField_True);

	tms.emplace_back();
	_ = tms.back().path2tag("id", CanAddField_True);
	_ = tms.back().path2tag("string", CanAddField_True);
	_ = tms.back().path2tag("data", CanAddField_True);
	_ = tms.back().path2tag("data.value", CanAddField_True);
	_ = tms.back().path2tag("yet_another_additional_data", CanAddField_True);

	auto resultTm = TagsMatcher::CreateMergedTagsMatcher(tms);

	EXPECT_EQ(resultTm.name2tag("id"), 1_Tag);
	EXPECT_EQ(resultTm.name2tag("string"), 2_Tag);
	EXPECT_EQ(resultTm.name2tag("data"), 3_Tag);
	EXPECT_EQ(resultTm.name2tag("value"), 4_Tag);
	EXPECT_EQ(resultTm.name2tag("additional_data"), 5_Tag);
	EXPECT_EQ(resultTm.name2tag("something_else"), 6_Tag);
	EXPECT_EQ(resultTm.name2tag("yet_another_additional_data"), 7_Tag);

	EXPECT_TRUE(tms[0].IsSubsetOf(resultTm));
	EXPECT_TRUE(tms[1].IsSubsetOf(resultTm));
	EXPECT_FALSE(tms[2].IsSubsetOf(resultTm));
	EXPECT_FALSE(tms[3].IsSubsetOf(resultTm));
}

TEST_F(NsApi, SparseComparatorConversion) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace,
						   {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
							IndexDeclaration{"sparse_field", "hash", "string", IndexOpts().Sparse().SetCollateMode(CollateNumeric), 0}});

	rt.UpsertJSON(default_namespace, R"j({"id":0, "sparse_field":"100"})j");
	rt.UpsertJSON(default_namespace, R"j({"id":1, "sparse_field":99}")j");
	rt.UpsertJSON(default_namespace, R"j({"id":2, "sparse_field":null})j");
	rt.UpsertJSON(default_namespace, R"j({"id":3}")j");
	rt.UpsertJSON(default_namespace, R"j({"id":4, "sparse_field":"50"})j");

	auto qr = rt.Select(reindexer::Query(default_namespace).Where("sparse_field", CondLt, "100").Sort("id", false));
	// TODO: Sort("sparse_field", false)) does not works here #2039
	auto results = rt.GetSerializedQrItems(qr);
	ASSERT_EQ(results.size(), 2);
	EXPECT_EQ(results[0], R"j({"id":1,"sparse_field":"99"})j");
	EXPECT_EQ(results[1], R"j({"id":4,"sparse_field":"50"})j");

	qr = rt.Select(reindexer::Query(default_namespace).Where("sparse_field", CondGe, "100").Sort("sparse_field", false));
	results = rt.GetSerializedQrItems(qr);
	ASSERT_EQ(results.size(), 1);
	EXPECT_EQ(results[0], R"j({"id":0,"sparse_field":"100"})j");
}

TEST_F(NsApi, ArrayDistinct) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"idx", "hash", "int", IndexOpts().Array(), 0}});

	// clang-format off
	const std::vector<std::string_view> docs = {
		R"j({"id":0, "idx":[0, 10]})j",
		R"j({"id":1, "idx":[10, 0]}")j",
		R"j({"id":2, "idx":[1, 11]}")j",
		R"j({"id":3, "idx":[0,10]})j",
		R"j({"id":4, "idx":[0]})j",
		R"j({"id":5, "idx":[]})j",
		R"j({"id":6, "idx":[10]})j",
		R"j({"id":7, "idx":[77]})j",
		R"j({"id":8, "idx":[1, 10, 45]})j",
		R"j({"id":9, "idx":[99]})j",
		R"j({"id":10, "idx":[100]})j"
	};
	// clang-format on

	for (auto& doc : docs) {
		rt.UpsertJSON(default_namespace, doc);
	}

	struct Case {
		const std::string_view name;
		const Query query;
		const std::set<int> expectedIDs;
		const std::string_view expectedAggString;
	};
	const std::vector<Case> cases = {
		Case{.name = "no limit, no offset",
			 .query = reindexer::Query(default_namespace).Distinct("idx"),
			 .expectedIDs = {0, 2, 7, 8, 9, 10},
			 .expectedAggString = R"j(.*"distincts":\["0","1","77","10","11","45","99","100"\].*)j"},
		Case{.name = "no limit, with offset",
			 .query = reindexer::Query(default_namespace).Distinct("idx").Offset(2),
			 .expectedIDs = {7, 8, 9, 10},
			 .expectedAggString = R"j(.*"distincts":\["1","77","10","45","99","100"\].*)j"},
		Case{.name = "with limit, no offset",
			 .query = reindexer::Query(default_namespace).Distinct("idx").Limit(4),
			 .expectedIDs = {0, 2, 7, 8},
			 .expectedAggString = R"j(.*"distincts":\["0","1","77","10","11","45"\].*)j"},
		Case{.name = "with limit, with offset",
			 .query = reindexer::Query(default_namespace).Distinct("idx").Offset(1).Limit(2),
			 .expectedIDs = {2, 7},
			 .expectedAggString = R"j(.*"distincts":\["1","77","11"\].*)j"},
	};

	for (auto& c : cases) {
		SCOPED_TRACE(c.name);
		auto expectedIDs = c.expectedIDs;
		auto qr = rt.Select(c.query);
		for (auto& it : qr) {
			auto item = it.GetItem();
			if (!expectedIDs.erase(item["id"].As<int>())) {
				EXPECT_TRUE(false) << "Unexpected item: " << item.GetJSON();
			}
		}
		for (auto& exp : expectedIDs) {
			EXPECT_TRUE(false) << "Missing item: " << docs[exp];
		}
		reindexer::WrSerializer ser;
		qr.GetAggregationResults()[0].GetJSON(ser);
		EXPECT_THAT(ser.Slice(), testing::MatchesRegex(c.expectedAggString)) << ser.Slice();
	}
}

TEST(AsyncStorage, SyncReadSimpleTest) {
	using namespace reindexer;
	constexpr static int kTestBatchSize = AsyncStorage::kFlushChunckSize + 100;

	AsyncStorage storage;
	const auto kStoragePath = fs::JoinPath(fs::GetTempDir(), "AsyncStorage.SyncReadSimpleTest/");
	std::ignore = fs::RmDirAll(kStoragePath);
	auto err = storage.Open(datastorage::StorageType::LevelDB, {}, kStoragePath, StorageOpts{}.CreateIfMissing());
	ASSERT_TRUE(err.ok()) << err.what();

	auto test = [&storage]() {
		enum [[nodiscard]] State { BeforeFlush, AfterFlush, AfterRemove };
		auto read = [&](State state) {
			for (int i = 0; i < kTestBatchSize; ++i) {
				const bool mustBeFound = state == AfterFlush || (storage.WithProxy() && state == BeforeFlush);
				auto expected = mustBeFound ? std::to_string(i) : std::string{};

				std::string value;
				auto err = storage.Read(StorageOpts{}, std::to_string(i), value);
				ASSERT_EQ(err.code(), mustBeFound ? errOK : errNotFound) << err.what();
				ASSERT_EQ(value, expected);
			}
		};

		for (int i = 0; i < kTestBatchSize; ++i) {
			storage.Write(std::to_string(i), std::to_string(i));
		}

		read(BeforeFlush);
		storage.Flush(StorageFlushOpts{});
		read(AfterFlush);

		for (int i = 0; i < kTestBatchSize; ++i) {
			if (storage.WithProxy()) {
				storage.Remove(std::to_string(i));
			} else {
				storage.RemoveSync(StorageOpts{}, std::to_string(i));
			}
		}
		read(AfterRemove);
	};

	test();
	storage.WithProxy(true);
	test();
}

TEST(AsyncStorage, SyncReadConcurrentTest) {
	using namespace reindexer;
	constexpr static int kTestBatchSize = AsyncStorage::kFlushChunckSize + 100;
#ifndef REINDEX_WITH_TSAN
	const size_t limit = 1'000'000;
#else
	const size_t limit = 100'000;
#endif
	AsyncStorage storage;
	const auto kStoragePath = fs::JoinPath(fs::GetTempDir(), "AsyncStorage.SyncReadConcurrentTest/");
	std::ignore = fs::RmDirAll(kStoragePath);
	auto err = storage.Open(datastorage::StorageType::LevelDB, {}, kStoragePath, StorageOpts{}.CreateIfMissing());
	ASSERT_TRUE(err.ok()) << err.what();

	storage.WithProxy(true);

	std::atomic_bool stopped = false;
	auto flushThread = std::thread([&storage, &stopped] {
		while (!stopped) {
			storage.Flush(StorageFlushOpts{});
			std::this_thread::sleep_for(std::chrono::milliseconds(200 + std::rand() % 100));
		}

		storage.Flush(StorageFlushOpts{});
	});

	std::atomic<size_t> inserted = 0;

	auto writeReadThread = std::thread([&inserted, &stopped, &storage] {
		while (inserted.load(std::memory_order_acquire) < limit) {
			auto from = inserted.load(std::memory_order_acquire);
			auto to = from + kTestBatchSize;
			for (size_t i = from; i < to; ++i) {
				storage.Write(std::to_string(i), std::to_string(i));
				inserted.fetch_add(1, std::memory_order_release);
			}

			Error err;
			std::string value;
			for (size_t i = from; i < to; ++i) {
				err = storage.Read(StorageOpts{}, std::to_string(i), value);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(value, std::to_string(i));
			}

			std::this_thread::sleep_for(std::chrono::milliseconds(50 + std::rand() % 50));
		}
		stopped = true;
	});

	auto readThread = std::thread([&inserted, &stopped, &storage] {
		while (!stopped) {
			auto lim = inserted.load(std::memory_order_acquire);
			auto from = lim ? std::rand() % lim : 0;
			auto to = std::min(from + 10 * kTestBatchSize, lim);
			std::string value;
			for (size_t i = from; i < to; ++i) {
				auto err = storage.Read(StorageOpts{}, std::to_string(i), value);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(value, std::to_string(i));
			}
			// TODO: Delete this sleep when switching to RocksDB.
			// Right now, a read timeout is needed in order to avoid read-write degradation inside leveldb lib(#2193).
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	});

	writeReadThread.join();
	readThread.join();
	flushThread.join();

	std::string value;
	for (size_t i = 0; i < inserted; ++i) {
		err = storage.Read(StorageOpts{}, std::to_string(i), value);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(std::to_string(i), value);
	}

	for (size_t i = 0; i < inserted; ++i) {
		storage.RemoveSync(StorageOpts{}, std::to_string(i));
	}

	for (size_t i = 0; i < inserted; ++i) {
		err = storage.Read(StorageOpts{}, std::to_string(i), value);
		ASSERT_EQ(err.code(), errNotFound) << err.what();
	}
}

TEST(AsyncStorage, ConsistReadWriteRemoveTest) {
	using namespace reindexer;
	constexpr static int kTestBatchSize = AsyncStorage::kFlushChunckSize + 100;
	enum class [[nodiscard]] State { NotInit = -2, Unset = -1 };

	AsyncStorage storage;
	const auto kStoragePath = fs::JoinPath(fs::GetTempDir(), "AsyncStorage.SyncReadConcurrentTest/");
	std::ignore = fs::RmDirAll(kStoragePath);
	auto err = storage.Open(datastorage::StorageType::LevelDB, {}, kStoragePath, StorageOpts{}.CreateIfMissing());
	ASSERT_TRUE(err.ok()) << err.what();

	storage.WithProxy(true);

	const size_t limit = 100'000;
	std::vector<std::atomic<int>> checkKeyValues(limit);
	for (size_t i = 0; i < limit; ++i) {
		checkKeyValues[i] = int(State::NotInit);
	}

	std::atomic_bool stopped = false;
	auto flushThread = std::thread([&storage, &stopped] {
		while (!stopped) {
			storage.Flush(StorageFlushOpts{});
			std::this_thread::sleep_for(std::chrono::milliseconds(200 + std::rand() % 100));
		}

		storage.Flush(StorageFlushOpts{});
	});

	auto write = [&](int idx) {
		auto value = std::rand() % 1000;
		storage.Write(std::to_string(idx), std::to_string(value));
		checkKeyValues[idx].store(value, std::memory_order_release);
	};

	auto remove = [&](int idx) {
		storage.Remove(std::to_string(idx));
		checkKeyValues[idx].store(int(State::Unset), std::memory_order_release);
	};

	auto read = [&](int idx) {
		std::string value;
		auto expected = checkKeyValues[idx].load(std::memory_order_acquire);
		auto err = storage.Read(StorageOpts{}, std::to_string(idx), value);

		if (State(expected) == State::NotInit || State(expected) == State::Unset) {
			ASSERT_EQ(err.code(), errNotFound) << fmt::format("i = {}, expected={}, value = {} err = {}", idx, expected, value, err.what());
		} else {
			ASSERT_EQ(value, std::to_string(expected));
		}
	};

	using testScenario = std::vector<std::function<void(int)>>;
	std::vector<testScenario> scenarioChains{
		testScenario{write, read},		 testScenario{write, read, remove, read}, testScenario{remove, read, write, read},
		testScenario{read, write, read}, testScenario{read, remove, read},		  testScenario{read},
	};

	auto mainThread = std::thread([&] {
		while (!stopped) {
			const auto from = std::rand() % (limit - kTestBatchSize - 1);
			for (size_t i = from; i < from + kTestBatchSize; ++i) {
				auto scenario = scenarioChains[std::rand() % scenarioChains.size()];
				for (auto& op : scenario) {
					op(i);
				}
			}
		}
	});

	auto checkFillingThread = std::thread([&]() {
		while (!stopped) {
			auto filledCount = std::count_if(checkKeyValues.begin(), checkKeyValues.end(), [](const auto& value) {
				return value.load(std::memory_order_acquire) != int(State::NotInit);
			});
			if (float(filledCount) / float(limit) > 0.9) {
				stopped = true;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
		}
	});

	flushThread.join();
	mainThread.join();
	checkFillingThread.join();

	for (size_t i = 0; i < limit; ++i) {
		read(i);
	}
}
