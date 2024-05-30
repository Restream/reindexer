#include <string_view>
#include "core/cbinding/resultserializer.h"
#include "core/cjson/ctag.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"
#include "estl/fast_hash_set.h"
#include "estl/span.h"
#include "ns_api.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

TEST_F(NsApi, IndexDrop) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(
		default_namespace,
		{IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"date", "", "int64", IndexOpts(), 0},
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
		item["data"] = rand();
		item["price"] = rand();
		item["serialNumber"] = i * 100;
		item["fileName"] = "File" + std::to_string(i);
		item["ft11"] = RandString();
		item["ft12"] = RandString();
		item["ft21"] = RandString();
		item["ft22"] = RandString();
		item["ft23"] = RandString();
		auto err = rt.reindexer->Insert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	reindexer::IndexDef idef("price");
	err = rt.reindexer->DropIndex(default_namespace, idef);
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(NsApi, AddTooManyIndexes) {
	constexpr size_t kHalfOfStartNotCompositeIndexesCount = 80;
	constexpr size_t kMaxCompositeIndexesCount = 100;
	static const std::string ns = "too_many_indexes";
	Error err = rt.reindexer->OpenNamespace(ns);
	ASSERT_TRUE(err.ok()) << err.what();

	size_t notCompositeIndexesCount = 0;
	size_t compositeIndexesCount = 0;
	while (notCompositeIndexesCount < reindexer::kMaxIndexes - 1) {
		reindexer::IndexDef idxDef;
		if (notCompositeIndexesCount < 2 * kHalfOfStartNotCompositeIndexesCount || rand() % 4 != 0 ||
			compositeIndexesCount >= kMaxCompositeIndexesCount) {
			const std::string indexName = "index_" + std::to_string(notCompositeIndexesCount);
			idxDef = reindexer::IndexDef{indexName, {indexName}, "tree", "int", IndexOpts{}};
			++notCompositeIndexesCount;
		} else {
			const std::string firstSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount);
			const std::string secondSubIndex =
				"index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount + kHalfOfStartNotCompositeIndexesCount);
			const std::string indexName = std::string(firstSubIndex).append("+").append(secondSubIndex);
			idxDef = reindexer::IndexDef{indexName, {firstSubIndex, secondSubIndex}, "tree", "composite", IndexOpts{}};
			++compositeIndexesCount;
		}
		err = rt.reindexer->AddIndex(ns, idxDef);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	// Add composite index
	std::string firstSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount);
	std::string secondSubIndex =
		"index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount + kHalfOfStartNotCompositeIndexesCount);
	std::string indexName = std::string(firstSubIndex).append("+").append(secondSubIndex);
	err = rt.reindexer->AddIndex(ns, reindexer::IndexDef{indexName, {firstSubIndex, secondSubIndex}, "tree", "composite", IndexOpts{}});
	ASSERT_TRUE(err.ok()) << err.what();

	// Add non-composite index
	indexName = "index_" + std::to_string(notCompositeIndexesCount);
	err = rt.reindexer->AddIndex(ns, reindexer::IndexDef{indexName, {indexName}, "tree", "int", IndexOpts{}});
	ASSERT_FALSE(err.ok());
	ASSERT_EQ(err.what(),
			  "Cannot add index 'too_many_indexes.index_255'. Too many non-composite indexes. 255 non-composite indexes are allowed only");

	// Add composite index
	firstSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount);
	secondSubIndex = "index_" + std::to_string(rand() % kHalfOfStartNotCompositeIndexesCount + kHalfOfStartNotCompositeIndexesCount);
	indexName = std::string(firstSubIndex).append("+").append(secondSubIndex);
	err = rt.reindexer->AddIndex(ns, reindexer::IndexDef{indexName, {firstSubIndex, secondSubIndex}, "tree", "composite", IndexOpts{}});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(NsApi, TruncateNamespace) {
	TruncateNamespace([&](const std::string &nsName) { return rt.reindexer->TruncateNamespace(nsName); });
	TruncateNamespace([&](const std::string &nsName) {
		QueryResults qr;
		return rt.reindexer->Select("TRUNCATE " + nsName, qr);
	});
}

TEST_F(NsApi, UpsertWithPrecepts) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{updatedTimeSecFieldName.c_str(), "", "int64", IndexOpts(), 0},
											   IndexDeclaration{updatedTimeMSecFieldName.c_str(), "", "int64", IndexOpts(), 0},
											   IndexDeclaration{updatedTimeUSecFieldName.c_str(), "", "int64", IndexOpts(), 0},
											   IndexDeclaration{updatedTimeNSecFieldName.c_str(), "", "int64", IndexOpts(), 0},
											   IndexDeclaration{serialFieldName.c_str(), "", "int64", IndexOpts(), 0},
											   IndexDeclaration{stringField.c_str(), "text", "string", IndexOpts(), 0}});

	Item item = NewItem(default_namespace);
	item[idIdxName] = idNum;

	// Set precepts
	std::vector<std::string> precepts = {updatedTimeSecFieldName + "=NOW()",	  updatedTimeMSecFieldName + "=NOW(msec)",
										 updatedTimeUSecFieldName + "=NOW(usec)", updatedTimeNSecFieldName + "=NOW(nsec)",
										 serialFieldName + "=SERIAL()",			  stringField + "=SERIAL()"};
	item.SetPrecepts(precepts);

	// Upsert item a few times
	for (int i = 0; i < upsertTimes; i++) {
		auto err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// Get item
	reindexer::QueryResults res;
	err = rt.reindexer->Select("SELECT * FROM " + default_namespace + " WHERE id=" + std::to_string(idNum), res);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : res) {
		Item item = it.GetItem(false);
		for (auto idx = 1; idx < item.NumFields(); idx++) {
			auto field = item[idx].Name();

			if (field == updatedTimeSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("sec") - value < 1) << "Precept function `now()/now(sec)` doesn't work properly";
			} else if (field == updatedTimeMSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("msec") - value < 1000) << "Precept function `now(msec)` doesn't work properly";
			} else if (field == updatedTimeUSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("usec") - value < 1000000) << "Precept function `now(usec)` doesn't work properly";
			} else if (field == updatedTimeNSecFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(reindexer::getTimeNow("nsec") - value < 1000000000) << "Precept function `now(nsec)` doesn't work properly";
			} else if (field == serialFieldName) {
				int64_t value = item[field].Get<int64_t>();
				ASSERT_TRUE(value == upsertTimes) << "Precept function `serial()` didn't increment a value to " << upsertTimes << " after "
												  << upsertTimes << " upsert times";
			} else if (field == stringField) {
				auto value = item[field].Get<std::string_view>();
				ASSERT_TRUE(value == std::to_string(upsertTimes)) << "Precept function `serial()` didn't increment a value to "
																  << upsertTimes << " after " << upsertTimes << " upsert times";
			}
		}
	}
}

TEST_F(NsApi, ReturnOfItemChange) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{updatedTimeNSecFieldName.c_str(), "", "int64", IndexOpts(), 0},
											   IndexDeclaration{serialFieldName.c_str(), "", "int64", IndexOpts(), 0}});

	Item item = NewItem(default_namespace);
	item[idIdxName] = idNum;

	// Set precepts
	std::vector<std::string> precepts = {updatedTimeNSecFieldName + "=NOW(nsec)", serialFieldName + "=SERIAL()"};
	item.SetPrecepts(precepts);

	// Check Insert
	err = rt.reindexer->Insert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::QueryResults res1;
	err = rt.reindexer->Select("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum), res1);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(res1.Count(), 1);
	Item selectedItem = res1.begin().GetItem(false);
	CheckItemsEqual(item, selectedItem);

	// Check Update
	err = rt.reindexer->Update(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::QueryResults res2;
	err = rt.reindexer->Select("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum), res2);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(res2.Count(), 1);
	selectedItem = res2.begin().GetItem(false);
	CheckItemsEqual(item, selectedItem);

	// Check Delete
	err = rt.reindexer->Delete(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	CheckItemsEqual(item, selectedItem);

	// Check Upsert
	item[idIdxName] = idNum;
	err = rt.reindexer->Upsert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::QueryResults res3;
	err = rt.reindexer->Select("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum), res3);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(res3.Count(), 1);
	selectedItem = res3.begin().GetItem(false);
	CheckItemsEqual(item, selectedItem);
}

TEST_F(NsApi, UpdateIndex) {
	Error err = rt.reindexer->InitSystemNamespaces();
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0}});

	auto const wrongIdx = reindexer::IndexDef(idIdxName, reindexer::JsonPaths{"wrongPath"}, "hash", "double", IndexOpts().PK());
	err = rt.reindexer->UpdateIndex(default_namespace, wrongIdx);
	ASSERT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Unsupported combination of field 'id' type 'double' and index type 'hash'");

	auto newIdx = reindexer::IndexDef(idIdxName, "tree", "int64", IndexOpts().PK().Dense());
	err = rt.reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<reindexer::NamespaceDef> nsDefs;
	err = rt.reindexer->EnumNamespaces(nsDefs, reindexer::EnumNamespacesOpts());
	ASSERT_TRUE(err.ok()) << err.what();

	auto nsDefIt =
		std::find_if(nsDefs.begin(), nsDefs.end(), [&](const reindexer::NamespaceDef &nsDef) { return nsDef.name == default_namespace; });

	ASSERT_TRUE(nsDefIt != nsDefs.end()) << "Namespace " + default_namespace + " is not found";

	auto &indexes = nsDefIt->indexes;
	auto receivedIdx = std::find_if(indexes.begin(), indexes.end(), [&](const reindexer::IndexDef &idx) { return idx.name_ == idIdxName; });
	ASSERT_TRUE(receivedIdx != indexes.end()) << "Expect index was created, but it wasn't";

	reindexer::WrSerializer newIdxSer;
	newIdx.GetJSON(newIdxSer);

	reindexer::WrSerializer receivedIdxSer;
	receivedIdx->GetJSON(receivedIdxSer);

	auto newIdxJson = newIdxSer.Slice();
	auto receivedIdxJson = receivedIdxSer.Slice();

	ASSERT_TRUE(newIdxJson == receivedIdxJson);
}

TEST_F(NsApi, QueryperfstatsNsDummyTest) {
	Error err = rt.reindexer->InitSystemNamespaces();
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0}});

	const char *const configNs = "#config";
	Item item = NewItem(configNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	std::string newConfig = R"json({
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

	err = item.FromJSON(newConfig);
	ASSERT_TRUE(err.ok()) << err.what();

	Upsert(configNs, item);
	Commit(configNs);

	struct QueryPerformance {
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

	auto performSimpleQuery = [&]() {
		QueryResults qr;
		Error err = rt.reindexer->Select(testQuery, qr);
		ASSERT_TRUE(err.ok()) << err.what();
	};

	auto getPerformanceParams = [&](QueryPerformance &performanceRes) {
		QueryResults qres;
		Error err = rt.reindexer->Select(Query("#queriesperfstats").Where("query", CondEq, Variant(querySql)), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		if (qres.Count() == 0) {
			QueryResults qr;
			err = rt.reindexer->Select(Query("#queriesperfstats"), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_GT(qr.Count(), 0) << "#queriesperfstats table is empty!";
			for (size_t i = 0; i < qr.Count(); ++i) {
				std::cout << qr[i].GetItem(false).GetJSON() << std::endl;
			}
		}
		ASSERT_EQ(qres.Count(), 1);
		Item item = qres[0].GetItem(false);
		Variant val;
		val = item["latency_stddev"];
		performanceRes.latencyStddev = static_cast<double>(val);
		val = item["min_latency_us"];
		performanceRes.minLatencyUs = val.As<int64_t>();
		val = item["max_latency_us"];
		performanceRes.maxLatencyUs = val.As<int64_t>();
		val = item["query"];
		performanceRes.query = val.As<std::string>();
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

static void checkIfItemJSONValid(QueryResults::Iterator &it, bool print = false) {
	reindexer::WrSerializer wrser;
	Error err = it.GetJSON(wrser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	if (err.ok() && print) std::cout << wrser.Slice() << std::endl;
}

TEST_F(NsApi, TestUpdateIndexedField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qrUpdate;
	Query updateQuery{Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500))).Set(stringField, "bingo!")};
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qrAll;
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500))), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

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

	QueryResults qrUpdate;
	Query updateQuery{Query(default_namespace).Where("id", CondGe, Variant("1500")).Set("nested.bonus", static_cast<int>(100500))};
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qrUpdate.Count(), 500);

	QueryResults qrAll;
	err = rt.reindexer->Select(Query(default_namespace).Where("id", CondGe, Variant("1500")), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();
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

	QueryResults qrUpdate;
	Query updateQuery{Query(default_namespace).Where("id", CondGe, Variant("1500")).Set("sparse_field", static_cast<int>(100500))};
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qrUpdate.Count(), 500);

	QueryResults qrAll;
	err = rt.reindexer->Select(Query(default_namespace).Where("id", CondGe, Variant("1500")), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qrAll.Count(), 500);

	for (auto it : qrAll) {
		Item item = it.GetItem(false);
		Variant val = item["sparse_field"];
		ASSERT_TRUE(val.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(val.As<int>() == 100500);
		checkIfItemJSONValid(it);
	}
}

// Test of the currious case: https://github.com/restream/reindexer/-/issues/697
// Updating entire object field and some indexed field at once.
TEST_F(NsApi, TestUpdateTwoFields) {
	// Set and fill Database
	DefineDefaultNamespace();
	FillDefaultNamespace();

	// Try to update 2 fields at once: indexed field 'stringField'
	// + adding and setting a new object-field called 'very_nested'
	QueryResults qrUpdate;
	Query updateQuery = Query(default_namespace)
							.Where(idIdxName, CondEq, 1)
							.Set(stringField, "Bingo!")
							.SetObject("very_nested", R"({"id":111, "name":"successfully updated!"})");
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);

	// Make sure query worked well
	ASSERT_TRUE(err.ok()) << err.what();
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

		Variant nestedId = item["very_nested.id"];
		EXPECT_TRUE(nestedId.As<int>() == 111);

		Variant nestedName = item["very_nested.name"];
		EXPECT_TRUE(nestedName.As<std::string>() == "successfully updated!");
	}
}

TEST_F(NsApi, TestUpdateNewFieldCheckTmVersion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	auto check = [this](const Query &query, int tmVersion) {
		QueryResults qrUpdate;
		auto err = rt.reindexer->Update(query, qrUpdate);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qrUpdate.Count(), 1);
		ASSERT_EQ(qrUpdate.getTagsMatcher(0).version(), tmVersion);
	};

	QueryResults qr;
	Error err = rt.reindexer->Select(Query(default_namespace).Where(idIdxName, CondEq, 1), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	auto tmVersion = qr.getTagsMatcher(0).version();
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

static void checkUpdateArrayFieldResults(std::string_view updateFieldPath, const QueryResults &results, const VariantArray &values) {
	for (auto it : results) {
		Item item = it.GetItem(false);
		VariantArray val = item[updateFieldPath];
		if (values.empty()) {
			ASSERT_EQ(val.size(), 1);
			ASSERT_TRUE(val.IsNullValue()) << val.ArrayType().Name();
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

static void updateArrayField(const std::shared_ptr<reindexer::Reindexer> &reindexer, const std::string &ns,
							 std::string_view updateFieldPath, const VariantArray &values) {
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

	QueryResults qr;
	Error err = rt.reindexer->Select(R"(update test_namespace set nested.bonus=[{"first":1,"second":2,"third":3}] where id = 1000;)", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr[0].GetItem(false);
	std::string_view json = item.GetJSON();
	size_t pos = json.find(R"("nested":{"bonus":[{"first":1,"second":2,"third":3}])");
	ASSERT_TRUE(pos != std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField3) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err =
		rt.reindexer->Select(R"(update test_namespace set nested.bonus=[{"id":1},{"id":2},{"id":3},{"id":4}] where id = 1000;)", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr[0].GetItem(false);
	VariantArray val = item["nested.bonus"];
	ASSERT_TRUE(val.size() == 4);

	size_t length = 0;
	std::string_view json = item.GetJSON();
	gason::JsonParser jsonParser;
	ASSERT_NO_THROW(jsonParser.Parse(json, &length));
	ASSERT_TRUE(length > 0);

	size_t pos = json.find(R"("nested":{"bonus":[{"id":1},{"id":2},{"id":3},{"id":4}])");
	ASSERT_TRUE(pos != std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField4) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err = rt.reindexer->Select(R"(update test_namespace set nested.bonus=[0] where id = 1000;)", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr[0].GetItem(false);
	std::string_view json = item.GetJSON();
	size_t pos = json.find(R"("nested":{"bonus":[0])");
	ASSERT_NE(pos, std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField5) {
	DefineDefaultNamespace();
	AddUnindexedData();
	updateArrayField(rt.reindexer, default_namespace, "string_array", {});
	updateArrayField(
		rt.reindexer, default_namespace, "string_array",
		{Variant(std::string("one")), Variant(std::string("two")), Variant(std::string("three")), Variant(std::string("four"))});
	updateArrayField(rt.reindexer, default_namespace, "string_array", {Variant(std::string("single one"))});
}

TEST_F(NsApi, TestUpdateIndexedArrayField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();
	updateArrayField(rt.reindexer, default_namespace, indexedArrayField,
					 {Variant(7), Variant(8), Variant(9), Variant(10), Variant(11), Variant(12), Variant(13)});
}

TEST_F(NsApi, TestUpdateIndexedArrayField2) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	VariantArray value;
	value.emplace_back(static_cast<int>(77));
	Query q{Query(default_namespace).Where(idIdxName, CondEq, static_cast<int>(1000)).Set(indexedArrayField, std::move(value.MarkArray()))};
	Error err = rt.reindexer->Update(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr[0].GetItem(false);
	std::string_view json = item.GetJSON();
	size_t pos = json.find(R"("indexed_array_field":[77])");
	ASSERT_NE(pos, std::string::npos) << "'indexed_array_field' was not updated properly" << json;
}

static void addAndSetNonindexedField(const std::shared_ptr<reindexer::Reindexer> &reindexer, const std::string &ns,
									 const std::string &updateFieldPath) {
	QueryResults qrUpdate;
	Query updateQuery{Query(ns).Where("nested.bonus", CondGe, Variant(500)).Set(updateFieldPath, static_cast<int64_t>(777))};
	Error err = reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qrAll;
	err = reindexer->Select(Query(ns).Where("nested.bonus", CondGe, Variant(500)), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : qrAll) {
		Item item = it.GetItem(false);
		Variant val = item[updateFieldPath.c_str()];
		ASSERT_TRUE(val.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(val.As<int64_t>() == 777);
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

static void setAndCheckArrayItem(const std::shared_ptr<reindexer::Reindexer> &reindexer, const std::string &ns,
								 const std::string &fullItemPath, const std::string &jsonPath, int i = IndexValueType::NotSet,
								 int j = IndexValueType::NotSet) {
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
	auto checkItem = [](const VariantArray &values, size_t index) {
		ASSERT_TRUE(index < values.size());
		ASSERT_TRUE(values[index].Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(values[index].As<int64_t>() == 777);
	};

	// Check every item according to it's index, where i is the index of parent's array
	// and j is the index of a nested array:
	// 1) objects[1].prices[0]: i = 1, j = 0
	// 2) objects[2].prices[*]: i = 2, j = IndexValueType::NotSet
	// etc.
	for (auto it : qrAll) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item[jsonPath.c_str()];
		if (i == j && i == IndexValueType::NotSet) {
			for (size_t i = 0; i < values.size(); ++i) {
				checkItem(values, i);
			}
		} else if (i == IndexValueType::NotSet) {
			for (int k = 0; k < kPricesSize; ++k) {
				checkItem(values, k * kPricesSize + j);
			}
		} else if (j == IndexValueType::NotSet) {
			for (int k = 0; k < kPricesSize; ++k) {
				checkItem(values, i * kPricesSize + k);
			}
		} else {
			checkItem(values, i * kPricesSize + j);
		}
	}
}

TEST_F(NsApi, TestAddAndSetArrayField) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Set array item(s) value to 777 and check if it was set properly
	DefineDefaultNamespace();
	AddUnindexedData();
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[0].prices[2]", "nested.nested_array.prices", 0, 2);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[2].nested.array[1]", "nested.nested_array.nested.array", 0,
						 1);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[2].nested.array[*]", "nested.nested_array.nested.array", 0);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[1].prices[*]", "nested.nested_array.prices", 1);
}

TEST_F(NsApi, TestAddAndSetArrayField2) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Set array item(s) value to 777 and check if it was set properly
	DefineDefaultNamespace();
	AddUnindexedData();
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[*].prices[0]", "nested.nested_array.prices",
						 IndexValueType::NotSet, 0);
	setAndCheckArrayItem(rt.reindexer, default_namespace, "nested.nested_array[*].name", "nested.nested_array.name");
}

TEST_F(NsApi, TestAddAndSetArrayField3) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set array item(s) value to 777 and check if it was set properly
	QueryResults qrUpdate;
	Query updateQuery{
		Query(default_namespace).Where("nested.bonus", CondGe, Variant(500)).Set("indexed_array_field[0]", static_cast<int>(777))};
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	// 4. Make sure each item's indexed_array_field[0] is of type Int and equal to 777
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item[indexedArrayField];
		ASSERT_TRUE(values[0].Type().Is<reindexer::KeyValueType::Int>());
		ASSERT_TRUE(values[0].As<int>() == 777);
	}
}

TEST_F(NsApi, TestAddAndSetArrayField4) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set array item(s) value to 777 and check if it was set properly
	QueryResults qrUpdate;
	Query updateQuery{
		Query(default_namespace).Where("nested.bonus", CondGe, Variant(500)).Set("indexed_array_field[*]", static_cast<int>(777))};
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	// 4. Make sure all items of indexed_array_field are of type Int and set to 777
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item[indexedArrayField];
		ASSERT_TRUE(values.size() == 9);
		for (size_t i = 0; i < values.size(); ++i) {
			ASSERT_TRUE(values[i].Type().Is<reindexer::KeyValueType::Int>());
			ASSERT_TRUE(values[i].As<int>() == 777);
		}
	}
}

static void dropArrayItem(const std::shared_ptr<reindexer::Reindexer> &reindexer, const std::string &ns, const std::string &fullItemPath,
						  const std::string &jsonPath, int i = IndexValueType::NotSet, int j = IndexValueType::NotSet) {
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
	// Approach is to check array size (because after removing some of it's items
	// it should decrease).
	for (auto it : qrAll) {
		checkIfItemJSONValid(it);
		Item item = it.GetItem(false);
		VariantArray values = item[jsonPath.c_str()];
		if (i == IndexValueType::NotSet && j == IndexValueType::NotSet) {
			ASSERT_TRUE(values.size() == 0) << values.size();
		} else if (i == IndexValueType::NotSet || j == IndexValueType::NotSet) {
			ASSERT_TRUE(int(values.size()) == kPricesSize * 2) << values.size();
		} else {
			ASSERT_TRUE(int(values.size()) == kPricesSize * 3 - 1) << values.size();
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

#if (0)	 // #1500
TEST_F(NsApi, DropArrayField4) {
	// 1. Define NS
	// 2. Fill NS
	// 3. Drop array item(s) and check it was properly removed
	DefineDefaultNamespace();
	AddUnindexedData();
	DropArrayItem(rt.reindexer, default_namespace, "nested.nested_array[0].prices[((2+4)*2)/6]", "nested.nested_array.prices", 0,
				  ((2 + 4) * 2) / 6);
}
#endif

TEST_F(NsApi, SetArrayFieldWithSql) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set all items of array to 777
	Query updateQuery = Query::FromSQL("update test_namespace set nested.nested_array[1].prices[*] = 777");
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	const int kElements = 3;

	// 4. Make sure all items of array nested.nested_array.prices are equal to 777 and of type Int
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		VariantArray values = item["nested.nested_array.prices"];
		for (int i = 0; i < kElements; ++i) {
			ASSERT_TRUE(values[kElements + i].As<int>() == 777);
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
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	const int kElements = 3;

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
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	const int kElements = 3;

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
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	const int kElements = 3;

	// 4. Make sure all items of array have proper values
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item["array_field"];
		int i = 0;
		ASSERT_TRUE(values.size() == kElements * 2 + 1 + 1);
		for (; i < kElements; ++i) {
			ASSERT_TRUE(values[i].As<int>() == i + 1);
		}
		ASSERT_TRUE(values[i++].As<int>() == 0);
		for (; i < 7; ++i) {
			ASSERT_TRUE(values[i].As<int>() == 22);
		}
		ASSERT_TRUE(values[i].As<int>() == 11);
	}
}

TEST_F(NsApi, ExtendArrayWithExpressions) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Extend array_field with expression via Query builder
	Query updateQuery = Query(default_namespace);
	updateQuery.Set("array_field",
					Variant(std::string("[88,88,88] || array_field || [99, 99, 99] || indexed_array_field || objects.more[1].array[4]")),
					true);
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	const int kElements = 3;

	// Check if array_field was modified properly
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		VariantArray values = item["array_field"];
		ASSERT_TRUE(values.size() == kElements * 3 + 9 + 1);
		int i = 0;
		for (; i < kElements; ++i) {
			ASSERT_TRUE(values[i].As<int>() == 88);
		}
		ASSERT_TRUE(values[i++].As<int>() == 1);
		ASSERT_TRUE(values[i++].As<int>() == 2);
		ASSERT_TRUE(values[i++].As<int>() == 3);
		for (; i < 9; ++i) {
			ASSERT_TRUE(values[i].As<int>() == 99);
		}
		for (int k = 1; k < 10; ++i, ++k) {
			ASSERT_TRUE(values[i].As<int>() == k * 11) << k << "; " << i << "; " << values[i].As<int>();
		}
		ASSERT_TRUE(values[i++].As<int>() == 0);
	}
}

static void validateResults(const std::shared_ptr<reindexer::Reindexer> &reindexer, const Query &baseQuery, std::string_view ns,
							const QueryResults &qr, std::string_view pattern, std::string_view field, const VariantArray &expectedValues,
							std::string_view description, int resCount = 5) {
	const std::string fullDescription = "Description: " + std::string(description) + ";\n";
	// Check initial result
	ASSERT_EQ(qr.Count(), resCount) << fullDescription;
	std::vector<std::string> initialResults;
	initialResults.reserve(qr.Count());
	for (auto it : qr) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		const auto json = item.GetJSON();
		ASSERT_NE(json.find(pattern), std::string::npos) << fullDescription << "JSON: " << json << ";\npattern: " << pattern;
		initialResults.emplace_back(json);
		const VariantArray values = item[field];
		ASSERT_EQ(values.size(), expectedValues.size()) << fullDescription;
		ASSERT_EQ(values.IsArrayValue(), expectedValues.IsArrayValue()) << fullDescription;
		for (size_t i = 0; i < values.size(); ++i) {
			ASSERT_TRUE(values[i].Type().IsSame(expectedValues[i].Type()))
				<< fullDescription << values[i].Type().Name() << "!=" << expectedValues[i].Type().Name();
			ASSERT_EQ(values[i], expectedValues[i]) << fullDescription;
		}
	}
	// Check select results
	QueryResults qrSelect;
	const Query q = expectedValues.size() ? Query(ns).Where(std::string(field), CondAllSet, expectedValues) : baseQuery;
	auto err = reindexer->Select(q, qrSelect);
	ASSERT_TRUE(err.ok()) << fullDescription << err.what();
	ASSERT_EQ(qrSelect.Count(), qr.Count()) << fullDescription;
	unsigned i = 0;
	for (auto it : qrSelect) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		const auto json = item.GetJSON();
		ASSERT_EQ(json, initialResults[i++]) << fullDescription;
		const VariantArray values = item[field];
		ASSERT_EQ(values.size(), expectedValues.size()) << fullDescription;
		ASSERT_EQ(values.IsArrayValue(), expectedValues.IsArrayValue()) << fullDescription;
		for (size_t j = 0; j < values.size(); ++j) {
			ASSERT_TRUE(values[j].Type().IsSame(expectedValues[j].Type()))
				<< fullDescription << values[j].Type().Name() << "!=" << expectedValues[j].Type().Name();
			ASSERT_EQ(values[j], expectedValues[j]) << fullDescription;
		}
	}
}

// Check if it's possible to use append operation with empty arrays and null fields
TEST_F(NsApi, ExtendEmptyArrayWithExpressions) {
	const std::string kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		const auto description = "append value to the empty indexed array";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("indexed_array_field || [99, 99, 99]"), true);
		QueryResults qr;
		Error err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, description);
	}
	{
		const auto description = "append empty array to the indexed array";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("indexed_array_field || []"), true);
		QueryResults qr;
		Error err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, description);
	}
	{
		const auto description = "append value to the empty non-indexed array";
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || [88, 88]"), true);
		QueryResults qr;
		Error err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88])", "non_indexed_array_field",
						{Variant(int64_t(88)), Variant(int64_t(88))}, description);
	}
	{
		const auto description = "append empty array to the non-indexed array";
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || []"), true);
		QueryResults qr;
		Error err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88])", "non_indexed_array_field",
						{Variant(int64_t(88)), Variant(int64_t(88))}, description);
	}
	{
		const auto description = "append empty array to the non-existing field";
		const Query query = Query(kBaseQuery).Set("non_existing_field", Variant("non_existing_field || []"), true);
		QueryResults qr;
		Error err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88],"non_existing_field":[])",
						"non_existing_field", VariantArray().MarkArray(), description);
	}

	{
		const auto description = "append non-empty array to the non-existing field";
		const Query query = Query(kBaseQuery).Set("non_existing_field1", Variant("non_existing_field1 || [546]"), true);
		QueryResults qr;
		Error err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(
			rt.reindexer, kBaseQuery, kEmptyArraysNs, qr,
			R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[88,88],"non_existing_field":[],"non_existing_field1":[546])",
			"non_existing_field1", VariantArray{Variant(int64_t(546))}.MarkArray(), description);
	}
}

TEST_F(NsApi, ArrayRemove) {
	const std::string kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		// remove items from empty indexed non array field
		const Query query = Query(kBaseQuery).Set("id", Variant("array_remove(id, [1, 99])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Only an array field is expected as first parameter of command 'array_remove_once/array_remove'");
	}
	{
		const auto description = "remove empty array from empty indexed array";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", {}, description);
	}
	{
		const auto description = "remove all values from empty indexed array with append empty array";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [1, 99]) || []"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", {}, description);
	}
	{
		const auto description = "remove non-used values from empty indexed array with append";
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [1, 2, 3, 99]) || [99, 99, 99]"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, description);
	}
	{
		// negative: remove string value from indexed numeric array field
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant(std::string(R"(array_remove(indexed_array_field, ['test']))")), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Can't convert 'test' to number");
	}
	{
		const auto description = "remove all values from indexed array with duplicates";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field[0], [99, 1])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", {}, description);
	}
	{
		const auto description = "remove all values from empty indexed array with append";
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [1]) || [4, 3, 3]"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[4,3,3],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(4), Variant(3), Variant(3)}, description);
	}
	{
		const auto description = R"("remove used\non-used values from indexed array with append empty array")";
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, [2, 5, 3]) || []"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[4],"non_indexed_array_field":[])",
						"indexed_array_field", VariantArray{Variant(4)}, description);
	}
	{
		const auto description = R"("remove items from indexed array by single value scalar")";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, 4)"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", {}, description);
	}
}

TEST_F(NsApi, ArrayRemoveExtra) {
	const std::string kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		const auto description = "add array to empty non-indexed array";
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || [99, 99, 99]"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[99,99,99])",
						"non_indexed_array_field", {Variant(int64_t(99)), Variant(int64_t(99)), Variant(int64_t(99))}, description);
	}
	{
		const auto description = "remove from yourself indexed array field (empty) with append non-indexed field";
		const Query query = Query(kBaseQuery)
								.Set("indexed_array_field",
									 Variant("array_remove(indexed_array_field, indexed_array_field) || non_indexed_array_field"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr,
						R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[99,99,99])", "indexed_array_field",
						{Variant(99), Variant(99), Variant(99)}, description);
	}
	{
		const auto description = "remove from yourself indexed array field with append";
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove(indexed_array_field, indexed_array_field) || [1,2]"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[1,2],"non_indexed_array_field":[99,99,99])",
						"indexed_array_field", {Variant(1), Variant(2)}, description);
	}
	{
		const auto description =
			"mixed remove indexed array field with append remove in non-indexed field and append array (remove scalar)";
		const Query query =
			Query(kBaseQuery)
				.Set("indexed_array_field",
					 Variant("array_remove(indexed_array_field, 1) || array_remove_once(non_indexed_array_field, 99) || [3]"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr,
						R"("indexed_array_field":[2,99,99,3],"non_indexed_array_field":[99,99,99])", "indexed_array_field",
						{Variant(2), Variant(99), Variant(99), Variant(3)}, description);
	}
}

TEST_F(NsApi, ArrayRemoveOnce) {
	const std::string kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		// remove once value from empty indexed non array field
		const Query query = Query(kBaseQuery).Set("id", Variant("array_remove_once(id, [99])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Only an array field is expected as first parameter of command 'array_remove_once/array_remove'");
	}
	{
		const auto description = "remove once empty array from empty indexed array";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", {}, description);
	}
	{
		const auto description = "remove once values from empty indexed array";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [1, 99])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[])",
						"indexed_array_field", {}, description);
	}
	{
		const auto description = "remove once non-used values from empty indexed array with append";
		const Query query =
			Query(kBaseQuery)
				.Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [1, 2, 3, 99]) || [99, 99, 99]"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, description);
	}
	{
		// negative: remove once string value from indexed array
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant(std::string(R"(array_remove_once(indexed_array_field, ['test']))")), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Can't convert 'test' to number");
	}
	{
		// negative: remove once string value (scalar) from indexed array
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant(std::string(R"(array_remove_once(indexed_array_field, 'Boo'))")), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Can't convert 'Boo' to number");
	}
	{
		const auto description = "remove once empty array from non empty indexed array";
		const Query query = Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, description);
	}
	{
		const auto description = "remove once non-used values from indexed array";
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [1, 2, 3])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[99,99,99],"non_indexed_array_field":[])",
						"indexed_array_field", {Variant(99), Variant(99), Variant(99)}, description);
	}
	{
		const auto description = "remove one value twice from indexed array with duplicates and with append empty array";
		const Query query =
			Query(kBaseQuery).Set("indexed_array_field", Variant("array_remove_once(indexed_array_field, [99, 99]) || []"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[99],"non_indexed_array_field":[])",
						"indexed_array_field", VariantArray{Variant(99)}, description);
	}
}

TEST_F(NsApi, ArrayRemoveNonIndexed) {
	const std::string kEmptyArraysNs = "empty_arrays_ns";
	CreateEmptyArraysNamespace(kEmptyArraysNs);
	const Query kBaseQuery = Query(kEmptyArraysNs).Where("id", CondSet, {100, 105, 189, 113, 153});

	{
		const auto description = "add array to empty non-indexed array";
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("non_indexed_array_field || [99, 99, 99]"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[99,99,99])",
						"non_indexed_array_field", {Variant(int64_t(99)), Variant(int64_t(99)), Variant(int64_t(99))}, description);
	}
	{
		const auto description = "remove scalar value from non-indexed array with append array";
		const Query query = Query(kBaseQuery)
								.Set("non_indexed_array_field",
									 Variant(std::string(R"(array_remove_once(non_indexed_array_field, '99') || [1, 2]))")), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[99,99,1,2])",
						"non_indexed_array_field", {Variant(int64_t(99)), Variant(int64_t(99)), Variant(int64_t(1)), Variant(int64_t(2))},
						description);
	}
	{
		const auto description = "remove with duplicates from non indexed array";
		const Query query = Query(kBaseQuery).Set("non_indexed_array_field", Variant("array_remove(non_indexed_array_field, [99])"), true);
		QueryResults qr;
		const auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, kEmptyArraysNs, qr, R"("indexed_array_field":[],"non_indexed_array_field":[1,2])",
						"non_indexed_array_field", {Variant(int64_t(1)), Variant(int64_t(2))}, description);
	}
}

TEST_F(NsApi, ArrayRemoveSparseStrings) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"str_h_field", "hash", "string", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"str_h_empty", "hash", "string", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"str_t_field", "tree", "string", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"str_t_empty", "tree", "string", IndexOpts().Array().Sparse(), 0}});
	const std::string json =
		R"json({
				"id": 1,
				"str_h_field" : ["1", "2", "3", "3"],
				"str_t_field": ["11","22","33","33"],
			})json";

	Item item = NewItem(default_namespace);
	EXPECT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(json);
	EXPECT_TRUE(err.ok()) << err.what();

	Upsert(default_namespace, item);
	Commit(default_namespace);

	const Query kBaseQuery = Query(default_namespace).Where("id", CondEq, {1});

	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty, [])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.1", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once([], str_h_empty)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.2", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty, ['1'])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.3", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty, str_h_field)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_empty",
						VariantArray().MarkArray(), "Step 1.4", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_field", Variant("array_remove_once(str_h_field, str_h_empty)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":[])", "str_h_field",
						{Variant("1"), Variant("2"), Variant("3"), Variant("3")}, "Step 1.5", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove(str_h_field, ['1','3'])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":["2"])", "str_h_empty",
						VariantArray{Variant("2")}.MarkArray(), "Step 1.6", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_h_empty", Variant("array_remove(['1'], str_h_empty)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["1","2","3","3"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"])", "str_h_empty",
						VariantArray{Variant("1")}.MarkArray(), "Step 1.7", 1);
	}
	{
		const Query query =
			Query(kBaseQuery).Set("str_h_field", Variant("array_remove(str_h_field, ['1','3','first']) || ['POCOMAXA']"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"])", "str_h_field",
						{Variant("2"), Variant("POCOMAXA")}, "Step 1.8", 1);
	}

	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove_once(str_t_empty, [])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.1", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove_once(str_t_empty, ['1'])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.2", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove_once(str_t_empty, str_t_field)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.3", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove(str_t_empty, ['11','33','32'])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":[])",
						"str_t_empty", VariantArray().MarkArray(), "Step 2.4", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_empty", Variant("array_remove(['7'], str_t_empty)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["11","22","33","33"],"str_h_empty":["1"],"str_t_empty":["7"])",
						"str_t_empty", VariantArray{Variant("7")}.MarkArray(), "Step 2.5", 1);
	}
	{
		const Query query =
			Query(kBaseQuery).Set("str_t_field", Variant("array_remove_once(str_t_field, ['11', '33',  'first']) || ['POCOMAXA']"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("str_h_field":["2","POCOMAXA"],"str_t_field":["22","33","POCOMAXA"],"str_h_empty":["1"],"str_t_empty":["7"])",
						"str_t_field", {Variant("22"), Variant("33"), Variant("POCOMAXA")}, "Step 2.6", 1);
	}

	{
		const Query query =
			Query(kBaseQuery).Set("str_h_empty", Variant("array_remove_once(str_h_empty,   str_t_empty) || ['007','XXX']"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(
			rt.reindexer, kBaseQuery, default_namespace, qr,
			R"("str_h_field":["2","POCOMAXA"],"str_t_field":["22","33","POCOMAXA"],"str_h_empty":["1","007","XXX"],"str_t_empty":["7"])",
			"str_h_empty", {Variant("1"), Variant("007"), Variant("XXX")}, "Step 3.1", 1);
	}
	{
		const Query query =
			Query(kBaseQuery).Set("str_t_field", Variant("[ '7', 'XXX' ]  ||  array_remove_once( str_t_field , str_h_field ) "), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(
			rt.reindexer, kBaseQuery, default_namespace, qr,
			R"("str_h_field":["2","POCOMAXA"],"str_t_field":["7","XXX","22","33"],"str_h_empty":["1","007","XXX"],"str_t_empty":["7"])",
			"str_t_field", {Variant("7"), Variant("XXX"), Variant("22"), Variant("33")}, "Step 3.2", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("str_t_field", Variant("array_remove_once( str_t_field , '22' \t) "), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(
			rt.reindexer, kBaseQuery, default_namespace, qr,
			R"("str_h_field":["2","POCOMAXA"],"str_t_field":["7","XXX","33"],"str_h_empty":["1","007","XXX"],"str_t_empty":["7"])",
			"str_t_field", {Variant("7"), Variant("XXX"), Variant("33")}, "Step 3.3", 1);
	}
}

TEST_F(NsApi, ArrayRemoveSparseDoubles) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"double_field", "tree", "double", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"double_empty", "tree", "double", IndexOpts().Array().Sparse(), 0}});
	Item item = NewItem(default_namespace);
	EXPECT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(R"json({"id": 1, "double_field": [1.11,2.22,3.33,3.33]})json");
	EXPECT_TRUE(err.ok()) << err.what();

	Upsert(default_namespace, item);
	Commit(default_namespace);

	const Query kBaseQuery = Query(default_namespace).Where("id", CondEq, {1});

	{
		const Query query = Query(kBaseQuery).Set("double_empty", Variant("array_remove(double_empty, [])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr, R"("double_field":[1.11,2.22,3.33,3.33],"double_empty":[])",
						"double_empty", VariantArray{}.MarkArray(), "Step 1.1", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("double_empty", Variant("array_remove_once(double_empty, double_field) || [0.07]"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr, R"("double_field":[1.11,2.22,3.33,3.33],"double_empty":[0.07])",
						"double_empty", VariantArray{Variant(0.07)}.MarkArray(), "Step 1.2", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("double_field", Variant("[7.77] || array_remove(double_field, double_empty)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr,
						R"("double_field":[7.77,1.11,2.22,3.33,3.33],"double_empty":[0.07])", "double_field",
						{Variant(7.77), Variant(1.11), Variant(2.22), Variant(3.33), Variant(3.33)}, "Step 1.3", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("double_field", Variant("array_remove_once(double_field, [3.33,3.33,1.11,99])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr, R"("double_field":[7.77,2.22],"double_empty":[0.07])",
						"double_field", {Variant(7.77), Variant(2.22)}, "Step 1.4", 1);
	}
}

TEST_F(NsApi, ArrayRemoveSparseBooleans) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"bool_field", "-", "bool", IndexOpts().Array().Sparse(), 0},
											   IndexDeclaration{"bool_empty", "-", "bool", IndexOpts().Array().Sparse(), 0}});
	Item item = NewItem(default_namespace);
	EXPECT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(R"json({"id": 1, "bool_field": [true,true,false,false]})json");
	EXPECT_TRUE(err.ok()) << err.what();

	Upsert(default_namespace, item);
	Commit(default_namespace);

	const Query kBaseQuery = Query(default_namespace).Where("id", CondEq, {1});

	{
		const Query query = Query(kBaseQuery).Set("bool_empty", Variant("array_remove(bool_empty, [])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr, R"("bool_field":[true,true,false,false],"bool_empty":[])",
						"bool_empty", VariantArray().MarkArray(), "Step 1.1", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("bool_empty", Variant("array_remove_once(bool_empty, bool_field) || [1]"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr, R"("bool_field":[true,true,false,false],"bool_empty":[true])",
						"bool_empty", VariantArray{Variant(true)}.MarkArray(), "Step 1.2", 1);
	}
	{
		const Query query = Query(kBaseQuery).Set("bool_field", Variant("array_remove_once(bool_field, bool_empty)"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr, R"("bool_field":[true,false,false],"bool_empty":[true])",
						"bool_field", {Variant(true), Variant(false), Variant(false)}, "Step 1.3", 1);
	}
	{
		const Query query =
			Query(kBaseQuery)
				.Set("bool_field", Variant("[true] || array_remove(bool_field, [false]) || array_remove_once(bool_empty, [0])"), true);
		QueryResults qr;
		err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		validateResults(rt.reindexer, kBaseQuery, default_namespace, qr, R"("bool_field":[true,true,true],"bool_empty":[true])",
						"bool_field", {Variant(true), Variant(true), Variant(true)}, "Step 1.4", 1);
	}
}

TEST_F(NsApi, ArrayRemoveSeveralJsonPathsField) {
	const std::string testNS = "remove_arrays_ns";
	const std::string intField0 = "int_field0";
	const std::string intField1 = "int_field1";
	const std::string multiPathArrayField = "array_index";
	const Query kBaseQuery = Query(testNS).Where("id", CondSet, {1, 5, 8, 3});

	Error err = rt.reindexer->OpenNamespace(testNS);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(testNS, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
									IndexDeclaration{intField0.c_str(), "hash", "int", IndexOpts(), 0},
									IndexDeclaration{intField1.c_str(), "hash", "int", IndexOpts(), 0}});
	err = rt.reindexer->AddIndex(
		testNS, {multiPathArrayField, reindexer::JsonPaths{intField0, intField1}, "hash", "composite", IndexOpts().Array()});
	ASSERT_TRUE(err.ok()) << err.what();

	static constexpr int sz = 10;
	for (int i = 0; i < sz; ++i) {
		Item item = NewItem(testNS);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		item[idIdxName] = i;
		item[intField0] = sz - i;
		item[intField1] = sz + i;

		Upsert(testNS, item);
		Commit(testNS);
	}

	const Query query =
		Query(kBaseQuery)
			.Set(multiPathArrayField, Variant(std::string(fmt::sprintf(R"(array_remove_once(%s, ['99']))", multiPathArrayField))), true);
	QueryResults qr;
	err = rt.reindexer->Update(query, qr);
	ASSERT_FALSE(err.ok());
	ASSERT_EQ(err.what(),
			  fmt::sprintf(R"(Ambiguity when updating field with several json paths by index name: '%s')", multiPathArrayField));
}

TEST_F(NsApi, ArrayRemoveWithSql) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Remove from array_field with expression substantially
	{
		Query updateQuery = Query::FromSQL(
			"update test_namespace set array_field = [0] || array_remove(array_field, [3,2,1]) || array_remove_once(indexed_array_field, "
			"[99]) "
			"|| [7,9]");
		QueryResults qrUpdate;
		Error err = rt.reindexer->Update(updateQuery, qrUpdate);
		ASSERT_TRUE(err.ok()) << err.what();

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["array_field"];
			ASSERT_TRUE(values.size() == 1 + 8 + 2) << 1 + 8 + 2 << " != " << values.size();
			int i = 0;
			ASSERT_TRUE(values[i++].As<int>() == 0) << 0 << " != " << values[i].As<int>();
			for (int k = 1; k < 9; ++k) {
				ASSERT_TRUE(values[i++].As<int>() == k * 11) << k << "; " << i << "; " << k * 11 << " != " << values[i].As<int>();
			}
			ASSERT_TRUE(values[i++].As<int>() == 7) << i << " - " << values[i].As<int>();
			ASSERT_TRUE(values[i++].As<int>() == 9) << i << " - " << values[i].As<int>();
		}
	}

	// 4. Second attempt. Remove & concatenate for string array field
	{
		Query updateQuery =
			Query::FromSQL("update test_namespace set string_array = array_remove(string_array, [\'first\']) || [\'POCOMAXA\']");
		QueryResults qrUpdate;
		Error err = rt.reindexer->Update(updateQuery, qrUpdate);
		ASSERT_TRUE(err.ok()) << err.what();

		// Check if array_field was modified properly
		for (auto it : qrUpdate) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			VariantArray values = item["string_array"];
			ASSERT_TRUE(values.size() == 3) << 3 << " != " << values.size();
			int i = 0;
			ASSERT_TRUE(values[i++].As<std::string>() == "second");
			ASSERT_TRUE(values[i++].As<std::string>() == "third");
			ASSERT_TRUE(values[i++].As<std::string>() == "POCOMAXA");
		}
	}
}

TEST_F(NsApi, UpdateObjectsArray) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Update object array and change one of it's items
	Query updateQuery =
		Query::FromSQL(R"(update test_namespace set nested.nested_array[1] = {"id":1,"name":"modified", "prices":[4,5,6]})");
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	// 4. Make sure nested.nested_array[1] is set to a new value properly
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		ASSERT_TRUE(item.GetJSON().find(R"({"id":1,"name":"modified","prices":[4,5,6]})") != std::string::npos);
	}
}

TEST_F(NsApi, UpdateObjectsArray2) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set all items of the object array to a new value
	Query updateQuery = Query::FromSQL(R"(update test_namespace set nested.nested_array[*] = {"ein":1,"zwei":2, "drei":3})");
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	// 4. Make sure all items of nested.nested_array are set to a new value correctly
	for (auto it : qrUpdate) {
		Item item = it.GetItem(false);
		checkIfItemJSONValid(it);
		ASSERT_TRUE(item.GetJSON().find(
						R"("nested_array":[{"ein":1,"zwei":2,"drei":3},{"ein":1,"zwei":2,"drei":3},{"ein":1,"zwei":2,"drei":3}]})") !=
					std::string::npos);
	}
}

TEST_F(NsApi, UpdateObjectsArray3) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set all items of the object array to a new value via Query builder
	Query updateQuery = Query(default_namespace);
	updateQuery.SetObject("nested.nested_array[*]", Variant(std::string(R"({"ein":1,"zwei":2, "drei":3})")), false);
	QueryResults qrUpdate;
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

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
	const std::vector<std::string> indexTypes = {"regular", "sparse", "none"};
	constexpr char kIndexName[] = "objects.array.field";
	const Query kBaseQuery = Query(default_namespace).Where("id", CondSet, {1199, 1201, 1203, 1210, 1240});

	auto ValidateResults = [this, &kBaseQuery](const QueryResults &qr, std::string_view pattern, std::string_view indexType,
											   std::string_view description) {
		const std::string fullDescription = fmt::sprintf("Description: %s; %s;\n", description, indexType);
		// Check initial result
		ASSERT_EQ(qr.Count(), 5) << fullDescription;
		std::vector<std::string> initialResults;
		initialResults.reserve(qr.Count());
		for (auto it : qr) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			const auto json = item.GetJSON();
			ASSERT_NE(json.find(pattern), std::string::npos) << fullDescription << "JSON: " << json << ";\npattern: " << pattern;
			initialResults.emplace_back(json);
		}
		// Check select results
		QueryResults qrSelect;
		auto err = rt.reindexer->Select(kBaseQuery, qrSelect);
		ASSERT_TRUE(err.ok()) << fullDescription << err.what();
		ASSERT_EQ(qrSelect.Count(), qr.Count()) << fullDescription;
		unsigned i = 0;
		for (auto it : qrSelect) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			const auto json = item.GetJSON();
			ASSERT_EQ(json, initialResults[i++]) << fullDescription;
		}
	};

	for (const auto &index : indexTypes) {
		Error err = rt.reindexer->TruncateNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		// 2. Refill NS
		AddHeterogeniousNestedData();
		err =
			rt.reindexer->DropIndex(default_namespace, reindexer::IndexDef(kIndexName, {kIndexName}, "hash", "int64", IndexOpts().Array()));
		(void)err;	// Error does not matter here
		if (index != "none") {
			err = rt.reindexer->AddIndex(default_namespace, reindexer::IndexDef(kIndexName, {kIndexName}, "hash", "int64",
																				IndexOpts().Array().Sparse(index == "sparse")));
			ASSERT_TRUE(err.ok()) << err.what();
		}
		const std::string indexTypeMsg = fmt::sprintf("Index type is '%s'", index);

		{
			const auto description = "Update array field, nested into objects array with explicit index (1 element)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[0].field[4]", {777}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":11},{"field":[4,3,2,1,0]},{"field":[99]}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Update array field, nested into objects array with explicit index (1 element, different position)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[2].field[3]", {8387}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":11},{"field":[4,3,2,8387,0]},{"field":[99]}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index with scalar type";
			// Make sure, that internal field's type ('scalar') was not changed
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", {537}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":537},{"field":[4,3,2,8387,0]},{"field":[99]}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Update scalar field, nested into objects array with explicit index with array type";
			// Make sure, that internal field's type ('array') was not changed
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[3].field[0]", {999}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":537},{"field":[4,3,2,8387,0]},{"field":[999]}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Change field type from array[1] to scalar";
			// Make sure, that internal field's type (array of 1 element) was changed to scalar
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[3].field", {837}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":[9,8,7,6,777]},{"field":537},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Change field type from array[4] to scalar";
			// Make sure, that internal field's type (array of 4 elements) was changed to scalar
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[0].field", {2345}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":537},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Change field type from scalar to array[1]";
			// Make sure, that internal field's type ('scalar') was changed to array
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{1847}}.MarkArray(), false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[1847]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index. Increase array size";
			Query updateQuery =
				Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{115}, Variant{1000}, Variant{501}}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[115,1000,501]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Reduce array size (to multiple elements)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{100}, Variant{999}}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[100,999]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description =
				"Update array field, nested into objects array without explicit index. Reduce array size (to single element)";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray{Variant{150}}.MarkArray(), false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Attempt to set array-value(1 element) by explicit index";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[0]", VariantArray{Variant{199}}.MarkArray(), false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << indexTypeMsg << err.what();

			qr.Clear();
			err = rt.reindexer->Select(kBaseQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Attempt to set array-value(multiple elements) by explicit index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[0]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << indexTypeMsg << err.what();

			qr.Clear();
			err = rt.reindexer->Select(kBaseQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Attempt to set array-value(1 element) by *-index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[*]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << indexTypeMsg << err.what();

			qr.Clear();
			err = rt.reindexer->Select(kBaseQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Attempt to set array-value(multiple elements) by *-index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field[*]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << indexTypeMsg << err.what();

			qr.Clear();
			err = rt.reindexer->Select(kBaseQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[4,3,2,8387,0]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Update array field, nested into objects array with *-index";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[2].field[*]", {199}, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[199,199,199,199,199]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Attempt to update scalar value by *-index";
			VariantArray v{Variant{199}, Variant{200}, Variant{300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[0].field[*]", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_EQ(err.code(), errParams) << indexTypeMsg << err.what();

			qr.Clear();
			err = rt.reindexer->Select(kBaseQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			// Make sure, that item was not changed
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[150]},{"field":[199,199,199,199,199]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index. Reduce array size to 0";
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", VariantArray().MarkArray(), false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(qr, R"("objects":[{"array":[{"field":2345},{"field":[]},{"field":[199,199,199,199,199]},{"field":837}]}])",
							indexTypeMsg, description);
		}
		{
			const auto description = "Update array field, nested into objects array without explicit index. Increase array size from 0";
			VariantArray v{Variant{11199}, Variant{11200}, Variant{11300}};
			Query updateQuery = Query(kBaseQuery).Set("objects[0].array[1].field", v, false);
			QueryResults qr;
			err = rt.reindexer->Update(updateQuery, qr);
			ASSERT_TRUE(err.ok()) << indexTypeMsg << err.what();
			ValidateResults(
				qr, R"("objects":[{"array":[{"field":2345},{"field":[11199,11200,11300]},{"field":[199,199,199,199,199]},{"field":837}]}])",
				indexTypeMsg, description);
		}
	}
}

TEST_F(NsApi, UpdateArrayIndexFieldWithSeveralJsonPaths) {
	struct Values {
		std::vector<std::string> valsList, newValsList;
	};
	const int fieldsCnt = 5;
	const int valsPerFieldCnt = 4;
	std::vector<Values> fieldsValues(fieldsCnt);
	for (int i = 0; i < fieldsCnt; ++i) {
		for (int j = 0; j < valsPerFieldCnt; ++j) {
			fieldsValues[i].valsList.emplace_back(fmt::sprintf("data%d%d", i, j));
			fieldsValues[i].newValsList.emplace_back(fmt::sprintf("data%d%d", i, j + i));
		}
	}

	enum class OpT { Insert, Update };

	auto makeFieldsList = [&fieldsValues](const reindexer::fast_hash_set<int> &indexes, OpT type) {
		auto quote = type == OpT::Insert ? '"' : '\'';
		std::vector<std::string> Values::*list = type == OpT::Insert ? &Values::valsList : &Values::newValsList;
		const auto fieldsListTmplt = type == OpT::Insert ? R"("%sfield%d": [%s])" : R"(%sfield%d = [%s])";
		std::string fieldsList;
		for (int idx : indexes) {
			std::string fieldList;
			for (const auto &data : fieldsValues[idx].*list) {
				fieldList += std::string(fieldList.empty() ? "" : ", ") + quote + data + quote;
			}
			fieldsList += fmt::sprintf(fieldsListTmplt, fieldsList.empty() ? "" : ", ", idx, fieldList);
		}
		return fieldsList;
	};

	auto makeItem = [&makeFieldsList](int id, const reindexer::fast_hash_set<int> &indexes) {
		auto list = makeFieldsList(indexes, OpT::Insert);
		return fmt::sprintf(R"({"id": %d%s})", id, (list.empty() ? "" : ", ") + list);
	};

	auto makeUpdate = [this, &makeFieldsList](int id, const reindexer::fast_hash_set<int> &indexes) {
		return fmt::sprintf("UPDATE %s SET %s WHERE id = %d", default_namespace, makeFieldsList(indexes, OpT::Update), id);
	};

	struct TestCase {
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

	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"array_index", reindexer::JsonPaths{"field0", "field1", "field2", "field3", "field4"},
													 "hash", "string", IndexOpts().Array()});
	ASSERT_TRUE(err.ok()) << err.what();

	for (size_t i = 0; i < testCases.size(); ++i) {
		AddItemFromJSON(default_namespace, makeItem(i, testCases[i].insertIdxs));
		{
			QueryResults qr;
			err = rt.reindexer->Select(makeUpdate(i, testCases[i].updateIdxs), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 1);

			auto item = qr.begin().GetItem(false);
			for (auto idx : testCases[i].expected()) {
				int varArrCnt = 0;
				for (auto &&var : VariantArray(item[fmt::sprintf("field%d", idx)])) {
					const auto &data = testCases[i].updateIdxs.count(idx) ? fieldsValues[idx].newValsList : fieldsValues[idx].valsList;
					ASSERT_EQ(var.As<std::string>(), data[varArrCnt++]);
				}
			}
		}
	}

	// Check that prohibited updating an index array field with several json paths by index name
	QueryResults qr;
	err = rt.reindexer->Select(fmt::sprintf(R"(UPDATE %s SET array_index = ['data0', 'data1', 'data2'] WHERE id = 0)", default_namespace),
							   qr);
	ASSERT_FALSE(err.ok());
	ASSERT_EQ(err.what(), "Ambiguity when updating field with several json paths by index name: 'array_index'");
}

TEST_F(NsApi, UpdateWithObjectAndFieldsDuplication) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"nested", reindexer::JsonPaths{"n.idv"}, "hash", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<std::string_view> items = {R"({"id":0,"data":"data0","n":{"idv":"index_str_1","dat":"data1"}})",
										   R"({"id":5,"data":"data5","n":{"idv":"index_str_3","dat":"data2"}})"};

	AddItemFromJSON(default_namespace, items[0]);
	AddItemFromJSON(default_namespace, items[1]);

	{
		QueryResults qr;
		err = rt.reindexer->Update(Query(default_namespace)
									   .SetObject("n", R"({"idv":"index_str_3_modified","idv":"index_str_5_modified","dat":"data2_mod"})")
									   .Where("id", CondEq, 5),
								   qr);
		ASSERT_EQ(err.code(), errLogic) << err.what();
	}
	{
		// Check all the items
		QueryResults qr;
		err = rt.reindexer->Select(Query(default_namespace).Sort("id", false), qr);
		ASSERT_EQ(qr.Count(), 2);
		unsigned i = 0;
		for (auto it : qr) {
			ASSERT_EQ(it.GetItem().GetJSON(), items[i]) << i;
			++i;
		}
	}
	{
		// Check old indexed value (have to exists)
		QueryResults qr;
		err = rt.reindexer->Select(Query(default_namespace).Where("nested", CondEq, std::string("index_str_3")), qr);
		ASSERT_EQ(qr.Count(), 1);
		ASSERT_EQ(qr.begin().GetItem().GetJSON(), items[1]);
	}
	{
		// Check new indexed values (have to not exist)
		QueryResults qr;
		err = rt.reindexer->Select(
			Query(default_namespace).Where("nested", CondSet, {std::string("index_str_3_modified"), std::string("index_str_5_modified")}),
			qr);
		ASSERT_EQ(qr.Count(), 0);
	}
}

TEST_F(NsApi, UpdateOutOfBoundsArrayField) {
	// Check, that item modifier does not allow to set value in the array with out of bound index
	const int kTargetID = 1500;

	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	struct Case {
		const std::string name;
		const std::string baseUpdateExpr;
		const std::vector<int> arrayIdx;
		const std::function<Query(const std::string &)> createQueryF;
	};
	const std::vector<Case> cases = {
		{.name = "update-index-array-field",
		 .baseUpdateExpr = "indexed_array_field[%d]",
		 .arrayIdx = {9, 10, 100, 10000, 5000000},
		 .createQueryF =
			 [&](const std::string &path) {
				 return Query(default_namespace).Where("id", CondEq, kTargetID).Set(path, static_cast<int>(777));
			 }},
		{.name = "update-non-indexed-array-field",
		 .baseUpdateExpr = "array_field[%d]",
		 .arrayIdx = {3, 4, 100, 10000, 5000000},
		 .createQueryF =
			 [&](const std::string &path) {
				 return Query(default_namespace).Where("id", CondEq, kTargetID).Set(path, static_cast<int>(777));
			 }},
		{.name = "update-object-array-field",
		 .baseUpdateExpr = "nested.nested_array[%d]",
		 .arrayIdx = {3, 4, 100, 10000, 5000000},
		 .createQueryF = [&](const std::string &path) {
			 return Query(default_namespace)
				 .Where("id", CondEq, kTargetID)
				 .SetObject(path, Variant(std::string(R"({"id":5,"name":"fifth", "prices":[3,5,5]})")), false);
		 }}};

	for (auto &c : cases) {
		TestCout() << c.name << std::endl;

		for (auto idx : c.arrayIdx) {
			// 3. Get initial array value
			std::string initialItemJSON;
			{
				QueryResults qr;
				Error err = rt.reindexer->Select(Query(default_namespace).Where("id", CondEq, kTargetID), qr);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(qr.Count(), 1);
				reindexer::WrSerializer ser;
				err = qr.begin().GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				initialItemJSON = ser.Slice();
			}

			// 4. Set item with out of bound index to specific value via Query builder
			const auto path = fmt::sprintf(c.baseUpdateExpr, idx);
			QueryResults qrUpdate;
			const auto updateQuery = c.createQueryF(path);
			Error err = rt.reindexer->Update(updateQuery, qrUpdate);
			EXPECT_FALSE(err.ok()) << path;

			{
				// 5. Make sure, that item was not changed
				QueryResults qr;
				err = rt.reindexer->Select(Query(default_namespace).Where("id", CondEq, kTargetID), qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; " << path;
				ASSERT_EQ(qr.Count(), 1) << path;
				reindexer::WrSerializer ser;
				err = qr.begin().GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what() << "; " << path;
				ASSERT_EQ(initialItemJSON, ser.Slice()) << path;
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
	QueryResults qr;
	Error err = rt.reindexer->Update(Query(default_namespace).Set("indexed_array_field[0]", Variant(int(777))), qr);
	ASSERT_TRUE(err.ok()) << err.what();

	// 4. Try to access elements of different arrays with Item object functionality
	// to make sure if GetValueByJSONPath() works properly.
	for (auto it : qr) {
		checkIfItemJSONValid(it);

		reindexer::Item item = it.GetItem(false);

		Variant value1 = item["indexed_array_field[0]"];
		ASSERT_TRUE(value1.Type().Is<reindexer::KeyValueType::Int>());
		ASSERT_TRUE(static_cast<int>(value1) == 777);

		Variant value2 = item["objects[0].more[0].array[0]"];
		ASSERT_TRUE(value2.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value2) == 9);

		Variant value3 = item["objects[0].more[0].array[1]"];
		ASSERT_TRUE(value3.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value3) == 8);

		Variant value4 = item["objects[0].more[0].array[2]"];
		ASSERT_TRUE(value4.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value4) == 7);

		Variant value5 = item["objects[0].more[0].array[3]"];
		ASSERT_TRUE(value5.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value5) == 6);

		Variant value6 = item["objects[0].more[0].array[4]"];
		ASSERT_TRUE(value6.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value6) == 5);

		Variant value7 = item["nested.nested_array[1].prices[1]"];
		ASSERT_TRUE(value7.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value7) == 5);
	}
}

TEST_F(NsApi, UpdateComplexArrayItem) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Set objects[0].more[1].array[1] to 777
	QueryResults qr;
	Error err = rt.reindexer->Update(
		Query(default_namespace).Where(idIdxName, CondEq, Variant(1000)).Set("objects[0].more[1].array[1]", Variant(int64_t(777))), qr);
	ASSERT_TRUE(err.ok()) << err.what();

	// 4. Make sure the value of objects[0].more[1].array[1] which was updated above,
	// can be accesses correctly with no problems.
	for (auto it : qr) {
		checkIfItemJSONValid(it);

		reindexer::Item item = it.GetItem(false);

		Variant value = item["objects[0].more[1].array[1]"];
		ASSERT_TRUE(value.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value) == 777);

		Variant value2 = item["objects[0].more[1].array[2]"];
		ASSERT_TRUE(value2.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value2) == 2);
	}
}

TEST_F(NsApi, CheckIndexedArrayItem) {
	// 1. Define NS
	// 2. Fill NS
	DefineDefaultNamespace();
	AddUnindexedData();

	// 3. Select all items of the namespace
	QueryResults qr;
	Error err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();

	// 4. Check if the value of indexed array objects[0].more[1].array[1]
	// can be accessed easily.
	for (auto it : qr) {
		checkIfItemJSONValid(it);

		reindexer::Item item = it.GetItem(false);

		Variant value = item["objects[0].more[1].array[1]"];
		ASSERT_TRUE(value.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value) == 3);

		Variant value1 = item["objects[0].more[1].array[3]"];
		ASSERT_TRUE(value1.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value1) == 1);

		Variant value2 = item["objects[0].more[0].array[4]"];
		ASSERT_TRUE(value2.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(static_cast<int64_t>(value2) == 5);
	}
}

static void checkFieldConversion(const std::shared_ptr<reindexer::Reindexer> &reindexer, const std::string &ns,
								 const std::string &updateFieldPath, const VariantArray &newValue, const VariantArray &updatedValue,
								 reindexer::KeyValueType sourceType, bool expectFail) {
	const Query selectQuery{Query(ns).Where("id", CondGe, Variant("500"))};
	QueryResults qrUpdate;
	Query updateQuery = selectQuery;
	updateQuery.Set(updateFieldPath, newValue);
	Error err = reindexer->Update(updateQuery, qrUpdate);
	if (expectFail) {
		if (err.ok()) {
			for (auto it : qrUpdate) checkIfItemJSONValid(it, true);
		}
		ASSERT_TRUE(!err.ok());
	} else {
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_GT(qrUpdate.Count(), 0);

		QueryResults qrAll;
		err = reindexer->Select(selectQuery, qrAll);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qrAll.Count(), qrUpdate.Count());

		for (auto it : qrAll) {
			Item item = it.GetItem(false);
			VariantArray val = item[updateFieldPath.c_str()];
			ASSERT_TRUE(val.size() == updatedValue.size());
			for (const Variant &v : val) {
				ASSERT_TRUE(v.Type().IsSame(sourceType)) << v.Type().Name();
			}
			ASSERT_TRUE(val == updatedValue);
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

	VariantArray newValue = {Variant(3.33f), Variant(4.33), Variant(5.33), Variant(6.33)};
	checkFieldConversion(rt.reindexer, default_namespace, "array_field", newValue, newValue, reindexer::KeyValueType::Double{}, false);
}

TEST_F(NsApi, TestUpdatePkFieldNoConditions) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qrCount;
	Error err = rt.reindexer->Select("select count(*) from test_namespace", qrCount);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qr;
	err = rt.reindexer->Select("update test_namespace set id = id + " + std::to_string(qrCount.totalCount + 100), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_GT(qr.Count(), 0);

	int i = 0;
	for (auto &it : qr) {
		Item item = it.GetItem(false);
		Variant intFieldVal = item[idIdxName];
		ASSERT_EQ(static_cast<int>(intFieldVal), i + qrCount.totalCount + 100);
		i++;
	}
}

TEST_F(NsApi, TestUpdateIndexArrayWithNull) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace set indexed_array_field = null where id = 1;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	for (auto &it : qr) {
		Item item = it.GetItem(false);
		VariantArray fieldVal = item[indexedArrayField];
		ASSERT_TRUE(fieldVal.empty());
	}
}

TEST_F(NsApi, TestUpdateIndexToSparse) {
	Error err = rt.reindexer->InitSystemNamespaces();
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	const std::string compIndexName = idIdxName + "+" + stringField;

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{intField.c_str(), "hash", "int", IndexOpts(), 0},
											   IndexDeclaration{stringField.c_str(), "hash", "string", IndexOpts(), 0},
											   IndexDeclaration{compIndexName.c_str(), "hash", "composite", IndexOpts(), 0}});
	Item item = NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	const int i = rand() % 20;
	item[idIdxName] = i * 2;
	item[intField] = i;
	item[stringField] = "str_" + std::to_string(i * 5);
	Upsert(default_namespace, item);
	Commit(default_namespace);

	QueryResults qr;
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondEq, i), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	qr.Clear();
	err = rt.reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}),
		qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	auto newIdx = reindexer::IndexDef(intField, "hash", "int", IndexOpts().Sparse());
	err = rt.reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_TRUE(err.ok()) << err.what();

	qr.Clear();
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondEq, i), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	qr.Clear();
	err = rt.reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}),
		qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	newIdx = reindexer::IndexDef(compIndexName, {idIdxName, stringField}, "hash", "composite", IndexOpts().Sparse());
	err = rt.reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_EQ(err.code(), errParams) << err.what();
	ASSERT_EQ(err.what(), "Composite index cannot be sparse. Use non-sparse composite instead");
	// Sparse composite do not have any purpose, so just make sure this index was not affected by updateIndex

	qr.Clear();
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondEq, i), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	qr.Clear();
	err = rt.reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}),
		qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	newIdx = reindexer::IndexDef(intField, "hash", "int", IndexOpts());
	err = rt.reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_TRUE(err.ok()) << err.what();

	qr.Clear();
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondEq, i), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	qr.Clear();
	err = rt.reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}),
		qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	newIdx = reindexer::IndexDef(compIndexName, {idIdxName, stringField}, "hash", "composite", IndexOpts());
	err = rt.reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_TRUE(err.ok()) << err.what();

	qr.Clear();
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondEq, i), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	qr.Clear();
	err = rt.reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compIndexName, CondEq, {reindexer::VariantArray{Variant{i * 2}, Variant{"str_" + std::to_string(i * 5)}}}),
		qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
}

TEST_F(NsApi, TestUpdateNonIndexFieldWithNull) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace set extra = null where id = 1001;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	for (auto &it : qr) {
		Item item = it.GetItem(false);
		Variant fieldVal = item["extra"];
		ASSERT_TRUE(fieldVal.Type().Is<reindexer::KeyValueType::Null>());
	}
}

TEST_F(NsApi, TestUpdateIndexedFieldWithNull) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace set string_field = null where id = 1;", qr);
	EXPECT_TRUE(!err.ok());
}

TEST_F(NsApi, TestUpdateEmptyArrayField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace set indexed_array_field = [] where id = 1;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	Item item = qr[0].GetItem(false);
	Variant idFieldVal = item[idIdxName];
	ASSERT_TRUE(static_cast<int>(idFieldVal) == 1);

	VariantArray arrayFieldVal = item[indexedArrayField];
	ASSERT_TRUE(arrayFieldVal.empty());
}

// Update 2 fields with one query in this order: object field, ordinary field of type String
// https://github.com/restream/reindexer/-/tree/issue_777
TEST_F(NsApi, TestUpdateObjectFieldWithScalar) {
	// Define namespace's schema and fill with data
	DefineDefaultNamespace();
	AddUnindexedData();

	// Prepare and execute Update query
	QueryResults qr;
	Query q = Query(default_namespace)
				  .Set("int_field", 7)
				  .Set("extra", 8)
				  .SetObject("nested2", Variant(std::string(R"({"bonus2":13,"extra2":"new"})")));
	Error err = rt.reindexer->Update(q, qr);
	// Make sure it executed successfully
	ASSERT_TRUE(err.ok()) << err.what();

	// Check in the loop that all the updated fields have correct values
	for (auto it : qr) {
		reindexer::Item item = it.GetItem(false);

		Variant intVal = item["int_field"];
		ASSERT_TRUE(intVal.Type().Is<reindexer::KeyValueType::Int>());
		ASSERT_TRUE(intVal.As<int>() == 7);
		Variant extraVal = item["extra"];
		ASSERT_TRUE(extraVal.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(extraVal.As<int>() == 8);

		std::string_view json = item.GetJSON();
		std::string_view::size_type pos = json.find(R"("nested2":{"bonus2":13,"extra2":"new"})");
		ASSERT_TRUE(pos != std::string_view::npos);

		Variant bonus2Val = item["nested2.bonus2"];
		ASSERT_TRUE(bonus2Val.Type().Is<reindexer::KeyValueType::Int64>());
		ASSERT_TRUE(bonus2Val.As<int>() == 13);
		Variant extra2Val = item["nested2.extra2"];
		ASSERT_TRUE(extra2Val.Type().Is<reindexer::KeyValueType::String>());
		ASSERT_TRUE(extra2Val.As<std::string>() == "new");
	}
}

TEST_F(NsApi, TestUpdateEmptyIndexedField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Query q = Query(default_namespace)
				  .Where("id", CondEq, Variant(1001))
				  .Set(emptyField, Variant("NEW GENERATION"))
				  .Set(indexedArrayField, {Variant(static_cast<int>(4)), Variant(static_cast<int>(5)), Variant(static_cast<int>(6))});
	Error err = rt.reindexer->Update(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	QueryResults qr2;
	err = rt.reindexer->Select("select * from test_namespace where id = 1001;", qr2);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr2.Count(), 1);
	for (auto it : qr2) {
		Item item = it.GetItem(false);

		Variant val = item[emptyField];
		ASSERT_TRUE(val.As<std::string>() == "NEW GENERATION");

		std::string_view json = item.GetJSON();
		ASSERT_TRUE(json.find_first_of("\"empty_field\":\"NEW GENERATION\"") != std::string::npos);

		VariantArray arrayVals = item[indexedArrayField];
		ASSERT_TRUE(arrayVals.size() == 3);
		ASSERT_TRUE(arrayVals[0].As<int>() == 4);
		ASSERT_TRUE(arrayVals[1].As<int>() == 5);
		ASSERT_TRUE(arrayVals[2].As<int>() == 6);
	}
}

TEST_F(NsApi, TestDropField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace drop extra where id >= 1000 and id < 1010;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 10);

	for (auto it : qr) {
		Item item = it.GetItem(false);
		VariantArray val = item["extra"];
		EXPECT_TRUE(val.empty());
		EXPECT_TRUE(item.GetJSON().find("extra") == std::string::npos);
	}

	QueryResults qr2;
	err = rt.reindexer->Select("update test_namespace drop nested.bonus where id >= 1005 and id < 1010;", qr2);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr2.Count(), 5);

	for (auto it : qr2) {
		Item item = it.GetItem(false);
		VariantArray val = item["nested.bonus"];
		EXPECT_TRUE(val.empty());
		EXPECT_TRUE(item.GetJSON().find("nested.bonus") == std::string::npos);
	}

	QueryResults qr3;
	err = rt.reindexer->Select("update test_namespace drop string_field where id >= 1000 and id < 1010;", qr3);
	ASSERT_TRUE(!err.ok());

	QueryResults qr4;
	err = rt.reindexer->Select("update test_namespace drop nested2 where id >= 1030 and id <= 1040;", qr4);
	ASSERT_TRUE(err.ok()) << err.what();
	for (auto it : qr4) {
		Item item = it.GetItem(false);
		EXPECT_TRUE(item.GetJSON().find("nested2") == std::string::npos);
	}
}

TEST_F(NsApi, TestUpdateFieldWithFunction) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	int64_t updateTime = std::chrono::duration_cast<std::chrono::milliseconds>(reindexer::system_clock_w::now().time_since_epoch()).count();

	QueryResults qr;
	Error err = rt.reindexer->Select(
		"update test_namespace set int_field = SERIAL(), extra = SERIAL(), nested.timeField = NOW(msec) where id >= 0;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_GT(qr.Count(), 0);

	int i = 1;
	for (auto &it : qr) {
		Item item = it.GetItem(false);
		Variant intFieldVal = item[intField];
		Variant extraFieldVal = item["extra"];
		Variant timeFieldVal = item["nested.timeField"];
		ASSERT_TRUE(intFieldVal.As<int>() == i++) << intFieldVal.As<int>();
		ASSERT_TRUE(intFieldVal.As<int>() == extraFieldVal.As<int>()) << extraFieldVal.As<int>();
		ASSERT_TRUE(timeFieldVal.As<int64_t>() >= updateTime);
	}
}

TEST_F(NsApi, TestUpdateFieldWithExpressions) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qr;
	Error err = rt.reindexer->Select(
		"update test_namespace set int_field = ((7+8)*(4-3))/3, extra = (SERIAL() + 1)*3, nested.timeField = int_field - 1 where id >= "
		"0;",
		qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_GT(qr.Count(), 0);

	int i = 1;
	for (auto &it : qr) {
		Item item = it.GetItem(false);
		Variant intFieldVal = item[intField];
		Variant extraFieldVal = item["extra"];
		Variant timeFieldVal = item["nested.timeField"];
		ASSERT_TRUE(intFieldVal.As<int>() == 5) << intFieldVal.As<int>();
		ASSERT_TRUE(extraFieldVal.As<int>() == (i + 1) * 3) << extraFieldVal.As<int>();
		ASSERT_TRUE(timeFieldVal.As<int>() == 4) << timeFieldVal.As<int>();
		++i;
	}
}

static void checkQueryDsl(const Query &src) {
	Query dst;
	const std::string dsl = src.GetJSON();
	Error err = dst.FromJSON(dsl);
	EXPECT_TRUE(err.ok()) << err.what();
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
				reindexer::prettyPrintJSON(reindexer::giftStr(std::string_view(src.UpdateFields()[i].Values().front())), wrser1);
				reindexer::WrSerializer wrser2;
				reindexer::prettyPrintJSON(reindexer::giftStr(std::string_view(dst.UpdateFields()[i].Values().front())), wrser2);
				EXPECT_TRUE(wrser1.Slice() == wrser2.Slice());
				objectValues = true;
			}
		}
	}
	if (objectValues) {
		EXPECT_EQ(src.Entries(), dst.Entries());
		EXPECT_EQ(src.aggregations_, dst.aggregations_);
		EXPECT_EQ(src.NsName(), dst.NsName());
		EXPECT_EQ(src.sortingEntries_, dst.sortingEntries_);
		EXPECT_EQ(src.CalcTotal(), dst.CalcTotal());
		EXPECT_EQ(src.Offset(), dst.Offset());
		EXPECT_EQ(src.Limit(), dst.Limit());
		EXPECT_EQ(src.GetDebugLevel(), dst.GetDebugLevel());
		EXPECT_EQ(src.GetStrictMode(), dst.GetStrictMode());
		EXPECT_EQ(src.forcedSortOrder_, dst.forcedSortOrder_);
		EXPECT_EQ(src.SelectFilters(), dst.SelectFilters());
		EXPECT_EQ(src.selectFunctions_, dst.selectFunctions_);
		EXPECT_EQ(src.GetJoinQueries(), dst.GetJoinQueries());
		EXPECT_EQ(src.GetMergeQueries(), dst.GetMergeQueries());
	} else {
		EXPECT_EQ(dst, src);
	}
}

TEST_F(NsApi, TestModifyQueriesSqlEncoder) {
	const std::string sqlUpdate =
		"UPDATE ns SET field1 = 'mrf',field2 = field2+1,field3 = ['one','two','three','four','five'] WHERE a = true AND location = "
		"'msk'";
	Query q1 = Query::FromSQL(sqlUpdate);
	EXPECT_EQ(q1.GetSQL(), sqlUpdate);
	checkQueryDsl(q1);

	const std::string sqlDrop = "UPDATE ns DROP field1,field2 WHERE a = true AND location = 'msk'";
	Query q2 = Query::FromSQL(sqlDrop);
	EXPECT_EQ(q2.GetSQL(), sqlDrop);
	checkQueryDsl(q2);

	const std::string sqlUpdateWithObject =
		R"(UPDATE ns SET field = {"id":0,"name":"apple","price":1000,"nested":{"n_id":1,"desription":"good","array":[{"id":1,"description":"first"},{"id":2,"description":"second"},{"id":3,"description":"third"}]},"bonus":7} WHERE a = true AND location = 'msk')";
	Query q3 = Query::FromSQL(sqlUpdateWithObject);
	EXPECT_EQ(q3.GetSQL(), sqlUpdateWithObject);
	checkQueryDsl(q3);

	const std::string sqlTruncate = R"(TRUNCATE ns)";
	Query q4 = Query::FromSQL(sqlTruncate);
	EXPECT_EQ(q4.GetSQL(), sqlTruncate);
	checkQueryDsl(q4);

	const std::string sqlArrayAppend = R"(UPDATE ns SET array = array||[1,2,3]||array2||objects[0].nested.prices[0])";
	Query q5 = Query::FromSQL(sqlArrayAppend);
	EXPECT_EQ(q5.GetSQL(), sqlArrayAppend);
	checkQueryDsl(q5);

	const std::string sqlIndexUpdate = R"(UPDATE ns SET objects[0].nested.prices[*] = 'NE DOROGO!')";
	Query q6 = Query::FromSQL(sqlIndexUpdate);
	EXPECT_EQ(q6.GetSQL(), sqlIndexUpdate);
	checkQueryDsl(q6);

	const std::string sqlSpeccharsUpdate = R"(UPDATE ns SET f1 = 'HELLO\n\r\b\f',f2 = '\t',f3 = '\"')";
	Query q7 = Query::FromSQL(sqlSpeccharsUpdate);
	EXPECT_EQ(q7.GetSQL(), sqlSpeccharsUpdate);
	checkQueryDsl(q7);
}

static void generateObject(reindexer::JsonBuilder &builder, const std::string &prefix, ReindexerApi *rtapi) {
	builder.Put(prefix + "ID", rand() % 1000);
	builder.Put(prefix + "Name", rtapi->RandString());
	builder.Put(prefix + "Rating", rtapi->RandString());
	builder.Put(prefix + "Description", rtapi->RandString());
	builder.Put(prefix + "Price", rand() % 1000 + 100);
	builder.Put(prefix + "IMDB", 7.77777777777f);
	builder.Put(prefix + "Subsription", bool(rand() % 100 > 50 ? 1 : 0));
	{
		auto idsArray = builder.Array(prefix + "IDS");
		for (auto id : rtapi->RandIntVector(10, 10, 1000)) idsArray.Put(0, id);
	}
	{
		auto homogeneousArray = builder.Array(prefix + "HomogeneousValues");
		for (int i = 0; i < 20; ++i) {
			if (i % 2 == 0) {
				homogeneousArray.Put(0, rand());
			} else {
				if (i % 5 == 0) {
					homogeneousArray.Put(0, 234.778f);
				} else {
					homogeneousArray.Put(0, rtapi->RandString());
				}
			}
		}
	}
}

void addObjectsArray(reindexer::JsonBuilder &builder, bool withInnerArray, ReindexerApi *rtapi) {
	size_t size = rand() % 10 + 5;
	reindexer::JsonBuilder array = builder.Array("object");
	for (size_t i = 0; i < size; ++i) {
		reindexer::JsonBuilder obj = array.Object(0);
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
			for (auto price : RandIntVector(10, 10, 1000)) priceArray.Put(0, price);
		}
		{
			reindexer::JsonBuilder objectBuilder = jsonBuilder.Object("nested1");
			generateObject(objectBuilder, "nested1", this);
			addObjectsArray(objectBuilder, true, this);
		}
		jsonBuilder.Put("superBonus", RuRandString());
		addObjectsArray(jsonBuilder, false, this);
		jsonBuilder.End();

		Item item = NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		Error err = item.FromJSON(wrser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		Upsert(default_namespace, item);

		reindexer::WrSerializer wrSer2;
		err = item.GetMsgPack(wrSer2);
		ASSERT_TRUE(err.ok()) << err.what();

		err = item.GetMsgPack(wrSer1);
		ASSERT_TRUE(err.ok()) << err.what();

		size_t offset = 0;
		Item item2 = NewItem(default_namespace);
		err = item2.FromMsgPack(std::string_view(reinterpret_cast<const char *>(wrSer2.Buf()), wrSer2.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		std::string json1(item.GetJSON());
		std::string json2(item2.GetJSON());
		ASSERT_TRUE(json1 == json2);
		items.emplace_back(json2);
	}

	QueryResults qr;
	int i = 0;
	size_t length = wrSer1.Len();
	size_t offset = 0;
	while (offset < length) {
		Item item = NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		Error err = item.FromMsgPack(std::string_view(reinterpret_cast<const char *>(wrSer1.Buf()), wrSer1.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Update(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();

		std::string json(item.GetJSON());
		ASSERT_EQ(json, items[i++]);
	}

	reindexer::WrSerializer wrSer3;
	for (size_t i = 0; i < qr.Count(); ++i) {
		const auto err = qr[i].GetMsgPack(wrSer3, false);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	i = 0;
	offset = 0;
	while (offset < length) {
		Item item = NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		Error err = item.FromMsgPack(std::string_view(reinterpret_cast<const char *>(wrSer3.Buf()), wrSer3.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		std::string json(item.GetJSON());
		ASSERT_EQ(json, items[i++]);
	}
}

TEST_F(NsApi, MsgPackFromJson) {
	DefineDefaultNamespace();
	const std::string json = R"xxx({
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
	ASSERT_TRUE(item1.Status().ok()) << item1.Status().what();

	size_t offset = 0;
	Error err = item1.FromMsgPack(msgpackSer.Slice(), offset);
	ASSERT_TRUE(err.ok()) << err.what();

	Item item2 = NewItem(default_namespace);
	ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();

	err = item2.FromJSON(jsonSer.Slice());
	ASSERT_TRUE(err.ok()) << err.what();

	std::string json1(item1.GetJSON());
	std::string json2(item2.GetJSON());
	ASSERT_TRUE(json1 == json2);
}

TEST_F(NsApi, DeleteLastItems) {
	// Check for bug with memory access after items removing
	DefineDefaultNamespace();
	FillDefaultNamespace(2);
	QueryResults qr;
	auto err = rt.reindexer->Delete(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 2);
}

TEST_F(NsApi, IncorrectNsName) {
	auto check = [&](const std::vector<std::string> &names, auto func) {
		for (const auto &v : names) {
			func(v);
		}
	};
	std::vector<std::string> variants = {"tes@t1", "@test1", "test1@",	"tes#t1",	 "#test1",		 "test1#", "test 1",
										 " test1", "test1 ", "'test1'", "\"test1\"", "<a>test1</a>", "/test1", "test1,test2"};

	auto open = [&](const std::string &name) {
		Error err = rt.reindexer->OpenNamespace(name);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
	};
	check(variants, open);

	auto add = [&](const std::string &name) {
		reindexer::NamespaceDef nsDef(name);
		Error err = rt.reindexer->AddNamespace(nsDef);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
	};
	check(variants, add);

	auto rename = [&](const std::string &name) {
		const std::string kNsName("test3");
		reindexer::NamespaceDef nsDef(kNsName);
		Error err = rt.reindexer->AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->RenameNamespace(kNsName, name);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		err = rt.reindexer->DropNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
	};
	check(variants, rename);
}
