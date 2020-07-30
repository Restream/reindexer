#include <chrono>
#include "core/cbinding/resultserializer.h"
#include "core/cjson/ctag.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"
#include "core/itemimpl.h"
#include "ns_api.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"
#include "vendor/msgpack/msgpack.h"

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
											   IndexDeclaration{serialFieldName.c_str(), "", "int64", IndexOpts(), 0}});

	Item item = NewItem(default_namespace);
	item[idIdxName] = idNum;

	// Set precepts
	vector<string> precepts = {updatedTimeSecFieldName + "=NOW()", updatedTimeMSecFieldName + "=NOW(msec)",
							   updatedTimeUSecFieldName + "=NOW(usec)", updatedTimeNSecFieldName + "=NOW(nsec)",
							   serialFieldName + "=SERIAL()"};
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
		Item item = it.GetItem();
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
	vector<string> precepts = {updatedTimeNSecFieldName + "=NOW(nsec)", serialFieldName + "=SERIAL()"};
	item.SetPrecepts(precepts);

	// Check Insert
	err = rt.reindexer->Insert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::QueryResults res1;
	err = rt.reindexer->Select("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum), res1);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(res1.Count(), 1);
	Item selectedItem = res1.begin().GetItem();
	CheckItemsEqual(item, selectedItem);

	// Check Update
	err = rt.reindexer->Update(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::QueryResults res2;
	err = rt.reindexer->Select("SELECT * FROM " + default_namespace + " WHERE " + idIdxName + "=" + std::to_string(idNum), res2);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(res2.Count(), 1);
	selectedItem = res2.begin().GetItem();
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
	selectedItem = res3.begin().GetItem();
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

	auto newIdx = reindexer::IndexDef(idIdxName, "-", "int64", IndexOpts().PK().Dense());
	err = rt.reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_TRUE(err.ok()) << err.what();

	vector<reindexer::NamespaceDef> nsDefs;
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

	string newConfig = R"json({
                       "type":"profiling",
                       "profiling":{
                           "queriesperfstats":true,
                           "queries_threshold_us":0,
                           "perfstats":true,
                           "memstats":true
                       }
                   })json";

	err = item.FromJSON(newConfig);
	ASSERT_TRUE(err.ok()) << err.what();

	Upsert(configNs, item);
	err = Commit(configNs);
	ASSERT_TRUE(err.ok()) << err.what();

	struct QueryPerformance {
		string query;
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
	const string querySql(testQuery.GetSQL(true));

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
			ASSERT_TRUE(qr.Count() > 0) << "#queriesperfstats table is empty!";
			for (size_t i = 0; i < qr.Count(); ++i) {
				std::cout << qr[i].GetItem().GetJSON() << std::endl;
			}
		}
		ASSERT_TRUE(qres.Count() == 1) << "Expected 1 row for this query, got " << qres.Count();
		Item item = qres[0].GetItem();
		Variant val;
		val = item["latency_stddev"];
		performanceRes.latencyStddev = static_cast<double>(val);
		val = item["min_latency_us"];
		performanceRes.minLatencyUs = val.As<int64_t>();
		val = item["max_latency_us"];
		performanceRes.maxLatencyUs = val.As<int64_t>();
		val = item["query"];
		performanceRes.query = val.As<string>();
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

void checkIfItemJSONValid(QueryResults::Iterator &it, bool print = false) {
	reindexer::WrSerializer wrser;
	Error err = it.GetJSON(wrser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	if (err.ok() && print) std::cout << wrser.Slice() << std::endl;
}

TEST_F(NsApi, TestUpdateIndexedField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qrUpdate;
	Query updateQuery = Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500))).Set(stringField, "bingo!");
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qrAll;
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500))), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : qrAll) {
		Item item = it.GetItem();
		Variant val = item[stringField];
		ASSERT_TRUE(val.Type() == KeyValueString);
		ASSERT_TRUE(val.As<string>() == "bingo!") << val.As<string>();
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, TestUpdateNonindexedField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qrUpdate;
	Query updateQuery = Query(default_namespace).Where("id", CondGe, Variant("1500")).Set("nested.bonus", static_cast<int>(100500));
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qrUpdate.Count() == 500) << qrUpdate.Count();

	QueryResults qrAll;
	err = rt.reindexer->Select(Query(default_namespace).Where("id", CondGe, Variant("1500")), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qrAll.Count() == 500) << qrAll.Count();

	for (auto it : qrAll) {
		Item item = it.GetItem();
		Variant val = item["nested.bonus"];
		ASSERT_TRUE(val.Type() == KeyValueInt64);
		ASSERT_TRUE(val.As<int64_t>() == 100500);
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, TestUpdateSparseField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qrUpdate;
	Query updateQuery = Query(default_namespace).Where("id", CondGe, Variant("1500")).Set("sparse_field", static_cast<int>(100500));
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qrUpdate.Count() == 500) << qrUpdate.Count();

	QueryResults qrAll;
	err = rt.reindexer->Select(Query(default_namespace).Where("id", CondGe, Variant("1500")), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qrAll.Count() == 500) << qrAll.Count();

	for (auto it : qrAll) {
		Item item = it.GetItem();
		Variant val = item["sparse_field"];
		ASSERT_TRUE(val.Type() == KeyValueInt64);
		ASSERT_TRUE(val.As<int>() == 100500);
		checkIfItemJSONValid(it);
	}
}

void updateArrayField(std::shared_ptr<reindexer::Reindexer> reindexer, const string &ns, const string &updateFieldPath,
					  const VariantArray &values) {
	QueryResults qrUpdate;
	Query updateQuery = Query(ns).Where("id", CondGe, Variant("500")).Set(updateFieldPath, values);
	Error err = reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qrUpdate.Count() > 0) << qrUpdate.Count();

	QueryResults qrAll;
	err = reindexer->Select(Query(ns).Where("id", CondGe, Variant("500")), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qrAll.Count() == qrUpdate.Count()) << qrAll.Count();

	for (auto it : qrAll) {
		Item item = it.GetItem();
		VariantArray val = item[updateFieldPath.c_str()];
		ASSERT_TRUE(val.size() == values.size()) << val.size() << ":" << values.size();
		ASSERT_TRUE(val == values);
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, TestUpdateNonindexedArrayField) {
	DefineDefaultNamespace();
	AddUnindexedData();
	updateArrayField(rt.reindexer, default_namespace, "array_field",
					 {Variant(static_cast<int64_t>(3)), Variant(static_cast<int64_t>(4)), Variant(static_cast<int64_t>(5)),
					  Variant(static_cast<int64_t>(6))});
}

TEST_F(NsApi, TestUpdateNonindexedArrayField2) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err = rt.reindexer->Select(R"(update test_namespace set 'nested.bonus'=[{"first":1,"second":2,"third":3}] where id = 1000;)", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1) << qr.Count();

	Item item = qr[0].GetItem();
	reindexer::string_view json = item.GetJSON();
	size_t pos = json.find(R"("nested":{"bonus":[{"first":1,"second":2,"third":3}]})");
	ASSERT_TRUE(pos != std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField3) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err =
		rt.reindexer->Select(R"(update test_namespace set 'nested.bonus'=[{"id":1},{"id":2},{"id":3},{"id":4}] where id = 1000;)", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1) << qr.Count();

	Item item = qr[0].GetItem();
	VariantArray val = item["nested.bonus"];
	ASSERT_TRUE(val.size() == 4);

	size_t length = 0;
	reindexer::string_view json = item.GetJSON();
	gason::JsonParser jsonParser;
	ASSERT_NO_THROW(jsonParser.Parse(json, &length));
	ASSERT_TRUE(length > 0);

	size_t pos = json.find(R"("nested":{"bonus":[{"id":1},{"id":2},{"id":3},{"id":4}]})");
	ASSERT_TRUE(pos != std::string::npos) << "'nested.bonus' was not updated properly" << json;
}

TEST_F(NsApi, TestUpdateNonindexedArrayField4) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err = rt.reindexer->Select(R"(update test_namespace set 'nested.bonus'=[0] where id = 1000;)", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1) << qr.Count();

	Item item = qr[0].GetItem();
	reindexer::string_view json = item.GetJSON();
	size_t pos = json.find(R"("nested":{"bonus":[0]})");
	ASSERT_TRUE(pos != std::string::npos) << "'nested.bonus' was not updated properly" << json;
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
	value.MarkArray();
	Query q = Query(default_namespace).Where(idIdxName, CondEq, static_cast<int>(1000)).Set(indexedArrayField, std::move(value));
	Error err = rt.reindexer->Update(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1) << qr.Count();

	Item item = qr[0].GetItem();
	reindexer::string_view json = item.GetJSON();
	size_t pos = json.find(R"("indexed_array_field":[77])");
	ASSERT_TRUE(pos != std::string::npos) << "'indexed_array_field' was not updated properly" << json;
}

void addAndSetNonindexedField(std::shared_ptr<reindexer::Reindexer> reindexer, const string &ns, const string &updateFieldPath) {
	QueryResults qrUpdate;
	Query updateQuery = Query(ns).Where("nested.bonus", CondGe, Variant(500)).Set(updateFieldPath, static_cast<int64_t>(777));
	Error err = reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qrAll;
	err = reindexer->Select(Query(ns).Where("nested.bonus", CondGe, Variant(500)), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : qrAll) {
		Item item = it.GetItem();
		Variant val = item[updateFieldPath.c_str()];
		ASSERT_TRUE(val.Type() == KeyValueInt64);
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

void checkFieldConversion(std::shared_ptr<reindexer::Reindexer> reindexer, const string &ns, const string &updateFieldPath,
						  const VariantArray &newValue, const VariantArray &updatedValue, KeyValueType sourceType, bool expectFail) {
	const Query selectQuery = Query(ns).Where("id", CondGe, Variant("500"));
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
		ASSERT_TRUE(qrUpdate.Count() > 0) << qrUpdate.Count();

		QueryResults qrAll;
		err = reindexer->Select(selectQuery, qrAll);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qrAll.Count() == qrUpdate.Count()) << qrAll.Count();

		for (auto it : qrAll) {
			Item item = it.GetItem();
			VariantArray val = item[updateFieldPath.c_str()];
			ASSERT_TRUE(val.size() == updatedValue.size());
			for (const Variant &v : val) {
				ASSERT_TRUE(v.Type() == sourceType) << v.Type();
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
						 {Variant(static_cast<int>(13.33f))}, KeyValueInt, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(static_cast<int64_t>(13))}, {Variant(static_cast<int>(13))},
						 KeyValueInt, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(static_cast<bool>(false))}, {Variant(static_cast<int>(0))},
						 KeyValueInt, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(static_cast<bool>(true))}, {Variant(static_cast<int>(1))},
						 KeyValueInt, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(string("100500"))}, {Variant(static_cast<int>(100500))},
						 KeyValueInt, false);

	checkFieldConversion(rt.reindexer, default_namespace, intField, {Variant(string("Jesus Christ"))}, {Variant(static_cast<int>(0))},
						 KeyValueInt, false);
}

TEST_F(NsApi, TestDoubleIndexedFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<int>(13333))},
						 {Variant(static_cast<double>(13333.0f))}, KeyValueDouble, false);

	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<int64_t>(13333))},
						 {Variant(static_cast<double>(13333.0f))}, KeyValueDouble, false);

	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<bool>(false))},
						 {Variant(static_cast<double>(0.0f))}, KeyValueDouble, false);

	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(static_cast<bool>(true))},
						 {Variant(static_cast<double>(1.0f))}, KeyValueDouble, false);

	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(string("100500.1"))},
						 {Variant(static_cast<double>(100500.100000))}, KeyValueDouble, false);

	checkFieldConversion(rt.reindexer, default_namespace, doubleField, {Variant(string("Jesus Christ"))}, {Variant()}, KeyValueDouble,
						 true);
}

TEST_F(NsApi, TestBoolIndexedFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(static_cast<int>(100500))}, {Variant(true)}, KeyValueBool,
						 false);

	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(static_cast<int64_t>(100500))}, {Variant(true)}, KeyValueBool,
						 false);

	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(static_cast<double>(100500.1))}, {Variant(true)},
						 KeyValueBool, false);

	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(string("1"))}, {Variant(false)}, KeyValueBool, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(string("0"))}, {Variant(false)}, KeyValueBool, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(string("true"))}, {Variant(true)}, KeyValueBool, false);
	checkFieldConversion(rt.reindexer, default_namespace, boolField, {Variant(string("false"))}, {Variant(false)}, KeyValueBool, false);
}

TEST_F(NsApi, TestStringIndexedFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(rt.reindexer, default_namespace, stringField, {Variant(static_cast<int>(100500))}, {Variant("100500")},
						 KeyValueString, false);

	checkFieldConversion(rt.reindexer, default_namespace, stringField, {Variant(true)}, {Variant(string("true"))}, KeyValueString, false);

	checkFieldConversion(rt.reindexer, default_namespace, stringField, {Variant(false)}, {Variant(string("false"))}, KeyValueString, false);
}

TEST_F(NsApi, TestIntNonindexedFieldConversion) {
	DefineDefaultNamespace();
	AddUnindexedData();

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<double>(13.33f))},
						 {Variant(static_cast<double>(13.33f))}, KeyValueDouble, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<int>(13))},
						 {Variant(static_cast<int64_t>(13))}, KeyValueInt64, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<bool>(false))},
						 {Variant(static_cast<bool>(false))}, KeyValueBool, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<bool>(true))},
						 {Variant(static_cast<bool>(true))}, KeyValueBool, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(string("100500"))}, {Variant(string("100500"))},
						 KeyValueString, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(string("Jesus Christ"))},
						 {Variant(string("Jesus Christ"))}, KeyValueString, false);
}

TEST_F(NsApi, TestIndexedArrayFieldConversion) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	checkFieldConversion(
		rt.reindexer, default_namespace, indexedArrayField,
		{Variant(static_cast<double>(1.33f)), Variant(static_cast<double>(2.33f)), Variant(static_cast<double>(3.33f)),
		 Variant(static_cast<double>(4.33f))},
		{Variant(static_cast<int>(1)), Variant(static_cast<int>(2)), Variant(static_cast<int>(3)), Variant(static_cast<int>(4))},
		KeyValueInt, false);
}

TEST_F(NsApi, TestNonIndexedArrayFieldConversion) {
	DefineDefaultNamespace();
	AddUnindexedData();

	VariantArray newValue = {Variant(3.33f), Variant(4.33), Variant(5.33), Variant(6.33)};
	checkFieldConversion(rt.reindexer, default_namespace, "array_field", newValue, newValue, KeyValueDouble, false);
}

TEST_F(NsApi, TestUpdatePkFieldNoConditions) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace set id = id + 1;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() > 0);

	int i = 1;
	for (auto &it : qr) {
		Item item = it.GetItem();
		Variant intFieldVal = item[idIdxName];
		ASSERT_TRUE(static_cast<int>(intFieldVal) == i++);
	}
}

TEST_F(NsApi, TestUpdateIndexArrayWithNull) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace set indexed_array_field = null where id = 1;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1);

	for (auto &it : qr) {
		Item item = it.GetItem();
		VariantArray fieldVal = item[indexedArrayField];
		ASSERT_TRUE(fieldVal.empty());
	}
}

TEST_F(NsApi, TestUpdateNonIndexFieldWithNull) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace set extra = null where id = 1001;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1);

	for (auto &it : qr) {
		Item item = it.GetItem();
		Variant fieldVal = item["extra"];
		ASSERT_TRUE(fieldVal.Type() == KeyValueNull);
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
	ASSERT_TRUE(qr.Count() == 1);

	Item item = qr[0].GetItem();
	Variant idFieldVal = item[idIdxName];
	ASSERT_TRUE(static_cast<int>(idFieldVal) == 1);

	VariantArray arrayFieldVal = item[indexedArrayField];
	ASSERT_TRUE(arrayFieldVal.empty());
}

TEST_F(NsApi, TestDropField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qr;
	Error err = rt.reindexer->Select("update test_namespace drop extra where id >= 1000 and id < 1010;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 10) << qr.Count();

	for (auto it : qr) {
		Item item = it.GetItem();
		VariantArray val = item["extra"];
		EXPECT_TRUE(val.empty());
		EXPECT_TRUE(item.GetJSON().find("extra") == string::npos);
	}

	QueryResults qr2;
	err = rt.reindexer->Select("update test_namespace drop 'nested.bonus' where id >= 1005 and id < 1010;", qr2);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr2.Count() == 5);

	for (auto it : qr2) {
		Item item = it.GetItem();
		VariantArray val = item["nested.bonus"];
		EXPECT_TRUE(val.empty());
		EXPECT_TRUE(item.GetJSON().find("nested.bonus") == string::npos);
	}

	QueryResults qr3;
	err = rt.reindexer->Select("update test_namespace drop string_field where id >= 1000 and id < 1010;", qr3);
	ASSERT_TRUE(!err.ok());

	QueryResults qr4;
	err = rt.reindexer->Select("update test_namespace drop nested2 where id >= 1030 and id <= 1040;", qr4);
	ASSERT_TRUE(err.ok()) << err.what();
	for (auto it : qr4) {
		Item item = it.GetItem();
		EXPECT_TRUE(item.GetJSON().find("nested2") == string::npos);
	}
}

TEST_F(NsApi, TestUpdateFieldWithFunction) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	int64_t updateTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

	QueryResults qr;
	Error err = rt.reindexer->Select(
		"update test_namespace set int_field = SERIAL(), extra = SERIAL(), 'nested.timeField' = NOW(msec) where id >= 0;", qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() > 0);

	int i = 1;
	for (auto &it : qr) {
		Item item = it.GetItem();
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
		"update test_namespace set int_field = ((7+8)*(4-3))/3, extra = (SERIAL() + 1)*3, 'nested.timeField' = int_field - 1 where id >= "
		"0;",
		qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() > 0) << qr.Count();

	int i = 1;
	for (auto &it : qr) {
		Item item = it.GetItem();
		Variant intFieldVal = item[intField];
		Variant extraFieldVal = item["extra"];
		Variant timeFieldVal = item["nested.timeField"];
		ASSERT_TRUE(intFieldVal.As<int>() == 5) << intFieldVal.As<int>();
		ASSERT_TRUE(extraFieldVal.As<int>() == (i + 1) * 3) << extraFieldVal.As<int>();
		ASSERT_TRUE(timeFieldVal.As<int>() == 4) << timeFieldVal.As<int>();
		++i;
	}
}

TEST_F(NsApi, TestUpdateQuerySqlEncoder) {
	const string sqlUpdate = "UPDATE ns SET field1 = 5,field2 = field2+1 WHERE a > 0 AND b = 77";
	Query q1;
	q1.FromSQL(sqlUpdate);
	EXPECT_TRUE(q1.GetSQL() == sqlUpdate) << q1.GetSQL();

	const string sqlDrop = "UPDATE ns DROP field1,field2 WHERE a > 0 AND b = 77";
	Query q2;
	q2.FromSQL(sqlDrop);
	EXPECT_TRUE(q2.GetSQL() == sqlDrop) << q2.GetSQL();

	const string sqlUpdateWithObject =
		R"(UPDATE ns SET field = {"id":0,"name":"apple","price":1000,"nested":{"n_id":1,"desription":"good"},"bonus":7} WHERE a > 0 AND b = 77)";
	Query q3;
	q3.FromSQL(sqlUpdateWithObject);
	EXPECT_TRUE(q3.GetSQL() == sqlUpdateWithObject) << q3.GetSQL();
}

void generateObject(reindexer::JsonBuilder &builder, const string &prefix, ReindexerApi *rtapi) {
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

	vector<string> items;
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
		err = item2.FromMsgPack(reindexer::string_view(reinterpret_cast<const char *>(wrSer2.Buf()), wrSer2.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		string json1(item.GetJSON());
		string json2(item2.GetJSON());
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

		Error err = item.FromMsgPack(reindexer::string_view(reinterpret_cast<const char *>(wrSer1.Buf()), wrSer1.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Update(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();

		string json(item.GetJSON());
		ASSERT_TRUE(json == items[i++]);

		qr.AddItem(item, true, false);
	}

	qr.lockResults();

	reindexer::WrSerializer wrSer3;
	for (size_t i = 0; i < qr.Count(); ++i) {
		qr[i].GetMsgPack(wrSer3, false);
	}

	i = 0;
	offset = 0;
	while (offset < length) {
		Item item = NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		Error err = item.FromMsgPack(reindexer::string_view(reinterpret_cast<const char *>(wrSer3.Buf()), wrSer3.Len()), offset);
		ASSERT_TRUE(err.ok()) << err.what();

		string json(item.GetJSON());
		ASSERT_TRUE(json == items[i++]);
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
	reindexer::MsgPackBuilder msgpackBuilder(msgpackSer, ObjType::TypeObject, 1);
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

	string json1(item1.GetJSON());
	string json2(item2.GetJSON());
	ASSERT_TRUE(json1 == json2);
}
