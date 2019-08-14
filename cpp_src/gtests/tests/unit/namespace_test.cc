#include <chrono>
#include "ns_api.h"
#include "tools/serializer.h"

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
	err = rt.reindexer->EnumNamespaces(nsDefs, false);
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
                           "queries_threshold_us":10,
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
		double latencyStddev = 0;
		int64_t minLatencyUs = 0;
		int64_t maxLatencyUs = 0;
		void Dump() const {
			std::cout << "stddev: " << latencyStddev << std::endl;
			std::cout << "min: " << minLatencyUs << std::endl;
			std::cout << "max: " << maxLatencyUs << std::endl;
		}
	};

	Query testQuery(default_namespace);
	reindexer::WrSerializer querySerializer;
	testQuery.GetSQL(querySerializer, true);
	const string querySql(querySerializer.Slice());

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
				std::cout << qr[0].GetItem().GetJSON() << std::endl;
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
		prevQperf = qperf;
	}
}

void checkIfItemJSONValid(QueryResults::Iterator &it, bool print = false) {
	reindexer::WrSerializer wrser;
	Error err = it.GetJSON(wrser);
	ASSERT_TRUE(err.ok()) << err.what();
	if (err.ok() && print) std::cout << wrser.Slice() << std::endl;
}

TEST_F(NsApi, TestUpdateIndexedField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();

	QueryResults qrUpdate;
	Query updateQuery = Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500)));
	updateQuery.updateFields_.push_back({stringField, {Variant("bingo!")}});
	Error err = rt.reindexer->Update(updateQuery, qrUpdate);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qrAll;
	err = rt.reindexer->Select(Query(default_namespace).Where(intField, CondGe, Variant(static_cast<int>(500))), qrAll);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : qrAll) {
		Item item = it.GetItem();
		Variant val = item[stringField];
		ASSERT_TRUE(val.Type() == KeyValueString);
		ASSERT_TRUE(val.As<string>() == "bingo!");
		checkIfItemJSONValid(it);
	}
}

TEST_F(NsApi, TestUpdateNonindexedField) {
	DefineDefaultNamespace();
	AddUnindexedData();

	QueryResults qrUpdate;
	Query updateQuery = Query(default_namespace).Where("id", CondGe, Variant("1500"));
	updateQuery.updateFields_.push_back({"nested.bonus", {Variant(static_cast<int>(100500))}});
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
	Query updateQuery = Query(default_namespace).Where("id", CondGe, Variant("1500"));
	updateQuery.updateFields_.push_back({"sparse_field", {Variant(static_cast<int>(100500))}});
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
	Query updateQuery = Query(ns).Where("id", CondGe, Variant("500"));
	updateQuery.updateFields_.push_back({updateFieldPath, values});
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
		ASSERT_TRUE(val.size() == values.size());
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

TEST_F(NsApi, TestUpdateIndexedArrayField) {
	DefineDefaultNamespace();
	FillDefaultNamespace();
	updateArrayField(rt.reindexer, default_namespace, indexedArrayField,
					 {Variant(7), Variant(8), Variant(9), Variant(10), Variant(11), Variant(12), Variant(13)});
}

void addAndSetNonindexedField(std::shared_ptr<reindexer::Reindexer> reindexer, const string &ns, const string &updateFieldPath) {
	QueryResults qrUpdate;
	Query updateQuery = Query(ns).Where("nested.bonus", CondGe, Variant(500));
	updateQuery.updateFields_.push_back({updateFieldPath, {Variant(static_cast<int64_t>(777))}});
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
	updateQuery.updateFields_.push_back({updateFieldPath, newValue});
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
				ASSERT_TRUE(v.Type() == sourceType);
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
						 {Variant(static_cast<int64_t>(13))}, KeyValueInt64, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<int>(13))},
						 {Variant(static_cast<int64_t>(13))}, KeyValueInt64, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<bool>(false))},
						 {Variant(static_cast<int64_t>(0))}, KeyValueInt64, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(static_cast<bool>(true))},
						 {Variant(static_cast<int64_t>(1))}, KeyValueInt64, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(string("100500"))},
						 {Variant(static_cast<int64_t>(100500))}, KeyValueInt64, false);

	checkFieldConversion(rt.reindexer, default_namespace, "nested.bonus", {Variant(string("Jesus Christ"))},
						 {Variant(static_cast<int64_t>(0))}, KeyValueInt64, true);
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
	ASSERT_TRUE(qr.Count() > 0);

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
