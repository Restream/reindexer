#include "ns_api.h"
#include "tools/serializer.h"

TEST_F(NsApi, UpsertWithPrecepts) {
	Error err = reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK()},
											   IndexDeclaration{updatedTimeSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{updatedTimeMSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{updatedTimeUSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{updatedTimeNSecFieldName.c_str(), "", "int64", IndexOpts()},
											   IndexDeclaration{serialFieldName.c_str(), "", "int64", IndexOpts()}});

	Item item = NewItem(default_namespace);
	item["id"] = idNum;

	// Set precepts
	vector<string> precepts = {updatedTimeSecFieldName + "=NOW()", updatedTimeMSecFieldName + "=NOW(msec)",
							   updatedTimeUSecFieldName + "=NOW(usec)", updatedTimeNSecFieldName + "=NOW(nsec)",
							   serialFieldName + "=SERIAL()"};
	item.SetPrecepts(precepts);

	// Upsert item a few times
	for (int i = 0; i < upsertTimes; i++) {
		auto err = reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// Get item
	reindexer::QueryResults res;
	err = reindexer->Select("SELECT * FROM " + default_namespace + " WHERE id=" + to_string(idNum), res);
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

TEST_F(NsApi, UpdateIndex) {
	Error err = reindexer->InitSystemNamespaces();
	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK()}});

	auto newIdx = reindexer::IndexDef(idIdxName, "-", "int64", IndexOpts().PK().Dense());
	err = reindexer->UpdateIndex(default_namespace, newIdx);
	ASSERT_TRUE(err.ok()) << err.what();

	vector<reindexer::NamespaceDef> nsDefs;
	err = reindexer->EnumNamespaces(nsDefs, false);
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

	string newIdxJson = newIdxSer.Slice().ToString();
	string receivedIdxJson = receivedIdxSer.Slice().ToString();

	ASSERT_TRUE(newIdxJson == receivedIdxJson);
}

TEST_F(NsApi, QueryperfstatsNsDummyTest) {
	Error err = reindexer->InitSystemNamespaces();
	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK()}});

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
	const string querySql = querySerializer.Slice().ToString();

	auto performSimpleQuery = [&]() {
		QueryResults qr;
		Error err = reindexer->Select(testQuery, qr);
		ASSERT_TRUE(err.ok()) << err.what();
	};

	auto getPerformanceParams = [&](QueryPerformance &performanceRes) {
		QueryResults qres;
		Error err = reindexer->Select(Query("#queriesperfstats").Where("query", CondEq, Variant(querySql)), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		if (qres.Count() == 0) {
			QueryResults qr;
			err = reindexer->Select(Query("#queriesperfstats"), qr);
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
