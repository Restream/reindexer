#include <unordered_map>
#include <unordered_set>
#include "replication_load_api.h"
#include "replicator/walrecord.h"

using reindexer::Query;

// clang-format off
constexpr std::string_view kReplTestSchema1 = R"xxx(
	{
	  "required": [
		"id"
	  ],
	  "properties": {
		"id": {
		  "type": "number"
		},
		"Field": {
		  "type": "number"
		}
	  },
	  "additionalProperties": false,
	  "type": "object",
	  "x-protobuf-ns-number": 99998
	})xxx";


constexpr std::string_view kReplTestSchema2 = R"xxx(
	{
	  "required": [
		"id"
	  ],
	  "properties": {
		"id": {
		  "type": "number"
		},
		"data": {
		  "type": "number"
		},
		"data123": {
		  "type": "string"
		},
		"f": {
		  "type": "bolean"
		}
	  },
	  "additionalProperties": false,
	  "type": "object",
	  "x-protobuf-ns-number": 99999
	})xxx";
// clang-format on

TEST_F(ReplicationLoadApi, Base) {
	InitNs();
	stop = false;
	SetWALSize(masterId_, 100000, "some");
	FillData(1000);

	std::thread destroyer([this]() {
		int count = 0;
		while (!stop) {
			if (!(count % 30)) {
				int i = rand() % kDefaultServerCount;
				RestartServer(i);
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	});

	SetWALSize(masterId_, 50000, "some");
	for (size_t i = 0; i < 2; ++i) {
		if (i % 3 == 0) {
			DeleteFromMaster();
		}
		SetWALSize(masterId_, (int64_t(i) + 1) * 25000, "some1");
		FillData(1000);
		SetWALSize(masterId_, (int64_t(i) + 1) * 50000, "some");
		SimpleSelect(0);
	}

	SetWALSize(masterId_, 50000, "some1");

	stop = true;
	destroyer.join();

	ForceSync();  // restart_replicator call syncDatabase (syncByWal or forceSync)
	WaitSync("some");
	WaitSync("some1");
}

TEST_F(ReplicationLoadApi, UpdateTimeAfterRestart) {
	InitNs();

	constexpr std::string_view kNs = "some";
	constexpr int kTargetSrv = 1;
	SetOptmizationSortWorkers(kTargetSrv, 0, kNs);

	FillData(10);
	WaitSync(kNs);

	const auto state0 = GetSrv(kTargetSrv)->GetState(kNs);
	EXPECT_GT(state0.updateUnixNano, 0);

	RestartServer(kTargetSrv);
	const auto state1 = GetSrv(kTargetSrv)->GetState(kNs);
	ASSERT_EQ(state0.updateUnixNano, state1.updateUnixNano);
}

TEST_F(ReplicationLoadApi, BaseTagsMatcher) {
	StopServer(1);
	StopServer(2);

	InitNs();
	SetSchema(masterId_, "some1", kReplTestSchema2);
	FillData(1000);
	for (size_t i = 0; i < 2; ++i) {
		if (i == 1) {
			DeleteFromMaster();
		}
		FillData(1000);
	}
	StartServer(1);
	StartServer(2);

	ForceSync();  // restart_replicator call syncDatabase (syncByWal or forceSync)
	WaitSync("some");
	auto version = ValidateTagsmatchersVersions("some");
	WaitSync("some1");
	ValidateTagsmatchersVersions("some1");
	SetSchema(masterId_, "some", kReplTestSchema1);
	WaitSync("some");
	ValidateTagsmatchersVersions("some", version);
	ValidateSchemas("some", kReplTestSchema1);
	ValidateSchemas("some1", kReplTestSchema2);
}

TEST_F(ReplicationLoadApi, WALResizeStaticData) {
	InitNs();

	const std::string nsName("some");
	auto master = GetSrv(masterId_)->api.reindexer;
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 1000, nsName));
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(0)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 4);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(2)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 2);
	}

	FillData(500);

	BaseApi::QueryResultsType qrLast100_1(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	BaseApi::QueryResultsType qrLast100_2(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	BaseApi::QueryResultsType qrLast100_3(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);

	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(0)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 504);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(504)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qrLast100_1);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_1.Count(), 101);
	}
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 100, nsName));
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(402)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qrLast100_2);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_2.Count(), 101);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(504)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 2000, nsName));
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(402)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qrLast100_3);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_3.Count(), 101);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(504)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}

	auto qrToSet = [](const BaseApi::QueryResultsType& qr) {
		std::unordered_set<std::string> items;
		reindexer::WrSerializer ser;
		for (auto& item : qr) {
			if (item.IsRaw()) {
				reindexer::WALRecord rec(item.GetRaw());
				EXPECT_EQ(rec.type, reindexer::WalReplState);
			} else {
				ser.Reset();
				auto err = item.GetCJSON(ser, false);
				EXPECT_TRUE(err.ok());
				items.emplace(ser.Slice());
			}
		}
		return items;
	};
	auto items_1 = qrToSet(qrLast100_1);
	auto items_2 = qrToSet(qrLast100_2);
	auto items_3 = qrToSet(qrLast100_3);
	ASSERT_EQ(items_1.size(), 100);
	ASSERT_TRUE(items_1 == items_2);
	ASSERT_TRUE(items_1 == items_3);
}

TEST_F(ReplicationLoadApi, WALResizeDynamicData) {
	InitNs();

	const std::string nsName("some");
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 1000, nsName));
	FillData(500);

	auto master = GetSrv(masterId_)->api.reindexer;
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 100, nsName));
	FillData(50);
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(452)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(453)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 101);
	}
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 200, nsName));
	FillData(500);
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(852)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(853)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 201);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(1053)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(master.get(), kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(1054)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
}

TEST_F(ReplicationLoadApi, DISABLED_BasicTestNoMasterRestart) {
	InitNs();
	stop = false;
	FillData(1000);

	std::thread destroyer([this]() {
		while (!stop) {
			RestartServer(rand() % 3 + 1);
			std::this_thread::sleep_for(std::chrono::seconds(3));
		}
	});

	for (size_t i = 0; i < 10; ++i) {
		if (i % 3 == 0) {
			DeleteFromMaster();
		}
		FillData(1000);
		SimpleSelect(0);
		SimpleSelect(rand() % 3 + 1);
	}

	stop = true;
	destroyer.join();
}

TEST_F(ReplicationLoadApi, SingleSlaveTest) {
	InitNs();
	stop = false;
	FillData(1000);

	std::thread writingThread([this]() {
		while (!stop) {
			FillData(1000);
		}
	});

	std::thread removingThread([this]() {
		size_t counter = 0;
		while (!stop) {
			std::this_thread::sleep_for(std::chrono::seconds(3));
			int i = rand() % 2;
			counter++;

			RestartServer(i);
			if (counter % 3 == 0) {
				DeleteFromMaster();
			}
		}
	});

	for (size_t i = 0; i < 2; ++i) {
		SimpleSelect(0);
		SetWALSize(masterId_, (int64_t(i) + 1) * 1000, "some1");
		SetWALSize(masterId_, (int64_t(i) + 1) * 1000, "some");
		std::this_thread::sleep_for(std::chrono::seconds(3));
	}

	stop = true;
	writingThread.join();
	removingThread.join();
	ForceSync();
	WaitSync("some");
	WaitSync("some1");
}

TEST_F(ReplicationLoadApi, DuplicatePKSlaveTest) {
	InitNs();
	const unsigned int kItemCount = 5;
	auto srv = GetSrv(masterId_);
	auto& api = srv->api;

	std::string changedIds;
	const unsigned int kChangedCount = 2;
	std::unordered_set<int> ids;
	for (unsigned i = 0; i < kChangedCount; ++i) {
		ids.insert(std::rand() % kItemCount);
	}

	bool isFirst = true;
	for (const auto id : ids) {
		if (!isFirst) {
			changedIds += ", ";
		}
		changedIds += std::to_string(id);
		isFirst = false;
	}

	std::unordered_map<int, std::pair<std::string, std::string>> items;
	Error err;
	for (size_t i = 0; i < kItemCount; ++i) {
		std::string jsonChange;
		BaseApi::ItemType item = api.NewItem("some");
		auto json = fmt::sprintf(R"json({"id":%d,"int":%d,"string":"%s","uuid":"%s"})json", i, i + 100, std::to_string(1 + 1000), nilUUID);
		err = item.FromJSON(json);
		api.Upsert("some", item);
		jsonChange = json;
		int idNew = i;
		if (ids.find(i) != ids.end()) {
			jsonChange = fmt::sprintf(R"json({"id":%d,"int":%d,"string":"%s","uuid":"%s"})json", kItemCount * 2 + i, i + 100,
									  std::to_string(1 + 1000), nilUUID);
			idNew = kItemCount * 2 + i;
		}
		items.emplace(idNew, std::make_pair(json, jsonChange));
	}

	WaitSync("some");
	{
		BaseApi::QueryResultsType qr(api.reindexer.get());
		err = api.reindexer->Select("Update some set id=id+" + std::to_string(kItemCount * 2) + " where id in(" + changedIds + ")", qr);
		ASSERT_TRUE(err.ok());
		WaitSync("some");
	}

	for (size_t k = 0; k < GetServersCount(); k++) {
		auto server = GetSrv(k);
		{
			BaseApi::QueryResultsType qr(api.reindexer.get());
			err = server->api.reindexer->Select("select * from some order by id", qr);
			ASSERT_TRUE(err.ok());
			ASSERT_EQ(qr.Count(), items.size());
			for (auto i : qr) {
				reindexer::WrSerializer ser;
				err = i.GetJSON(ser, false);
				gason::JsonParser parser;
				auto root = parser.Parse(ser.Slice());
				int id = root["id"].As<int>();
				ASSERT_TRUE(err.ok());
				ASSERT_EQ(ser.Slice(), items[id].second);
			}
		}
		{
			for (auto id : ids) {
				BaseApi::QueryResultsType qr(api.reindexer.get());
				err = server->api.reindexer->Select("select * from some where id=" + std::to_string(id), qr);
				ASSERT_TRUE(err.ok());
				ASSERT_EQ(qr.Count(), 0);
			}
		}
		{
			for (auto id : ids) {
				BaseApi::QueryResultsType qr(api.reindexer.get());
				err = server->api.reindexer->Select("select * from some where id=" + std::to_string(id + kItemCount * 2), qr);
				ASSERT_TRUE(err.ok());
				ASSERT_EQ(qr.Count(), 1);
			}
		}
	}
}

TEST_F(ReplicationLoadApi, ConfigSync) {
	ReplicationConfigTest config("slave", true, false, 0, "cproto://127.0.0.1:6534/0", "slave_1");
	const size_t kTestSlaveID = 2;
	RestartWithConfigFile(kTestSlaveID,
						  "role: slave\n"
						  "master_dsn: cproto://127.0.0.1:6534/0\n"
						  "app_name: slave_1\n"
						  "cluster_id: 2\n"
						  "force_sync_on_logic_error: true\n"
						  "force_sync_on_wrong_data_hash: false\n"
						  "namespaces: []");
	CheckSlaveConfigFile(kTestSlaveID, config);
	config = ReplicationConfigTest("slave", false, true, 0, "cproto://127.0.0.1:6534/12345", "slave_1", {"ns1", "ns2"});
	SetServerConfig(kTestSlaveID, config);
	CheckSlaveConfigFile(kTestSlaveID, config);
	config = ReplicationConfigTest("slave", true, false, 0, "cproto://127.0.0.1:6534/999", "slave_1");
	SetServerConfig(kTestSlaveID, config);
	CheckSlaveConfigFile(kTestSlaveID, config);
	std::this_thread::sleep_for(std::chrono::seconds(2));  // In case if OS doesn't have nanosecods in stat result

	GetSrv(kTestSlaveID)
		->WriteServerConfig(
			"role: slave\n"
			"master_dsn: cproto://127.0.0.1:6534/somensname\n"
			"app_name: slave_1\n"
			"cluster_id: 2\n"
			"force_sync_on_logic_error: false\n"
			"force_sync_on_wrong_data_hash: true\n"
			"namespaces:\n"
			"  - ns1\n"
			"  - ns3\n");
	config = ReplicationConfigTest("slave", false, true, 0, "cproto://127.0.0.1:6534/somensname", "slave_1", {"ns1", "ns3"});
	CheckSlaveConfigNamespace(kTestSlaveID, config, std::chrono::seconds(3));
}

#if !defined(REINDEX_WITH_TSAN)
TEST_F(ReplicationLoadApi, DynamicRoleSwitch) {
	InitNs();
	stop = false;

	// Create #config changing threads
	std::vector<std::thread> configUpdateThreads(GetServersCount());
	for (size_t i = 0; i < configUpdateThreads.size(); ++i) {
		configUpdateThreads[i] = std::thread(
			[this](size_t id) {
				while (!stop) {
					std::this_thread::sleep_for(std::chrono::milliseconds(100));
					size_t cnt = rand() % 5;
					SetOptmizationSortWorkers(id, cnt, "*");
				}
			},
			i);
	}

	// Switch master and await sync in each loop iteration
	for (size_t i = 1; i < 8; i++) {
		FillData(2000);
		WaitSync("some");
		WaitSync("some1");
		SwitchMaster(i % kDefaultServerCount, {"some", "some1"});
	}

	stop = true;
	for (auto& th : configUpdateThreads) {
		th.join();
	}
}
#endif
