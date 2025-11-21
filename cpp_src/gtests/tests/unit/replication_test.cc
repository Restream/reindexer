#include <unordered_map>
#include <unordered_set>
#include "cluster/stats/replicationstats.h"
#include "replication_load_api.h"
#include "wal/walrecord.h"

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
	// Check replication in multithread mode with data writes and server restarts
	std::atomic<bool> leaderWasRestarted = false;
	const std::string kNsSome = "some";
	const std::string kNsSome1 = "some1";
	InitNs();
	stop = false;
	SetWALSize(masterId_, 100000, kNsSome);
	WaitSync(kNsSome);
	WaitSync(kNsSome1);

	FillData(1000);

	std::thread destroyer([this, &leaderWasRestarted]() {
		int count = 0;
		while (!stop) {
			if (!(count % 30)) {
				auto restartId = rand() % kDefaultServerCount;
				RestartServer(restartId);
				if (restartId == masterId_) {
					leaderWasRestarted = true;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	});

	std::thread statsReader([this]() {
		while (!stop) {
			std::ignore = GetReplicationStats(masterId_);
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	});

	SetWALSize(masterId_, 50000, kNsSome);
	for (size_t i = 0; i < 2; ++i) {
		if (i % 3 == 0) {
			std::ignore = DeleteFromMaster();
		}
		SetWALSize(masterId_, (int64_t(i) + 1) * 25000, kNsSome1);
		FillData(1000);
		std::ignore = GetReplicationStats(masterId_);
		SetWALSize(masterId_, (int64_t(i) + 1) * 50000, kNsSome);
		std::ignore = SimpleSelect(0);
	}

	SetWALSize(masterId_, 50000, "some1");

	stop = true;
	destroyer.join();
	statsReader.join();

	ForceSync();
	WaitSync(kNsSome);
	WaitSync(kNsSome1);

	std::this_thread::sleep_for(std::chrono::seconds(1));  // Add some time for stats stabilization

	// Check final stats
	auto stats = GetReplicationStats(masterId_);
	EXPECT_EQ(stats.logLevel, LogTrace);
	// Validate force/wal syncs
	if (leaderWasRestarted) {
		EXPECT_GE(stats.forceSyncs.count + stats.walSyncs.count, 2 * (kDefaultServerCount - 1))
			<< "Force syncs: " << stats.forceSyncs.count << "; WAL syncs: " << stats.walSyncs.count;
	} else {
		EXPECT_GE(stats.walSyncs.count, kDefaultServerCount - 1);
		EXPECT_GT(stats.walSyncs.avgTimeUs, 0);
		EXPECT_GT(stats.walSyncs.maxTimeUs, 0);
	}
	if (stats.forceSyncs.count > 0) {
		EXPECT_GT(stats.forceSyncs.avgTimeUs, 0);
		EXPECT_GT(stats.forceSyncs.maxTimeUs, 0);
	} else {
		EXPECT_EQ(stats.forceSyncs.avgTimeUs, 0);
		EXPECT_EQ(stats.forceSyncs.maxTimeUs, 0);
	}
	if (stats.walSyncs.count > 0) {
		EXPECT_GT(stats.walSyncs.avgTimeUs, 0);
		EXPECT_GT(stats.walSyncs.maxTimeUs, 0);
	} else {
		EXPECT_EQ(stats.walSyncs.avgTimeUs, 0);
		EXPECT_EQ(stats.walSyncs.maxTimeUs, 0);
	}
	// Validate nodes/ns states
	auto replConf = GetSrv(masterId_)->GetServerConfig(ServerControl::ConfigType::Namespace);
	ASSERT_EQ(replConf.nodes.size(), stats.nodeStats.size());
	for (auto& nodeStat : stats.nodeStats) {
		using namespace reindexer::cluster;
		auto dsnIt = std::find_if(replConf.nodes.begin(), replConf.nodes.end(),
								  [&nodeStat](const AsyncReplicationConfigTest::Node& node) { return nodeStat.dsn == node.dsn; });
		ASSERT_NE(dsnIt, replConf.nodes.end()) << fmt::format("Unexpected dsn value: {}", nodeStat.dsn);
		ASSERT_EQ(nodeStat.status, NodeStats::Status::Online);
		ASSERT_EQ(nodeStat.syncState, NodeStats::SyncState::OnlineReplication);
		ASSERT_EQ(nodeStat.role, RaftInfo::Role::Follower);
		ASSERT_TRUE(nodeStat.namespaces.empty());
	}
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
			std::ignore = DeleteFromMaster();
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

#if !defined(REINDEX_WITH_TSAN)
TEST_F(ReplicationLoadApi, SingleSlaveTest) {
	// Check replication in multithread mode with data writes, delete queries and server restarts
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
				std::ignore = DeleteFromMaster();
			}
		}
	});

	for (size_t i = 0; i < 2; ++i) {
		std::ignore = SimpleSelect(0);
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
#endif

TEST_F(ReplicationLoadApi, WALResizeStaticData) {
	// Check WAL resizing with constant data part
	InitNs();

	const std::string nsName("some");
	auto master = GetSrv(masterId_)->api.reindexer;
	// Check new wal size with empty namespace
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 1000, nsName));
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(0)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 4);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(2)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 2);
	}

	// Add data, which do not exceed current wal size
	FillData(500);

	BaseApi::QueryResultsType qrLast100_1(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	BaseApi::QueryResultsType qrLast100_2(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	BaseApi::QueryResultsType qrLast100_3(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);

	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(0)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 504);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(504)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(404)), qrLast100_1);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_1.Count(), 100);
	}
	// Set wal size, which is less than current data count
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 100, nsName));
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(404)), qrLast100_2);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_2.Count(), 100);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(504)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	// Set wal size, which is larger than current data count
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 2000, nsName));
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(404)), qrLast100_3);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_3.Count(), 100);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
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
	// Validate, that there are some records, which were not changed after all the wal resizings
	auto items_1 = qrToSet(qrLast100_1);
	auto items_2 = qrToSet(qrLast100_2);
	auto items_3 = qrToSet(qrLast100_3);
	ASSERT_EQ(items_1.size(), 99);
	ASSERT_TRUE(items_1 == items_2);
	ASSERT_TRUE(items_1 == items_3);
}

TEST_F(ReplicationLoadApi, WALResizeDynamicData) {
	// Check WAL resizing in combination with data refilling
	InitNs();

	// Check case, when new wal size is larger, than actual records count, and records count does not exceed wal size after setting
	const std::string nsName("some");
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 1000, nsName));
	FillData(500);

	// Check case, when new wal size is less, than actual records count
	auto master = GetSrv(masterId_)->api.reindexer;
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 100, nsName));
	FillData(50);
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(453)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(454)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 100);
	}
	// Check case, when new wal size is larger, than actual records count, and records count exceeds wal size after setting
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 200, nsName));
	FillData(500);
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(853)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(854)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 200);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(1053)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(1054)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
}

TEST_F(ReplicationLoadApi, ConfigReadingOnStartup) {
	// Check if server reads config on startup
	const size_t kTestServerID = 0;

	auto srv = GetSrv(kTestServerID);
	const auto kReplFilePath = srv->GetReplicationConfigFilePath();
	const auto kAsyncReplFilePath = srv->GetAsyncReplicationConfigFilePath();
	srv.reset();
	StopServer(kTestServerID);
	WriteConfigFile(kAsyncReplFilePath,
					"role: none\n"
					"mode: default\n"
					"retry_sync_interval_msec: 3000\n"
					"syncs_per_thread: 2\n"
					"app_name: node_XXX\n"
					"force_sync_on_logic_error: true\n"
					"force_sync_on_wrong_data_hash: false\n"
					"online_updates_delay_msec: 200\n"
					"namespaces: []\n"
					"nodes: []");
	WriteConfigFile(kReplFilePath,
					"server_id: 4\n"
					"cluster_id: 2\n");
	StartServer(kTestServerID);
	AsyncReplicationConfigTest config("none", {}, true, false, 4, "node_XXX", {}, "default", 200);
	CheckReplicationConfigNamespace(kTestServerID, config);
}

TEST_F(ReplicationLoadApi, DuplicatePKFollowerTest) {
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
		auto json = fmt::format(R"json({{"id":{},"int":{},"string":"{}","uuid":"{}"}})json", i, i + 100, std::to_string(1 + 1000), nilUUID);
		err = item.FromJSON(json);
		api.Upsert("some", item);
		jsonChange = json;
		int idNew = i;
		if (ids.find(i) != ids.end()) {
			jsonChange = fmt::format(R"json({{"id":{},"int":{},"string":"{}","uuid":"{}"}})json", kItemCount * 2 + i, i + 100,
									 std::to_string(1 + 1000), nilUUID);
			idNew = kItemCount * 2 + i;
		}
		items.emplace(idNew, std::make_pair(json, jsonChange));
	}

	WaitSync("some");
	{
		std::ignore = api.ExecSQL("Update some set id=id+" + std::to_string(kItemCount * 2) + " where id in(" + changedIds + ")");
		WaitSync("some");
	}

	for (size_t k = 0; k < GetServersCount(); k++) {
		auto server = GetSrv(k);
		{
			auto qr = server->api.ExecSQL("select * from some order by id");
			ASSERT_EQ(qr.Count(), items.size());
			for (auto i : qr) {
				reindexer::WrSerializer ser;
				err = i.GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				gason::JsonParser parser;
				auto root = parser.Parse(ser.Slice());
				int id = root["id"].As<int>();
				ASSERT_EQ(ser.Slice(), items[id].second);
			}
		}
		{
			for (auto id : ids) {
				auto qr = server->api.ExecSQL("select * from some where id=" + std::to_string(id));
				ASSERT_EQ(qr.Count(), 0);
			}
		}
		{
			for (auto id : ids) {
				auto qr = server->api.ExecSQL("select * from some where id=" + std::to_string(id + kItemCount * 2));
				ASSERT_EQ(qr.Count(), 1);
			}
		}
	}
}

TEST_F(ReplicationLoadApi, ConfigSync) {
	// Check automatic replication config file and #config namespace sync
	using ReplNode = AsyncReplicationConfigTest::Node;
	const size_t kTestServerID = 0;

	SCOPED_TRACE("Set replication config via file");
	RestartWithReplicationConfigFiles(kTestServerID,
									  "role: none\n"
									  "retry_sync_interval_msec: 3000\n"
									  "syncs_per_thread: 2\n"
									  "app_name: node_1\n"
									  "force_sync_on_logic_error: true\n"
									  "force_sync_on_wrong_data_hash: false\n"
									  "namespaces: []\n"
									  "nodes: []",
									  "server_id: 3\n"
									  "cluster_id: 2\n");
	// Validate config file
	AsyncReplicationConfigTest config("none", {}, true, false, 3, "node_1", {}, "default");
	CheckReplicationConfigNamespace(kTestServerID, config);

	config =
		AsyncReplicationConfigTest("leader", {ReplNode{DSN("cproto://127.0.0.1:53019/db")}, ReplNode{DSN("cproto://127.0.0.1:53020/db")}},
								   false, true, 3, "node_1", {"ns1", "ns2"}, "default");
	SCOPED_TRACE("Set replication config(two nodes) via namespace");
	SetServerConfig(kTestServerID, config);
	// Validate #config namespace
	CheckReplicationConfigFile(kTestServerID, config);

	config =
		AsyncReplicationConfigTest("leader", {ReplNode{DSN("cproto://127.0.0.1:45000/db")}}, false, true, 3, "node_xxx", {}, "default");
	SCOPED_TRACE("Set replication config(one node) via namespace");
	SetServerConfig(kTestServerID, config);
	// Validate replication.conf file
	CheckReplicationConfigFile(kTestServerID, config);

	config = AsyncReplicationConfigTest("leader", {ReplNode{DSN("cproto://127.0.0.1:45000/db"), {{"ns1", "ns2"}}}}, false, true, 3,
										"node_xxx", {}, "default", 150);
	SCOPED_TRACE("Set replication config with custom ns list for existing node via namespace");
	SetServerConfig(kTestServerID, config);
	// Validate replication.conf file
	CheckReplicationConfigFile(kTestServerID, config);
	std::this_thread::sleep_for(std::chrono::seconds(2));  // In case if OS doesn't have nanosecods in stat result

	SCOPED_TRACE("Set replication config via file");
	GetSrv(kTestServerID)
		->WriteAsyncReplicationConfig(
			"role: leader\n"
			"retry_sync_interval_msec: 3000\n"
			"syncs_per_thread: 2\n"
			"app_name: node_1\n"
			"force_sync_on_logic_error: false\n"
			"force_sync_on_wrong_data_hash: true\n"
			"online_updates_delay_msec: 50\n"
			"namespaces:\n"
			"  - ns1\n"
			"  - ns3\n"
			"nodes:\n"
			"  -\n"
			"    dsn: cproto://127.0.0.1:53001/db1\n"
			"    namespaces:\n"
			"      - ns4\n"
			"  -\n"
			"    dsn: cproto://127.0.0.1:53002/db2\n");
	config = AsyncReplicationConfigTest(
		"leader", {ReplNode{DSN("cproto://127.0.0.1:53001/db1"), {{"ns4"}}}, ReplNode{DSN("cproto://127.0.0.1:53002/db2")}}, false, true, 3,
		"node_1", {"ns1", "ns3"}, "default", 50);
	// Validate #config namespace
	CheckReplicationConfigNamespace(kTestServerID, config, std::chrono::seconds(3));

	SCOPED_TRACE("Check server id switch");
	GetSrv(kTestServerID)
		->WriteReplicationConfig(
			"server_id: 2\n"
			"cluster_id: 2\n");
	config.serverId = 2;
	// Validate #config namespace
	CheckReplicationConfigNamespace(kTestServerID, config, std::chrono::seconds(3));
}

#if !defined(REINDEX_WITH_TSAN)
TEST_F(ReplicationLoadApi, DynamicRoleSwitch) {
	// Validate replication behavior after node's role switch
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
	const size_t kPortionSize = 2000;
	size_t expectedLsnCounter = 3;
	for (size_t i = 1; i < 8; i++) {
		FillData(kPortionSize);
		expectedLsnCounter += kPortionSize;
		WaitSync("some", reindexer::lsn_t(expectedLsnCounter, masterId_));
		WaitSync("some1", reindexer::lsn_t(expectedLsnCounter, masterId_));
		SwitchMaster(i % kDefaultServerCount, {"some", "some1"}, (i % 2 == 0) ? "default" : "from_sync_leader");
	}

	stop = true;
	for (auto& th : configUpdateThreads) {
		th.join();
	}
}
#endif

TEST_F(ReplicationLoadApi, NodeOfflineLastError) {
	InitNs();

	auto leader = GetSrv(0);
	StopServer(1);
	for (std::size_t i = 0; i < 10; i++) {
		auto stats = leader->GetReplicationStats(reindexer::cluster::kAsyncReplStatsType);
		if (!stats.nodeStats.empty() && stats.nodeStats[0].lastError.code() == errNetwork) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	auto stats = leader->GetReplicationStats(reindexer::cluster::kAsyncReplStatsType);
	ASSERT_EQ(stats.nodeStats.size(), std::size_t(3));
	ASSERT_EQ(stats.nodeStats[0].lastError.code(), errNetwork);
	ASSERT_FALSE(stats.nodeStats[0].lastError.whatStr().empty());
}

TEST_F(ReplicationLoadApi, LogLevel) {
	// Check async replication log level setup
	InitNs();

	std::atomic<bool> stop = {false};
	std::thread th([this, &stop] {
		// Simple insertion thread for race detection
		while (!stop) {
			FillData(1);
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	});

	// Replication in tests must be started with 'Trace' log level
	auto stats = GetReplicationStats(masterId_);
	EXPECT_EQ(stats.logLevel, LogTrace);

	// Changing log level
	const LogLevel levels[] = {LogInfo, LogTrace, LogWarning, LogError, LogNone, LogInfo};
	for (auto level : levels) {
		SetReplicationLogLevel(masterId_, LogLevel(level));
		stats = GetReplicationStats(masterId_);
		EXPECT_EQ(stats.logLevel, LogLevel(level));
	}

	// Checking log level after replication restart. It should be reset to 'Trace'
	ForceSync();
	stats = GetReplicationStats(masterId_);
	EXPECT_EQ(stats.logLevel, LogTrace);

	stop = true;
	th.join();
}

#ifndef REINDEX_WITH_TSAN
// TSAN reports lock order inversion here, but it's fine, because of the top level locks.
// Skip the test for TSAN to avoid global suppression for this case.
TEST_F(ReplicationLoadApi, FollowerNamespaceOperations) {
	// Check available namespace operations on the async follower
	constexpr std::string_view kNsSome = "some";
	constexpr std::string_view kNsSome2 = "some2";
	constexpr std::string_view kNsSome3 = "some3";
	InitNs();
	WaitSync(kNsSome);

	auto follower = GetSrv(1)->api.reindexer;
	// Replicated namespace drop is not allowed
	auto err = follower->DropNamespace(kNsSome);
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();
	// Replicated namespace open is allowed
	err = follower->OpenNamespace(kNsSome);
	ASSERT_TRUE(err.ok()) << err.what();
	// Own namespace open is allowed
	err = follower->OpenNamespace(kNsSome2);
	ASSERT_TRUE(err.ok()) << err.what();
	// Rename of the replicated namespace is not allowed
	err = follower->RenameNamespace(kNsSome, std::string(kNsSome2));
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();
	// Rename to the replicated namespace is not allowed
	err = follower->RenameNamespace(kNsSome2, std::string(kNsSome));
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();
	// Rename of the own namespace is allowed
	err = follower->RenameNamespace(kNsSome2, std::string(kNsSome3));
	ASSERT_TRUE(err.ok()) << err.what();
	// Own namespace drop is allowed
	err = follower->DropNamespace(kNsSome3);
	ASSERT_TRUE(err.ok()) << err.what();
	// Replicated namespace drop is allowed after role reset
	err = GetSrv(1)->TryResetReplicationRole(kNsSome);
	ASSERT_TRUE(err.ok()) << err.what();
	err = follower->DropNamespace(kNsSome);
	ASSERT_TRUE(err.ok()) << err.what();
}
#endif	// REINDEX_WITH_TSAN
