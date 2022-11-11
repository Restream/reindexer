#include <unordered_map>
#include <unordered_set>
#include "cluster/stats/replicationstats.h"
#include "replication_load_api.h"
#include "wal/walrecord.h"

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
			GetReplicationStats(masterId_, cluster::kAsyncReplStatsType);
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	});

	SetWALSize(masterId_, 50000, kNsSome);
	for (size_t i = 0; i < 2; ++i) {
		if (i % 3 == 0) DeleteFromMaster();
		SetWALSize(masterId_, (int64_t(i) + 1) * 25000, kNsSome1);
		FillData(1000);
		GetReplicationStats(masterId_, cluster::kAsyncReplStatsType);
		SetWALSize(masterId_, (int64_t(i) + 1) * 50000, kNsSome);
		SimpleSelect(0);
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
	auto stats = GetReplicationStats(masterId_, cluster::kAsyncReplStatsType);
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
	ASSERT_EQ(replConf.nodes_.size(), stats.nodeStats.size());
	for (auto& nodeStat : stats.nodeStats) {
		auto dsnIt = std::find_if(replConf.nodes_.begin(), replConf.nodes_.end(),
								  [&nodeStat](const AsyncReplicationConfigTest::Node& node) { return nodeStat.dsn == node.dsn; });
		ASSERT_NE(dsnIt, replConf.nodes_.end()) << "Unexpected dsn value: " << nodeStat.dsn;
		ASSERT_EQ(nodeStat.status, cluster::NodeStats::Status::Online);
		ASSERT_EQ(nodeStat.syncState, cluster::NodeStats::SyncState::OnlineReplication);
		ASSERT_EQ(nodeStat.role, cluster::RaftInfo::Role::Follower);
		ASSERT_TRUE(nodeStat.namespaces.empty());
	}
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
			if (counter % 3 == 0) DeleteFromMaster();
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
		EXPECT_EQ(qr.Count(), 3);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(2)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
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
		EXPECT_EQ(qr.Count(), 503);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(502)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qrLast100_1);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_1.Count(), 100);
	}
	// Set wal size, which is less than current data count
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 100, nsName));
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(402)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qrLast100_2);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_2.Count(), 100);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(502)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	// Set wal size, which is larger than current data count
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 2000, nsName));
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(402)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(403)), qrLast100_3);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qrLast100_3.Count(), 100);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(502)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(503)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}

	auto qrToSet = [](const BaseApi::QueryResultsType& qr) {
		std::unordered_set<std::string> items;
		WrSerializer ser;
		for (auto& item : qr) {
			if (item.IsRaw()) {
				reindexer::WALRecord rec(item.GetRaw());
				EXPECT_EQ(rec.type, WalReplState);
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
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(452)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(453)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 100);
	}
	// Check case, when new wal size is larger, than actual records count, and records count exceeds wal size after setting
	ASSERT_NO_FATAL_FAILURE(SetWALSize(masterId_, 200, nsName));
	FillData(500);
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(852)), qr);
		EXPECT_EQ(err.code(), errOutdatedWAL) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(853)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 200);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(1052)), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}
	{
		BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		Error err = master->Select(Query(nsName).Where("#lsn", CondGt, int64_t(1053)), qr);
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
					"namespaces: []\n"
					"nodes: []");
	WriteConfigFile(kReplFilePath,
					"server_id: 4\n"
					"cluster_id: 2\n");
	StartServer(kTestServerID);
	AsyncReplicationConfigTest config("none", {}, true, false, 4, "node_XXX", {}, "default");
	CheckReplicationConfigNamespace(kTestServerID, config);
}

TEST_F(ReplicationLoadApi, ConfigSync) {
	// Check automatic replication config file and #config namespace sync
	using ReplNode = AsyncReplicationConfigTest::Node;
	const size_t kTestServerID = 0;

	// Set replication config via file
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
	config = AsyncReplicationConfigTest("leader", {ReplNode{"cproto://127.0.0.1:53019/db"}, ReplNode{"cproto://127.0.0.1:53020/db"}}, false,
										true, 3, "node_1", {"ns1", "ns2"}, "default");
	// Set replication config via namespace
	SetServerConfig(kTestServerID, config);
	// Validate #config namespace
	CheckReplicationConfigFile(kTestServerID, config);
	config = AsyncReplicationConfigTest("leader", {ReplNode{"cproto://127.0.0.1:45000/db"}}, false, true, 3, "node_xxx", {}, "default");
	// Set replication config via namespace
	SetServerConfig(kTestServerID, config);
	// Validate replication.conf file
	CheckReplicationConfigFile(kTestServerID, config);

	config = AsyncReplicationConfigTest("leader", {ReplNode{"cproto://127.0.0.1:45000/db", {{"ns1", "ns2"}}}}, false, true, 3, "node_xxx",
										{}, "default");
	// Set replication config with custom ns list for existing node via namespace
	SetServerConfig(kTestServerID, config);
	// Validate replication.conf file
	CheckReplicationConfigFile(kTestServerID, config);
	std::this_thread::sleep_for(std::chrono::seconds(2));  // In case if OS doesn't have nanosecods in stat result

	// Set replication config via file
	GetSrv(kTestServerID)
		->WriteAsyncReplicationConfig(
			"role: leader\n"
			"retry_sync_interval_msec: 3000\n"
			"syncs_per_thread: 2\n"
			"app_name: node_1\n"
			"force_sync_on_logic_error: false\n"
			"force_sync_on_wrong_data_hash: true\n"
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
	config = AsyncReplicationConfigTest("leader",
										{ReplNode{"cproto://127.0.0.1:53001/db1", {{"ns4"}}}, ReplNode{"cproto://127.0.0.1:53002/db2"}},
										false, true, 3, "node_1", {"ns1", "ns3"}, "default");
	// Validate #config namespace
	CheckReplicationConfigNamespace(kTestServerID, config, std::chrono::seconds(3));

	// Check server id switch
	GetSrv(kTestServerID)
		->WriteReplicationConfig(
			"server_id: 2\n"
			"cluster_id: 2\n");
	config.serverId_ = 2;
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
	size_t expectedLsnCounter = 2;
	for (size_t i = 1; i < 8; i++) {
		FillData(kPortionSize);
		expectedLsnCounter += kPortionSize;
		WaitSync("some", lsn_t(expectedLsnCounter, masterId_));
		WaitSync("some1", lsn_t(expectedLsnCounter, masterId_));
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

	ServerControl::Interface::Ptr leader = GetSrv(0);

	StopServer(1);
	for (std::size_t i = 0; i < 10; i++) {
		reindexer::cluster::ReplicationStats stats = leader->GetReplicationStats("async");
		if (!stats.nodeStats.empty() && stats.nodeStats[0].lastError.code() == errNetwork) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	reindexer::cluster::ReplicationStats stats = leader->GetReplicationStats("async");
	ASSERT_EQ(stats.nodeStats.size(), std::size_t(3));
	ASSERT_EQ(stats.nodeStats[0].lastError.code(), errNetwork);
	ASSERT_FALSE(stats.nodeStats[0].lastError.what().empty());
}
