#include <gtest/gtest.h>
#include "core/defnsconfigs.h"
#include "core/formatters/lsn_fmt.h"
#include "servercontrol.h"

using namespace reindexer;

class ReplicationSlaveSlaveApi : public ::testing::Test {
protected:
	void SetUp() override { fs::RmDirAll(kBaseTestsetDbPath); }

	void TearDown() override {}

public:
	class ServerControlVec : public std::vector<ServerControl> {
	public:
		~ServerControlVec() {
			for (auto& node : *this) {
				node.Stop();
			}
		}
	};

	void WaitSync(ServerControl& s1, ServerControl& s2, std::string_view nsName) {
		auto now = std::chrono::milliseconds(0);
		const auto pause = std::chrono::milliseconds(100);
		ReplicationTestState state1, state2;
		while (true) {
			now += pause;
			ASSERT_TRUE(now < kMaxSyncTime) << fmt::format(
				"Wait sync is too long. s1 lsn: {}; s2 lsn: {}; s1 count: {}; s2 count: {}; s1 hash: {}; s2 hash: {}", state1.lsn,
				state2.lsn, state1.dataCount, state2.dataCount, state1.dataHash, state2.dataHash);
			state1 = s1.Get()->GetState(nsName);
			state2 = s2.Get()->GetState(nsName);
			if (state1.lsn == state2.lsn && state1.dataCount == state2.dataCount && state1.dataHash == state2.dataHash) {
				return;
			}
			std::this_thread::sleep_for(pause);
		}
	}

	void CreateConfiguration(ServerControlVec& nodes, const std::vector<int>& slaveConfiguration, int basePort, int baseServerId,
							 const std::string& dbPathMaster) {
		for (size_t i = 0; i < slaveConfiguration.size(); i++) {
			nodes.push_back(ServerControl());
			nodes.back().InitServer(i, basePort + i, basePort + 1000 + i, dbPathMaster + std::to_string(i), "db", true);
			if (i == 0) {
				ReplicationConfigTest config("master");
				nodes.back().Get()->MakeMaster(config);
			} else {
				std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(slaveConfiguration[i]) + "/db";
				ReplicationConfigTest config("slave", false, true, baseServerId + i, masterDsn);
				nodes.back().Get()->MakeSlave(config);
			}
		}
	}

	void RestartServer(size_t id, ServerControlVec& nodes, int port, const std::string& dbPathMaster) {
		assertrx(id < nodes.size());
		if (nodes[id].Get()) {
			nodes[id].Stop();
			nodes[id].Drop();
			size_t counter = 0;
			while (nodes[id].IsRunning()) {
				counter++;
				// we have only 10sec timeout to restart server!!!!
				EXPECT_TRUE(counter < 1000);
				assertrx(counter < 1000);

				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}
		nodes[id].InitServer(id, port + id, port + 1000 + id, dbPathMaster + std::to_string(id), "db", true);
	}

	void ApplyConfig(ServerControl& sc, std::string_view json) {
		auto& rx = *sc.Get()->api.reindexer;
		auto item = rx.NewItem(kConfigNamespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.Upsert(kConfigNamespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void CheckTxCopyEventsCount(ServerControl& sc, int expectedCount) {
		auto& rx = *sc.Get()->api.reindexer;
		client::SyncCoroQueryResults qr(&rx);
		auto err = rx.Select(Query(kPerfStatsNamespace), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 1);
		WrSerializer ser;
		err = qr.begin().GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		auto resJS = parser.Parse(ser.Slice());
		ASSERT_EQ(resJS["transactions"]["total_copy_count"].As<int>(-1), expectedCount) << ser.Slice();
	}

protected:
	const std::string kBaseTestsetDbPath = fs::JoinPath(fs::GetTempDir(), "rx_test/ReplicationSlaveSlaveApi");

private:
	const std::chrono::seconds kMaxSyncTime = std::chrono::seconds(15);
};

class TestNamespace1 {
public:
	TestNamespace1(ServerControl& masterControl, const std::string& nsName = std::string("ns1")) : nsName_(nsName) {
		auto master = masterControl.Get();
		auto opt = StorageOpts().Enabled(true);
		Error err = master->api.reindexer->OpenNamespace(nsName_, opt);
		EXPECT_TRUE(err.ok()) << err.what();
		master->api.DefineNamespaceDataset(nsName_, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
	}

	void AddRows(ServerControl& masterControl, int from, unsigned int count, size_t dataLen = 0) {
		auto master = masterControl.Get();
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = master->api.NewItem(nsName_);
			auto err = item.FromJSON("{\"id\":" + std::to_string(from + i) +
									 (dataLen ? (",\"data\":\"" + reindexer::randStringAlph(dataLen) + "\"") : "") + "}");
			ASSERT_TRUE(err.ok()) << err.what();
			master->api.Upsert(nsName_, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	void AddRowsTx(ServerControl& masterControl, int from, unsigned int count, size_t dataLen = 8) {
		auto& rx = *masterControl.Get()->api.reindexer;
		reindexer::client::SyncCoroTransaction tr = rx.NewTransaction(nsName_);
		ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = tr.NewItem();
			auto err = item.FromJSON("{\"id\":" + std::to_string(from + i) +
									 (dataLen ? (",\"data\":\"" + reindexer::randStringAlph(dataLen) + "\"") : "") + "}");
			ASSERT_TRUE(err.ok()) << err.what();
			err = tr.Upsert(std::move(item));
			ASSERT_TRUE(err.ok()) << err.what();
		}
		auto err = rx.CommitTransaction(tr);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	std::function<void(ServerControl&, int, unsigned int, std::string)> AddRow1msSleep = [](ServerControl& masterControl, int from,
																							unsigned int count, std::string_view nsName) {
		auto master = masterControl.Get();
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = master->api.NewItem("ns1");
			auto err = item.FromJSON("{\"id\":" + std::to_string(i + from) + "}");
			ASSERT_TRUE(err.ok()) << err.what();
			master->api.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
			// std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	};

	void GetData(ServerControl& node, std::vector<int>& ids) {
		Query qr = Query(nsName_).Sort("id", false);
		BaseApi::QueryResultsType res(node.Get()->api.reindexer.get());
		auto err = node.Get()->api.reindexer->Select(qr, res);
		ASSERT_TRUE(err.ok()) << err.what();
		for (auto it : res) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(ser.Slice());
			ids.push_back(root["id"].As<int>());
		}
	}
	void GetDataWithStrings(ServerControl& node, std::map<int64_t, std::string>& ids) {
		Query qr = Query(nsName_).Sort("id", false);
		BaseApi::QueryResultsType res(node.Get()->api.reindexer.get());
		auto err = node.Get()->api.reindexer->Select(qr, res);
		ASSERT_TRUE(err.ok()) << err.what();
		for (auto it : res) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(ser.Slice());
			int id = root["id"].As<int>();
			std::string s = root["data"].As<std::string>();
			ids[id] = s;
		}
	}

	const std::string nsName_;
};

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveSyncByWalAddRow) {
	// Check WAL synchronization on a single row
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveSyncByWalAddRow"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int port = 9999;

	std::vector<int> slaveConfiguration = {-1, port};
	ServerControlVec nodes;
	CreateConfiguration(nodes, slaveConfiguration, port, 10, kDbPathMaster);

	TestNamespace1 ns1(nodes[0]);

	nodes[0].Get()->SetWALSize(100000, "ns1");
	int n1 = 1000;
	ns1.AddRows(nodes[0], 0, n1);

	WaitSync(nodes[0], nodes[1], "ns1");
	// Shutdown slave
	if (nodes[1].Get()) {
		nodes[1].Drop();
		size_t counter = 0;
		while (nodes[1].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assertrx(counter < 1000);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}

	const int startId = 10000;
	const unsigned int n2 = 2000;
	ServerControl& master = nodes[0];
	auto ThreadAdd = [&master, &ns1]() {
		master.Get()->SetWALSize(50000, "ns1");
		ns1.AddRow1msSleep(master, startId, n2, ns1.nsName_);
	};

	std::thread insertThread(ThreadAdd);

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	nodes[1].InitServer(1, port + 1, port + 1000 + 1, kDbPathMaster + std::to_string(1), "db", true);

	insertThread.join();

	WaitSync(nodes[0], nodes[1], "ns1");

	std::vector<int> ids0;
	ns1.GetData(nodes[0], ids0);
	std::vector<int> ids1;
	ns1.GetData(nodes[1], ids1);

	EXPECT_TRUE(ids1.size() == (n1 + n2));
	EXPECT_TRUE(ids0 == ids1);
}

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveStart) {
	// Check WAL/force sync on multiple rows
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveStart"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int port = 9999;

	std::vector<int> slaveConfiguration = {-1, port};
	ServerControlVec nodes;
	CreateConfiguration(nodes, slaveConfiguration, port, 10, kDbPathMaster);

	// Insert 100 rows
	std::string nsName("ns1");
	TestNamespace1 ns1(nodes[0], nsName);

	unsigned int n1 = 100;
	ns1.AddRows(nodes[0], 0, n1);
	nodes[0].Get()->SetWALSize(1000, "ns1");

	WaitSync(nodes[0], nodes[1], nsName);
	// restart Slave
	RestartServer(1, nodes, port, kDbPathMaster);
	WaitSync(nodes[0], nodes[1], nsName);

	// shutdown slave
	if (nodes[1].Get()) {
		nodes[1].Drop();
		size_t counter = 0;
		while (nodes[1].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assertrx(counter < 1000);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	// insert another 100 rows (200 total)
	ns1.AddRows(nodes[0], n1 + 1, n1);

	// run slave
	nodes[1].InitServer(1, port + 1, port + 1000 + 1, kDbPathMaster + std::to_string(1), "db", true);
	WaitSync(nodes[0], nodes[1], nsName);

	std::vector<int> ids0;
	ns1.GetData(nodes[0], ids0);
	std::vector<int> ids1;
	ns1.GetData(nodes[1], ids1);

	EXPECT_TRUE(ids1.size() == (n1 + n1));
	EXPECT_TRUE(ids0 == ids1);
}

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveSlave2) {
	// Check WAL/force sync on cascade setups
	auto SimpleTest = [this](int port, const std::vector<int>& slaveConfiguration) {
		const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveSlave2"));
		const std::string kDbPathMaster(kBaseDbPath + "/test_");
		int serverId = 5;
		ServerControlVec nodes;

		CreateConfiguration(nodes, slaveConfiguration, port, serverId, kDbPathMaster);

		ServerControl& master = nodes[0];
		TestNamespace1 ns1(master);

		const int count = 1000;
		ns1.AddRows(master, 0, count);

		for (size_t i = 1; i < nodes.size(); i++) {
			WaitSync(nodes[0], nodes[i], "ns1");
		}

		std::vector<std::vector<int>> results;

		Query qr = Query("ns1").Sort("id", true);
		for (size_t i = 0; i < nodes.size(); i++) {
			results.push_back(std::vector<int>());
			ns1.GetData(nodes[i], results.back());
		}

		for (size_t i = 1; i < results.size(); ++i) {
			EXPECT_TRUE((results[0] == results[i]));
		}
	};

	const int port = 9999;
	{
		/*
				m
				|
				1
				|
				2
		*/
		std::vector<int> slaveConfiguration = {-1, port, port + 1};
		SimpleTest(port, slaveConfiguration);
	}
	{
		/*
				m
			   / \
			  1   2
			  |   | \
			  3   4  5
		*/

		std::vector<int> slaveConfiguration = {-1, port, port, port + 1, port + 2, port + 2};
		SimpleTest(port, slaveConfiguration);
	}
}

//--

//--
#if !defined(REINDEX_WITH_TSAN)

TEST_F(ReplicationSlaveSlaveApi, MasterSlaveSlaveReload) {
	// Check synchronization continous nodes' restarting
	const int port = 9999;
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveSlaveReload"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int serverId = 5;
	ServerControlVec nodes;
	std::atomic_bool stopRestartServerThread(false);

	/*
			m
		   / \
		  1   2
		  |   | \
		  3   4  5
	*/
	std::vector<int> slaveConfiguration = {-1, port, port, port + 1, port + 2, port + 2};

	CreateConfiguration(nodes, slaveConfiguration, port, serverId, kDbPathMaster);

	ServerControl& master = nodes[0];
	TestNamespace1 ns1(master);

	const int startId = 1000;
	const int n2 = 20000;
	auto AddThread = [&master, &ns1]() { ns1.AddRow1msSleep(master, startId, n2, ns1.nsName_); };

	auto restartServer = [this, &nodes, &kDbPathMaster, &stopRestartServerThread]() {
		while (!stopRestartServerThread) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			int N = rand() % 3;
			RestartServer(N + 1, nodes, port, kDbPathMaster);
		}
	};

	std::thread insertThread(AddThread);
	std::thread restartThread(restartServer);

	insertThread.join();
	stopRestartServerThread = true;
	restartThread.join();
	//---------------------------

	for (size_t i = 1; i < nodes.size(); i++) {
		WaitSync(nodes[0], nodes[i], "ns1");
	}

	std::vector<std::vector<int>> results;

	Query qr = Query("ns1").Sort("id", true);

	for (size_t i = 0; i < nodes.size(); i++) {
		results.push_back(std::vector<int>());
		ns1.GetData(nodes[i], results.back());
	}

	for (size_t i = 1; i < results.size(); ++i) {
		EXPECT_TRUE((results[0] == results[i])) << i << "; size[0]: " << results[0].size() << "; size[i]: " << results[i].size();
	}
}

#endif

TEST_F(ReplicationSlaveSlaveApi, TransactionTest) {
	// Check transactions replication for cascade setup
	/*
			m
			|
			1
			|
			2
			|
			3
			|
			4
	*/
	const int port = 9999;
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "TransactionTest"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int serverId = 5;

	std::vector<int> slaveConfiguration = {-1, port, port + 1, port + 2, port + 3};

	ServerControlVec nodes;

	CreateConfiguration(nodes, slaveConfiguration, port, serverId, kDbPathMaster);

	size_t kRows = 100;
	std::string nsName("ns1");

	ServerControl& master = nodes[0];
	TestNamespace1 ns1(master, nsName);

	ns1.AddRows(master, 0, kRows);

	for (size_t i = 1; i < nodes.size(); i++) {
		WaitSync(nodes[0], nodes[i], nsName);
	}

	ns1.AddRowsTx(master, 0, kRows);

	for (size_t i = 1; i < nodes.size(); i++) {
		WaitSync(nodes[0], nodes[i], nsName);
	}

	std::vector<std::vector<int>> results;
	for (size_t i = 0; i < nodes.size(); i++) {
		results.push_back(std::vector<int>());
		ns1.GetData(nodes[i], results.back());
	}

	for (size_t i = 1; i < results.size(); ++i) {
		EXPECT_TRUE((results[0] == results[i]));
	}
}

TEST_F(ReplicationSlaveSlaveApi, TransactionCopyPolicyForceSync) {
	// Check transactions copy policy after force sync
	/*
			m
			|
			1
			|
			2
	*/
	constexpr std::string_view kJsonCfgNss = R"=({
		"namespaces": [
		{
			"namespace": "*",
			"start_copy_policy_tx_size": 10000,
			"copy_policy_multiplier": 5,
			"tx_size_to_always_copy": 100000
		},
		{
			"namespace": "ns1",
			"start_copy_policy_tx_size": 10000,
			"copy_policy_multiplier": 5,
			"tx_size_to_always_copy": 1
		}
		],
		"type": "namespaces"
	})=";
	constexpr int port = 9999;
	const std::string kDbPathMaster(fs::JoinPath(fs::JoinPath(kBaseTestsetDbPath, "TransactionCopyPolicyForceSync"), "test_"));
	constexpr int serverId = 5;
	constexpr size_t kRows = 100;
	const std::string nsName("ns1");

	std::vector<int> slaveConfiguration = {-1, port, port + 1};
	ServerControlVec nodes;
	for (size_t i = 0; i < slaveConfiguration.size(); i++) {
		nodes.emplace_back().InitServer(i, port + i, port + 1000 + i, kDbPathMaster + std::to_string(i), "db", true);
		nodes.back().Get()->EnableAllProfilings();
	}

	// Set tx copy policy for the node '2' to 'always copy'
	ApplyConfig(nodes[2], kJsonCfgNss);

	for (size_t i = 0; i < slaveConfiguration.size(); i++) {
		if (i == 0) {
			ReplicationConfigTest config("master");
			nodes[i].Get()->MakeMaster(config);
		} else {
			std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(slaveConfiguration[i]) + "/db";
			ReplicationConfigTest config("slave", false, true, serverId + i, masterDsn);
			nodes[i].Get()->MakeSlave(config);
		}
	}
	nodes[2].Drop();

	ServerControl& master = nodes[0];
	TestNamespace1 ns1(master, nsName);
	WaitSync(nodes[0], nodes[1], nsName);

	// Restart node '2'
	nodes[2].InitServer(2, port + 2, port + 1000 + 2, kDbPathMaster + std::to_string(2), "db", true);
	std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(slaveConfiguration[2]) + "/db";
	ReplicationConfigTest config("slave", false, true, serverId + 2, masterDsn);
	nodes[2].Get()->MakeSlave(config);
	WaitSync(nodes[0], nodes[2], nsName);

	// Check copy tx event in the perfstats before tx
	CheckTxCopyEventsCount(nodes[2], 0);

	// Apply tx
	ns1.AddRowsTx(master, 0, kRows);
	WaitSync(nodes[0], nodes[2], nsName);

	// Check copy tx event in the perfstats after tx
	CheckTxCopyEventsCount(nodes[2], 1);
}

TEST_F(ReplicationSlaveSlaveApi, TransactionCopyPolicyWalSync) {
	// Check transactions copy policy during the wal sync
	/*
			m
			|
			1
	*/
	constexpr std::string_view kJsonCfgNss = R"=({
		"namespaces": [
		{
			"namespace": "*",
			"start_copy_policy_tx_size": 10000,
			"copy_policy_multiplier": 5,
			"tx_size_to_always_copy": 100000
		},
		{
			"namespace": "ns1",
			"start_copy_policy_tx_size": 10000,
			"copy_policy_multiplier": 5,
			"tx_size_to_always_copy": 1
		}
		],
		"type": "namespaces"
	})=";
	constexpr int port = 9999;
	const std::string kDbPathMaster(fs::JoinPath(fs::JoinPath(kBaseTestsetDbPath, "TransactionCopyPolicyWalSync"), "test_"));
	constexpr int serverId = 5;
	constexpr size_t kRows = 100;
	const std::string nsName("ns1");

	std::vector<int> slaveConfiguration = {-1, port};
	ServerControlVec nodes;
	for (size_t i = 0; i < slaveConfiguration.size(); i++) {
		nodes.emplace_back().InitServer(i, port + i, port + 1000 + i, kDbPathMaster + std::to_string(i), "db", true);
		nodes.back().Get()->EnableAllProfilings();
	}
	const std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(slaveConfiguration[1]) + "/db";

	// Set tx copy policy for the node '1' to 'always copy'
	ApplyConfig(nodes[1], kJsonCfgNss);

	nodes[0].Get()->MakeMaster(ReplicationConfigTest("master"));
	nodes[1].Get()->MakeSlave(ReplicationConfigTest("slave", false, true, serverId + 1, masterDsn));

	ServerControl& master = nodes[0];
	TestNamespace1 ns1(master, nsName);
	WaitSync(master, nodes[1], nsName);

	nodes[1].Drop();
	// Apply tx
	ns1.AddRowsTx(master, 0, kRows);

	// Restart node '1'
	nodes[1].InitServer(1, port + 1, port + 1000 + 1, kDbPathMaster + std::to_string(1), "db", true);
	nodes[1].Get()->MakeSlave(ReplicationConfigTest("slave", false, true, serverId + 1, masterDsn));
	WaitSync(master, nodes[1], nsName);

	// Check copy tx event in the perfstats
	CheckTxCopyEventsCount(nodes[1], 1);
}

TEST_F(ReplicationSlaveSlaveApi, ForceSync3Node) {
	// Check force-sync for cascade setup
	/*
			m
			|
			1
			|
			2
			|
			3
	*/

	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "ForceSync3Node"));
	ServerControl master;
	master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
	TestNamespace1 testns(master);
	testns.AddRows(master, 10, 1000);

	master.Get()->MakeMaster();

	ServerControl slave1;
	slave1.InitServer(1, 7771, 7881, kBaseDbPath + "/slave1", "db", true);
	std::string upDsn1 = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest configSlave1("slave", false, true, 1, upDsn1);
	slave1.Get()->MakeMaster();

	ServerControl slave2;
	slave2.InitServer(2, 7772, 7882, kBaseDbPath + "/slave2", "db", true);
	std::string upDsn2 = "cproto://127.0.0.1:7771/db";
	ReplicationConfigTest configSlave2("slave", false, true, 2, upDsn2);
	slave2.Get()->MakeSlave(configSlave2);

	ServerControl slave3;
	slave3.InitServer(3, 7773, 7883, kBaseDbPath + "/slave3", "db", true);
	std::string upDsn3 = "cproto://127.0.0.1:7772/db";
	ReplicationConfigTest configSlave3("slave", false, true, 3, upDsn3);
	slave3.Get()->MakeSlave(configSlave3);

	slave1.Get()->MakeSlave(configSlave1);

	WaitSync(master, slave1, "ns1");
	WaitSync(master, slave2, "ns1");
	WaitSync(master, slave3, "ns1");

	std::vector<int> results_m;
	testns.GetData(master, results_m);

	std::vector<int> results_s1;
	testns.GetData(slave1, results_s1);

	std::vector<int> results_s2;
	testns.GetData(slave2, results_s2);

	std::vector<int> results_s3;
	testns.GetData(slave3, results_s3);

	EXPECT_TRUE(results_m == results_s1);
	EXPECT_TRUE(results_m == results_s2);
	EXPECT_TRUE(results_m == results_s3);
}

TEST_F(ReplicationSlaveSlaveApi, NodeWithMasterAndSlaveNs1) {
	// Check syncing namespaces filtering and writable namespaces on slave
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "NodeWithMasterAndSlaveNs1"));
	ServerControl master;
	master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 11, 113);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, 11, 113);
	master.Get()->MakeMaster();

	const unsigned int c1 = 5011;
	const unsigned int c2 = 6013;
	const unsigned int n = 121;
	ServerControl slave;
	slave.InitServer(0, 7771, 7881, kBaseDbPath + "/slave", "db", true);
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn);
	slave.Get()->MakeSlave(configSlave);
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, "ns1");
	WaitSync(master, slave, "ns2");
	{
		std::vector<int> results_m;
		testns1.GetData(master, results_m);

		std::vector<int> results_s1;
		testns1.GetData(slave, results_s1);
		EXPECT_TRUE(results_m == results_s1);
	}
	{
		std::vector<int> results_m;
		testns2.GetData(master, results_m);

		std::vector<int> results_s1;
		testns2.GetData(slave, results_s1);
		EXPECT_TRUE(results_m == results_s1);
	}
	{
		std::vector<int> results_data;
		for (unsigned int i = 0; i < n; i++) {
			results_data.push_back(c1 + i);
		}
		for (unsigned int i = 0; i < n; i++) {
			results_data.push_back(c2 + i);
		}

		std::vector<int> results_3;
		testns3.GetData(slave, results_3);
		EXPECT_TRUE(results_data == results_3);
	}
}

TEST_F(ReplicationSlaveSlaveApi, NodeWithMasterAndSlaveNs2) {
	// Check existing namespace resync
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "NodeWithMasterAndSlaveNs2"));
	const unsigned int cm1 = 11;
	const unsigned int cm2 = 999;
	const unsigned int cm3 = 1999;
	const unsigned int nm = 113;

	ServerControl master;
	master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, cm1, nm);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, cm2, nm);
	master.Get()->MakeMaster();

	const unsigned int c1 = 5001;
	const unsigned int c2 = 6007;
	const unsigned int n = 101;
	ServerControl slave;
	slave.InitServer(0, 7771, 7881, kBaseDbPath + "/slave", "db", true);
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	TestNamespace1 testns4(slave, "ns1");
	testns4.AddRows(slave, c1, n);
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest::NsSet nsSet = {"ns1"};
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn, "slave", nsSet);
	slave.Get()->MakeSlave(configSlave);
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, "ns1");

	testns1.AddRows(master, cm3, nm);
	testns2.AddRows(master, cm2, nm);

	WaitSync(master, slave, "ns1");

	{
		std::vector<int> results_m;
		testns1.GetData(master, results_m);

		std::vector<int> results_s1;
		testns1.GetData(slave, results_s1);
		EXPECT_TRUE(results_m == results_s1);
	}
	{
		std::vector<int> results_data;
		for (unsigned int i = 0; i < n; i++) {
			results_data.push_back(c1 + i);
		}
		for (unsigned int i = 0; i < n; i++) {
			results_data.push_back(c2 + i);
		}

		std::vector<int> results_3;
		testns3.GetData(slave, results_3);
		EXPECT_TRUE(results_data == results_3);
	}
}

TEST_F(ReplicationSlaveSlaveApi, NodeWithMasterAndSlaveNs3) {
	// Check syncing namespaces filtering and writable namespaces on slave after role switch
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "NodeWithMasterAndSlaveNs3"));
	const unsigned int c1 = 5001;
	const unsigned int c2 = 6001;
	const unsigned int n = 101;
	ServerControl master;
	master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 11, n);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, 11, n);
	master.Get()->MakeMaster();

	ServerControl slave;
	slave.InitServer(0, 7771, 7881, kBaseDbPath + "/slave", "db", true);
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	TestNamespace1 testns4(slave, "ns1");
	testns4.AddRows(slave, c1, n);
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest::NsSet nsSet = {"ns1"};
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn, "slave", nsSet);
	slave.Get()->MakeSlave(configSlave);
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, "ns1");

	slave.Get()->MakeMaster();
	testns4.AddRows(slave, c1 + c2, n);
	{
		std::vector<int> results_m;
		testns4.GetData(slave, results_m);
		EXPECT_TRUE(results_m.size() == n * 2);
	}
}

TEST_F(ReplicationSlaveSlaveApi, RenameSlaveNs) {
	// create on master ns1 and ns2
	// create on slave  ns1 and ns3 ,ns1 sync whith master
	// 1. check on slave rename ns3 to ns3Rename ok
	// 2. check on slave rename ns1 to ns1RenameSlave fail
	// create on master temporary ns (tmpNsName)
	// 3. check on master rename tmpNsName to tmpNsNameRename fail
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "RenameSlaveNs"));
	ServerControl master;
	master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
	TestNamespace1 testns1(master, "ns1");
	const unsigned int n = 101;
	testns1.AddRows(master, 11, n);
	master.Get()->MakeMaster();
	TestNamespace1 testns2(master, "ns2");
	testns1.AddRows(master, 10015, n);
	Error err = master.Get()->api.reindexer->RenameNamespace("ns2", "ns2Rename");
	ASSERT_TRUE(err.ok()) << err.what();

	ServerControl slave;
	slave.InitServer(0, 7771, 7881, kBaseDbPath + "/slave", "db", true);
	TestNamespace1 testns3(slave, "ns3");
	unsigned int n3 = 1234;
	testns3.AddRows(slave, 5015, n3);
	TestNamespace1 testns4(slave, "ns1");
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ReplicationConfigTest::NsSet nsSet = {"ns1"};
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn, "slave", nsSet);
	slave.Get()->MakeSlave(configSlave);

	WaitSync(master, slave, "ns1");

	err = slave.Get()->api.reindexer->RenameNamespace("ns3", "ns3Rename");
	ASSERT_TRUE(err.ok()) << err.what();

	Query qr = Query("ns3Rename").Sort("id", false);
	BaseApi::QueryResultsType res(slave.Get()->api.reindexer.get());
	err = slave.Get()->api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	std::vector<int> results_m;
	for (auto it : res) {
		WrSerializer ser;
		auto err = it.GetJSON(ser, false);
		EXPECT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		auto root = parser.Parse(ser.Slice());
		results_m.push_back(root["id"].As<int>());
	}
	ASSERT_TRUE(results_m.size() == n3);

	err = slave.Get()->api.reindexer->RenameNamespace("ns1", "ns1RenameSlave");
	ASSERT_FALSE(err.ok());

	std::string tmpNsName("tmpNsName");
	NamespaceDef tmpNsDef = NamespaceDef(tmpNsName, StorageOpts().Enabled().CreateIfMissing());
	tmpNsDef.AddIndex("id", "hash", "int", IndexOpts().PK());
	tmpNsDef.isTemporary = true;
	err = master.Get()->api.reindexer->AddNamespace(tmpNsDef);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::client::Item item = master.Get()->api.NewItem(tmpNsName);
	err = item.FromJSON("{\"id\":" + std::to_string(10) + "}");
	ASSERT_TRUE(err.ok()) << err.what();
	err = master.Get()->api.reindexer->Upsert(tmpNsName, item);
	ASSERT_TRUE(err.ok()) << err.what();
	err = master.Get()->api.reindexer->RenameNamespace(tmpNsName, tmpNsName + "Rename");
	ASSERT_FALSE(err.ok());

	BaseApi::QueryResultsType r1(master.Get()->api.reindexer.get());
	err = master.Get()->api.reindexer->Select("Select * from " + tmpNsName, r1);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(r1.Count() == 1);
}
TEST_F(ReplicationSlaveSlaveApi, Node3ApplyWal) {
	// Node configuration:
	//			master
	//			  |
	//			slave1
	//            |
	//          slave2
	// Checks applying syncNamespaceByWAL on slave1 and slave2 node.

	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "Node3ApplyWal"));
	const std::string upDsn1 = "cproto://127.0.0.1:7770/db";
	const std::string upDsn2 = "cproto://127.0.0.1:7771/db";
	const unsigned int n = 2000;  // 11;
	{
		ServerControl master;
		ServerControl slave1;
		ServerControl slave2;
		master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
		master.Get()->MakeMaster();
		TestNamespace1 testns1(master, "ns1");
		testns1.AddRows(master, 3000, n);
		// start init of slave
		{
			slave1.InitServer(1, 7771, 7881, kBaseDbPath + "/slave1", "db", true);
			slave2.InitServer(2, 7772, 7882, kBaseDbPath + "/slave2", "db", true);
			ReplicationConfigTest configSlave1("slave", false, true, 1, upDsn1, "slave1");
			slave1.Get()->MakeSlave(configSlave1);
			ReplicationConfigTest configSlave2("slave", false, true, 2, upDsn2, "slave2");
			slave2.Get()->MakeSlave(configSlave2);
			WaitSync(master, slave1, "ns1");
			WaitSync(master, slave2, "ns1");
		}
		master.Stop();
		slave1.Stop();
		slave2.Stop();
	}

	{
		ServerControl master;
		master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
		master.Get()->MakeMaster();
		TestNamespace1 testns1(master, "ns1");
		testns1.AddRows(master, 30000, n);
	}
	ServerControl master;
	master.InitServer(0, 7770, 7880, kBaseDbPath + "/master", "db", true);
	master.Get()->MakeMaster();

	ServerControl slave1;
	slave1.InitServer(1, 7771, 7881, kBaseDbPath + "/slave1", "db", true);

	ServerControl slave2;
	slave2.InitServer(2, 7772, 7882, kBaseDbPath + "/slave2", "db", true);

	// std::this_thread::sleep_for(std::chrono::milliseconds(1000));

	WaitSync(master, slave1, "ns1");
	WaitSync(master, slave2, "ns1");
	master.Stop();
	slave1.Stop();
	slave2.Stop();
}

TEST_F(ReplicationSlaveSlaveApi, RestrictUpdates) {
	//  1. create master node,
	//  2. set max updates size 1024 * 5
	//  3. add 10000 rows
	//  4. start inser thread
	//  5. start slave node
	//  6. wait sync
	const std::string kBaseStoragePath = reindexer::fs::JoinPath(kBaseTestsetDbPath, "RestrictUpdates");
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	ServerControl master;
	master.InitServer(0, 7770, 7880, reindexer::fs::JoinPath(kBaseStoragePath, "master"), "db", true, 1024 * 5);

	master.Get()->MakeMaster(ReplicationConfigTest("master", "appMaster"));
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 0, 10000);

	const int count = 1000;
	const int from = 1000000;
	const std::string nsName("ns1");
	auto ThreadAdd = [&master, &nsName]() {
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = master.Get()->api.NewItem("ns1");
			std::string itemJson = "{\"id\":" + std::to_string(i + from) + "}";
			auto err = item.FromJSON(itemJson);
			ASSERT_TRUE(err.ok()) << err.what();
			master.Get()->api.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
			if (i % 100 == 0) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
	};

	std::thread insertThread(ThreadAdd);

	ServerControl slave;
	slave.InitServer(0, 7771, 7881, reindexer::fs::JoinPath(kBaseStoragePath, "slave"), "db", true);
	ReplicationConfigTest configSlave("slave", false, true, 0, upDsn, "slave");
	slave.Get()->MakeSlave(configSlave);

	insertThread.join();
	WaitSync(master, slave, "ns1");
}

TEST_F(ReplicationSlaveSlaveApi, LSNConflictWithSQLUpdate) {
	//  1. create leader/follower nodes,
	//  2. sync empty namespace
	//  3. shutdown follower
	//  4. add 20 rows
	//  5. perform full namespace update # here statement based replication could break the leader
	//  6. restart follower
	//  7. wait sync
	const std::string kBaseStoragePath = reindexer::fs::JoinPath(kBaseTestsetDbPath, "RestrictUpdatesWithSQLUpdate");
	const std::string upDsn = "cproto://127.0.0.1:7770/db";
	const std::string kNsName = "ns1";
	constexpr size_t kDataCount = 20;
	ServerControl leader;
	leader.InitServer(0, 7770, 7880, reindexer::fs::JoinPath(kBaseStoragePath, "leader"), "db", true, 1024 * 1024 * 1024);

	leader.Get()->MakeMaster(ReplicationConfigTest("master", "appLeader"));
	TestNamespace1 testns1(leader, kNsName);

	ServerControl follower;
	follower.InitServer(0, 7771, 7881, reindexer::fs::JoinPath(kBaseStoragePath, "follower"), "db", true);
	ReplicationConfigTest configFollower("slave", false, true, 0, upDsn, "follower");
	follower.Get()->MakeSlave(configFollower);
	WaitSync(leader, follower, kNsName);

	follower.Stop();
	follower.Drop();
	testns1.AddRows(leader, 0, kDataCount);
	auto leaderRx = leader.Get()->api.reindexer;
	client::SyncCoroQueryResults qr(leaderRx.get());
	auto err = leaderRx->Update(Query(kNsName).Set("new_data", "some string value"), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), kDataCount);

	follower.InitServer(0, 7771, 7881, reindexer::fs::JoinPath(kBaseStoragePath, "follower"), "db", true);
	WaitSync(leader, follower, kNsName);
}

#if !defined(REINDEX_WITH_TSAN)
TEST_F(ReplicationSlaveSlaveApi, ConcurrentForceSync) {
	/*
	 * Check concurrent force sync and updates subscription on nodes 1, 2 and 3
		 m
		 |
		 1
		/ \
	   2  3
	*/
	const int kBasePort = 9999;
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "ConcurrentForceSync"));
	const std::string kDbName("db");
	const std::vector<std::string> kNsList = {"ns1", "ns2", "ns3", "ns4"};
	const size_t kNsSyncCount = 3;
	const int kBaseServerId = 5;

	ServerControlVec nodes;
	auto createSlave = [&kBaseDbPath, &kDbName, &nodes, &kNsList](const std::string& masterDsn) {
		size_t id = nodes.size();
		nodes.push_back(ServerControl());
		nodes.back().InitServer(id, kBasePort + id, kBasePort + 1000 + id, kBaseDbPath + "/slave" + std::to_string(id), kDbName, true);
		ReplicationConfigTest::NsSet nsSet;
		for (size_t i = 0; i < kNsSyncCount; ++i) {
			nsSet.emplace(kNsList[i]);
		}
		ReplicationConfigTest config("slave", false, true, kBaseServerId + id, masterDsn, "slave" + std::to_string(id), nsSet);
		nodes.back().Get()->MakeSlave(config);
	};

	// Create master
	{
		nodes.push_back(ServerControl());
		nodes.back().InitServer(0, kBasePort, kBasePort + 1000, kBaseDbPath + "/master", kDbName, true);
		ReplicationConfigTest config("master");
		nodes.back().Get()->MakeMaster(config);
	}

	// Fill master's data
	const size_t kRows = 10000;
	const size_t kDataBytes = 1000;
	std::vector<TestNamespace1> testNsList;
	for (auto& ns : kNsList) {
		testNsList.emplace_back(nodes[0], ns);
		testNsList.back().AddRows(nodes[0], 0, kRows, kDataBytes);
	}

	// Create semimaster
	const std::string kMasterDsn = "cproto://127.0.0.1:" + std::to_string(kBasePort) + "/db";
	const std::string kSemimasterDsn = "cproto://127.0.0.1:" + std::to_string(kBasePort + nodes.size()) + "/db";
	createSlave(kMasterDsn);

	// Create slaves
	createSlave(kSemimasterDsn);
	createSlave(kSemimasterDsn);

	for (size_t i = 1; i < nodes.size(); i++) {
		for (size_t j = 0; j < kNsSyncCount; ++j) {
			WaitSync(nodes[0], nodes[i], kNsList[j]);
		}
	}

	// Add one more row to master
	for (auto& ns : testNsList) {
		ns.AddRows(nodes[0], kRows, 1, kDataBytes);
	}

	for (size_t i = 0; i < kNsSyncCount; ++i) {
		std::vector<std::vector<int>> results;
		for (size_t j = 0; j < nodes.size(); j++) {
			results.push_back(std::vector<int>());
			WaitSync(nodes[0], nodes[j], kNsList[i]);
			testNsList[i].GetData(nodes[j], results.back());
		}

		for (size_t j = 1; j < results.size(); ++j) {
			EXPECT_TRUE((results[0] == results[j]));
		}
	}
	for (size_t i = 0; i < nodes.size(); ++i) {
		std::vector<NamespaceDef> nsDefs;
		auto err = nodes[i].Get()->api.reindexer->EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem().WithClosed());
		ASSERT_TRUE(err.ok()) << err.what();
		if (i == 0) {
			EXPECT_EQ(nsDefs.size(), kNsList.size());
		} else {
			EXPECT_EQ(nsDefs.size(), kNsSyncCount);
		}
	}
}
#endif

TEST_F(ReplicationSlaveSlaveApi, WriteIntoSlaveNsAfterReconfiguration) {
	// Check if it possible to write in slave's ns after removing this ns from replication ns list
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "WriteIntoSlaveNsAfterReconfiguration/node_"));
	const unsigned int n = 5;
	const int kBasePort = 7770;
	const int kServerId = 5;
	const std::string kNs1 = "ns1";
	const std::string kNs2 = "ns2";
	int manualItemId = 5;
	ServerControlVec nodes;
	CreateConfiguration(nodes, {-1, kBasePort}, kBasePort, kServerId, kBaseDbPath);
	TestNamespace1 testns1(nodes[0], kNs1);
	testns1.AddRows(nodes[0], 0, n);
	TestNamespace1 testns2(nodes[0], kNs2);
	testns2.AddRows(nodes[0], 1, n);

	WaitSync(nodes[0], nodes[1], kNs1);
	WaitSync(nodes[0], nodes[1], kNs2);

	auto createItem = [](ServerControl& node, const std::string& ns, int itemId) -> reindexer::client::Item {
		reindexer::client::Item item = node.Get()->api.NewItem(ns);
		auto err = item.FromJSON("{\"id\":" + std::to_string(itemId) + "}");
		EXPECT_TRUE(err.ok()) << err.what();
		return item;
	};

	auto item = createItem(nodes[1], kNs1, manualItemId);
	auto err = nodes[1].Get()->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errLogic) << err.what();
	item = createItem(nodes[1], kNs2, manualItemId);
	err = nodes[1].Get()->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errLogic) << err.what();

	// Remove ns1 from replication config
	const std::string kUpDsn = "cproto://127.0.0.1:7770/db";
	{
		ReplicationConfigTest::NsSet nsSet = {"ns2"};
		ReplicationConfigTest configSlave("slave", false, true, kServerId + 1, kUpDsn, "slave", nsSet);
		nodes[1].Get()->MakeSlave(configSlave);
	}

	item = createItem(nodes[1], kNs1, manualItemId);
	err = nodes[1].Get()->api.reindexer->Upsert(kNs1, item);
	ASSERT_TRUE(err.ok()) << err.what();
	item = createItem(nodes[1], kNs2, manualItemId++);
	err = nodes[1].Get()->api.reindexer->Upsert(kNs2, item);
	ASSERT_EQ(err.code(), errLogic) << err.what();

	testns1.AddRows(nodes[0], 100, n);
	testns2.AddRows(nodes[0], 100, n);
	WaitSync(nodes[0], nodes[1], kNs2);

	// Restart slave
	RestartServer(1, nodes, kBasePort, kBaseDbPath);

	item = createItem(nodes[1], kNs1, manualItemId);
	err = nodes[1].Get()->api.reindexer->Upsert(kNs1, item);
	ASSERT_TRUE(err.ok()) << err.what();
	item = createItem(nodes[1], kNs2, manualItemId++);
	err = nodes[1].Get()->api.reindexer->Upsert(kNs2, item);
	ASSERT_EQ(err.code(), errLogic) << err.what();

	auto validateItemsCount = [](ServerControl& node, const std::string& nsName, size_t expectedCnt) {
		BaseApi::QueryResultsType qr(node.Get()->api.reindexer.get());
		auto err = node.Get()->api.reindexer->Select(Query(nsName), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), expectedCnt);
	};
	validateItemsCount(nodes[0], kNs1, 2 * n);
	validateItemsCount(nodes[0], kNs2, 2 * n);
	validateItemsCount(nodes[1], kNs1, n + 2);
	validateItemsCount(nodes[1], kNs2, 2 * n);

	// Enable slave mode for ns1
	{
		ReplicationConfigTest::NsSet nsSet = {"ns1", "ns2"};
		ReplicationConfigTest configSlave("slave", false, true, kServerId + 1, kUpDsn, "slave", nsSet);
		nodes[1].Get()->MakeSlave(configSlave);
		WaitSync(nodes[0], nodes[1], kNs1);
		WaitSync(nodes[0], nodes[1], kNs2);
	}

	item = createItem(nodes[1], kNs1, manualItemId);
	err = nodes[1].Get()->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errLogic) << err.what();
	item = createItem(nodes[1], kNs2, manualItemId);
	err = nodes[1].Get()->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errLogic) << err.what();

	testns1.AddRows(nodes[0], 200, n);
	testns2.AddRows(nodes[0], 200, n);
	WaitSync(nodes[0], nodes[1], kNs1);
	WaitSync(nodes[0], nodes[1], kNs2);
	validateItemsCount(nodes[0], kNs1, 3 * n);
	validateItemsCount(nodes[0], kNs2, 3 * n);
	validateItemsCount(nodes[1], kNs1, 3 * n);
	validateItemsCount(nodes[1], kNs2, 3 * n);
}

struct DataStore {
	void Add(int64_t id, const std::string& s) {
		std::unique_lock l(mtx);
		data[id] = s;
	}
	bool Check(const std::map<int64_t, std::string>& r) {
		std::unique_lock l(mtx);
		return data == r;
	}
	int64_t Size() {
		std::unique_lock l(mtx);
		return data.size();
	}

private:
	std::mutex mtx;
	std::map<int64_t, std::string> data;
};

class ServerIdChange : public ReplicationSlaveSlaveApi, public ::testing::WithParamInterface<int> {
protected:
	void SetUp() { fs::RmDirAll(kBaseTestsetDbPathServerIdChange); }

	void TearDown() {}

public:
	void AddFun(ServerControl& master, DataStore& dataStore, int fromId, unsigned int dn) {
		for (unsigned int i = 0; i < dn; i++) {
			reindexer::client::Item item = master.Get()->api.NewItem("ns1");
			int64_t id = fromId + i;
			std::string ss = reindexer::randStringAlph(10);
			dataStore.Add(id, ss);
			auto err = item.FromJSON("{\"id\":" + std::to_string(id) + ",\"data\":\"" + ss + "\"" + "}");
			ASSERT_TRUE(err.ok()) << err.what();
			master.Get()->api.Upsert("ns1", item);
		}
	}

	void ChangeServerId(bool isMaster, ServerControl& node, int newServerId, int port) {
		if (isMaster) {
			ReplicationConfigTest config("master");
			config.serverId_ = newServerId;
			node.Get()->MakeMaster(config);
		} else {
			std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(port) + "/db";
			ReplicationConfigTest config("slave", false, true, newServerId, masterDsn);
			node.Get()->MakeSlave(config);
		}
	}

protected:
	const std::string kBaseTestsetDbPathServerIdChange = fs::JoinPath(fs::GetTempDir(), "rx_test/ServerIdChange");
};

TEST_P(ServerIdChange, UpdateServerId) {
	const int port = 10100;
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPathServerIdChange, "UpdateServerId"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	std::vector<ServerControl> nodes;
	DataStore dataStore;

	/*
			m
		   / \
		  1   2
		  |
		  3
	*/

	std::vector<int> slaveConfiguration = {-1, port, port, port + 1};
	for (size_t i = 0; i < slaveConfiguration.size(); i++) {
		nodes.emplace_back().InitServer(i, port + i, port + 1000 + i, kDbPathMaster + std::to_string(i), "db", true);
		ChangeServerId(i == 0, nodes.back(), 0, slaveConfiguration[i]);
	}

	ServerControl& master = nodes[0];
	TestNamespace1 ns(master);

	const int startId = 0;
	const int n2 = 20000;
	const int dn = 10;

	AddFun(master, dataStore, startId, n2);

	for (size_t i = 1; i < nodes.size(); i++) {
		WaitSync(nodes[0], nodes[i], "ns1");
	}
	for (auto& n : nodes) {
		n.Get()->SetWALSize(GetParam(), "ns1");
	}

	auto changeConfig = [this, &nodes, &slaveConfiguration, &ns, &dataStore](bool isMaster, int configurationIndex, int newServerId,
																			 int from) {
		std::atomic_bool stopInsertThread = false;
		std::mutex m;
		std::condition_variable cv;
		bool startChange = false;

		auto AddThreadFun = [this, &startChange, &m, &stopInsertThread, &nodes, &dataStore, &cv]() {
			bool isFirst = true;
			while (!stopInsertThread) {
				int64_t fromId = rand() % 1'000'000;
				AddFun(nodes[0], dataStore, fromId, 10);
				if (isFirst) {
					{
						std::unique_lock lk(m);
						startChange = true;
					}
					cv.notify_all();
				}
				std::this_thread::sleep_for(std::chrono::microseconds(10));
				isFirst = false;
			}
		};

		std::unique_lock lk(m);
		std::thread insertThread(AddThreadFun);
		cv.wait(lk, [&startChange] { return startChange; });
		lk.unlock();

		ChangeServerId(isMaster, nodes[configurationIndex], newServerId, slaveConfiguration[configurationIndex]);
		AddFun(nodes[0], dataStore, from, dn);

		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		stopInsertThread = true;
		insertThread.join();

		for (size_t i = 1; i < nodes.size(); i++) {
			WaitSync(nodes[0], nodes[i], "ns1");
		}

		std::vector<std::map<int64_t, std::string>> results;

		Query qr = Query("ns1").Sort("id", true);

		for (size_t i = 0; i < nodes.size(); i++) {
			results.emplace_back();
			ns.GetDataWithStrings(nodes[i], results.back());
			ASSERT_EQ(results.back().size(), dataStore.Size()) << " nodeIndex=" << i;
		}

		for (size_t i = 1; i < results.size(); ++i) {
			ASSERT_TRUE(dataStore.Check(results[i]));
		}
	};

	std::unordered_set<int> usedId;
	for (int i = 0; i < 10; i++) {
		int sId = 0;
		while (true) {
			sId = rand() % 100 + 300;
			if (usedId.find(sId) == usedId.end()) {
				usedId.insert(sId);
				break;
			}
		}

		bool isMaster = rand() % 2;
		int configurationIndex = 0;
		if (!isMaster) {
			configurationIndex = rand() % 3 + 1;
		}
		changeConfig(isMaster, configurationIndex, sId, startId + n2 + dn * (i + 1));
	}

	for (auto& node : nodes) {
		node.Stop();
	}
}

INSTANTIATE_TEST_SUITE_P(WalSize, ServerIdChange, ::testing::Values(1, 4000000));
