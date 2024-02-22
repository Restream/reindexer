#include <thread>
#include "cascade_replication_api.h"
#include "core/cjson/jsonbuilder.h"
#include "core/queryresults/queryresults.h"
#include "vendor/gason/gason.h"

using namespace reindexer;

TEST_F(CascadeReplicationApi, MasterSlaveSyncByWalAddRow) {
	// Check WAL synchronization on a single row
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveSyncByWalAddRow"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int port = 9999;

	std::vector<int> clusterConfig = {-1, 0};
	Cluster cluster = CreateConfiguration(clusterConfig, port, 10, kDbPathMaster);

	TestNamespace1 ns1(cluster.Get(0));
	cluster.Get(0)->SetWALSize(100000, ns1.nsName_);
	int n1 = 1000;
	ns1.AddRows(cluster.Get(0), 0, n1);

	WaitSync(cluster.Get(0), cluster.Get(1), ns1.nsName_);

	const auto replState = cluster.Get(1)->GetState(ns1.nsName_);
	ASSERT_EQ(replState.role, ClusterizationStatus::Role::SimpleReplica);

	cluster.ShutdownServer(1);

	const int startId = 10000;
	const unsigned int n2 = 2000;
	auto master = cluster.Get(0);
	auto ThreadAdd = [&master, &ns1]() {
		master->SetWALSize(50000, ns1.nsName_);
		ns1.AddRows(master, startId, n2);
	};

	std::thread insertThread(ThreadAdd);

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	cluster.InitServer(1, port + 1, port + 1000 + 1, kDbPathMaster + std::to_string(1), "db", true);

	insertThread.join();

	WaitSync(cluster.Get(0), cluster.Get(1), "ns1");

	std::vector<int> ids0;
	ns1.GetData(cluster.Get(0), ids0);
	std::vector<int> ids1;
	ns1.GetData(cluster.Get(1), ids1);

	EXPECT_TRUE(ids1.size() == (n1 + n2));
	EXPECT_TRUE(ids0 == ids1);
}

TEST_F(CascadeReplicationApi, MasterSlaveStart) {
	// Check WAL/force sync on multiple rows
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveStart"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int port = 9999;

	std::vector<int> clusterConfig = {-1, 0};
	auto cluster = CreateConfiguration(clusterConfig, port, 10, kDbPathMaster);

	// Insert 100 rows
	std::string nsName("ns1");
	TestNamespace1 ns1(cluster.Get(0), nsName);

	unsigned int n1 = 100;
	ns1.AddRows(cluster.Get(0), 0, n1);
	cluster.Get(0)->SetWALSize(1000, "ns1");

	WaitSync(cluster.Get(0), cluster.Get(1), nsName);
	// restart Slave
	cluster.RestartServer(1, port, kDbPathMaster);
	WaitSync(cluster.Get(0), cluster.Get(1), nsName);

	// shutdown slave
	cluster.ShutdownServer(1);
	// insert another 100 rows (200 total)
	ns1.AddRows(cluster.Get(0), n1 + 1, n1);

	// run slave
	cluster.InitServer(1, port + 1, port + 1000 + 1, kDbPathMaster + std::to_string(1), "db", true);
	WaitSync(cluster.Get(0), cluster.Get(1), nsName);

	std::vector<int> ids0;
	ns1.GetData(cluster.Get(0), ids0);
	std::vector<int> ids1;
	ns1.GetData(cluster.Get(1), ids1);

	EXPECT_TRUE(ids1.size() == (n1 + n1));
	EXPECT_TRUE(ids0 == ids1);
}

TEST_F(CascadeReplicationApi, InterceptingSeparateSlaveNsLists) {
	// Check replication with intercepting separate nodes namespace lists
	/*
				 leader
			 /      |    \
			1       2    3
		(ns1,ns2) (ns1) (*-ns1,ns2,ns3)
	*/
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "InterceptingSeparateSlaveNsLists"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int port = 9999;
	const std::string kNs1 = "ns1";
	const std::string kNs2 = "ns2";
	const std::string kNs3 = "ns3";
	const std::vector<std::string> kFollowerNsList1 = {kNs1, kNs2};
	const std::vector<std::string> kFollowerNsList2 = {kNs1};
	const std::vector<std::string> kFollowerNsList3 = {};

	std::vector<FollowerConfig> clusterConfig = {FollowerConfig{-1}, FollowerConfig{0, kFollowerNsList1},
												 FollowerConfig{0, kFollowerNsList2}, FollowerConfig{0}};
	auto cluster = CreateConfiguration(clusterConfig, port, 10, kDbPathMaster, {});

	// Insert few rows to each namespace
	auto leader = cluster.Get(0);
	std::vector<TestNamespace1> testNss = {TestNamespace1{leader, kNs1}, TestNamespace1{leader, kNs2}, TestNamespace1{leader, kNs3}};
	const unsigned int n1 = 20;
	for (auto& ns : testNss) {
		ns.AddRows(leader, 0, n1);
	}

	WaitSync(leader, cluster.Get(1), kNs1);
	WaitSync(leader, cluster.Get(1), kNs2);
	WaitSync(leader, cluster.Get(2), kNs1);
	WaitSync(leader, cluster.Get(3), kNs1);
	WaitSync(leader, cluster.Get(3), kNs2);
	WaitSync(leader, cluster.Get(3), kNs3);
	ValidateNsList(cluster.Get(1), clusterConfig[1].nsList.value());  // NOLINT(bugprone-unchecked-optional-access)
	ValidateNsList(cluster.Get(2), clusterConfig[2].nsList.value());  // NOLINT(bugprone-unchecked-optional-access)
	ValidateNsList(cluster.Get(3), {kNs1, kNs2, kNs3});

	auto stats = leader->GetReplicationStats(cluster::kAsyncReplStatsType);
	WrSerializer wser;
	stats.GetJSON(wser);
	ASSERT_EQ(stats.nodeStats.size(), 3) << wser.Slice();
	ASSERT_EQ(stats.nodeStats[0].namespaces, kFollowerNsList1) << wser.Slice();
	ASSERT_EQ(stats.nodeStats[1].namespaces, kFollowerNsList2) << wser.Slice();
	ASSERT_EQ(stats.nodeStats[2].namespaces, kFollowerNsList3) << wser.Slice();
}

TEST_F(CascadeReplicationApi, NonInterceptingSeparateSlaveNsLists) {
	// Check replication with non-intercepting separate nodes namespace lists
	/*
				 leader
			 /      |    \
			1       2    3
		(ns1)     (ns2) (*-ns3)
	*/
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "NonInterceptingSeparateSlaveNsLists"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int port = 9999;
	const std::string kNs1 = "ns1";
	const std::string kNs2 = "ns2";
	const std::string kNs3 = "ns3";

	std::vector<FollowerConfig> clusterConfig = {FollowerConfig{-1}, FollowerConfig{0, {{kNs1}}}, FollowerConfig{0, {{kNs2}}},
												 FollowerConfig{0}};
	auto cluster = CreateConfiguration(clusterConfig, port, 10, kDbPathMaster, {kNs3});

	// Insert few rows to each namespace
	auto leader = cluster.Get(0);
	std::vector<TestNamespace1> testNss = {TestNamespace1{leader, kNs1}, TestNamespace1{leader, kNs2}, TestNamespace1{leader, kNs3}};
	const unsigned int n1 = 20;
	for (auto& ns : testNss) {
		ns.AddRows(leader, 0, n1);
	}

	WaitSync(leader, cluster.Get(1), kNs1);
	WaitSync(leader, cluster.Get(2), kNs2);
	WaitSync(leader, cluster.Get(3), kNs3);
	ValidateNsList(cluster.Get(1), clusterConfig[1].nsList.value());  // NOLINT(bugprone-unchecked-optional-access)
	ValidateNsList(cluster.Get(2), clusterConfig[2].nsList.value());  // NOLINT(bugprone-unchecked-optional-access)
	ValidateNsList(cluster.Get(3), {kNs3});
}

TEST_F(CascadeReplicationApi, MasterSlaveSlave2) {
	// Check WAL/force sync on cascade setups
	auto SimpleTest = [this](int port, const std::vector<int>& clusterConfig) {
		const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveSlave2"));
		const std::string kDbPathMaster(kBaseDbPath + "/test_");
		const int serverId = 5;
		auto cluster = CreateConfiguration(clusterConfig, port, serverId, kDbPathMaster);

		auto master = cluster.Get(0);
		TestNamespace1 ns1(master);

		const int count = 1000;
		ns1.AddRows(master, 0, count);

		for (size_t i = 1; i < cluster.Size(); i++) {
			WaitSync(master, cluster.Get(i), ns1.nsName_);
		}

		std::vector<std::vector<int>> results;
		for (size_t i = 0; i < clusterConfig.size(); i++) {
			results.push_back(std::vector<int>());
			ns1.GetData(cluster.Get(i), results.back());
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
		std::vector<int> clusterConfig = {-1, 0, 1};
		SimpleTest(port, clusterConfig);
	}
	{
		/*
				m
			   / \
			  1   2
			  |   | \
			  3   4  5
		*/

		std::vector<int> clusterConfig = {-1, 0, 0, 1, 2, 2};
		SimpleTest(port, clusterConfig);
	}
}

#if !defined(REINDEX_WITH_TSAN)
TEST_F(CascadeReplicationApi, MasterSlaveSlaveReload) {
	// Check synchronization continous nodes' restarting
	const int port = 9999;
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "MasterSlaveSlaveReload"));
	const std::string kDbPathMaster(kBaseDbPath + "/test_");
	const int serverId = 5;
	std::atomic_bool stopRestartServerThread(false);

	/*
			m
		   / \
		  1   2
		  |   | \
		  3   4  5
	*/
	const std::vector<int> clusterConfig = {-1, 0, 0, 1, 2, 2};
	auto cluster = CreateConfiguration(clusterConfig, port, serverId, kDbPathMaster);
	auto leader = cluster.Get(0);
	TestNamespace1 ns1(leader);
	const int startId = 1000;
	const int n2 = 20000;

	auto AddThread = [&leader, &ns1]() { ns1.AddRows(leader, startId, n2); };

	auto restartServer = [&cluster, &kDbPathMaster, &stopRestartServerThread]() {
		while (!stopRestartServerThread) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			int N = rand() % 3;
			cluster.RestartServer(N + 1, port, kDbPathMaster);
		}
	};

	std::thread insertThread(AddThread);
	std::thread restartThread(restartServer);

	insertThread.join();
	stopRestartServerThread = true;
	restartThread.join();
	//---------------------------

	for (size_t i = 1; i < cluster.Size(); ++i) {
		TestCout() << "Awaiting sync with " << i << std::endl;
		WaitSync(leader, cluster.Get(i), ns1.nsName_);
	}

	std::vector<std::vector<int>> results;

	Query qr = Query(ns1.nsName_).Sort("id", true);

	for (size_t i = 0; i < cluster.Size(); ++i) {
		results.push_back(std::vector<int>());
		ns1.GetData(cluster.Get(i), results.back());
	}

	for (size_t i = 1; i < results.size(); ++i) {
		EXPECT_TRUE((results[0] == results[i])) << i << "; size[0]: " << results[0].size() << "; size[i]: " << results[i].size();
	}
}
#endif

TEST_F(CascadeReplicationApi, TransactionTest) {
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
	const std::string kDbPathMaster(fs::JoinPath(fs::JoinPath(kBaseTestsetDbPath, "TransactionTest"), "test_"));
	const int serverId = 5;
	const std::vector<int> clusterConfig = {-1, 0, 1, 2, 3};
	auto cluster = CreateConfiguration(clusterConfig, port, serverId, kDbPathMaster);
	const size_t kRows = 100;

	auto master = cluster.Get(0);
	TestNamespace1 ns1(master);

	ns1.AddRows(master, 0, kRows);

	for (size_t i = 1; i < cluster.Size(); i++) {
		WaitSync(master, cluster.Get(i), ns1.nsName_);
	}

	auto tr = master->api.reindexer->NewTransaction(ns1.nsName_);
	for (unsigned int i = 0; i < kRows; i++) {
		reindexer::client::Item item = tr.NewItem();
		auto err = item.FromJSON("{\"id\":" + std::to_string(i + kRows * 10) + "}");
		tr.Upsert(std::move(item));
	}
	BaseApi::QueryResultsType qr;
	master->api.reindexer->CommitTransaction(tr, qr);

	for (size_t i = 1; i < cluster.Size(); i++) {
		WaitSync(master, cluster.Get(i), ns1.nsName_);
	}

	std::vector<std::vector<int>> results;
	for (size_t i = 0; i < cluster.Size(); i++) {
		results.push_back(std::vector<int>());
		ns1.GetData(cluster.Get(i), results.back());
	}

	for (size_t i = 1; i < results.size(); ++i) {
		EXPECT_TRUE((results[0] == results[i]));
	}
}

TEST_F(CascadeReplicationApi, TransactionCopyPolicyForceSync) {
	// Check transactions copy policy after force sync
	/*
			l
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

	auto nodes = CreateConfiguration({-1, 0, 1}, port, serverId, kDbPathMaster);
	for (size_t i = 0; i < nodes.Size(); ++i) {
		nodes.Get(i)->EnableAllProfilings();
	}

	// Set tx copy policy for the node '2' to 'always copy'
	ApplyConfig(nodes.Get(2), kJsonCfgNss);

	nodes.ShutdownServer(2);

	auto leader = nodes.Get(0);
	TestNamespace1 ns1(leader, nsName);
	WaitSync(leader, nodes.Get(1), nsName);

	// Restart node '2'
	nodes.InitServer(2, port + 2, port + 1000 + 2, kDbPathMaster + std::to_string(2), "db", true);
	auto follower = nodes.Get(2);
	WaitSync(leader, follower, nsName);

	// Check copy tx events in the perfstats before tx
	CheckTxCopyEventsCount(follower, 0);

	// Apply tx
	ns1.AddRowsTx(leader, 0, kRows);
	WaitSync(leader, follower, nsName);

	// Check copy tx events in the perfstats after tx
	CheckTxCopyEventsCount(follower, 1);
}

TEST_F(CascadeReplicationApi, TransactionCopyPolicyWalSync) {
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
	const std::string kDbPathMaster(fs::JoinPath(fs::JoinPath(kBaseTestsetDbPath, "TransactionCopyPolicyWalSync"), "/test_"));
	constexpr int serverId = 5;
	constexpr size_t kRows = 100;
	const std::string nsName("ns1");

	auto nodes = CreateConfiguration({-1, 0}, port, serverId, kDbPathMaster);
	for (size_t i = 0; i < nodes.Size(); ++i) {
		nodes.Get(i)->EnableAllProfilings();
	}

	// Set tx copy policy for the node '1' to 'always copy'
	ApplyConfig(nodes.Get(1), kJsonCfgNss);

	auto leader = nodes.Get(0);
	TestNamespace1 ns1(leader, nsName);
	WaitSync(leader, nodes.Get(1), nsName);

	nodes.ShutdownServer(1);
	// Apply tx
	ns1.AddRowsTx(leader, 0, kRows);

	// Restart node '1'
	nodes.InitServer(1, port + 1, port + 1000 + 1, kDbPathMaster + std::to_string(1), "db", true);
	WaitSync(leader, nodes.Get(1), nsName);

	// Check copy tx event in the perfstats
	CheckTxCopyEventsCount(nodes.Get(1), 1);
}

TEST_F(CascadeReplicationApi, ForceSync3Node) {
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
	ServerControl masterSc;

	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
	auto master = masterSc.Get();
	TestNamespace1 testns(master);
	testns.AddRows(master, 10, 1000);
	master->MakeLeader();

	ServerControl slave1;
	slave1.InitServer(ServerControlConfig(1, 7771, 7881, kBaseDbPath + "/slave1", "db"));
	slave1.Get()->MakeFollower();
	master->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave1.Get()->RpcPort()));

	ServerControl slave2;
	slave2.InitServer(ServerControlConfig(2, 7772, 7882, kBaseDbPath + "/slave2", "db"));
	slave2.Get()->MakeFollower();
	slave1.Get()->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave2.Get()->RpcPort()));

	ServerControl slave3;
	slave3.InitServer(ServerControlConfig(3, 7773, 7883, kBaseDbPath + "/slave3", "db"));
	slave3.Get()->MakeFollower();
	slave2.Get()->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave3.Get()->RpcPort()));

	WaitSync(master, slave1.Get(), testns.nsName_);
	WaitSync(master, slave2.Get(), testns.nsName_);
	WaitSync(master, slave3.Get(), testns.nsName_);

	std::vector<int> results_m;
	testns.GetData(master, results_m);

	std::vector<int> results_s1;
	testns.GetData(slave1.Get(), results_s1);

	std::vector<int> results_s2;
	testns.GetData(slave2.Get(), results_s2);

	std::vector<int> results_s3;
	testns.GetData(slave3.Get(), results_s3);

	EXPECT_TRUE(results_m == results_s1);
	EXPECT_TRUE(results_m == results_s2);
	EXPECT_TRUE(results_m == results_s3);
}

TEST_F(CascadeReplicationApi, NodeWithMasterAndSlaveNs1) {
	// Check syncing namespaces filtering and writable namespaces on slave
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "NodeWithMasterAndSlaveNs1"));
	ServerControl masterSc;
	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
	auto master = masterSc.Get();
	master->MakeLeader();
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 11, 113);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, 11, 113);

	const unsigned int c1 = 5011;
	const unsigned int c2 = 6013;
	const unsigned int n = 121;
	ServerControl slaveSc;
	slaveSc.InitServer(ServerControlConfig(1, 7771, 7881, kBaseDbPath + "/slave", "db"));
	auto slave = slaveSc.Get();
	slave->MakeFollower();
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	master->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave->RpcPort()));
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, testns1.nsName_);
	WaitSync(master, slave, testns2.nsName_);
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
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c1 + i);
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c2 + i);

		std::vector<int> results_3;
		testns3.GetData(slave, results_3);
		EXPECT_TRUE(results_data == results_3);
	}
}

TEST_F(CascadeReplicationApi, NodeWithMasterAndSlaveNs2) {
	// Check existing namespace resync
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "NodeWithMasterAndSlaveNs2"));
	const unsigned int cm1 = 11;
	const unsigned int cm2 = 999;
	const unsigned int cm3 = 1999;
	const unsigned int nm = 113;

	ServerControl masterSc;
	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
	auto master = masterSc.Get();
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, cm1, nm);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, cm2, nm);

	const unsigned int c1 = 5001;
	const unsigned int c2 = 6007;
	const unsigned int n = 101;
	ServerControl slaveSc;
	slaveSc.InitServer(ServerControlConfig(0, 7771, 7881, kBaseDbPath + "/slave", "db"));
	auto slave = slaveSc.Get();
	slave->MakeFollower();
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	TestNamespace1 testns4(slave, "ns1");
	testns4.AddRows(slave, c1, n);
	master->MakeLeader(AsyncReplicationConfigTest("leader", {}, true, true, 0, "node0"));
	master->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave->RpcPort()), {{"ns1"}});
	testns3.AddRows(slave, c2, n);

	WaitSync(master, slave, testns1.nsName_);

	testns1.AddRows(master, cm3, nm);
	testns2.AddRows(master, cm2, nm);

	ASSERT_EQ(testns1.nsName_, testns4.nsName_);
	WaitSync(master, slave, testns1.nsName_);

	{
		std::vector<int> results_m;
		testns1.GetData(master, results_m);

		std::vector<int> results_s1;
		testns1.GetData(slave, results_s1);
		EXPECT_TRUE(results_m == results_s1);
	}
	{
		std::vector<int> results_data;
		results_data.reserve(2 * n);
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c1 + i);
		for (unsigned int i = 0; i < n; i++) results_data.push_back(c2 + i);

		std::vector<int> results_3;
		results_3.reserve(results_data.size());
		testns3.GetData(slave, results_3);
		EXPECT_TRUE(results_data == results_3);
	}
}

TEST_F(CascadeReplicationApi, NodeWithMasterAndSlaveNs3) {
	// Check syncing namespaces filtering and writable namespaces on slave after role switch
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "NodeWithMasterAndSlaveNs3"));
	const unsigned int c1 = 5001;
	const unsigned int c2 = 6001;
	const unsigned int n = 101;
	ServerControl masterSc;
	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
	auto master = masterSc.Get();
	master->MakeLeader();
	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 11, n);
	TestNamespace1 testns2(master, "ns2");
	testns2.AddRows(master, 11, n);

	ServerControl slaveSc;
	slaveSc.InitServer(ServerControlConfig(0, 7771, 7881, kBaseDbPath + "/slave", "db"));
	auto slave = slaveSc.Get();
	slave->MakeFollower();
	TestNamespace1 testns3(slave, "ns3");
	testns3.AddRows(slave, c1, n);
	TestNamespace1 testns4(slave, "ns1");
	testns4.AddRows(slave, c1, n);
	master->SetReplicationConfig(AsyncReplicationConfigTest("leader", {}, true, true, 0, "node0"));
	master->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave->RpcPort()), {{"ns1"}});
	testns3.AddRows(slave, c2, n);

	ASSERT_EQ(testns1.nsName_, testns4.nsName_);
	WaitSync(master, slave, testns1.nsName_);

	slave->MakeLeader();
	master->SetReplicationConfig(AsyncReplicationConfigTest("leader", {}, true, true, 0, "node0", {}));
	slave->ResetReplicationRole();
	testns4.AddRows(slave, c1 + c2, n);

	std::vector<int> results_m;
	testns4.GetData(slave, results_m);
	ASSERT_TRUE(results_m.size() == n * 2);
	ValidateNsList(master, {testns1.nsName_, testns2.nsName_});
	ValidateNsList(slave, {testns3.nsName_, testns4.nsName_});
}

TEST_F(CascadeReplicationApi, RenameError) {
	// Check if rename still returns error
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "ForceSync3Node"));
	ServerControl masterSc;

	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
	auto master = masterSc.Get();
	TestNamespace1 testns(master);
	testns.AddRows(master, 10, 10);
	master->MakeLeader();

	ServerControl slave1;
	slave1.InitServer(ServerControlConfig(1, 7771, 7881, kBaseDbPath + "/slave1", "db"));
	slave1.Get()->MakeFollower();
	master->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave1.Get()->RpcPort()));

	WaitSync(master, slave1.Get(), testns.nsName_);

	// Check if ns renaming is not posible in this config
	auto err = master->api.reindexer->RenameNamespace(testns.nsName_, "new_ns");
	ASSERT_EQ(err.code(), errParams) << err.what();
	std::vector<NamespaceDef> defs;
	err = master->api.reindexer->EnumNamespaces(defs, EnumNamespacesOpts().OnlyNames().HideSystem());
	ASSERT_EQ(defs.size(), 1);
	ASSERT_EQ(defs[0].name, testns.nsName_);
}

// TODO: Enable this test, when new repliation will support namesapce rename
TEST_F(CascadeReplicationApi, DISABLED_RenameSlaveNs) {
	// create on master ns1 and ns2
	// create on slave  ns1 and ns3 ,ns1 sync whith master
	// 1. check on slave rename ns3 to ns3Rename ok
	// 2. check on slave rename ns1 to ns1RenameSlave fail
	// create on master temporary ns (tmpNsName)
	// 3. check on master rename tmpNsName to tmpNsNameRename fail
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "RenameSlaveNs"));
	ServerControl masterSc;
	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
	auto master = masterSc.Get();
	TestNamespace1 testns1(master, "ns1");
	const unsigned int n = 101;
	testns1.AddRows(master, 11, n);
	master->MakeLeader();
	TestNamespace1 testns2(master, "ns2");
	testns1.AddRows(master, 10015, n);
	Error err = master->api.reindexer->RenameNamespace("ns2", "ns2Rename");
	ASSERT_TRUE(err.ok()) << err.what();

	ServerControl slaveSc;
	slaveSc.InitServer(ServerControlConfig(0, 7771, 7881, kBaseDbPath + "/slave", "db"));
	auto slave = slaveSc.Get();
	TestNamespace1 testns3(slave, "ns3");
	unsigned int n3 = 1234;
	testns3.AddRows(slave, 5015, n3);
	TestNamespace1 testns4(slave, "ns1");
	std::string upDsn = "cproto://127.0.0.1:7770/db";
	AsyncReplicationConfigTest::NsSet nsSet = {"ns1"};
	// ReplicationConfigTest configSlave("slave", false, true, 0, upDsn, "slave", nsSet);
	// slave->MakeFollower(0, configSlave);

	WaitSync(master, slave, testns1.nsName_);

	err = slave->api.reindexer->RenameNamespace("ns3", "ns3Rename");
	ASSERT_TRUE(err.ok()) << err.what();

	Query qr = Query("ns3Rename").Sort("id", false);
	BaseApi::QueryResultsType res;
	err = slave->api.reindexer->Select(qr, res);
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

	err = slave->api.reindexer->RenameNamespace("ns1", "ns1RenameSlave");
	ASSERT_FALSE(err.ok());

	std::string tmpNsName("tmpNsName");
	NamespaceDef tmpNsDef = NamespaceDef(tmpNsName, StorageOpts().Enabled().CreateIfMissing());
	tmpNsDef.AddIndex("id", "hash", "int", IndexOpts().PK());
	tmpNsDef.isTemporary = true;
	err = master->api.reindexer->AddNamespace(tmpNsDef);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::client::Item item = master->api.NewItem(tmpNsName);
	err = item.FromJSON("{\"id\":" + std::to_string(10) + "}");
	ASSERT_TRUE(err.ok()) << err.what();
	err = master->api.reindexer->Upsert(tmpNsName, item);
	ASSERT_TRUE(err.ok()) << err.what();
	err = master->api.reindexer->RenameNamespace(tmpNsName, tmpNsName + "Rename");
	ASSERT_FALSE(err.ok());

	BaseApi::QueryResultsType r1;
	err = master->api.reindexer->Select("Select * from " + tmpNsName, r1);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(r1.Count() == 1);
}

TEST_F(CascadeReplicationApi, Node3ApplyWal) {
	// Node configuration:
	//			master
	//			  |
	//			slave1
	//            |
	//          slave2
	// Checks applying syncNamespaceByWAL on slave1 and slave2 node.

	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "Node3ApplyWal"));
	const std::string kNsName = "ns1";
	const unsigned int n = 2;
	{
		ServerControl masterSc;
		ServerControl slave1Sc;
		ServerControl slave2Sc;
		masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
		auto master = masterSc.Get();
		master->MakeLeader();
		TestNamespace1 testns1(master, kNsName);
		testns1.AddRows(master, 3000, n);
		// start init of slave
		{
			slave1Sc.InitServer(ServerControlConfig(1, 7771, 7881, kBaseDbPath + "/slave1", "db"));
			slave2Sc.InitServer(ServerControlConfig(2, 7772, 7882, kBaseDbPath + "/slave2", "db"));
			auto slave1 = slave1Sc.Get();
			auto slave2 = slave2Sc.Get();
			slave1->MakeFollower();
			master->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave1->RpcPort()));
			slave2->MakeFollower();
			slave1->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave2->RpcPort()));
			WaitSync(master, slave1, kNsName);
			WaitSync(master, slave2, kNsName);
		}
	}

	{
		ServerControl masterSc;
		masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));
		auto master = masterSc.Get();
		TestNamespace1 testns1(master, kNsName);
		testns1.AddRows(master, 30000, n);
	}
	ServerControl masterSc;
	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db"));

	ServerControl slave1Sc;
	slave1Sc.InitServer(ServerControlConfig(1, 7771, 7881, kBaseDbPath + "/slave1", "db"));

	ServerControl slave2Sc;
	slave2Sc.InitServer(ServerControlConfig(2, 7772, 7882, kBaseDbPath + "/slave2", "db"));

	WaitSync(masterSc.Get(), slave1Sc.Get(), kNsName);
	WaitSync(masterSc.Get(), slave2Sc.Get(), kNsName);
}

static int64_t AwaitUpdatesReplication(const ServerControl::Interface::Ptr& node) {
	auto awaitTime = std::chrono::milliseconds(10000);
	constexpr auto step = std::chrono::milliseconds(100);
	cluster::ReplicationStats stats;
	WrSerializer ser;
	for (; awaitTime.count() > 0; awaitTime -= step) {
		stats = node->GetReplicationStats("async");
		assert(stats.nodeStats.size() == 1);
		ser.Reset();
		stats.GetJSON(ser);
		if (stats.pendingUpdatesCount == 0 && stats.nodeStats[0].updatesCount == 0) {
			return stats.updateDrops;
		}
		std::this_thread::sleep_for(step);
	}
	assertf(false, "Stats: %s", ser.Slice());
	return 0;
}

TEST_F(CascadeReplicationApi, RestrictUpdates) {
	// 1. create master node,
	// 2. set max updates size 1024 * 5 (actual size will be 1024 * 1024)
	// 3. add 5000 rows
	// 4. start slave node
	// 5. insert more (updates will be pended in queue due to force sync)
	// 6. wait sync
	const std::string upDsn = "cproto://127.0.0.1:7770/db";
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "RestrictUpdates"));
	ServerControl masterSc;
	masterSc.InitServer(ServerControlConfig(0, 7770, 7880, kBaseDbPath + "/master", "db", true, 1024 * 5));
	auto master = masterSc.Get();
	master->MakeLeader();

	ServerControl slaveSc;
	slaveSc.InitServer(ServerControlConfig(1, 7771, 7881, kBaseDbPath + "/slave", "db"));
	auto slave = slaveSc.Get();
	slave->MakeFollower();

	TestNamespace1 testns1(master, "ns1");
	testns1.AddRows(master, 0, 2000, 10000);

	const int count = 400;
	const int from = 1000000;
	const std::string nsName("ns1");
	std::string dataString;
	for (size_t i = 0; i < 10000; ++i) {
		dataString.append("xxx");
	}

	master->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", slave->RpcPort()));

	for (unsigned int i = 0; i < count; i++) {
		reindexer::client::Item item = master->api.NewItem("ns1");
		std::string itemJson = fmt::sprintf(R"json({"id": %d, "data": "%s" })json", i + from, dataString);
		auto err = item.Unsafe().FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();
		master->api.Upsert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	WaitSync(master, slave, nsName);

	const auto updatesDrops1 = AwaitUpdatesReplication(master);

	// Make sure, that replication is works fine after updates drop
	testns1.AddRows(master, 0, 100, 2000);
	WaitSync(master, slave, nsName);

	const auto updatesDrops2 = AwaitUpdatesReplication(master);
	if (!updatesDrops2 || !updatesDrops1) {
		// Mark test as skipped, because we didn't got any updates drops
		GTEST_SKIP();
	}
}

#if !defined(REINDEX_WITH_TSAN)
TEST_F(CascadeReplicationApi, ConcurrentForceSync) {
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

	std::vector<ServerControl> nodes;
	auto createFollower = [&kBaseDbPath, &kDbName, &nodes, &kNsList](const ServerPtr& leader) {
		size_t id = nodes.size();
		nodes.push_back(ServerControl());
		nodes.back().InitServer(
			ServerControlConfig(id, kBasePort + id, kBasePort + 1000 + id, kBaseDbPath + "/slave" + std::to_string(id), kDbName));
		AsyncReplicationConfigTest::NsSet nsSet;
		for (size_t i = 0; i < kNsSyncCount; ++i) {
			nsSet.emplace(kNsList[i]);
		}
		auto follower = nodes.back().Get();
		follower->SetReplicationConfig(
			AsyncReplicationConfigTest{"follower", {}, false, true, int(id), "node_" + std::to_string(id), std::move(nsSet)});
		leader->AddFollower(fmt::format("cproto://127.0.0.1:{}/{}", follower->RpcPort(), kDbName));
		return follower;
	};

	// Create leader
	ServerPtr leader;
	{
		nodes.push_back(ServerControl());
		nodes.back().InitServer(ServerControlConfig(0, kBasePort, kBasePort + 1000, kBaseDbPath + "/master", kDbName));
		AsyncReplicationConfigTest::NsSet nsSet;
		for (size_t i = 0; i < kNsSyncCount; ++i) {
			nsSet.emplace(kNsList[i]);
		}
		leader = nodes.back().Get();
		leader->SetReplicationConfig(AsyncReplicationConfigTest{"leader", {}, false, true, 0, "node_0", std::move(nsSet)});
	}

	// Fill leader's data
	const size_t kRows = 10000;
	const size_t kDataBytes = 1000;
	std::vector<TestNamespace1> testNsList;
	for (auto& ns : kNsList) {
		testNsList.emplace_back(nodes[0].Get(), ns);
		testNsList.back().AddRows(nodes[0].Get(), 0, kRows, kDataBytes);
	}

	// Create semileader
	auto semiNode = createFollower(leader);

	// Create slaves
	createFollower(semiNode);
	createFollower(semiNode);

	for (size_t i = 1; i < nodes.size(); i++) {
		for (size_t j = 0; j < kNsSyncCount; ++j) {
			WaitSync(leader, nodes[i].Get(), kNsList[j]);
		}
	}

	// Add one more row to master
	for (auto& ns : testNsList) {
		ns.AddRows(nodes[0].Get(), kRows, 1, kDataBytes);
	}

	for (size_t i = 0; i < kNsSyncCount; ++i) {
		std::vector<std::vector<int>> results;
		for (size_t j = 0; j < nodes.size(); j++) {
			results.push_back(std::vector<int>());
			WaitSync(nodes[0].Get(), nodes[j].Get(), kNsList[i]);
			testNsList[i].GetData(nodes[j].Get(), results.back());
		}

		for (size_t j = 1; j < results.size(); ++j) {
			EXPECT_TRUE((results[0] == results[j]));
		}
	}

	// Allow server to handle disconnects and remove temporary namespaces
	std::this_thread::sleep_for(std::chrono::seconds(2));

	std::vector<std::string> syncNsList(kNsSyncCount);
	std::copy(kNsList.begin(), kNsList.begin() + kNsSyncCount, syncNsList.begin());
	for (size_t i = 0; i < nodes.size(); ++i) {
		if (i == 0) {
			ValidateNsList(nodes[i].Get(), kNsList);
		} else {
			ValidateNsList(nodes[i].Get(), syncNsList);
		}
	}
}
#endif

TEST_F(CascadeReplicationApi, WriteIntoSlaveNsAfterReconfiguration) {
	// Check if it is possible to write in slave's ns after removing this ns from replication ns list
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "WriteIntoSlaveNsAfterReconfiguration/node_"));
	const unsigned int n = 5;
	const int kBasePort = 7770;
	const int kServerId = 5;
	const std::string kNs1 = "ns1";
	const std::string kNs2 = "ns2";
	int manualItemId = 5;
	auto cluster = CreateConfiguration({-1, 0}, kBasePort, kServerId, kBaseDbPath);
	TestNamespace1 testns1(cluster.Get(0), kNs1);
	testns1.AddRows(cluster.Get(0), 0, n);
	TestNamespace1 testns2(cluster.Get(0), kNs2);
	testns2.AddRows(cluster.Get(0), 1, n);

	WaitSync(cluster.Get(0), cluster.Get(1), kNs1);
	WaitSync(cluster.Get(0), cluster.Get(1), kNs2);

	auto createItem = [](const ServerPtr& node, const std::string& ns, int itemId) -> reindexer::client::Item {
		reindexer::client::Item item = node->api.NewItem(ns);
		auto err = item.FromJSON("{\"id\":" + std::to_string(itemId) + "}");
		EXPECT_TRUE(err.ok()) << err.what();
		return item;
	};

	auto item = createItem(cluster.Get(1), kNs1, manualItemId);
	auto err = cluster.Get(1)->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();
	item = createItem(cluster.Get(1), kNs2, manualItemId);
	err = cluster.Get(1)->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();

	// Remove ns1 from replication config
	{
		auto config = cluster.Get(0)->GetServerConfig(ServerControl::ConfigType::Namespace);
		config.namespaces = {kNs2};
		cluster.Get(0)->SetReplicationConfig(config);
		cluster.Get(1)->ResetReplicationRole(kNs1);
		// Await for replicator startup
		testns1.AddRows(cluster.Get(0), 100, n);
		testns2.AddRows(cluster.Get(0), 100, n);
		WaitSync(cluster.Get(0), cluster.Get(1), kNs2);
	}

	item = createItem(cluster.Get(1), kNs1, manualItemId);
	err = cluster.Get(1)->api.reindexer->Upsert(kNs1, item);
	ASSERT_TRUE(err.ok()) << err.what();
	item = createItem(cluster.Get(1), kNs2, manualItemId++);
	err = cluster.Get(1)->api.reindexer->Upsert(kNs2, item);
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();
	WaitSync(cluster.Get(0), cluster.Get(1), kNs2);

	// Restart slave
	cluster.RestartServer(1, kBasePort, kBaseDbPath);

	item = createItem(cluster.Get(1), kNs1, manualItemId);
	err = cluster.Get(1)->api.reindexer->Upsert(kNs1, item);
	ASSERT_TRUE(err.ok()) << err.what();
	item = createItem(cluster.Get(1), kNs2, manualItemId++);
	err = cluster.Get(1)->api.reindexer->Upsert(kNs2, item);
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();

	auto validateItemsCount = [](const ServerPtr& node, const std::string& nsName, size_t expectedCnt) {
		BaseApi::QueryResultsType qr;
		auto err = node->api.reindexer->Select(Query(nsName), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), expectedCnt);
	};
	validateItemsCount(cluster.Get(0), kNs1, 2 * n);
	validateItemsCount(cluster.Get(0), kNs2, 2 * n);
	validateItemsCount(cluster.Get(1), kNs1, n + 2);
	validateItemsCount(cluster.Get(1), kNs2, 2 * n);

	// Enable slave mode for ns1
	{
		AsyncReplicationConfigTest::NsSet nsSet = {kNs1, kNs2};
		auto config = cluster.Get(0)->GetServerConfig(ServerControl::ConfigType::Namespace);
		config.namespaces = {kNs1, kNs2};
		cluster.Get(0)->SetReplicationConfig(config);
		WaitSync(cluster.Get(0), cluster.Get(1), kNs1);
		WaitSync(cluster.Get(0), cluster.Get(1), kNs2);
	}

	item = createItem(cluster.Get(1), kNs1, manualItemId);
	err = cluster.Get(1)->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();
	item = createItem(cluster.Get(1), kNs2, manualItemId);
	err = cluster.Get(1)->api.reindexer->Upsert(kNs1, item);
	ASSERT_EQ(err.code(), errWrongReplicationData) << err.what();

	testns1.AddRows(cluster.Get(0), 200, n);
	testns2.AddRows(cluster.Get(0), 200, n);
	WaitSync(cluster.Get(0), cluster.Get(1), kNs1);
	WaitSync(cluster.Get(0), cluster.Get(1), kNs2);
	validateItemsCount(cluster.Get(0), kNs1, 3 * n);
	validateItemsCount(cluster.Get(0), kNs2, 3 * n);
	validateItemsCount(cluster.Get(1), kNs1, 3 * n);
	validateItemsCount(cluster.Get(1), kNs2, 3 * n);
}

static void AwaitFollowersState(const ServerControl::Interface::Ptr& node, cluster::NodeStats::Status expectedStatus,
								cluster::NodeStats::SyncState expectedSyncState) {
	constexpr std::chrono::milliseconds step{100};
	std::chrono::milliseconds awaitTime{10000};
	WrSerializer wser;
	while (awaitTime.count() > 0) {
		auto stats = node->GetReplicationStats(cluster::kAsyncReplStatsType);
		wser.Reset();
		stats.GetJSON(wser);
		ASSERT_EQ(stats.nodeStats.size(), 1) << wser.Slice();
		ASSERT_EQ(stats.nodeStats[0].role, cluster::RaftInfo::Role::Follower) << wser.Slice();
		if (stats.nodeStats[0].status == expectedStatus && stats.nodeStats[0].syncState == expectedSyncState) {
			return;
		}

		std::this_thread::sleep_for(step);
		awaitTime -= step;
	}
	ASSERT_TRUE(false) << "Timeout: " << wser.Slice();
}

TEST_F(CascadeReplicationApi, FollowerNetworkAndSyncStatus) {
	// Check if network and sync status of the follower depends on actual follower's state
	const std::string kBaseDbPath(fs::JoinPath(kBaseTestsetDbPath, "WriteIntoSlaveNsAfterReconfiguration/node_"));
	const unsigned int n = 5;
	const int kBasePort = 7770;
	const int kServerId = 5;
	const std::string kNs1 = "ns1";
	auto cluster = CreateConfiguration({-1, 0}, kBasePort, kServerId, kBaseDbPath);
	TestNamespace1 testns1(cluster.Get(0), kNs1);
	testns1.AddRows(cluster.Get(0), 0, n);
	WaitSync(cluster.Get(0), cluster.Get(1), kNs1);

	AwaitFollowersState(cluster.Get(0), cluster::NodeStats::Status::Online, cluster::NodeStats::SyncState::OnlineReplication);

	cluster.ShutdownServer(1);
	AwaitFollowersState(cluster.Get(0), cluster::NodeStats::Status::Offline, cluster::NodeStats::SyncState::AwaitingResync);
}
