#include "cluster/consts.h"
#include "cluster_operation_api.h"
#include "estl/mutex.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "yaml-cpp/yaml.h"

using namespace reindexer;

TEST_F(ClusterOperationApi, LeaderElections) {
	// Check leader election on deffirent conditions
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports);
		// Await leader and make sure, that it will be elected only once
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime, true);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Terminating servers..." << std::endl;
		cluster.StopServers(0, kClusterSize);

		TestCout() << "Launch half of the servers..." << std::endl;
		// Launch half of the servers (no consensus)
		for (size_t i = 0; i < kClusterSize / 2; ++i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_EQ(leaderId, -1);

		TestCout() << "Now we should have consensus..." << std::endl;
		// Now we should have consensus
		ASSERT_TRUE(cluster.StartServer(kClusterSize / 2));
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);

		TestCout() << "Launch rest of the nodes..." << std::endl;
		// Launch rest of the nodes
		for (size_t i = kClusterSize / 2 + 1; i < kClusterSize; ++i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		auto newLeaderId = cluster.AwaitLeader(kMaxElectionsTime, true);
		ASSERT_EQ(leaderId, newLeaderId);

		TestCout() << "Stop nodes without cluster fail..." << std::endl;
		// Stop nodes without cluster fail
		auto safeToRemoveCnt = kClusterSize - (kClusterSize / 2) - 1;
		cluster.StopServers(0, safeToRemoveCnt);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);

		TestCout() << "Remove one more node (cluster should not be able to choose the leader)..." << std::endl;
		// Remove one more node (cluster should not be able to choose the leader)
		ASSERT_TRUE(cluster.StopServer(safeToRemoveCnt));
		loop.sleep(std::chrono::seconds(5));
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, true);
		ASSERT_EQ(leaderId, -1);
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, TransactionTimeout) {
	// Checking correct timeout processing when creating transaction on cluster without consensus
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 3;
		constexpr size_t kNodesCountToStart = 1;
		const std::string_view kNsSome = "some";

		TestCout() << "Starting cluster with one cluster node of " << kClusterSize << "..." << std::endl;
		auto config = Cluster::CreateClusterConfigStatic(0, kClusterSize, ports);
		Cluster cluster(loop, 0, kNodesCountToStart, ports, 0UL, config);
		// Nodes startup is checked during cluster initialization

		TestCout() << "Attempting to create new transaction. Attempt timeout: 1 sec" << std::endl;
		auto start = steady_clock_w::now();
		auto db = cluster.GetNode(0)->api.reindexer->WithTimeout(std::chrono::seconds(1));
		auto tx = db.NewTransaction(kNsSome);
		const auto& err = tx.Status();
		ASSERT_EQ(err.code(), errTimeout) << err;
		auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(steady_clock_w::now() - start).count();
		auto thresholdSec = std::chrono::seconds(5).count();
		ASSERT_LE(elapsedSec, thresholdSec);

		TestCout() << "Done" << std::endl;
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, OnlineUpdates) {
	// Check basic online replication in cluster
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);

		const auto kNodeIdWithStats = (leaderId + 1) % kClusterSize;
		cluster.EnablePerfStats(kNodeIdWithStats);

		// Create namespace and fill initial data
		const std::string_view kNsSome = "some";
		constexpr size_t kDataPortion = 100;
		TestCout() << "Init NS" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		TestCout() << "Fill data" << std::endl;
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);
		TestCout() << "Wait sync" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop node, fill more data and await sync
		for (size_t i = 0; i < (kClusterSize) / 2; ++i) {
			TestCout() << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
			TestCout() << "Await leader" << std::endl;
			leaderId = cluster.AwaitLeader(kMaxElectionsTime);
			ASSERT_NE(leaderId, -1);
			TestCout() << "Fill data" << std::endl;
			cluster.FillData(leaderId, kNsSome, (i + 1) * kDataPortion, kDataPortion / 2);
			cluster.FillDataTx(leaderId, kNsSome, (i + 1) * kDataPortion + kDataPortion / 2, kDataPortion / 2);
			TestCout() << "Wait sync" << std::endl;
			cluster.WaitSync(kNsSome);
		}
		TestCout() << "Done" << std::endl;
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, ForceAndWalSync) {
	// Check full cluster synchronization via all the available mechanisms
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 7;
		Cluster cluster(loop, 0, kClusterSize, ports);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		const std::string kNsSome = "some";
		constexpr size_t kDataPortion = 100;

		// Fill data for N/2 + 1 nodes
		cluster.StopServers(0, kClusterSize / 2);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		TestCout() << "Fill data 1" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);

		{
			// Some update request with row-based replication mode
			auto node = cluster.GetNode(leaderId);
			BaseApi::QueryResultsType qr;
			Query q =
				Query(kNsSome).Where(kIdField, CondGe, int(10)).Where(kIdField, CondLe, int(12)).Set(kStringField, randStringAlph(15));
			auto err = node->api.reindexer->Update(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		// Check if the data were replicated after nodes restart
		for (size_t i = 0; i < (kClusterSize) / 2; ++i) {
			TestCout() << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		TestCout() << "Wait sync 1" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop half of the nodes again
		cluster.StopServers(kClusterSize / 2 + 1, kClusterSize);

		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		// Save sync stats
		auto leader = cluster.GetNode(leaderId);
		auto stats = leader->GetReplicationStats(cluster::kClusterReplStatsType);
		const auto forceSyncs = stats.forceSyncs.count;
		const auto walSyncs = stats.walSyncs.count;

		// Fill data and then start node. Repeat until all nodes in the cluster are alive
		TestCout() << "Fill data 2" << std::endl;
		cluster.FillData(leaderId, kNsSome, kDataPortion, kDataPortion);
		bool indexUpdated = false;
		for (size_t i = (kClusterSize) / 2 + 1; i < kClusterSize; ++i) {
			TestCout() << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
			if (!indexUpdated) {
				// Check special case for index update in WAL from issue #2136
				indexUpdated = true;
				auto err = leader->api.reindexer->UpdateIndex(
					kNsSome,
					reindexer::IndexDef{std::string(kIdField), {std::string(kIdField)}, "hash", "int", IndexOpts().PK().NoIndexColumn()});
				EXPECT_TRUE(err.ok()) << err.what();
				TestCout() << "Wait sync 2" << std::endl;
				cluster.WaitSync(kNsSome);
			}
			TestCout() << "Fill more" << std::endl;
			cluster.FillData(leaderId, kNsSome, (i + 2) * kDataPortion, kDataPortion / 2);
			cluster.FillDataTx(leaderId, kNsSome, (i + 2) * kDataPortion + kDataPortion / 2, kDataPortion / 2);
		}
		TestCout() << "Wait sync 3" << std::endl;
		cluster.WaitSync(kNsSome);

		// Validate sync stats
		stats = leader->GetReplicationStats(cluster::kClusterReplStatsType);
		EXPECT_EQ(forceSyncs, stats.forceSyncs.count);	// No new force syncs
		EXPECT_LT(walSyncs, stats.walSyncs.count);		// Few new wal syncs
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, TruncateForceAndWalSync) {
	// Check for truncate WAL-replication
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		const std::string kNsSome = "some";
		constexpr size_t kDataPortion = 100;

		// Stopping 1 server to initiate force sync later
		cluster.StopServer(0);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		TestCout() << "Fill data" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);

		TestCout() << "Wait sync 1" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stopping another server to initiate WAL sync
		cluster.StopServer(1);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		TestCout() << "Wait sync 2" << std::endl;
		cluster.WaitSync(kNsSome);

		auto err = cluster.GetNode(leaderId)->api.reindexer->TruncateNamespace(kNsSome);
		ASSERT_TRUE(err.ok());
		TestCout() << "Wait sync on running nodes" << std::endl;
		cluster.WaitSync(kNsSome);
		cluster.StartServer(0);
		cluster.StartServer(1);
		TestCout() << "Wait final sync" << std::endl;
		cluster.WaitSync(kNsSome);
	}));

	loop.run();
}

// clang-format off
#define O_EXPECT_EQ(enable_assertion, val1, val2)    \
	if (enable_assertion) { EXPECT_EQ(val1, val2); } \
	if ((val1) != (val2)) { return false; }

#define O_EXPECT_GT(enable_assertion, val1, val2)    \
	if (enable_assertion) { EXPECT_GT(val1, val2); } \
	if ((val1) <= (val2)) { return false; }

#define O_EXPECT_GE(enable_assertion, val1, val2)    \
	if (enable_assertion) { EXPECT_GE(val1, val2); } \
	if ((val1) < (val2)) { return false; }

#define O_EXPECT_LE(enable_assertion, val1, val2)    \
	if (enable_assertion) { EXPECT_LE(val1, val2); } \
	if ((val1) > (val2)) { return false; }

#define O_EXPECT_TRUE(enable_assertion, val)    \
	if (enable_assertion) { EXPECT_TRUE(val); } \
	if (!(val)) { return false; }
// clang-format on

static bool ValidateStatsOnTestBeginning(size_t kClusterSize, const ClusterOperationApi::Defaults& ports, const std::set<int>& onlineNodes,
										 const cluster::ReplicationStats& stats, bool withAssertion) {
	WrSerializer wser;
	stats.GetJSON(wser);

	O_EXPECT_EQ(withAssertion, stats.initialSync.walSyncs.count, 0)
	O_EXPECT_GT(withAssertion, stats.initialSync.totalTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.walSyncs.maxTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.walSyncs.avgTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.forceSyncs.count, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.forceSyncs.maxTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.forceSyncs.avgTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.forceSyncs.count, 0)
	O_EXPECT_EQ(withAssertion, stats.walSyncs.count, 0)
	O_EXPECT_EQ(withAssertion, stats.nodeStats.size(), kClusterSize)

	bool hasUpdates = false;
	for (auto& nodeStat : stats.nodeStats) {
		const DSN kExpectedDsn = MakeDsn(reindexer_server::UserRole::kRoleReplication, nodeStat.serverId,
										 ports.defaultRpcPort + nodeStat.serverId, "node" + std::to_string(nodeStat.serverId));
		O_EXPECT_EQ(withAssertion, nodeStat.dsn, kExpectedDsn)
		if (nodeStat.updatesCount > 0) {
			hasUpdates = true;
			break;
		}
		O_EXPECT_EQ(withAssertion, nodeStat.updatesCount, 0)
		O_EXPECT_TRUE(withAssertion, nodeStat.namespaces.empty())
		if (onlineNodes.count(nodeStat.serverId)) {
			O_EXPECT_EQ(withAssertion, nodeStat.status, cluster::NodeStats::Status::Online)
			O_EXPECT_EQ(withAssertion, nodeStat.syncState, cluster::NodeStats::SyncState::OnlineReplication)
		} else {
			O_EXPECT_EQ(withAssertion, nodeStat.status, cluster::NodeStats::Status::Offline)
			O_EXPECT_EQ(withAssertion, nodeStat.syncState, cluster::NodeStats::SyncState::AwaitingResync)
		}
	}
	return !hasUpdates;
}

static bool ValidateStatsOnTestBeginning(size_t kClusterSize, const ClusterOperationApi::Defaults& ports, const std::set<int>& onlineNodes,
										 const ServerControl::Interface::Ptr& node) {
	constexpr auto kMaxRetries = 40;
	cluster::ReplicationStats stats;
	for (int i = 0; i < kMaxRetries; ++i) {
		stats = node->GetReplicationStats(cluster::kClusterReplStatsType);
		if (ValidateStatsOnTestBeginning(kClusterSize, ports, onlineNodes, stats, false)) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
	}

	if (ValidateStatsOnTestBeginning(kClusterSize, ports, onlineNodes, stats, true)) {
		return true;
	}
	WrSerializer wser;
	stats.GetJSON(wser);
	TestCout() << "Stats json: " << wser.Slice() << std::endl;
	return false;
}

static bool ValidateStatsOnTestEnding(int leaderId, int kTransitionServer, const std::vector<size_t>& kFirstNodesGroup,
									  const std::vector<size_t>& kSecondNodesGroup, size_t kClusterSize,
									  const ClusterOperationApi::Defaults& ports, const cluster::ReplicationStats& stats,
									  bool withAssertion) {
	WrSerializer wser;
	stats.GetJSON(wser);
	O_EXPECT_EQ(withAssertion, stats.initialSync.walSyncs.count, 1)
	O_EXPECT_GT(withAssertion, stats.initialSync.totalTimeUs, 0)
	O_EXPECT_GT(withAssertion, stats.initialSync.walSyncs.maxTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.walSyncs.maxTimeUs, stats.initialSync.walSyncs.avgTimeUs)
	O_EXPECT_EQ(withAssertion, stats.initialSync.forceSyncs.count, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.forceSyncs.maxTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.initialSync.forceSyncs.avgTimeUs, 0)
	O_EXPECT_EQ(withAssertion, stats.forceSyncs.count, 0)
	O_EXPECT_EQ(withAssertion, stats.logLevel, LogTrace)
	if (leaderId == kTransitionServer) {
		O_EXPECT_EQ(withAssertion, stats.walSyncs.count, kFirstNodesGroup.size())
	} else {
		O_EXPECT_GE(withAssertion, stats.walSyncs.count, kFirstNodesGroup.size() - 1)
		O_EXPECT_LE(withAssertion, stats.walSyncs.count, kFirstNodesGroup.size())
	}
	O_EXPECT_EQ(withAssertion, stats.nodeStats.size(), kClusterSize) {
		std::set<size_t> onlineNodes, offlineNodes;
		for (auto node : kSecondNodesGroup) {
			offlineNodes.emplace(node);
		}
		for (auto node : kFirstNodesGroup) {
			onlineNodes.emplace(node);
		}
		onlineNodes.emplace(kTransitionServer);
		for (auto& nodeStat : stats.nodeStats) {
			const DSN kExpectedDsn = MakeDsn(reindexer_server::UserRole::kRoleReplication, nodeStat.serverId,
											 ports.defaultRpcPort + nodeStat.serverId, "node" + std::to_string(nodeStat.serverId));
			O_EXPECT_EQ(withAssertion, nodeStat.dsn, kExpectedDsn)
			O_EXPECT_EQ(withAssertion, nodeStat.updatesCount, 0)
			if (onlineNodes.count(nodeStat.serverId)) {
				O_EXPECT_EQ(withAssertion, onlineNodes.count(nodeStat.serverId), 1)
				O_EXPECT_EQ(withAssertion, nodeStat.status, cluster::NodeStats::Status::Online)
				O_EXPECT_EQ(withAssertion, nodeStat.syncState, cluster::NodeStats::SyncState::OnlineReplication)
				O_EXPECT_EQ(withAssertion, nodeStat.isSynchronized, true)
				O_EXPECT_EQ(withAssertion, nodeStat.lastError, Error())
				if (nodeStat.serverId == leaderId) {
					O_EXPECT_EQ(withAssertion, nodeStat.role, cluster::RaftInfo::Role::Leader)
				} else {
					O_EXPECT_EQ(withAssertion, nodeStat.role, cluster::RaftInfo::Role::Follower)
				}
				onlineNodes.erase(nodeStat.serverId);
			} else if (offlineNodes.count(nodeStat.serverId)) {
				O_EXPECT_EQ(withAssertion, offlineNodes.count(nodeStat.serverId), 1)
				O_EXPECT_EQ(withAssertion, nodeStat.status, cluster::NodeStats::Status::Offline)
				O_EXPECT_EQ(withAssertion, nodeStat.syncState, cluster::NodeStats::SyncState::AwaitingResync)
				O_EXPECT_EQ(withAssertion, nodeStat.role, cluster::RaftInfo::Role::None)
				O_EXPECT_EQ(withAssertion, nodeStat.isSynchronized, false)
				O_EXPECT_EQ(withAssertion, nodeStat.lastError.code(), errNetwork);
				offlineNodes.erase(nodeStat.serverId);
			} else {
				EXPECT_TRUE(false) << "Unexpected server id: " << nodeStat.serverId << "; json: " << wser.Slice();
				return false;
			}
		}
		O_EXPECT_EQ(withAssertion, onlineNodes.size(), 0)
		O_EXPECT_EQ(withAssertion, offlineNodes.size(), 0)
	}
	return true;
}

static cluster::ReplicationStats& nullifyUpdatesStats(cluster::ReplicationStats& stats) {
	stats.pendingUpdatesCount = 0;
	stats.allocatedUpdatesCount = 0;
	stats.allocatedUpdatesSizeBytes = 0;
	for (auto& node : stats.nodeStats) {
		node.updatesCount = 0;
		node.lastError = Error();
	}
	return stats;
}

TEST_F(ClusterOperationApi, InitialLeaderSync) {
	// Check if new leader is able to get newest data from other nodes after elections
	net::ev::dynamic_loop loop;
	const auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 7;
		const std::vector<size_t> kFirstNodesGroup = {0, 1, 2};	  // Servers to stop after data fill
		const size_t kTransitionServer = 3;						  // The only server, which will have actual data. It belongs to both groups
		const std::vector<size_t> kSecondNodesGroup = {4, 5, 6};  // Empty servers, which have to perfomr sync
		Cluster cluster(loop, 0, kClusterSize, ports);

		const std::string_view kNsSome = "some";
		constexpr size_t kDataPortion = 100;

		// Check force initial sync for nodes from the second group
		cluster.StopServers(kSecondNodesGroup);

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;
		std::set<int> onlineNodes = {kTransitionServer};
		for (auto id : kFirstNodesGroup) {
			onlineNodes.emplace(id);
		}
		ASSERT_TRUE(ValidateStatsOnTestBeginning(kClusterSize, ports, onlineNodes, cluster.GetNode(leaderId)));

		// Fill data for nodes from the first group
		TestCout() << "Fill data 1" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);
		TestCout() << "Wait sync 1" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop cluster
		cluster.StopServers(kFirstNodesGroup);
		TestCout() << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start second group. kTransitionServer is the only node with data from previous step
		for (auto i : kSecondNodesGroup) {
			TestCout() << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		TestCout() << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

		TestCout() << "Wait sync 2" << std::endl;
		cluster.WaitSync(kNsSome);

		// Make sure, that our cluster didn't miss it's data in process
		auto state = cluster.GetNode(kClusterSize - 1)->GetState(std::string(kNsSome));
		ASSERT_EQ(state.lsn.Counter(), kDataPortion + 4);

		// Check WAL initial sync for nodes from the first group
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		// Fill additional data for second group
		TestCout() << "Fill data 2" << std::endl;
		cluster.FillDataTx(leaderId, kNsSome, 150, kDataPortion);
		cluster.FillData(leaderId, kNsSome, 50, kDataPortion);

		// Stop cluster
		cluster.StopServers(kSecondNodesGroup);
		TestCout() << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start first group againt and make sure, that all of the data were replicated from transition node
		for (auto i : kFirstNodesGroup) {
			TestCout() << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		TestCout() << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

		TestCout() << "Wait sync 3" << std::endl;
		cluster.WaitSync(kNsSome);

		state = cluster.GetNode(0)->GetState(std::string(kNsSome));
		ASSERT_EQ(state.lsn.Counter(), 3 * kDataPortion + 4 + 2);  // Data + indexes + tx records

		// Validate stats
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;
		cluster::ReplicationStats stats;
		for (unsigned i = 0; i < 40; ++i) {
			stats = cluster.GetNode(leaderId)->GetReplicationStats(cluster::kClusterReplStatsType);
			if (ValidateStatsOnTestEnding(leaderId, kTransitionServer, kFirstNodesGroup, kSecondNodesGroup, kClusterSize, ports, stats,
										  false)) {
				break;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(250));
		}
		WrSerializer serLeader, serFollower;
		stats.GetJSON(serLeader);
		SCOPED_TRACE(fmt::format("Leader's stats: {}", serLeader.Slice()));
		{
			ASSERT_TRUE(ValidateStatsOnTestEnding(leaderId, kTransitionServer, kFirstNodesGroup, kSecondNodesGroup, kClusterSize, ports,
												  stats, true));
		}
		// Validate stats, recieved via followers (this request has to be proxied)
		for (auto nodeId : kFirstNodesGroup) {
			serFollower.Reset();
			auto followerStats = cluster.GetNode(nodeId)->GetReplicationStats(cluster::kClusterReplStatsType);
			followerStats.GetJSON(serFollower);
			SCOPED_TRACE(fmt::format("Follower's stats: {}", serFollower.Slice()));
			EXPECT_EQ(nullifyUpdatesStats(stats), nullifyUpdatesStats(followerStats)) << "Server id: " << nodeId;
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, InitialLeaderSyncWithConcurrentSnapshots) {
	// Check if new leader is able to get newest data from other nodes after elections and handle max snapshots limit correctly
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 5;
		const std::vector<size_t> kFirstNodesGroup = {0, 1};   // Servers to stop after data fill
		const size_t kTransitionServer = 2;					   // The only server, which will have actual data. It belongs to both groups
		const std::vector<size_t> kSecondNodesGroup = {3, 4};  // Empty servers, which have to perfomr sync
		Cluster cluster(loop, 0, kClusterSize, ports, std::vector<std::string>(), std::chrono::milliseconds(3000), -1, 1);

		const std::vector<std::string> kNsNames = {"ns1", "ns2", "ns3", "ns4", "ns5", "ns6", "ns7", "ns8", "ns9", "ns10"};
		constexpr size_t kDataPortion = 20;

		// Check force initial sync for nodes from the second group
		cluster.StopServers(kSecondNodesGroup);

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		// Fill data for nodes from the first group
		TestCout() << "Fill data 1" << std::endl;
		for (auto& ns : kNsNames) {
			cluster.InitNs(leaderId, ns);
			cluster.FillData(leaderId, ns, 0, kDataPortion);
		}
		TestCout() << "Wait sync 1" << std::endl;
		for (auto& ns : kNsNames) {
			cluster.WaitSync(ns);
		}

		// Stop cluster
		cluster.StopServers(kFirstNodesGroup);
		TestCout() << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start second group. kTransitionServer is the only node with data from previous step
		for (auto i : kSecondNodesGroup) {
			TestCout() << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		TestCout() << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

#ifdef RX_LONG_REPLICATION_TIMEOUT
		// On Github CI OSX runners ns renames are really slow sometimes
		const std::chrono::seconds syncTime = std::chrono::seconds(45);
#else
		const std::chrono::seconds syncTime = kMaxSyncTime;
#endif

		TestCout() << "Wait sync 2" << std::endl;
		for (auto& ns : kNsNames) {
			cluster.WaitSync(ns, lsn_t(), lsn_t(), syncTime);
		}

		// Make sure, that our cluster didn't miss it's data in process
		for (auto& ns : kNsNames) {
			auto state = cluster.GetNode(kClusterSize - 1)->GetState(ns);
			ASSERT_EQ(state.lsn.Counter(), kDataPortion + 4);
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, MultithreadSyncTest) {
	// Check full cluster synchronization via all the available mechanisms
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports, std::vector<std::string>(), std::chrono::milliseconds(200));

		const std::vector<std::string> kNsNames = {"ns1", "ns2", "ns3"};
		std::vector<std::thread> threads;
		constexpr size_t kDataPortion = 100;
		constexpr size_t kMaxDataId = 10000;
		std::atomic<bool> terminate = {false};

		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		auto dataFillF = ([&cluster, &terminate, leaderId](const std::string& ns) {
			while (!terminate) {
				constexpr size_t kDataPart = kDataPortion / 10;
				for (size_t i = 0; i < 10; ++i) {
					auto idx = rand() % kMaxDataId;
					cluster.FillData(leaderId, ns, idx, kDataPart);
					std::this_thread::sleep_for(std::chrono::milliseconds(20));
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		});
		auto txF = ([&cluster, &terminate, leaderId](const std::string& ns) {
			try {
				while (!terminate) {
					auto node = cluster.GetNode(leaderId);
					auto tx = node->api.reindexer->NewTransaction(ns);
					ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();

					constexpr size_t kDataPart = kDataPortion / 5;
					for (size_t i = 0; i < kDataPart; ++i) {
						auto idx = rand() % kMaxDataId;
						auto item = tx.NewItem();
						cluster.FillItem(node->api, item, idx);
						auto err = tx.Upsert(std::move(item));
						ASSERT_TRUE(err.ok()) << err.what();
					}
					for (size_t i = 0; i < 2; ++i) {
						int minIdx = rand() % kMaxDataId;
						int maxIdx = minIdx + 100;
						Query q =
							Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx).Set(kStringField, randStringAlph(25));
						q.type_ = QueryUpdate;
						auto err = tx.Modify(std::move(q));
						ASSERT_TRUE(err.ok()) << err.what();
					}
					for (size_t i = 0; i < 2; ++i) {
						int minIdx = rand() % kMaxDataId;
						int maxIdx = minIdx + 100;
						Query q = Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx);
						q.type_ = QueryDelete;
						auto err = tx.Modify(std::move(q));
						ASSERT_TRUE(err.ok()) << err.what();
					}
					BaseApi::QueryResultsType qrTx;
					auto err = node->api.reindexer->CommitTransaction(tx, qrTx);
					ASSERT_TRUE(err.ok()) << err.what();

					std::this_thread::sleep_for(std::chrono::milliseconds(50));
				}
			} catch (Error& e) {
				ASSERT_TRUE(false) << e.what();
			}
		});
		auto selectF = ([&cluster, &terminate, leaderId](const std::string& ns) {
			unsigned counter = 0;
			while (!terminate) {
				if (counter++ % 2 == 0) {
					// FT request, which must be valid
					auto node = cluster.GetNode(leaderId);
					BaseApi::QueryResultsType qr;
					Query q = Query(ns).Where(kFTField, CondEq, "text");
					auto err = node->api.reindexer->Select(q, qr);
					ASSERT_TRUE(err.ok()) << err.what();
				} else {
					// Select from random node (node may be offline)
					auto id = rand() % kClusterSize;
					auto node = cluster.GetNode(id);
					if (node) {
						BaseApi::QueryResultsType qr;
						auto err = node->api.reindexer->Select(Query(ns), qr);
						(void)err;	// errors are acceptable here
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(30));
			}
		});
		auto updateF = ([&cluster, &terminate, leaderId](const std::string& ns) {
			std::vector<std::string> newFields;
			while (!terminate) {
				int minIdx = rand() % kMaxDataId;
				int maxIdx = minIdx + 300;
				{
					auto node = cluster.GetNode(leaderId);
					BaseApi::QueryResultsType qr;
					Query q =
						Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx).Set(kStringField, randStringAlph(15));
					auto err = node->api.reindexer->Update(q, qr);
					ASSERT_TRUE(err.ok()) << err.what();
				}
				if (newFields.size() >= 20) {
					auto node = cluster.GetNode(leaderId);
					BaseApi::QueryResultsType qr;
					Query q = Query(ns);
					for (size_t i = 0; i < newFields.size(); ++i) {
						q.Drop(newFields[i]);
					}
					auto err = node->api.reindexer->Update(q, qr);
					ASSERT_TRUE(err.ok()) << err.what();
					newFields.clear();
				}
				{
					newFields.emplace_back(randStringAlph(20));
					auto node = cluster.GetNode(leaderId);
					BaseApi::QueryResultsType qr;
					Query q =
						Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx).Set(newFields.back(), randStringAlph(15));
					auto err = node->api.reindexer->Update(q, qr);
					ASSERT_TRUE(err.ok()) << err.what();
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		});
		auto deleteF = ([&cluster, &terminate, leaderId](const std::string& ns) {
			while (!terminate) {
				int minIdx = rand() % kMaxDataId;
				int maxIdx = minIdx + 200;
				auto node = cluster.GetNode(leaderId);
				BaseApi::QueryResultsType qr;
				Query q = Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx);
				auto err = node->api.reindexer->Delete(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
		});

		// Create few different thread for each namespace
		for (auto& ns : kNsNames) {
			cluster.InitNs(leaderId, ns);

			threads.emplace_back(dataFillF, std::ref(ns));
			threads.emplace_back(dataFillF, std::ref(ns));
			threads.emplace_back(txF, std::ref(ns));
			threads.emplace_back(updateF, std::ref(ns));
			threads.emplace_back(deleteF, std::ref(ns));
			threads.emplace_back(selectF, std::ref(ns));
		}

		// Restart followers
		std::vector<size_t> stopped;
		{
			int i = 0;
			while (stopped.size() < kClusterSize / 2) {
				if (i != leaderId) {
					std::this_thread::sleep_for(std::chrono::milliseconds(1500));
					ASSERT_TRUE(cluster.StopServer(i));
					stopped.emplace_back(i);
				}
				++i;
			}
		}
		for (auto i : stopped) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1500));
			ASSERT_TRUE(cluster.StartServer(i));
		}

		terminate = true;
		for (auto& th : threads) {
			th.join();
		}

		// Make shure, that cluster is synchronized
		for (auto& ns : kNsNames) {
			TestCout() << "Wait sync for " << ns << std::endl;
			cluster.WaitSync(ns);
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, NamespaceOperationsOnlineReplication) {
	// Check if Add/Rename/Drop namespace are replicating correctly in concurrent environment
	constexpr size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);

		const std::string kBaseNsName = "ns_name";
		std::vector<std::thread> threads;
		constexpr size_t kBaseDataPortion = 10;

		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		std::vector<NamespaceData> existingNamespaces;
		reindexer::mutex mtx;
		auto nsOperationsSequenceF = ([&cluster, leaderId, &existingNamespaces, &mtx, &kBaseNsName](size_t tid) {
			size_t id = 1;
			{
				// Add and fill namespace
				const std::string kNsName = kBaseNsName + "_" + std::to_string(tid) + "_" + std::to_string(id);
				cluster.InitNs(leaderId, kNsName);
				const auto dataCount = kBaseDataPortion * (1 + tid) + id;
				cluster.FillData(leaderId, kNsName, 0, dataCount);
				const auto state = cluster.GetNode(leaderId)->GetState(kNsName);
				ASSERT_EQ(state.nsVersion.Server(), leaderId);
				lock_guard lck(mtx);
				existingNamespaces.emplace_back(NamespaceData{kNsName, lsn_t(dataCount + 4, leaderId), state.nsVersion});
			}
			++id;
			{
				// Add, fill and drop namespace
				const std::string kNsName = kBaseNsName + "_" + std::to_string(tid) + "_" + std::to_string(id);
				cluster.InitNs(leaderId, kNsName);
				const auto dataCount = kBaseDataPortion * (1 + tid) + id;
				cluster.FillData(leaderId, kNsName, 0, dataCount);
				auto err = cluster.GetNode(leaderId)->api.reindexer->DropNamespace(kNsName);
				ASSERT_TRUE(err.ok()) << err.what();
			}
			++id;
			{
				// Add, fill, drop and add again
				const std::string kNsName = kBaseNsName + "_" + std::to_string(tid) + "_" + std::to_string(id);
				cluster.InitNs(leaderId, kNsName);
				const auto dataCount = kBaseDataPortion * (1 + tid) + id;
				cluster.FillData(leaderId, kNsName, 0, dataCount);
				auto err = cluster.GetNode(leaderId)->api.reindexer->DropNamespace(kNsName);
				ASSERT_TRUE(err.ok()) << err.what();
				cluster.InitNs(leaderId, kNsName);
				cluster.FillData(leaderId, kNsName, 0, dataCount);
				const auto state = cluster.GetNode(leaderId)->GetState(kNsName);
				ASSERT_EQ(state.nsVersion.Server(), leaderId);
				lock_guard lck(mtx);
				existingNamespaces.emplace_back(NamespaceData{kNsName, lsn_t(dataCount + 4, leaderId), state.nsVersion});
			}
		});

		threads.emplace_back(nsOperationsSequenceF, 0);
		threads.emplace_back(nsOperationsSequenceF, 1);
		threads.emplace_back(nsOperationsSequenceF, 2);

		for (auto& th : threads) {
			th.join();
		}

		// Make shure, that cluster is synchronized
		cluster.ValidateNamespaceList(existingNamespaces);
	}));

	loop.run();
}

TEST_F(ClusterOperationApi, NamespaceVersioning) {
	// Check if new leader will get the latest namespace version if multiple versions are available
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports);

		const std::string_view kNsName = "some";
		constexpr size_t kDataPortion1 = 30;
		constexpr size_t kDataPortion2 = 20;
		constexpr size_t kDataPortion3 = 10;

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;
		size_t id = 0;

		ReplicationTestState latestNsState;
		auto CreateAndDropNsFn = [&leaderId, &cluster, kNsName, &latestNsState](size_t dataCount, size_t stopId, bool drop,
																				bool awaitLeader) {
			// Create ns and fill data
			TestCout() << "Fill data" << std::endl;
			cluster.InitNs(leaderId, kNsName);
			cluster.FillData(leaderId, kNsName, 0, dataCount);
			TestCout() << "Wait sync" << std::endl;
			cluster.WaitSync(kNsName);
			latestNsState = cluster.GetNode(leaderId)->GetState(std::string(kNsName));
			// Stop one of the nodes
			TestCout() << "Stopping " << stopId << std::endl;
			EXPECT_TRUE(cluster.StopServer(stopId));
			if (awaitLeader) {
				leaderId = cluster.AwaitLeader(kMaxElectionsTime);
				EXPECT_NE(leaderId, -1);
				TestCout() << "Leader id is " << leaderId << std::endl;
			}
			if (drop) {
				cluster.DropNs(leaderId, kNsName);
			}
		};

		// Stop one of the nodes with ns version 1
		CreateAndDropNsFn(kDataPortion1, id++, true, true);
		// Stop one of the nodes with ns version 2
		CreateAndDropNsFn(kDataPortion2, id++, true, true);
		// Stop one of the nodes with ns version 3 and get latest ns state
		CreateAndDropNsFn(kDataPortion3, id++, false, false);
		const auto kLastRestartId = id + 1;
		for (size_t i = id; i < kClusterSize; ++i) {
			TestCout() << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
		}

		for (size_t i = 0; i < kLastRestartId; ++i) {
			TestCout() << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Wait final sync" << std::endl;
		cluster.WaitSync(kNsName);
		const auto state = cluster.GetNode(leaderId)->GetState(std::string(kNsName));
		ASSERT_EQ(latestNsState.lsn, state.lsn);
		ASSERT_EQ(latestNsState.nsVersion, state.nsVersion);
		ASSERT_EQ(latestNsState.dataHash, state.dataHash);
		ASSERT_EQ(latestNsState.dataCount, state.dataCount);
	}));

	loop.run();
}
