#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include "client/queryresults.h"
#include "client/raftclient.h"
#include "clusterization_api.h"
#include "core/cjson/jsonbuilder.h"

TEST_F(ClusterizationApi, LeaderElections) {
	// Check leader election on deffirent conditions
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports);
		// Await leader and make sure, that it will be elected only once
		auto leaderId = cluster.AwaitLeader(std::chrono::seconds(5), true);
		ASSERT_NE(leaderId, -1);
		std::cout << "Terminating servers..." << std::endl;
		cluster.StopServers(0, kClusterSize);

		std::cout << "Launch half of the servers..." << std::endl;
		// Launch half of the servers (no consensus)
		for (size_t i = 0; i < kClusterSize / 2; ++i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_EQ(leaderId, -1);

		std::cout << "Now we should have consensus..." << std::endl;
		// Now we should have consensus
		cluster.StartServer(kClusterSize / 2);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);

		std::cout << "Launch rest of the nodes..." << std::endl;
		// Launch rest of the nodes
		for (size_t i = kClusterSize / 2 + 1; i < kClusterSize; ++i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		auto newLeaderId = cluster.AwaitLeader(kMaxElectionsTime, true);
		ASSERT_EQ(leaderId, newLeaderId);

		std::cout << "Stop nodes without cluster fail..." << std::endl;
		// Stop nodes without cluster fail
		auto safeToRemoveCnt = kClusterSize - (kClusterSize / 2) - 1;
		cluster.StopServers(0, safeToRemoveCnt);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);

		std::cout << "Remove one more node (cluster should not be able to choose the leader)..." << std::endl;
		// Remove one more node (cluster should not be able to choose the leader)
		ASSERT_TRUE(cluster.StopServer(safeToRemoveCnt));
		loop.sleep(std::chrono::seconds(5));
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, true);
		ASSERT_EQ(leaderId, -1);
	});

	loop.run();
}

TEST_F(ClusterizationApi, OnlineUpdates) {
	// Check basic online replication in cluster
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);

		const auto kNodeIdWithStats = (leaderId + 1) % kClusterSize;
		cluster.EnablePerfStats(kNodeIdWithStats);

		// Create namespace and fill initial data
		const std::string_view kNsSome = "some";
		constexpr size_t kDataPortion = 100;
		std::cout << "Init NS" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		std::cout << "Fill data" << std::endl;
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);
		std::cout << "Wait sync" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop node, fill more data and await sync
		for (size_t i = 0; i < (kClusterSize) / 2; ++i) {
			std::cout << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
			std::cout << "Await leader" << std::endl;
			leaderId = cluster.AwaitLeader(kMaxElectionsTime);
			ASSERT_NE(leaderId, -1);
			std::cout << "Fill data" << std::endl;
			cluster.FillData(leaderId, kNsSome, (i + 1) * kDataPortion, kDataPortion / 2);
			cluster.FillDataTx(leaderId, kNsSome, (i + 1) * kDataPortion + kDataPortion / 2, kDataPortion / 2);
			std::cout << "Wait sync" << std::endl;
			cluster.WaitSync(kNsSome);
		}
		std::cout << "Done" << std::endl;
	});

	loop.run();
}

TEST_F(ClusterizationApi, ForceAndWalSync) {
	// Check full cluster synchronization via all the available mechanisms
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 7;
		Cluster cluster(loop, 0, kClusterSize, ports);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

		const std::string kNsSome = "some";
		constexpr size_t kDataPortion = 100;

		// Fill data for N/2 + 1 nodes
		cluster.StopServers(0, kClusterSize / 2);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime, false);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

		std::cout << "Fill data 1" << std::endl;
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
			std::cout << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		std::cout << "Wait sync 1" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop half of the nodes again
		cluster.StopServers(kClusterSize / 2 + 1, kClusterSize);

		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

		// Fill data and then start node. Repeat until all nodes in the cluster are alive
		std::cout << "Fill data 2" << std::endl;
		cluster.FillData(leaderId, kNsSome, kDataPortion, kDataPortion);
		for (size_t i = (kClusterSize) / 2 + 1; i < kClusterSize; ++i) {
			std::cout << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
			std::cout << "Fill more" << std::endl;
			cluster.FillData(leaderId, kNsSome, (i + 2) * kDataPortion, kDataPortion / 2);
			cluster.FillDataTx(leaderId, kNsSome, (i + 2) * kDataPortion + kDataPortion / 2, kDataPortion / 2);
		}
		std::cout << "Wait sync 2" << std::endl;
		cluster.WaitSync(kNsSome);
	});

	loop.run();
}

static void ValidateStatsOnTestBeginning(size_t kClusterSize, const ClusterizationApi::Defaults& ports, const std::set<int> onlineNodes,
										 ServerControl::Interface::Ptr node) {
	constexpr auto kMaxRetries = 100;
	for (int i = 0; i < kMaxRetries; ++i) {
		const auto stats = node->GetReplicationStats(cluster::kClusterReplStatsType);
		WrSerializer wser;
		stats.GetJSON(wser);

		ASSERT_EQ(stats.initialSync.walSyncs.count, 0) << "json: " << wser.Slice();
		ASSERT_GT(stats.initialSync.totalTimeUs, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.initialSync.walSyncs.maxTimeUs, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.initialSync.walSyncs.avgTimeUs, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.initialSync.forceSyncs.count, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.initialSync.forceSyncs.maxTimeUs, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.initialSync.forceSyncs.avgTimeUs, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.forceSyncs.count, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.walSyncs.count, 0) << "json: " << wser.Slice();
		ASSERT_EQ(stats.nodeStats.size(), kClusterSize) << "json: " << wser.Slice();

		bool hasUpdates = false;
		for (auto& nodeStat : stats.nodeStats) {
			const std::string kExpectedDsn =
				fmt::format("cproto://127.0.0.1:{}/node{}", ports.defaultRpcPort + nodeStat.serverId, nodeStat.serverId);
			ASSERT_EQ(nodeStat.dsn, kExpectedDsn) << "json: " << wser.Slice();
			if (nodeStat.updatesCount > 0) {
				hasUpdates = true;
				break;
			}
			ASSERT_EQ(nodeStat.updatesCount, 0) << "json: " << wser.Slice();
			ASSERT_TRUE(nodeStat.namespaces.empty()) << "json: " << wser.Slice();
			if (onlineNodes.count(nodeStat.serverId)) {
				ASSERT_EQ(nodeStat.status, cluster::NodeStats::Status::Online) << "json: " << wser.Slice();
				ASSERT_EQ(nodeStat.syncState, cluster::NodeStats::SyncState::OnlineReplication) << "json: " << wser.Slice();
			} else {
				ASSERT_EQ(nodeStat.status, cluster::NodeStats::Status::Offline) << "json: " << wser.Slice();
				ASSERT_EQ(nodeStat.syncState, cluster::NodeStats::SyncState::AwaitingResync) << "json: " << wser.Slice();
			}
		}
		if (!hasUpdates) {
			return;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		if (i == kMaxRetries - 1) {
			ASSERT_TRUE(false) << wser.Slice();
		}
	}
	assert(false);
}

static void ValidateStatsOnTestEnding(int leaderId, int kTransitionServer, const std::vector<size_t>& kFirstNodesGroup,
									  const std::vector<size_t>& kSecondNodesGroup, size_t kClusterSize,
									  const ClusterizationApi::Defaults& ports, const cluster::ReplicationStats& stats) {
	WrSerializer wser;
	stats.GetJSON(wser);
	ASSERT_EQ(stats.initialSync.walSyncs.count, 1) << "json: " << wser.Slice();
	ASSERT_GT(stats.initialSync.totalTimeUs, 0) << "json: " << wser.Slice();
	ASSERT_GT(stats.initialSync.walSyncs.maxTimeUs, 0) << "json: " << wser.Slice();
	ASSERT_EQ(stats.initialSync.walSyncs.maxTimeUs, stats.initialSync.walSyncs.avgTimeUs) << "json: " << wser.Slice();
	ASSERT_EQ(stats.initialSync.forceSyncs.count, 0) << "json: " << wser.Slice();
	ASSERT_EQ(stats.initialSync.forceSyncs.maxTimeUs, 0) << "json: " << wser.Slice();
	ASSERT_EQ(stats.initialSync.forceSyncs.avgTimeUs, 0) << "json: " << wser.Slice();
	ASSERT_EQ(stats.forceSyncs.count, 0) << "json: " << wser.Slice();
	if (leaderId == kTransitionServer) {
		ASSERT_EQ(stats.walSyncs.count, kFirstNodesGroup.size()) << "json: " << wser.Slice();
	} else {
		ASSERT_GE(stats.walSyncs.count, kFirstNodesGroup.size() - 1) << "json: " << wser.Slice();
		ASSERT_LE(stats.walSyncs.count, kFirstNodesGroup.size()) << "json: " << wser.Slice();
	}
	ASSERT_EQ(stats.nodeStats.size(), kClusterSize) << "json: " << wser.Slice();
	{
		std::set<size_t> onlineNodes, offlineNodes;
		for (auto node : kSecondNodesGroup) offlineNodes.emplace(node);
		for (auto node : kFirstNodesGroup) onlineNodes.emplace(node);
		onlineNodes.emplace(kTransitionServer);
		for (auto& nodeStat : stats.nodeStats) {
			const std::string kExpectedDsn =
				fmt::format("cproto://127.0.0.1:{}/node{}", ports.defaultRpcPort + nodeStat.serverId, nodeStat.serverId);
			ASSERT_EQ(nodeStat.dsn, kExpectedDsn) << "json: " << wser.Slice();
			ASSERT_TRUE(nodeStat.updatesCount == 0 || nodeStat.updatesCount == 1)
				<< "json: " << wser.Slice();  // (may contain "initial leader resync" update record
			if (onlineNodes.count(nodeStat.serverId)) {
				ASSERT_EQ(onlineNodes.count(nodeStat.serverId), 1) << "json: " << wser.Slice();
				ASSERT_EQ(nodeStat.status, cluster::NodeStats::Status::Online) << "json: " << wser.Slice();
				ASSERT_EQ(nodeStat.syncState, cluster::NodeStats::SyncState::OnlineReplication) << "json: " << wser.Slice();
				onlineNodes.erase(nodeStat.serverId);
				if (nodeStat.serverId == leaderId) {
					ASSERT_EQ(nodeStat.role, cluster::RaftInfo::Role::Leader) << "json: " << wser.Slice();
				} else {
					ASSERT_EQ(nodeStat.role, cluster::RaftInfo::Role::Follower) << "json: " << wser.Slice();
				}
			} else if (offlineNodes.count(nodeStat.serverId)) {
				ASSERT_EQ(offlineNodes.count(nodeStat.serverId), 1);
				ASSERT_EQ(nodeStat.status, cluster::NodeStats::Status::Offline) << "json: " << wser.Slice();
				ASSERT_EQ(nodeStat.syncState, cluster::NodeStats::SyncState::AwaitingResync) << "json: " << wser.Slice();
				offlineNodes.erase(nodeStat.serverId);
				ASSERT_EQ(nodeStat.role, cluster::RaftInfo::Role::None) << "json: " << wser.Slice();
			} else {
				ASSERT_TRUE(false) << "Unexpected server id: " << nodeStat.serverId << "; json: " << wser.Slice();
			}
		}
		ASSERT_EQ(onlineNodes.size(), 0) << "json: " << wser.Slice();
		ASSERT_EQ(offlineNodes.size(), 0) << "json: " << wser.Slice();
	}
}

TEST_F(ClusterizationApi, InitialLeaderSync) {
	// Check if new leader is able to get newest data from other nodes after elections
	net::ev::dynamic_loop loop;
	const auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
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
		std::cout << "Leader id is " << leaderId << std::endl;
		std::set<int> onlineNodes = {kTransitionServer};
		for (auto id : kFirstNodesGroup) {
			onlineNodes.emplace(id);
		}
		loop.sleep(std::chrono::seconds(2));  // await stats update
		ValidateStatsOnTestBeginning(kClusterSize, ports, onlineNodes, cluster.GetNode(leaderId));

		// Fill data for nodes from the first group
		std::cout << "Fill data 1" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);
		std::cout << "Wait sync 1" << std::endl;
		cluster.WaitSync(kNsSome);

		// Stop cluster
		cluster.StopServers(kFirstNodesGroup);
		std::cout << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start second group. kTransitionServer is the only node with data from previous step
		for (auto i : kSecondNodesGroup) {
			std::cout << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		std::cout << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

		std::cout << "Wait sync 2" << std::endl;
		cluster.WaitSync(kNsSome);

		// Make sure, that our cluster didn't miss it's data in process
		auto state = cluster.GetNode(kClusterSize - 1)->GetState(std::string(kNsSome));
		ASSERT_EQ(state.lsn.Counter(), kDataPortion + 2);

		// Check WAL initial sync for nodes from the first group
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

		// Fill additional data for second group
		std::cout << "Fill data 2" << std::endl;
		cluster.FillDataTx(leaderId, kNsSome, 150, kDataPortion);
		cluster.FillData(leaderId, kNsSome, 50, kDataPortion);

		// Stop cluster
		cluster.StopServers(kSecondNodesGroup);
		std::cout << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start first group againt and make sure, that all of the data were replicated from transition node
		for (auto i : kFirstNodesGroup) {
			std::cout << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		std::cout << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

		std::cout << "Wait sync 3" << std::endl;
		cluster.WaitSync(kNsSome);

		state = cluster.GetNode(0)->GetState(std::string(kNsSome));
		ASSERT_EQ(state.lsn.Counter(), 3 * kDataPortion + 2 + 2);  // Data + indexes + tx records

		// Add some time for stats stabilization
		std::this_thread::sleep_for(std::chrono::seconds(2));

		// Validate stats
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;
		auto stats = cluster.GetNode(leaderId)->GetReplicationStats(cluster::kClusterReplStatsType);
		ValidateStatsOnTestEnding(leaderId, kTransitionServer, kFirstNodesGroup, kSecondNodesGroup, kClusterSize, ports, stats);
		// Validate stats, revieved via followers (this request has to be proxied)
		for (auto nodeId : kFirstNodesGroup) {
			auto followerStats = cluster.GetNode(nodeId)->GetReplicationStats(cluster::kClusterReplStatsType);
			ASSERT_EQ(stats, followerStats) << "Server id: " << nodeId;
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, InitialLeaderSyncWithConcurrentSnapshots) {
	// Check if new leader is able to get newest data from other nodes after elections and handle max snapshots limit correctly
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		const std::vector<size_t> kFirstNodesGroup = {0, 1};   // Servers to stop after data fill
		const size_t kTransitionServer = 2;					   // The only server, which will have actual data. It belongs to both groups
		const std::vector<size_t> kSecondNodesGroup = {3, 4};  // Empty servers, which have to perfomr sync
		Cluster cluster(loop, 0, kClusterSize, ports, std::vector<std::string>(), std::chrono::milliseconds(3000), -1, 1);

		const std::vector<std::string> kNsNames = {"ns1", "ns2",  "ns3",  "ns4",  "ns5",  "ns6",  "ns7", "ns8",
												   "ns9", "ns10", "ns11", "ns12", "ns13", "ns14", "ns15"};
		constexpr size_t kDataPortion = 20;

		// Check force initial sync for nodes from the second group
		cluster.StopServers(kSecondNodesGroup);

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

		// Fill data for nodes from the first group
		std::cout << "Fill data 1" << std::endl;
		for (auto& ns : kNsNames) {
			cluster.InitNs(leaderId, ns);
			cluster.FillData(leaderId, ns, 0, kDataPortion);
		}
		std::cout << "Wait sync 1" << std::endl;
		for (auto& ns : kNsNames) {
			cluster.WaitSync(ns);
		}

		// Stop cluster
		cluster.StopServers(kFirstNodesGroup);
		std::cout << "Stopping " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StopServer(kTransitionServer));

		// Start second group. kTransitionServer is the only node with data from previous step
		for (auto i : kSecondNodesGroup) {
			std::cout << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		std::cout << "Starting " << kTransitionServer << std::endl;
		ASSERT_TRUE(cluster.StartServer(kTransitionServer));

		std::cout << "Wait sync 2" << std::endl;
		for (auto& ns : kNsNames) {
			cluster.WaitSync(ns);
		}

		// Make sure, that our cluster didn't miss it's data in process
		for (auto& ns : kNsNames) {
			auto state = cluster.GetNode(kClusterSize - 1)->GetState(ns);
			ASSERT_EQ(state.lsn.Counter(), kDataPortion + 2);
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, SpecifyClusterNamespaceList) {
	// Check if with specified cluster namespaces list we are able to write into follower's non-cluster namespaces
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		const std::vector<std::string> kClusterNsNames = {"ns1", "ns2"};
		const std::string kNonClusterNsName = "ns3";

		Cluster cluster(loop, 0, kClusterSize, ports, kClusterNsNames);

		std::vector<std::thread> threads;
		constexpr size_t kDataPortion = 100;

		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

		struct NodeData {
			size_t nodeId;
			size_t dataCount;
		};
		std::vector<NodeData> nodesData;

		// Fill ns on one of the followers
		const size_t kFollowerDataCount1 = kDataPortion + rand() % kDataPortion;
		const size_t kFollowerId1 = (leaderId + 1) % kClusterSize;
		nodesData.emplace_back(NodeData{kFollowerId1, kFollowerDataCount1});
		cluster.InitNs(kFollowerId1, kNonClusterNsName);
		cluster.FillData(kFollowerId1, kNonClusterNsName, 0, kFollowerDataCount1);

		// Fill ns on another follower
		const size_t kFollowerId2 = (leaderId + 2) % kClusterSize;
		const size_t kFollowerDataCount2 = kFollowerDataCount1 + 10;
		nodesData.emplace_back(NodeData{kFollowerId2, kFollowerDataCount2});
		cluster.InitNs(kFollowerId2, kNonClusterNsName);
		cluster.FillData(kFollowerId2, kNonClusterNsName, 0, kFollowerDataCount2);

		// Restart leader to make shure, that non-cluster namespaces will be ignored
		const int oldLeaderId = leaderId;
		cluster.StopServer(leaderId);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;
		cluster.StartServer(oldLeaderId);

		auto nodeId = oldLeaderId;
		if (size_t(leaderId) != kFollowerId1 && size_t(leaderId) != kFollowerId2) {
			nodeId = leaderId;
		}
		// Fill ns on leader
		const size_t kLeaderDataCount = (kDataPortion + rand() % kDataPortion) / 2;
		nodesData.emplace_back(NodeData{size_t(nodeId), kLeaderDataCount});
		cluster.InitNs(nodeId, kNonClusterNsName);
		cluster.FillData(nodeId, kNonClusterNsName, 0, kLeaderDataCount);

		for (auto& cns : kClusterNsNames) {
			cluster.InitNs(leaderId, cns);
			cluster.FillData(leaderId, cns, 0, kDataPortion + rand() % kDataPortion);
		}

		// Make shure, that cluster is synchronized
		for (auto& ns : kClusterNsNames) {
			std::cout << "Wait sync for " << ns << std::endl;
			cluster.WaitSync(ns);
		}

		for (auto nodeData : nodesData) {
			auto node = cluster.GetNode(nodeData.nodeId);
			BaseApi::QueryResultsType qr;
			auto err = node->api.reindexer->Select(Query(kNonClusterNsName), qr);
			ASSERT_TRUE(err.ok()) << "Node id: " << nodeData.nodeId << "; expected data count: " << nodeData.dataCount
								  << "; error: " << err.what();
			ASSERT_EQ(qr.Count(), nodeData.dataCount) << "Node id: " << nodeData.nodeId;
		}

		{
			// Validate namespaces in stats
			WrSerializer wser;
			auto stats = cluster.GetNode(leaderId)->GetReplicationStats(cluster::kClusterReplStatsType);
			stats.GetJSON(wser);
			ASSERT_EQ(stats.nodeStats.size(), kClusterSize) << wser.Slice();
			for (auto& nodeStat : stats.nodeStats) {
				ASSERT_EQ(nodeStat.namespaces, kClusterNsNames) << wser.Slice();
			}
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, MultithreadSyncTest) {
	// Check full cluster synchronization via all the available mechanisms
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports, std::vector<std::string>(), std::chrono::milliseconds(200));

		const std::vector<std::string> kNsNames = {"ns1", "ns2", "ns3"};
		std::vector<std::thread> threads;
		constexpr size_t kDataPortion = 100;
		constexpr size_t kMaxDataId = 10000;
		std::atomic<bool> terminate = {false};

		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

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
						tx.Upsert(std::move(item));
					}
					for (size_t i = 0; i < 2; ++i) {
						int minIdx = rand() % kMaxDataId;
						int maxIdx = minIdx + 100;
						Query q =
							Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx).Set(kStringField, randStringAlph(25));
						q.type_ = QueryUpdate;
						tx.Modify(std::move(q));
					}
					for (size_t i = 0; i < 2; ++i) {
						int minIdx = rand() % kMaxDataId;
						int maxIdx = minIdx + 100;
						Query q = Query(ns).Where(kIdField, CondGe, minIdx).Where(kIdField, CondLe, maxIdx);
						q.type_ = QueryDelete;
						tx.Modify(std::move(q));
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
		auto selectF = ([&cluster, &terminate](const std::string& ns) {
			while (!terminate) {
				auto id = rand() % kClusterSize;
				auto node = cluster.GetNode(id);
				if (node) {
					BaseApi::QueryResultsType qr;
					node->api.reindexer->Select(Query(ns), qr);
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
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
			std::cout << "Wait sync for " << ns << std::endl;
			cluster.WaitSync(ns);
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, NamespaceOperationsOnlineReplication) {
	// Check if Add/Rename/Drop namespace are replicating correctly in concurrent environment
	constexpr size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		Cluster cluster(loop, 0, kClusterSize, ports);

		const std::string kBaseNsName = "ns_name";
		std::vector<std::thread> threads;
		constexpr size_t kBaseDataPortion = 10;

		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;

		std::vector<NamespaceData> existingNamespaces;
		std::mutex mtx;
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
				std::lock_guard<std::mutex> lck(mtx);
				existingNamespaces.emplace_back(NamespaceData{kNsName, lsn_t(dataCount + 2, leaderId), state.nsVersion});
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
				std::lock_guard<std::mutex> lck(mtx);
				existingNamespaces.emplace_back(NamespaceData{kNsName, lsn_t(dataCount + 2, leaderId), state.nsVersion});
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
	});

	loop.run();
}

TEST_F(ClusterizationApi, NamespaceVersioning) {
	// Check if new leader will get the latest namespace version if multiple versions are available
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		Cluster cluster(loop, 0, kClusterSize, ports);

		const std::string_view kNsName = "some";
		constexpr size_t kDataPortion1 = 30;
		constexpr size_t kDataPortion2 = 20;
		constexpr size_t kDataPortion3 = 10;

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Leader id is " << leaderId << std::endl;
		size_t id = 0;

		ReplicationStateApi latestNsState;
		auto CreateAndDropNsFn = [&leaderId, &cluster, kNsName, &latestNsState](size_t dataCount, size_t stopId, bool drop,
																				bool awaitLeader) {
			// Create ns and fill data
			std::cout << "Fill data" << std::endl;
			cluster.InitNs(leaderId, kNsName);
			cluster.FillData(leaderId, kNsName, 0, dataCount);
			std::cout << "Wait sync" << std::endl;
			cluster.WaitSync(kNsName);
			latestNsState = cluster.GetNode(leaderId)->GetState(std::string(kNsName));
			// Stop one of the nodes
			std::cout << "Stopping " << stopId << std::endl;
			EXPECT_TRUE(cluster.StopServer(stopId));
			if (awaitLeader) {
				leaderId = cluster.AwaitLeader(kMaxElectionsTime);
				EXPECT_NE(leaderId, -1);
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
			std::cout << "Stopping " << i << std::endl;
			ASSERT_TRUE(cluster.StopServer(i));
		}

		for (size_t i = 0; i < kLastRestartId; ++i) {
			std::cout << "Starting " << i << std::endl;
			ASSERT_TRUE(cluster.StartServer(i));
		}
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Wait final sync" << std::endl;
		cluster.WaitSync(kNsName);
		const auto state = cluster.GetNode(leaderId)->GetState(std::string(kNsName));
		ASSERT_EQ(latestNsState.lsn, state.lsn);
		ASSERT_EQ(latestNsState.nsVersion, state.nsVersion);
		ASSERT_EQ(latestNsState.dataHash, state.dataHash);
		ASSERT_EQ(latestNsState.dataCount, state.dataCount);
	});

	loop.run();
}

static void AwaitSyncNodesCount(ClusterizationApi::Cluster& cluster, size_t leaderId, size_t expectedCount, std::chrono::seconds maxTime) {
	std::chrono::milliseconds step(100);
	std::chrono::milliseconds awaitTime = std::chrono::duration_cast<std::chrono::milliseconds>(maxTime);
	while (awaitTime.count() > 0) {
		if (cluster.GetSynchronizedNodesCount(leaderId) == expectedCount) {
			return;
		}
		awaitTime -= step;
		std::this_thread::sleep_for(step);
	}
	auto stats = cluster.GetNode(leaderId)->GetReplicationStats(cluster::kClusterReplStatsType);
	WrSerializer wser;
	stats.GetJSON(wser);
	ASSERT_TRUE(false) << "Stats: " << wser.Slice();
}

TEST_F(ClusterizationApi, SynchronizationStatusTest) {
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		const std::string_view kNsName = "some";
		constexpr size_t kDataPortion = 10;
		constexpr size_t kStatsThreadsCnt = 5;
		constexpr size_t kUpsertThreadsCnt = 5;
		std::atomic<bool> terminate = false;
		constexpr size_t restartNodeId = 0;
		Cluster cluster(loop, 0, kClusterSize, ports);

		cluster.StopServer(restartNodeId);
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::cout << "Checking sync status without data" << std::endl;
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize - 1, kMaxSyncTime);

		std::cout << "Checking sync status during online updates" << std::endl;
		cluster.InitNs(leaderId, kNsName);
		std::vector<std::thread> upsertThreads;
		upsertThreads.reserve(kUpsertThreadsCnt);
		for (size_t tid = 0; tid < kStatsThreadsCnt; ++tid) {
			upsertThreads.emplace_back(std::thread([leaderId, &kNsName, &cluster, &terminate] {
				size_t id = 0;
				while (!terminate) {
					cluster.FillData(leaderId, kNsName, id, kDataPortion);
					id += kDataPortion;
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
			}));
		}
		std::vector<std::thread> statThreads;
		statThreads.reserve(kStatsThreadsCnt);
		for (size_t tid = 0; tid < kStatsThreadsCnt; ++tid) {
			statThreads.emplace_back(std::thread([leaderId, &cluster] {
				for (size_t i = 0; i < 10; ++i) {
					auto syncNodes = cluster.GetSynchronizedNodesCount(leaderId);
					if (syncNodes < kClusterSize / 2 + 1) {
						auto stats = cluster.GetNode(leaderId)->GetReplicationStats(cluster::kClusterReplStatsType);
						WrSerializer wser;
						stats.GetJSON(wser);
						ASSERT_TRUE(false) << "Stats: " << wser.Slice();
					}
					std::this_thread::sleep_for(std::chrono::milliseconds(20));
				}
			}));
		}
		for (auto& stTh : statThreads) {
			stTh.join();
		}
		terminate = true;
		for (auto& uTh : upsertThreads) {
			uTh.join();
		}
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize - 1, kMaxSyncTime);

		std::cout << "Checking sync status after new follower sync" << std::endl;
		cluster.StartServer(restartNodeId);
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize, kMaxSyncTime);

		std::cout << "Checking sync status after follower shutdown" << std::endl;
		cluster.StopServer(restartNodeId);
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize - 1, kMaxSyncTime);

		std::cout << "Checking sync status after full cluster restart" << std::endl;
		cluster.StopServers(0, kClusterSize);
		for (size_t i = 0; i < kClusterSize; ++i) {
			cluster.StartServer(i);
		}
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize, kMaxSyncTime);
	});

	loop.run();
}

TEST_F(ClusterizationApi, SynchronizationStatusOnInitialSyncTest) {
	// Check, that cluster initializes synchronized nodes list on leader's initial sync
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 5;
		const std::string_view kNsName = "some";
		constexpr size_t kDataPortion = 100;
		std::atomic<bool> terminate = false;
		constexpr size_t kRestartNodeIdForce = 0;
		constexpr size_t kRestartNodeIdWal = 1;
		constexpr size_t kMinSynchronizedNodesCount = kClusterSize - 3;
		Cluster cluster(loop, 0, kClusterSize, ports);

		cluster.StopServer(kRestartNodeIdForce);
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		cluster.InitNs(leaderId, kNsName);
		cluster.FillData(leaderId, kNsName, 0, kDataPortion);
		cluster.WaitSync(kNsName);
		cluster.StopServer(kRestartNodeIdWal);
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		cluster.FillData(leaderId, kNsName, kDataPortion, 9 * kDataPortion);
		cluster.WaitSync(kNsName);
		cluster.StopServers(0, kClusterSize);
		constexpr size_t kRunningServerId = kClusterSize - 2;
		cluster.StartServer(kRunningServerId);
		std::thread statThread([&cluster, &terminate, kMinSynchronizedNodesCount]() noexcept {
			while (!terminate) {
				auto syncCount = cluster.GetSynchronizedNodesCount(kRunningServerId);
				if (syncCount) {
					ASSERT_GE(syncCount, kMinSynchronizedNodesCount);
				}
			}
			size_t syncCount;
			for (size_t i = 0; i < 10; ++i) {
				syncCount = cluster.GetSynchronizedNodesCount(kRunningServerId);
				if (syncCount >= kClusterSize - 1) {
					break;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
			ASSERT_EQ(syncCount, kClusterSize - 1);
		});
		for (int i = kRunningServerId; i >= 0; --i) {
			cluster.StartServer(i);
		}
		cluster.WaitSync(kNsName);
		terminate = true;
		statThread.join();
	});

	loop.run();
}

TEST_F(ClusterizationApi, RestrictUpdates) {
	// Check, that updates drop doesn't breaks raft cluster
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 3;
		constexpr size_t kInactiveNode = kClusterSize - 1;
		const std::string_view kNsName = "some";
		Cluster cluster(loop, 0, kClusterSize, ports, std::vector<std::string>(), std::chrono::milliseconds(1000), -1, 2, 5 * 1024);
		cluster.StopServer(kInactiveNode);

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		cluster.InitNs(leaderId, kNsName);
		cluster.FillData(leaderId, kNsName, 0, 2000);

		const int count = 396;	// 4 additioonal updates for AddNamespace and indexes
		const int from = 1000000;
		const std::string nsName("ns1");
		std::string dataString;
		for (size_t i = 0; i < 10000; ++i) {
			dataString.append("xxx");
		}

		cluster.StartServer(kInactiveNode);

		auto leader = cluster.GetNode(leaderId);
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = leader->api.NewItem(kNsName);
			std::string itemJson = fmt::sprintf(R"json({"id": %d, "string": "%s" })json", i + from, dataString);
			auto err = item.Unsafe().FromJSON(itemJson);
			ASSERT_TRUE(err.ok()) << err.what();
			leader->api.Upsert(kNsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		cluster.WaitSync(kNsName);

		std::this_thread::sleep_for(std::chrono::seconds(5));
		auto stats = cluster.GetNode(leaderId)->GetReplicationStats(cluster::kClusterReplStatsType);

		const auto updatesDrops1 = stats.updateDrops;
		ASSERT_EQ(stats.pendingUpdatesCount, 0);
		ASSERT_GE(stats.allocatedUpdatesCount, 0);
		if (stats.allocatedUpdatesCount) {
			ASSERT_GE(stats.allocatedUpdatesSizeBytes, 0);
		} else {
			ASSERT_EQ(stats.allocatedUpdatesSizeBytes, 0);
		}

		// Make sure, that replication is works fine after updates drop
		cluster.FillData(leaderId, kNsName, 10000, 100);
		cluster.WaitSync(kNsName);

		stats = cluster.GetNode(leaderId)->GetReplicationStats(cluster::kClusterReplStatsType);
		ASSERT_EQ(stats.pendingUpdatesCount, 0);
		if (stats.allocatedUpdatesCount) {
			ASSERT_GE(stats.allocatedUpdatesSizeBytes, 0);
		} else {
			ASSERT_EQ(stats.allocatedUpdatesSizeBytes, 0);
		}
		const auto updatesDrops2 = stats.updateDrops;

		if (!updatesDrops2 || !updatesDrops1) {
			// Mark test as skipped, because we didn't got any updates drops
			GTEST_SKIP();
		}
	});

	loop.run();
}

TEST_F(ClusterizationApi, ErrorOnAttemptToResetClusterNsRole) {
	// Check error on attempt to reset cluster namespace role
	net::ev::dynamic_loop loop;
	auto ports = GetDefaultPorts();
	loop.spawn([&loop, &ports]() noexcept {
		constexpr size_t kClusterSize = 3;
		const std::string kNsSome = "some";
		Cluster cluster(loop, 0, kClusterSize, ports);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);

		std::cout << "Init NS" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.WaitSync(kNsSome);
		std::cout << "Trying to reset role" << std::endl;
		const auto followerId = (leaderId + 1) % kClusterSize;
		auto err = cluster.GetNode(followerId)->TryResetReplicationRole(kNsSome);
		EXPECT_EQ(err.code(), errLogic) << err.what();
		ReplicationStateV2 replState;
		err = cluster.GetNode(followerId)->api.reindexer->GetReplState(kNsSome, replState);
		WrSerializer ser;
		JsonBuilder jb(ser);
		replState.GetJSON(jb);
		ASSERT_EQ(replState.clusterStatus.role, ClusterizationStatus::Role::ClusterReplica) << ser.Slice();
		std::cout << "Done" << std::endl;
	});

	loop.run();
}
