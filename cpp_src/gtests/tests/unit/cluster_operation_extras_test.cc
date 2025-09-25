#include "cluster/consts.h"
#include "cluster_operation_extras_api.h"
#include "core/cjson/jsonbuilder.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"

using namespace reindexer;

TEST_F(ClusterOperationExtrasApi, SpecifyClusterNamespaceList) {
	// Check if with specified cluster namespaces list we are able to write into follower's non-cluster namespaces
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 5;
		const std::vector<std::string> kClusterNsNames = {"ns1", "ns2"};
		const std::string kNonClusterNsName = "ns3";

		Cluster cluster(loop, 0, kClusterSize, ports, kClusterNsNames);

		std::vector<std::thread> threads;
		constexpr size_t kDataPortion = 100;

		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Leader id is " << leaderId << std::endl;

		struct [[nodiscard]] NodeData {
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
		TestCout() << "Leader id is " << leaderId << std::endl;
		ASSERT_TRUE(cluster.StartServer(oldLeaderId));

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
			TestCout() << "Wait sync for " << ns << std::endl;
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
	}));

	loop.run();
}

TEST_F(ClusterOperationExtrasApi, SynchronizationStatusTest) {
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn([&loop, &ports, this]() noexcept {
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
		TestCout() << "Checking sync status without data" << std::endl;
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize - 1, kMaxSyncTime);

		TestCout() << "Checking sync status during online updates" << std::endl;
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

		TestCout() << "Checking sync status after new follower sync" << std::endl;
		ASSERT_TRUE(cluster.StartServer(restartNodeId));
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize, kMaxSyncTime);

		TestCout() << "Checking sync status after follower shutdown" << std::endl;
		ASSERT_TRUE(cluster.StopServer(restartNodeId));
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize - 1, kMaxSyncTime);

		TestCout() << "Checking sync status after full cluster restart" << std::endl;
		cluster.StopServers(0, kClusterSize);
		for (size_t i = 0; i < kClusterSize; ++i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		AwaitSyncNodesCount(cluster, leaderId, kClusterSize, kMaxSyncTime);
	});

	loop.run();
}

TEST_F(ClusterOperationExtrasApi, SynchronizationStatusOnInitialSyncTest) {
	// Check, that cluster initializes synchronized nodes list on leader's initial sync
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
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
		ASSERT_TRUE(cluster.StopServer(kRestartNodeIdWal));
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		cluster.FillData(leaderId, kNsName, kDataPortion, 9 * kDataPortion);
		cluster.WaitSync(kNsName);
		cluster.StopServers(0, kClusterSize);
		constexpr size_t kRunningServerId = kClusterSize - 2;
		ASSERT_TRUE(cluster.StartServer(kRunningServerId));
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
		for (int i = kRunningServerId - 1; i >= 0; --i) {
			ASSERT_TRUE(cluster.StartServer(i));
		}
		cluster.WaitSync(kNsName);
		terminate = true;
		statThread.join();
	}));

	loop.run();
}

TEST_F(ClusterOperationExtrasApi, RestrictUpdates) {
	// Check, that updates drop doesn't breaks raft cluster
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports]() {
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
		std::string dataString;
		for (size_t i = 0; i < 10000; ++i) {
			dataString.append("xxx");
		}

		ASSERT_TRUE(cluster.StartServer(kInactiveNode));

		auto leader = cluster.GetNode(leaderId);
		for (unsigned int i = 0; i < count; i++) {
			reindexer::client::Item item = leader->api.NewItem(kNsName);
			std::string itemJson = fmt::format(R"json({{"id": {}, "string": "{}" }})json", i + from, dataString);
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
	}));

	loop.run();
}

TEST_F(ClusterOperationExtrasApi, ErrorOnAttemptToResetClusterNsRole) {
	// Check error on attempt to reset cluster namespace role
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 3;
		const std::string kNsSome = "some";
		Cluster cluster(loop, 0, kClusterSize, ports);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);

		TestCout() << "Init NS" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.WaitSync(kNsSome);
		TestCout() << "Trying to reset role" << std::endl;
		const auto followerId = (leaderId + 1) % kClusterSize;
		auto err = cluster.GetNode(followerId)->TryResetReplicationRole(kNsSome);
		EXPECT_EQ(err.code(), errLogic) << err.what();
		ReplicationStateV2 replState;
		err = cluster.GetNode(followerId)->api.reindexer->GetReplState(kNsSome, replState);
		WrSerializer ser;
		JsonBuilder jb(ser);
		replState.GetJSON(jb);
		ASSERT_EQ(replState.clusterStatus.role, ClusterOperationStatus::Role::ClusterReplica) << ser.Slice();
		TestCout() << "Done" << std::endl;
	}));

	loop.run();
}

TEST_F(ClusterOperationExtrasApi, LogLevel) {
	// Check sync replication log level setup
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 3;
		const std::string kNsSome = "some";
		Cluster cluster(loop, 0, kClusterSize, ports);
		const int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		TestCout() << "Init NS" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.WaitSync(kNsSome);
		auto leader = cluster.GetNode(leaderId);

		auto checkLogLevel = [&cluster](LogLevel expected) {
			// Expecting same log level on each node (due to proxying logic)
			for (unsigned i = 0; i < kClusterSize; ++i) {
				auto stats = cluster.GetNode(i)->GetReplicationStats("cluster");
				ASSERT_EQ(stats.logLevel, expected) << "Node id: " << i;
			}
		};

		std::atomic<bool> stop = {false};
		std::thread th([&] {
			// Simple insertion thread for race check
			for (unsigned i = 0; !stop; ++i) {
				auto item = leader->api.NewItem(kNsSome);
				auto err = item.Unsafe().FromJSON(fmt::format(R"json({{"id": {}, "string": "{}" }})json", i, randStringAlph(8)));
				ASSERT_TRUE(err.ok()) << err.what();
				leader->api.Upsert(kNsSome, item);
				ASSERT_TRUE(err.ok()) << err.what();
				std::this_thread::sleep_for(std::chrono::milliseconds(5));
			}
		});

		// Changing log level
		const LogLevel levels[] = {LogInfo, LogTrace, LogWarning, LogError, LogNone};
		for (auto level : levels) {
			leader->SetReplicationLogLevel(LogLevel(level), "cluster");
			checkLogLevel(level);
		}

		stop = true;
		th.join();
		leader.reset();

		// Check log level on the old leader after full cluster restart. It should be reset to 'Trace'
		cluster.StopServers(0, kClusterSize);
		for (unsigned i = 0; i < kClusterSize; ++i) {
			cluster.StartServer(i);
		}
		TestCout() << "Awaiting new leader" << std::endl;
		int newLeaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(newLeaderId, -1);
		TestCout() << "Changing leader from " << newLeaderId << " to " << leaderId << std::endl;
		cluster.GetNode(newLeaderId)->SetClusterLeader(leaderId);
		newLeaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_EQ(newLeaderId, leaderId);
		checkLogLevel(LogTrace);

		// Check if it's possible to set log level without the leader
		cluster.StopServers({0, 1});
		loop.sleep(std::chrono::seconds(3));
		cluster.GetNode(2)->SetReplicationLogLevel(LogLevel(LogInfo), "cluster");
	}));

	loop.run();
}
