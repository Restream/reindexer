#include "cluster_operation_async_api.h"
#include "gtests/tests/gtest_cout.h"
#include "tools/fsops.h"

using namespace reindexer;

TEST_F(ClusterOperationAsyncApi, AsyncReplicationForClusterNamespaces) {
	//       async1     async2
	//         \         /
	//          cl1 - cl2 <- updates
	//           \    /
	//            cl3
	//             |
	//           async3

	net::ev::dynamic_loop loop;
	auto defaults = GetDefaults();
	loop.spawn([&loop, &defaults]() noexcept {
		constexpr size_t kClusterSize = 3;
		constexpr size_t kDataPortion = 50;
		const std::string kNsSome = "some";
		Cluster cluster(loop, 0, kClusterSize, defaults);
		int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		std::vector<ServerControl> asyncNodes(kClusterSize);
		for (size_t i = 0; i < kClusterSize; ++i) {
			const int id = kClusterSize + 1 + i;
			ServerControlConfig scConfig(id, defaults.defaultRpcPort + id, defaults.defaultHttpPort + id,
										 fs::JoinPath(defaults.baseTestsetDbPath, "node/" + std::to_string(id)),
										 "node" + std::to_string(id), true, 0);
			asyncNodes[i].InitServer(std::move(scConfig));
		}
		auto WaitSyncWithExternalNodes = [&asyncNodes](const std::string& nsName, size_t count, const ServerControl::Interface::Ptr& s) {
			for (size_t i = 0; i < count; ++i) {
				ServerControl::WaitSync(s, asyncNodes[i].Get(), nsName);
			}
		};

		for (size_t i = 0; i < kClusterSize - 1; ++i) {
			asyncNodes[i].Get()->MakeFollower();
			cluster.AddAsyncNode(i, asyncNodes[i].Get(), cluster::AsyncReplicationMode::Default);
		}
		TestCout() << "Init NS" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.WaitSync(kNsSome);
		TestCout() << "Wait replication to async nodes (2/3)" << std::endl;
		WaitSyncWithExternalNodes(kNsSome, kClusterSize - 1, cluster.GetNode(leaderId));

		const auto kLastNode = kClusterSize - 1;
		asyncNodes[kLastNode].Get()->MakeFollower();
		cluster.AddAsyncNode(kLastNode, asyncNodes[kLastNode].Get(), cluster::AsyncReplicationMode::Default);
		TestCout() << "Wait replication to the last async node" << std::endl;
		WaitSyncWithExternalNodes(kNsSome, kClusterSize, cluster.GetNode(leaderId));
		TestCout() << "Fill data" << std::endl;
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);
		cluster.FillDataTx(leaderId, kNsSome, kDataPortion, kDataPortion);
		TestCout() << "Wait cluster sync" << std::endl;
		cluster.WaitSync(kNsSome);
		TestCout() << "Wait replication to async nodes" << std::endl;
		WaitSyncWithExternalNodes(kNsSome, kClusterSize - 1, cluster.GetNode(leaderId));

		// Check if ns renaming is not posible in this config
		auto err = cluster.GetNode(leaderId)->api.reindexer->RenameNamespace(kNsSome, "new_ns");
		ASSERT_EQ(err.code(), errParams) << err.what();
		std::vector<NamespaceDef> defs;
		err = cluster.GetNode(leaderId)->api.reindexer->EnumNamespaces(defs, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_EQ(defs.size(), 1);
		ASSERT_EQ(defs[0].name, kNsSome);
	});

	loop.run();
}

TEST_F(ClusterOperationAsyncApi, GetLastErrorFromInterceptingNamespaces) {
	net::ev::dynamic_loop loop;
	auto defaults = GetDefaults();
	loop.spawn([&loop, &defaults]() noexcept {
		constexpr size_t kClusterSize = 3;
		constexpr size_t kReplicatedNodeId = 2;
		static const std::string namespaceName = "somenamespace";
		static const std::string expectedError = "Replication namespace '" + namespaceName +
												 "' is present on target node in sync cluster config. Target namespace can not be a "
												 "part of sync cluster";

		// Prepare clusters
		Cluster cluster1(loop, 0, kClusterSize, defaults);
		Cluster cluster2(loop, kClusterSize, kClusterSize, defaults, {namespaceName});

		const int leaderId1 = cluster1.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId1, -1);

		const int leaderId2 = cluster2.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId2, -1);

		// Add intercepting namespace
		cluster1.AddAsyncNode(kReplicatedNodeId, cluster2.GetNode(kReplicatedNodeId), cluster::AsyncReplicationMode::Default,
							  {{namespaceName}});

		// Get stats
		ServerControl::Interface::Ptr node = cluster1.GetNode(kReplicatedNodeId);
		for (std::size_t i = 0; i < 10; i++) {
			reindexer::cluster::ReplicationStats stats = node->GetReplicationStats("async");
			if (!stats.nodeStats.empty() && stats.nodeStats[0].lastError.code() == errParams) {
				break;
			}
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}

		reindexer::cluster::ReplicationStats stats = node->GetReplicationStats("async");

		ASSERT_EQ(stats.nodeStats.size(), std::size_t(1));
		ASSERT_EQ(stats.nodeStats[0].lastError.code(), errParams);
		ASSERT_EQ(stats.nodeStats[0].lastError.what(), expectedError);
	});

	loop.run();
}

TEST_F(ClusterOperationAsyncApi, AsyncReplicationBetweenClustersDefaultMode) {
	//          cluster1 (ns1, ns2)        cluster2 (ns1)
	// updates -> cl10 - cl11               cl20 - cl21
	//              \    /  async repl(ns2)  \    /
	//              cl12 -------------------> cl22

	net::ev::dynamic_loop loop;
	auto defaults = GetDefaults();
	loop.spawn([&loop, &defaults]() noexcept {
		constexpr size_t kClusterSize = 3;
		constexpr size_t kDataPortion = 20;
		const std::string kNs1 = "ns1";
		const std::string kNs2 = "ns2";
		constexpr size_t kReplicatedNodeId = 2;
		Cluster cluster1(loop, 0, kClusterSize, defaults);
		Cluster cluster2(loop, kClusterSize, kClusterSize, defaults, {kNs1});
		const int leaderId1 = cluster1.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId1, -1);
		const int leaderId2 = cluster2.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId2, -1);

		cluster2.GetNode(kReplicatedNodeId)->MakeFollower();
		cluster1.AddAsyncNode(kReplicatedNodeId, cluster2.GetNode(kReplicatedNodeId), cluster::AsyncReplicationMode::Default, {{kNs2}});

		TestCout() << "Init NS" << std::endl;
		cluster1.InitNs(leaderId1, kNs1);
		cluster1.InitNs(leaderId1, kNs2);
		cluster2.InitNs(leaderId2, kNs1);
		cluster1.WaitSync(kNs1);
		cluster1.WaitSync(kNs2);
		cluster2.WaitSync(kNs1);
		for (size_t i = 0; i < kClusterSize; ++i) {
			cluster1.FillData(i, kNs2, 2 * i * kDataPortion, 2 * kDataPortion);
			cluster2.FillData(i, kNs1, i * kDataPortion, kDataPortion);
		}
		TestCout() << "Wait cluster1 sync" << std::endl;
		cluster1.WaitSync(kNs2);
		TestCout() << "Wait cluster2 sync" << std::endl;
		cluster2.WaitSync(kNs1);
		TestCout() << "Wait replication to async node" << std::endl;
		ServerControl::WaitSync(cluster1.GetNode(kReplicatedNodeId), cluster2.GetNode(kReplicatedNodeId), kNs2);
		TestCout() << "Check second cluster state" << std::endl;
		for (size_t i = 0; i < kClusterSize; ++i) {
			auto rx = cluster2.GetNode(i)->api.reindexer;
			client::QueryResults qr;
			auto err = rx->Select(Query(kNs1), qr);
			ASSERT_TRUE(err.ok()) << "i = " << i << "; " << err.what();
			ASSERT_EQ(qr.Count(), kClusterSize * kDataPortion);
			qr = client::QueryResults();
			err = rx->Select(Query(kNs2), qr);
			if (i != kReplicatedNodeId) {
				ASSERT_EQ(err.code(), errNotFound) << "i = " << i << "; " << err.what();
			} else {
				ASSERT_TRUE(err.ok()) << "i = " << i << "; " << err.what();
				ASSERT_EQ(qr.Count(), 2 * kClusterSize * kDataPortion);
			}
		}

		client::Item it = cluster2.GetNode(kReplicatedNodeId)->api.reindexer->NewItem(kNs2);
		auto err = it.FromJSON(R"json({"id":1})json");
		ASSERT_TRUE(err.ok()) << err.what();
		err = cluster2.GetNode(kReplicatedNodeId)->api.reindexer->Upsert(kNs2, it);
		ASSERT_EQ(err.code(), errWrongReplicationData);
	});

	loop.run();
}

TEST_F(ClusterOperationAsyncApi, AsyncReplicationBetweenClustersLeaderMode) {
	// Perform replication to each node of the second cluster from leader of the first cluster
	//          cluster1 (ns1, ns2)        cluster2 (ns1)
	// updates -> cl10 - cl11               cl20 - cl21
	//              \    /  async repl(ns2)  \    /
	//              cl12 -------------------> cl22

	net::ev::dynamic_loop loop;
	auto defaults = GetDefaults();
	loop.spawn([&loop, &defaults]() noexcept {
		constexpr size_t kClusterSize = 3;
		constexpr size_t kDataPortion = 20;
		const std::string kNs1 = "ns1";
		const std::string kNs2 = "ns2";
		Cluster cluster1(loop, 0, kClusterSize, defaults);
		Cluster cluster2(loop, kClusterSize, kClusterSize, defaults, {kNs1});
		int leaderId1 = cluster1.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId1, -1);
		const int leaderId2 = cluster2.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId2, -1);

		for (unsigned i = 0; i < kClusterSize; ++i) {
			cluster2.GetNode(i)->MakeFollower();
			for (unsigned j = 0; j < kClusterSize; ++j) {
				cluster1.AddAsyncNode(j, cluster2.GetNode(i), cluster::AsyncReplicationMode::FromClusterLeader, {{kNs2}});
			}
		}

		TestCout() << "Init NS" << std::endl;
		cluster1.InitNs(leaderId1, kNs1);
		cluster1.InitNs(leaderId1, kNs2);
		cluster2.InitNs(leaderId2, kNs1);
		cluster1.WaitSync(kNs1);
		cluster1.WaitSync(kNs2);
		cluster2.WaitSync(kNs1);
		for (size_t i = 0; i < kClusterSize; ++i) {
			cluster1.FillData(i, kNs2, 2 * i * kDataPortion, 2 * kDataPortion);
			cluster2.FillData(i, kNs1, i * kDataPortion, kDataPortion);
		}
		TestCout() << "Wait cluster1 sync" << std::endl;
		cluster1.WaitSync(kNs2);
		TestCout() << "Wait cluster2 sync" << std::endl;
		cluster2.WaitSync(kNs1);
		for (unsigned i = 0; i < kClusterSize; ++i) {
			TestCout() << fmt::format("Wait replication to async node ({})", i) << std::endl;
			ServerControl::WaitSync(cluster1.GetNode(leaderId1), cluster2.GetNode(i), kNs2);
		}

		TestCout() << "Checking replication, when one of the cluster1 nodes is stopped" << std::endl;
		cluster1.StopServer(leaderId1);
		const unsigned deadServerId = unsigned(leaderId1);
		leaderId1 = cluster1.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId1, -1);
		for (unsigned i = 0; i < kClusterSize; ++i) {
			if (i != deadServerId) {
				cluster1.AwaitLeaderBecomeAvailable(i);
				cluster1.FillData(i, kNs2, 10 * (i + 1) * kDataPortion, kDataPortion);
			}
		}
		for (unsigned i = 0; i < kClusterSize; ++i) {
			TestCout() << fmt::format("Wait replication to async node ({})", i) << std::endl;
			ServerControl::WaitSync(cluster1.GetNode(leaderId1), cluster2.GetNode(i), kNs2);
		}

		for (unsigned i = 0; i < kClusterSize; ++i) {
			client::Item it = cluster2.GetNode(i)->api.reindexer->NewItem(kNs2);
			auto err = it.FromJSON(R"json({"id":1})json");
			ASSERT_TRUE(err.ok()) << err.what();
			err = cluster2.GetNode(i)->api.reindexer->Upsert(kNs2, it);
			ASSERT_EQ(err.code(), errWrongReplicationData);
		}
	});

	loop.run();
}
