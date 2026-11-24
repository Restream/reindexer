#include "cluster/consts.h"
#include "cluster_operation_async_api.h"
#include "core/system_ns_names.h"
#include "gtests/tests/gtest_cout.h"
#include "tools/fsops.h"

namespace reindexer_tests {

using namespace reindexer;

TEST_F(ClusterOperationAsyncApi, SynchronizeReplicationRolesDoesNotAffectSyncCluster) {
	net::ev::dynamic_loop loop;
	const auto defaults{GetDefaults()};
	// NOLINTNEXTLINE(bugprone-exception-escape)
	loop.spawn([&loop, &defaults]() noexcept {
		constexpr size_t kClusterSize = 3;
		const std::string clusterNs{"cluster_ns"}, active{"active"}, removed{"removed"};
		Cluster cluster{loop, 0, kClusterSize, defaults, {clusterNs}};
		const auto leaderId{cluster.AwaitLeader(kMaxElectionsTime)};
		ASSERT_NE(leaderId, -1);

		ServerControl asyncServer;
		asyncServer.InitServer(ServerControlConfig{kClusterSize, static_cast<unsigned short>(defaults.defaultRpcPort + kClusterSize),
												   static_cast<unsigned short>(defaults.defaultHttpPort + kClusterSize),
												   fs::JoinPath(defaults.baseTestsetDbPath, "async_leader"), "async_leader"});
		const auto source{asyncServer.Get()};
		AsyncReplicationConfigTest config{"leader"};
		// Match the synchronous leader ID so that the role check, rather than an ID mismatch, protects its namespaces.
		config.serverId = leaderId;
		config.namespaces = {active, removed};
		source->SetReplicationConfig(config);
		for (const auto& ns : {active, removed}) {
			const auto err{source->api.reindexer->OpenNamespace(ns, StorageOpts{}.Enabled())};
			ASSERT_TRUE(err.ok()) << err.what();
			source->api.DefineNamespaceDataset(ns, {IndexDeclaration{"id", "hash", "int", IndexOpts{}.PK(), 0}});
			ASSERT_NO_FATAL_FAILURE(source->api.UpsertJSON(ns, R"({"id":1})"));
		}
		for (size_t node = 0; node < kClusterSize; ++node) {
			const auto target{cluster.GetNode(node)};
			target->MakeFollower();
			source->AddFollower(target);
			ASSERT_NO_FATAL_FAILURE(ServerControl::WaitSync(source, target, active));
			ASSERT_NO_FATAL_FAILURE(ServerControl::WaitSync(source, target, removed));
			target->UpdateConfigReplTokens(NsNamesHashMapT<std::string>{{NamespaceName{removed}, "wrong-token"}});
		}
		config = source->GetServerConfig(ServerControl::ConfigType::Namespace);
		config.namespaces = {active};
		source->SetReplicationConfig(config);
		constexpr std::string_view action{R"({"type":"action","action":{"command":"synchronize_replication_roles"}})"};
		// This synchronous action waits behind the automatic reset in the task queue.
		// Keep the wrong tokens until both operations have finished.
		ASSERT_NO_FATAL_FAILURE(source->api.UpsertJSON(kConfigNamespace, action));
		for (size_t node = 0; node < kClusterSize; ++node) {
			const auto target{cluster.GetNode(node)};
			ASSERT_EQ(target->GetState(removed).role, ClusterOperationStatus::Role::SimpleReplica);
			const auto replicationConfig{fmt::format(R"({{"type":"replication","replication":{{"server_id":{},"cluster_id":2,
				"admissible_replication_tokens":[]}}}})",
													 node)};
			ASSERT_NO_FATAL_FAILURE(target->api.UpsertJSON(kConfigNamespace, replicationConfig));
			EXPECT_EQ(target->api
						  .Select(Query(kConfigNamespace)
									  .Where("type", CondEq, "replication")
									  .Where("replication.server_id", CondEq, int(node))
									  .Where("replication.cluster_id", CondEq, 2)
									  .Where("replication.admissible_replication_tokens", CondEmpty, Variant{}))
						  .Count(),
					  1);
		}
		// Finish token configuration before creating and synchronizing the cluster namespace.
		auto currentLeader{cluster.AwaitLeader(kMaxElectionsTime)};
		ASSERT_NE(currentLeader, -1);
		if (currentLeader != leaderId) {
			ASSERT_NO_FATAL_FAILURE(cluster.ChangeLeader(currentLeader, leaderId));
		}
		ASSERT_NO_FATAL_FAILURE(cluster.InitNs(leaderId, clusterNs));
		ASSERT_NO_FATAL_FAILURE(cluster.FillData(leaderId, clusterNs, 0, 5));
		ASSERT_NO_FATAL_FAILURE(cluster.WaitSync(clusterNs));
		std::vector<ClusterOperationStatus> clusterStatus;
		for (size_t node = 0; node < kClusterSize; ++node) {
			SCOPED_TRACE(node);
			const auto target{cluster.GetNode(node)};
			ReplicationStateV2 state;
			const auto err{target->api.reindexer->GetReplState(clusterNs, state)};
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(state.clusterStatus.role,
					  node == size_t(leaderId) ? ClusterOperationStatus::Role::None : ClusterOperationStatus::Role::ClusterReplica);
			if (state.clusterStatus.role == ClusterOperationStatus::Role::ClusterReplica) {
				ASSERT_EQ(state.clusterStatus.leaderId, config.serverId);
			}
			clusterStatus.emplace_back(state.clusterStatus);
		}

		ASSERT_NO_FATAL_FAILURE(source->api.UpsertJSON(kConfigNamespace, action));
		for (size_t node = 0; node < kClusterSize; ++node) {
			SCOPED_TRACE(node);
			const auto target{cluster.GetNode(node)};
			EXPECT_EQ(target->GetState(removed).role, ClusterOperationStatus::Role::None);
			EXPECT_EQ(target->GetState(active).role, ClusterOperationStatus::Role::SimpleReplica);
			ReplicationStateV2 state;
			ASSERT_TRUE(target->api.reindexer->GetReplState(clusterNs, state).ok());
			EXPECT_EQ(state.clusterStatus.role, clusterStatus[node].role);
			EXPECT_EQ(state.clusterStatus.leaderId, clusterStatus[node].leaderId);
			EXPECT_EQ(target->GetState(clusterNs).dataCount, 5);
		}

		// Writes through every synchronous node must still reach the whole cluster.
		for (size_t node = 0; node < kClusterSize; ++node) {
			const auto writeError{cluster.AddRowWithErr(node, clusterNs, 100 + node, Cluster::DataParam{})};
			ASSERT_TRUE(writeError.ok()) << writeError.what();
		}
		ASSERT_NO_FATAL_FAILURE(cluster.WaitSync(clusterNs));
		ASSERT_NO_FATAL_FAILURE(source->api.UpsertJSON(active, R"({"id":2})"));
		for (size_t node = 0; node < kClusterSize; ++node) {
			const auto target{cluster.GetNode(node)};
			EXPECT_EQ(target->GetState(clusterNs).dataCount, 5 + kClusterSize);
			for (size_t writer = 0; writer < kClusterSize; ++writer) {
				EXPECT_EQ(target->api.Select(Query(clusterNs).Where("id", CondEq, int(100 + writer))).Count(), 1);
			}
			ASSERT_NO_FATAL_FAILURE(ServerControl::WaitSync(source, target, active));
			EXPECT_EQ(target->api.Select(Query(active).Where("id", CondEq, 2)).Count(), 1);
			ASSERT_NO_FATAL_FAILURE(target->api.UpsertJSON(removed, R"({"id":2})"));
			EXPECT_EQ(target->api.Select(Query(removed).Where("id", CondEq, 2)).Count(), 1);
		}
	});
	loop.run();
}

TEST_F(ClusterOperationAsyncApi, AsyncReplicationForClusterNamespaces) {
	// The leader of the cluster replicates to all the async nodes. The last one is added to the config later
	//   cluster (cl1, cl2, cl3) <- updates
	//      |       |        |
	//   async1   async2   async3

	net::ev::dynamic_loop loop;
	auto defaults = GetDefaults();
	// NOLINTNEXTLINE(bugprone-exception-escape)
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

		AsyncReplicationConfigTest config;
		for (size_t i = 0; i < kClusterSize - 1; ++i) {
			asyncNodes[i].Get()->MakeFollower();
			config.nodes.emplace_back(MakeDsn(reindexer_server::UserRole::kRoleReplication, asyncNodes[i].Get()));
		}
		cluster.ApplyAsyncReplicationConfig(leaderId, config);
		TestCout() << "Init NS" << std::endl;
		cluster.InitNs(leaderId, kNsSome);
		cluster.WaitSync(kNsSome);
		TestCout() << "Wait replication to async nodes (2/3)" << std::endl;
		WaitSyncWithExternalNodes(kNsSome, kClusterSize - 1, cluster.GetNode(leaderId));

		const auto kLastNode = kClusterSize - 1;
		asyncNodes[kLastNode].Get()->MakeFollower();
		config.nodes.emplace_back(MakeDsn(reindexer_server::UserRole::kRoleReplication, asyncNodes[kLastNode].Get()));
		cluster.ApplyAsyncReplicationConfig(leaderId, config);
		TestCout() << "Wait replication to the last async node" << std::endl;
		WaitSyncWithExternalNodes(kNsSome, kClusterSize, cluster.GetNode(leaderId));
		TestCout() << "Fill data" << std::endl;
		cluster.FillData(leaderId, kNsSome, 0, kDataPortion);
		cluster.FillDataTx(leaderId, kNsSome, kDataPortion, kDataPortion);
		TestCout() << "Wait cluster sync" << std::endl;
		cluster.WaitSync(kNsSome);
		TestCout() << "Wait replication to async nodes" << std::endl;
		WaitSyncWithExternalNodes(kNsSome, kClusterSize, cluster.GetNode(leaderId));

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
	// The cluster's leader does not replicate to the single node target ('dsn'), whose RAFT cluster contains the replicated namespace,
	// and shows the reason in the target's last error. Replication to the RAFT cluster requires the cluster target ('dsns')
	net::ev::dynamic_loop loop;
	auto defaults = GetDefaults();
	// NOLINTNEXTLINE(bugprone-exception-escape)
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
		cluster1.ApplyAsyncReplicationConfig(
			leaderId1,
			AsyncReplicationConfigTest({{MakeDsn(reindexer_server::UserRole::kRoleReplication, cluster2.GetNode(kReplicatedNodeId)),
										 AsyncReplicationConfigTest::NsSet{namespaceName}}}));

		ServerControl::Interface::Ptr node = cluster1.GetNode(leaderId1);
		cluster::ReplicationStats stats;
		AwaitCondition(
			std::chrono::seconds(10),
			[&] {
				stats = node->GetReplicationStats(cluster::kAsyncReplStatsType);
				return stats.nodeStats.size() == 1 && stats.nodeStats[0].lastError.what() == expectedError;
			},
			[&] { return StatsJSON(stats); });
		ASSERT_EQ(stats.nodeStats.size(), std::size_t(1));
		ASSERT_EQ(stats.nodeStats[0].lastError.code(), errParams);
	});

	loop.run();
}

TEST_F(ClusterOperationAsyncApi, AsyncReplicationBetweenClustersAllTargets) {
	// Perform replication to each node of the second cluster from leader of the first cluster
	//          cluster1 (ns1, ns2)        cluster2 (ns1)
	// updates -> cl10 - cl11               cl20 - cl21
	//              \    /  async repl(ns2)  \    /
	//              cl12 -------------------> cl22

	net::ev::dynamic_loop loop;
	auto defaults = GetDefaults();
	// NOLINTNEXTLINE(bugprone-exception-escape)
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

		std::vector<AsyncReplicationConfigTest::Node> targets;
		targets.reserve(kClusterSize);
		for (unsigned i = 0; i < kClusterSize; ++i) {
			cluster2.GetNode(i)->MakeFollower();
			targets.emplace_back(MakeDsn(reindexer_server::UserRole::kRoleReplication, cluster2.GetNode(i)),
								 AsyncReplicationConfigTest::NsSet{kNs2});
		}
		cluster1.ApplyAsyncReplicationConfig(leaderId1, AsyncReplicationConfigTest(std::move(targets)));

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

}  // namespace reindexer_tests
