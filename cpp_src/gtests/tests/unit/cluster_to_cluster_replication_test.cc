#include <initializer_list>
#include <tuple>
#include <utility>
#include "async_repl_tools.h"
#include "client/cororeindexer.h"
#include "cluster/consts.h"
#include "cluster_to_cluster_replication_api.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "tools/fsops.h"
#include "yaml-cpp/yaml.h"

namespace reindexer_tests {

using namespace reindexer;
using reindexer_tests_tools::exceptionWrapper;
using cluster::NodeStats;
using NsSet = AsyncReplicationConfigTest::NsSet;
using ClusterT = ClusterOperationApi::Cluster;

namespace {

constexpr size_t kClusterSize = 3;
constexpr size_t kDataCount = 20;
constexpr auto kSyncTime = kAsyncReplSyncTime;

void awaitNamespaceOwnership(ClusterT& cluster, std::string_view ns, IsPrimary isPrimary) {
	for (size_t i = 0; i < cluster.Size(); ++i) {
		if (auto node = cluster.GetNode(i); node) {
			Error err;
			AwaitCondition(
				kSyncTime,
				[&] {
					ReplicationStateV2 state;
					err = node->api.reindexer->GetReplState(ns, state);
					return err.ok() && (state.clusterStatus.isPrimary == *isPrimary);
				},
				[&] {
					return fmt::format("Node {}, ns '{}': {}", i, ns, err.ok() ? fmt::format("is_primary={}", !*isPrimary) : err.whatStr());
				});
		}
	}
}

// Awaits, until all the running nodes of the secondary cluster have the same data as the primary cluster's leader, and the namespace
// is owned by the primary cluster
void awaitClusterReplication(ClusterT& primary, ClusterT& secondary, std::string_view ns) {
	const int leaderId = AwaitClusterLeader(primary);
	ASSERT_NE(leaderId, -1);
	primary.WaitSync(ns, lsn_t(), lsn_t(), kSyncTime);
	for (size_t i = 0; i < secondary.Size(); ++i) {
		if (auto node = secondary.GetNode(i); node) {
			ServerControl::WaitSync(primary.GetNode(leaderId), node, std::string(ns), kSyncTime);
		}
	}
	awaitNamespaceOwnership(secondary, ns, IsPrimary_False);
}

void awaitNamespaceDropped(ClusterT& cluster, std::string_view ns) {
	for (size_t i = 0; i < cluster.Size(); ++i) {
		if (auto node = cluster.GetNode(i); node) {
			AwaitCondition(
				kSyncTime, [&] { return node->api.EnumNamespaces(EnumNamespacesOpts().WithFilter(ns)).empty(); },
				[&] { return fmt::format("Node {}, ns '{}' is not dropped", i, ns); });
		}
	}
}

std::string itemJSON(int id, std::string_view value) { return fmt::format(R"json({{"id":{},"string":"{}"}})json", id, value); }

// User's modifications of the namespaces, replicated from the primary cluster, are forbidden on each node of the secondary
void checkReadOnly(const ServerControl::Interface::Ptr& node, std::string_view ns) {
	auto item = node->api.NewItem(ns);
	ASSERT_TRUE(item.FromJSON(itemJSON(100500, "forbidden")).ok());
	auto err = node->api.reindexer->Upsert(ns, item);
	EXPECT_EQ(err.code(), errForbidden) << err.what();
	BaseApi::QueryResultsType qr;
	err = node->api.reindexer->Delete(Query(ns).Where("id", CondGe, 0), qr);
	EXPECT_EQ(err.code(), errForbidden) << err.what();
	err = node->api.reindexer->TruncateNamespace(ns);
	EXPECT_EQ(err.code(), errForbidden) << err.what();
	err = node->api.reindexer->AddIndex(ns, IndexDef{"new_idx", {"new_idx"}, "hash", "int", IndexOpts()});
	EXPECT_EQ(err.code(), errForbidden) << err.what();
	err = node->api.reindexer->SetSchema(ns, R"json({"type":"object","properties":{"id":{"type":"integer"}}})json");
	EXPECT_EQ(err.code(), errForbidden) << err.what();
	err = node->api.reindexer->PutMeta(ns, "key", "forbidden");
	EXPECT_EQ(err.code(), errForbidden) << err.what();
	// Namespaces of the RAFT cluster can not be renamed at all
	err = node->api.reindexer->RenameNamespace(ns, std::string(ns) + "_renamed");
	EXPECT_EQ(err.code(), errParams) << err.what();
	err = node->api.reindexer->DropNamespace(ns);
	EXPECT_EQ(err.code(), errForbidden) << err.what();
}

// Awaits the error of the first target, containing 'error'. The cluster target's error is awaited from its leader 'dsn', if it is set
cluster::ReplicationStats awaitReplicationError(const ServerControl::Interface::Ptr& node, std::string_view error = {},
												const std::optional<DSN>& dsn = {}) {
	cluster::ReplicationStats stats;
	AwaitCondition(
		kSyncTime,
		[&] {
			stats = node->GetReplicationStats(cluster::kAsyncReplStatsType);
			if (stats.nodeStats.empty()) {
				return false;
			}
			const auto& target = stats.nodeStats[0];
			if (target.lastError.ok() || target.status == NodeStats::Status::Online || (dsn && target.dsn != *dsn) ||
				target.lastError.whatStr().find(error) == std::string::npos) {
				return false;
			}
			TestCout() << "Replication error: " << target.lastError.what() << std::endl;
			return true;
		},
		[&] { return fmt::format("Error '{}' was expected{}: {}", error, dsn ? fmt::format(" from {}", *dsn) : "", StatsJSON(stats)); });
	return stats;
}

cluster::ReplicationStats awaitAsyncOnline(const ServerControl::Interface::Ptr& node, size_t targetsCount) {
	cluster::ReplicationStats stats;
	AwaitCondition(
		kSyncTime,
		[&] {
			stats = node->GetReplicationStats(cluster::kAsyncReplStatsType);
			return !stats.nodeStats.empty() && stats.nodeStats.size() == targetsCount && stats.pendingUpdatesCount == 0 &&
				   std::ranges::all_of(stats.nodeStats, [](const NodeStats& target) {
					   return target.syncState == NodeStats::SyncState::OnlineReplication && target.lastError.ok();
				   });
		},
		[&] {
			return fmt::format("Node {}, expected {} online targets and an empty queue: {}", node->Id(), targetsCount, StatsJSON(stats));
		});
	return stats;
}

void checkRows(const ServerControl::Interface::Ptr& node, std::string_view ns,
			   std::initializer_list<std::pair<int, std::string_view>> expected) {
	SCOPED_TRACE(fmt::format("Node {}, ns '{}'", node->Id(), ns));
	auto qr = node->api.Select(Query(ns).Sort("id", SortOrder::Asc));
	ASSERT_EQ(qr.Count(), expected.size());
	auto it = qr.begin();
	for (const auto& [id, value] : expected) {
		WrSerializer ser;
		const auto err = it.GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		const auto root = parser.Parse(ser.Slice());
		EXPECT_EQ(root["id"].As<int>(), id);
		EXPECT_EQ(root["string"].As<std::string_view>(), value);
		++it;
	}
}

}  // namespace

TEST_F(ClusterToClusterReplicationApi, BasicReplication) {
	// Namespaces, indexes and data of the primary cluster are replicated to all the nodes of the secondary cluster. Server IDs are unique
	// inside each cluster only
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_basic";
		const auto secondaryDefaults =
			Defaults{defaults.defaultRpcPort + 100, defaults.defaultHttpPort + 100, fs::JoinPath(defaults.baseTestsetDbPath, "secondary")};
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, 0, kClusterSize, secondaryDefaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);

		const auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kNs});
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		AwaitAsyncReplicationConfig(primary, config);

		primary.InitNs(primaryLeader, kNs);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		primary.FillDataTx(primaryLeader, kNs, kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);

		const auto stats = primary.GetNode(primaryLeader)->GetReplicationStats(cluster::kAsyncReplStatsType);
		ASSERT_EQ(stats.nodeStats.size(), 1);
		EXPECT_EQ(stats.nodeStats[0].dsn, config.nodes[0].dsns[secondaryLeader]);
		EXPECT_EQ(stats.nodeStats[0].dsns, config.nodes[0].dsns);
		EXPECT_EQ(stats.nodeStats[0].status, NodeStats::Status::Online);
		EXPECT_EQ(stats.nodeStats[0].syncState, NodeStats::SyncState::OnlineReplication);
		// The config's source is shown for the RAFT cluster's nodes only
		EXPECT_FALSE(stats.nodeStats[0].asyncReplConfig.has_value());
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, InitialSyncOfExistingData) {
	// Initial sync of the existing data. Data of the primary cluster overwrites the secondary cluster's data
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kPrimaryOnlyNs = "ns_primary_only";
		const std::string kEmptyOnSecondaryNs = "ns_empty_on_secondary";
		const std::string kNonConflictingNs = "ns_non_conflicting";
		const std::string kConflictingNs = "ns_conflicting";
		const NsSet kNamespaces{kPrimaryOnlyNs, kEmptyOnSecondaryNs, kNonConflictingNs, kConflictingNs};
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);

		for (const auto& ns : kNamespaces) {
			primary.InitNs(primaryLeader, ns);
			primary.FillData(primaryLeader, ns, 0, kDataCount);
		}
		secondary.InitNs(secondaryLeader, kEmptyOnSecondaryNs);
		secondary.InitNs(secondaryLeader, kNonConflictingNs);
		secondary.FillData(secondaryLeader, kNonConflictingNs, 1000, kDataCount);
		secondary.InitNs(secondaryLeader, kConflictingNs);
		for (int id = 0; id < int(kDataCount); ++id) {
			secondary.GetNode(secondaryLeader)->api.UpsertJSON(kConflictingNs, itemJSON(id, "secondary"));
		}

		primary.ApplyAsyncReplicationConfig(primaryLeader, ClusterT::ClusterTargetConfig(secondary, kNamespaces));
		for (const auto& ns : kNamespaces) {
			awaitClusterReplication(primary, secondary, ns);
		}
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, SecondaryNamespacesAccess) {
	// Namespaces, replicated from the primary cluster, are read-only on the secondary cluster, while its own namespaces may be created,
	// modified and dropped. Updates of the replicated namespaces are accepted from the primary cluster by the secondary cluster's leader
	// only and from the current RAFT leader by the followers
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kReplicatedNs = "ns_replicated";
		const std::string kOwnNs = "ns_own";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);

		primary.ApplyAsyncReplicationConfig(primaryLeader, ClusterT::ClusterTargetConfig(secondary, NsSet{kReplicatedNs}));
		primary.InitNs(primaryLeader, kReplicatedNs);
		primary.FillData(primaryLeader, kReplicatedNs, 0, kDataCount);
		awaitClusterReplication(primary, secondary, kReplicatedNs);
		for (size_t i = 0; i < secondary.Size(); ++i) {
			checkReadOnly(secondary.GetNode(i), kReplicatedNs);
		}

		secondary.InitNs(secondaryLeader, kOwnNs);
		secondary.FillData(secondaryLeader, kOwnNs, 0, kDataCount);
		secondary.WaitSync(kOwnNs);
		awaitNamespaceOwnership(secondary, kOwnNs, IsPrimary_True);
		const size_t follower = (secondaryLeader + 1) % kClusterSize;
		secondary.GetNode(follower)->api.UpsertJSON(kOwnNs, itemJSON(100500, "own"));
		secondary.DropNs(follower, kOwnNs);

		// Updates from the primary cluster and from the stale RAFT leader to the secondary's follower
		for (const auto sender :
			 {ReplicationSender{.clusterTarget = true}, ReplicationSender{.clusterLeaderId = int(kClusterSize + follower)}}) {
			const auto followerNode = secondary.GetNode(follower);
			client::Reindexer client(client::ReindexerConfig{});
			auto err = client.Connect(MakeDsn(reindexer_server::UserRole::kRoleReplication, followerNode),
									  client::ConnectOpts().WithReplicationSender(sender));
			ASSERT_TRUE(err.ok()) << err.what();
			ReplicationStateV2 state;
			err = client.GetReplState(kReplicatedNs, state);
			ASSERT_TRUE(err.ok()) << err.what();
			auto item = client.NewItem(kReplicatedNs);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			ASSERT_TRUE(item.FromJSON(R"json({"id":100500,"string":"from_primary"})json").ok());
			// Requests with LSN require the deadline, as the replicator's ones
			err = client.WithLSN(lsn_t(state.lastLsn.Counter() + 1, primaryLeader)).WithTimeout(kSyncTime).Upsert(kReplicatedNs, item);
			EXPECT_EQ(err.code(), errWrongReplicationData) << err.what();
		}

		primary.FillData(primaryLeader, kReplicatedNs, kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kReplicatedNs);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, ForeignNamespacesProtection) {
	// Secondary cluster's leader rejects the primary cluster's DDL for the namespaces outside its RAFT config without creating them, and
	// the primary's updates of such namespaces, even of the replicas of the other nodes
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kClusterNs = "ns_cluster";
		const std::string kLocalNs = "ns_local";
		const std::string kForeignNs = "ns_foreign";
		constexpr int kPrimaryServerId = int(kClusterSize);
		ClusterT secondary(loop, 0, kClusterSize, defaults, {kClusterNs});
		const int leaderId = AwaitClusterLeader(secondary);
		// Cluster namespace's creation awaits the leader's initial sync
		secondary.InitNs(leaderId, kClusterNs);
		secondary.InitNs(leaderId, kLocalNs);

		client::CoroReindexer primaryClient, adminClient;
		auto err = primaryClient.Connect(MakeDsn(reindexer_server::UserRole::kRoleReplication, secondary.GetNode(leaderId)), loop,
										 client::ConnectOpts().WithReplicationSender(ReplicationSender{.clusterTarget = true}));
		ASSERT_TRUE(err.ok()) << err.what();
		const auto expectNotInRaftConfig = [](const Error& error, std::string_view ns) {
			EXPECT_EQ(error.code(), errParams) << error.what();
			EXPECT_EQ(error.whatStr(), fmt::format("Namespace '{}' is not in the RAFT config of the secondary cluster", ns));
		};
		// Requests with LSN require the deadline, as the replicator's ones
		err = primaryClient.WithLSN(lsn_t(0, kPrimaryServerId)).WithTimeout(kSyncTime).AddNamespace(NamespaceDef(kForeignNs));
		expectNotInRaftConfig(err, kForeignNs);
		std::string tmpNs;
		err = primaryClient.WithLSN(lsn_t(0, kPrimaryServerId)).WithTimeout(kSyncTime).CreateTemporaryNamespace(kForeignNs, tmpNs);
		expectNotInRaftConfig(err, kForeignNs);

		err = adminClient.Connect(secondary.GetNode(leaderId)->kRPCDsn, loop);
		ASSERT_TRUE(err.ok()) << err.what();
		err = adminClient.SetClusterOperationStatus(
			kLocalNs, ClusterOperationStatus{{.leaderId = kPrimaryServerId + 1, .role = ClusterOperationStatus::Role::SimpleReplica}});
		ASSERT_TRUE(err.ok()) << err.what();
		ReplicationStateV2 state;
		err = adminClient.GetReplState(kLocalNs, state);
		ASSERT_TRUE(err.ok()) << err.what();
		const lsn_t lsn(state.lastLsn.Counter() + 1, kPrimaryServerId);
		auto item = primaryClient.NewItem(kLocalNs);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		ASSERT_TRUE(item.FromJSON(itemJSON(100500, "from_primary")).ok());
		err = primaryClient.WithLSN(lsn).WithTimeout(kSyncTime).Upsert(kLocalNs, item);
		expectNotInRaftConfig(err, kLocalNs);
		err = primaryClient.WithLSN(lsn).WithTimeout(kSyncTime).PutMeta(kLocalNs, "key", "from_primary");
		expectNotInRaftConfig(err, kLocalNs);
		auto tx = primaryClient.WithLSN(lsn).WithTimeout(kSyncTime).NewTransaction(kLocalNs);
		ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
		auto txItem = tx.NewItem();
		ASSERT_TRUE(txItem.FromJSON(itemJSON(100500, "from_primary")).ok());
		err = tx.Upsert(std::move(txItem), lsn);
		ASSERT_TRUE(err.ok()) << err.what();
		client::CoroQueryResults qr;
		err = primaryClient.WithLSN(lsn).WithTimeout(kSyncTime).CommitTransaction(tx, qr);
		expectNotInRaftConfig(err, kLocalNs);
		primaryClient.Stop();
		adminClient.Stop();

		// Storage, left by the rejected DDL, would be loaded as the namespace on the restart
		ASSERT_TRUE(secondary.StopServer(leaderId));
		ASSERT_TRUE(secondary.StartServer(leaderId));
		err = secondary.GetNode(leaderId)->api.reindexer->GetReplState(kForeignNs, state);
		EXPECT_EQ(err.code(), errNotFound) << err.what();
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, NamespaceRemovedFromSecondaryRaftConfig) {
	// Primary's updates of the namespace, removed from the secondary cluster's RAFT config, are rejected even by the former leader, which
	// applied them before, so the nodes do not diverge. The namespace stays read-only, until its role is reset on each node
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kKeptNs = "ns_kept";
		const std::string kRemovedNs = "ns_removed";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults, {kKeptNs, kRemovedNs});
		const int primaryLeader = AwaitClusterLeader(primary);
		const int formerLeader = AwaitClusterLeader(secondary);

		auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kKeptNs, kRemovedNs});
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		for (const auto& ns : {kKeptNs, kRemovedNs}) {
			primary.InitNs(primaryLeader, ns);
			primary.FillData(primaryLeader, ns, 0, kDataCount);
			awaitClusterReplication(primary, secondary, ns);
		}

		std::vector<std::string> configPaths;
		configPaths.reserve(secondary.Size());
		for (size_t i = 0; i < secondary.Size(); ++i) {
			configPaths.emplace_back(secondary.GetNode(i)->GetClusterConfigFilePath());
		}
		secondary.StopServers(0, kClusterSize);
		for (const auto& path : configPaths) {
			YAML::Node clusterConf = YAML::LoadFile(path);
			clusterConf["namespaces"] = std::vector<std::string>{kKeptNs};
			ASSERT_GT(fs::WriteFile(path, YAML::Dump(clusterConf)), 0) << path;
		}
		for (size_t i = 0; i < secondary.Size(); ++i) {
			ASSERT_TRUE(secondary.StartServer(i));
		}
		int leader = formerLeader;
		secondary.ChangeLeader(leader, formerLeader);

		// The namespace is removed from the RAFT config, so the secondary cluster rejects the updates
		primary.FillData(primaryLeader, kRemovedNs, kDataCount, kDataCount);
		std::ignore = awaitReplicationError(primary.GetNode(primaryLeader),
											fmt::format("Namespace '{}' is not in the RAFT config of the secondary cluster", kRemovedNs),
											config.nodes[0].dsns[formerLeader]);
		for (size_t i = 0; i < secondary.Size(); ++i) {
			const auto node = secondary.GetNode(i);
			EXPECT_EQ(node->api.Select(Query(kRemovedNs)).Count(), kDataCount) << "Node " << i;
			const auto err = TryUpsertJSON(node->api, kRemovedNs, itemJSON(100500, "local"));
			EXPECT_EQ(err.code(), int(i) == formerLeader ? errForbidden : errWrongReplicationData) << "Node " << i << ": " << err.what();
		}

		// Excluded from the primary's config, the namespace stops blocking the replication and becomes writable after the role reset
		config = ClusterT::ClusterTargetConfig(secondary, NsSet{kKeptNs});
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		for (size_t i = 0; i < secondary.Size(); ++i) {
			const auto node = secondary.GetNode(i);
			node->ResetReplicationRole(kRemovedNs);
			node->api.UpsertJSON(kRemovedNs, itemJSON(100500, "local"));
		}
		primary.FillData(primaryLeader, kKeptNs, kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kKeptNs);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, NamespaceCreatedOnPrimaryOverwritesSecondary) {
	// Namespace, created on the primary cluster during the online replication (not on the initial sync), overwrites the secondary
	// cluster's namespace with the same name
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_overwritten";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);

		primary.ApplyAsyncReplicationConfig(primaryLeader, ClusterT::ClusterTargetConfig(secondary, NsSet{kNs}));
		secondary.InitNs(secondaryLeader, kNs);
		secondary.FillData(secondaryLeader, kNs, 0, 2 * kDataCount);
		secondary.WaitSync(kNs);

		primary.InitNs(primaryLeader, kNs);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, PrimaryLeaderChange) {
	// Replication continues after the primary cluster's leader switch and after the leader's failure. The namespace, dropped on the primary
	// cluster, while the secondary cluster is stopped, is dropped on the secondary cluster by the new leader
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_primary_leader_change";
		const std::string kDroppedNs = "ns_primary_leader_change_dropped";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		int primaryLeader = AwaitClusterLeader(primary);
		std::ignore = AwaitClusterLeader(secondary);

		const auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kNs, kDroppedNs});
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		// Followers queue the async updates only after applying the config
		AwaitAsyncReplicationConfig(primary, config);
		primary.InitNs(primaryLeader, kNs);
		primary.InitNs(primaryLeader, kDroppedNs);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
		awaitClusterReplication(primary, secondary, kDroppedNs);

		TestCout() << "Stopping secondary cluster" << std::endl;
		secondary.StopServers(0, kClusterSize);
		primary.DropNs(primaryLeader, kDroppedNs);
		awaitNamespaceDropped(primary, kDroppedNs);
		primary.ChangeLeader(primaryLeader, (primaryLeader + 1) % kClusterSize);
		for (size_t i = 0; i < kClusterSize; ++i) {
			ASSERT_TRUE(secondary.StartServer(i));
		}
		awaitNamespaceDropped(secondary, kDroppedNs);
		primary.FillData(primaryLeader, kNs, kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);

		TestCout() << "Stopping primary's leader" << std::endl;
		ASSERT_TRUE(primary.StopServer(primaryLeader));
		primaryLeader = AwaitClusterLeader(primary);
		primary.FillData(primaryLeader, kNs, 2 * kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, FormerLeaderAsyncReplication) {
	// Former leader suspends the async replication right after the leader's switch, even without the updates: the connection is closed and
	// the queued updates are discarded. New leader replicates right after its initial sync without awaiting the retry interval. Replication
	// continues after the leadership's return. The unavailable target does not keep the updates of the namespaces, which it does not
	// replicate, in the leader's queue
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_former_leader";
		const std::string kUnavailableTargetNs = "ns_former_leader_unavailable";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);
		const auto awaitTargetStats = [](const ServerControl::Interface::Ptr& node, auto&& condition) {
			cluster::ReplicationStats stats;
			AwaitCondition(
				kSyncTime,
				[&] {
					stats = node->GetReplicationStats(cluster::kAsyncReplStatsType);
					return (stats.nodeStats.size() == 2) && condition(stats);
				},
				[&] { return fmt::format("Node {}: {}", node->Id(), StatsJSON(stats)); });
		};
		const auto isOnline = [](const cluster::ReplicationStats& stats) { return stats.nodeStats[0].status == NodeStats::Status::Online; };
		const auto isQueueEmpty = [](const cluster::ReplicationStats& stats) { return stats.pendingUpdatesCount == 0; };
		const auto isSuspended = [](const cluster::ReplicationStats& stats) {
			return stats.pendingUpdatesCount == 0 && std::ranges::all_of(stats.nodeStats, [](const NodeStats& target) {
					   return (target.syncState == NodeStats::SyncState::Suspended) && (target.status == NodeStats::Status::None) &&
							  target.lastError.ok();
				   });
		};

		auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kNs});
		config.nodes.emplace_back(DSN("cproto://127.0.0.1:1/former_leader"), NsSet{kUnavailableTargetNs});
		// Setting the large timeout to assure, that the suspended replication is resumed without awaiting
		config.retrySyncIntervalMSec = 600'000;
		// So the first sync has to succeed: the secondary cluster passes the validation, when all of its nodes are synchronized
		AwaitCondition(
			kSyncTime, [&] { return secondary.GetSynchronizedNodesCount(secondaryLeader) == kClusterSize; },
			[] { return std::string("Secondary cluster is not synchronized"); });
		primary.InitNs(primaryLeader, kNs);
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		AwaitAsyncReplicationConfig(primary, config);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
		awaitTargetStats(primary.GetNode(primaryLeader), isOnline);
		awaitTargetStats(primary.GetNode(primaryLeader), isQueueEmpty);

		const int formerLeader = primaryLeader;
		const int neverLeader = (primaryLeader + 2) % kClusterSize;
		primary.ChangeLeader(primaryLeader, (primaryLeader + 1) % kClusterSize);
		awaitTargetStats(primary.GetNode(formerLeader), isSuspended);
		awaitTargetStats(primary.GetNode(primaryLeader), isOnline);

		primary.FillData(primaryLeader, kNs, kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
		awaitTargetStats(primary.GetNode(primaryLeader), isQueueEmpty);
		awaitTargetStats(primary.GetNode(formerLeader), isSuspended);
		awaitTargetStats(primary.GetNode(neverLeader), isSuspended);

		primary.ChangeLeader(primaryLeader, formerLeader);
		primary.FillData(primaryLeader, kNs, 2 * kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, SecondaryLeaderChange) {
	// Replication continues after the secondary cluster's leader switch: the new leader becomes the target's active DSN, even if the former
	// leader is alive. The new leader keeps the namespace read-only
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_secondary_leader_change";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		int secondaryLeader = AwaitClusterLeader(secondary);

		const auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kNs});
		const auto expectActiveDsn = [&] {
			const auto stats = primary.GetNode(primaryLeader)->GetReplicationStats(cluster::kAsyncReplStatsType);
			ASSERT_EQ(stats.nodeStats.size(), 1);
			EXPECT_EQ(stats.nodeStats[0].dsn, config.nodes[0].dsns[secondaryLeader]);
		};
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		primary.InitNs(primaryLeader, kNs);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
		expectActiveDsn();

		secondary.ChangeLeader(secondaryLeader, (secondaryLeader + 1) % kClusterSize);
		checkReadOnly(secondary.GetNode(secondaryLeader), kNs);
		primary.FillData(primaryLeader, kNs, kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
		expectActiveDsn();

		TestCout() << "Stopping secondary's leader" << std::endl;
		ASSERT_TRUE(secondary.StopServer(secondaryLeader));
		secondaryLeader = AwaitClusterLeader(secondary);
		checkReadOnly(secondary.GetNode(secondaryLeader), kNs);
		primary.FillData(primaryLeader, kNs, 2 * kDataCount, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);
		expectActiveDsn();
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, ReplicationTokens) {
	// Replication tokens of the secondary cluster restrict the updates of the primary cluster only. Token's change on the secondary cluster
	// forbids the replication with the tokens' mismatch error, but the namespace stays owned by the primary cluster
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_token";
		const std::string kDeniedNs = "ns_token_denied";
		const std::string kOwnNs = "ns_own_token";
		const std::string kToken = "primary_token";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);

		for (size_t i = 0; i < secondary.Size(); ++i) {
			secondary.GetNode(i)->UpdateConfigReplTokens(
				NsNamesHashMapT<std::string>{{NamespaceName(kNs), kToken}, {NamespaceName(kOwnNs), "secondary_token"}});
		}
		secondary.InitNs(secondaryLeader, kOwnNs);
		secondary.FillData(secondaryLeader, kOwnNs, 0, kDataCount);
		secondary.WaitSync(kOwnNs);

		// Check data insertion with valid tokens
		auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kNs, kDeniedNs});
		config.selfReplicationToken = kToken;
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		for (const auto& ns : {kNs, kDeniedNs}) {
			primary.InitNs(primaryLeader, ns);
			primary.FillData(primaryLeader, ns, 0, kDataCount);
			awaitClusterReplication(primary, secondary, ns);
		}

		// Check data insertion with invalid tokens
		for (size_t i = 0; i < secondary.Size(); ++i) {
			secondary.GetNode(i)->UpdateConfigReplTokens(NsNamesHashMapT<std::string>{{NamespaceName(kDeniedNs), "wrong_token"}});
		}
		primary.FillData(primaryLeader, kDeniedNs, kDataCount, kDataCount);
		std::ignore = awaitReplicationError(primary.GetNode(primaryLeader), "Different replication tokens");
		// Check data is not replicated
		for (size_t i = 0; i < secondary.Size(); ++i) {
			EXPECT_EQ(secondary.GetNode(i)->api.Select(Query(kDeniedNs)).Count(), kDataCount);
		}
		awaitNamespaceOwnership(secondary, kDeniedNs, IsPrimary_False);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, LeaveSecondaryCluster) {
	// Secondary cluster leaves the replication without the primary cluster's notification: its new replication tokens forbid the primary
	// cluster to take the namespace over again. The primary cluster's leader is not switched, and offline nodes get the namespace's
	// ownership on reconnect
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_leave";
		const NsNamesHashMapT<std::string> kTokens{{NamespaceName(kNs), "secondary_token"}};
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);

		primary.ApplyAsyncReplicationConfig(primaryLeader, ClusterT::ClusterTargetConfig(secondary, NsSet{kNs}));
		primary.InitNs(primaryLeader, kNs);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);

		const size_t offlineFollower = (secondaryLeader + 1) % kClusterSize;
		ASSERT_TRUE(secondary.StopServer(offlineFollower));
		for (size_t i = 0; i < secondary.Size(); ++i) {
			if (auto node = secondary.GetNode(i); node) {
				node->UpdateConfigReplTokens(kTokens);
			}
		}
		// Tokens' update restarts RAFT on each node, so the secondary cluster's leader may change
		const int leader = AwaitClusterLeader(secondary);
		ASSERT_NE(leader, -1);
		secondary.GetNode(leader)->ResetReplicationRole();
		EXPECT_EQ(secondary.AwaitLeader(std::chrono::seconds(3), true), leader);
		awaitNamespaceOwnership(secondary, kNs, IsPrimary_True);
		secondary.FillData(leader, kNs, kDataCount, kDataCount);

		primary.FillData(primaryLeader, kNs, 2 * kDataCount, kDataCount);
		std::ignore =
			awaitReplicationError(primary.GetNode(primaryLeader), fmt::format("Unable to set cluster operation status for '{}'", kNs));
		awaitNamespaceOwnership(secondary, kNs, IsPrimary_True);

		ASSERT_TRUE(secondary.StartServer(offlineFollower));
		// The follower may become the leader, so it needs the tokens as well
		secondary.GetNode(offlineFollower)->UpdateConfigReplTokens(kTokens);
		secondary.WaitSync(kNs, lsn_t(), lsn_t(), kSyncTime);
		awaitNamespaceOwnership(secondary, kNs, IsPrimary_True);
		secondary.GetNode(offlineFollower)->api.UpsertJSON(kNs, itemJSON(100500, "after_leave"));
		secondary.WaitSync(kNs, lsn_t(), lsn_t(), kSyncTime);
		EXPECT_EQ(secondary.GetNode(offlineFollower)->api.Select(Query(kNs)).Count(), 2 * kDataCount + 1);
		EXPECT_EQ(AwaitClusterLeader(primary), primaryLeader);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, RoleSwapRoundTrip) {
	// Reversing replication makes the former primary read-only and replaces its divergent data with the new primary's data.
	// Replication after failback preserves the promoted cluster's data and delivers new writes after initial sync.
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_role_swap";
		ClusterT clusterA(loop, 0, kClusterSize, defaults);
		ClusterT clusterB(loop, kClusterSize, kClusterSize, defaults);
		const int leaderA = AwaitClusterLeader(clusterA);
		const int leaderB = AwaitClusterLeader(clusterB);
		const auto configA = ClusterT::ClusterTargetConfig(clusterB, NsSet{kNs});
		const auto configB = ClusterT::ClusterTargetConfig(clusterA, NsSet{kNs});
		auto stoppedConfig = configA;
		stoppedConfig.nodes.clear();

		clusterA.InitNs(leaderA, kNs);
		clusterA.GetNode(leaderA)->api.UpsertJSON(kNs, itemJSON(0, "base_0"));
		clusterA.GetNode(leaderA)->api.UpsertJSON(kNs, itemJSON(1, "base_1"));
		clusterA.ApplyAsyncReplicationConfig(leaderA, configA);
		awaitClusterReplication(clusterA, clusterB, kNs);
		std::ignore = awaitAsyncOnline(clusterA.GetNode(leaderA), 1);

		{
			SCOPED_TRACE("Promote B and replace divergent A data");
			clusterA.ApplyAsyncReplicationConfig(leaderA, stoppedConfig);
			AwaitAsyncReplicationConfig(clusterA, stoppedConfig);
			for (int id = 100; id < 103; ++id) {
				clusterA.GetNode(leaderA)->api.UpsertJSON(kNs, itemJSON(id, fmt::format("old_a_{}", id - 99)));
			}
			clusterA.WaitSync(kNs, lsn_t(), lsn_t(), kSyncTime);
			clusterB.GetNode(leaderB)->ResetReplicationRole(kNs);
			awaitNamespaceOwnership(clusterB, kNs, IsPrimary_True);
			clusterB.GetNode(leaderB)->api.UpsertJSON(kNs, itemJSON(200, "new_b"));
			clusterB.WaitSync(kNs, lsn_t(), lsn_t(), kSyncTime);
			checkRows(clusterB.GetNode(leaderB), kNs, {{0, "base_0"}, {1, "base_1"}, {200, "new_b"}});

			const auto stateA = clusterA.GetNode(leaderA)->GetState(kNs);
			const auto stateB = clusterB.GetNode(leaderB)->GetState(kNs);
			ASSERT_EQ(stateA.nsVersion, stateB.nsVersion);
			ASSERT_GT(stateA.lsn.Counter(), stateB.lsn.Counter());

			clusterB.ApplyAsyncReplicationConfig(leaderB, configB);
			awaitClusterReplication(clusterB, clusterA, kNs);
			checkRows(clusterA.GetNode(leaderA), kNs, {{0, "base_0"}, {1, "base_1"}, {200, "new_b"}});
			const auto err = TryUpsertJSON(clusterA.GetNode(leaderA)->api, kNs, itemJSON(100500, "forbidden"));
			EXPECT_EQ(err.code(), errForbidden) << err.what();
		}

		{
			SCOPED_TRACE("Restore A -> B, then deliver a new write");
			std::ignore = awaitAsyncOnline(clusterB.GetNode(leaderB), 1);
			clusterB.ApplyAsyncReplicationConfig(leaderB, stoppedConfig);
			AwaitAsyncReplicationConfig(clusterB, stoppedConfig);
			clusterA.GetNode(leaderA)->ResetReplicationRole(kNs);
			awaitNamespaceOwnership(clusterA, kNs, IsPrimary_True);
			clusterA.ApplyAsyncReplicationConfig(leaderA, configA);
			awaitClusterReplication(clusterA, clusterB, kNs);
			std::ignore = awaitAsyncOnline(clusterA.GetNode(leaderA), 1);
			clusterA.GetNode(leaderA)->api.UpsertJSON(kNs, itemJSON(300, "after_failback"));
			awaitClusterReplication(clusterA, clusterB, kNs);
			checkRows(clusterB.GetNode(leaderB), kNs, {{0, "base_0"}, {1, "base_1"}, {200, "new_b"}, {300, "after_failback"}});
			const auto err = TryUpsertJSON(clusterB.GetNode(leaderB)->api, kNs, itemJSON(100500, "forbidden"));
			EXPECT_EQ(err.code(), errForbidden) << err.what();
		}
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, ClustersRestart) {
	// Secondary cluster keeps the namespace's ownership after the restart without the primary cluster, which would take it over again on
	// reconnect. Updates, which were not replicated before the restart of both clusters, are replicated after it
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_clusters_restart";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		std::ignore = AwaitClusterLeader(secondary);

		primary.ApplyAsyncReplicationConfig(primaryLeader, ClusterT::ClusterTargetConfig(secondary, NsSet{kNs}));
		primary.InitNs(primaryLeader, kNs);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		awaitClusterReplication(primary, secondary, kNs);

		TestCout() << "Restarting both clusters" << std::endl;
		secondary.StopServers(0, kClusterSize);
		primary.FillData(primaryLeader, kNs, kDataCount, kDataCount);
		primary.StopServers(0, kClusterSize);
		for (size_t i = 0; i < kClusterSize; ++i) {
			ASSERT_TRUE(secondary.StartServer(i));
		}
		awaitNamespaceOwnership(secondary, kNs, IsPrimary_False);
		const int secondaryLeader = AwaitClusterLeader(secondary);
		checkReadOnly(secondary.GetNode(secondaryLeader), kNs);

		for (size_t i = 0; i < kClusterSize; ++i) {
			ASSERT_TRUE(primary.StartServer(i));
		}
		awaitClusterReplication(primary, secondary, kNs);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, InvalidClusterTarget) {
	// Cluster targets with an extra or substituted DSN are rejected before changing data or ownership, without blocking valid targets.
	// Correcting the target restores replication.
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_invalid_target";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		const int primaryLeader = AwaitClusterLeader(primary);
		const int secondaryLeader = AwaitClusterLeader(secondary);

		ServerControl singleNode;
		InitStandaloneServer(singleNode, 2 * kClusterSize, defaults);
		singleNode.Get()->MakeFollower();

		secondary.InitNs(secondaryLeader, kNs);
		auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kNs});
		const auto validDsns = config.nodes[0].dsns;
		// The extra DSN is unreachable, so validation finds the leader and fails on the composition check.
		config.nodes[0].dsns.emplace_back("cproto://127.0.0.1:1/not_in_cluster");
		config.nodes.emplace_back(MakeDsn(reindexer_server::UserRole::kRoleReplication, singleNode.Get()));
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		primary.InitNs(primaryLeader, kNs);
		primary.FillData(primaryLeader, kNs, 0, kDataCount);
		ServerControl::WaitSync(primary.GetNode(primaryLeader), singleNode.Get(), kNs, kSyncTime);

		{
			SCOPED_TRACE("Extra DSN");
			const auto stats = awaitReplicationError(
				primary.GetNode(primaryLeader),
				fmt::format("Secondary cluster consists of {} nodes, but {} DSNs are configured", kClusterSize, kClusterSize + 1));
			EXPECT_EQ(secondary.GetNode(secondaryLeader)->api.Select(Query(kNs)).Count(), 0);
			awaitNamespaceOwnership(secondary, kNs, IsPrimary_True);
			ASSERT_EQ(stats.nodeStats.size(), 2);
			EXPECT_EQ(stats.nodeStats[0].dsn, DSN());
			EXPECT_EQ(stats.nodeStats[0].dsns, config.nodes[0].dsns);
			EXPECT_EQ(stats.nodeStats[1].dsn, config.nodes[1].dsn);
			EXPECT_TRUE(stats.nodeStats[1].dsns.empty());
		}

		{
			SCOPED_TRACE("Substituted follower DSN");
			secondary.WaitSync(kNs, lsn_t(), lsn_t(), kSyncTime);
			config.nodes[0].dsns = validDsns;
			// Keep the real leader reachable, so validation reaches the membership check.
			config.nodes[0].dsns[(secondaryLeader + 1) % kClusterSize] = config.nodes[1].dsn;
			primary.ApplyAsyncReplicationConfig(primaryLeader, config);
			const auto stats = awaitReplicationError(primary.GetNode(primaryLeader), "is missing in the configured DSNs");
			ASSERT_EQ(stats.nodeStats.size(), 2);
			EXPECT_EQ(stats.nodeStats[0].lastError.code(), errParams) << StatsJSON(stats);
			EXPECT_EQ(stats.nodeStats[0].dsn, DSN()) << StatsJSON(stats);
			primary.FillData(primaryLeader, kNs, kDataCount, kDataCount);
			ServerControl::WaitSync(primary.GetNode(primaryLeader), singleNode.Get(), kNs, kSyncTime);
			for (size_t i = 0; i < secondary.Size(); ++i) {
				SCOPED_TRACE(fmt::format("Secondary node {}", i));
				EXPECT_EQ(secondary.GetNode(i)->api.Select(Query(kNs)).Count(), 0);
			}
			awaitNamespaceOwnership(secondary, kNs, IsPrimary_True);
		}

		config.nodes[0].dsns = validDsns;
		primary.ApplyAsyncReplicationConfig(primaryLeader, config);
		awaitClusterReplication(primary, secondary, kNs);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, SingleNodeToCluster) {
	// Standalone node replicates to the RAFT cluster
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_single_to_cluster";
		ClusterT secondary(loop, kClusterSize, kClusterSize, defaults);
		std::ignore = AwaitClusterLeader(secondary);

		const size_t kPrimaryId = 0;
		ServerControl primary;
		InitStandaloneServer(primary, kPrimaryId, defaults);
		auto config = ClusterT::ClusterTargetConfig(secondary, NsSet{kNs});
		config.serverId = kPrimaryId;
		// The role does not disable the replication
		config.role = "none";
		primary.Get()->SetReplicationConfig(config);

		auto& api = primary.Get()->api;
		ASSERT_TRUE(api.reindexer->OpenNamespace(kNs).ok());
		api.DefineNamespaceDataset(kNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
		for (int id = 0; id < int(kDataCount); ++id) {
			primary.Get()->api.UpsertJSON(kNs, itemJSON(id, "single"));
		}
		for (size_t i = 0; i < secondary.Size(); ++i) {
			ServerControl::WaitSync(primary.Get(), secondary.GetNode(i), kNs, kSyncTime);
		}
		awaitNamespaceOwnership(secondary, kNs, IsPrimary_False);
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, CascadeMainTransitEnd) {
	// A transit cluster forwards existing and newly created upstream namespaces, plus its own data, across its leader change.
	// Online namespace creation needs no snapshot sync; upstream data stays read-only and local namespaces are not forwarded.
	//
	// +------------------+      +------------------+
	// | Main cluster     |--A-->| Transit cluster  |
	// | [0] [1] [2]      |      | [3] [4] [5]      |
	// +------------------+      +--------+---------+
	//                                    |
	//                                    B
	//                           +--------+--------+
	//                           v                 v
	//                  +------------------+       [9] End node
	//                  | End cluster      |
	//                  | [6] [7] [8]      |
	//                  +------------------+
	//
	// A: ns_main, ns_main_online
	// B: ns_main, ns_main_online, ns_transit
	// Leader changes on Transit; ns_transit is its own namespace.
	// Not forwarded: ns_main_local on Main; per-node ns_transit_local on Transit.
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kMainNs = "ns_main";
		const std::string kMainOnlineNs = "ns_main_online";
		const std::string kMainExcludedNs = "ns_main_local";
		const std::string kTransitNs = "ns_transit";
		const std::string kTransitLocalNs = "ns_transit_local";
		ClusterT mainCluster(loop, 0, kClusterSize, defaults);
		ClusterT transit(loop, kClusterSize, kClusterSize, defaults, {kMainNs, kMainOnlineNs, kTransitNs});
		ClusterT endCluster(loop, 2 * kClusterSize, kClusterSize, defaults);
		const int mainLeader = AwaitClusterLeader(mainCluster);
		int transitLeader = AwaitClusterLeader(transit);
		const int formerLeader = transitLeader;
		const int nextLeader = (transitLeader + 1) % kClusterSize;
		const int endLeader = AwaitClusterLeader(endCluster);

		ServerControl endNode;
		InitStandaloneServer(endNode, 3 * kClusterSize, defaults);
		endNode.Get()->MakeFollower();

		mainCluster.InitNs(mainLeader, kMainNs);
		mainCluster.InitNs(mainLeader, kMainExcludedNs);
		mainCluster.FillData(mainLeader, kMainNs, 0, kDataCount);
		mainCluster.FillData(mainLeader, kMainExcludedNs, 0, kDataCount);
		transit.InitNs(transitLeader, kTransitNs);
		transit.FillData(transitLeader, kTransitNs, 0, kDataCount);
		transit.InitNs(formerLeader, kTransitLocalNs);
		transit.InitNs(nextLeader, kTransitLocalNs);
		transit.GetNode(formerLeader)->api.UpsertJSON(kTransitLocalNs, itemJSON(0, "former_leader"));
		transit.GetNode(nextLeader)->api.UpsertJSON(kTransitLocalNs, itemJSON(0, "future_leader"));

		const auto mainConfig = ClusterT::ClusterTargetConfig(transit, NsSet{kMainNs, kMainOnlineNs});
		mainCluster.ApplyAsyncReplicationConfig(mainLeader, mainConfig);
		AwaitAsyncReplicationConfig(mainCluster, mainConfig);
		awaitClusterReplication(mainCluster, transit, kMainNs);
		auto transitConfig = ClusterT::ClusterTargetConfig(endCluster, NsSet{kMainNs, kMainOnlineNs, kTransitNs});
		transitConfig.nodes.emplace_back(MakeDsn(reindexer_server::UserRole::kRoleReplication, endNode.Get()),
										 NsSet{kMainNs, kMainOnlineNs, kTransitNs});
		transit.ApplyAsyncReplicationConfig(transitLeader, transitConfig);
		AwaitAsyncReplicationConfig(transit, transitConfig);

		const auto awaitMainData = [&](std::string_view ns) {
			SCOPED_TRACE(fmt::format("Main -> transit/end, ns '{}'", ns));
			awaitClusterReplication(mainCluster, transit, ns);
			awaitClusterReplication(mainCluster, endCluster, ns);
			ServerControl::WaitSync(mainCluster.GetNode(mainLeader), endNode.Get(), std::string(ns), kSyncTime);
		};
		const auto awaitTransitData = [&] {
			SCOPED_TRACE("Transit -> end, own namespace");
			awaitClusterReplication(transit, endCluster, kTransitNs);
			ServerControl::WaitSync(transit.GetNode(transitLeader), endNode.Get(), kTransitNs, kSyncTime);
		};
		const auto checkLocalNotForwarded = [&] {
			for (size_t i = 0; i < endCluster.Size(); ++i) {
				SCOPED_TRACE(fmt::format("End cluster node {}, excluded ns '{}'", i, kTransitLocalNs));
				EXPECT_TRUE(endCluster.GetNode(i)->api.EnumNamespaces(EnumNamespacesOpts().WithFilter(kTransitLocalNs)).empty());
			}
			SCOPED_TRACE(fmt::format("End node, excluded ns '{}'", kTransitLocalNs));
			EXPECT_TRUE(endNode.Get()->api.EnumNamespaces(EnumNamespacesOpts().WithFilter(kTransitLocalNs)).empty());
		};

		{
			SCOPED_TRACE("Initial cascade sync");
			awaitMainData(kMainNs);
			awaitTransitData();
			std::ignore = awaitAsyncOnline(mainCluster.GetNode(mainLeader), 1);
			std::ignore = awaitAsyncOnline(transit.GetNode(transitLeader), 2);
			checkLocalNotForwarded();
			EXPECT_TRUE(transit.GetNode(transitLeader)->api.EnumNamespaces(EnumNamespacesOpts().WithFilter(kMainExcludedNs)).empty());
		}

		{
			SCOPED_TRACE("Online namespace creation without async resync");
			const auto mainBefore = awaitAsyncOnline(mainCluster.GetNode(mainLeader), 1);
			const auto transitBefore = awaitAsyncOnline(transit.GetNode(transitLeader), 2);
			mainCluster.InitNs(mainLeader, kMainOnlineNs);
			for (int id = 0; id < 3; ++id) {
				mainCluster.GetNode(mainLeader)->api.UpsertJSON(kMainOnlineNs, itemJSON(id, "online"));
			}
			awaitMainData(kMainOnlineNs);
			for (const auto& node : {transit.GetNode(transitLeader), endCluster.GetNode(endLeader)}) {
				SCOPED_TRACE(fmt::format("Read-only online namespace on node {}", node->Id()));
				const auto err = TryUpsertJSON(node->api, kMainOnlineNs, itemJSON(100500, "forbidden"));
				EXPECT_EQ(err.code(), errForbidden) << err.what();
			}
			const auto mainAfter = awaitAsyncOnline(mainCluster.GetNode(mainLeader), 1);
			const auto transitAfter = awaitAsyncOnline(transit.GetNode(transitLeader), 2);
			EXPECT_EQ(mainAfter.walSyncs.count, mainBefore.walSyncs.count) << StatsJSON(mainAfter);
			EXPECT_EQ(mainAfter.forceSyncs.count, mainBefore.forceSyncs.count) << StatsJSON(mainAfter);
			EXPECT_EQ(transitAfter.walSyncs.count, transitBefore.walSyncs.count) << StatsJSON(transitAfter);
			EXPECT_EQ(transitAfter.forceSyncs.count, transitBefore.forceSyncs.count) << StatsJSON(transitAfter);
		}

		{
			SCOPED_TRACE("Cascade after transit leader change");
			transit.ChangeLeader(transitLeader, nextLeader);
			// Ownership must protect the new leader before main sends the next update.
			for (const auto& ns : {kMainNs, kMainOnlineNs}) {
				SCOPED_TRACE(ns);
				const auto err = TryUpsertJSON(transit.GetNode(transitLeader)->api, ns, itemJSON(100500, "forbidden"));
				EXPECT_EQ(err.code(), errForbidden) << err.what();
			}
			std::ignore = awaitAsyncOnline(transit.GetNode(transitLeader), 2);
			transit.GetNode(transitLeader)->api.UpsertJSON(kTransitLocalNs, itemJSON(0, "after_election"));
			transit.FillData(transitLeader, kTransitNs, kDataCount, kDataCount);
			mainCluster.FillData(mainLeader, kMainNs, kDataCount, kDataCount);
			mainCluster.GetNode(mainLeader)->api.UpsertJSON(kMainOnlineNs, itemJSON(3, "after_election"));
			awaitMainData(kMainNs);
			awaitMainData(kMainOnlineNs);
			awaitTransitData();
			std::ignore = awaitAsyncOnline(transit.GetNode(transitLeader), 2);
			checkLocalNotForwarded();
			checkRows(transit.GetNode(formerLeader), kTransitLocalNs, {{0, "former_leader"}});
			checkRows(transit.GetNode(transitLeader), kTransitLocalNs, {{0, "after_election"}});
		}
	}));
	loop.run();
}

TEST_F(ClusterToClusterReplicationApi, MultiplePrimaries) {
	// Namespaces from two primary clusters remain independent through a shared transit cluster and its downstream replica,
	// including after one primary changes its leader.
	//
	// +------------------+      +------------------+
	// | Main A           |      | Main B           |
	// | [0] [1] [2]      |      | [3] [4] [5]      |
	// +--------+---------+      +--------+---------+
	//          | ns_from_a               | ns_from_b
	//          +------------+------------+
	//                       v
	//              +------------------+
	//              | Transit cluster  |
	//              | [6] [7] [8]      |
	//              +--------+---------+
	//                       | ns_from_a, ns_from_b
	//                       v
	//                      [9] End node
	//
	// Leader changes on Main A.
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNsA = "ns_from_a";
		const std::string kNsB = "ns_from_b";
		ClusterT mainA(loop, 0, kClusterSize, defaults);
		ClusterT mainB(loop, kClusterSize, kClusterSize, defaults);
		ClusterT transit(loop, 2 * kClusterSize, kClusterSize, defaults);
		int leaderA = AwaitClusterLeader(mainA);
		const int leaderB = AwaitClusterLeader(mainB);
		const int transitLeader = AwaitClusterLeader(transit);

		ServerControl endNode;
		InitStandaloneServer(endNode, 3 * kClusterSize, defaults);
		endNode.Get()->MakeFollower();

		mainA.InitNs(leaderA, kNsA);
		mainB.InitNs(leaderB, kNsB);
		mainA.GetNode(leaderA)->api.UpsertJSON(kNsA, itemJSON(0, "a0"));
		mainA.GetNode(leaderA)->api.UpsertJSON(kNsA, itemJSON(1, "a1"));
		mainB.GetNode(leaderB)->api.UpsertJSON(kNsB, itemJSON(0, "b0"));
		mainB.GetNode(leaderB)->api.UpsertJSON(kNsB, itemJSON(1, "b1"));
		const auto configA = ClusterT::ClusterTargetConfig(transit, NsSet{kNsA});
		const auto configB = ClusterT::ClusterTargetConfig(transit, NsSet{kNsB});
		mainA.ApplyAsyncReplicationConfig(leaderA, configA);
		mainB.ApplyAsyncReplicationConfig(leaderB, configB);
		const AsyncReplicationConfigTest transitConfig(
			"none",
			{AsyncReplicationConfigTest::Node(MakeDsn(reindexer_server::UserRole::kRoleReplication, endNode.Get()), NsSet{kNsA, kNsB})});
		transit.ApplyAsyncReplicationConfig(transitLeader, transitConfig);

		const auto awaitData = [&](ClusterT& source, std::string_view ns) {
			SCOPED_TRACE(fmt::format("Primary -> transit/end, ns '{}'", ns));
			awaitClusterReplication(source, transit, ns);
			ServerControl::WaitSync(source.GetNode(AwaitClusterLeader(source)), endNode.Get(), std::string(ns), kSyncTime);
		};
		{
			SCOPED_TRACE("Initial data from both primaries");
			awaitData(mainA, kNsA);
			awaitData(mainB, kNsB);
			checkRows(endNode.Get(), kNsA, {{0, "a0"}, {1, "a1"}});
			checkRows(endNode.Get(), kNsB, {{0, "b0"}, {1, "b1"}});
		}
		{
			SCOPED_TRACE("Only A changes");
			mainA.GetNode(leaderA)->api.UpsertJSON(kNsA, itemJSON(1, "a1_updated"));
			awaitData(mainA, kNsA);
			checkRows(endNode.Get(), kNsA, {{0, "a0"}, {1, "a1_updated"}});
			checkRows(endNode.Get(), kNsB, {{0, "b0"}, {1, "b1"}});
		}
		{
			SCOPED_TRACE("Only B changes");
			mainB.GetNode(leaderB)->api.UpsertJSON(kNsB, itemJSON(1, "b1_updated"));
			awaitData(mainB, kNsB);
			checkRows(endNode.Get(), kNsA, {{0, "a0"}, {1, "a1_updated"}});
			checkRows(endNode.Get(), kNsB, {{0, "b0"}, {1, "b1_updated"}});
		}
		{
			SCOPED_TRACE("Independent writes after A changes its leader");
			AwaitAsyncReplicationConfig(mainA, configA);
			std::ignore = awaitAsyncOnline(mainA.GetNode(leaderA), 1);
			std::ignore = awaitAsyncOnline(transit.GetNode(transitLeader), 1);
			mainA.ChangeLeader(leaderA, (leaderA + 1) % kClusterSize);
			mainB.GetNode(leaderB)->api.UpsertJSON(kNsB, itemJSON(2, "b2"));
			awaitData(mainB, kNsB);
			checkRows(endNode.Get(), kNsB, {{0, "b0"}, {1, "b1_updated"}, {2, "b2"}});
			mainA.GetNode(leaderA)->api.UpsertJSON(kNsA, itemJSON(2, "a2"));
			awaitData(mainA, kNsA);
			checkRows(endNode.Get(), kNsA, {{0, "a0"}, {1, "a1_updated"}, {2, "a2"}});
			checkRows(endNode.Get(), kNsB, {{0, "b0"}, {1, "b1_updated"}, {2, "b2"}});
		}
	}));
	loop.run();
}

}  // namespace reindexer_tests
