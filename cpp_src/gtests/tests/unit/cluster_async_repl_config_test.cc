#include "async_repl_tools.h"
#include "client/cororeindexer.h"
#include "cluster/clustercontrolrequest.h"
#include "cluster/consts.h"
#include "cluster_async_repl_config_api.h"
#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "estl/expected.h"
#include "gtests/tools.h"

namespace reindexer_tests {

using namespace reindexer;
using reindexer_tests_tools::exceptionWrapper;
using cluster::NodeStats;
using Node = AsyncReplicationConfigTest::Node;
using NsSet = AsyncReplicationConfigTest::NsSet;
using ClusterT = ClusterOperationApi::Cluster;

namespace {

constexpr size_t kClusterSize = 3;
constexpr size_t kDataCount = 20;
constexpr auto kSyncTime = kAsyncReplSyncTime;

// The cluster's part of the config with its source, which the node distributes
Expected<AsyncReplConfigSource> requestConfigSource(const ServerControl::Interface::Ptr& node, net::ev::dynamic_loop& loop) {
	client::CoroReindexer client;
	auto err = client.Connect(node->kRPCDsn, loop);
	ClusterControlResponseData res;
	if (err.ok()) {
		err = client.ClusterControlRequest(ClusterControlRequestData{GetAsyncReplConfigSourceCommand{}}, res);
	}
	client.Stop();
	if (!err.ok()) {
		return Unexpected(std::move(err));
	}
	return AsyncReplConfigSource(std::get<GetAsyncReplConfigSourceCommand>(res.data));
}

int64_t getConfigSourceId(const ServerControl::Interface::Ptr& node, net::ev::dynamic_loop& loop) {
	const auto source = requestConfigSource(node, loop);
	EXPECT_TRUE(source.has_value()) << source.error().what();
	return source.has_value() ? source->sourceId : int64_t(ShardingSourceId::NotSet);
}

void awaitConfigSourceId(const ServerControl::Interface::Ptr& node, int64_t sourceId, net::ev::dynamic_loop& loop) {
	AwaitCondition(
		kSyncTime,
		[&] {
			const auto source = requestConfigSource(node, loop);
			return source.has_value() && source->sourceId == sourceId;
		},
		[&] { return fmt::format("Node {} has not got the config's source {}", node->Id(), sourceId); });
}

// The node publishes the source after applying the config itself
void awaitConfigSourceId(ClusterT& cluster, int64_t sourceId, net::ev::dynamic_loop& loop) {
	for (size_t i = 0; i < cluster.Size(); ++i) {
		if (auto node = cluster.GetNode(i); node) {
			awaitConfigSourceId(node, sourceId, loop);
		}
	}
}

// Awaits the async replication config of the node 'serverId' in the RAFT cluster's stats, which are collected by its leader: the node's
// source is 'sourceId' (unknown, if it is not set) and there is no applying error
void awaitAsyncReplConfigStats(const ServerControl::Interface::Ptr& node, int serverId, std::optional<int64_t> sourceId) {
	const cluster::AsyncReplConfigStats expected{.sourceId = sourceId, .applyError = Error()};
	cluster::ReplicationStats stats;
	AwaitCondition(
		kSyncTime,
		[&] {
			stats = node->GetReplicationStats(cluster::kClusterReplStatsType);
			const auto found = std::ranges::find(stats.nodeStats, serverId, &NodeStats::serverId);
			return found != stats.nodeStats.end() && found->asyncReplConfig == expected;
		},
		[&] { return fmt::format("Unexpected async replication config of the node {} in the stats: {}", serverId, StatsJSON(stats)); });
}

// Server IDs of the nodes are equal to their indexes
void awaitAsyncReplConfigStats(ClusterT& cluster, size_t leaderId, std::optional<int64_t> sourceId) {
	for (size_t id = 0; id < cluster.Size(); ++id) {
		awaitAsyncReplConfigStats(cluster.GetNode(leaderId), int(id), sourceId);
	}
}

// Node, which is not available, so its replication fails
AsyncReplicationConfigTest::Node unavailableNode(std::string_view db) {
	return AsyncReplicationConfigTest::Node(DSN(fmt::format("cproto://127.0.0.1:1/{}", db)));
}

cluster::AsyncReplConfigData getAsyncReplicationConfig(const ServerControl::Interface::Ptr& node) {
	cluster::AsyncReplConfigData config;
	auto qr = node->api.Select(Query(kConfigNamespace).Where("type", CondEq, "async_replication"));
	EXPECT_EQ(qr.Count(), 1);
	if (qr.Count() != 1) {
		return config;
	}
	WrSerializer ser;
	auto err = qr.begin().GetJSON(ser, false);
	EXPECT_TRUE(err.ok()) << err.what();
	gason::JsonParser parser;
	err = config.FromJSON(parser.Parse(ser.Slice())["async_replication"]);
	EXPECT_TRUE(err.ok()) << err.what();
	return config;
}

// Local change of the node's config via #config
template <typename F>
void changeAsyncReplicationConfig(const ServerControl::Interface::Ptr& node, F&& change) {
	auto config = getAsyncReplicationConfig(node);
	change(config);
	WrSerializer ser;
	{
		JsonBuilder jb(ser);
		jb.Put("type", "async_replication");
		auto configObj = jb.Object("async_replication");
		config.GetJSON(configObj, cluster::MaskingDSN::Disabled);
	}
	node->api.UpsertJSON(kConfigNamespace, ser.Slice());
}

void awaitAppName(const ServerControl::Interface::Ptr& node, std::string_view appName) {
	AwaitCondition(
		kSyncTime, [&] { return node->GetServerConfig(ServerControl::ConfigType::Namespace).appName == appName; },
		[&] { return fmt::format("Node {} has not got app_name '{}'", node->Id(), appName); });
}

std::string actionJSON(const ServerControl::Interface::Ptr& node, const AsyncReplicationConfigTest& config) {
	return fmt::format(R"json({{"type":"action","action":{}}})json", node->AsyncReplicationActionJSON(config));
}

// The node keeps the parse error of 'namespaces' until its next valid write
void breakNamespacesConfig(const ServerControl::Interface::Ptr& node) {
	const auto err = TryUpsertJSON(node->api, kConfigNamespace,
								   R"json({"type":"namespaces","namespaces":[{"namespace":"*","max_preselect_part":2.0}]})json");
	ASSERT_EQ(err.code(), errParseJson) << err.what();
}

}  // namespace

TEST_F(ClusterAsyncReplConfigApi, OutdatedConfigOnNewLeader) {
	// The node, which has missed the config's update, takes the actual config from the quorum after becoming the leader. The source of
	// the action's config is shared by the nodes and survives the restart, including the same config in the file. Own role of each node is
	// kept via the action, on the elections and via the push. The leader shows the source of each node in the RAFT cluster's stats
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs1 = "ns_outdated_config_1";
		const std::string kNs2 = "ns_outdated_config_2";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		ServerControl target;
		InitStandaloneServer(target, kClusterSize, defaults);
		target.Get()->MakeFollower();
		const auto targetConfig = [&target](const std::string& ns) {
			return AsyncReplicationConfigTest({Node(MakeDsn(reindexer_server::UserRole::kRoleReplication, target.Get()), NsSet{ns})});
		};
		int primaryLeader = AwaitClusterLeader(primary);
		awaitAsyncReplConfigStats(primary, primaryLeader, ShardingSourceId::NotSet);

		const size_t formerLeader = primaryLeader;
		const size_t outdated = (primaryLeader + 1) % kClusterSize;
		const size_t actual = (primaryLeader + 2) % kClusterSize;
		// The roles differ from the default one, so its loss is detected on each node
		std::vector<std::string> roles;
		for (size_t id = 0; id < kClusterSize; ++id) {
			roles.emplace_back(id == outdated ? "leader" : "follower");
			primary.GetNode(id)->SetAsyncReplicationConfig(AsyncReplicationConfigTest(roles.back()));
		}
		const auto checkRoles = [&] {
			for (size_t id = 0; id < kClusterSize; ++id) {
				EXPECT_EQ(primary.GetNode(id)->GetServerConfig(ServerControl::ConfigType::Namespace).role, roles[id]) << "Node " << id;
			}
		};

		primary.InitNs(primaryLeader, kNs1);
		primary.InitNs(primaryLeader, kNs2);
		const auto kNs1Config = targetConfig(kNs1);
		primary.ApplyAsyncReplicationConfig(primaryLeader, kNs1Config);
		AwaitAsyncReplicationConfig(primary, kNs1Config);
		const int64_t sourceId1 = getConfigSourceId(primary.GetNode(primaryLeader), loop);
		ASSERT_NE(sourceId1, ShardingSourceId::NotSet);
		awaitConfigSourceId(primary, sourceId1, loop);
		awaitAsyncReplConfigStats(primary, primaryLeader, sourceId1);
		checkRoles();
		primary.FillData(primaryLeader, kNs1, 0, kDataCount);
		ServerControl::WaitSync(primary.GetNode(primaryLeader), target.Get(), kNs1, kSyncTime);

		ASSERT_TRUE(primary.StopServer(outdated));
		const auto kNs2Config = targetConfig(kNs2);
		primary.ApplyAsyncReplicationConfig(primaryLeader, kNs2Config);
		AwaitAsyncReplicationConfig(primary, kNs2Config);
		const int64_t sourceId2 = getConfigSourceId(primary.GetNode(primaryLeader), loop);
		EXPECT_TRUE(cluster::ConfigSourceId::IsNewer(sourceId2, sourceId1)) << sourceId2 << " vs " << sourceId1;
		awaitConfigSourceId(primary, sourceId2, loop);
		primary.FillData(primaryLeader, kNs1, kDataCount, kDataCount);

		// The same config, loaded from the file on startup, keeps its source
		auto fileConfig = kNs2Config;
		fileConfig.role = roles[actual];
		primary.GetNode(actual)->WriteAsyncReplicationConfig(fileConfig);
		primary.StopServers(0, kClusterSize);
		ASSERT_TRUE(primary.StartServer(actual));
		EXPECT_EQ(getConfigSourceId(primary.GetNode(actual), loop), sourceId2);
		// The node without the quorum only votes for the desired leader, so the outdated node wins before getting the config
		auto err = primary.GetClient(actual).SetDesiredLeaderId(int(outdated));
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(primary.StartServer(outdated));
		primaryLeader = AwaitClusterLeader(primary);
		ASSERT_EQ(primaryLeader, int(outdated));
		ASSERT_TRUE(primary.StartServer(formerLeader));
		AwaitAsyncReplicationConfig(primary, kNs2Config);
		awaitConfigSourceId(primary, sourceId2, loop);
		awaitAsyncReplConfigStats(primary, primaryLeader, sourceId2);
		checkRoles();

		// kNs1's updates precede kNs2's ones in the updates queue
		primary.FillData(primaryLeader, kNs1, 2 * kDataCount, kDataCount);
		primary.FillData(primaryLeader, kNs2, 0, kDataCount);
		ServerControl::WaitSync(primary.GetNode(primaryLeader), target.Get(), kNs2, kSyncTime);
		EXPECT_EQ(target.Get()->api.Select(Query(kNs1)).Count(), kDataCount);
	}));
	loop.run();
}

TEST_F(ClusterAsyncReplConfigApi, ConfigSourceRevocation) {
	// Only the change of the cluster's nodes, including their namespaces lists, inherited from the common one, revokes the config's source.
	// Leader's part without the source keeps the source of the follower with the same nodes, and the leader shows it in its stats. The
	// action, issued by the query to #config on the follower, is proxied to the leader and responds after the follower applies the config
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		ClusterT primary(loop, 0, kClusterSize, defaults);
		const int leader = AwaitClusterLeader(primary);
		const auto follower1 = primary.GetNode((leader + 1) % kClusterSize);
		const AsyncReplicationConfigTest config("none", {unavailableNode("cluster")}, false, true, 0, "rx_node", NsSet{"ns1"});
		const auto actionQuery = Query("#CONFIG")
									 .Where("type", CondEq, "action")
									 .SetObject("action", Variant{follower1->AsyncReplicationActionJSON(config)})
									 .SetObject("async_replication", Variant{R"({"app_name":"foreign","nodes":[]})"});
		EXPECT_EQ(follower1->api.Update(actionQuery), 1);
		EXPECT_EQ(follower1->GetServerConfig(ServerControl::ConfigType::Namespace).nodes, config.nodes);
		const int64_t sourceId = getConfigSourceId(follower1, loop);
		ASSERT_NE(sourceId, ShardingSourceId::NotSet);
		AwaitAsyncReplicationConfig(primary, config);
		awaitConfigSourceId(primary, sourceId, loop);
		const auto sourceBeforeDelete = requestConfigSource(follower1, loop);
		ASSERT_TRUE(sourceBeforeDelete.has_value()) << sourceBeforeDelete.error().what();
		EXPECT_EQ(follower1->api.Delete(Query("#CONFIG").Where("type", CondEq, "action")), 1);
		const auto sourceAfterDelete = requestConfigSource(follower1, loop);
		ASSERT_TRUE(sourceAfterDelete.has_value()) << sourceAfterDelete.error().what();
		EXPECT_EQ(sourceAfterDelete->sourceId, sourceBeforeDelete->sourceId);
		EXPECT_EQ(sourceAfterDelete->config, sourceBeforeDelete->config);

		changeAsyncReplicationConfig(follower1, [](cluster::AsyncReplConfigData& c) { c.logLevel = LogError; });
		EXPECT_EQ(getConfigSourceId(follower1, loop), sourceId);
		changeAsyncReplicationConfig(follower1, [](cluster::AsyncReplConfigData& c) { c.selfReplToken = "changed_token"; });
		EXPECT_EQ(getConfigSourceId(follower1, loop), sourceId);
		changeAsyncReplicationConfig(follower1,
									 [](cluster::AsyncReplConfigData& c) { c.role = cluster::AsyncReplConfigData::Role::Follower; });
		EXPECT_EQ(getConfigSourceId(follower1, loop), sourceId);
		// The target without its own namespaces list replicates the common one
		const auto setNamespaces = [&follower1](std::string_view namespaces) {
			const auto qr = follower1->api.ExecSQL(
				fmt::format("UPDATE #config SET async_replication.namespaces = {} WHERE type = 'async_replication'", namespaces));
			EXPECT_EQ(qr.Count(), 1);
		};
		setNamespaces("['ns1', 'ns2']");
		EXPECT_EQ(getConfigSourceId(follower1, loop), ShardingSourceId::NotSet);
		// The source does not revive
		setNamespaces("['ns1']");
		EXPECT_EQ(getConfigSourceId(follower1, loop), ShardingSourceId::NotSet);
		EXPECT_EQ(getConfigSourceId(primary.GetNode(leader), loop), sourceId);

		// The leader's source is revoked and does not revive, but its nodes are the same as the followers' ones again
		const auto leaderNode = primary.GetNode(leader);
		const auto nodes = getAsyncReplicationConfig(leaderNode).nodes;
		changeAsyncReplicationConfig(leaderNode,
									 [](cluster::AsyncReplConfigData& c) { c.nodes = {DSN("cproto://127.0.0.1:1/cluster_2")}; });
		changeAsyncReplicationConfig(leaderNode, [&nodes](cluster::AsyncReplConfigData& c) { c.nodes = nodes; });
		EXPECT_EQ(getConfigSourceId(leaderNode, loop), ShardingSourceId::NotSet);
		awaitAsyncReplConfigStats(leaderNode, leader, ShardingSourceId::NotSet);
		const size_t follower2Id = (leader + 2) % kClusterSize;
		ASSERT_TRUE(primary.StopServer(follower2Id));
		awaitAsyncReplConfigStats(leaderNode, int(follower2Id), std::nullopt);
		ASSERT_TRUE(primary.StartServer(follower2Id));
		awaitAsyncReplConfigStats(leaderNode, int(follower2Id), sourceId);
		const auto follower2 = primary.GetNode(follower2Id);
		EXPECT_EQ(getConfigSourceId(follower2, loop), sourceId);

		changeAsyncReplicationConfig(follower2,
									 [](cluster::AsyncReplConfigData& c) { c.nodes.emplace_back(DSN("cproto://127.0.0.1:1/cluster_2")); });
		EXPECT_EQ(getConfigSourceId(follower2, loop), ShardingSourceId::NotSet);
	}));
	loop.run();
}

TEST_F(ClusterAsyncReplConfigApi, ConfigCommonParameters) {
	// Common parameters are distributed as the leader's current state: the leader's local change reaches the followers on reconnect with
	// the same source, the follower's local change is rolled back by the next push, and the new leader with the same source distributes its
	// own parameters
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		ClusterT primary(loop, 0, kClusterSize, defaults);
		int leader = AwaitClusterLeader(primary);
		const AsyncReplicationConfigTest config({unavailableNode("cluster")}, "app_of_action");
		primary.ApplyAsyncReplicationConfig(leader, config);
		AwaitAsyncReplicationConfig(primary, config);
		const int64_t sourceId = getConfigSourceId(primary.GetNode(leader), loop);
		ASSERT_NE(sourceId, ShardingSourceId::NotSet);
		awaitConfigSourceId(primary, sourceId, loop);
		const auto setAppName = [](const ServerControl::Interface::Ptr& node, std::string_view appName) {
			changeAsyncReplicationConfig(node, [appName](cluster::AsyncReplConfigData& c) { c.appName = appName; });
		};

		const size_t follower1 = (leader + 1) % kClusterSize;
		const size_t follower2 = (leader + 2) % kClusterSize;
		setAppName(primary.GetNode(leader), "app_of_leader");
		EXPECT_EQ(getConfigSourceId(primary.GetNode(leader), loop), sourceId);
		ASSERT_TRUE(primary.StopServer(follower1));
		ASSERT_TRUE(primary.StartServer(follower1));
		awaitAppName(primary.GetNode(follower1), "app_of_leader");
		awaitConfigSourceId(primary.GetNode(follower1), sourceId, loop);

		setAppName(primary.GetNode(follower2), "app_of_follower");
		EXPECT_EQ(getConfigSourceId(primary.GetNode(follower2), loop), sourceId);
		ASSERT_TRUE(primary.StopServer(follower2));
		ASSERT_TRUE(primary.StartServer(follower2));
		awaitAppName(primary.GetNode(follower2), "app_of_leader");

		setAppName(primary.GetNode(follower1), "app_of_new_leader");
		primary.ChangeLeader(leader, follower1);
		for (size_t id = 0; id < kClusterSize; ++id) {
			awaitAppName(primary.GetNode(id), "app_of_new_leader");
		}
		awaitConfigSourceId(primary, sourceId, loop);
	}));
	loop.run();
}

TEST_F(ClusterAsyncReplConfigApi, ConfigUnknownFields) {
	// The fields, which are unknown for this version, are distributed as they were received by the action
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		ClusterT primary(loop, 0, kClusterSize, defaults);
		const int leader = AwaitClusterLeader(primary);
		const size_t offline = (leader + 1) % kClusterSize;
		ASSERT_TRUE(primary.StopServer(offline));
		constexpr std::string_view kUnknownField = R"("future_option")";
		const auto err = TryUpsertJSON(
			primary.GetNode(leader)->api, kConfigNamespace,
			fmt::format(
				R"json({{"type":"action","action":{{"command":"apply_async_replication_config","config":{{{}:{{"enabled":true}},"nodes":[]}}}}}})json",
				kUnknownField));
		ASSERT_TRUE(err.ok()) << err.what();
		const int64_t sourceId = getConfigSourceId(primary.GetNode(leader), loop);
		ASSERT_NE(sourceId, ShardingSourceId::NotSet);

		// The offline node gets the config via the push
		ASSERT_TRUE(primary.StartServer(offline));
		awaitConfigSourceId(primary, sourceId, loop);
		for (size_t id = 0; id < kClusterSize; ++id) {
			const auto source = requestConfigSource(primary.GetNode(id), loop);
			ASSERT_TRUE(source.has_value()) << source.error().what();
			EXPECT_NE(source->config.find(kUnknownField), std::string::npos) << "Node " << id << ": " << source->config;
		}
	}));
	loop.run();
}

TEST_F(ClusterAsyncReplConfigApi, OtherConfigTypeErrors) {
	// Parse errors of the other config types, kept by the nodes, do not prevent applying of the cluster's part on the elections, via the
	// action and via the updates queue. The action still reminds of them
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		ClusterT primary(loop, 0, kClusterSize, defaults);
		int leader = AwaitClusterLeader(primary);
		const AsyncReplicationConfigTest kConfig1({unavailableNode("cluster_1")});
		const AsyncReplicationConfigTest kConfig2({unavailableNode("cluster_2")});
		const AsyncReplicationConfigTest kConfig3({unavailableNode("cluster_3")});
		primary.ApplyAsyncReplicationConfig(leader, kConfig1);
		const int64_t sourceId1 = getConfigSourceId(primary.GetNode(leader), loop);
		ASSERT_NE(sourceId1, ShardingSourceId::NotSet);
		awaitConfigSourceId(primary, sourceId1, loop);

		const size_t newLeader = (leader + 1) % kClusterSize;
		const size_t follower = (leader + 2) % kClusterSize;
		// The local change revokes the new leader's source, so it applies the quorum's config on the elections
		primary.GetNode(newLeader)->SetAsyncReplicationConfig(kConfig2);
		breakNamespacesConfig(primary.GetNode(newLeader));
		breakNamespacesConfig(primary.GetNode(follower));
		primary.ChangeLeader(leader, newLeader);
		awaitAsyncReplConfigStats(primary, leader, sourceId1);

		const auto err = TryUpsertJSON(primary.GetNode(leader)->api, kConfigNamespace, actionJSON(primary.GetNode(leader), kConfig3));
		EXPECT_EQ(err.code(), errParseJson) << err.what();
		const int64_t sourceId3 = getConfigSourceId(primary.GetNode(leader), loop);
		ASSERT_TRUE(cluster::ConfigSourceId::IsNewer(sourceId3, sourceId1)) << sourceId3 << " vs " << sourceId1;
		awaitAsyncReplConfigStats(primary, leader, sourceId3);
	}));
	loop.run();
}

TEST_F(ClusterAsyncReplConfigApi, LocalConfigRollout) {
	// Config, changed locally on all the nodes (e.g. via the files), is not rolled back by the node, which was offline during the change
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kNs = "ns_local_rollout";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		int leader = AwaitClusterLeader(primary);
		primary.InitNs(leader, kNs);

		const AsyncReplicationConfigTest kConfig1({unavailableNode("cluster_1")});
		const AsyncReplicationConfigTest kConfig2({unavailableNode("cluster_2")});
		primary.ApplyAsyncReplicationConfig(leader, kConfig1);
		AwaitAsyncReplicationConfig(primary, kConfig1);
		const int64_t sourceId = getConfigSourceId(primary.GetNode(leader), loop);
		ASSERT_NE(sourceId, ShardingSourceId::NotSet);
		awaitConfigSourceId(primary, sourceId, loop);

		const size_t offline = (leader + 1) % kClusterSize;
		const size_t online = (leader + 2) % kClusterSize;
		ASSERT_TRUE(primary.StopServer(offline));
		for (const size_t id : {size_t(leader), online}) {
			primary.GetNode(id)->WriteAsyncReplicationConfig(kConfig2);
			awaitConfigSourceId(primary.GetNode(id), ShardingSourceId::NotSet, loop);
		}
		ASSERT_TRUE(primary.StartServer(offline));
		AwaitAsyncReplicationConfig(primary.GetNode(offline), kConfig2);
		awaitConfigSourceId(primary.GetNode(offline), ShardingSourceId::NotSet, loop);

		primary.ChangeLeader(leader, offline);
		// Writes on the new leader await its initial sync, which takes the newest config of the cluster
		primary.FillData(leader, kNs, 0, kDataCount);
		primary.WaitSync(kNs, lsn_t(), lsn_t(), kSyncTime);
		EXPECT_EQ(primary.GetNode(leader)->GetServerConfig(ServerControl::ConfigType::Namespace).nodes, kConfig2.nodes);
	}));
	loop.run();
}

TEST_F(ClusterAsyncReplConfigApi, RejectedConfigWrites) {
	// Meta of #config with the config's source is reserved for the core access only. The action, proxied to the follower, is rejected.
	// The action inside the item of another type is ignored
	net::ev::dynamic_loop loop;
	const auto defaults = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string kSourceKey = std::string(kConfigSourceMetaPrefix) + "async_replication";
		ClusterT primary(loop, 0, kClusterSize, defaults);
		const int leader = AwaitClusterLeader(primary);

		const AsyncReplicationConfigTest config({unavailableNode("cluster")});
		primary.ApplyAsyncReplicationConfig(leader, config);
		auto node = primary.GetNode(leader);
		const int64_t sourceId = getConfigSourceId(node, loop);
		ASSERT_NE(sourceId, ShardingSourceId::NotSet);

		std::string data;
		auto err = node->api.reindexer->GetMeta(kConfigNamespace, kSourceKey, data);
		EXPECT_EQ(err.code(), errForbidden) << err.what();
		err = node->api.reindexer->PutMeta(kConfigNamespace, kSourceKey, "{}");
		EXPECT_EQ(err.code(), errForbidden) << err.what();
		err = node->api.reindexer->PutMeta(kConfigNamespace, std::string(kConfigSourceMetaPrefix) + "any", "{}");
		EXPECT_EQ(err.code(), errForbidden) << err.what();
		err = node->api.reindexer->DeleteMeta(kConfigNamespace, kSourceKey);
		EXPECT_EQ(err.code(), errForbidden) << err.what();
		auto tx = node->api.reindexer->NewTransaction(kConfigNamespace);
		ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
		err = tx.PutMeta(kSourceKey, "{}");
		EXPECT_EQ(err.code(), errForbidden) << err.what();
		err = node->api.reindexer->RollBackTransaction(tx);
		ASSERT_TRUE(err.ok()) << err.what();
		std::vector<std::string> keys;
		err = node->api.reindexer->EnumMeta(kConfigNamespace, keys);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(std::find(keys.begin(), keys.end(), kSourceKey), keys.end());

		const auto follower = primary.GetNode((leader + 1) % kClusterSize);
		err = TryUpsertJSON(follower->api, kConfigNamespace, actionJSON(follower, AsyncReplicationConfigTest({unavailableNode("proxied")})),
							leader);
		EXPECT_EQ(err.code(), errAlreadyProxied) << err.what();
		EXPECT_EQ(getConfigSourceId(node, loop), sourceId);

		const AsyncReplicationConfigTest ignored({unavailableNode("ignored")});
		for (const char* type : {"async_replication", "profiling"}) {
			SCOPED_TRACE(type);
			const auto query =
				Query("#CONFIG").Where("type", CondEq, type).SetObject("action", Variant{node->AsyncReplicationActionJSON(ignored)});
			EXPECT_EQ(node->api.Update(query), 1);
			EXPECT_EQ(getConfigSourceId(node, loop), sourceId);
			EXPECT_EQ(node->GetServerConfig(ServerControl::ConfigType::Namespace).nodes, config.nodes);
		}
	}));
	loop.run();
}

}  // namespace reindexer_tests
