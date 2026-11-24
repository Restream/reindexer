#include "async_repl_tools.h"
#include "tools/catch_and_return.h"
#include "tools/fsops.h"

namespace reindexer_tests {

using namespace reindexer;

int AwaitClusterLeader(ClusterOperationApi::Cluster& cluster) {
	const int leaderId = cluster.AwaitLeader(ClusterOperationApi::kMaxElectionsTime);
	EXPECT_NE(leaderId, -1);
	return leaderId;
}

void AwaitAsyncReplicationConfig(const ServerControl::Interface::Ptr& node, const AsyncReplicationConfigTest& config) {
	AwaitCondition(
		kAsyncReplSyncTime, [&] { return node->GetServerConfig(ServerControl::ConfigType::Namespace).nodes == config.nodes; },
		[&] { return fmt::format("Node {} has not got the config", node->Id()); });
}

void AwaitAsyncReplicationConfig(ClusterOperationApi::Cluster& cluster, const AsyncReplicationConfigTest& config) {
	for (size_t i = 0; i < cluster.Size(); ++i) {
		if (auto node = cluster.GetNode(i); node) {
			AwaitAsyncReplicationConfig(node, config);
		}
	}
}

void InitStandaloneServer(ServerControl& server, size_t id, const ClusterOperationApi::Defaults& defaults) {
	server.InitServer(ServerControlConfig(id, defaults.defaultRpcPort + id, defaults.defaultHttpPort + id,
										  fs::JoinPath(defaults.baseTestsetDbPath, "node/" + std::to_string(id)),
										  "node" + std::to_string(id)));
}

Error TryUpsertJSON(BaseApi& api, std::string_view ns, std::string_view json, int emitterServerId) noexcept {
	try {
		auto item = api.NewItem(ns);
		if (!item.Status().ok()) {
			return item.Status();
		}
		if (auto err = item.FromJSON(json); !err.ok()) {
			return err;
		}
		return api.reindexer->WithEmitterServerId(emitterServerId).Upsert(ns, item);
	} CATCH_AND_RETURN
}

}  // namespace reindexer_tests
