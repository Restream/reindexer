#pragma once

#include "cluster_operation_api.h"

namespace reindexer_tests {

// Await time of the async replication and of the async replication config's distribution
constexpr auto kAsyncReplSyncTime = std::chrono::seconds(40);

int AwaitClusterLeader(ClusterOperationApi::Cluster& cluster);
void AwaitAsyncReplicationConfig(const ServerControl::Interface::Ptr& node, const AsyncReplicationConfigTest& config);
// Config is applied on the majority of the nodes synchronously and on the rest of them asynchronously
void AwaitAsyncReplicationConfig(ClusterOperationApi::Cluster& cluster, const AsyncReplicationConfigTest& config);
// Server outside the clusters with the ports and the storage of the cluster's node 'id'
void InitStandaloneServer(ServerControl& server, size_t id, const ClusterOperationApi::Defaults& defaults);
reindexer::Error TryUpsertJSON(BaseApi& api, std::string_view ns, std::string_view json, int emitterServerId = -1) noexcept;

}  // namespace reindexer_tests
