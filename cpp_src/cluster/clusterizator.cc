#include "clusterizator.h"
#include "core/reindexer_impl/reindexerimpl.h"

namespace reindexer {
namespace cluster {

ClusterManager::ClusterManager(ReindexerImpl& thisNode, size_t maxUpdatesSize)
	: updatesQueue_(maxUpdatesSize),
	  clusterReplicator_(updatesQueue_, sharedSyncState_, thisNode),
	  asyncReplicator_(updatesQueue_, sharedSyncState_, thisNode, *this) {}

void ClusterManager::Configure(ReplicationConfigData replConfig) {
	unique_lock lck(mtx_);
	if (!enabled_.load(std::memory_order_acquire)) {
		return;
	}
	clusterReplicator_.Configure(replConfig);
	asyncReplicator_.Configure(std::move(replConfig));
}

void ClusterManager::Configure(ClusterConfigData clusterConfig) {
	unique_lock lck(mtx_);
	if (!enabled_.load(std::memory_order_acquire)) {
		return;
	}
	clusterReplicator_.Configure(std::move(clusterConfig));
}

void ClusterManager::Configure(AsyncReplConfigData asyncConfig) {
	lock_guard lck(mtx_);
	if (!enabled_.load(std::memory_order_acquire)) {
		return;
	}
	asyncReplicator_.Configure(std::move(asyncConfig));
}

bool ClusterManager::IsExpectingAsyncReplStartup() const noexcept {
	lock_guard lck(mtx_);
	return enabled_.load(std::memory_order_acquire) && asyncReplicator_.IsExpectingStartup();
}

bool ClusterManager::IsExpectingClusterStartup() const noexcept {
	lock_guard lck(mtx_);
	return enabled_.load(std::memory_order_acquire) && clusterReplicator_.IsExpectingStartup();
}

Error ClusterManager::StartClusterRepl() {
	lock_guard lck(mtx_);
	try {
		if (!enabled_.load(std::memory_order_acquire)) {
			return Error(errLogic, "ClusterManager is disabled");
		}
		validateConfig();
		clusterReplicator_.Run();
	} catch (Error& e) {
		return e;
	}
	return Error();
}

Error ClusterManager::StartAsyncRepl() {
	lock_guard lck(mtx_);
	try {
		if (!enabled_.load(std::memory_order_acquire)) {
			return Error(errLogic, "ClusterManager is disabled");
		}
		validateConfig();
		asyncReplicator_.Run();
	} catch (Error& e) {
		return e;
	}
	return Error();
}

void ClusterManager::StopCluster() noexcept {
	lock_guard lck(mtx_);
	clusterReplicator_.Stop();
}

void ClusterManager::StopAsyncRepl() noexcept {
	lock_guard lck(mtx_);
	asyncReplicator_.Stop();
}

void ClusterManager::Stop(bool disable) noexcept {
	lock_guard lck(mtx_);
	clusterReplicator_.Stop(disable);
	asyncReplicator_.Stop(disable);
	if (disable) {
		enabled_.store(false, std::memory_order_release);
	}
}

void ClusterManager::SuggestLeader(const NodeData& suggestion, NodeData& response) {
	clusterReplicator_.SuggestLeader(suggestion, response);
}

void ClusterManager::SetDesiredLeaderId(int leaderId, bool sendToOtherNodes) {
	clusterReplicator_.SetDesiredLeaderId(leaderId, sendToOtherNodes);
}

void ClusterManager::ForceElections() { clusterReplicator_.ForceElections(); }

void ClusterManager::LeadersPing(const NodeData& leader) { clusterReplicator_.LeadersPing(leader); }

RaftInfo ClusterManager::GetRaftInfo(bool allowTransitState, const RdxContext& ctx) const {
	return clusterReplicator_.GetRaftInfo(allowTransitState, ctx);
}

bool ClusterManager::NamespaceIsInClusterConfig(std::string_view nsName) { return clusterReplicator_.NamespaceIsInClusterConfig(nsName); }

bool ClusterManager::NamesapceIsInReplicationConfig(std::string_view nsName) {
	return clusterReplicator_.NamespaceIsInClusterConfig(nsName) || asyncReplicator_.NamespaceIsInAsyncConfig(nsName);
}

Error ClusterManager::Replicate(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	if (replicationIsNotRequired(recs)) {
		return {};
	}

	std::pair<Error, bool> res;
	if (ctx.GetOriginLSN().isEmpty()) {
		res = updatesQueue_.Push(std::move(recs), std::move(beforeWaitF), ctx);
	} else {
		// Update can't be replicated to cluster from another node, so may only be replicated to async replicas
		res = updatesQueue_.PushAsync(std::move(recs));
	}

	if (!res.first.ok()) {
		logWarn("Error while Pushing updates queue: {}", res.first.what());
	}
	if (res.second) {
		return res.first;
	}
	return {};	// This namespace is not taking part in any replication
}

Error ClusterManager::ReplicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) {
	if (replicationIsNotRequired(recs)) {
		return {};
	}

	std::pair<Error, bool> res;
	if (ctx.GetOriginLSN().isEmpty()) {
		res = updatesQueue_.PushNowait(std::move(recs));
	} else {
		// Update can't be replicated to cluster from another node, so may only be replicated to async replicas
		res = updatesQueue_.PushAsync(std::move(recs));
	}

	if (!res.first.ok()) {
		logWarn("Error while Pushing updates queue: {}", res.first.what());
	}

	return {};	// This namespace is not taking part in any replication
}

ReplicationStats ClusterManager::GetAsyncReplicationStats() const { return asyncReplicator_.GetReplicationStats(); }

ReplicationStats ClusterManager::GetClusterReplicationStats() const { return clusterReplicator_.GetReplicationStats(); }

void ClusterManager::SetAsyncReplicatonLogLevel(LogLevel level) noexcept { asyncReplicator_.SetLogLevel(level); }

void ClusterManager::SetClusterReplicatonLogLevel(LogLevel level) noexcept { clusterReplicator_.SetLogLevel(level); }

bool ClusterManager::replicationIsNotRequired(const UpdatesContainer& recs) noexcept {
	return recs.empty() || isSystemNamespaceNameFast(recs[0].NsName());
}

void ClusterManager::validateConfig() const {
	auto& clusterConf = clusterReplicator_.Config();
	auto& asyncConf = asyncReplicator_.Config();
	if (!asyncConf.has_value() && !clusterConf.has_value()) {
		throw Error(errParams, "Config is not set");
	}
}

}  // namespace cluster
}  // namespace reindexer
