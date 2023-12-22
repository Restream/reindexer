#include "clusterizator.h"
#include <assert.h>
#include <unordered_set>
#include "core/reindexer_impl/reindexerimpl.h"

namespace reindexer {
namespace cluster {

Clusterizator::Clusterizator(ReindexerImpl& thisNode, size_t maxUpdatesSize)
	: updatesQueue_(maxUpdatesSize),
	  clusterReplicator_(updatesQueue_, sharedSyncState_, thisNode),
	  asyncReplicator_(updatesQueue_, sharedSyncState_, thisNode, *this) {}

void Clusterizator::Configure(ReplicationConfigData replConfig) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (!enabled_.load(std::memory_order_acquire)) {
		return;
	}
	clusterReplicator_.Configure(replConfig);
	asyncReplicator_.Configure(replConfig);
}

void Clusterizator::Configure(ClusterConfigData clusterConfig) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (!enabled_.load(std::memory_order_acquire)) {
		return;
	}
	clusterReplicator_.Configure(std::move(clusterConfig));
}

void Clusterizator::Configure(AsyncReplConfigData asyncConfig) {
	std::lock_guard lck(mtx_);
	if (!enabled_.load(std::memory_order_acquire)) {
		return;
	}
	asyncReplicator_.Configure(std::move(asyncConfig));
}

bool Clusterizator::IsExpectingAsyncReplStartup() const noexcept {
	std::lock_guard lck(mtx_);
	return enabled_.load(std::memory_order_acquire) && asyncReplicator_.IsExpectingStartup();
}

bool Clusterizator::IsExpectingClusterStartup() const noexcept {
	std::lock_guard lck(mtx_);
	return enabled_.load(std::memory_order_acquire) && clusterReplicator_.IsExpectingStartup();
}

Error Clusterizator::StartClusterRepl() {
	std::lock_guard lck(mtx_);
	try {
		if (!enabled_.load(std::memory_order_acquire)) {
			return Error(errLogic, "Clusterization is disabled");
		}
		validateConfig();
		clusterReplicator_.Run();
	} catch (Error& e) {
		return e;
	}
	return Error();
}

Error Clusterizator::StartAsyncRepl() {
	std::lock_guard lck(mtx_);
	try {
		if (!enabled_.load(std::memory_order_acquire)) {
			return Error(errLogic, "Clusterization is disabled");
		}
		validateConfig();
		asyncReplicator_.Run();
	} catch (Error& e) {
		return e;
	}
	return Error();
}

void Clusterizator::StopCluster() noexcept {
	std::lock_guard lck(mtx_);
	clusterReplicator_.Stop();
}

void Clusterizator::StopAsyncRepl() noexcept {
	std::lock_guard lck(mtx_);
	asyncReplicator_.Stop();
}

void Clusterizator::Stop(bool disable) noexcept {
	std::lock_guard lck(mtx_);
	clusterReplicator_.Stop(disable);
	asyncReplicator_.Stop(disable);
	if (disable) {
		enabled_.store(false, std::memory_order_release);
	}
}

Error Clusterizator::SuggestLeader(const NodeData& suggestion, NodeData& response) {
	return clusterReplicator_.SuggestLeader(suggestion, response);
}

Error Clusterizator::SetDesiredLeaderId(int leaderId, bool sendToOtherNodes) {
	return clusterReplicator_.SetDesiredLeaderId(leaderId, sendToOtherNodes);
}

Error Clusterizator::LeadersPing(const NodeData& leader) { return clusterReplicator_.LeadersPing(leader); }

RaftInfo Clusterizator::GetRaftInfo(bool allowTransitState, const RdxContext& ctx) const {
	return clusterReplicator_.GetRaftInfo(allowTransitState, ctx);
}

bool Clusterizator::NamespaceIsInClusterConfig(std::string_view nsName) { return clusterReplicator_.NamespaceIsInClusterConfig(nsName); }

bool Clusterizator::NamesapceIsInReplicationConfig(std::string_view nsName) {
	return clusterReplicator_.NamespaceIsInClusterConfig(nsName) || asyncReplicator_.NamespaceIsInAsyncConfig(nsName);
}

Error Clusterizator::Replicate(UpdateRecord&& rec, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	UpdatesContainer recs(1);
	recs[0] = std::move(rec);
	return Clusterizator::Replicate(std::move(recs), std::move(beforeWaitF), ctx);
}

Error Clusterizator::Replicate(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	if (replicationIsNotRequired(recs)) return errOK;

	std::pair<Error, bool> res;
	if (ctx.GetOriginLSN().isEmpty()) {
		res = updatesQueue_.Push(std::move(recs), std::move(beforeWaitF), ctx);
	} else {
		// Update can't be replicated to cluster from another node, so may only be replicated to async replicas
		res = updatesQueue_.PushAsync(std::move(recs));
	}
	if (res.second) {
		return res.first;
	}
	return Error();	 // This namespace is not taking part in any replication
}

Error Clusterizator::ReplicateAsync(UpdateRecord&& rec, const RdxContext& ctx) {
	UpdatesContainer recs(1);
	recs[0] = std::move(rec);
	return Clusterizator::ReplicateAsync(std::move(recs), ctx);
}

Error Clusterizator::ReplicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) {
	if (replicationIsNotRequired(recs)) return errOK;

	std::pair<Error, bool> res;
	if (ctx.GetOriginLSN().isEmpty()) {
		res = updatesQueue_.PushNowait(std::move(recs));
	} else {
		// Update can't be replicated to cluster from another node, so may only be replicated to async replicas
		res = updatesQueue_.PushAsync(std::move(recs));
	}
	return Error();	 // This namespace is not taking part in any replication
}

bool Clusterizator::replicationIsNotRequired(const UpdatesContainer& recs) noexcept {
	if (recs.empty()) return true;
	std::string_view name = recs[0].GetNsName();
	return name.size() && name[0] == '#';
}

void Clusterizator::validateConfig() const {
	auto& clusterConf = clusterReplicator_.Config();
	auto& asyncConf = asyncReplicator_.Config();
	if (!asyncConf.has_value() && !clusterConf.has_value()) {
		throw Error(errParams, "Config is not set");
	}
}

}  // namespace cluster
}  // namespace reindexer
