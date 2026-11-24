#pragma once

#include "cluster/config.h"
#include "core/dbconfig.h"
#include "estl/mutex.h"
#include "idatareplicator.h"
#include "net/ev/ev.h"
#include "replication/asyncdatareplicator.h"
#include "replication/clusterdatareplicator.h"

namespace reindexer {

class ReindexerImpl;

namespace cluster {

class [[nodiscard]] ClusterManager : public IDataReplicator, public IDataSyncer {
public:
	ClusterManager(ReindexerImpl& thisNode, size_t maxUpdatesSize);

	void Configure(ReplicationConfigData replConfig);
	void Configure(ClusterConfigData clusterConfig);
	void Configure(AsyncReplConfigData asyncConfig);
	RolesSyncResult SynchronizeReplicationRoles();
	bool IsExpectingAsyncReplStartup() const noexcept;
	bool IsExpectingClusterStartup() const noexcept;
	Error StartClusterRepl() noexcept;
	Error StartAsyncRepl() noexcept;
	void StopCluster() noexcept;
	void StopAsyncRepl() noexcept;
	void Stop(bool disable = false) noexcept;
	void Enable() noexcept { enabled_.store(true, std::memory_order_release); }
	bool Enabled() const noexcept { return enabled_.load(std::memory_order_acquire); }
	void SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response);
	void SetDesiredLeaderId(int leaderId, bool sendToOtherNodes);
	void ForceElections();
	void LeadersPing(const cluster::NodeData&);
	RaftInfo GetRaftInfo(bool allowTransitState, const RdxContext& ctx) const;
	bool NamespaceIsInClusterConfig(std::string_view nsName);
	bool NamesapceIsInReplicationConfig(std::string_view nsName);

	Error Replicate(UpdatesContainer&& recs, const std::function<void()>& beforeWaitF, const RdxContext& ctx) override final;
	Error ReplicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) override final;
	void AwaitInitialSync(const NamespaceName& nsName, const RdxContext& ctx) const override final {
		if (enabled_.load(std::memory_order_acquire)) {
			sharedSyncState_.AwaitInitialSync(nsName, ctx);
		}
	}
	void AwaitInitialSync(const RdxContext& ctx) const override final {
		if (enabled_.load(std::memory_order_acquire)) {
			sharedSyncState_.AwaitInitialSync(ctx);
		}
	}
	bool IsInitialSyncDone(const NamespaceName& name) const override final {
		return !enabled_.load(std::memory_order_acquire) || sharedSyncState_.IsInitialSyncDone(name);
	}
	bool IsInitialSyncDone() const override final {
		return !enabled_.load(std::memory_order_acquire) || sharedSyncState_.IsInitialSyncDone();
	}
	bool IsClusterNamespace(std::string_view nsName) const override final {
		return enabled_.load(std::memory_order_acquire) && sharedSyncState_.IsClusterNamespace(nsName);
	}
	std::optional<SharedSyncState::RejectedAsyncReplConfig> GetRejectedAsyncReplConfig() const noexcept {
		return sharedSyncState_.GetRejectedAsyncReplConfig();
	}
	void ResetRejectedAsyncReplConfig() noexcept { sharedSyncState_.ResetRejectedAsyncReplConfig(); }
	bool IsRaftCluster() const noexcept { return sharedSyncState_.IsRaftCluster(); }
	ReplicationStats GetAsyncReplicationStats() const;
	ReplicationStats GetClusterReplicationStats(std::optional<int64_t> thisNodeSourceId = std::nullopt) const;
	void SetAsyncReplicatonLogLevel(LogLevel level) noexcept;
	void SetClusterReplicatonLogLevel(LogLevel level) noexcept;

private:
	constexpr std::string_view logModuleName() noexcept { return std::string_view("clusterizator"); }

	static bool replicationIsNotRequired(const UpdatesContainer& recs) noexcept;
	// Local updates and the primary cluster's updates are replicated to the RAFT cluster. Updates from this node's RAFT or async leader
	// are replicated further to the async replicas only
	static bool isReplicatedToCluster(const RdxContext& ctx) noexcept;
	void validateConfig() const;
	void updateManagerLogLevel() noexcept;

	mutable mutex mtx_;
	UpdatesQueuePair<updates::UpdateRecord> updatesQueue_;
	SharedSyncState sharedSyncState_;
	ClusterDataReplicator clusterReplicator_;
	AsyncDataReplicator asyncReplicator_;
	std::atomic<bool> enabled_ = {false};
	Logger log_;
	std::shared_ptr<NamespacesSyncScheduler> nssSyncScheduler_;
};

}  // namespace cluster
}  // namespace reindexer
