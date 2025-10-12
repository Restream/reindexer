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
	bool IsExpectingAsyncReplStartup() const noexcept;
	bool IsExpectingClusterStartup() const noexcept;
	Error StartClusterRepl();
	Error StartAsyncRepl();
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

	Error Replicate(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) override final;
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
	ReplicationStats GetAsyncReplicationStats() const;
	ReplicationStats GetClusterReplicationStats() const;
	void SetAsyncReplicatonLogLevel(LogLevel level) noexcept;
	void SetClusterReplicatonLogLevel(LogLevel level) noexcept;

private:
	constexpr std::string_view logModuleName() noexcept { return std::string_view("clusterizator"); }

	static bool replicationIsNotRequired(const UpdatesContainer& recs) noexcept;
	void validateConfig() const;

	mutable mutex mtx_;
	UpdatesQueuePair<updates::UpdateRecord> updatesQueue_;
	SharedSyncState sharedSyncState_;
	ClusterDataReplicator clusterReplicator_;
	AsyncDataReplicator asyncReplicator_;
	net::ev::async terminateAsync_;
	std::atomic<bool> enabled_ = {false};
	Logger log_;
};

}  // namespace cluster
}  // namespace reindexer
