#pragma once

#include "client/cororeindexer.h"
#include "cluster/config.h"
#include "core/dbconfig.h"
#include "insdatareplicator.h"
#include "net/ev/ev.h"
#include "replication/asyncdatareplicator.h"
#include "replication/clusterdatareplicator.h"

namespace reindexer {

class ReindexerImpl;

namespace cluster {

class Clusterizator : public INsDataReplicator {
public:
	Clusterizator(ReindexerImpl &thisNode, size_t maxUpdatesSize);

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
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	Error SetDesiredLeaderId(int leaderId, bool sendToOtherNodes);
	Error LeadersPing(const cluster::NodeData &);
	RaftInfo GetRaftInfo(bool allowTransitState, const RdxContext &ctx) const;
	bool NamespaceIsInClusterConfig(std::string_view nsName);
	bool NamesapceIsInReplicationConfig(std::string_view nsName);

	Error Replicate(UpdateRecord &&rec, std::function<void()> beforeWaitF, const RdxContext &ctx) override final;
	Error Replicate(UpdatesContainer &&recs, std::function<void()> beforeWaitF, const RdxContext &ctx) override final;
	Error ReplicateAsync(UpdateRecord &&rec, const RdxContext &ctx) override final;
	Error ReplicateAsync(UpdatesContainer &&recs, const RdxContext &ctx) override final;
	void AwaitInitialSync(std::string_view nsName, const RdxContext &ctx) const override final {
		if (enabled_.load(std::memory_order_acquire)) {
			sharedSyncState_.AwaitInitialSync(nsName, ctx);
		}
	}
	void AwaitInitialSync(const RdxContext &ctx) const override final {
		if (enabled_.load(std::memory_order_acquire)) {
			sharedSyncState_.AwaitInitialSync(ctx);
		}
	}
	bool IsInitialSyncDone(std::string_view name) const override final {
		return !enabled_.load(std::memory_order_acquire) || sharedSyncState_.IsInitialSyncDone(name);
	}
	bool IsInitialSyncDone() const override final {
		return !enabled_.load(std::memory_order_acquire) || sharedSyncState_.IsInitialSyncDone();
	}
	ReplicationStats GetAsyncReplicationStats() const { return asyncReplicator_.GetReplicationStats(); }
	ReplicationStats GetClusterReplicationStats() const { return clusterReplicator_.GetReplicationStats(); }
	void SetAsyncReplicatonLogLevel(LogLevel level) noexcept { asyncReplicator_.SetLogLevel(level); }
	void SetClusterReplicatonLogLevel(LogLevel level) noexcept { clusterReplicator_.SetLogLevel(level); }

private:
	static bool replicationIsNotRequired(const UpdatesContainer &recs) noexcept;
	void validateConfig() const;

	mutable std::mutex mtx_;
	UpdatesQueuePair<UpdateRecord> updatesQueue_;
	SharedSyncState<> sharedSyncState_;
	ClusterDataReplicator clusterReplicator_;
	AsyncDataReplicator asyncReplicator_;
	net::ev::async terminateAsync_;
	std::atomic<bool> enabled_ = {false};
};

}  // namespace cluster
}  // namespace reindexer
