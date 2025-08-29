#pragma once

#include "cluster/stats/synchronizationlist.h"
#include "replicationthread.h"

namespace reindexer::cluster {

class SharedSyncState;

class [[nodiscard]] ClusterThreadParam {
public:
	ClusterThreadParam(const NsNamesHashSetT* namespaces, coroutine::channel<bool>& ch, SharedSyncState& st, SynchronizationList& syncList,
					   std::function<void()> cb);

	bool IsLeader() const noexcept { return !leadershipAwaitCh_.opened(); }
	void AwaitReplPermission() { leadershipAwaitCh_.pop(); }
	void OnNewNsAppearance(const NamespaceName& ns);
	void OnUpdateReplicationFailure();
	bool IsNamespaceInConfig(size_t, const NamespaceName& ns) const noexcept {
		return namespaces_->empty() || (namespaces_->find(ns) != namespaces_->end());
	}
	bool IsNamespaceInConfig(size_t, std::string_view ns) const noexcept {
		return namespaces_->empty() || (namespaces_->find(ns) != namespaces_->end());
	}
	void OnNodeBecameUnsynchonized(uint32_t nodeId) { syncList_.MarkUnsynchonized(nodeId); }
	void OnAllUpdatesReplicated(uint32_t nodeId, int64_t lastUpdateID) { syncList_.MarkSynchronized(nodeId, lastUpdateID); }
	void OnUpdateSucceed(uint32_t nodeId, int64_t lastUpdateID) { syncList_.MarkSynchronized(nodeId, lastUpdateID); }
	Error CheckReplicationMode() const noexcept { return Error(); }

private:
	const NsNamesHashSetT* namespaces_;
	coroutine::channel<bool>& leadershipAwaitCh_;
	SharedSyncState& sharedSyncState_;
	std::function<void()> requestElectionsRestartCb_;
	SynchronizationList& syncList_;
};

class [[nodiscard]] ClusterReplThread {
public:
	ClusterReplThread(int serverId, ReindexerImpl& thisNode, const NsNamesHashSetT*,
					  std::shared_ptr<updates::UpdatesQueue<updates::UpdateRecord, ReplicationStatsCollector, Logger>>, SharedSyncState&,
					  SynchronizationList&, std::function<void()> requestElectionsRestartCb, ReplicationStatsCollector, const Logger&);
	~ClusterReplThread();
	void Run(ReplThreadConfig config, std::vector<std::pair<uint32_t, ClusterNodeConfig>>&& nodesList, size_t totalNodesCount);
	void SendTerminate() noexcept;
	void AwaitTermination();
	void OnRoleSwitch();

private:
	std::thread th;
	coroutine::channel<bool> leadershipAwaitCh;
	net::ev::async roleSwitchAsync_;
	ReplThread<ClusterThreadParam> base_;
	SharedSyncState& sharedSyncState_;
	steady_clock_w::time_point roleSwitchTm_;
};

}  // namespace reindexer::cluster
