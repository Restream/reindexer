#pragma once

#include "replicationthread.h"

namespace reindexer::cluster {

class SharedSyncState;

class [[nodiscard]] AsyncThreadParam {
public:
	AsyncThreadParam(const std::vector<AsyncReplNodeConfig>* n, AsyncReplicationMode replMode, SharedSyncState& syncState);
	AsyncThreadParam(AsyncThreadParam&& o) = default;
	AsyncThreadParam(const AsyncThreadParam& o) = default;

	bool IsLeader() const noexcept { return true; }
	void AwaitReplPermission() const noexcept {}
	void OnNewNsAppearance(const NamespaceName&) const noexcept {}
	void OnUpdateReplicationFailure() const noexcept {}
	bool IsNamespaceInConfig(size_t nodeId, const NamespaceName& ns) const noexcept { return (*nodes_)[nodeId].Namespaces()->IsInList(ns); }
	bool IsNamespaceInConfig(size_t nodeId, std::string_view ns) const noexcept { return (*nodes_)[nodeId].Namespaces()->IsInList(ns); }
	void OnNodeBecameUnsynchonized(uint32_t) const noexcept {}
	void OnAllUpdatesReplicated(uint32_t, int64_t) const noexcept {}
	void OnUpdateSucceed(uint32_t, int64_t) const noexcept {}
	Error CheckReplicationMode(uint32_t nodeId) const noexcept;

private:
	const std::vector<AsyncReplNodeConfig>* nodes_;
	AsyncReplicationMode replMode_;
	SharedSyncState& syncState_;
};

class [[nodiscard]] AsyncReplThread {
public:
	using BaseT = ReplThread<AsyncThreadParam>;
	AsyncReplThread(int serverId, ReindexerImpl& thisNode, std::shared_ptr<BaseT::UpdatesQueueT>,
					const std::vector<AsyncReplNodeConfig>& nodesList, AsyncReplicationMode, SharedSyncState&, ReplicationStatsCollector,
					const Logger&);
	~AsyncReplThread();
	void Run(ReplThreadConfig config, std::vector<std::pair<uint32_t, AsyncReplNodeConfig> >&& nodesList, size_t totalNodesCount);
	void SendTerminate() noexcept;
	void AwaitTermination();

private:
	std::thread th;
	BaseT base_;
};

}  // namespace reindexer::cluster
