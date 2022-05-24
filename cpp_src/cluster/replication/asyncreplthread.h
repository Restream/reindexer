#pragma once

#include "replicationthread.h"

namespace reindexer {
namespace cluster {

class AsyncThreadParam {
public:
	AsyncThreadParam(const std::vector<AsyncReplNodeConfig> *n, AsyncReplicationMode replMode, SharedSyncState<> &syncState)
		: nodes_(n), replMode_(replMode), syncState_(syncState) {
		assert(nodes_);
	}
	AsyncThreadParam(AsyncThreadParam &&o) = default;
	AsyncThreadParam(const AsyncThreadParam &o) = default;

	bool IsLeader() const noexcept { return true; }
	void AwaitReplPermission() const noexcept {}
	void OnNewNsAppearance(const std::string &) const noexcept {}
	void OnUpdateReplicationFailure() const noexcept {}
	bool IsNamespaceInConfig(size_t nodeId, std::string_view ns) const noexcept { return (*nodes_)[nodeId].Namespaces()->IsInList(ns); }
	void OnNodeBecameUnsynchonized(uint32_t) const noexcept {}
	void OnAllUpdatesReplicated(uint32_t, int64_t) const noexcept {}
	void OnUpdateSucceed(uint32_t, int64_t) const noexcept {}
	Error CheckReplicationMode(uint32_t nodeId) const noexcept {
		auto replMode = replMode_;
		const auto &nodeReplMode = (*nodes_)[nodeId].GetReplicationMode();
		if (nodeReplMode.has_value()) {
			replMode = nodeReplMode.value();
		}
		if (replMode == AsyncReplicationMode::FromClusterLeader) {
			const auto rp = syncState_.GetRolesPair();
			if (rp.first.role != rp.second.role || (rp.first.role != RaftInfo::Role::Leader && rp.first.role != RaftInfo::Role::None)) {
				return Error(errParams,
							 "Current node has roles '%s:%s', but role 'leader' (or 'none') is required to replicate, when "
							 "replication mode set to 'from_sync_leader'",
							 RaftInfo::RoleToStr(rp.first.role), RaftInfo::RoleToStr(rp.second.role));
			}
		}
		return Error();
	}

private:
	const std::vector<AsyncReplNodeConfig> *nodes_;
	AsyncReplicationMode replMode_;
	SharedSyncState<> &syncState_;
};

class AsyncReplThread {
public:
	using BaseT = ReplThread<AsyncThreadParam>;
	AsyncReplThread(int serverId, ReindexerImpl &thisNode, std::shared_ptr<BaseT::UpdatesQueueT> q,
					const std::vector<AsyncReplNodeConfig> &nodesList, AsyncReplicationMode replMode, SharedSyncState<> &syncState,
					ReplicationStatsCollector statsCollector);
	~AsyncReplThread();
	void Run(ReplThreadConfig config, std::vector<std::pair<uint32_t, AsyncReplNodeConfig> > &&nodesList, size_t totalNodesCount);
	void SendTerminate() noexcept;
	void AwaitTermination();

private:
	std::thread th;
	BaseT base_;
};

}  // namespace cluster
}  // namespace reindexer
