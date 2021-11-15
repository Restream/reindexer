#pragma once

#include "replicationthread.h"

namespace reindexer {
namespace cluster {

class AsyncThreadParam {
public:
	AsyncThreadParam(const std::vector<AsyncReplNodeConfig> *n) : nodes_(n) { assert(nodes_); }
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

private:
	const std::vector<AsyncReplNodeConfig> *nodes_;
};

class AsyncReplThread {
public:
	using BaseT = ReplThread<AsyncThreadParam>;
	AsyncReplThread(int serverId, ReindexerImpl &thisNode, std::shared_ptr<BaseT::UpdatesQueueT> q,
					const std::vector<AsyncReplNodeConfig> &nodesList, ReplicationStatsCollector statsCollector);
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
