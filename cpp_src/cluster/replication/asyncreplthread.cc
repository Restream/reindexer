#include "asyncreplthread.h"
#include "sharedsyncstate.h"
#include "tools/catch_and_return.h"

namespace reindexer::cluster {

AsyncThreadParam::AsyncThreadParam(const std::vector<AsyncReplNodeConfig>* n, AsyncReplicationMode replMode, SharedSyncState& syncState)
	: nodes_(n), replMode_(replMode), syncState_(syncState) {
	assert(nodes_);
}

Error AsyncThreadParam::CheckReplicationMode(uint32_t nodeId) const noexcept {
	try {
		auto replMode = replMode_;
		const auto& nodeReplMode = (*nodes_)[nodeId].GetReplicationMode();
		if (nodeReplMode.has_value()) {
			replMode = nodeReplMode.value();
		}
		if (replMode == AsyncReplicationMode::FromClusterLeader) {
			const auto rp = syncState_.GetRolesPair();
			if (rp.first.role != rp.second.role || (rp.first.role != RaftInfo::Role::Leader && rp.first.role != RaftInfo::Role::None)) {
				return Error(errParams,
							 "Current node has roles '{}:{}', but role 'leader' (or 'none') is required to replicate, when "
							 "replication mode set to 'from_sync_leader'",
							 RaftInfo::RoleToStr(rp.first.role), RaftInfo::RoleToStr(rp.second.role));
			}
		}
	}
	CATCH_AND_RETURN;
	return Error();
}

AsyncReplThread::AsyncReplThread(int serverId, ReindexerImpl& thisNode, std::shared_ptr<BaseT::UpdatesQueueT> q,
								 const std::vector<AsyncReplNodeConfig>& nodesList, AsyncReplicationMode replMode,
								 SharedSyncState& syncState, ReplicationStatsCollector statsCollector, const Logger& l)
	: base_(serverId, thisNode, std::move(q), AsyncThreadParam(&nodesList, replMode, syncState), statsCollector, l) {}

AsyncReplThread::~AsyncReplThread() {
	if (th.joinable()) {
		SendTerminate();
		th.join();
	}
}

void AsyncReplThread::Run(ReplThreadConfig config, std::vector<std::pair<uint32_t, AsyncReplNodeConfig>>&& nodesList,
						  size_t totalNodesCount) {
	assert(!th.joinable());
	th = std::thread([this, config = std::move(config), nodesList = std::move(nodesList), totalNodesCount]() mutable {
		base_.Run(std::move(config), nodesList, 0, totalNodesCount);
	});
}

void AsyncReplThread::SendTerminate() noexcept { base_.SetTerminate(true); }

void AsyncReplThread::AwaitTermination() {
	assert(base_.Terminated() == true);
	th.join();
	base_.SetTerminate(false);
}

}  // namespace reindexer::cluster
