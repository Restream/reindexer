#include "asyncreplthread.h"
#include "sharedsyncstate.h"

namespace reindexer::cluster {

AsyncThreadParam::AsyncThreadParam(const std::vector<AsyncReplNodeConfig>* n, SharedSyncState& syncState)
	: nodes_(n), syncState_(syncState) {
	assert(nodes_);
}

bool AsyncThreadParam::IsLeader() const noexcept { return GetReplPermission().error.ok(); }

AsyncReplPermission AsyncThreadParam::GetReplPermission() const noexcept { return syncState_.GetAsyncReplPermission(); }

AsyncReplThread::AsyncReplThread(int serverId, ReindexerImpl& thisNode, std::shared_ptr<BaseT::UpdatesQueueT> q,
								 std::shared_ptr<NamespacesSyncScheduler> scheduler, const std::vector<AsyncReplNodeConfig>& nodesList,
								 SharedSyncState& syncState, ReplicationStatsCollector statsCollector, const Logger& l)
	: base_(serverId, thisNode, std::move(q), std::move(scheduler), AsyncThreadParam(&nodesList, syncState), statsCollector, l),
	  syncState_(syncState) {
	syncState_.AddAsyncReplPermissionObserver(this, [this] { base_.SendUpdatesAsyncNotification(); });
}

AsyncReplThread::~AsyncReplThread() {
	syncState_.RemoveAsyncReplPermissionObserver(this);
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
