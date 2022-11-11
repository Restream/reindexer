#include "asyncreplthread.h"

namespace reindexer {
namespace cluster {

AsyncReplThread::AsyncReplThread(int serverId, ReindexerImpl& thisNode, std::shared_ptr<BaseT::UpdatesQueueT> q,
								 const std::vector<AsyncReplNodeConfig>& nodesList, AsyncReplicationMode replMode,
								 SharedSyncState<>& syncState, ReplicationStatsCollector statsCollector)
	: base_(serverId, thisNode, std::move(q), AsyncThreadParam(&nodesList, replMode, syncState), statsCollector) {}

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

}  // namespace cluster
}  // namespace reindexer
