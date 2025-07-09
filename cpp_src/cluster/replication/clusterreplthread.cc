#include "clusterreplthread.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "sharedsyncstate.h"

namespace reindexer::cluster {

ClusterThreadParam::ClusterThreadParam(const NsNamesHashSetT* namespaces, coroutine::channel<bool>& ch, SharedSyncState& st,
									   SynchronizationList& syncList, std::function<void()> cb)
	: namespaces_(namespaces),
	  leadershipAwaitCh_(ch),
	  sharedSyncState_(st),
	  requestElectionsRestartCb_(std::move(cb)),
	  syncList_(syncList) {
	assert(namespaces_);
}

void ClusterThreadParam::OnNewNsAppearance(const NamespaceName& ns) { sharedSyncState_.MarkSynchronized(ns); }

void ClusterThreadParam::OnUpdateReplicationFailure() {
	if (sharedSyncState_.GetRolesPair().second.role == RaftInfo::Role::Leader) {
		requestElectionsRestartCb_();
	}
}

ClusterReplThread::ClusterReplThread(int serverId, ReindexerImpl& thisNode, const NsNamesHashSetT* namespaces,
									 std::shared_ptr<updates::UpdatesQueue<updates::UpdateRecord, ReplicationStatsCollector, Logger>> q,
									 SharedSyncState& syncState, SynchronizationList& syncList,
									 std::function<void()> requestElectionsRestartCb, ReplicationStatsCollector statsCollector,
									 const Logger& l)
	: base_(serverId, thisNode, std::move(q),
			ClusterThreadParam(namespaces, leadershipAwaitCh, syncState, syncList, std::move(requestElectionsRestartCb)), statsCollector,
			l),
	  sharedSyncState_(syncState) {
	roleSwitchAsync_.set(base_.loop);
	roleSwitchAsync_.set([this](net::ev::async& watcher) {
		watcher.loop.spawn([this]() noexcept {
			if (base_.Terminated()) {
				leadershipAwaitCh.close();
				return;
			}
			auto newState = sharedSyncState_.CurrentRole();
			base_.SetNodesRequireResync();
			if (newState.role == RaftInfo::Role::Leader) {
				if (leadershipAwaitCh.opened() && sharedSyncState_.IsInitialSyncDone()) {
					leadershipAwaitCh.close();
				}
			} else {
				if (!leadershipAwaitCh.opened()) {
					leadershipAwaitCh.reopen();
				}
				base_.DisconnectNodes();
			}
			base_.SendUpdatesAsyncNotification();  // To prevent stucking on updates loop
		});
	});
}

ClusterReplThread::~ClusterReplThread() {
	if (th.joinable()) {
		SendTerminate();
		th.join();
	}
}

void ClusterReplThread::Run(ReplThreadConfig config, std::vector<std::pair<uint32_t, ClusterNodeConfig>>&& nodesList,
							size_t totalNodesCount) {
	assert(!th.joinable());
	roleSwitchAsync_.start();
	th = std::thread([this, config = std::move(config), nodesList = std::move(nodesList), totalNodesCount]() mutable {
		base_.Run(std::move(config), nodesList, GetConsensusForN(totalNodesCount), totalNodesCount - 1);

		roleSwitchAsync_.stop();
	});
}

void ClusterReplThread::SendTerminate() noexcept {
	base_.SetTerminate(true);
	// This will close channels
	roleSwitchAsync_.send();
}

void ClusterReplThread::AwaitTermination() {
	assert(base_.Terminated() == true);
	th.join();
	base_.SetTerminate(false);
}

void ClusterReplThread::OnRoleSwitch() { roleSwitchAsync_.send(); }

}  // namespace reindexer::cluster
