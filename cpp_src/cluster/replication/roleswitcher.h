#pragma once

#include <chrono>
#include "client/cororeindexer.h"
#include "cluster/stats/relicationstatscollector.h"
#include "cluster/stats/synchronizationlist.h"
#include "coroutine/channel.h"
#include "leadersyncer.h"
#include "sharedsyncstate.h"

namespace reindexer {

class ReindexerImpl;

namespace cluster {

class Logger;

class RoleSwitcher {
public:
	using NsNamesHashSetT = fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str>;
	struct Config {
		bool enableCompression = false;
		int clusterId = 0;
		int serverId = -1;
		NsNamesHashSetT namespaces;
		std::function<void()> onRoleSwitchDone;
		std::chrono::milliseconds netTimeout;
		int64_t maxWALDepthOnForceSync = -1;
		int syncThreads = -1;
		int maxConcurrentSnapshotsPerNode = -1;
	};

	RoleSwitcher(SharedSyncState<> &, SynchronizationList &, ReindexerImpl &, const ReplicationStatsCollector &, const Logger &);

	void Run(std::vector<std::string> &&dsns, RoleSwitcher::Config &&cfg);
	void OnRoleChanged() {
		std::lock_guard lck(mtx_);
		if (syncer_) {
			syncer_->Terminate();
		}
		roleSwitchAsync_.send();
	}
	void SetTerminationFlag(bool val) noexcept {
		std::lock_guard lck(mtx_);
		terminate_ = val;
		if (val) {
			if (syncer_) {
				syncer_->Terminate();
			}
			roleSwitchAsync_.send();
		}
	}

private:
	struct Node {
		std::string dsn;
		client::CoroReindexer client;
	};

	static constexpr std::string_view logModuleName() noexcept { return std::string_view("roleswitcher"); }
	void await();
	void notify() {
		if (!awaitCh_.full()) awaitCh_.push(true);
	}
	void terminate() { awaitCh_.close(); }
	void handleRoleSwitch();
	template <typename ContainerT>
	void switchNamespaces(const RaftInfo &state, const ContainerT &namespaces);
	void handleInitialSync(RaftInfo::Role newRole);
	void initialLeadersSync();
	Error getNodesListForNs(std::string_view nsName, std::list<reindexer::cluster::LeaderSyncQueue::Entry> &syncQueue);
	NsNamesHashSetT collectNsNames();
	template <typename RxT>
	Error appendNsNamesFrom(RxT &rx, RoleSwitcher::NsNamesHashSetT &set);
	void connectNodes();
	void disconnectNodes();
	size_t getConsensusCnt() const noexcept { return GetConsensusForN(nodes_.size() + 1); }
	bool isTerminated() const noexcept { return !awaitCh_.opened(); }

	std::vector<Node> nodes_;
	net::ev::dynamic_loop loop_;
	SharedSyncState<> &sharedSyncState_;
	ReindexerImpl &thisNode_;
	ReplicationStatsCollector statsCollector_;
	std::chrono::time_point<std::chrono::high_resolution_clock> roleSwitchTm_;
	coroutine::channel<bool> awaitCh_;
	RaftInfo::Role curRole_ = RaftInfo::Role::None;
	net::ev::timer leaderResyncTimer_;
	coroutine::wait_group leaderResyncWg_;
	RdxContext ctx_;
	net::ev::async roleSwitchAsync_;
	std::atomic<bool> terminate_ = {false};
	SynchronizationList &syncList_;
	bool timerIsCanceled_ = false;
	Config cfg_;

	std::mutex mtx_;
	std::unique_ptr<LeaderSyncer> syncer_;
	const Logger &log_;
};

}  // namespace cluster
}  // namespace reindexer
