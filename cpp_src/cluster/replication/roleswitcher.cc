#include "roleswitcher.h"
#include "cluster/logger.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "net/cproto/cproto.h"

namespace reindexer {
namespace cluster {

constexpr auto kLeaderNsResyncInterval = std::chrono::milliseconds(1000);

RoleSwitcher::RoleSwitcher(SharedSyncState& syncState, SynchronizationList& syncList, ReindexerImpl& thisNode,
						   const ReplicationStatsCollector& statsCollector, const Logger& l)
	: sharedSyncState_(syncState), thisNode_(thisNode), statsCollector_(statsCollector), syncList_(syncList), log_(l) {
	leaderResyncTimer_.set(loop_);
	roleSwitchAsync_.set(loop_);
}

void RoleSwitcher::Run(std::vector<DSN>&& dsns, RoleSwitcher::Config&& cfg) {
	cfg_ = std::move(cfg);
	client::ReindexerConfig clientCfg;
	clientCfg.NetTimeout = cfg_.netTimeout;
	clientCfg.EnableCompression = cfg_.enableCompression;
	clientCfg.RequestDedicatedThread = true;

	if (!awaitCh_.opened()) {
		awaitCh_.reopen();
	}

	nodes_.reserve(dsns.size());
	for (auto&& dsn : dsns) {
		nodes_.emplace_back(Node{std::move(dsn), client::CoroReindexer{clientCfg}});
	}
	assert(cfg_.onRoleSwitchDone);

	roleSwitchAsync_.set([this](net::ev::async&) {
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop_.spawn([this]() noexcept {
			if (terminate_) {
				terminate();
			} else {
				notify();
			}
		});
	});
	roleSwitchAsync_.start();
	roleSwitchAsync_.send();  // Initiate role switch on startup
	// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
	loop_.spawn([this]() noexcept {
		while (!terminate_) {
			await();
		}
	});
	loop_.run();
	roleSwitchAsync_.stop();
}

void RoleSwitcher::OnRoleChanged() noexcept {
	lock_guard lck(mtx_);
	if (syncer_) {
		syncer_->Terminate();
	}
	roleSwitchAsync_.send();
}

void RoleSwitcher::SetTerminationFlag(bool val) noexcept {
	lock_guard lck(mtx_);
	terminate_ = val;
	if (val) {
		if (syncer_) {
			syncer_->Terminate();
		}
		roleSwitchAsync_.send();
	}
}

void RoleSwitcher::await() {
	std::ignore = awaitCh_.pop();
	if (!isTerminated()) {
		handleRoleSwitch();
	}
}

void RoleSwitcher::notify() {
	if (!awaitCh_.full()) {
		awaitCh_.push(true);
	}
}

void RoleSwitcher::terminate() { awaitCh_.close(); }

void RoleSwitcher::handleRoleSwitch() {
	auto rolesPair = sharedSyncState_.GetRolesPair();
	auto& newState = rolesPair.second;
	auto& curState = rolesPair.first;
	while (newState != curState) {
		logInfo("{}: onRoleSwitchAsync: {}({}) -> {}({})", cfg_.serverId, RaftInfo::RoleToStr(curState.role), curState.leaderId,
				RaftInfo::RoleToStr(newState.role), newState.leaderId);

		// Set DB status
		ClusterOperationStatus status;
		switch (newState.role) {
			case RaftInfo::Role::None:
			case RaftInfo::Role::Leader:
				status.role = ClusterOperationStatus::Role::None;  // -V1048
				break;
			case RaftInfo::Role::Follower:
				status.role = ClusterOperationStatus::Role::ClusterReplica;
				break;
			case RaftInfo::Role::Candidate:
				status.role = (curState.role == RaftInfo::Role::Leader) ? ClusterOperationStatus::Role::None
																		: ClusterOperationStatus::Role::ClusterReplica;
				break;
		}
		status.leaderId = newState.leaderId;
		thisNode_.setClusterOperationStatus(std::move(status), ctx_);

		if (cfg_.namespaces.empty()) {
			std::vector<std::string> namespaces;
			std::vector<NamespaceDef> nsDefs;
			auto err = thisNode_.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideTemporary().HideSystem(), RdxContext());
			if (!err.ok()) {
				logInfo("{}: onRoleSwitchAsync error: {}", cfg_.serverId, err.what());
			}
			namespaces.reserve(nsDefs.size());
			for (auto& ns : nsDefs) {
				namespaces.emplace_back(std::move(ns.name));
			}
			switchNamespaces(newState, namespaces);
		} else {
			switchNamespaces(newState, cfg_.namespaces);
		}

		curState = newState;
		syncList_.Clear();
		newState = sharedSyncState_.TryTransitRole(newState);
	}

	if (curRole_ != newState.role) {
		handleInitialSync(newState.role);
		curRole_ = newState.role;
	}
}

template <typename ContainerT>
void RoleSwitcher::switchNamespaces(const RaftInfo& newState, const ContainerT& namespaces) {
	for (auto& nsName : namespaces) {
		// 0) Mark local namespaces with new leader data
		auto nsPtr = thisNode_.getNamespaceNoThrow(nsName, ctx_);
		if (nsPtr) {
			ClusterOperationStatus status;
			switch (newState.role) {
				case RaftInfo::Role::None:
				case RaftInfo::Role::Leader:
					status.role = ClusterOperationStatus::Role::None;  // -V1048
					break;
				case RaftInfo::Role::Follower:
					status.role = ClusterOperationStatus::Role::ClusterReplica;
					break;
				case RaftInfo::Role::Candidate:
					status.role = (curRole_ == RaftInfo::Role::Leader) ? ClusterOperationStatus::Role::None
																	   : ClusterOperationStatus::Role::ClusterReplica;
					break;
			}
			status.leaderId = newState.leaderId;
			logInfo("{}: Setting new role '{}' and leader id {} for '{}'", cfg_.serverId, RaftInfo::RoleToStr(newState.role),
					status.leaderId, nsName);
			if (auto err = nsPtr->SetClusterOperationStatus(std::move(status), ctx_); !err.ok()) {
				logWarn("SetClusterOperationStatus for the '{}' namespace error: {}", nsName, err.what());
			}
		}
	}
}

void RoleSwitcher::handleInitialSync(RaftInfo::Role newRole) {
	auto leaderSyncFn = [this](net::ev::timer& timer, int) {
		assert(leaderResyncWg_.wait_count() == 0);
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop_.spawn(leaderResyncWg_, [this, &timer]() noexcept {
			try {
				connectNodes();
				initialLeadersSync();
				disconnectNodes();
			} catch (const Error& e) {
				Error err = e;	// MSVC19 in our CI can not handle cath in lambda without this
				disconnectNodes();

				if (!timerIsCanceled_) {
					double interval = std::chrono::duration_cast<std::chrono::milliseconds>(kLeaderNsResyncInterval).count();
					timer.start(interval / 1000);
					logWarn("{}: Leader's sync failed. Last error is: {}. New sync was scheduled in {} ms", cfg_.serverId, err.what(),
							std::chrono::duration_cast<std::chrono::milliseconds>(kLeaderNsResyncInterval).count());
				}
				return;
			}

			log_.Info([this] { rtfmt("{}: Leader's resync done", cfg_.serverId); });
			statsCollector_.OnSyncStateChanged(ReplicationStatCounter::kLeaderUID, NodeStats::SyncState::OnlineReplication);
			cfg_.onRoleSwitchDone();
		});
	};
	leaderResyncTimer_.set(leaderSyncFn);

	logInfo("{}: got new role: '{}'", cfg_.serverId, RaftInfo::RoleToStr(newRole));
	if (newRole == RaftInfo::Role::Leader) {
		log_.Info([this] { rtfmt("{}: Leader resync has begun", cfg_.serverId); });
		statsCollector_.OnSyncStateChanged(ReplicationStatCounter::kLeaderUID, NodeStats::SyncState::InitialLeaderSync);
		timerIsCanceled_ = false;
		roleSwitchTm_ = steady_clock_w::now();
		leaderSyncFn(leaderResyncTimer_, 0);
	} else {
		if (curRole_ == RaftInfo::Role::Leader) {
			log_.Info([this] { rtfmt("{}: Leader resync timer was canceled", cfg_.serverId); });
			timerIsCanceled_ = true;
			leaderResyncTimer_.stop();
			leaderResyncWg_.wait();
		}
		cfg_.onRoleSwitchDone();
	}
}

void RoleSwitcher::initialLeadersSync() {
	Error lastErr;
	coroutine::wait_group leaderSyncWg;
	std::vector<int64_t> synchronizedNodes(nodes_.size(), SynchronizationList::kEmptyID);
	elist<LeaderSyncQueue::Entry> syncQueue;

	NsNamesHashSetT nsList;
	if (cfg_.namespaces.size()) {
		nsList = cfg_.namespaces;
	} else {
		nsList = collectNsNames();
	}

	// coroutine::tokens_pool<bool> syncTokens(maxConcurrentSnapshots);
	std::vector<size_t> snapshotsOnNode(nodes_.size(), 0);
	for (auto& ns : nsList) {
		logInfo("{}: Leader's resync timer: create coroutine for '{}'", cfg_.serverId, ns);
		loop_.spawn(leaderSyncWg, [this, &lastErr, &ns, &syncQueue]() mutable noexcept {
			try {
				auto err = getNodesListForNs(ns, syncQueue);
				if (!err.ok()) {
					lastErr = std::move(err);
				}
			} catch (...) {
				assert(false);
			}
		});
	}
	leaderSyncWg.wait();
	if (!lastErr.ok()) {
		throw lastErr;
	}

	constexpr auto kMaxThreadsCount = 32;
	const auto syncThreadsCount = (cfg_.syncThreads > 0) ? std::min(cfg_.syncThreads, kMaxThreadsCount) : kMaxThreadsCount;
	std::vector<DSN> dsns(nodes_.size());
	std::transform(nodes_.begin(), nodes_.end(), dsns.begin(), [](const Node& node) { return node.dsn; });
	LeaderSyncer::Config syncerCfg{
		dsns,
		cfg_.maxWALDepthOnForceSync,
		cfg_.clusterId,
		cfg_.serverId,
		size_t(syncThreadsCount),
		cfg_.maxConcurrentSnapshotsPerNode > 0 ? size_t(cfg_.maxConcurrentSnapshotsPerNode) : net::cproto::kMaxConcurentSnapshots,
		cfg_.enableCompression,
		cfg_.netTimeout};

	for (size_t nodeId = 0; nodeId < synchronizedNodes.size(); ++nodeId) {
		for (auto& e : syncQueue) {
			if (std::find(e.nodes.begin(), e.nodes.end(), nodeId) == e.nodes.cend()) {
				synchronizedNodes[nodeId] = SynchronizationList::kUnsynchronizedID;
				continue;
			}
		}
	}
	{
		unique_lock lck(mtx_);
		if (terminate_) {
			throw Error(errCanceled, "Terminated");
		}
		syncer_ = std::make_unique<LeaderSyncer>(syncerCfg, log_);
		lck.unlock();
		logInfo("{}: Syncing namespaces from remote nodes", cfg_.serverId);
		lastErr = syncer_->Sync(std::move(syncQueue), sharedSyncState_, thisNode_, statsCollector_);
		logInfo("{}: Sync namespaces from remote nodes done: {}", cfg_.serverId, lastErr.ok() ? "OK" : lastErr.what());
		lck.lock();
		syncer_.reset();
		lck.unlock();
	}
	if (!lastErr.ok()) {
		throw lastErr;
	}
	syncList_.Init(std::move(synchronizedNodes));
	assert(!sharedSyncState_.IsInitialSyncDone());
	sharedSyncState_.MarkSynchronized();
	statsCollector_.OnInitialSyncDone(std::chrono::duration_cast<std::chrono::microseconds>(steady_clock_w::now() - roleSwitchTm_));
}

Error RoleSwitcher::awaitRoleSwitchForNamespace(client::CoroReindexer& client, const NamespaceName& nsName, ReplicationStateV2& st) {
	uint32_t step = 0;
	const bool isDbStatus = nsName.empty();
	do {
		auto err = client.WithTimeout(kStatusCmdTimeout).GetReplState(nsName, st);
		if (!err.ok()) {
			return err;
		}
		if (st.clusterStatus.role == ClusterOperationStatus::Role::ClusterReplica && st.clusterStatus.leaderId == cfg_.serverId) {
			return {};
		}
		if (step++ >= kMaxRetriesOnRoleSwitchAwait) {
			return Error(errTimeout, "Gave up on the remote node role switch awaiting for the '{}'",
						 isDbStatus ? "whole database" : std::string_view(nsName));
		}
		logInfo("{}: Awaiting role switch on the remote node for the '{}'. Current status is {{ role: {}; leader: {} }}", cfg_.serverId,
				isDbStatus ? "whole database" : std::string_view(nsName), st.clusterStatus.RoleStr(), st.clusterStatus.leaderId);
		loop_.sleep(kRoleSwitchStepTime);
		auto [cur, next] = sharedSyncState_.GetRolesPair();
		if (cur != next) {
			return Error(errCanceled, "New role switch was scheduled ({} -> {}) during the sync", RaftInfo::RoleToStr(cur.role),
						 RaftInfo::RoleToStr(next.role));
		}
		if (cur.role != RaftInfo::Role::Leader) {
			return Error(errCanceled, "Role was switch to {} during the sync", RaftInfo::RoleToStr(cur.role));
		}
	} while (true);
}

Error RoleSwitcher::getNodesListForNs(const NamespaceName& nsName, elist<LeaderSyncQueue::Entry>& syncQueue) {
	// 1) Find most recent data among all the followers
	LeaderSyncQueue::Entry nsEntry;
	nsEntry.nsName = nsName;
	{
		ReplicationStateV2 state;
		auto err = thisNode_.GetReplState(nsEntry.nsName, state, RdxContext());
		if (err.ok()) {
			nsEntry.localLsn = nsEntry.latestLsn = ExtendedLsn(state.nsVersion, state.lastLsn);
			nsEntry.localData.hash = state.dataHash;
			nsEntry.localData.count = state.dataCount;
		}
		logInfo("{}: Begin leader's sync for '{}'. Ns version: {}, lsn: {}", cfg_.serverId, nsEntry.nsName, nsEntry.localLsn.NsVersion(),
				nsEntry.localLsn.LSN());
	}
	size_t responses = 1;
	coroutine::wait_group lwg;

	std::vector<std::pair<Error, ReplicationStateV2>> nodesStates(nodes_.size());
	for (int id = 0; size_t(id) < nodes_.size(); ++id) {
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop_.spawn(lwg, [this, id, &nodesStates]() noexcept {
			ReplicationStateV2 st;
			nodesStates[id].first = awaitRoleSwitchForNamespace(nodes_[id].client, NamespaceName(), st);
		});
	}
	lwg.wait();

	for (int id = 0; size_t(id) < nodes_.size(); ++id) {
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop_.spawn(lwg, [this, id, &nsEntry, &responses, &nodesStates]() noexcept {
			Error err = nodesStates[id].first;
			if (err.ok()) {
				err = awaitRoleSwitchForNamespace(nodes_[id].client, nsEntry.nsName, nodesStates[id].second);
			}
			if (err.ok()) {
				const auto& state = nodesStates[id].second;
				++responses;
				logInfo("{}: Got ns version {} and lsn {} from node {} for '{}'", cfg_.serverId, state.nsVersion, state.lastLsn, id,
						nsEntry.nsName);
				const ExtendedLsn remoteLsn(state.nsVersion, state.lastLsn);
				if (nsEntry.latestLsn.IsEmpty() ||
					(nsEntry.latestLsn.IsCompatibleByNsVersion(remoteLsn) && !remoteLsn.LSN().isEmpty() &&
					 nsEntry.latestLsn.LSN().Counter() < remoteLsn.LSN().Counter()) ||
					(!remoteLsn.NsVersion().isEmpty() && remoteLsn.NsVersion().Counter() > nsEntry.latestLsn.NsVersion().Counter())) {
					nsEntry.nodes.clear();
					nsEntry.nodes.emplace_back(id);
					nsEntry.data.clear();
					nsEntry.data.emplace_back(LeaderSyncQueue::Entry::NodeData{state.dataHash, state.dataCount});
					nsEntry.latestLsn = remoteLsn;
				} else if (nsEntry.latestLsn == remoteLsn) {
					const bool sameAsLocal =
						(nsEntry.localData.hash == state.dataHash && (!state.HasDataCount() || nsEntry.localData.count == state.dataCount));
					const bool sameAsRemote =
						(nsEntry.data.size() && nsEntry.data[0].hash == state.dataHash &&
						 (!nsEntry.data[0].HasDataCount() || !state.HasDataCount() || nsEntry.data[0].count == state.dataCount));
					if (sameAsLocal || sameAsRemote) {
						nsEntry.nodes.emplace_back(id);
						nsEntry.data.emplace_back(LeaderSyncQueue::Entry::NodeData{state.dataHash, state.dataCount});
					}
				}
			} else if (err.code() == errNotFound) {
				++responses;
			}
			nodesStates[id].first = std::move(err);
		});
	}
	logTrace("{} Awaiting namespace collecting...({})", cfg_.serverId, lwg.wait_count());
	lwg.wait();
	const auto consensusCnt = getConsensusCnt();
	if (responses < consensusCnt) {
		return Error(errClusterConsensus, "Unable to get enough responses. Got {} out of {}", responses, consensusCnt);
	}
	if (nsEntry.IsLocal()) {
		SyncTimeCounter timeCounter(SyncTimeCounter::Type::InitialWalSync, statsCollector_);
		logInfo("{}: Local namespace '{}' is up-to-date (ns version: {}, lsn: {})", cfg_.serverId, nsEntry.nsName,
				nsEntry.localLsn.NsVersion(), nsEntry.localLsn.LSN());
		sharedSyncState_.MarkSynchronized(std::move(nsEntry.nsName));
	} else {
		logInfo("{}: '{}'. Local ns version: {}, local lsn: {}, latest ns version: {}, latest lsn: {}", cfg_.serverId, nsName,
				nsEntry.localLsn.NsVersion(), nsEntry.localLsn.LSN(), nsEntry.latestLsn.NsVersion(), nsEntry.latestLsn.LSN());
		syncQueue.emplace_back(std::move(nsEntry));
	}
	return Error();
}

NsNamesHashSetT RoleSwitcher::collectNsNames() {
	NsNamesHashSetT set;
	coroutine::wait_group lwg;
	size_t responses = 1;
	for (size_t nodeId = 0; nodeId < nodes_.size(); ++nodeId) {
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop_.spawn(lwg, [this, nodeId, &set, &responses]() noexcept {
			auto& node = nodes_[nodeId];
			auto client = node.client.WithTimeout(kStatusCmdTimeout);
			auto err = appendNsNamesFrom(client, set);
			if (err.ok()) {
				++responses;
			} else {
				logInfo("{}:{} Unable to recv namespaces on initial sync: {}", cfg_.serverId, nodeId, err.what());
			}
		});
	}
	lwg.wait();
	const auto consensusCnt = getConsensusCnt();
	auto err = appendNsNamesFrom(thisNode_, set);
	if (err.ok()) {
		logInfo("{} Recv total {} namespaces on initial sync", cfg_.serverId, set.size());
	} else {
		throw Error(errClusterConsensus, "Unable to recv local namespaces on initial sync: {}", err.what());
	}
	if (responses < consensusCnt) {
		throw Error(errClusterConsensus, "Unable to get enough responses. Got {} out of {}", responses, consensusCnt);
	}

	return set;
}

template <typename RxT>
Error RoleSwitcher::appendNsNamesFrom(RxT& rx, NsNamesHashSetT& set) {
	std::vector<NamespaceDef> nsDefs;
	Error err;
	if constexpr (std::is_same_v<RxT, ReindexerImpl>) {
		err = rx.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideTemporary().HideSystem(), RdxContext());
	} else {
		err = rx.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideTemporary().HideSystem());
	}
	if (err.ok()) {
		for (auto&& def : nsDefs) {
			set.emplace(std::move(def.name));
		}
	}
	return err;
}

void RoleSwitcher::connectNodes() {
	const auto opts = client::ConnectOpts().CreateDBIfMissing().WithExpectedClusterID(cfg_.clusterId);
	for (auto& node : nodes_) {
		auto err = node.client.Connect(node.dsn, loop_, opts);
		(void)err;	// ignore. Error will be handled during the further requests
	}
}

void RoleSwitcher::disconnectNodes() {
	coroutine::wait_group swg;
	for (auto& node : nodes_) {
		loop_.spawn(swg, [&node]() noexcept { node.client.Stop(); }, k16kCoroStack);
	}
	swg.wait();
}

}  // namespace cluster
}  // namespace reindexer
