#include "roleswitcher.h"
#include "client/snapshot.h"
#include "cluster/logger.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "coroutine/tokens_pool.h"
#include "net/cproto/cproto.h"
#include "tools/logger.h"

namespace reindexer {
namespace cluster {

constexpr auto kLeaderNsResyncInterval = std::chrono::milliseconds(1000);

RoleSwitcher::RoleSwitcher(SharedSyncState<>& syncState, SynchronizationList& syncList, ReindexerImpl& thisNode,
						   const ReplicationStatsCollector& statsCollector, const Logger& l)
	: sharedSyncState_(syncState), thisNode_(thisNode), statsCollector_(statsCollector), syncList_(syncList), log_(l) {
	leaderResyncTimer_.set(loop_);
	roleSwitchAsync_.set(loop_);
}

void RoleSwitcher::Run(std::vector<std::string>&& dsns, RoleSwitcher::Config&& cfg) {
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
		nodes_.emplace_back(Node{std::move(dsn), {clientCfg}});
	}
	assert(cfg_.onRoleSwitchDone);

	roleSwitchAsync_.set([this](net::ev::async&) {
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
	loop_.spawn([this]() noexcept {
		while (!terminate_) {
			await();
		}
	});
	loop_.run();
	roleSwitchAsync_.stop();
}

void RoleSwitcher::await() {
	awaitCh_.pop();
	if (!isTerminated()) {
		handleRoleSwitch();
	}
}

void RoleSwitcher::handleRoleSwitch() {
	auto rolesPair = sharedSyncState_.GetRolesPair();
	auto& newState = rolesPair.second;
	auto& curState = rolesPair.first;
	while (newState != curState) {
		logInfo("%d: onRoleSwitchAsync: %s(%d) -> %s(%d)", cfg_.serverId, RaftInfo::RoleToStr(curState.role), curState.leaderId,
				RaftInfo::RoleToStr(newState.role), newState.leaderId);

		// Set DB status
		ClusterizationStatus status;
		switch (newState.role) {
			case RaftInfo::Role::None:
			case RaftInfo::Role::Leader:
				status.role = ClusterizationStatus::Role::None;	 // -V1048
				break;
			case RaftInfo::Role::Follower:
				status.role = ClusterizationStatus::Role::ClusterReplica;
				break;
			case RaftInfo::Role::Candidate:
				status.role = (curState.role == RaftInfo::Role::Leader) ? ClusterizationStatus::Role::None
																		: ClusterizationStatus::Role::ClusterReplica;
				break;
		}
		status.leaderId = newState.leaderId;
		thisNode_.setClusterizationStatus(std::move(status), ctx_);

		if (cfg_.namespaces.empty()) {
			std::vector<std::string> namespaces;
			std::vector<NamespaceDef> nsDefs;
			auto err = thisNode_.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideTemporary().HideSystem(), RdxContext());
			if (!err.ok()) {
				logInfo("%d: onRoleSwitchAsync error: %s", cfg_.serverId, err.what());
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
			ClusterizationStatus status;
			switch (newState.role) {
				case RaftInfo::Role::None:
				case RaftInfo::Role::Leader:
					status.role = ClusterizationStatus::Role::None;	 // -V1048
					break;
				case RaftInfo::Role::Follower:
					status.role = ClusterizationStatus::Role::ClusterReplica;
					break;
				case RaftInfo::Role::Candidate:
					status.role = (curRole_ == RaftInfo::Role::Leader) ? ClusterizationStatus::Role::None
																	   : ClusterizationStatus::Role::ClusterReplica;
					break;
			}
			status.leaderId = newState.leaderId;
			logInfo("%d: Setting new role '%s' and leader id %d for '%s'", cfg_.serverId, RaftInfo::RoleToStr(newState.role),
					status.leaderId, nsName);
			nsPtr->SetClusterizationStatus(std::move(status), ctx_);
		}
	}
}

void RoleSwitcher::handleInitialSync(RaftInfo::Role newRole) {
	auto leaderSyncFn = [this](net::ev::timer& timer, int) {
		assert(leaderResyncWg_.wait_count() == 0);
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
					logWarn("%d: Leader's sync failed. Last error is: %s. New sync was scheduled in %d ms", cfg_.serverId, err.what(),
							std::chrono::duration_cast<std::chrono::milliseconds>(kLeaderNsResyncInterval).count());
				}
				return;
			}

			log_.Info([this] { rtfmt("%d: Leader's resync done", cfg_.serverId); });
			statsCollector_.OnSyncStateChanged(ReplicationStatCounter::kLeaderUID, NodeStats::SyncState::OnlineReplication);
			cfg_.onRoleSwitchDone();
		});
	};
	leaderResyncTimer_.set(leaderSyncFn);

	logInfo("%d: got new role: '%s'", cfg_.serverId, RaftInfo::RoleToStr(newRole));
	if (newRole == RaftInfo::Role::Leader) {
		log_.Info([this] { rtfmt("%d: Leader resync has begun", cfg_.serverId); });
		statsCollector_.OnSyncStateChanged(ReplicationStatCounter::kLeaderUID, NodeStats::SyncState::InitialLeaderSync);
		timerIsCanceled_ = false;
		roleSwitchTm_ = std::chrono::high_resolution_clock::now();
		leaderSyncFn(leaderResyncTimer_, 0);
	} else {
		if (curRole_ == RaftInfo::Role::Leader) {
			log_.Info([this] { rtfmt("%d: Leader resync timer was canceled", cfg_.serverId); });
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
	std::list<LeaderSyncQueue::Entry> syncQueue;

	NsNamesHashSetT nsList;
	if (cfg_.namespaces.size()) {
		nsList = cfg_.namespaces;
	} else {
		nsList = collectNsNames();
	}

	// coroutine::tokens_pool<bool> syncTokens(maxConcurrentSnapshots);
	std::vector<size_t> snapshotsOnNode(nodes_.size(), 0);
	for (auto& ns : nsList) {
		logInfo("%d: Leader's resync timer: create coroutine for '%s'", cfg_.serverId, ns);
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
	std::vector<std::string> dsns(nodes_.size());
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
		std::unique_lock lck(mtx_);
		if (terminate_) {
			throw Error(errCanceled, "Terminated");
		}
		syncer_ = std::make_unique<LeaderSyncer>(syncerCfg, log_);
		lck.unlock();
		logInfo("%d: Syncing namespaces from remote nodes", cfg_.serverId);
		lastErr = syncer_->Sync(std::move(syncQueue), sharedSyncState_, thisNode_, statsCollector_);
		logInfo("%d: Sync namespaces from remote nodes done: %s", cfg_.serverId, lastErr.what());
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
	statsCollector_.OnInitialSyncDone(
		std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - roleSwitchTm_));
}

Error RoleSwitcher::getNodesListForNs(std::string_view nsName, std::list<LeaderSyncQueue::Entry>& syncQueue) {
	// 1) Find most recent data among all the followers
	LeaderSyncQueue::Entry nsEntry;
	nsEntry.nsName = nsName;
	{
		ReplicationStateV2 state;
		auto err = thisNode_.GetReplState(nsName, state, RdxContext());
		if (err.ok()) {
			nsEntry.localLsn = nsEntry.latestLsn = ExtendedLsn(state.nsVersion, state.lastLsn);
			nsEntry.localDatahash = state.dataHash;
		}
		logInfo("%d: Begin leader's sync for '%s'. Ns version: %d, lsn: %d", cfg_.serverId, nsName, nsEntry.localLsn.NsVersion(),
				nsEntry.localLsn.LSN());
	}
	size_t responses = 1;
	coroutine::wait_group lwg;
	std::vector<std::pair<Error, ReplicationStateV2>> nodesStates(nodes_.size());
	for (int id = 0; size_t(id) < nodes_.size(); ++id) {
		loop_.spawn(lwg, [this, id, &nsName, &nsEntry, &responses, &nodesStates]() noexcept {
			auto err = nodes_[id].client.WithTimeout(kStatusCmdTimeout).GetReplState(nsName, nodesStates[id].second);
			const auto& state = nodesStates[id].second;
			if (err.ok()) {
				++responses;
				logInfo("%d: Got ns version %d and lsn %d from node %d for '%s'", cfg_.serverId, state.nsVersion, state.lastLsn, id,
						nsName);
				const ExtendedLsn remoteLsn(state.nsVersion, state.lastLsn);
				if (nsEntry.latestLsn.IsEmpty() ||
					(nsEntry.latestLsn.IsCompatibleByNsVersion(remoteLsn) && !remoteLsn.LSN().isEmpty() &&
					 nsEntry.latestLsn.LSN().Counter() < remoteLsn.LSN().Counter()) ||
					(!remoteLsn.NsVersion().isEmpty() && remoteLsn.NsVersion().Counter() > nsEntry.latestLsn.NsVersion().Counter())) {
					nsEntry.nodes.clear();
					nsEntry.nodes.emplace_back(id);
					nsEntry.dataHashes.clear();
					nsEntry.dataHashes.emplace_back(state.dataHash);
					nsEntry.latestLsn = remoteLsn;
				} else if (nsEntry.latestLsn == remoteLsn && ((nsEntry.dataHashes.size() && nsEntry.dataHashes[0] == state.dataHash) ||
															  nsEntry.localDatahash == state.dataHash)) {
					nsEntry.nodes.emplace_back(id);
					nsEntry.dataHashes.emplace_back(state.dataHash);
				}
			} else if (err.code() == errNotFound) {
				++responses;
			}
			nodesStates[id].first = std::move(err);
		});
	}
	logTrace("%d Awaiting namespace collecting...(%d)", cfg_.serverId, lwg.wait_count());
	lwg.wait();
	const auto consensusCnt = getConsensusCnt();
	if (responses < consensusCnt) {
		return Error(errClusterConsensus, "Unable to get enough responses. Got %d out of %d", responses, consensusCnt);
	}
	if (nsEntry.IsLocal()) {
		SyncTimeCounter timeCounter(SyncTimeCounter::Type::InitialWalSync, statsCollector_);
		logInfo("%d: Local namespace '%s' is up-to-date (ns version: %d, lsn: %d)", cfg_.serverId, nsName, nsEntry.localLsn.NsVersion(),
				nsEntry.localLsn.LSN());
		sharedSyncState_.MarkSynchronized(std::string(nsName));
	} else {
		logInfo("%d: '%s'. Local ns version: %d, local lsn: %d, latest ns version: %d, latest lsn: %d", cfg_.serverId, nsName,
				nsEntry.localLsn.NsVersion(), nsEntry.localLsn.LSN(), nsEntry.latestLsn.NsVersion(), nsEntry.latestLsn.LSN());
		syncQueue.emplace_back(std::move(nsEntry));
	}
	return Error();
}

RoleSwitcher::NsNamesHashSetT RoleSwitcher::collectNsNames() {
	NsNamesHashSetT set;
	coroutine::wait_group lwg;
	size_t responses = 1;
	for (size_t nodeId = 0; nodeId < nodes_.size(); ++nodeId) {
		loop_.spawn(lwg, [this, nodeId, &set, &responses]() noexcept {
			auto& node = nodes_[nodeId];
			auto client = node.client.WithTimeout(kStatusCmdTimeout);
			auto err = appendNsNamesFrom(client, set);
			if (err.ok()) {
				++responses;
			} else {
				logInfo("%d:%d Unable to recv namespaces on initial sync: %s", cfg_.serverId, nodeId, err.what());
			}
		});
	}
	lwg.wait();
	const auto consensusCnt = getConsensusCnt();
	auto err = appendNsNamesFrom(thisNode_, set);
	if (err.ok()) {
		logInfo("%d Recv total %d namespaces on initial sync", cfg_.serverId, set.size());
	} else {
		throw Error(errClusterConsensus, "Unable to recv local namespaces on initial sync: %s", err.what());
	}
	if (responses < consensusCnt) {
		throw Error(errClusterConsensus, "Unable to get enough responses. Got %d out of %d", responses, consensusCnt);
	}

	return set;
}

template <typename RxT>
Error RoleSwitcher::appendNsNamesFrom(RxT& rx, RoleSwitcher::NsNamesHashSetT& set) {
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
	client::ConnectOpts opts;
	opts.CreateDBIfMissing().WithExpectedClusterID(cfg_.clusterId);
	for (auto& node : nodes_) {
		node.client.Connect(node.dsn, loop_, opts);
	}
}

void RoleSwitcher::disconnectNodes() {
	coroutine::wait_group swg;
	for (auto& node : nodes_) {
		loop_.spawn(
			swg, [&node]() noexcept { node.client.Stop(); }, k16kCoroStack);
	}
	swg.wait();
}

}  // namespace cluster
}  // namespace reindexer
