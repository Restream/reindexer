#include "clusterdatareplicator.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "core/system_ns_names.h"
#include "tools/randomgenerator.h"

namespace reindexer {
namespace cluster {

ClusterDataReplicator::ClusterDataReplicator(ClusterDataReplicator::UpdatesQueueT& q, SharedSyncState& s, ReindexerImpl& thisNode)
	: statsCollector_(std::string(kClusterReplStatsType)),
	  raftManager_(loop_, statsCollector_, log_,
				   [this](uint32_t uid, bool online) {
					   UpdatesContainer recs;
					   recs.emplace_back(updates::URType::NodeNetworkCheck, uid, online);
					   std::pair<Error, bool> res = updatesQueue_.PushNowait(std::move(recs));
					   if (!res.first.ok()) {
						   logWarn("Error while PushNowait: {}", res.first.what());
					   }
				   }),
	  updatesQueue_(q),
	  sharedSyncState_(s),
	  thisNode_(thisNode),
	  roleSwitcher_(sharedSyncState_, syncList_, thisNode, statsCollector_, log_) {}

void ClusterDataReplicator::Configure(ClusterConfigData config) {
	lock_guard lck(mtx_);
	if ((config_.has_value() && config_.value() != config) || !config_.has_value()) {
		stop();
		config_ = std::move(config);
		if (baseConfig_.has_value()) {
			raftManager_.Configure(baseConfig_.value(), config_.value());
		}
	}
}

void ClusterDataReplicator::Configure(ReplicationConfigData config) {
	lock_guard lck(mtx_);
	if ((baseConfig_.has_value() && baseConfig_.value() != config) || !baseConfig_.has_value()) {
		stop();
		baseConfig_ = std::move(config);
		if (config_.has_value()) {
			raftManager_.Configure(baseConfig_.value(), config_.value());
		}
	}
}

bool ClusterDataReplicator::IsExpectingStartup() const noexcept {
	lock_guard lck(mtx_);
	return isExpectingStartup();
}

void ClusterDataReplicator::Run() {
	lock_guard lck(mtx_);
	if (!isExpectingStartup()) {
		log_.Warn([] { rtstr("ClusterDataReplicator: startup is not expected"); });
		return;
	}

	std::unordered_set<int> ids;
	// NOLINTBEGIN (bugprone-unchecked-optional-access) Optionals were checked in isExpectingStartup()
	bool serverIsInCluster = config_->nodes.empty();
	if (config_->nodes.size() && config_->nodes.size() < 3) {
		throw Error(errParams, "Minimal cluster size is 3, but only {} nodes were in config", config_->nodes.size());
	}
	if (config_->nodes.size() > UpdatesQueueT::kMaxReplicas) {
		throw Error(errParams, "Sync cluster nodes limit was reached: {}", UpdatesQueueT::kMaxReplicas);
	}
	for (auto& node : config_->nodes) {
		if (ids.count(node.serverId)) {
			throw Error(errParams, "Duplicated server id in cluster config: {}", node.serverId);
		} else {
			ids.emplace(node.serverId);
		}
		if (node.serverId == baseConfig_->serverID) {
			serverIsInCluster = true;
		}
	}
	if (!serverIsInCluster) {
		throw Error(errParams, "Server id {} is not in cluster", baseConfig_->serverID);
	}

	log_.SetLevel(config_->logLevel);
	auto nodes = config_->nodes;
	ClusterNodeConfig thisNode;
	for (auto it = nodes.begin(); it != nodes.end();) {
		if (it->serverId == serverID()) {
			thisNode = std::move(*it);
			it = nodes.erase(it);
			break;
		} else {
			++it;
		}
	}

	{
		std::vector<std::string> nss;
		nss.reserve(config_->namespaces.size());
		for (auto& ns : config_->namespaces) {
			nss.emplace_back(ns);
		}
		statsCollector_.Init(thisNode, nodes, nss);
	}
	for (size_t nodeId = 0; nodeId < nodes.size(); ++nodeId) {
		statsCollector_.OnServerIdChanged(nodeId, nodes[nodeId].serverId);
	}
	syncList_.Init(std::vector<int64_t>(nodes.size(), SynchronizationList::kUnsynchronizedID));

	updatesQueue_.ReinitSyncQueue(statsCollector_, std::optional<NsNamesHashSetT>(config_->namespaces), log_);
	const bool isSyncCluster = config_->nodes.size() > 1;
	sharedSyncState_.Reset(config_->namespaces, 1, isSyncCluster);
	if (isSyncCluster) {
		onRoleChanged(RaftInfo::Role::Candidate, -1);
	}

	assert(replThreads_.empty());
	ReplThreadConfig threadsConfig(baseConfig_.value(), config_.value());
	for (size_t i = 0; i < threadsCount(); ++i) {
		std::vector<std::pair<uint32_t, ClusterNodeConfig>> nodesShard;
		nodesShard.reserve(nodes.size() / threadsCount() + 1);
		for (size_t j = i; j < nodes.size(); j += threadsCount()) {
			nodesShard.emplace_back(std::make_pair(j, nodes[j]));
		}
		if (nodesShard.size()) {
			replThreads_.emplace_back(
				baseConfig_->serverID, thisNode_, &config_->namespaces, updatesQueue_.GetSyncQueue(), sharedSyncState_, syncList_,
				[this]() noexcept { restartElections_ = true; }, statsCollector_, log_);
			replThreads_.back().Run(threadsConfig, std::move(nodesShard), config_->nodes.size());
		}
	}

	raftThread_ = std::thread(
		[this](int serverId) {
			loop_.spawn([this, serverId]() noexcept {
				do {
					try {
						clusterControlRoutine(serverId);
					} catch (std::exception& e) {
						logError("{}: Unexpected exception in RAFT thread: '{}'. Trying to restart routine...", serverId, e.what());
					}
				} while (!terminate_);
				raftManager_.AwaitTermination();
			});
			loop_.run();
		},
		baseConfig_->serverID);

	roleSwitchThread_ = std::thread(
		[this, netTimeout = std::chrono::seconds(config_->syncTimeoutSec), maxWALDepthOnForceSync = config_->maxWALDepthOnForceSync,
		 maxConcurrentSnapshotsPerNode = config_->leaderSyncConcurrentSnapshotsPerNode, syncThreads = config_->leaderSyncThreads](
			ReplicationConfigData baseConfig, NsNamesHashSetT namespaces, std::vector<ClusterNodeConfig>&& nodes) mutable noexcept {
			RoleSwitcher::Config cfg;
			cfg.serverId = baseConfig.serverID;
			cfg.clusterId = baseConfig.clusterID;
			cfg.namespaces = std::move(namespaces);
			cfg.netTimeout = netTimeout;
			cfg.maxWALDepthOnForceSync = maxWALDepthOnForceSync;
			cfg.syncThreads = syncThreads;
			cfg.maxConcurrentSnapshotsPerNode = maxConcurrentSnapshotsPerNode;
			cfg.onRoleSwitchDone = [this] {
				for (auto& th : replThreads_) {
					th.OnRoleSwitch();
				}
			};
			std::vector<DSN> dsns;
			dsns.reserve(nodes.size());
			for (auto& node : nodes) {
				dsns.emplace_back(node.GetRPCDsn());
			}

			roleSwitcher_.Run(std::move(dsns), std::move(cfg));
		},
		baseConfig_.value(), config_->namespaces, std::move(nodes));
	// NOLINTEND (bugprone-unchecked-optional-access)
}

void ClusterDataReplicator::Stop(bool resetConfig) {
	lock_guard lck(mtx_);
	stop();
	if (resetConfig) {
		config_.reset();
	}
}

void ClusterDataReplicator::SuggestLeader(const NodeData& suggestion, NodeData& response) {
	lock_guard lck(mtx_);
	if (!isRunning()) {
		throw Error(errNotValid, "Cluster replicator is not running");
	}
	raftManager_.SuggestLeader(suggestion, response);
	response.dsn = getManagementDsn(response.serverId);
}

void ClusterDataReplicator::SetDesiredLeaderId(int nextLeaderId, bool sendToOtherNodes) {
	unique_lock lck(mtx_);
	if (!isRunning()) {
		throw Error(errNotValid, "Cluster replicator is not running");
	}
	logInfo("{}: Setting desired leader ID: {}. Sending to other nodes: {}", serverID(), nextLeaderId, sendToOtherNodes ? "true" : "false");
	std::promise<Error> promise;
	std::future<Error> future = promise.get_future();
	ClusterCommand c = ClusterCommand(ClusterCommand::SetDesiredLeaderT{}, nextLeaderId, sendToOtherNodes, std::move(promise));
	commands_.AddCommand(std::move(c));
	lck.unlock();
	auto err = future.get();
	if (!err.ok()) {
		throw err;
	}
}

void ClusterDataReplicator::ForceElections() {
	unique_lock lck(mtx_);
	if (!isRunning()) {
		throw Error(errNotValid, "Cluster replicator is not running");
	}
	logInfo("{}: Forcing new elections by request...", serverID());
	std::promise<Error> promise;
	std::future<Error> future = promise.get_future();
	ClusterCommand c = ClusterCommand(ClusterCommand::ForceElectionsT{}, std::move(promise));
	commands_.AddCommand(std::move(c));
	lck.unlock();
	auto err = future.get();
	if (!err.ok()) {
		throw err;
	}
}

void ClusterDataReplicator::LeadersPing(const NodeData& leader) {
	lock_guard lck(mtx_);
	if (!isRunning()) {
		throw Error(errNotValid, "Cluster replicator is not running");
	}
	raftManager_.LeadersPing(leader);
}

RaftInfo ClusterDataReplicator::GetRaftInfo(bool allowTransitState, const RdxContext& ctx) const {
	return sharedSyncState_.AwaitRole(allowTransitState, ctx);
}

bool ClusterDataReplicator::NamespaceIsInClusterConfig(std::string_view nsName) const {
	if (nsName == kReplicationStatsNamespace) {
		return true;
	}
	if (isSystemNamespaceNameFastReplication(nsName)) {
		return false;
	}

	lock_guard lck(mtx_);
	if (!config_.has_value()) {
		return false;
	}
	const auto& config = config_.value();
	return config.namespaces.empty() || (config.namespaces.find(nsName) != config.namespaces.end());
}

ReplicationStats ClusterDataReplicator::GetReplicationStats() const {
	auto stats = statsCollector_.Get();
	stats.logLevel = log_.GetLevel();
	auto raftInfo = sharedSyncState_.CurrentRole();
	const bool isInitialSyncDone = sharedSyncState_.IsInitialSyncDone();
	auto syncList = syncList_.GetSynchronized(GetConsensusForN(stats.nodeStats.size()) - 1);
	const bool currentNodeIsLeader = (raftInfo.role == RaftInfo::Role::Leader);
	const int leaderId = (raftInfo.role == RaftInfo::Role::Leader || raftInfo.role == RaftInfo::Role::Follower) ? raftInfo.leaderId : -1;
	for (uint32_t nodeId = 0; nodeId < stats.nodeStats.size(); ++nodeId) {
		auto& node = stats.nodeStats[nodeId];
		if (node.serverId == leaderId) {
			node.role = RaftInfo::Role::Leader;
			node.isSynchronized = currentNodeIsLeader && isInitialSyncDone;
		} else if (node.status == NodeStats::Status::Online) {
			node.role = RaftInfo::Role::Follower;
			node.isSynchronized = currentNodeIsLeader && isInitialSyncDone && (nodeId < syncList.size()) &&
								  (syncList[nodeId] != SynchronizationList::kUnsynchronizedID);
		} else {
			node.role = RaftInfo::Role::None;
			node.isSynchronized = false;
		}
	}

	return stats;
}

bool ClusterDataReplicator::isExpectingStartup() const noexcept {
	return config_.has_value() && baseConfig_.has_value() && config_->nodes.size() && baseConfig_->serverID >= 0 && !isRunning();
}

void ClusterDataReplicator::clusterControlRoutine(int serverId) {
	logInfo("{}: Beginning control routine", serverId);

	RaftInfo raftInfo;
	while (!terminate_) {
		onRoleChanged(RaftInfo::Role::Candidate, raftInfo.role == RaftInfo::Role::Leader ? serverId : raftInfo.leaderId);
		raftInfo.role = RaftInfo::Role::Candidate;
		restartElections_ = false;

		RaftInfo newRaftInfo;
		const auto newRoleOpt = raftManager_.RunElectionsRound();
		if (terminate_) {
			break;
		}

		if (!newRoleOpt.has_value()) {
			handleClusterCommands(serverId, raftInfo);
			continue;
		}

		newRaftInfo.role = newRoleOpt.value();
		const int desiredLeaderId = raftManager_.GetDesiredLeaderId();
		if (desiredLeaderId == serverId && newRaftInfo.role != RaftInfo::Role::Leader) {
			continue;
		}

		newRaftInfo.leaderId = raftManager_.GetLeaderId();

		if (newRaftInfo != raftInfo) {
			onRoleChanged(newRaftInfo.role, newRaftInfo.role == RaftInfo::Role::Leader ? serverId : newRaftInfo.leaderId);
		}
		raftInfo = newRaftInfo;
		std::function<bool()> condPredicate;
		if (raftInfo.role == RaftInfo::Role::Leader) {
			logInfo("{}: Became leader", serverId);
			condPredicate = [this]() { return raftManager_.FollowersAreAvailable(); };
		} else if (raftInfo.role == RaftInfo::Role::Follower) {
			logInfo("{}: Became follower ({})", serverId, raftInfo.leaderId);
			condPredicate = [this]() noexcept { return raftManager_.LeaderIsAvailable(RaftManager::ClockT::now()); };
		} else {
			assertrx(false);
			std::abort();
		}

		static_assert(kGranularSleepInterval < kMinStateCheckInerval, "Sleep interval has to be less or equal to check interval");
		assertrx_dbg(condPredicate);
		do {
			auto checkInterval =
				kMinStateCheckInerval + std::chrono::milliseconds(tools::RandomGenerator::getu32(0, kMaxStateCheckDiff.count()));
			while (!terminate_ && checkInterval.count() > 0) {
				loop_.sleep(kGranularSleepInterval);
				checkInterval -= kGranularSleepInterval;

				handleClusterCommands(serverId, raftInfo);
				if (restartElections_) {
					logWarn("{}: Elections restart on request", serverId);
					break;
				}

				int curLeaderId = raftManager_.GetLeaderId();
				if (raftInfo.leaderId != curLeaderId && raftInfo.role == RaftInfo::Role::Follower) {
					logWarn("{}: Leader was changed: {} -> {}", serverId, raftInfo.leaderId, curLeaderId);
					raftInfo.leaderId = curLeaderId;
					onRoleChanged(RaftInfo::Role::Follower, raftInfo.leaderId);
				}
			}
		} while (!terminate_ && !restartElections_ && condPredicate());
	}
}

void ClusterDataReplicator::handleClusterCommands(int serverId, const RaftInfo& curRaftInfo) {
	ClusterCommand c;
	while (commands_.GetCommand(c)) {
		Error err;
		switch (c.id) {
			case kCmdSetDesiredLeader: {
				if (c.send) {
					err = raftManager_.SendDesiredLeaderId(c.serverId);
				}
				if (err.ok()) {
					try {
						raftManager_.SetDesiredLeaderId(c.serverId);
						restartElections_ = true;
						onRoleChanged(RaftInfo::Role::Candidate,
									  curRaftInfo.role == RaftInfo::Role::Leader ? serverId : raftManager_.GetLeaderId());
					} catch (std::exception& e) {
						err = std::move(e);
					}
				}
				if (!err.ok()) {
					logError("{}: Error send desired leader: '{}'", serverId, err.what());
				}

			} break;
			case kCmdForceElections: {
				Error err;
				try {
					// Restart election for followers only. Do not call this for leader to avoid working cluster break
					if (curRaftInfo.role == RaftInfo::Role::Follower) {
						restartElections_ = true;
						onRoleChanged(RaftInfo::Role::Candidate,
									  curRaftInfo.role == RaftInfo::Role::Leader ? serverId : raftManager_.GetLeaderId());
					}
				} catch (std::exception& e) {
					err = std::move(e);
				}
				if (!err.ok()) {
					logError("{}: Error on election restart attempt: '{}'", serverId, err.what());
				}
			} break;
			case kNoCommand:
			default:
				err = Error(errParams, "Unknown cluster command id: {}", int(c.id));
				break;
		}
		c.result.set_value(std::move(err));
	}
}

DSN ClusterDataReplicator::getManagementDsn(int id) const {
	if (config_.has_value()) {
		for (auto& node : config_.value().nodes) {
			if (node.serverId == id) {
				return node.GetManagementDsn();
			}
		}
	}
	return {};
}

void ClusterDataReplicator::onRoleChanged(RaftInfo::Role to, int leaderId) {
	RaftInfo info;
	info.role = to;
	info.leaderId = leaderId;
	if (!terminate_) {
		if (to == RaftInfo::Role::Leader) {
			updatesQueue_.GetSyncQueue()->SetWritable(true, Error());
		} else {
			updatesQueue_.GetSyncQueue()->SetWritable(false,
													  Error(errUpdateReplication, "Role was switched to {}", RaftInfo::RoleToStr(to)));
		}
	}
	sharedSyncState_.SetRole(info);
	roleSwitcher_.OnRoleChanged();
}

void ClusterDataReplicator::stop() {
	if (isRunning()) {
		raftManager_.SetTerminateFlag(true);
		terminate_ = true;
		roleSwitcher_.SetTerminationFlag(true);
		for (auto& th : replThreads_) {
			th.SendTerminate();
		}
		for (auto& th : replThreads_) {
			th.AwaitTermination();
		}
		raftThread_.join();
		roleSwitchThread_.join();
		replThreads_.clear();
		updatesQueue_.GetSyncQueue()->SetWritable(false, Error(errUpdateReplication, "Cluster is not running"));
		updatesQueue_.ReinitSyncQueue(statsCollector_, std::optional<NsNamesHashSetT>(), log_);
		sharedSyncState_.SetTerminated();
		syncList_.Clear();
		statsCollector_.Clear();

		terminate_ = false;
		raftManager_.SetTerminateFlag(false);
		roleSwitcher_.SetTerminationFlag(false);
	}
}

}  // namespace cluster
}  // namespace reindexer
