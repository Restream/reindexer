#include "clusterizator.h"
#include <assert.h>
#include <unordered_set>
#include "core/reindexerimpl.h"
#include "tools/logger.h"

namespace reindexer {
namespace cluster {

Clusterizator::Clusterizator(ReindexerImpl& thisNode) : raftManager_(loop_), replicator_(thisNode, [this] { RequestElectionsRestart(); }) {}

Error Clusterizator::Start() {
	std::lock_guard<std::mutex> lck(mtx_);
	if (controlThread_.joinable() || replicatorThread_.joinable()) {
		assert(controlThread_.joinable() == replicatorThread_.joinable());
		return Error(errLogic, "Replicator is already started");
	}
	auto err = validateConfig();
	if (err.ok()) {
		controlThread_ = std::thread([this] { this->run(); });
		replicatorThread_ = std::thread([this] {
			// TODO: pass replication nodes here
			replicator_.Run(replConfig_.serverId, clusterConfig_);
		});
	}
	return err;
}

bool Clusterizator::Configure(ClusterConfigData clusterConfig, ReplicationConfigData replConfig) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (!enabled_.load(std::memory_order_acquire)) {
		return false;
	}

	if (clusterConfig_ != clusterConfig || replConfig_ != replConfig) {
		if (controlThread_.joinable() || replicatorThread_.joinable()) {
			stop();
		}
		clusterConfig_ = std::move(clusterConfig);
		replConfig_ = std::move(replConfig);
		auto err = validateConfig();
		if (!err.ok()) {
			logPrintf(LogInfo, "[cluster] Clusterizator was configured with invalid config: %s", err.what());
			return false;
		}
	}

	return !controlThread_.joinable() && !replicatorThread_.joinable();
}

void Clusterizator::Stop() noexcept {
	std::unique_lock<std::mutex> lck(mtx_);
	stop();
}

Error Clusterizator::SuggestLeader(const NodeData& suggestion, NodeData& response) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (!controlThread_.joinable()) {
		return Error(errNotValid, "Clusterizator is not running");
	}
	auto err = raftManager_.SuggestLeader(suggestion, response);
	if (err.ok()) {
		response.dsn = getManagementDsn(response.serverId);
	}
	return err;
}

Error Clusterizator::LeadersPing(const NodeData& leader) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (!controlThread_.joinable()) {
		return Error(errNotValid, "Clusterizator is not running");
	}
	return raftManager_.LeadersPing(leader);
}

Error Clusterizator::Replicate(UpdateRecord&& rec, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	return replicator_.Replicate(std::move(rec), std::move(beforeWaitF), ctx);
}

Error Clusterizator::Replicate(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	return replicator_.Replicate(std::move(recs), std::move(beforeWaitF), ctx);
}

void Clusterizator::run() {
	loop_.spawn([this] { clusterControlRoutine(); });
	loop_.run();
}

void Clusterizator::clusterControlRoutine() {
	logPrintf(LogInfo, "[cluster] Beginning control routine");
	raftManager_.Configure(replConfig_.serverId, clusterConfig_);

	RaftInfo raftInfo;
	while (!terminate_) {
		replicator_.OnRoleChanged(RaftInfo::Role::Candidate,
								  raftInfo.role == RaftInfo::Role::Leader ? replConfig_.serverId : raftManager_.GetLeaderId());
		if (restartElections_) {
			raftInfo.role = RaftInfo::Role::Candidate;
			restartElections_ = false;
		}
		RaftInfo newRaftInfo;
		newRaftInfo.role = raftManager_.Elections();
		if (terminate_) {
			break;
		}

		newRaftInfo.leaderId = raftManager_.GetLeaderId();

		if (newRaftInfo != raftInfo) {
			replicator_.OnRoleChanged(newRaftInfo.role,
									  newRaftInfo.role == RaftInfo::Role::Leader ? replConfig_.serverId : newRaftInfo.leaderId);
		}
		raftInfo = newRaftInfo;
		std::function<bool()> condPredicat;
		if (raftInfo.role == RaftInfo::Role::Leader) {
			logPrintf(LogInfo, "[cluster] Became leader");
			condPredicat = [this] { return raftManager_.FollowersAreAvailable(); };
		} else if (raftInfo.role == RaftInfo::Role::Follower) {
			logPrintf(LogInfo, "[cluster] Became follower");
			condPredicat = [this] { return raftManager_.LeaderIsAvailable(RaftManager::ClockT::now()); };
		} else {
			assert(false);
		}

		static_assert(kGranularSleepInterval < kMinStateCheckInerval, "Sleep interval has to be less or equal to check interval");
		do {
			auto checkInterval = kMinStateCheckInerval + std::chrono::milliseconds(std::rand() % kMaxStateCheckDiff.count());
			while (!terminate_ && checkInterval.count() > 0) {
				loop_.sleep(kGranularSleepInterval);
				checkInterval -= kGranularSleepInterval;

				if (restartElections_) {
					std::cerr << "Elections restart on request" << std::endl;
					break;
				}

				int curLeaderId = raftManager_.GetLeaderId();
				if (raftInfo.leaderId != curLeaderId && raftInfo.role == RaftInfo::Role::Follower) {
					std::cerr << "Leader was changed: " << raftInfo.leaderId << " -> " << curLeaderId << std::endl;
					raftInfo.leaderId = curLeaderId;
					replicator_.OnRoleChanged(RaftInfo::Role::Follower, raftInfo.leaderId);
				}
			}
			if (restartElections_) {
				break;
			}
		} while (!terminate_ && condPredicat());
	}
	raftManager_.AwaitTermination();
}

std::string Clusterizator::getManagementDsn(int id) const {
	for (auto& node : clusterConfig_.nodes) {
		if (node.serverId == id) {
			return node.GetManagementDsn();
		}
	}
	return std::string();
}

void Clusterizator::stop() noexcept {
	raftManager_.SetTerminateFlag(true);
	replicator_.SetTerminateFlag(true);
	terminate_ = true;
	if (controlThread_.joinable()) {
		controlThread_.join();
	}
	if (replicatorThread_.joinable()) {
		replicatorThread_.join();
	}
	terminate_ = false;
	replicator_.SetTerminateFlag(false);
	raftManager_.SetTerminateFlag(false);
}

Error Clusterizator::validateConfig() const noexcept {
	if (replConfig_.serverId < 0) {
		return Error(errParams, "Server ID is not set");
	}
	std::unordered_set<int> ids;
	bool serverIsInCluster =
		clusterConfig_.nodes.empty();  // In case if we're using setup with async replication only, there is no need in this check
	if (clusterConfig_.nodes.size() && clusterConfig_.nodes.size() < 3) {
		return Error(errParams, "Minimal cluster size is 3, but only %d nodes were in config", clusterConfig_.nodes.size());
	}
	for (auto& node : clusterConfig_.nodes) {
		if (ids.count(node.serverId)) {
			return Error(errParams, "Duplicated server id in cluster config: %d", node.serverId);
		} else {
			ids.emplace(node.serverId);
		}
		if (node.serverId == replConfig_.serverId) {
			serverIsInCluster = true;
		}
	}
	if (!serverIsInCluster) {
		return Error(errParams, "Server id %d is not in cluster", replConfig_.serverId);
	}
	return errOK;
}

}  // namespace cluster
}  // namespace reindexer
