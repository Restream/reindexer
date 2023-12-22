#include "raftmanager.h"
#include "cluster/logger.h"
#include "core/dbconfig.h"
#include "tools/randomgenerator.h"

namespace reindexer {
namespace cluster {

constexpr auto kRaftTimeout = std::chrono::seconds(2);

RaftManager::RaftManager(net::ev::dynamic_loop& loop, ReplicationStatsCollector statsCollector, const Logger& l,
						 std::function<void(uint32_t, bool)> onNodeNetworkStatusChangedCb)
	: loop_(loop), statsCollector_(statsCollector), onNodeNetworkStatusChangedCb_(std::move(onNodeNetworkStatusChangedCb)), log_(l) {
	assert(onNodeNetworkStatusChangedCb_);
}

void RaftManager::Configure(const ReplicationConfigData& baseConfig, const ClusterConfigData& config) {
	serverId_ = baseConfig.serverID;
	clusterID_ = baseConfig.clusterID;
	nodes_.clear();
	nodes_.reserve(config.nodes.size());

	client::ReindexerConfig rpcCfg;
	rpcCfg.AppName = config.appName;
	rpcCfg.NetTimeout = kRaftTimeout;
	rpcCfg.EnableCompression = false;
	rpcCfg.RequestDedicatedThread = true;
	size_t uid = 0;
	for (uint32_t i = 0; i < config.nodes.size(); ++i) {
		if (config.nodes[i].serverId != serverId_) {
			nodes_.emplace_back(rpcCfg, config.nodes[i].GetManagementDsn(), uid++, config.nodes[i].serverId);
		}
	}
}

Error RaftManager::SendDesiredLeaderId(int nextServerId) {
	logTrace("%d SendDesiredLeaderId nextLeaderId = %d", serverId_, nextServerId);
	size_t nextServerNodeIndex = nodes_.size();
	for (size_t i = 0; i < nodes_.size(); i++) {
		if (nodes_[i].serverId == nextServerId) {
			nextServerNodeIndex = i;
			break;
		}
	}

	std::shared_ptr<void> CloseConnection(nullptr, [this](void*) {
		if (GetLeaderId() != serverId_) {  // leader node clients, used for pinging
			coroutine::wait_group wgStop;
			for (auto& node : nodes_) {
				loop_.spawn(wgStop, [&node]() { node.client.Stop(); });
			}
			wgStop.wait();
		}
	});

	if (nextServerNodeIndex != nodes_.size()) {
		Error err = clientStatus(nextServerNodeIndex, kDesiredLeaderTimeout);
		if (!err.ok()) {
			return Error(err.code(), "Target node %s is not available.", nodes_[nextServerNodeIndex].dsn);
		}
	}

	uint32_t okCount = 1;
	coroutine::wait_group wg;
	std::string errString;

	for (size_t nodeId = 0; nodeId < nodes_.size(); ++nodeId) {
		if (nodeId == nextServerNodeIndex) {
			continue;
		}

		loop_.spawn(wg, [this, nodeId, nextServerId, &errString, &okCount] {
			try {
				const auto err = sendDesiredServerIdToNode(nodeId, nextServerId);
				if (err.ok()) {
					++okCount;
				} else {
					errString += "[" + err.what() + "]";
				}
			} catch (...) {
				logInfo("%d: Unable to send desired leader: got unknonw exception", serverId_);
			}
		});
	}
	wg.wait();
	if (nextServerNodeIndex != nodes_.size()) {
		Error err = sendDesiredServerIdToNode(nextServerNodeIndex, nextServerId);
		if (!err.ok()) {
			return err;
		}
		okCount++;
	}

	if (okCount >= GetConsensusForN(nodes_.size() + 1)) {
		return errOK;
	}

	return Error(errNetwork, "Can't send nextLeaderId to servers okCount %d err: %s", okCount, errString);
}

RaftInfo::Role RaftManager::Elections() {
	std::vector<coroutine::routine_t> succeedRoutines;
	succeedRoutines.reserve(nodes_.size());
	while (!terminate_.load()) {
		const int nextServerId = nextServerId_.GetNextServerId();
		int32_t term = beginElectionsTerm(nextServerId);
		logInfo("Starting new elections term for %d. Term number: %d", serverId_, term);
		if (nextServerId != -1 && nextServerId != serverId_) {
			endElections(term, RaftInfo::Role::Follower);
			logInfo("Skipping elections (desired leader id is %d)", serverId_, nextServerId);
			return RaftInfo::Role::Follower;
		}
		const bool isDesiredLeader = (nextServerId == serverId_);
		coroutine::wait_group wg;
		succeedRoutines.resize(0);
		struct {
			size_t succeedPhase1 = 1;
			size_t succeedPhase2 = 1;
			size_t failed = 0;
		} electionsStat;
		for (size_t nodeId = 0; nodeId < nodes_.size(); ++nodeId) {
			loop_.spawn(wg, [this, &electionsStat, nodeId, term, &succeedRoutines, isDesiredLeader] {
				auto& node = nodes_[nodeId];
				if (!node.client.Status().ok()) {
					node.client.Connect(node.dsn, loop_, client::ConnectOpts().WithExpectedClusterID(clusterID_));
				}
				NodeData suggestion, result;
				suggestion.serverId = serverId_;
				suggestion.electionsTerm = term;
				auto err = node.client.SuggestLeader(suggestion, result);
				bool succeed = err.ok() && serverId_ == result.serverId;
				if (succeed) {
					logInfo("%d: Suggested as leader for node %d", serverId_, nodeId);
					++electionsStat.succeedPhase1;
				} else {
					logInfo("%d: Error on leader suggest for node %d (response leader is %d): %s", serverId_, nodeId, result.serverId,
							err.what());
					++electionsStat.failed;
				}
				if (electionsStat.failed + electionsStat.succeedPhase1 == nodes_.size() + 1 ||
					electionsStat.succeedPhase1 > (nodes_.size() + 1) / 2) {
					std::vector<coroutine::routine_t> succeedRoutinesTmp;
					while (succeedRoutines.size()) {
						std::swap(succeedRoutinesTmp, succeedRoutines);
						for (auto routine : succeedRoutinesTmp) {
							coroutine::resume(routine);
						}
						succeedRoutinesTmp.clear();
					}
					if (!succeed) {
						return;
					}
				} else if (succeed) {
					succeedRoutines.emplace_back(coroutine::current());
					coroutine::suspend();
				} else {
					return;
				}

				const bool leaderIsAvailable = !isDesiredLeader && LeaderIsAvailable(ClockT::now());
				if (leaderIsAvailable || !isConsensus(electionsStat.succeedPhase1)) {
					logInfo(
						"%d: Skip leaders ping. Elections are outdated. leaderIsAvailable: %d. Successfull "
						"responses: %d",
						serverId_, leaderIsAvailable ? 1 : 0, electionsStat.succeedPhase1);
					return;	 // This elections are outdated
				}
				err = node.client.LeadersPing(suggestion);
				if (err.ok()) {
					++electionsStat.succeedPhase2;
				} else {
					logInfo("%d: leader's ping error: %s", serverId_, err.what());
				}
			});
		}

		RaftInfo::Role result = nodes_.size() ? RaftInfo::Role::Follower : RaftInfo::Role::Leader;

		while (wg.wait_count()) {
			wg.wait_next();
			if (isConsensus(electionsStat.succeedPhase2)) {
				result = RaftInfo::Role::Leader;
				if (endElections(term, result)) {
					logInfo("%d: end elections with role: leader", serverId_);
					wg.wait();

					return result;
				}
			}
		}
		logInfo("%d: votes stats: phase1: %d; phase2: %d; fails: %d", serverId_, electionsStat.succeedPhase1, electionsStat.succeedPhase2,
				electionsStat.failed);

		if (endElections(term, result)) {
			logInfo("[%d: end elections with role: %s(%d)", serverId_, RaftInfo::RoleToStr(result), GetLeaderId());
			return result;
		} else {
			logInfo("%d: Failed to end elections with chosen role: %s", serverId_, RaftInfo::RoleToStr(result));
		}
	}
	return RaftInfo::Role::Follower;
}

bool RaftManager::LeaderIsAvailable(ClockT::time_point now) {
	return ((now - lastLeaderPingTs_.load()) < kMinLeaderAwaitInterval) || (GetRole() == RaftInfo::Role::Leader);
}

bool RaftManager::FollowersAreAvailable() {
	size_t aliveNodes = 0;
	for (auto& n : nodes_) {
		n.isOk && ++aliveNodes;
	}
	logTrace("%d: Alive followers cnt: %d", serverId_, aliveNodes);
	return isConsensus(aliveNodes + 1);
}

Error RaftManager::SuggestLeader(const NodeData& suggestion, NodeData& response) {
	const auto now = ClockT::now();
	logTrace("%d Leader suggestion info. Local leaderId: %d; local term: %d; local time: %d; leader's ts: %d)", serverId_, GetLeaderId(),
			 GetTerm(), now.time_since_epoch().count(), lastLeaderPingTs_.load().time_since_epoch().count());
	while (true) {
		int64_t oldVoteData = voteData_.load();
		int64_t newVoteData;
		auto localTerm = getTerm(oldVoteData);
		auto localId = getLeaderId(oldVoteData);
		int nextServerId = nextServerId_.GetNextServerId();
		if (nextServerId != -1) {
			response.serverId = nextServerId;
		} else {
			response.serverId = localId;
		}
		response.electionsTerm = localTerm;
		if (suggestion.electionsTerm > localTerm) {
			logTrace("%d suggestion.electionsTerm > localTerm", serverId_);
			const bool leaderIsAvailable = LeaderIsAvailable(now);
			if (!leaderIsAvailable) {
				int sId = suggestion.serverId;
				if (nextServerId != -1) {
					sId = nextServerId;
				}

				newVoteData = setLeaderId(setTerm(oldVoteData, suggestion.electionsTerm), sId);
				if (voteData_.compare_exchange_strong(oldVoteData, newVoteData)) {
					response.serverId = sId;
					response.electionsTerm = suggestion.electionsTerm;
					break;
				}
			} else if (nextServerId != -1) {
				newVoteData = setLeaderId(setTerm(oldVoteData, suggestion.electionsTerm), nextServerId);
				if (voteData_.compare_exchange_strong(oldVoteData, newVoteData)) {
					response.serverId = nextServerId;
					response.electionsTerm = suggestion.electionsTerm;
					break;
				}
			} else {
				break;
			}
		} else {
			break;
		}
	}

	logTrace("%d Suggestion: servedId: %d; term: %d; Response: servedId: %d; term: %d; Local: servedId: %d; term: %d", serverId_,
			 suggestion.serverId, suggestion.electionsTerm, response.serverId, response.electionsTerm, GetLeaderId(), GetTerm());
	return Error();
}

Error RaftManager::LeadersPing(const NodeData& leader) {
	int64_t oldVoteData, newVoteData;
	oldVoteData = voteData_.load();
	auto now = ClockT::now();
	do {
		const auto nextServerId = nextServerId_.GetNextServerId();
		if (nextServerId >= 0 && nextServerId != leader.serverId) {
			return Error(errLogic, "This node has different desired leader: %d", nextServerId);
		}
		if (getRole(oldVoteData) == RaftInfo::Role::Leader) {
			return Error(errLogic, "This node is a leader itself");
		}
		if (getLeaderId(oldVoteData) != leader.serverId && LeaderIsAvailable(now)) {
			return Error(errLogic, "This node has another leader: %d", getLeaderId(oldVoteData));
		}
		newVoteData = setTerm(setLeaderId(oldVoteData, leader.serverId), leader.electionsTerm);
	} while (!voteData_.compare_exchange_strong(oldVoteData, newVoteData));
	now = ClockT::now();
	lastLeaderPingTs_.store(now);
	return Error();
}

void RaftManager::AwaitTermination() {
	assert(terminate_);
	coroutine::wait_group wg;
	pingWg_.wait();
	for (auto& node : nodes_) {
		loop_.spawn(wg, [&node]() { node.client.Stop(); });
	}
	wg.wait();
	SetTerminateFlag(false);
}

void RaftManager::startPingRoutines() {
	assert(pingWg_.wait_count() == 0);
	for (size_t nodeId = 0; nodeId < nodes_.size(); ++nodeId) {
		nodes_[nodeId].isOk = true;
		nodes_[nodeId].hasNetworkError = false;
		loop_.spawn(pingWg_, [this, nodeId]() noexcept {
			auto& node = nodes_[nodeId];
			node.client.Connect(node.dsn, loop_);
			auto voteData = voteData_.load();
			bool isFirstPing = true;
			while (!terminate_.load() && getRole(voteData) == RaftInfo::Role::Leader) {
				NodeData leader;
				leader.serverId = serverId_;
				leader.electionsTerm = getTerm(voteData);
				const auto err = node.client.LeadersPing(leader);
				const bool isNetworkError = (err.code() == errTimeout) || (err.code() == errNetwork);
				if (node.isOk != err.ok() || isNetworkError != node.hasNetworkError || isFirstPing) {
					node.isOk = err.ok();

					statsCollector_.OnStatusChanged(
						nodeId, node.isOk ? NodeStats::Status::Online
										  : (isNetworkError ? NodeStats::Status::Offline : NodeStats::Status::RaftError));
					if (isNetworkError != node.hasNetworkError) {
						logTrace("%d Network status was changed for %d(%d). Status: %d, network: %d", serverId_, node.uid, node.serverId,
								 node.isOk ? 1 : 0, isNetworkError ? 0 : 1);
						onNodeNetworkStatusChangedCb_(node.uid, !isNetworkError);
					}
					node.hasNetworkError = isNetworkError;
					isFirstPing = false;
				}
				loop_.sleep(kLeaderPingInterval);
				voteData = voteData_.load();
			}
			node.client.Stop();
		});
	}
}

void RaftManager::randomizedSleep(net::ev::dynamic_loop& loop, std::chrono::milliseconds base, std::chrono::milliseconds maxDiff) {
	const auto interval = base + std::chrono::milliseconds(tools::RandomGenerator::getu32(0, maxDiff.count()));
	loop.sleep(interval);
}

int32_t RaftManager::beginElectionsTerm(int presetLeader) {
	int64_t oldVoteData, newVoteData;
	int32_t term;
	oldVoteData = voteData_.load();
	RaftInfo::Role oldRole;
	do {
		oldRole = getRole(oldVoteData);
		term = getTerm(oldVoteData) + 1;
		newVoteData =
			setLeaderId(setRole(setTerm(oldVoteData, term), RaftInfo::Role::Candidate), presetLeader >= 0 ? presetLeader : serverId_);
	} while (!voteData_.compare_exchange_strong(oldVoteData, newVoteData));
	logTrace("%d: Role has been switched to candidate from %s", serverId_, RaftInfo::RoleToStr(oldRole));
	if (oldRole == RaftInfo::Role::Leader) {
		pingWg_.wait();
	}
	return term;
}

bool RaftManager::endElections(int32_t term, RaftInfo::Role result) {
	auto oldVoteData = voteData_.load();
	int64_t newVoteData;
	switch (result) {
		case RaftInfo::Role::Leader:
			newVoteData = setRole(oldVoteData, result);
			if (getTerm(oldVoteData) == term && voteData_.compare_exchange_strong(oldVoteData, newVoteData)) {
				startPingRoutines();
				return true;
			}
			return false;
		case RaftInfo::Role::Follower: {
			do {
				newVoteData = setRole(oldVoteData, result);
			} while (!voteData_.compare_exchange_strong(oldVoteData, newVoteData));
			coroutine::wait_group wg;
			for (auto& node : nodes_) {
				loop_.spawn(wg, [&node]() { node.client.Stop(); });
			}
			wg.wait();
			randomizedSleep(loop_, kMinLeaderAwaitInterval, kMaxLeaderAwaitDiff);
			return LeaderIsAvailable(RaftManager::ClockT::now());
		}
		case RaftInfo::Role::None:
		case RaftInfo::Role::Candidate:
			assert(false);
			// This should not happen
	}
	return false;
}

bool RaftManager::isConsensus(size_t num) const noexcept { return num >= GetConsensusForN(nodes_.size() + 1); }

Error RaftManager::clientStatus(size_t index, std::chrono::seconds timeout) {
	Error err;
	if (!nodes_[index].client.WithTimeout(timeout).Status(true).ok()) {
		err = nodes_[index].client.Connect(nodes_[index].dsn, loop_, client::ConnectOpts().WithExpectedClusterID(clusterID_));
		if (err.ok()) {
			err = nodes_[index].client.WithTimeout(timeout).Status(true);
		}
	}
	return err;
}

Error RaftManager::sendDesiredServerIdToNode(size_t index, int nextServerId) {
	Error err = clientStatus(index, kDesiredLeaderTimeout);
	if (!err.ok()) {
		return err;
	}
	logTrace("%d Sending desired server ID (%d) to node with server ID %d", serverId_, nextServerId, nodes_[index].serverId);
	return nodes_[index].client.WithTimeout(kDesiredLeaderTimeout).SetDesiredLeaderId(nextServerId);
}

}  // namespace cluster
}  // namespace reindexer
