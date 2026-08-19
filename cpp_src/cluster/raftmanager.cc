#include "raftmanager.h"
#include "cluster/logger.h"
#include "core/dbconfig.h"
#include "tools/randomgenerator.h"

namespace reindexer {
namespace cluster {

constexpr auto kRaftTimeout = std::chrono::seconds(2);

RaftManager::RaftManager(net::ev::dynamic_loop& loop, const ReplicationStatsCollector& statsCollector, const Logger& l,
						 std::function<void(uint32_t, bool)> onNodeNetworkStatusChangedCb)
	: loop_(loop),
	  statsCollector_(statsCollector),
	  onNodeNetworkStatusChangedCb_(std::move(onNodeNetworkStatusChangedCb)),
	  log_(l),
	  voting_(l) {
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

// NOLINTNEXTLINE(bugprone-exception-escape) TODO: noexcept logger fallback
std::optional<RaftInfo::Role> RaftManager::RunElectionsRound() noexcept {
	coroutine::wait_group wg;
	std::optional<RaftInfo::Role> roundResult;
	struct {
		size_t succeedPhase1 = 1;
		size_t succeedPhase2 = 1;
		size_t failed = 0;
	} electionsStat;
	std::vector<coroutine::routine_t> succeedRoutines;

	const auto roundBeg = ClockT::now();
	try {
		succeedRoutines.reserve(nodes_.size());

		const int nextLeaderId = voting_.GetDesiredLeaderId();
		const bool isDesiredLeader = (nextLeaderId == serverId_);
		if (!isDesiredLeader && nextLeaderId != -1) {
			logInfo("{}: Skipping elections (desired leader id is {})", serverId_, nextLeaderId);
			if (endElections(-1, roundBeg, RaftInfo::Role::Follower)) {
				roundResult = RaftInfo::Role::Follower;
			} else {
				logInfo("{}: Failed to end elections with chosen role: follower (desired leader id is {})", serverId_, nextLeaderId);
			}
		} else {
			int32_t term = beginElectionsTerm(nextLeaderId);
			logInfo("{}: Starting new elections term. Term number: {}", serverId_, term);
			for (size_t nodeId = 0; nodeId < nodes_.size(); ++nodeId) {
				// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
				loop_.spawn(wg, [this, &electionsStat, nodeId, term, &succeedRoutines, isDesiredLeader] {
					auto& node = nodes_[nodeId];
					if (!node.client.Status().ok()) {
						auto err = node.client.Connect(node.dsn, loop_, createConnectionOpts());
						(void)err;	// Error will be handled during the further requests
					}
					NodeData suggestion, result;
					suggestion.serverId = serverId_;
					suggestion.electionsTerm = term;
					auto err = node.client.SuggestLeader(suggestion, result);
					bool succeed = err.ok() && serverId_ == result.serverId;
					if (succeed) {
						logInfo("{}: Suggested as leader for node {}", serverId_, nodeId);
						++electionsStat.succeedPhase1;
					} else {
						logInfo("{}: Error on leader suggest for node {} (response leader is {}): {}", serverId_, nodeId, result.serverId,
								err.what());
						++electionsStat.failed;
					}
					if (electionsStat.failed + electionsStat.succeedPhase1 == nodes_.size() + 1 ||
						electionsStat.succeedPhase1 > (nodes_.size() + 1) / 2) {
						std::vector<coroutine::routine_t> succeedRoutinesTmp;
						while (succeedRoutines.size()) {
							std::swap(succeedRoutinesTmp, succeedRoutines);
							for (auto routine : succeedRoutinesTmp) {
								std::ignore = coroutine::resume(routine);
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

					const auto voteData = voting_.GetVoteData();
					const bool leaderIsAvailable = !isDesiredLeader && LeaderIsAvailable(ClockT::now());
					const bool voteMoved = (voteData.leaderId != serverId_) || (voteData.term != term);
					if (leaderIsAvailable || voteMoved || !isConsensus(electionsStat.succeedPhase1)) {
						logInfo(
							"{}: Skip leaders ping. Elections are outdated. leaderIsAvailable: {}. voteMoved: {}. Successful requests: {}",
							serverId_, leaderIsAvailable ? 1 : 0, voteMoved ? 1 : 0, electionsStat.succeedPhase1);
						return;	 // These elections are outdated
					}
					suggestion.leaderCommitState = LeaderCommitState::Election;
					err = node.client.LeadersPing(suggestion);
					if (err.ok()) {
						++electionsStat.succeedPhase2;
					} else {
						logInfo("{}: leader's ping error: {}", serverId_, err.what());
					}
				});
			}

			RaftInfo::Role result = nodes_.empty() ? RaftInfo::Role::Leader : RaftInfo::Role::Follower;
			bool leaderCommitAttempted = false;

			while (wg.wait_count()) {
				wg.wait_next();
				if (isConsensus(electionsStat.succeedPhase2)) {
					result = RaftInfo::Role::Leader;
					leaderCommitAttempted = true;
					if (endElections(term, roundBeg, result)) {
						logInfo("{}: end elections with role: leader", serverId_);
						roundResult = result;
					} else {
						logInfo("{}: Failed to end elections with chosen role: leader", serverId_);
					}
					break;
				}
			}
			if (!roundResult) {
				logInfo("{}: votes stats: phase1: {}; phase2: {}; fails: {}", serverId_, electionsStat.succeedPhase1,
						electionsStat.succeedPhase2, electionsStat.failed);

				if (result == RaftInfo::Role::Leader && !leaderCommitAttempted) {
					if (endElections(term, roundBeg, result)) {
						logInfo("{}: end elections with role: leader", serverId_);
						roundResult = result;
					} else {
						logInfo("{}: Failed to end elections with chosen role: leader", serverId_);
					}
				} else if (result == RaftInfo::Role::Follower) {
					if (endElections(term, roundBeg, result)) {
						logInfo("{}: end elections with role: {}({})", serverId_, RaftInfo::RoleToStr(result), GetLeaderId());
						roundResult = result;
					} else {
						logInfo("{}: Failed to end elections with chosen role: {}", serverId_, RaftInfo::RoleToStr(result));
					}
				}
			}
		}
	} catch (const std::exception& e) {
		logError("{}: exception during the elections: {}", serverId_, e.what());
	}
	wg.wait();
	return roundResult;
}

bool RaftManager::FollowersAreAvailable() const noexcept {
	size_t aliveNodes = 0;
	for (auto& n : nodes_) {
		n.isOk && ++aliveNodes;
	}
	logTrace("{}: Alive followers cnt: {}", serverId_, aliveNodes);
	return isConsensus(aliveNodes + 1);
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

LeaderCommitState RaftManager::VotingManager::commitStateAfterAccept(int prevId, LeaderCommitState prevState, int newId,
																	 LeaderCommitState incoming) noexcept {
	if (!GrantsCommittedAvailability(incoming) && GrantsCommittedAvailability(prevState) && prevId == newId) {
		return prevState;
	}
	return incoming;
}

void RaftManager::VotingManager::LeadersPing(const NodeData& leader, int thisServerId) {
	lock_guard lck(mtx_);

	const auto nextLeaderId = nextLeaderId_.GetNextLeaderId();
	if (nextLeaderId >= 0 && nextLeaderId != leader.serverId) {
		throw Error(errLogic, "This node has different desired leader: {}", nextLeaderId);
	}
	if (data_.role == RaftInfo::Role::Leader) {
		throw Error(errLogic, "This node is a leader itself");
	}

	const auto now = ClockT::now();
	const bool remoteStrong = GrantsCommittedAvailability(leader.leaderCommitState);
	const bool localStrong = leaderIsCommittedAvailable(now);
	const bool localAvailable = leaderIsAvailable(now);
	const bool otherId = (data_.leaderId != leader.serverId) && (data_.leaderId >= 0);

	if (!remoteStrong && leader.electionsTerm < data_.term) {
		throw Error(errLogic, "Stale election ping: term {} < local {}", leader.electionsTerm, data_.term);
	}
	if (otherId) {
		const bool blockedByLiveLease = localAvailable && (!remoteStrong || localStrong);
		const bool blockedByForeignVote = !remoteStrong && data_.leaderId != thisServerId;
		if (blockedByLiveLease || blockedByForeignVote) {
			throw Error(errLogic, "This node has another leader: {}", data_.leaderId);
		}
	}

	const auto prevId = data_.leaderId;
	const auto prevState = data_.lastCommitState;
	data_.leaderId = leader.serverId;
	data_.term = leader.electionsTerm;
	data_.lastLeaderPingTs = now;
	data_.lastCommitState = commitStateAfterAccept(prevId, prevState, leader.serverId, leader.leaderCommitState);
}

void RaftManager::VotingManager::bindLeaderSuggestion(int adoptedId, int32_t suggestionTerm) noexcept {
	const auto prevId = data_.leaderId;
	const bool retainLiveLease = leaderIsAvailable(ClockT::now()) && prevId == adoptedId;

	data_.term = suggestionTerm;
	data_.leaderId = adoptedId;
	if (!retainLiveLease) {
		data_.lastLeaderPingTs = ClockT::time_point{};
		data_.lastCommitState = LeaderCommitState::Election;
	}
}

void RaftManager::VotingManager::SuggestLeader(int thisServerId, const NodeData& suggestion, NodeData& response) {
	lock_guard lck(mtx_);
	auto voteData = data_;
	logTrace("{} Leader suggestion info. Local leaderId: {}; local term: {}; local time: {}; leader's ts: {})", thisServerId,
			 voteData.leaderId, voteData.term, ClockT::now().time_since_epoch().count(),
			 voteData.lastLeaderPingTs.time_since_epoch().count());
	const int nextLeaderId = nextLeaderId_.GetNextLeaderId();
	if (suggestion.electionsTerm > data_.term) {
		logTrace("{} suggestion.electionsTerm > localTerm", thisServerId);

		if (!leaderIsAvailable(ClockT::now())) {
			int sId = suggestion.serverId;
			if (nextLeaderId != -1) {
				sId = nextLeaderId;
			}
			bindLeaderSuggestion(sId, suggestion.electionsTerm);
			response.serverId = sId;
			response.electionsTerm = suggestion.electionsTerm;
		} else if (nextLeaderId != -1) {
			bindLeaderSuggestion(nextLeaderId, suggestion.electionsTerm);
		}
	}
	if (nextLeaderId != -1) {
		response.serverId = nextLeaderId;
	} else {
		response.serverId = data_.leaderId;
	}
	response.electionsTerm = data_.term;

	voteData = data_;
	logTrace("{} Suggestion: servedId: {}; term: {}; Response: servedId: {}; term: {}; Local: servedId: {}; term: {}", thisServerId,
			 suggestion.serverId, suggestion.electionsTerm, response.serverId, response.electionsTerm, voteData.leaderId, voteData.term);
}

bool RaftManager::SetDesiredLeaderId(int desiredLeaderId) { return voting_.SetDesiredLeaderId(serverId_, desiredLeaderId); }

bool RaftManager::VotingManager::SetDesiredLeaderId(int thisServerId, int desiredLeaderId) {
	lock_guard lck(mtx_);
	logInfo("{}: Set ({}) as a desired leader", thisServerId, desiredLeaderId);
	const bool demoteLeader = (data_.role == RaftInfo::Role::Leader) && (desiredLeaderId != thisServerId);
	nextLeaderId_.SetNextLeaderId(desiredLeaderId);
	data_.lastLeaderPingTs = ClockT::time_point{};
	data_.lastCommitState = LeaderCommitState::Unspecified;
	if (demoteLeader) {
		// Drop VoteData Leader only; shared role stays Candidate until replicator onRoleChanged.
		data_.role = RaftInfo::Role::Candidate;
		logInfo("{}: Demoted VoteData Leader -> Candidate for desired leader transfer ({})", thisServerId, desiredLeaderId);
	}
	return demoteLeader;
}

void RaftManager::VotingManager::ClearDesiredLeaderId() noexcept {
	lock_guard lck(mtx_);
	nextLeaderId_.ClearNextLeaderId();
}

int RaftManager::VotingManager::GetDesiredLeaderId() noexcept {
	lock_guard lck(mtx_);
	return nextLeaderId_.GetNextLeaderId();
}

bool RaftManager::VotingManager::LeaderIsAvailable(ClockT::time_point now) const noexcept {
	lock_guard lck(mtx_);
	return leaderIsAvailable(now);
}

bool RaftManager::VotingManager::LeaderIsCommittedAvailable(ClockT::time_point now) const noexcept {
	lock_guard lck(mtx_);
	return leaderIsCommittedAvailable(now);
}

bool RaftManager::VotingManager::leaderIsAvailable(ClockT::time_point now) const noexcept {
	const bool hasRecentLeadersPing = (now - data_.lastLeaderPingTs) < kMinLeaderAwaitInterval;
	return hasRecentLeadersPing || (data_.role == RaftInfo::Role::Leader);
}

bool RaftManager::VotingManager::leaderIsCommittedAvailable(ClockT::time_point now) const noexcept {
	return (data_.role == RaftInfo::Role::Leader) || (leaderIsAvailable(now) && GrantsCommittedAvailability(data_.lastCommitState));
}

bool RaftManager::VotingManager::followerReady(ClockT::time_point now, bool requireForeignLeader, int thisServerId) noexcept {
	const int desired = nextLeaderId_.GetNextLeaderId();
	return leaderIsCommittedAvailable(now) && (!requireForeignLeader || data_.leaderId != thisServerId) &&
		   (desired < 0 || data_.leaderId == desired);
}

bool RaftManager::VotingManager::FollowerPublishReady(int thisServerId, ClockT::time_point now) noexcept {
	lock_guard lck(mtx_);
	constexpr bool requireForeignLeader = true;
	return followerReady(now, requireForeignLeader, thisServerId);
}

bool RaftManager::VotingManager::FollowerStayReady(ClockT::time_point now) noexcept {
	lock_guard lck(mtx_);
	constexpr bool requireForeignLeader = false;
	return followerReady(now, requireForeignLeader, /*thisServerId=*/-1);
}

void RaftManager::startPingRoutines() {
	assert(pingWg_.wait_count() == 0);
	for (size_t nodeId = 0; nodeId < nodes_.size(); ++nodeId) {
		nodes_[nodeId].isOk = true;
		nodes_[nodeId].hasNetworkError = false;
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop_.spawn(pingWg_, [this, nodeId]() noexcept {
			auto& node = nodes_[nodeId];
			auto err = node.client.Connect(node.dsn, loop_);
			(void)err;	// Error will be handled during the further requests
			bool isFirstPing = true;
			while (!terminate_.load()) {
				auto voteData = voting_.GetVoteData();
				if (voteData.role != RaftInfo::Role::Leader) {
					break;
				}
				NodeData leader;
				leader.serverId = serverId_;
				leader.electionsTerm = voteData.term;
				leader.leaderCommitState = LeaderCommitState::Committed;
#ifdef RX_ENABLE_EXTRA_CLUSTER_LOGS
				logTrace("{} Sending ping to {}({})", serverId_, node.uid, node.serverId);
#endif	// RX_ENABLE_EXTRA_CLUSTER_LOGS
				err = node.client.LeadersPing(leader);
#ifdef RX_ENABLE_EXTRA_CLUSTER_LOGS
				logTrace("{} Ping to {}({}) was sent", serverId_, node.uid, node.serverId);
#endif	// RX_ENABLE_EXTRA_CLUSTER_LOGS
				const bool isNetworkError = (err.code() == errTimeout) || (err.code() == errNetwork);
				if (node.isOk != err.ok() || isNetworkError != node.hasNetworkError || isFirstPing) {
					node.isOk = err.ok();

					statsCollector_.OnStatusChanged(
						nodeId, node.isOk ? NodeStats::Status::Online
										  : (isNetworkError ? NodeStats::Status::Offline : NodeStats::Status::RaftError));
					if (isNetworkError != node.hasNetworkError) {
						logTrace("{} Network status was changed for {}({}). Status: {}, network: {}", serverId_, node.uid, node.serverId,
								 node.isOk ? 1 : 0, isNetworkError ? 0 : 1);
						onNodeNetworkStatusChangedCb_(node.uid, !isNetworkError);
					}
					node.hasNetworkError = isNetworkError;
					isFirstPing = false;
				}
				loop_.sleep(kLeaderPingInterval);
			}
			node.client.Stop();
		});
	}
}

int32_t RaftManager::beginElectionsTerm(int presetLeader) {
	const auto [term, oldRole] = voting_.StartNewTerm(presetLeader >= 0 ? presetLeader : serverId_);

	logTrace("{}: Role has been switched to candidate from {}", serverId_, RaftInfo::RoleToStr(oldRole));
	if (oldRole == RaftInfo::Role::Leader) {
		pingWg_.wait();
	}
	return term;
}

bool RaftManager::endElections(int32_t term, ClockT::time_point roundBeg, RaftInfo::Role result) {
	switch (result) {
		case RaftInfo::Role::Leader: {
			if ((ClockT::now() - roundBeg) > (0.8 * kMinLeaderAwaitInterval)) {
				logTrace("{}: Elections term {} took too long. Unable to become leader", serverId_, term);
				return false;
			}
			if (!voting_.TryToSetLeaderRoleInTerm(term, serverId_)) {
				return false;
			}

			startPingRoutines();
			return true;
		}
		case RaftInfo::Role::Follower: {
			coroutine::wait_group wg;
			for (auto& node : nodes_) {
				loop_.spawn(wg, [&node]() { node.client.Stop(); });
			}
			wg.wait();
			const auto await =
				kMinLeaderAwaitInterval + std::chrono::milliseconds(tools::RandomGenerator::getu32(0, kMaxLeaderAwaitDiff.count()));
			const auto pred = [this] { return voting_.FollowerPublishReady(serverId_, ClockT::now()); };
			loop_.granular_sleep(await, kGranularSleepInterval, pred);
			if (!pred()) {
				return false;
			}
			voting_.SetFollowerRole();
			return true;
		}
		case RaftInfo::Role::None:
		case RaftInfo::Role::Candidate:
			assert(false);
			// This should not happen
	}
	return false;
}

bool RaftManager::isConsensus(size_t num) const noexcept { return num >= GetConsensusForN(nodes_.size() + 1); }

Error RaftManager::SendDesiredLeaderId(int nextLeaderId) noexcept {
	DesiredLeaderIdSender sender(loop_, nodes_, serverId_, nextLeaderId, log_);
	auto err = sender.Send();
	sender.StopClients();
	return err;
}

Error RaftManager::DesiredLeaderIdSender::Send() noexcept {
	auto err = startClients();
	if (!err.ok()) {
		return err;
	}

	uint32_t okCount = 1;
	coroutine::wait_group wg;
	std::string errString;

	const bool thisNodeIsNext = (nextServerNodeIndex_ == nodes_.size());
	if (!thisNodeIsNext) {
		logTrace("{} Checking if node with desired server ID ({}) is available", thisServerId_, nextLeaderId_);
		if (auto err = clients_[nextServerNodeIndex_].WithTimeout(kDesiredLeaderTimeout).Status(true); !err.ok()) {
			return Error(err.code(), "Target node {} is not available.", nodes_[nextServerNodeIndex_].dsn);
		}
	}
	for (size_t nodeId = 0; nodeId < clients_.size(); ++nodeId) {
		if (nodeId == nextServerNodeIndex_) {
			continue;
		}

		try {
			// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
			loop_.spawn(wg, [this, nodeId, &errString, &okCount] {
				logTrace("{} Sending desired server ID ({}) to node with server ID {}", thisServerId_, nextLeaderId_,
						 nodes_[nodeId].serverId);
				if (auto err = sendDesiredServerIdToNode(nodeId); err.ok()) {
					++okCount;
				} else {
					errString += "[" + err.whatStr() + "]";
				}
			});
		} catch (std::exception& e) {
			logError("{}: Unable to spawn desired leader sender coroutine: '{}'", thisServerId_, e.what());
		}
	}
	wg.wait();

	if (!thisNodeIsNext) {
		logTrace("{} Sending desired server ID ({}) to node with server ID {}", thisServerId_, nextLeaderId_,
				 nodes_[nextServerNodeIndex_].serverId);
		if (auto err = sendDesiredServerIdToNode(nextServerNodeIndex_); !err.ok()) {
			return err;
		}
		++okCount;
	}

	if (okCount < GetConsensusForN(nodes_.size() + 1)) {
		return Error(errNetwork, "Can't send nextLeaderId to servers okCount {} err: {}", okCount, errString);
	}
	return Error();
}

// NOLINTNEXTLINE (bugprone-exception-escape) May throw std::bad_alloc, but there are no good ways to recover, crash is intended
void RaftManager::DesiredLeaderIdSender::StopClients() noexcept {
	coroutine::wait_group wgStop;
	for (auto& client : clients_) {
		loop_.spawn(wgStop, [&client]() noexcept { client.Stop(); });
	}
	wgStop.wait();
	clients_.clear();
}

Error RaftManager::DesiredLeaderIdSender::startClients() noexcept {
	if (clients_.empty() && !nodes_.empty()) {
		try {
			client::ReindexerConfig rpcCfg;
			rpcCfg.AppName = "raft_manager_tmp";
			rpcCfg.NetTimeout = kRaftTimeout;
			rpcCfg.EnableCompression = false;
			rpcCfg.RequestDedicatedThread = true;
			clients_.reserve(nodes_.size());
			for (size_t i = 0; i < nodes_.size(); ++i) {
				auto& client = clients_.emplace_back(rpcCfg);
				auto err = client.Connect(nodes_[i].dsn, loop_);
				(void)err;	// Ignore connection errors. Handle them on the status phase
				if (nodes_[i].serverId == nextLeaderId_) {
					nextServerNodeIndex_ = i;
				}
			}
		} catch (std::exception& e) {
			StopClients();
			return std::move(e);
		}
	}
	return Error();
}

Error RaftManager::DesiredLeaderIdSender::sendDesiredServerIdToNode(size_t nodeId) noexcept {
	auto client = clients_[nodeId].WithTimeout(kDesiredLeaderTimeout);
	auto err = client.Status(true);
	return !err.ok() ? err : client.SetDesiredLeaderId(nextLeaderId_);
}

bool RaftManager::VotingManager::TryToSetLeaderRoleInTerm(int32_t term, int thisServerId) noexcept {
	lock_guard lck(mtx_);
	assertrx_dbg(term >= 0);

	if (data_.term != term) {
		return false;
	}
	data_.role = RaftInfo::Role::Leader;
	data_.leaderId = thisServerId;
	data_.lastCommitState = LeaderCommitState::Committed;
	return true;
}

void RaftManager::VotingManager::SetFollowerRole() noexcept {
	lock_guard lck(mtx_);
	data_.role = RaftInfo::Role::Follower;
}

std::pair<int32_t, RaftInfo::Role> RaftManager::VotingManager::StartNewTerm(int presetLeaderId) noexcept {
	lock_guard lck(mtx_);

	const int32_t term = data_.term + 1;
	const auto oldRole = data_.role;
	const auto now = ClockT::now();
	const bool wasLeader = (oldRole == RaftInfo::Role::Leader);
	// Keep a live remote Committed lease; drop Election ping-lease (otherwise skip phase-2 with nothing to publish).
	const bool remoteCommittedLeaseLive =
		leaderIsCommittedAvailable(now) && data_.leaderId >= 0 && data_.leaderId != presetLeaderId && !wasLeader;

	data_.term = term;
	data_.role = RaftInfo::Role::Candidate;
	if (!remoteCommittedLeaseLive) {
		data_.leaderId = presetLeaderId;
		data_.lastLeaderPingTs = ClockT::time_point{};
		data_.lastCommitState = LeaderCommitState::Unspecified;
	}
	return std::make_pair(term, oldRole);
}

}  // namespace cluster
}  // namespace reindexer
