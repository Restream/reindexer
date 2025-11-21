#include "raftmanager.h"
#include "cluster/logger.h"
#include "core/dbconfig.h"
#include "tools/randomgenerator.h"
#include "tools/scope_guard.h"

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
	auto wgWaiter = MakeScopeGuard([&wg]() noexcept { wg.wait(); });
	const auto roundBeg = ClockT::now();
	try {
		std::vector<coroutine::routine_t> succeedRoutines;
		succeedRoutines.reserve(nodes_.size());

		const int nextLeaderId = voting_.GetDesiredLeaderId();
		const bool isDesiredLeader = (nextLeaderId == serverId_);
		if (!isDesiredLeader && nextLeaderId != -1) {
			std::ignore = endElections(-1, roundBeg, RaftInfo::Role::Follower);
			logInfo("{}: Skipping elections (desired leader id is {})", serverId_, nextLeaderId);
			return RaftInfo::Role::Follower;
		}
		int32_t term = beginElectionsTerm(nextLeaderId);
		logInfo("{}: Starting new elections term. Term number: {}", serverId_, term);
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

				const bool leaderIsAvailable = !isDesiredLeader && LeaderIsAvailable(ClockT::now());
				if (leaderIsAvailable || !isConsensus(electionsStat.succeedPhase1)) {
					logInfo("{}: Skip leaders ping. Elections are outdated. leaderIsAvailable: {}. Successful requests: {}", serverId_,
							leaderIsAvailable ? 1 : 0, electionsStat.succeedPhase1);
					return;	 // These elections are outdated
				}
				err = node.client.LeadersPing(suggestion);
				if (err.ok()) {
					++electionsStat.succeedPhase2;
				} else {
					logInfo("{}: leader's ping error: {}", serverId_, err.what());
				}
			});
		}

		RaftInfo::Role result = nodes_.empty() ? RaftInfo::Role::Leader : RaftInfo::Role::Follower;

		while (wg.wait_count()) {
			wg.wait_next();
			if (isConsensus(electionsStat.succeedPhase2)) {
				result = RaftInfo::Role::Leader;
				if (endElections(term, roundBeg, result)) {
					logInfo("{}: end elections with role: leader", serverId_);
					return result;
				}
			}
		}
		logInfo("{}: votes stats: phase1: {}; phase2: {}; fails: {}", serverId_, electionsStat.succeedPhase1, electionsStat.succeedPhase2,
				electionsStat.failed);

		if (endElections(term, roundBeg, result)) {
			logInfo("[{}: end elections with role: {}({})", serverId_, RaftInfo::RoleToStr(result), GetLeaderId());
			return result;
		} else {
			logInfo("{}: Failed to end elections with chosen role: {}", serverId_, RaftInfo::RoleToStr(result));
		}
	} catch (std::exception& e) {
		logError("{}: exception during the elections: {}", serverId_, e.what());
	}
	return std::nullopt;
}

bool RaftManager::FollowersAreAvailable() const {
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

void RaftManager::VotingManager::LeadersPing(const NodeData& leader) {
	lock_guard lck(mtx_);

	const auto NextLeaderId = nextLeaderId_.GetNextLeaderId();
	if (NextLeaderId >= 0 && NextLeaderId != leader.serverId) {
		throw Error(errLogic, "This node has different desired leader: {}", NextLeaderId);
	}
	if (data_.role == RaftInfo::Role::Leader) {
		throw Error(errLogic, "This node is a leader itself");
	}
	if (data_.leaderId != leader.serverId && leaderIsAvailable(ClockT::now())) {
		throw Error(errLogic, "This node has another leader: {}", data_.leaderId);
	}
	data_.leaderId = leader.serverId;
	data_.term = leader.electionsTerm;
	data_.lastLeaderPingTs = ClockT::now();
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
			data_.term = suggestion.electionsTerm;
			data_.leaderId = sId;
			response.serverId = sId;
			response.electionsTerm = suggestion.electionsTerm;
		} else if (nextLeaderId != -1) {
			data_.term = suggestion.electionsTerm;
			data_.leaderId = nextLeaderId;
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

void RaftManager::VotingManager::SetDesiredLeaderId(int thisServerId, int desiredLeaderId) {
	lock_guard lck(mtx_);
	logInfo("{}: Set ({}) as a desired leader", thisServerId, desiredLeaderId);
	nextLeaderId_.SetNextLeaderId(desiredLeaderId);
	data_.lastLeaderPingTs = {ClockT::time_point()};
}

int RaftManager::VotingManager::GetDesiredLeaderId() noexcept {
	lock_guard lck(mtx_);
	return nextLeaderId_.GetNextLeaderId();
}

bool RaftManager::VotingManager::LeaderIsAvailable(ClockT::time_point now) const noexcept {
	lock_guard lck(mtx_);
	return leaderIsAvailable(now);
}

bool RaftManager::VotingManager::leaderIsAvailable(ClockT::time_point now) const noexcept {
	const bool hasRecentLeadersPing = (now - data_.lastLeaderPingTs) < kMinLeaderAwaitInterval;
	return hasRecentLeadersPing || (data_.role == RaftInfo::Role::Leader);
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
				err = node.client.LeadersPing(leader);
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

void RaftManager::randomizedSleep(net::ev::dynamic_loop& loop, std::chrono::milliseconds base, std::chrono::milliseconds maxDiff) {
	const auto interval = base + std::chrono::milliseconds(tools::RandomGenerator::getu32(0, maxDiff.count()));
	loop.sleep(interval);
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
			if (!voting_.TryToSetLeaderRoleInTerm(term)) {
				return false;
			}

			startPingRoutines();
			return true;
		}
		case RaftInfo::Role::Follower: {
			voting_.SetFollowerRole();
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

RaftManager::DesiredLeaderIdSender::DesiredLeaderIdSender(net::ev::dynamic_loop& loop, const std::vector<RaftNode>& nodes, int serverId,
														  int nextLeaderId, const Logger& log)
	: loop_(loop), nodes_(nodes), log_(log), thisServerId_(serverId), nextLeaderId_(nextLeaderId), nextServerNodeIndex_(nodes_.size()) {
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
			err = client.WithTimeout(kDesiredLeaderTimeout).Status(true);
			if (!err.ok()) {
				throw Error(err.code(), "Target node {} is not available.", nodes_[i].dsn);
			}
		}
	}
}

Error RaftManager::DesiredLeaderIdSender::operator()() {
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

		loop_.spawn(wg, [this, nodeId, &errString, &okCount] {
			try {
				logTrace("{} Sending desired server ID ({}) to node with server ID {}", thisServerId_, nextLeaderId_,
						 nodes_[nodeId].serverId);
				if (auto err = sendDesiredServerIdToNode(nodeId); err.ok()) {
					++okCount;
				} else {
					errString += "[" + err.whatStr() + "]";
				}
			} catch (...) {
				logInfo("{}: Unable to send desired leader: got unknown exception", thisServerId_);
			}
		});
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

bool RaftManager::VotingManager::TryToSetLeaderRoleInTerm(int32_t term) noexcept {
	lock_guard lck(mtx_);
	assertrx_dbg(term >= 0);

	if (data_.term != term) {
		return false;
	}
	data_.role = RaftInfo::Role::Leader;
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
	data_.term = term;
	data_.role = RaftInfo::Role::Candidate;
	data_.leaderId = presetLeaderId;
	return std::make_pair(term, oldRole);
}

}  // namespace cluster
}  // namespace reindexer
