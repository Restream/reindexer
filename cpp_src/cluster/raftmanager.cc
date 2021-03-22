#include "raftmanager.h"

namespace reindexer {
namespace cluster {

constexpr auto kRaftTimeout = std::chrono::seconds(2);

RaftManager::RaftManager(net::ev::dynamic_loop& loop) : loop_(loop) {}

void RaftManager::Configure(int serverId, const ClusterConfigData& config) {
	serverId_ = serverId;
	nodes_.clear();
	nodes_.reserve(config.nodes.size());

	client::CoroReindexerConfig rpcCfg;
	rpcCfg.AppName = config.appName;
	rpcCfg.ConnectTimeout = kRaftTimeout;
	rpcCfg.RequestTimeout = kRaftTimeout;
	rpcCfg.EnableCompression = false;
	for (auto& node : config.nodes) {
		if (node.serverId != serverId_) {
			nodes_.emplace_back(rpcCfg, node.GetManagementDsn());
		}
	}
}

RaftInfo::Role RaftManager::Elections() {
	std::vector<coroutine::routine_t> succeedRoutines;
	succeedRoutines.reserve(nodes_.size());
	while (!terminate_.load()) {
		int32_t term = beginElectionsTerm();
		std::cerr << "Starting new elections term for " << serverId_ << ": " << term << std::endl;
		coroutine::wait_group wg;
		wg.add(nodes_.size());
		succeedRoutines.resize(0);
		struct {
			size_t succeedPhase1 = 1;
			size_t succeedPhase2 = 1;
			size_t failed = 0;
		} electionsStat;
		for (auto& node : nodes_) {
			loop_.spawn([this, &electionsStat, &wg, &node, term, &succeedRoutines] {
				coroutine::wait_group_guard wgg(wg);
				if (!node.client.Status().ok()) {
					node.client.Connect(node.dsn, loop_);
				}
				NodeData suggestion, result;
				suggestion.serverId = serverId_;
				suggestion.electionsTerm = term;
				auto err = node.client.SuggestLeader(suggestion, result);
				bool succeed = err.ok() && serverId_ == result.serverId;
				if (succeed) {
					std::cerr << serverId_ << ": SuggestLeader OK" << std::endl;
					++electionsStat.succeedPhase1;
				} else {
					std::cerr << serverId_ << ": SuggestLeader error: " << err.what() << std::endl;
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

				const bool leaderIsAvailable = LeaderIsAvailable(ClockT::now());
				if (leaderIsAvailable || !isConsensus(electionsStat.succeedPhase1)) {
					std::cerr << serverId_ << ": Skip leaders ping: " << leaderIsAvailable << ":"
							  << !isConsensus(electionsStat.succeedPhase1) << err.what() << std::endl;
					return;	 // This elections are outdated
				}
				err = node.client.LeadersPing(suggestion);
				if (err.ok()) {
					++electionsStat.succeedPhase2;
				} else {
					std::cerr << serverId_ << ": LeadersPing error: " << err.what() << std::endl;
				}
			});
		}
		RaftInfo::Role result = nodes_.size() ? RaftInfo::Role::Follower : RaftInfo::Role::Leader;
		while (wg.wait_count()) {
			wg.wait_next();
			if (isConsensus(electionsStat.succeedPhase2)) {
				result = RaftInfo::Role::Leader;
				if (endElections(term, result)) {
					std::cerr << serverId_ << ": End elections with role: leader" << std::endl;
					wg.wait();
					return result;
				}
			}
		}

		std::cerr << serverId_ << ": Votes count: " << electionsStat.succeedPhase1 << ":" << electionsStat.succeedPhase2 << ":"
				  << electionsStat.failed << std::endl;

		if (endElections(term, result)) {
			std::cerr << serverId_ << ": End elections with role: " << std::string(RaftInfo::RoleToStr(result)) << std::endl;
			return result;
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
	std::cerr << "Alive followers cnt: " << aliveNodes << std::endl;
	return isConsensus(aliveNodes + 1);
}

Error RaftManager::SuggestLeader(const NodeData& suggestion, NodeData& response) {
	auto now = ClockT::now();
	std::cerr << "Local: " << GetLeaderId() << ":" << GetTerm() << "; now: " << now.time_since_epoch().count()
			  << "; leader's ts: " << lastLeaderPingTs_.load().time_since_epoch().count() << std::endl;
	while (true) {
		int64_t oldVoteData = voteData_.load();
		int64_t newVoteData;
		auto localTerm = getTerm(oldVoteData);
		auto localId = getLeaderId(oldVoteData);
		response.serverId = localId;
		response.electionsTerm = localTerm;
		if (suggestion.electionsTerm > localTerm && !LeaderIsAvailable(now)) {
			newVoteData = setLeaderId(setTerm(oldVoteData, suggestion.electionsTerm), suggestion.serverId);
			if (voteData_.compare_exchange_strong(oldVoteData, newVoteData)) {
				response.serverId = suggestion.serverId;
				response.electionsTerm = suggestion.electionsTerm;
				break;
			}
		} else {
			break;
		}
	}

	std::cerr << "Suggestion: " << suggestion.serverId << ":" << suggestion.electionsTerm << "; resp: " << response.serverId << ":"
			  << response.electionsTerm << "; local: " << GetLeaderId() << ":" << GetTerm() << std::endl;

	return errOK;
}

Error RaftManager::LeadersPing(const NodeData& leader) {
	int64_t oldVoteData, newVoteData;
	oldVoteData = voteData_.load();
	auto now = ClockT::now();
	do {
		if (getRole(oldVoteData) == RaftInfo::Role::Leader) {
			return Error(errLogic, "This node is a leader itself");
		} else if (getLeaderId(oldVoteData) != leader.serverId && LeaderIsAvailable(now)) {
			return Error(errLogic, "This node has another leader: %d", getLeaderId(oldVoteData));
		}
		newVoteData = setTerm(setLeaderId(oldVoteData, leader.serverId), leader.electionsTerm);
	} while (!voteData_.compare_exchange_strong(oldVoteData, newVoteData));
	now = ClockT::now();
	// std::cerr << "Recv ping at " << now.time_since_epoch().count() << std::endl;
	lastLeaderPingTs_.store(now);
	return errOK;
}

void RaftManager::AwaitTermination() {
	assert(terminate_);
	coroutine::wait_group wg;
	pingWg_.wait();
	for (auto& node : nodes_) {
		wg.add(1);
		loop_.spawn([&node, &wg]() {
			coroutine::wait_group_guard wgg(wg);
			node.client.Stop();
		});
	}
	wg.wait();
	SetTerminateFlag(false);
}

void RaftManager::startPingRoutines() {
	assert(pingWg_.wait_count() == 0);
	for (auto& node : nodes_) {
		node.isOk = true;
		pingWg_.add(1);
		loop_.spawn([this, &node] {
			coroutine::wait_group_guard wgg(pingWg_);
			node.client.Connect(node.dsn, loop_);
			auto voteData = voteData_.load();
			while (!terminate_.load() && getRole(voteData) == RaftInfo::Role::Leader) {
				NodeData leader;
				leader.serverId = serverId_;
				leader.electionsTerm = getTerm(voteData);
				// std::cerr << serverId_ << ": ping node " << node.dsn
				//		  << "; now: " << std::chrono::steady_clock::now().time_since_epoch().count() << std::endl;
				auto err = node.client.LeadersPing(leader);
				node.isOk = err.ok();
				// std::cerr << serverId_ << ": ping node: " << node.dsn << "; " << (node.isOk ? "OK" : "Dead")
				//		  << "; now: " << std::chrono::steady_clock::now().time_since_epoch().count() << std::endl;
				loop_.sleep(kLeaderPingInterval);
				voteData = voteData_.load();
			}
			node.client.Stop();
		});
	}
}

void RaftManager::randomizedSleep(net::ev::dynamic_loop& loop, std::chrono::milliseconds base, std::chrono::milliseconds maxDiff) {
	auto interval = base + std::chrono::milliseconds(std::rand() % maxDiff.count());
	loop.sleep(interval);
}

int32_t RaftManager::beginElectionsTerm() {
	int64_t oldVoteData, newVoteData;
	int32_t term;
	oldVoteData = voteData_.load();
	RaftInfo::Role oldRole;
	do {
		oldRole = getRole(oldVoteData);
		term = getTerm(oldVoteData) + 1;
		newVoteData = setLeaderId(setRole(setTerm(oldVoteData, term), RaftInfo::Role::Candidate), serverId_);
	} while (!voteData_.compare_exchange_strong(oldVoteData, newVoteData));
	std::cerr << serverId_ << ": Role has been switched to candidate from " << RaftInfo::RoleToStr(oldRole) << std::endl;
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
				wg.add(1);
				loop_.spawn([&node, &wg]() {
					coroutine::wait_group_guard wgg(wg);
					node.client.Stop();
				});
			}
			wg.wait();
			randomizedSleep(loop_, kMinLeaderAwaitInterval, kMaxLeaderAwaitDiff);
			return LeaderIsAvailable(RaftManager::ClockT::now());
		}
		default:
			assert(false);
			// This should not happen
	}
	return false;
}

bool RaftManager::isConsensus(size_t num) const noexcept { return num > ((nodes_.size() + 1) / 2); }

}  // namespace cluster
}  // namespace reindexer
