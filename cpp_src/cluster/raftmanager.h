#pragma once

#include <vector>
#include "client/raftclient.h"
#include "cluster/config.h"

namespace reindexer {
namespace cluster {

constexpr auto kControlTimeout = std::chrono::milliseconds(1000);
constexpr auto kLeaderPingInterval = std::chrono::milliseconds(200);
constexpr auto kMinLeaderAwaitInterval = kLeaderPingInterval * 4;
constexpr auto kMaxLeaderAwaitDiff = std::chrono::milliseconds(400);
constexpr auto kMinStateCheckInerval = kLeaderPingInterval * 4;
constexpr auto kMaxStateCheckDiff = std::chrono::milliseconds(400);
constexpr auto kGranularSleepInterval = std::chrono::milliseconds(50);

class RaftManager {
public:
	using ClockT = std::chrono::steady_clock;

	RaftManager(net::ev::dynamic_loop &loop);

	void SetTerminateFlag(bool val) noexcept { terminate_ = val; }
	void Configure(int serverId, const ClusterConfigData &config);
	RaftInfo::Role Elections();
	bool LeaderIsAvailable(ClockT::time_point now);
	bool FollowersAreAvailable();
	int32_t GetLeaderId() const { return getLeaderId(voteData_.load()); }
	RaftInfo::Role GetRole() const { return getRole(voteData_.load()); }
	int32_t GetTerm() const { return getTerm(voteData_.load()); }
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	Error LeadersPing(const cluster::NodeData &);
	void AwaitTermination();

private:
	struct RaftNode {
		RaftNode(const client::CoroReindexerConfig &config, std::string _dsn) : client(config), dsn(std::move(_dsn)) {}

		client::RaftClient client;
		std::string dsn;
		bool isOk = false;
	};

	void startPingRoutines();
	static int32_t getLeaderId(int64_t voteData) { return int32_t(voteData & 0x00FFFFFF); }
	static int64_t setLeaderId(int64_t voteData, int32_t leaderId) { return (leaderId & 0x00FFFFFF) | (voteData & ~0x00FFFFFFll); }
	static RaftInfo::Role getRole(int64_t voteData) { return RaftInfo::Role((voteData >> 24) & 0xFF); }
	static int64_t setRole(int64_t voteData, RaftInfo::Role role) { return (voteData & ~(0xFFll << 24)) | (int64_t(role) << 24); }
	static int32_t getTerm(int64_t voteData) { return int32_t(voteData >> 32); }
	static int64_t setTerm(int64_t voteData, int32_t term) { return (int64_t(term) << 32) + (voteData & ~(0xFFFFFFFFll << 32)); }
	static void randomizedSleep(net::ev::dynamic_loop &loop, std::chrono::milliseconds base, std::chrono::milliseconds maxDiff);
	int32_t beginElectionsTerm();
	bool endElections(int32_t term, RaftInfo::Role result);
	bool isConsensus(size_t num) const noexcept;

	net::ev::dynamic_loop &loop_;
	std::vector<RaftNode> nodes_;
	std::atomic_int_fast64_t voteData_ = {0};  // roundId << 32 + role << 24 + leaderId
	std::atomic<ClockT::time_point> lastLeaderPingTs_ = {ClockT::time_point()};
	std::atomic<bool> terminate_ = {false};
	coroutine::wait_group pingWg_;
	int32_t serverId_ = -1;
};

}  // namespace cluster
}  // namespace reindexer
