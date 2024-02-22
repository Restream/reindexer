#pragma once

#include <vector>
#include "client/raftclient.h"
#include "cluster/config.h"
#include "cluster/consts.h"
#include "cluster/stats/relicationstatscollector.h"
#include "tools/catch_and_return.h"

namespace reindexer {

struct ReplicationConfigData;

namespace cluster {

class Logger;

class RaftManager {
public:
	using ClockT = std::chrono::steady_clock;

	RaftManager(net::ev::dynamic_loop &loop, ReplicationStatsCollector statsCollector, const Logger &l,
				std::function<void(uint32_t, bool)> onNodeNetworkStatusChangedCb);

	void SetTerminateFlag(bool val) noexcept { terminate_ = val; }
	void Configure(const ReplicationConfigData &, const ClusterConfigData &);
	RaftInfo::Role Elections();
	bool LeaderIsAvailable(ClockT::time_point now) const noexcept;
	bool FollowersAreAvailable();
	int32_t GetLeaderId() const { return getLeaderId(voteData_.load()); }
	RaftInfo::Role GetRole() const { return getRole(voteData_.load()); }
	int32_t GetTerm() const { return getTerm(voteData_.load()); }
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	[[nodiscard]] Error SendDesiredLeaderId(int nextServerId) noexcept {
		RETURN_RESULT_NOEXCEPT(DesiredLeaderIdSender(loop_, nodes_, serverId_, nextServerId, log_)())
	}
	void SetDesiredLeaderId(int serverId);
	int GetDesiredLeaderId() { return nextServerId_.GetNextServerId(); }
	Error LeadersPing(const cluster::NodeData &);
	void AwaitTermination();

private:
	struct NextServerId {
		void SetNextServerId(int id) {
			std::lock_guard lock(mtx);
			nextServerId_ = id;
			startPoint_ = std::chrono::high_resolution_clock::now();
		}
		int GetNextServerId() {
			std::lock_guard lock(mtx);
			if (nextServerId_ != -1) {
				const auto current = std::chrono::high_resolution_clock::now();
				if (current - startPoint_ > kDesiredLeaderTimeout) {
					nextServerId_ = -1;
				}
			}
			return nextServerId_;
		}

	private:
		std::mutex mtx;
		int nextServerId_ = -1;
		std::chrono::time_point<std::chrono::high_resolution_clock> startPoint_;
	};

	struct RaftNode {
		RaftNode(const client::ReindexerConfig &config, std::string _dsn, uint32_t _uid, int _serverId)
			: client(config), dsn(std::move(_dsn)), uid(_uid), serverId(_serverId) {}
		client::RaftClient client;
		std::string dsn;
		bool isOk = false;
		bool hasNetworkError = false;
		uint32_t uid = 0;
		int serverId = -1;
	};

	class DesiredLeaderIdSender {
	public:
		DesiredLeaderIdSender(net::ev::dynamic_loop &, const std::vector<RaftNode> &, int serverId, int nextServerId, const Logger &);
		~DesiredLeaderIdSender() {
			coroutine::wait_group wgStop;
			for (auto &client : clients_) {
				loop_.spawn(wgStop, [&client]() { client.Stop(); });
			}
			wgStop.wait();
		}

		Error operator()();

	private:
		constexpr std::string_view logModuleName() noexcept { return std::string_view("raftmanager:leadersender"); }
		Error sendDesiredServerIdToNode(size_t nodeId) {
			auto client = clients_[nodeId].WithTimeout(kDesiredLeaderTimeout);
			auto err = client.Status(true);
			return !err.ok() ? err : client.SetDesiredLeaderId(nextServerId_);
		}

		net::ev::dynamic_loop &loop_;
		std::vector<client::RaftClient> clients_;
		const std::vector<RaftNode> &nodes_;
		const Logger &log_;
		const int thisServerId_;
		const int nextServerId_;
		size_t nextServerNodeIndex_;
	};

	constexpr std::string_view logModuleName() noexcept { return std::string_view("raftmanager"); }
	void startPingRoutines();
	static int32_t getLeaderId(int64_t voteData) { return int32_t(voteData & 0x00FFFFFF); }
	static int64_t setLeaderId(int64_t voteData, int32_t leaderId) { return (leaderId & 0x00FFFFFF) | (voteData & ~0x00FFFFFFll); }
	static RaftInfo::Role getRole(int64_t voteData) { return RaftInfo::Role((voteData >> 24) & 0xFF); }
	static int64_t setRole(int64_t voteData, RaftInfo::Role role) { return (voteData & ~(0xFFll << 24)) | (int64_t(role) << 24); }
	static int32_t getTerm(int64_t voteData) { return int32_t(voteData >> 32); }
	static int64_t setTerm(int64_t voteData, int32_t term) { return (int64_t(term) << 32) + (voteData & ~(0xFFFFFFFFll << 32)); }
	static void randomizedSleep(net::ev::dynamic_loop &loop, std::chrono::milliseconds base, std::chrono::milliseconds maxDiff);
	int32_t beginElectionsTerm(int presetLeader);
	bool endElections(int32_t term, RaftInfo::Role result);
	bool isConsensus(size_t num) const noexcept;
	bool hasRecentLeadersPing(ClockT::time_point now) const noexcept;
	client::ConnectOpts createConnectionOpts() const { return client::ConnectOpts().WithExpectedClusterID(clusterID_); }

	net::ev::dynamic_loop &loop_;
	ReplicationStatsCollector statsCollector_;
	std::vector<RaftNode> nodes_;
	std::atomic_int_fast64_t voteData_ = {0};  // roundId << 32 + role << 24 + leaderId
	std::atomic<ClockT::time_point> lastLeaderPingTs_ = {ClockT::time_point()};
	std::atomic<bool> terminate_ = {false};
	coroutine::wait_group pingWg_;
	int32_t serverId_ = -1;
	int clusterID_ = 1;
	NextServerId nextServerId_;
	std::function<void(uint32_t, bool)> onNodeNetworkStatusChangedCb_;
	const Logger &log_;
};

}  // namespace cluster
}  // namespace reindexer
