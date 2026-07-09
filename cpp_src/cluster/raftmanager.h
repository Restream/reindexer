#pragma once

#include <vector>
#include "client/raftclient.h"
#include "cluster/config.h"
#include "cluster/consts.h"
#include "cluster/stats/relicationstatscollector.h"
#include "estl/mutex.h"
#include "estl/thread_annotation_attributes.h"
namespace reindexer {

struct ReplicationConfigData;

namespace cluster {

class Logger;

class [[nodiscard]] RaftManager {
public:
	using ClockT = steady_clock_w;

	RaftManager(net::ev::dynamic_loop& loop, const ReplicationStatsCollector& statsCollector, const Logger& l,
				std::function<void(uint32_t, bool)> onNodeNetworkStatusChangedCb);

	void SetTerminateFlag(bool val) noexcept { terminate_ = val; }
	void Configure(const ReplicationConfigData&, const ClusterConfigData&);
	std::optional<RaftInfo::Role> RunElectionsRound() noexcept;
	bool LeaderIsAvailable(ClockT::time_point now) const noexcept { return voting_.LeaderIsAvailable(now); }
	bool FollowersAreAvailable() const noexcept;
	int GetLeaderId() const noexcept { return voting_.GetLeaderId(); }
	void SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response) {
		voting_.SuggestLeader(serverId_, suggestion, response);
	}
	void LeadersPing(const cluster::NodeData& leader) { voting_.LeadersPing(leader); }
	Error SendDesiredLeaderId(int nextLeaderId) noexcept;
	void SetDesiredLeaderId(int desiredLeaderId) { voting_.SetDesiredLeaderId(serverId_, desiredLeaderId); }
	int GetDesiredLeaderId() noexcept { return voting_.GetDesiredLeaderId(); }
	void AwaitTermination();

private:
	struct [[nodiscard]] RaftNode {
		RaftNode(const client::ReindexerConfig& config, DSN _dsn, uint32_t _uid, int _serverId)
			: client(config), dsn(std::move(_dsn)), uid(_uid), serverId(_serverId) {}
		client::RaftClient client;
		DSN dsn;
		bool isOk = false;
		bool hasNetworkError = false;
		uint32_t uid = 0;
		int serverId = -1;
	};

	// Noexcept wrapper. Designed to avoid coroutines switch during exception handling
	class [[nodiscard]] DesiredLeaderIdSender {
	public:
		DesiredLeaderIdSender(net::ev::dynamic_loop& loop, const std::vector<RaftNode>& nodes, int serverId, int nextLeaderId,
							  const Logger& log) noexcept
			: loop_(loop),
			  nodes_(nodes),
			  log_(log),
			  thisServerId_(serverId),
			  nextLeaderId_(nextLeaderId),
			  nextServerNodeIndex_(nodes_.size()) {}

		Error Send() noexcept;
		void StopClients() noexcept;

	private:
		Error startClients() noexcept;
		Error sendDesiredServerIdToNode(size_t nodeId) noexcept;
		constexpr std::string_view logModuleName() noexcept { return std::string_view("raftmanager:leadersender"); }

		net::ev::dynamic_loop& loop_;
		std::vector<client::RaftClient> clients_;
		const std::vector<RaftNode>& nodes_;
		const Logger& log_;
		const int thisServerId_;
		const int nextLeaderId_;
		size_t nextServerNodeIndex_;
	};

	class [[nodiscard]] VotingManager {
	public:
		VotingManager(const Logger& log) noexcept : log_{log} {}

		bool TryToSetLeaderRoleInTerm(int32_t term) noexcept RX_REQUIRES(!mtx_);
		void SetFollowerRole() noexcept RX_REQUIRES(!mtx_);
		std::pair<int32_t, RaftInfo::Role> StartNewTerm(int presetLeaderId) noexcept RX_REQUIRES(!mtx_);
		void LeadersPing(const cluster::NodeData&) RX_REQUIRES(!mtx_);
		void SuggestLeader(int thisServerId, const cluster::NodeData& suggestion, cluster::NodeData& response) RX_REQUIRES(!mtx_);
		void SetDesiredLeaderId(int thisServerId, int desiredLeaderId) RX_REQUIRES(!mtx_);
		int GetDesiredLeaderId() noexcept RX_REQUIRES(!mtx_);
		bool LeaderIsAvailable(ClockT::time_point now) const noexcept RX_REQUIRES(!mtx_);
		auto GetVoteData() const noexcept RX_REQUIRES(!mtx_) {
			lock_guard lck(mtx_);
			return data_;
		}
		int GetLeaderId() const noexcept RX_REQUIRES(!mtx_) { return GetVoteData().leaderId; }

	private:
		class [[nodiscard]] NextLeaderId {
		public:
			void SetNextLeaderId(int id) noexcept {
				nextLeaderId_ = id;
				startPoint_ = ClockT::now();
			}
			int GetNextLeaderId() noexcept {
				if (nextLeaderId_ != -1 && ClockT::now() - startPoint_ > kDesiredLeaderTimeout) {
					nextLeaderId_ = -1;
				}
				return nextLeaderId_;
			}

		private:
			int nextLeaderId_ = -1;
			ClockT::time_point startPoint_;
		};

		struct [[nodiscard]] VoteData {
			int16_t leaderId{-1};
			RaftInfo::Role role{RaftInfo::Role::None};
			int32_t term{0};
			ClockT::time_point lastLeaderPingTs;
		};

		constexpr static std::string_view logModuleName() noexcept { return std::string_view("raftmanager::votedata"); }
		bool leaderIsAvailable(ClockT::time_point now) const noexcept RX_REQUIRES(mtx_);

		mutable mutex mtx_;
		NextLeaderId nextLeaderId_ RX_GUARDED_BY(mtx_);
		VoteData data_ RX_GUARDED_BY(mtx_);
		const Logger& log_;
	};

	constexpr static std::string_view logModuleName() noexcept { return std::string_view("raftmanager"); }
	void startPingRoutines();
	static void randomizedSleep(net::ev::dynamic_loop& loop, std::chrono::milliseconds base, std::chrono::milliseconds maxDiff);
	int32_t beginElectionsTerm(int presetLeader);
	bool endElections(int32_t term, ClockT::time_point roundBeg, RaftInfo::Role result);
	bool isConsensus(size_t num) const noexcept;
	client::ConnectOpts createConnectionOpts() const noexcept { return client::ConnectOpts().WithExpectedClusterID(clusterID_); }
	bool leaderIsAvailable(ClockT::time_point now) const noexcept;

	net::ev::dynamic_loop& loop_;
	ReplicationStatsCollector statsCollector_;
	std::vector<RaftNode> nodes_;
	std::atomic<bool> terminate_ = {false};
	coroutine::wait_group pingWg_;
	int32_t serverId_ = -1;
	int clusterID_ = 1;

	const std::function<void(uint32_t, bool)> onNodeNetworkStatusChangedCb_;
	const Logger& log_;
	VotingManager voting_;
};

}  // namespace cluster
}  // namespace reindexer
