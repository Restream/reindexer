#pragma once

#include <future>
#include <queue>
#include "cluster/raftmanager.h"
#include "clusterreplthread.h"
#include "core/dbconfig.h"
#include "roleswitcher.h"
#include "updatesqueuepair.h"

namespace reindexer {
namespace cluster {

class ClusterDataReplicator {
public:
	using UpdatesQueueT = UpdatesQueuePair<UpdateRecord>;
	using UpdatesQueueShardT = UpdatesQueueT::QueueT;
	using NsNamesHashSetT = fast_hash_set<string, nocase_hash_str, nocase_equal_str>;

	ClusterDataReplicator(UpdatesQueueT &, SharedSyncState<> &, ReindexerImpl &);

	void Configure(ClusterConfigData config);
	void Configure(ReplicationConfigData config);
	bool IsExpectingStartup() const noexcept;
	void Run();
	void Stop(bool resetConfig = false);
	const std::optional<ClusterConfigData> &Config() const noexcept { return config_; }

	Error SuggestLeader(const NodeData &suggestion, NodeData &response);
	Error SetDesiredLeaderId(int serverId, bool sendToOtherNodes);

	Error LeadersPing(const NodeData &leader);
	RaftInfo GetRaftInfo(bool allowTransitState, const RdxContext &ctx) const;
	bool NamespaceIsInClusterConfig(std::string_view nsName) const;
	ReplicationStats GetReplicationStats() const;

private:
	int serverID() const noexcept { return baseConfig_.has_value() ? baseConfig_->serverID : -1; }
	bool isExpectingStartup() const noexcept;
	size_t threadsCount() const noexcept {
		return config_.has_value() && config_->replThreadsCount > 0 ? config_->replThreadsCount : kDefaultReplThreadCount;
	}
	bool isRunning() const noexcept { return raftThread_.joinable(); }
	void clusterControlRoutine(int serverId);
	std::string getManagementDsn(int id) const;
	void onRoleChanged(RaftInfo::Role to, int leaderId);
	void stop();

	ReplicationStatsCollector statsCollector_;
	enum ClusterCommandId { kNoComand = -1, kCmdSendDesiredLeader = 0 };

	struct ClusterCommand {
		ClusterCommand() = default;
		ClusterCommand(ClusterCommandId c, int server, std::promise<Error> p) : id(c), serverId(server), result(std::move(p)) {}
		ClusterCommand(ClusterCommand &&) = default;
		ClusterCommand &operator=(ClusterCommand &&other) = default;
		ClusterCommand(ClusterCommand &) = delete;
		ClusterCommand &operator=(ClusterCommand &other) = delete;

		ClusterCommandId id = kNoComand;
		int serverId = -1;
		std::promise<Error> result;
	};

	class CommandQuery {
	public:
		void AddCommand(ClusterCommand &&c) {
			std::lock_guard<std::mutex> lk(lock_);
			commands_.push(std::move(c));
		}
		bool GetCommand(ClusterCommand &c) {
			std::lock_guard<std::mutex> lk(lock_);
			if (commands_.empty()) {
				return false;
			}
			c = std::move(commands_.front());
			commands_.pop();
			return true;
		}

	private:
		std::mutex lock_;
		std::queue<ClusterCommand> commands_;
	};

	net::ev::dynamic_loop loop_;
	std::thread raftThread_;
	std::thread roleSwitchThread_;
	mutable std::mutex mtx_;
	std::atomic<bool> restartElections_ = {false};

	CommandQuery commands_;

	std::atomic<bool> terminate_ = {false};
	RaftManager raftManager_;
	UpdatesQueueT &updatesQueue_;
	SharedSyncState<> &sharedSyncState_;
	ReindexerImpl &thisNode_;
	std::deque<ClusterReplThread> replThreads_;
	std::function<void()> requestElectionsRestartCb_;
	std::optional<ClusterConfigData> config_;
	std::optional<ReplicationConfigData> baseConfig_;
	SynchronizationList syncList_;
	RoleSwitcher roleSwitcher_;

	static constexpr int kDefaultReplThreadCount = 4;
};

}  // namespace cluster
}  // namespace reindexer
