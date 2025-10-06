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

class [[nodiscard]] ClusterDataReplicator {
public:
	using UpdatesQueueT = UpdatesQueuePair<updates::UpdateRecord>;

	ClusterDataReplicator(UpdatesQueueT&, SharedSyncState&, ReindexerImpl&);

	void Configure(ClusterConfigData config);
	void Configure(ReplicationConfigData config);
	bool IsExpectingStartup() const noexcept;
	void Run();
	void Stop(bool resetConfig = false);
	const std::optional<ClusterConfigData>& Config() const noexcept { return config_; }

	void SuggestLeader(const NodeData& suggestion, NodeData& response);
	void SetDesiredLeaderId(int serverId, bool sendToOtherNodes);
	void ForceElections();
	void LeadersPing(const NodeData& leader);
	RaftInfo GetRaftInfo(bool allowTransitState, const RdxContext& ctx) const;
	bool NamespaceIsInClusterConfig(std::string_view nsName) const;
	ReplicationStats GetReplicationStats() const;
	void SetLogLevel(LogLevel level) noexcept { log_.SetLevel(level); }

private:
	static constexpr std::string_view logModuleName() noexcept { return std::string_view("syncreplicator"); }
	int serverID() const noexcept { return baseConfig_.has_value() ? baseConfig_->serverID : -1; }
	bool isExpectingStartup() const noexcept;
	size_t threadsCount() const noexcept {
		return config_.has_value() && config_->replThreadsCount > 0 ? config_->replThreadsCount : kDefaultReplThreadCount;
	}
	bool isRunning() const noexcept { return raftThread_.joinable(); }
	void clusterControlRoutine(int serverId);
	void handleClusterCommands(int serverId, const RaftInfo& curRaftInfo);
	DSN getManagementDsn(int id) const;
	void onRoleChanged(RaftInfo::Role to, int leaderId);
	void stop();

	ReplicationStatsCollector statsCollector_;
	enum ClusterCommandId { kNoCommand = -1, kCmdSetDesiredLeader = 0, kCmdForceElections = 1 };

	struct [[nodiscard]] ClusterCommand {
		struct SetDesiredLeaderT {};
		struct ForceElectionsT {};

		ClusterCommand() = default;
		ClusterCommand(SetDesiredLeaderT, int server, bool _send, std::promise<Error> p)
			: id(kCmdSetDesiredLeader), serverId(server), send(_send), result(std::move(p)) {}
		ClusterCommand(ForceElectionsT, std::promise<Error> p) : id(kCmdForceElections), result(std::move(p)) {}
		ClusterCommand(ClusterCommand&&) = default;
		ClusterCommand& operator=(ClusterCommand&& other) = default;
		ClusterCommand(ClusterCommand&) = delete;
		ClusterCommand& operator=(ClusterCommand& other) = delete;

		ClusterCommandId id = kNoCommand;
		int serverId = -1;
		bool send = false;
		std::promise<Error> result;
	};

	class [[nodiscard]] CommandQuery {
	public:
		void AddCommand(ClusterCommand&& c) {
			lock_guard lk(lock_);
			commands_.push(std::move(c));
		}
		bool GetCommand(ClusterCommand& c) noexcept {
			lock_guard lk(lock_);
			if (commands_.empty()) {
				return false;
			}
			c = std::move(commands_.front());
			commands_.pop();
			return true;
		}

	private:
		mutex lock_;
		std::queue<ClusterCommand> commands_;
	};

	net::ev::dynamic_loop loop_;
	std::thread raftThread_;
	std::thread roleSwitchThread_;
	mutable mutex mtx_;
	std::atomic<bool> restartElections_ = {false};

	CommandQuery commands_;

	std::atomic<bool> terminate_ = {false};
	Logger log_;
	RaftManager raftManager_;
	UpdatesQueueT& updatesQueue_;
	SharedSyncState& sharedSyncState_;
	ReindexerImpl& thisNode_;
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
