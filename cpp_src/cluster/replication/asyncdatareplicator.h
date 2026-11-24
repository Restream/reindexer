#pragma once

#include "asyncreplthread.h"
#include "cluster/logger.h"
#include "core/dbconfig.h"
#include "estl/condition_variable.h"
#include "estl/looptask.h"
#include "estl/looptaskexecutor.h"
#include "estl/mutex.h"
#include "estl/thread_annotation_attributes.h"
#include "rolesynchronizer.h"
#include "updatesqueuepair.h"

namespace reindexer {
namespace cluster {

class ClusterManager;

class [[nodiscard]] AsyncDataReplicator : private ILoopTaskExecutor {
public:
	using UpdatesQueueT = UpdatesQueuePair<updates::UpdateRecord>;

	AsyncDataReplicator(UpdatesQueueT&, SharedSyncState&, ReindexerImpl&, ClusterManager&);
	~AsyncDataReplicator() override;

	void Configure(AsyncReplConfigData config);
	void Configure(ReplicationConfigData config);
	RolesSyncResult SynchronizeReplicationRoles();
	bool IsExpectingStartup() const noexcept;
	void Run(const std::shared_ptr<NamespacesSyncScheduler>&);
	void Stop(bool resetConfig = false);
	const std::optional<AsyncReplConfigData>& Config() const noexcept { return config_; }
	ReplicationStats GetReplicationStats() const;
	bool NamespaceIsInAsyncConfig(std::string_view nsName) const;
	void SetLogLevel(LogLevel level) noexcept { log_.SetLevel(level); }
	LogLevel GetLogLevel() const noexcept { return log_.GetLevel(); }

private:
	static constexpr std::string_view logModuleName() noexcept { return std::string_view("asyncreplicator"); }
	bool isExpectingStartup() const noexcept;
	size_t threadsCount() const noexcept;
	bool isRunning() const noexcept { return replThreads_.size(); }
	void stop();
	void taskExecutionLoop();
	void addTask(LoopTask&& task) override;
	NsNamesHashSetT getLocalNamespaces();
	static NsNamesHashSetT getMergedNsConfig(const AsyncReplConfigData& config);

	ReplicationStatsCollector statsCollector_;
	mutable mutex mtx_;
	UpdatesQueueT& updatesQueue_;
	SharedSyncState& syncState_;
	ReindexerImpl& thisNode_;
	ClusterManager& clusterManager_;
	std::deque<AsyncReplThread> replThreads_;
	mutex tasksMutex_;
	bool terminate_ RX_GUARDED_BY(tasksMutex_) = false;
	net::ev::dynamic_loop loop_;
	condition_variable taskReady_;
	std::deque<LoopTask> tasksQueue_ RX_GUARDED_BY(tasksMutex_);
	std::thread taskExecutionThread_;
	RolesSynchronizer roleSynchronizer_;
	std::optional<AsyncReplConfigData> config_;
	std::optional<ReplicationConfigData> baseConfig_;
	Logger log_;
	static constexpr int kDefaultReplThreadCount = 4;
};

}  // namespace cluster
}  // namespace reindexer
