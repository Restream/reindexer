#pragma once

#include "asyncreplthread.h"
#include "cluster/logger.h"
#include "core/dbconfig.h"
#include "estl/mutex.h"
#include "updatesqueuepair.h"

namespace reindexer {
namespace cluster {

class ClusterManager;

class [[nodiscard]] AsyncDataReplicator {
public:
	using UpdatesQueueT = UpdatesQueuePair<updates::UpdateRecord>;

	AsyncDataReplicator(UpdatesQueueT&, SharedSyncState&, ReindexerImpl&, ClusterManager&);

	void Configure(AsyncReplConfigData config);
	void Configure(ReplicationConfigData config);
	bool IsExpectingStartup() const noexcept;
	void Run();
	void Stop(bool resetConfig = false);
	const std::optional<AsyncReplConfigData>& Config() const noexcept { return config_; }
	ReplicationStats GetReplicationStats() const;
	bool NamespaceIsInAsyncConfig(std::string_view nsName) const;
	void SetLogLevel(LogLevel level) noexcept { log_.SetLevel(level); }

private:
	static constexpr std::string_view logModuleName() noexcept { return std::string_view("asyncreplicator"); }
	bool isExpectingStartup() const noexcept;
	size_t threadsCount() const noexcept;
	bool isRunning() const noexcept { return replThreads_.size(); }
	void stop();
	NsNamesHashSetT getLocalNamespaces();
	static NsNamesHashSetT getMergedNsConfig(const AsyncReplConfigData& config);

	ReplicationStatsCollector statsCollector_;
	mutable mutex mtx_;
	UpdatesQueueT& updatesQueue_;
	SharedSyncState& syncState_;
	ReindexerImpl& thisNode_;
	ClusterManager& clusterManager_;
	std::deque<AsyncReplThread> replThreads_;
	std::optional<AsyncReplConfigData> config_;
	std::optional<ReplicationConfigData> baseConfig_;
	Logger log_;
	static constexpr int kDefaultReplThreadCount = 4;
};

}  // namespace cluster
}  // namespace reindexer
