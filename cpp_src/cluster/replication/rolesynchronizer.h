#pragma once

#include <vector>
#include "client/cororeindexer.h"
#include "core/dbconfig.h"
#include "estl/looptaskexecutor.h"
#include "rolesyncstatus.h"

namespace reindexer {
namespace cluster {

/// @brief Resets obsolete async replication roles on followers.
class [[nodiscard]] RolesSynchronizer {
public:
	/// @param loop - Event loop used by replication role tasks.
	/// @param executor - Enqueues tasks for execution on the supplied event loop.
	RolesSynchronizer(net::ev::dynamic_loop& loop, ILoopTaskExecutor& executor) : loop_{loop}, tasksExecutor_{executor} {}

	/// Schedules removed replication role resets without waiting for completion.
	/// @param baseConfig - Common replication configuration.
	/// @param oldConfig - Previous async replication configuration.
	/// @param newConfig - Updated async replication configuration.
	/// @param localNamespaces - Local leader namespaces used to expand [*].
	void ResetRemovedRoles(const ReplicationConfigData& baseConfig, const AsyncReplConfigData& oldConfig,
						   const AsyncReplConfigData& newConfig, const NsNamesHashSetT& localNamespaces) noexcept;

	/// Synchronizes follower roles with the current configuration and awaits for completion.
	/// @param baseConfig - Common replication configuration.
	/// @param config - Current async replication configuration.
	/// @param localNamespaces - Local leader namespaces for handling [*].
	RolesSyncResult SynchronizeRoles(const ReplicationConfigData& baseConfig, const AsyncReplConfigData& config,
									 const NsNamesHashSetT& localNamespaces) noexcept;

private:
	struct [[nodiscard]] ResetRoleTask {
		std::vector<NamespaceName> namespaces;
		DSN dsn;
		int serverId;
		int clusterId;
		client::ReindexerConfig clientConfig;
	};

	/// Builds one reset job per affected follower from
	/// the difference between the old and new configurations.
	/// @param oldConfig - Previous async replication configuration.
	/// @param newConfig - Updated async replication configuration.
	/// @param localNamespaces - Local leader namespaces used to expand [*].
	/// @param baseConfig - Common replication configuration.
	/// @param clientConfig - RPC client settings.
	static std::vector<ResetRoleTask> makeResetJobs(const AsyncReplConfigData& oldConfig, const AsyncReplConfigData& newConfig,
													const NsNamesHashSetT& localNamespaces, const ReplicationConfigData& baseConfig,
													const client::ReindexerConfig& clientConfig);

	/// Resets obsolete roles on one follower, recording counters and errors in status.
	/// @param followerDsn - Follower DSN.
	/// @param baseConfig - Common replication configuration.
	/// @param clientConfig - RPC client settings.
	/// @param deadline - RPC client deadline.
	/// @param namespacesToReplicate - Effective list of namespaces to preserve on this follower.
	/// @param status - Per-node result with reset attempts, successful resets, and the last error.
	void synchronizeNode(const DSN& followerDsn, const ReplicationConfigData& baseConfig, const client::ReindexerConfig& clientConfig,
						 const RdxDeadlineContext& deadline, const NsNamesHashSetT& namespacesToReplicate,
						 RolesSyncNodeStatus& status) noexcept;

	net::ev::dynamic_loop& loop_;
	ILoopTaskExecutor& tasksExecutor_;
};

}  // namespace cluster
}  // namespace reindexer
