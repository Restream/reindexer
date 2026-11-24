#include "rolesynchronizer.h"
#include "core/namespace/namespacestat.h"
#include "estl/looptask.h"
#include "tools/logger.h"
#include "tools/scope_guard.h"

namespace reindexer {
namespace cluster {
namespace {

constexpr auto kRolesSyncTimeout = std::chrono::seconds{10};
constexpr auto kRolesResetTimeout = std::chrono::seconds{10};

client::ReindexerConfig makeCoroReindexerConfig(const AsyncReplConfigData& oldConfig) {
	return client::ReindexerConfig{.FetchAmount = 10000,
								   .ReconnectAttempts = 0,
								   .NetTimeout = kRolesResetTimeout,
								   .EnableCompression = oldConfig.enableCompression,
								   .RequestDedicatedThread = false,
								   .AppName = oldConfig.appName,
								   .SyncRxCoroCount = 10,
								   .ReplToken = oldConfig.selfReplToken};
}
}  // namespace

std::vector<RolesSynchronizer::ResetRoleTask> RolesSynchronizer::makeResetJobs(const AsyncReplConfigData& oldConfig,
																			   const AsyncReplConfigData& newConfig,
																			   const NsNamesHashSetT& localNamespaces,
																			   const ReplicationConfigData& baseConfig,
																			   const client::ReindexerConfig& clientConfig) {
	std::vector<ResetRoleTask> tasks;
	tasks.reserve(oldConfig.nodes.size());
	for (const auto& follower : oldConfig.nodes) {
		for (const auto& followerDsn : follower.GetDSNs()) {
			const auto& oldNamespaces{follower.Namespaces()->data};
			const auto& replicatedNamespaces{oldNamespaces.empty() ? localNamespaces : oldNamespaces};
			auto newFollowerConfig{newConfig.nodes.end()};
			if (newConfig.role == AsyncReplConfigData::Role::Leader) {
				newFollowerConfig = std::find_if(newConfig.nodes.begin(), newConfig.nodes.end(), [&followerDsn](const auto& follower) {
					const auto& dsns{follower.GetDSNs()};
					return std::find(dsns.begin(), dsns.end(), followerDsn) != dsns.end();
				});
			}
			std::vector<NamespaceName> namespacesToReset;
			if (newFollowerConfig == newConfig.nodes.end()) {
				// The follower was removed, or this node is no longer a leader.
				// Reset roles for all namespaces previously replicated to this follower.
				namespacesToReset.assign(replicatedNamespaces.begin(), replicatedNamespaces.end());
			} else {
				const auto& newNamespaces{newFollowerConfig->Namespaces()->data};
				// An empty list means [*].
				// All namespaces are still replicated, so no reset is needed.
				if (newNamespaces.empty()) {
					continue;
				}
				// Reset only old namespaces not present in the new configuration.
				for (const auto& ns : replicatedNamespaces) {
					if (newNamespaces.count(ns)) {
						continue;
					}
					namespacesToReset.emplace_back(ns);
				}
			}
			if (!namespacesToReset.empty()) {
				tasks.emplace_back(std::move(namespacesToReset), followerDsn, baseConfig.serverID, baseConfig.clusterID, clientConfig);
			}
		}
	}
	return tasks;
}

void RolesSynchronizer::synchronizeNode(const DSN& followerDsn, const ReplicationConfigData& baseConfig,
										const client::ReindexerConfig& clientConfig, const RdxDeadlineContext& deadline,
										const NsNamesHashSetT& excludedNamespaces, RolesSyncNodeStatus& status) noexcept {
	try {
		client::CoroReindexer connection{clientConfig};
		auto stopGuard{MakeScopeGuard([&connection]() noexcept { connection.Stop(); })};
		if (auto err{connection.Connect(followerDsn, loop_, client::ConnectOpts{}.WithExpectedClusterID(baseConfig.clusterID))};
			!err.ok()) {
			status.lastError = std::move(err);
			return;
		}
		auto client{connection.WithContext(&deadline)};
		std::vector<NamespaceDef> followerNamespaces;
		if (auto err{client.EnumNamespaces(followerNamespaces, EnumNamespacesOpts{}.OnlyNames().HideSystem().HideTemporary())}; !err.ok()) {
			status.lastError = std::move(err);
			return;
		}
		const ClusterOperationStatus resetStatus{{baseConfig.serverID, ClusterOperationStatus::Role::None}};
		for (const auto& followerNamespace : followerNamespaces) {
			if (excludedNamespaces.count(followerNamespace.name)) {
				continue;
			}
			if (deadline.GetCancelType() != CancelType::None) {
				status.lastError = Error{errTimeout, "Replication role synchronization timed out"};
				return;
			}
			ReplicationStateV2 state;
			if (auto err{client.GetReplState(followerNamespace.name, state)}; !err.ok()) {
				status.lastError = std::move(err);
				continue;
			}
			if (state.clusterStatus.role != ClusterOperationStatus::Role::SimpleReplica ||
				state.clusterStatus.leaderId != baseConfig.serverID) {
				continue;
			}
			++status.namespacesAttempted;
			if (auto err{client.SetClusterOperationStatus(followerNamespace.name, resetStatus)}; !err.ok()) {
				status.lastError = std::move(err);
				continue;
			}
			++status.namespacesReset;
		}
	} catch (const std::exception& ex) {
		status.lastError = Error{ex};
	}
}

RolesSyncResult RolesSynchronizer::SynchronizeRoles(const ReplicationConfigData& baseConfig, const AsyncReplConfigData& config,
													const NsNamesHashSetT& localNamespaces) noexcept {
	RolesSyncResult result;
	Error error{errSystem, "Unable to start replication role synchronization"};
	size_t node{0};
	try {
		std::vector<const AsyncReplNodeConfig*> followerConfigs;
		result.nodes.reserve(config.nodes.size());
		for (const auto& node : config.nodes) {
			for (const auto& dsn : node.GetDSNs()) {
				result.nodes.emplace_back(RolesSyncNodeStatus{.dsn = dsn, .lastError = {}});
				followerConfigs.emplace_back(&node);
			}
		}
		if (result.nodes.empty()) {
			return result;
		}
		const auto clientConfig{makeCoroReindexerConfig(config)};
		const RdxDeadlineContext deadline{kRolesSyncTimeout};
		LoopTask::Completion completion;
		LoopTask loopTask{
			// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
			[&]() noexcept {
				try {
					for (; node < result.nodes.size(); ++node) {
						// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
						loop_.spawn([&, i = node]() noexcept {
							const auto& followerConfig{*followerConfigs[i]};
							const auto& configuredNamespaces{followerConfig.Namespaces()->data};
							// An empty configured list means [*]: all local namespaces on the leader.
							synchronizeNode(result.nodes[i].dsn, baseConfig, clientConfig, deadline,
											configuredNamespaces.empty() ? localNamespaces : configuredNamespaces, result.nodes[i]);
						});
					}
					error = errOK;
				} catch (const std::exception& ex) {
					error = Error{ex};
				}
				return node != 0;
			},
			&completion,
		};
		tasksExecutor_.addTask(std::move(loopTask));
		completion.wait();
	} catch (const std::exception& ex) {
		error = Error{ex};
	}
	if (!error.ok()) {
		for (size_t i = node; i < result.nodes.size(); ++i) {
			result.nodes[i].lastError = error;
		}
		logFmt(LogError, "Unable to synchronize async replication roles: {}", error.what());
	}
	return result;
}

void RolesSynchronizer::ResetRemovedRoles(const ReplicationConfigData& baseConfig, const AsyncReplConfigData& oldConfig,
										  const AsyncReplConfigData& newConfig, const NsNamesHashSetT& localNamespaces) noexcept {
	try {
		auto resetJobs{makeResetJobs(oldConfig, newConfig, localNamespaces, baseConfig, makeCoroReindexerConfig(oldConfig))};
		if (resetJobs.empty()) {
			return;
		}
		// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
		LoopTask loopTask{[this, jobs = std::move(resetJobs), deadline = RdxDeadlineContext{kRolesResetTimeout}]() noexcept {
			bool scheduled{false};
			try {
				for (const auto& job : jobs) {
					// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
					loop_.spawn([this, &job, &deadline]() noexcept {
						try {
							client::CoroReindexer connection{job.clientConfig};
							auto stopGuard{MakeScopeGuard([&connection]() noexcept { connection.Stop(); })};
							auto err{connection.Connect(job.dsn, loop_, client::ConnectOpts{}.WithExpectedClusterID(job.clusterId))};
							if (!err.ok()) {
								logFmt(LogError, "Unable to connect to '{}' to reset replication roles: {}", job.dsn, err.what());
								return;
							}
							const ClusterOperationStatus resetStatus{{job.serverId, ClusterOperationStatus::Role::None}};
							client::CoroReindexer client{connection.WithContext(&deadline)};
							for (const auto& ns : job.namespaces) {
								if (deadline.GetCancelType() != CancelType::None) {
									logFmt(LogError, "Unable to reset removed async replication roles on '{}': deadline exceeded", job.dsn);
									break;
								}
								if (err = client.SetClusterOperationStatus(ns, resetStatus); !err.ok()) {
									logFmt(LogError, "Unable to reset removed async replication role for '{}' on '{}': {}", ns.ToLower(),
										   job.dsn, err.what());
								}
							}
						} catch (const std::exception& ex) {
							logFmt(LogError, "Unable to reset removed async replication roles on '{}': {}", job.dsn, ex.what());
						}
					});
					scheduled = true;
				}
			} catch (const std::exception& ex) {
				logFmt(LogError, "Unable to schedule async replication role resets: {}", ex.what());
			}
			return scheduled;
		}};
		tasksExecutor_.addTask(std::move(loopTask));
	} catch (const std::exception& err) {
		logFmt(LogError, "Unable to reset removed async replication roles: {}", err.what());
	}
}
}  // namespace cluster
}  // namespace reindexer
