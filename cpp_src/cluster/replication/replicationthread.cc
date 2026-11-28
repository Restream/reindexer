#include <exception>
#include "asyncreplthread.h"
#include "cluster/clustercontrolrequest.h"
#include "cluster/consts.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "clusterreplthread.h"
#include "clustertargetvalidator.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "estl/algorithm.h"
#include "estl/expected.h"
#include "estl/gift_str.h"
#include "ns_sync_scheduler.h"
#include "sharedsyncstate.h"
#include "tools/catch_and_return.h"
#include "tools/scope_guard.h"
#include "updates/updatesqueue.h"
#include "updatesbatcher.h"

namespace reindexer {
namespace cluster {

constexpr static auto kAwaitNsCopyInterval = std::chrono::milliseconds(2000);
constexpr static auto kCoro32KStackSize = 32 * 1024;
// Some large value to avoid endless replicator lock in case of some core issues
constexpr static auto kLocalCallsTimeout = std::chrono::seconds(300);
// Some really large value (considered 'endless') instead of zero or negative timeouts in config
constexpr static auto kLargeDefaultTimeoutSec = 1200;

using updates::ItemReplicationRecord;
using updates::TagsMatcherReplicationRecord;
using updates::IndexReplicationRecord;
using updates::MetaReplicationRecord;
using updates::QueryReplicationRecord;
using updates::SchemaReplicationRecord;
using updates::AddNamespaceReplicationRecord;
using updates::NodeNetworkCheckRecord;
using updates::SaveNewShardingCfgRecord;
using updates::ApplyNewShardingCfgRecord;
using updates::ResetShardingCfgRecord;
using updates::AsyncReplicationCfgRecord;

static bool isNsLifecycleRecord(updates::URType type) noexcept {
	return type == updates::URType::AddNamespace || type == updates::URType::DropNamespace || type == updates::URType::CloseNamespace;
}

ReplThreadConfig::ReplThreadConfig(const ReplicationConfigData& baseConfig, const AsyncReplConfigData& config) {
	AppName = config.appName;
	EnableCompression = config.enableCompression;
	UpdatesTimeoutSec = (config.onlineUpdatesTimeoutSec > 0) ? config.onlineUpdatesTimeoutSec : kLargeDefaultTimeoutSec;
	RetrySyncIntervalMSec = config.retrySyncIntervalMSec;
	ParallelSyncsPerThreadCount = config.parallelSyncsPerThreadCount;
	BatchingRoutinesCount = config.batchingRoutinesCount > 0 ? size_t(config.batchingRoutinesCount) : 100;
	MaxWALDepthOnForceSync = config.maxWALDepthOnForceSync;
	ForceSyncOnLogicError = config.forceSyncOnLogicError;
	SyncTimeoutSec = std::max(config.syncTimeoutSec, config.onlineUpdatesTimeoutSec);
	if (SyncTimeoutSec <= 0) {
		SyncTimeoutSec = kLargeDefaultTimeoutSec;
	}
	ClusterID = baseConfig.clusterID;
	LeaderReplToken = config.selfReplToken;
	if (config.onlineUpdatesDelayMSec > 0) {
		OnlineUpdatesDelaySec = double(config.onlineUpdatesDelayMSec) / 1000.;
	} else if (config.onlineUpdatesDelayMSec == 0) {
		OnlineUpdatesDelaySec = 0;
	} else {
		OnlineUpdatesDelaySec = 0.1;
	}
}

ReplThreadConfig::ReplThreadConfig(const ReplicationConfigData& baseConfig, const ClusterConfigData& config) {
	AppName = config.appName;
	EnableCompression = config.enableCompression;
	UpdatesTimeoutSec = (config.onlineUpdatesTimeoutSec > 0) ? config.onlineUpdatesTimeoutSec : kLargeDefaultTimeoutSec;
	RetrySyncIntervalMSec = config.retrySyncIntervalMSec;
	ParallelSyncsPerThreadCount = config.parallelSyncsPerThreadCount;
	ClusterID = baseConfig.clusterID;
	ForceSyncOnLogicError = true;  // Always true for sync cluster
	MaxWALDepthOnForceSync = config.maxWALDepthOnForceSync;
	SyncTimeoutSec = std::max(config.syncTimeoutSec, config.onlineUpdatesTimeoutSec);
	if (SyncTimeoutSec <= 0) {
		SyncTimeoutSec = kLargeDefaultTimeoutSec;
	}
	BatchingRoutinesCount = config.batchingRoutinesCount > 0 ? size_t(config.batchingRoutinesCount) : 100;
	OnlineUpdatesDelaySec = 0;
}

namespace repl_thread_impl {
void NamespaceData::UpdateLsnOnRecord(const updates::UpdateRecord& rec) {
	if (!rec.IsDbRecord()) {
		// Updates with *Namespace types have fake lsn. Those updates should not be count in latestLsn
		latestLsn = rec.ExtLSN();
	} else if (rec.Type() == updates::URType::AddNamespace) {
		if (latestLsn.NsVersion().isEmpty() || latestLsn.NsVersion().Counter() < rec.ExtLSN().NsVersion().Counter()) {
			latestLsn = ExtendedLsn(rec.ExtLSN().NsVersion(), lsn_t());
		}
	} else if (rec.Type() == updates::URType::DropNamespace) {
		latestLsn = ExtendedLsn();
	}
}

void Node::Reconnect(net::ev::dynamic_loop& loop, const ReplThreadConfig& config) {
	Disconnect();
	const auto opts =
		client::ConnectOpts().CreateDBIfMissing().WithExpectedClusterID(config.ClusterID).WithReplicationSender(replicationSender);
	// Error will be checked during the further requests
	std::ignore = client.Connect(dsn, loop, opts);
}

void Node::Disconnect() noexcept {
	if (connObserverId.has_value()) {
		std::ignore = client.RemoveConnectionStateObserver(*connObserverId);
		connObserverId.reset();
	}
	client.Stop();
}
}  // namespace repl_thread_impl

template <typename BehaviourParamT>
bool UpdateApplyStatus::NeedsSingleNsResync() const noexcept {
	static_assert(std::is_same_v<BehaviourParamT, AsyncThreadParam> || std::is_same_v<BehaviourParamT, ClusterThreadParam>,
				  "Unexpected param type");
	if constexpr (std::is_same_v<BehaviourParamT, ClusterThreadParam>) {
		return type == updates::URType::ResyncNamespaceGeneric;
	} else {
		return type == updates::URType::ResyncNamespaceGeneric || type == updates::URType::ResyncNamespaceLeaderInit;
	}
}

template <typename BehaviourParamT>
bool UpdateApplyStatus::IsHaveToResync() const noexcept {
	return NeedsSingleNsResync<BehaviourParamT>() || type == updates::URType::ResyncOnUpdatesDrop;
}

template <typename BehaviourParamT>
ReplThread<BehaviourParamT>::ReplThread(int serverId, ReindexerImpl& _thisNode, std::shared_ptr<UpdatesQueueT> shard,
										std::shared_ptr<NamespacesSyncScheduler> nssSyncScheduler, BehaviourParamT&& bhvParam,
										ReplicationStatsCollector statsCollector, const Logger& l)
	: thisNode(_thisNode),
	  serverId_(serverId),
	  nssSyncScheduler_(std::move(nssSyncScheduler)),
	  bhvParam_(std::move(bhvParam)),
	  updates_(std::move(shard)),
	  statsCollector_(statsCollector),
	  log_(l) {
	assertrx(updates_);
	assertrx(nssSyncScheduler_);
	auto spawnNotifierRoutine = [this] {
		loop.spawn(
			wg,
			[this]() noexcept {
				while (hasPendingNotificaions_) {
					hasPendingNotificaions_ = false;
					if (!terminate_.load(std::memory_order_relaxed)) {
						updatesNotifier();
						if (terminate_.load(std::memory_order_relaxed)) {
							terminateNotifier();
						}
					} else {
						terminateNotifier();
					}
				}
				notificationInProgress_ = false;
			},
			kCoro32KStackSize);
	};
	updatesTimer_.set(loop);
	// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
	updatesTimer_.set([this, spawnNotifierRoutine](net::ev::timer&, int) noexcept {
		logTrace("{}: new updates notification (on timer)", serverId_);
		spawnNotifierRoutine();
	});
	updatesAsync_.set(loop);
	// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
	updatesAsync_.set([this, spawnNotifierRoutine](net::ev::async&) noexcept {
		hasPendingNotificaions_ = true;
		if (!terminate_.load(std::memory_order_relaxed)) {
			if (!notificationInProgress_) {
				notificationInProgress_ = true;
				if (config_.OnlineUpdatesDelaySec > 0) {
					logTrace("{}: new updates notification (delaying...)", serverId_);
					updatesTimer_.start(config_.OnlineUpdatesDelaySec);
				} else {
					logTrace("{}: new updates notification (on async)", serverId_);
					spawnNotifierRoutine();
				}
			}
		} else {
			notificationInProgress_ = true;
			logTrace("{}: new terminate notification", serverId_);
			spawnNotifierRoutine();
		}
	});
}

template <typename BehaviourParamT>
template <typename NodeConfigT>
void ReplThread<BehaviourParamT>::Run(ReplThreadConfig config, const std::vector<std::pair<uint32_t, NodeConfigT>>& nodesList,
									  size_t consensusCnt, size_t requiredReplicas) {
	config_ = std::move(config);
	consensusCnt_ = consensusCnt;
	requiredReplicas_ = requiredReplicas;

	// TODO: Currently there are no good ways to recover, crash is intended
	// NOLINTNEXTLINE(bugprone-exception-escape,rx-perf-lambda-to-std-function-allocation)
	loop.spawn([this, &nodesList, funcName = __FUNCTION__]() noexcept {
		nodes.clear();
		if (config_.ParallelSyncsPerThreadCount > 0) {
			nsSyncTokens_ = std::make_unique<coroutine::tokens_pool<bool>>(config_.ParallelSyncsPerThreadCount);
		} else {
			nsSyncTokens_.reset();
		}
		client::ReindexerConfig rpcCfg;
		rpcCfg.RequestDedicatedThread = true;
		rpcCfg.AppName = config_.AppName;
		rpcCfg.NetTimeout = std::chrono::seconds(config_.UpdatesTimeoutSec);
		rpcCfg.EnableCompression = config_.EnableCompression;
		rpcCfg.ReplToken = config_.LeaderReplToken;
		for (const auto& [uid, nodeCfg] : nodesList) {
			if constexpr (isClusterReplThread()) {
				auto& node = nodes.emplace_back(nodeCfg.GetServerID(), uid, rpcCfg);
				node.dsn = nodeCfg.GetRPCDsn();
				node.replicationSender.clusterLeaderId = serverId_;
			} else {
				// Async targets have no server ID
				auto& node = nodes.emplace_back(-1, uid, rpcCfg);
				if (nodeCfg.IsClusterTarget()) {
					node.replicationSender.clusterTarget = true;
					node.clusterDsns = nodeCfg.GetDSNs();
				} else {
					node.dsn = nodeCfg.GetDSNs().front();
				}
			}
		}

		bhvParam_.AwaitReplPermission();
		if (!terminate_) {
			log_.Info(
				[this] {
					std::string nodesString;
					for (size_t i = 0; i < nodes.size(); ++i) {
						if (i > 0) {
							nodesString.append(", ");
						}
						nodesString.append(fmt::format("Node {} - server ID {}", nodes[i].uid, nodes[i].serverId));
					}
					rtfmt("{}: starting dataReplicationThread. Nodes:'{}'", serverId_, nodesString);
				},
				__FILE__, __LINE__, funcName);
			updates_->AddDataNotifier(std::this_thread::get_id(), [this] { updatesAsync_.send(); });

			for (size_t i = 0; i < nodes.size(); ++i) {
				// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
				loop.spawn(wg, [this, i]() noexcept {
					// 3) Perform wal-sync/force-sync for each follower
					nodeReplicationRoutine(nodes[i]);  // NOLINT(rx-safety-noexcept-throw-call)
				});
			}
			// Await termination
			if (!terminateCh_.opened()) {
				terminateCh_.reopen();
			}
			std::ignore = terminateCh_.pop();
		}

		wg.wait();
		updatesAsync_.stop();
		updatesTimer_.stop();
		wg.wait();
	});

	updatesAsync_.start();

	loop.run();

	updates_->RemoveDataNotifier(std::this_thread::get_id());
	for (auto& node : nodes) {
		node.updateNotifier->close();
	}
	terminateCh_.close();
	nodes.clear();

	logInfo("{}: Replication thread was terminated. TID: {}", serverId_,
			static_cast<size_t>(std::hash<std::thread::id>()(std::this_thread::get_id())));
}

template <typename BehaviourParamT>
void ReplThread<BehaviourParamT>::SetTerminate(bool val) noexcept {
	terminate_ = val;
	if (val) {
		updatesAsync_.send();
	}
}

template <typename BehaviourParamT>
void ReplThread<BehaviourParamT>::DisconnectNodes() {
	coroutine::wait_group swg;
	for (auto& node : nodes) {
		loop.spawn(swg, [&node]() noexcept { node.Disconnect(); }, k16kCoroStack);
	}
	swg.wait();
}

template <typename BehaviourParamT>
constexpr bool ReplThread<BehaviourParamT>::isClusterReplThread() noexcept {
	static_assert(std::is_same_v<BehaviourParamT, AsyncThreadParam> || std::is_same_v<BehaviourParamT, ClusterThreadParam>,
				  "Unexpected param type");
	if constexpr (std::is_same_v<BehaviourParamT, ClusterThreadParam>) {
		return true;
	} else {
		return false;
	}
}

template <>
void ReplThread<AsyncThreadParam>::updateNodeStatus(size_t uid, NodeStats::Status st) {
	statsCollector_.OnStatusChanged(uid, st);
}
template <>
void ReplThread<ClusterThreadParam>::updateNodeStatus(size_t, NodeStats::Status) {}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::connectToClusterTargetLeader(Node& node) noexcept {
	try {
		const auto& dsns = node.clusterDsns;
		const auto leaderIdx = ValidateClusterTarget(loop, config_, dsns);
		if (!leaderIdx) {
			return leaderIdx.error();
		}
		node.dsn = dsns[*leaderIdx];
		node.Reconnect(loop, config_);
		// Validator's sessions are closed already, so the replicating session is checked too
		if (auto err = CheckClusterTargetCompatibility(node.client, node.dsn); !err.ok()) {
			return err;
		}
		node.isLeaderValidated = true;
		statsCollector_.OnActiveDsnChanged(node.uid, *leaderIdx);
		logInfo("{}:{} Replicating to the secondary cluster's leader '{}'", serverId_, node.uid, node.dsn);
	} CATCH_AND_RETURN
	return Error();
}

template <typename BehaviourParamT>
bool ReplThread<BehaviourParamT>::isClusterTargetLeader(Node& node) noexcept {
	RaftInfo info;
	const auto err = node.client.WithTimeout(kStatusCmdTimeout).GetRaftInfo(info);
	if (err.ok() && info.role == RaftInfo::Role::Leader) {
		return true;
	}
	logInfo("{}:{} '{}' is not the secondary cluster's leader anymore ({})", serverId_, node.uid, node.dsn,
			err.ok() ? RaftInfo::RoleToStr(info.role) : std::string_view(err.whatStr()));
	return false;
}

template <typename BehaviourParamT>
void ReplThread<BehaviourParamT>::invalidateClusterTargetLeader(Node& node) noexcept {
	if (std::exchange(node.isLeaderValidated, false)) {
		statsCollector_.OnActiveDsnChanged(node.uid, std::nullopt);
	}
}

template <>
bool ReplThread<AsyncThreadParam>::isReplPermissionChanged(const Node& node) const noexcept {
	return bhvParam_.GetReplPermission().epoch != node.replPermissionEpoch;
}
template <>
bool ReplThread<ClusterThreadParam>::isReplPermissionChanged(const Node&) const noexcept {
	return false;
}

// The suspended node does not keep the queued updates: they are covered by the full sync on the resume
template <>
void ReplThread<AsyncThreadParam>::discardSuspendedUpdates(Node& node) {
	// The round ends with the empty read only, which subscribes the thread to the new updates
	while (!terminate_) {
		auto updatePtr = updates_->Read(node.nextUpdateId, std::this_thread::get_id());
		if (!updatePtr) {
			break;
		}
		if (updatePtr->IsUpdatesDropBlock) {
			node.nextUpdateId = updatePtr->ID() + 1;
			continue;
		}
		node.nextUpdateId = std::max(updatePtr->ID(), node.nextUpdateId);
		// Invalidated block is read again as the updates drop block
		for (uint16_t offset = node.nextUpdateId - updatePtr->ID(); offset < updatePtr->Count() && !updatePtr->IsInvalidated(); ++offset) {
			++node.nextUpdateId;
			const auto& nsName = updatePtr->GetUpdate(offset).Data().NsName();
			if (!nsName.empty() && bhvParam_.IsNamespaceInConfig(node.uid, nsName)) {
				// Reconciliation on the resume drops the namespace on the target, if it is absent here
				std::ignore = node.NsData(nsName);
			}
			// Async updates have no results to await, but their erasure requires the emitter's flag
			constexpr bool kIsEmitter = true;
			std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset, kIsEmitter, Error());
		}
	}
}

// Async replication of the node, which is not allowed to replicate (e.g. is not the RAFT leader), is suspended without the connection,
// until the permission is given. Returns true, if the replication has been suspended
template <>
bool ReplThread<AsyncThreadParam>::suspendWhileForbidden(Node& node) {
	auto permission = bhvParam_.GetReplPermission();
	node.SetReplPermissionEpoch(permission.epoch);
	if (permission.error.ok()) {
		return false;
	}
	logInfo("{}:{} Replication is suspended: {}", serverId_, node.uid, permission.error.whatStr());
	invalidateClusterTargetLeader(node);
	node.Disconnect();
	updateNodeStatus(node.uid, NodeStats::Status::None);
	statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::Suspended);
	statsCollector_.SaveNodeError(node.uid, Error());
	while (!terminate_) {
		discardSuspendedUpdates(node);
		permission = bhvParam_.GetReplPermission();
		if (permission.error.ok()) {
			node.SetReplPermissionEpoch(permission.epoch);
			logInfo("{}:{} Replication is resumed", serverId_, node.uid);
			break;
		}
		if (!node.updateNotifier->pop().second) {
			break;
		}
	}
	return true;
}

template <>
bool ReplThread<AsyncThreadParam>::awaitReplPermission(Node& node) {
	const bool wasSuspended = suspendWhileForbidden(node);
	statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::AwaitingResync);
	return wasSuspended;
}
template <>
bool ReplThread<ClusterThreadParam>::awaitReplPermission(Node& node) {
	statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::AwaitingResync);
	// Is unknown until the push. Is reset before awaiting the leadership, so the previous term's values are not shown
	statsCollector_.SaveNodeAsyncReplConfig(node.uid, AsyncReplConfigStats());
	bhvParam_.AwaitReplPermission();
	return false;
}

template <typename BehaviourParamT>
void ReplThread<BehaviourParamT>::nodeReplicationRoutine(Node& node) {
	Error err;
	bool expectingReconnect = true;
	while (!terminate_) {
		const bool wasSuspended = awaitReplPermission(node);
		if (terminate_) {
			break;
		}
		if (wasSuspended) {
			logInfo("{}:{} Reconnecting... Reason: Replication is resumed", serverId_, node.uid);
			node.Reconnect(loop, config_);
		} else if (expectingReconnect && (!err.ok() || !node.client.WithTimeout(kStatusCmdTimeout).Status(true).ok())) {
			logInfo("{}:{} Reconnecting... Reason: {}", serverId_, node.uid, err.ok() ? "Not connected yet" : ("Error: " + err.whatStr()));
			node.Reconnect(loop, config_);
		}
		err = prepareReplicationTarget(node);
		statsCollector_.SaveNodeError(node.uid, err);  // Resets the last error, if the target is fine
		expectingReconnect = err.ok();
		if (err.ok()) {
			err = nodeReplicationImpl(node);
			statsCollector_.SaveNodeError(node.uid, err);
		} else {
			logError("{}:{} Replication is not allowed: {}", serverId_, node.uid, err.whatStr());
		}
		// Errors after the change of the permission are not the target's ones: the replication is suspended or resynced at once
		if (terminate_ || isReplPermissionChanged(node)) {
			continue;
		}
		if (err.ok()) {
			logTrace("{}:{} Doing resync...", serverId_, node.uid);
			continue;
		}
		if (node.isLeaderValidated && !isClusterTargetLeader(node)) {
			invalidateClusterTargetLeader(node);
		}
		bhvParam_.OnNodeBecameUnsynchonized(node.uid);
		updateNodeStatus(node.uid, NodeStats::Status::Offline);
		statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::AwaitingResync);
		awaitSyncRetry(node, err);
	}
	if (terminate_) {
		logTrace("{}:{} Node replication routine was terminated", serverId_, node.uid);
	}
	node.Disconnect();
}

template <typename BehaviourParamT>
void ReplThread<BehaviourParamT>::awaitSyncRetry(Node& node, const Error& err) {
	constexpr auto kGranularSleepInterval = std::chrono::milliseconds(150);
	auto awaitTime = isTxCopyError(err) ? kAwaitNsCopyInterval : std::chrono::milliseconds(config_.RetrySyncIntervalMSec);
	while (!terminate_ && awaitTime.count() > 0) {
		const auto diff = std::min(awaitTime, kGranularSleepInterval);
		awaitTime -= diff;
		loop.sleep(diff);
		if (isTimeoutError(err) || isReplPermissionChanged(node)) {
			return;
		}
		if ((isNodeUnavailableError(err) || isLeaderChangedError(err)) && handleUpdatesWithError(node, err)) {
			return;
		}
	}
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::addConnectionObserver(Node& node) noexcept {
	try {
		if (node.connObserverId.has_value()) {
			return Error();
		}
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		auto connObserverId = node.client.AddConnectionStateObserver([this, &node](const Error& err) noexcept {
			if (!err.ok() && updates_ && !terminate_) {
				logInfo("{}:{} Connection error: {}", serverId_, node.uid, err.whatStr());
				// The client reconnects transparently, while the leader may be restarted with the incompatible version
				invalidateClusterTargetLeader(node);
				UpdatesContainer recs;
				recs.emplace_back(updates::URType::NodeNetworkCheck, node.uid, false);
				node.requireResync = true;
				std::pair<Error, bool> res = updates_->template PushAsync<true>(std::move(recs));
				if (!res.first.ok()) {
					logWarn("Error while Pushing updates queue: {}", res.first.what());
				}
			}
		});
		if (!connObserverId.has_value()) {
			return connObserverId.error();
		}
		node.connObserverId = connObserverId.value();
		return Error();
	} CATCH_AND_RETURN
}

template <>
Error ReplThread<ClusterThreadParam>::syncShardingConfig(Node& node) noexcept {
	///////////////////////////////  ATTENTION!  /////////////////////////////////
	///////// This	 specialization	  	is	 necessary because clang-tyde ////////
	///////// falsely 	diagnoses 	the private	member access error here, ////////
	///////// despite the fact that  this code  is  under 'if constexpr'. ////////
	//////////////////////////////////////////////////////////////////////////////
	///////// This specialization should be located up to the point of use ///////
	///////// in function  `nodeReplicationImpl` in order to avoid 	IFNDR. ///////
	//////////////////////////////////////////////////////////////////////////////
	try {
		for (size_t i = 0; i < kMaxRetriesOnRoleSwitchAwait; ++i) {
			if (terminate_) [[unlikely]] {
				return Error(errCanceled, "{}:{} Terminated", serverId_, node.uid);
			}
			ReplicationStateV2 replState;
			auto err = node.client.GetReplState(std::string_view(), replState);

			if (!bhvParam_.IsLeader()) {
				return Error(errParams, "Leader was switched");
			}

			if (!err.ok()) {
				logWarn("{}:{} Unable to get repl state: {}", serverId_, node.uid, err.whatStr());
				return err;
			}

			statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::Syncing);
			updateNodeStatus(node.uid, NodeStats::Status::Online);
			if (replState.clusterStatus.role != ClusterOperationStatus::Role::ClusterReplica ||
				replState.clusterStatus.leaderId != serverId_) {
				// Await transition
				logTrace("{}:{} Awaiting role switch on the remote node", serverId_, node.uid);
				loop.sleep(kRoleSwitchStepTime);
				// TODO: Check if cluster is configured on remote node
				continue;
			}

			logInfo("{}:{} Start applying leader's sharding config locally", serverId_, node.uid);
			std::string config;
			std::optional<int64_t> sourceId;
			if (auto configPtr = thisNode.shardingConfig_.Get()) {
				config = configPtr->GetJSON(cluster::MaskingDSN::Disabled);
				sourceId = configPtr->sourceId;
			}

			sharding::ShardingControlResponseData res;
			return node.client.WithLSN(lsn_t(0, serverId_))
				.ShardingControlRequest({sharding::ControlCmdType::ApplyLeaderConfig, config, std::move(sourceId)}, res);
		}
		return Error(errTimeout, "{}:{} DB role switch waiting timeout", serverId_, node.uid);
	} CATCH_AND_RETURN
}

template <>
Error ReplThread<ClusterThreadParam>::sendAsyncReplicationConfig(Node& node, const AsyncReplConfigSource& source,
																 std::optional<int64_t>& appliedSourceId) noexcept {
	try {
		BindingCapabilities caps;
		if (auto err = node.client.GetServerCapabilities(caps); !err.ok()) {
			return err;
		}
		if (!caps.HasAsyncClusterReplication()) {
			logWarn("{}:{} Follower does not support leader's async replication config", serverId_, node.uid);
			// Older nodes do not keep the source and vote without it on the elections
			appliedSourceId = ShardingSourceId::NotSet;
			return Error();
		}
		ClusterControlResponseData response;
		auto err = node.client.WithLSN(lsn_t(0, serverId_))
					   .ClusterControlRequest(ClusterControlRequestData{ApplyLeaderAsyncReplConfigCommand{source}}, response);
		if (err.ok() && response.type == ClusterControlRequestData::Type::ApplyLeaderAsyncReplConfig) {
			appliedSourceId = std::get<ApplyLeaderAsyncReplConfigCommand>(response.data).sourceId;
		}
		return err;
	} CATCH_AND_RETURN
}

template <>
Error ReplThread<ClusterThreadParam>::applyAsyncReplicationConfig(Node& node, const AsyncReplConfigSource& source) noexcept {
	AsyncReplConfigStats stats;
	stats.applyError = sendAsyncReplicationConfig(node, source, stats.sourceId);
	statsCollector_.SaveNodeAsyncReplConfig(node.uid, stats);
	return stats.applyError;
}

template <>
Error ReplThread<ClusterThreadParam>::syncAsyncReplicationConfig(Node& node) noexcept {
	try {
		node.asyncConfigPushedAtQueueId = 0;
		if (auto rejection = bhvParam_.CheckAsyncReplConfigAccepted(); !rejection.ok()) {
			logWarn("{}:{} Async replication config is not sent: {}", serverId_, node.uid, rejection.whatStr());
			return Error();
		}
		// Is read before the config: the action publishes its config before queueing it
		const uint64_t queueId = updates_->GetNextUpdateID();
		auto err = applyAsyncReplicationConfig(node, *thisNode.asyncReplConfig_.GetSource());
		if (err.ok()) {
			node.asyncConfigPushedAtQueueId = queueId;
		}
		return err;
	} CATCH_AND_RETURN
}

template <>
Error ReplThread<ClusterThreadParam>::setNamespaceOwnership(Node& node, std::string_view nsName, IsPrimary isPrimary) noexcept {
	try {
		BindingCapabilities caps;
		if (auto err = node.client.GetServerCapabilities(caps); !err.ok()) {
			return err;
		}
		// Older followers would apply the whole cluster operation status
		if (!caps.HasAsyncClusterReplication()) {
			return Error(errVersion, "Follower does not support the namespaces, replicated from the primary cluster");
		}
		logTrace("{}:{}:{} Setting namespace ownership on follower: is_primary={}", serverId_, node.uid, nsName, *isPrimary);
		return node.client.WithLSN(lsn_t(0, serverId_)).SetClusterOperationStatus(nsName, ClusterOperationStatus{{}, *isPrimary});
	} CATCH_AND_RETURN
}

template <>
Error ReplThread<ClusterThreadParam>::syncNamespaceOwnership(Node& node, std::string_view nsName, IsPrimary isPrimaryOnFollower) noexcept {
	try {
		ReplicationStateV2 localState;
		const RdxDeadlineContext deadlineCtx(kLocalCallsTimeout, &terminateCtx_);
		auto err = thisNode.GetReplState(nsName, localState, RdxContext().WithCancelCtx(deadlineCtx));
		if (!err.ok()) {
			return err.code() == errNotFound ? Error() : err;
		}
		const IsPrimary isPrimary{localState.clusterStatus.isPrimary};
		return isPrimary == isPrimaryOnFollower ? Error() : setNamespaceOwnership(node, nsName, isPrimary);
	} CATCH_AND_RETURN
}

template <typename BehaviourParamT>
bool ReplThread<BehaviourParamT>::needForceSyncOnLogicError(const Error& err) const noexcept {
	return config_.ForceSyncOnLogicError && !isNetworkError(err) && !isTimeoutError(err);
}

template <typename BehaviourParamT>
Expected<std::vector<NamespaceDef>> ReplThread<BehaviourParamT>::generateSyncNssList(const Node& node) const noexcept {
	std::vector<NamespaceDef> nsList;
	logTrace("{}:{} Trying to collect local namespaces...", serverId_, node.uid);
	const RdxDeadlineContext deadlineCtx(kLocalCallsTimeout, &terminateCtx_);
	auto err = thisNode.EnumNamespaces(nsList, EnumNamespacesOpts().OnlyNames().HideSystem().HideTemporary(),
									   RdxContext().WithCancelCtx(deadlineCtx));
	if (!err.ok()) {
		logWarn("{}:{} Unable to enum local namespaces in node replication routine: {}", serverId_, node.uid, err.what());
		return Unexpected(std::move(err));
	}
	nsList.erase(unstable_remove_if(nsList.begin(), nsList.end(),
									[this, &node](const auto& ns) { return !bhvParam_.IsNamespaceInConfig(node.uid, ns.name); }),
				 nsList.cend());
	return nsList;
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::namespacesSyncImpl(Node& node, std::span<const NamespaceDef> nsList, SyncMode syncMode) {
	std::set<NamespaceName> nssToSync;
	for (const auto& ns : nsList) {
		nssToSync.emplace(ns.name);
	}

	struct [[nodiscard]] RoutineContext {
		coroutine::tokens_pool<bool>::token syncToken;
		NamespacesSyncScheduler::Guard ns;
	};

	coroutine::wait_group localWg;
	Error integralError;
	while (!nssToSync.empty() && integralError.ok()) {
		auto routineCtx = std::make_shared<RoutineContext>();
		// Get sync token
		if (nsSyncTokens_) {
			logTrace("{}:{} Awaiting sync token", serverId_, node.uid);
			routineCtx->syncToken = nsSyncTokens_->await_token();
			logTrace("{}:{} Got sync token", serverId_, node.uid);
		}
		if (!integralError.ok()) {
			break;
		}

		const bool isFirstNs = (nssToSync.size() == nsList.size());
		routineCtx->ns = nssSyncScheduler_->ScheduleNext(nssToSync);
		[[maybe_unused]] auto removed = nssToSync.erase(routineCtx->ns.Name());
		assertrx_dbg(removed == 1);

		// Delay allows other routines to get tokens for better rearranging
		if (isFirstNs && nsList.size() > 1) {
			loop.sleep(std::chrono::milliseconds(10));
		} else {
			loop.yield();
		}

		logInfo("{}:{} Creating new sync routine to sync '{}'", serverId_, node.uid, routineCtx->ns.Name());
		// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
		loop.spawn(localWg, [this, &integralError, &node, syncMode, routineCtx = std::move(routineCtx)]() mutable noexcept {
			// 3.1) Perform wal-sync/force-sync for specified namespace in separated routine
			auto err = namespaceSyncRoutine(node, routineCtx->ns.Name(), syncMode, integralError);
			if (!err.ok() && integralError.ok()) {
				integralError = std::move(err);
			}
			if (integralError.ok()) {
				statsCollector_.OnNamespaceSynchronized(node.uid);
			}
		});
	}

	localWg.wait();
	return integralError;
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::namespaceSyncRoutine(Node& node, const NamespaceName& nsName, SyncMode syncMode,
														const Error& integralError) noexcept {
	try {
		auto logGrd = MakeScopeGuard([&] { logInfo("{}:{} Sync routine for '{}' was completed", serverId_, node.uid, nsName); });
		ReplicationStateV2 replState;
		for (size_t i = 0; i < kMaxRetriesOnRoleSwitchAwait && integralError.ok(); ++i) {
			if (terminate_) [[unlikely]] {
				return Error(errCanceled, "{}:{}:{} Terminated", serverId_, node.uid, nsName);
			}
			auto err = node.client.GetReplState(nsName, replState);
			bool nsExists = true;
			if (err.code() == errNotFound) {
				nsExists = false;
				logInfo("{}:{} Namespace does not exist on remote node. Trying to get repl state for whole DB", serverId_, node.uid);
				err = node.client.GetReplState(std::string_view(), replState);
			}
			if (!bhvParam_.IsLeader() && integralError.ok()) {
				return Error(errParams, "Leader was switched");
			} else if (!integralError.ok()) {
				return integralError;
			}
			if (err.ok()) {
				statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::Syncing);
				updateNodeStatus(node.uid, NodeStats::Status::Online);
				if constexpr (isClusterReplThread()) {
					if (replState.clusterStatus.role != ClusterOperationStatus::Role::ClusterReplica ||
						replState.clusterStatus.leaderId != serverId_) {
						// Await transition
						logTrace("{}:{} Awaiting NS role switch on remote node", serverId_, node.uid);
						loop.sleep(kRoleSwitchStepTime);
						// TODO: Check if cluster is configured on remote node
						continue;
					}
				} else if (nsExists) {
					err = markExistingRemoteNs(node, nsName, replState);
				}
			} else {
				logWarn("{}:{} Unable to get repl state: {}", serverId_, node.uid, err.whatStr());
			}

			if (err.ok()) {
				if (!nsExists) {
					replState = ReplicationStateV2();
				}
				err = syncNamespace(node, nsName, replState, syncMode);
				if (!err.ok()) {
					logWarn("{}:{}:{} Namespace sync error: {}", serverId_, node.uid, nsName, err.whatStr());
					if (err.code() == errChecksumMismatch || needForceSyncOnLogicError(err)) {
						replState = ReplicationStateV2();
						err = syncNamespace(node, nsName, replState, syncMode);
						if (!err.ok()) {
							logWarn("{}:{}:{} Namespace sync error (resync due to {}): {}", serverId_, node.uid, nsName,
									err.code() == errChecksumMismatch ? "checksum mismatch" : "logic error", err.whatStr());
						}
					}
				}
				if constexpr (isClusterReplThread()) {
					if (err.ok()) {
						err = syncNamespaceOwnership(node, nsName, IsPrimary(replState.clusterStatus.isPrimary));
					}
				}
			}
			return err;
		}
		return Error(errTimeout, "{}:{}:{} Unable to sync namespace", serverId_, node.uid, nsName);
	} CATCH_AND_RETURN
}

// Async replication only: makes the existing remote namespace a replica of this node and renews its replication state
template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::markExistingRemoteNs(Node& node, std::string_view nsName, ReplicationStateV2& replState) noexcept {
	try {
		Error err;
		if (node.IsClusterTarget()) {
			if (!replState.clusterStatus.isPrimary) {
				return Error();
			}
			logInfo("{}:{} Taking ownership of '{}' on the secondary cluster", serverId_, node.uid, nsName);
			err = node.client.WithLSN(lsn_t(0, serverId_)).SetClusterOperationStatus(nsName, ClusterOperationStatus{{}, false});
		} else {
			if (replState.clusterStatus.role == ClusterOperationStatus::Role::SimpleReplica &&
				replState.clusterStatus.leaderId == serverId_) {
				return Error();
			}
			logTrace("{}:{} Switching role for '{}' on remote node", serverId_, node.uid, nsName);
			err = node.client.WithEmitterServerId(serverId_).SetClusterOperationStatus(
				nsName, ClusterOperationStatus{{serverId_, ClusterOperationStatus::Role::SimpleReplica}});
		}
		if (!err.ok()) {
			return err;
		}
		return node.client.GetReplState(nsName, replState);
	} CATCH_AND_RETURN
}

// Async replication only: makes the namespace, just created on the remote node by this node, a replica of this node
template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::markNewRemoteNs(Node& node, client::CoroReindexer& client, std::string_view nsName) noexcept {
	// Namespaces of the cluster target are created in the non-primary state
	if (node.IsClusterTarget()) {
		return Error();
	}
	return client.WithEmitterServerId(serverId_).SetClusterOperationStatus(
		nsName, ClusterOperationStatus{{serverId_, ClusterOperationStatus::Role::SimpleReplica}});
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::nodeReplicationImpl(Node& node) {
	if (auto err = addConnectionObserver(node); !err.ok()) {
		return err;
	}
	const auto syncNss = generateSyncNssList(node);
	if (!syncNss) {
		return syncNss.error();
	}
	const std::vector<NamespaceDef>& nsList = *syncNss;
	statsCollector_.OnEnqueueNamespacesSync(node.uid, nsList.size());
	node.requireResync = false;
	logTrace("{}:{} Performing ns data reconciliation...", serverId_, node.uid);
	NsNamesHashSetT openOnLeader(nsList.size());
	for (const auto& ns : nsList) {
		openOnLeader.emplace(ns.name);
	}
	for (auto nsDataIt = node.NsData().cbegin(); nsDataIt != node.NsData().cend();) {
		if (nsDataIt->first.empty()) {
			assertrx_dbg(false);
			++nsDataIt;
			continue;
		}
		rollbackNamespaceTx(node, nsDataIt->first);
		if (openOnLeader.find(nsDataIt->first) != openOnLeader.end()) {
			nsDataIt = node.EraseNsData(nsDataIt);
			continue;
		}
		ReindexerImpl::NamespacePresence presence{};
		const RdxDeadlineContext deadlineCtx(kLocalCallsTimeout, &terminateCtx_);
		if (auto err = thisNode.GetNamespacePresence(nsDataIt->first, presence, RdxContext().WithCancelCtx(deadlineCtx)); !err.ok()) {
			return err;
		}
		switch (presence) {
			case ReindexerImpl::NamespacePresence::Open:
				nsDataIt = node.EraseNsData(nsDataIt);
				break;
			case ReindexerImpl::NamespacePresence::ClosedWithStorage: {
				auto& nsData = node.NsData(nsDataIt->first);
				nsData.isClosed = true;
				nsData.requiresTmUpdate = true;
				++nsDataIt;
			} break;
			case ReindexerImpl::NamespacePresence::Absent: {
				// Lost Drop (or never-created ns). Cluster forbids Close without drop, so the same
				// reconstruction applies to both async and RAFT threads.
				// Local namespaces of the former leader may be outdated, e.g. the namespace may be recreated by the new one
				if (!bhvParam_.IsLeader()) {
					return Error(errParams, "Leader was switched");
				}
				logInfo("{}:{}:{} Namespace is absent on leader. Dropping it on follower (reconciliation)", serverId_, node.uid,
						nsDataIt->first);
				auto err = node.client.WithLSN(lsn_t(0, serverId_)).DropNamespace(nsDataIt->first);
				if (!err.ok() && err.code() != errNotFound) {
					logWarn("{}:{}:{} Unable to drop absent namespace on follower: {}", serverId_, node.uid, nsDataIt->first, err.what());
					return err;
				}
				nsDataIt = node.EraseNsData(nsDataIt);
				loop.yield();
				break;
			}
			default:
				std::abort();
		}
	}

	if constexpr (isClusterReplThread()) {
		if (auto err = syncShardingConfig(node); !err.ok()) {
			logWarn("{}", err.what());
			return err;
		}
		// Before the updates loop, which skips the queued configs, covered by the pushed one
		if (auto err = syncAsyncReplicationConfig(node); !err.ok()) {
			// The push is not retried until the reconnect, so its error is shown in the node's stats
			err = Error(err.code(), "Unable to apply leader's async replication config on follower: {}", err.whatStr());
			logWarn("{}:{} {}", serverId_, node.uid, err.whatStr());
			statsCollector_.SaveNodeError(node.uid, err);
		}
	}

	// 3) Perform wal-sync/force-sync for node's namespaces
	if (auto err = namespacesSyncImpl(node, nsList, SyncMode::FullSync); !err.ok()) {
		logError("{}:{} Unable to sync remote namespaces: {}", serverId_, node.uid, err.what());
		return err;
	}
	updateNodeStatus(node.uid, NodeStats::Status::Online);
	statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::OnlineReplication);
	node.unavailabilityCheck = Node::UnavailabilityCheck::NotStarted;

	// 4) Sending updates for this namespace
	const UpdateApplyStatus res = nodeUpdatesHandlingLoop(node);
	logInfo("{}:{} Updates handling loop was terminated", serverId_, node.uid);
	return res.err;
}

template <typename BehaviourParamT>
// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
void ReplThread<BehaviourParamT>::updatesNotifier() noexcept {
	for (auto& node : nodes) {
		if (node.updateNotifier->opened() && !node.updateNotifier->full()) {
			node.updateNotifier->push(true);
		}
	}
}

template <typename BehaviourParamT>
// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
void ReplThread<BehaviourParamT>::terminateNotifier() noexcept {
	logTrace("{}: got termination signal", serverId_);
	DisconnectNodes();
	for (auto& node : nodes) {
		node.updateNotifier->close();
	}
	terminateCh_.close();
}

template <typename BehaviourParamT>
// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
std::tuple<bool, UpdateApplyStatus> ReplThread<BehaviourParamT>::handleNetworkCheckRecord(Node& node, UpdatesQueueT::UpdatePtr& updPtr,
																						  uint16_t offset, bool currentlyOnline,
																						  const updates::UpdateRecord& rec) noexcept {
	bool hadActualNetworkCheck = false;
	auto& data = std::get<NodeNetworkCheckRecord>(*rec.Data());
	if (node.uid == data.nodeUid) {
		Error err;
		if (data.online != currentlyOnline) {
			logTrace("{}:{}: Checking network...", serverId_, node.uid);
			err = node.client.WithTimeout(kStatusCmdTimeout).Status(true);
			hadActualNetworkCheck = true;
		}
		std::ignore = updPtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset, false, Error());
		return std::make_tuple(hadActualNetworkCheck, UpdateApplyStatus(std::move(err), updates::URType::NodeNetworkCheck));
	}
	std::ignore = updPtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset, false, Error());
	return std::make_tuple(hadActualNetworkCheck, UpdateApplyStatus(Error(), updates::URType::NodeNetworkCheck));
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::handleMissingLeaderNs(Node& node, client::CoroReindexer& client, const NamespaceName& nsName,
														 std::string_view stage, bool& retrySync, ReplicationStateV2& retryLeaderState,
														 bool allowOpenRetry) {
	using Presence = ReindexerImpl::NamespacePresence;
	retrySync = false;

	auto loadPresence = [this, &nsName](Presence& presence) -> Error {
		presence = Presence{};
		const RdxDeadlineContext deadlineCtx(kLocalCallsTimeout, &terminateCtx_);
		return thisNode.GetNamespacePresence(nsName, presence, RdxContext().WithCancelCtx(deadlineCtx));
	};

	Presence presence{};
	if (auto err = loadPresence(presence); !err.ok()) {
		return err;
	}

	if (presence == Presence::Open && allowOpenRetry) {
		const RdxDeadlineContext deadlineCtx(kLocalCallsTimeout, &terminateCtx_);
		auto err = thisNode.GetReplState(nsName, retryLeaderState, RdxContext().WithCancelCtx(deadlineCtx));
		if (err.ok()) {
			logInfo("{}:{}:{} Namespace reappeared on leader at {}. Retrying sync", serverId_, node.uid, nsName, stage);
			retrySync = true;
			return Error();
		}
		if (err.code() != errNotFound) {
			return err;
		}
		if (auto probeErr = loadPresence(presence); !probeErr.ok()) {
			return probeErr;
		}
	}

	switch (presence) {
		case Presence::Open:
			return Error(errLogic, "{}:{}:{} Namespace is open on leader at {} but GetReplState failed", serverId_, node.uid, nsName,
						 stage);
		case Presence::ClosedWithStorage:
			node.NsData(nsName).isClosed = true;
			logInfo("{}:{}:{} Namespace is missing on leader at {} (closed with storage). Keeping follower data", serverId_, node.uid,
					nsName, stage);
			return Error();
		case Presence::Absent: {
			logInfo("{}:{}:{} Namespace is absent on leader ({}). Dropping it on follower", serverId_, node.uid, nsName, stage);
			rollbackNamespaceTx(node, node.NsData(nsName));
			auto dropRes = client.WithLSN(lsn_t(0, serverId_)).DropNamespace(nsName);
			if (!dropRes.ok() && dropRes.code() != errNotFound) {
				return dropRes;
			}
			node.EraseNsData(nsName);
			return Error();
		}
		default:
			std::abort();
	}
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::syncNamespace(Node& node, const NamespaceName& nsName, const ReplicationStateV2& followerState,
												 SyncMode syncMode) {
	try {
		class [[nodiscard]] TmpNsGuard {
		public:
			TmpNsGuard(client::CoroReindexer& client, int serverId, const Logger& log) : client_(client), serverId_(serverId), log_(log) {}
			// NOLINTNEXTLINE(bugprone-exception-escape) Exceptions in logging are unlikely
			~TmpNsGuard() {
				if (tmpNsName.size()) {
					logWarn("{}: Dropping tmp ns on error: '{}'", serverId_, tmpNsName);
					// NOLINTNEXTLINE(rx-safety-noexcept-throw-call): not throwing
					if (auto err = client_.WithLSN(lsn_t(0, serverId_)).DropNamespace(tmpNsName); err.ok()) {
						logWarn("{}: '{}' was dropped", serverId_, tmpNsName);
					} else {
						logWarn("{}: '{}' drop error: {}", serverId_, tmpNsName, err.whatStr());
					}
				}
			}

			std::string tmpNsName;

		private:
			client::CoroReindexer& client_;
			int serverId_;
			const Logger& log_;
		};

		if (!bhvParam_.IsLeader()) {
			return Error(errParams, "Leader was switched");
		}
		const auto hwm = updates_->GetNextUpdateID();
		SyncTimeCounter timeCounter(SyncTimeCounter::Type::WalSync, statsCollector_);

		ReplicationStateV2 localState;
		Snapshot snapshot;
		ExtendedLsn requiredLsn(followerState.nsVersion, followerState.lastLsn);
		bool createTmpNamespace = false;
		auto client = node.client.WithTimeout(std::chrono::seconds(config_.SyncTimeoutSec));
		TmpNsGuard tmpNsGuard{client, serverId_, log_};

		RdxDeadlineContext deadlineCtx(kLocalCallsTimeout, &terminateCtx_);
		auto err = thisNode.GetReplState(nsName, localState, RdxContext().WithCancelCtx(deadlineCtx));
		if (!err.ok()) {
			if (err.code() != errNotFound) {
				return err;
			}
			if (syncMode == SyncMode::OnlineSingleNs) {
				logInfo("{}:{} Namespace '{}' is not found on leader during online resync. Skipping it", serverId_, node.uid, nsName);
				return Error();
			}
			bool retrySync = false;
			err = handleMissingLeaderNs(node, client, nsName, "repl_state", retrySync, localState);
			if (!err.ok()) {
				return err;
			}
			if (!retrySync) {
				return Error();
			}
		}
		const ExtendedLsn localLsn(localState.nsVersion, localState.lastLsn);

		logInfo(
			"{}:{} ReplState for '{}': {{ local: {{ ns_version: {}, lsn: {}, checksum: {} }}, remote: {{ "
			"ns_version: {}, lsn: {}, checksum: {} }} }}",
			serverId_, node.uid, nsName, localState.nsVersion, localState.lastLsn, localState.checksum, followerState.nsVersion,
			followerState.lastLsn, followerState.checksum);

		if (!requiredLsn.IsEmpty() && localLsn.IsCompatibleByNsVersion(requiredLsn)) {
			if (requiredLsn.LSN().Counter() > localLsn.LSN().Counter()) {
				logWarn("{}:{}:{} unexpected follower's lsn: {}. Local lsn: {}", serverId_, node.uid, nsName, requiredLsn.LSN(),
						localLsn.LSN());
				requiredLsn = ExtendedLsn();
			} else if (requiredLsn.LSN().Counter() == localState.lastLsn.Counter() &&
					   requiredLsn.LSN().Server() != localState.lastLsn.Server()) {
				logWarn("{}:{}:{} unexpected follower's lsn: {}. Local lsn: {}. LSNs have different server ids", serverId_, node.uid,
						nsName, requiredLsn.LSN(), localLsn.LSN());
				requiredLsn = ExtendedLsn();
			} else if (requiredLsn.LSN() == localLsn.LSN() && followerState.checksum != localState.checksum) {
				logWarn("{}:{}:{} Checksum mismatch. Expected: {}, actual: {}", serverId_, node.uid, nsName, localState.checksum,
						followerState.checksum);
				assertrx_dbg(0 && "Does not expect checksum missmatch in debug build");
				requiredLsn = ExtendedLsn();
			}
		}

		deadlineCtx = RdxDeadlineContext(kLocalCallsTimeout, &terminateCtx_);
		err = thisNode.GetSnapshot(nsName, SnapshotOpts(requiredLsn, config_.MaxWALDepthOnForceSync), snapshot,
								   RdxContext().WithCancelCtx(deadlineCtx));
		if (!err.ok()) {
			if (err.code() != errNotFound) {
				return err;
			}
			if (syncMode == SyncMode::OnlineSingleNs) {
				logInfo("{}:{} Namespace '{}' is not found on leader while taking snapshot. Skipping it", serverId_, node.uid, nsName);
				return Error();
			}
			bool retrySync = false;
			err = handleMissingLeaderNs(node, client, nsName, "snapshot", retrySync, localState);
			if (!err.ok()) {
				return err;
			}
			if (!retrySync) {
				return Error();
			}
			deadlineCtx = RdxDeadlineContext(kLocalCallsTimeout, &terminateCtx_);
			err = thisNode.GetSnapshot(nsName, SnapshotOpts(requiredLsn, config_.MaxWALDepthOnForceSync), snapshot,
									   RdxContext().WithCancelCtx(deadlineCtx));
			if (err.code() == errNotFound) {
				constexpr bool allowOpenRetry = false;
				return handleMissingLeaderNs(node, client, nsName, "snapshot", retrySync, localState, allowOpenRetry);
			}
			if (!err.ok()) {
				return err;
			}
		}
		if (snapshot.HasRawData()) {
			logInfo("{}:{}:{} Snapshot has RAW data, creating tmp namespace (performing FORCE sync)", serverId_, node.uid, nsName);
			createTmpNamespace = true;
		} else if (snapshot.NsVersion().Server() != requiredLsn.NsVersion().Server() ||
				   snapshot.NsVersion().Counter() != requiredLsn.NsVersion().Counter()) {
			logInfo("{}:{}:{} Snapshot has different ns version ({} vs {}), creating tmp namespace (performing FORCE sync)", serverId_,
					node.uid, nsName, snapshot.NsVersion(), requiredLsn.NsVersion());
			createTmpNamespace = true;
		}

		std::string_view replNsName;
		if (createTmpNamespace) {
			timeCounter.SetType(SyncTimeCounter::Type::ForceSync);
			// TODO: Allow tmp ns without storage via config
			err = client.WithLSN(lsn_t(0, serverId_))
					  .CreateTemporaryNamespace(nsName, tmpNsGuard.tmpNsName, StorageOpts().Enabled(), snapshot.NsVersion());
			if (!err.ok()) {
				return err;
			}
			if constexpr (isClusterReplThread()) {
				// Ownership is set before the rename, so the replicated namespace is never writable on the follower
				if (!snapshot.ClusterOperationStat().isPrimary) {
					err = setNamespaceOwnership(node, tmpNsGuard.tmpNsName, IsPrimary_False);
				}
			} else {
				err = markNewRemoteNs(node, client, tmpNsGuard.tmpNsName);
			}
			if (!err.ok()) {
				return err;
			}
			replNsName = tmpNsGuard.tmpNsName;
		} else {
			replNsName = nsName;
		}
		logInfo("{}:{}:{} Target ns name: {}", serverId_, node.uid, nsName, replNsName);
		for (auto& it : snapshot) {
			if (terminate_) {
				logInfo("{}:{}:{} Terminated, while syncing namespace", serverId_, node.uid, nsName);
				return Error(errCanceled, "{}:{}:{} Terminated, while syncing namespace", serverId_, node.uid, nsName);
			}
			if (!bhvParam_.IsLeader()) {
				return Error(errParams, "Leader was switched");
			}
			err = client.WithLSN(lsn_t(0, serverId_)).ApplySnapshotChunk(replNsName, it.Chunk());
			if (!err.ok()) {
				return err;
			}
		}

		ReplicationStateV2 replState;
		err = client.GetReplState(replNsName, replState);
		if (!err.ok() && err.code() != errNotFound) {
			return err;
		}
		logInfo(
			"{}:{}:{} Sync done. {{ snapshot: {{ ns_version: {}, lsn: {}, checksum: {}, data_count: {} }}, remote: {{ ns_version: {}, "
			"lsn: "
			"{}, checksum: {}, data_count: {} }} }}",
			serverId_, node.uid, nsName, snapshot.NsVersion(), snapshot.LastLSN(), snapshot.ExpectedChecksum(),
			snapshot.ExpectedDataCount(), replState.nsVersion, replState.lastLsn, replState.checksum, replState.dataCount);

		const bool versionMissmatch = (!snapshot.LastLSN().isEmpty() && snapshot.LastLSN() != replState.lastLsn) ||
									  (!snapshot.NsVersion().isEmpty() && snapshot.NsVersion() != replState.nsVersion);
		if (versionMissmatch || replState.checksum != snapshot.ExpectedChecksum() || snapshot.ExpectedDataCount() != replState.dataCount) {
			logInfo("{}:{}:{} Snapshot dump on data missmatch: {}", serverId_, node.uid, nsName, snapshot.Dump());
			return Error(errChecksumMismatch,
						 "{}:{}:{}: Checksum or datacount mismatch after sync. Actual: {{ checksum: {}, data_count: {}, ns_version: {}, "
						 "lsn: {} }}; expected: {{ checksum: {}, data_count: {}, ns_version: {}, lsn: {} }}",
						 serverId_, node.uid, nsName, replState.checksum, replState.dataCount, replState.nsVersion, replState.lastLsn,
						 snapshot.ExpectedChecksum(), snapshot.ExpectedDataCount(), snapshot.NsVersion(), snapshot.LastLSN());
		}

		if (createTmpNamespace) {
			logInfo("{}:{}:{} Renaming: {} -> {}", serverId_, node.uid, nsName, replNsName, nsName);
			err = client.WithLSN(lsn_t(0, serverId_)).RenameNamespace(replNsName, nsName);
			if (!err.ok()) {
				return err;
			}
			tmpNsGuard.tmpNsName.clear();
		}
		auto& committedNsData = node.NsData(nsName);
		committedNsData.latestLsn = ExtendedLsn(replState.nsVersion, replState.lastLsn);
		committedNsData.isClosed = false;
		committedNsData.syncedAtQueueId = hwm;
	} catch (std::exception& err) {
		return std::move(err);
	}
	return Error();
}

template <typename BehaviourParamT>
void ReplThread<BehaviourParamT>::rollbackNamespaceTx(Node& node, NamespaceData& nsData) {
	if (nsData.tx.IsFree()) {
		return;
	}
	auto err = node.client.WithLSN(lsn_t(0, serverId_)).RollBackTransaction(nsData.tx);
	logInfo("{}:{} Rollback transaction result: {}", serverId_, node.uid,
			err.ok() ? "OK" : ("Error:" + std::to_string(err.code()) + ". " + err.whatStr()));
	nsData.tx = client::CoroTransaction();
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::resyncSingleNamespace(Node& node, const NamespaceName& nsName) noexcept {
	try {
		rollbackNamespaceTx(node, nsName);

		statsCollector_.OnEnqueueNamespacesSync(node.uid, 1);
		const NamespaceDef def(nsName, NamespaceDef::NameOnly{});
		if (auto err = namespacesSyncImpl(node, std::span<const NamespaceDef>(&def, 1), SyncMode::OnlineSingleNs); !err.ok()) {
			return err;
		}
		node.NsData(nsName).requiresTmUpdate = true;
		updateNodeStatus(node.uid, NodeStats::Status::Online);
		statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::OnlineReplication);
	} catch (std::exception& err) {
		return std::move(err);
	}
	return Error();
}

template <typename BehaviourParamT>
// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
UpdateApplyStatus ReplThread<BehaviourParamT>::nodeUpdatesHandlingLoop(Node& node) noexcept {
	logInfo("{}:{} Start updates handling loop", serverId_, node.uid);

	struct [[nodiscard]] Context {
		UpdatesQueueT::UpdatePtr updPtr;
		NamespaceData* nsData;
		uint16_t offset;
	};
	auto& updatesNotifier = *node.updateNotifier;
	bool requireReelections = false;

	auto applyUpdateF = [this, &node](const UpdatesQueueT::UpdateT::Value& upd, Context& ctx) {
		auto& it = upd.Data();
		auto& nsName = it.NsName();
		auto& nsData = *ctx.nsData;
		logTrace(
			"{}:{}:{} Applying update with type {} (batched), id: {}, ns version: {}, lsn: {}, last synced ns "
			"version: {}, last synced lsn: {}",
			serverId_, node.uid, nsName, int(it.Type()), ctx.updPtr->ID() + ctx.offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
			nsData.latestLsn.NsVersion(), nsData.latestLsn.LSN());
		return applyUpdate(it, node, *ctx.nsData);
	};
	auto onUpdateResF = [this, &node, &requireReelections](const UpdatesQueueT::UpdateT::Value& upd, const UpdateApplyStatus& res,
														   Context&& ctx) {
		auto& it = upd.Data();
		ctx.nsData->UpdateLsnOnRecord(it);
		logTrace("{}:{}:{} Apply update (lsn: {}, id: {}) result: {}. Replicas: {}", serverId_, node.uid, it.NsName(), it.ExtLSN().LSN(),
				 ctx.updPtr->ID() + ctx.offset, (res.err.ok() ? "OK" : "ERROR:" + res.err.whatStr()), upd.GetCounters().replicas + 1);
		const auto replRes = ctx.updPtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, ctx.offset,
														 it.EmitterServerID() == node.serverId, res.err);
		if (res.err.ok()) {
			bhvParam_.OnUpdateSucceed(node.uid, ctx.updPtr->ID() + ctx.offset);
		}
		requireReelections = requireReelections || (replRes == updates::ReplicationResult::Error);
	};
	auto convertResF = [](Error&& err, const UpdatesQueueT::UpdateT::Value& upd) {
		return UpdateApplyStatus(std::move(err), upd.Data().Type());
	};
	UpdatesBatcher<UpdatesQueueT::UpdateT::Value, Context, decltype(applyUpdateF), decltype(onUpdateResF), decltype(convertResF)> batcher(
		loop, config_.BatchingRoutinesCount, std::move(applyUpdateF), std::move(onUpdateResF), std::move(convertResF));

	while (!terminate_) {
		UpdateApplyStatus res;
		UpdatesQueueT::UpdatePtr updatePtr;
		do {
			if (node.requireResync) {
				logTrace("{}:{} Node is requiring resync", serverId_, node.uid);
				return UpdateApplyStatus();
			}
			if (!bhvParam_.IsLeader()) {
				logTrace("{}:{}: Is not leader anymore", serverId_, node.uid);
				return UpdateApplyStatus();
			}
			updatePtr = updates_->Read(node.nextUpdateId, std::this_thread::get_id());
			if (!updatePtr) {
				break;
			}
			if (updatePtr->IsUpdatesDropBlock) {
				const auto nextUpdateID = updatePtr->ID() + 1;
				logInfo("{}:{} Got updates drop block. Last replicated id: {}, Next update id: {}", serverId_, node.uid, node.nextUpdateId,
						nextUpdateID);
				node.nextUpdateId = nextUpdateID;
				statsCollector_.OnUpdateApplied(node.uid, updatePtr->ID());
				return UpdateApplyStatus(Error(), updates::URType::ResyncOnUpdatesDrop);
			}
			logTrace("{}:{} Got new update. Next update id: {}. Queue block id: {}, block count: {}", serverId_, node.uid,
					 node.nextUpdateId, updatePtr->ID(), updatePtr->Count());
			node.nextUpdateId = updatePtr->ID() > node.nextUpdateId ? updatePtr->ID() : node.nextUpdateId;
			for (uint16_t offset = node.nextUpdateId - updatePtr->ID(); offset < updatePtr->Count(); ++offset) {
				if (terminate_) [[unlikely]] {
					break;
				}
				if (updatePtr->IsInvalidated()) {
					logInfo("{}:{} Current update is invalidated", serverId_, node.uid);
					break;
				}
				++node.nextUpdateId;
				auto& upd = updatePtr->GetUpdate(offset);
				auto& it = upd.Data();
				if (it.IsNetworkCheckRecord()) {
					[[maybe_unused]] bool v;
					std::tie(v, res) = handleNetworkCheckRecord(node, updatePtr, offset, true, it);
					if (!res.err.ok()) {
						break;
					}
					continue;
				}
				const auto& nsName = it.NsName();
				if (nsName.empty()) {
					assertrx_dbg(it.IsDbRecord());
					res = batcher.AwaitBatchedUpdates();
					if (!res.err.ok()) {
						--node.nextUpdateId;  // Have to read this update again
						break;
					}
					if (const uint64_t eventId = updatePtr->ID() + offset;
						it.Type() == updates::URType::ApplyAsyncReplicationConfig && eventId < node.asyncConfigPushedAtQueueId) {
						logTrace("{}:{} Skipping async replication config, id: {} (covered by the config, pushed at {})", serverId_,
								 node.uid, eventId, node.asyncConfigPushedAtQueueId);
						std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset,
																 it.EmitterServerID() == node.serverId, Error());
						bhvParam_.OnUpdateSucceed(node.uid, eventId);
						continue;
					}

					logTrace("{}:{} Applying db-level update with type {} (empty ns name), id: {}, ns version: {}, lsn: {}", serverId_,
							 node.uid, int(it.Type()), updatePtr->ID() + offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN());
					NamespaceData dummy;
					res = applyUpdate(it, node, dummy);
					logTrace("{}:{} Apply db-level update result (id: {}, ns version: {}, lsn: {}): {}. Replicas: {}", serverId_, node.uid,
							 updatePtr->ID() + offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
							 (res.err.ok() ? "OK" : "ERROR:" + res.err.whatStr()), upd.GetCounters().replicas + 1);

					const auto replRes = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset,
																	it.EmitterServerID() == node.serverId, res.err);
					if (res.err.ok()) {
						bhvParam_.OnUpdateSucceed(node.uid, updatePtr->ID() + offset);
						continue;
					}
					requireReelections = requireReelections || (replRes == updates::ReplicationResult::Error);
					break;
				}
				if constexpr (!isClusterReplThread()) {
					if (!bhvParam_.IsNamespaceInConfig(node.uid, nsName)) {
						std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset, false, Error());
						bhvParam_.OnUpdateSucceed(node.uid, updatePtr->ID() + offset);
						continue;
					}
				}

				if (it.Type() == updates::URType::AddNamespace) {
					bhvParam_.OnNewNsAppearance(nsName);
				}

				auto& nsData = node.NsData(nsName);
				const uint64_t eventId = updatePtr->ID() + offset;
				if (eventId < nsData.syncedAtQueueId) {
					logTrace("{}:{}:{} Skipping update with type {}, id: {} (covered by snapshot synced at {})", serverId_, node.uid,
							 nsName, int(it.Type()), eventId, nsData.syncedAtQueueId);
					std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset,
															 it.EmitterServerID() == node.serverId, Error());
					bhvParam_.OnUpdateSucceed(node.uid, updatePtr->ID() + offset);
					continue;
				}
				if (nsData.isClosed && !isNsLifecycleRecord(it.Type())) {
					logTrace("{}:{}:{} Skipping update with type {}, id: {} (namespace is closed on leader)", serverId_, node.uid, nsName,
							 int(it.Type()), eventId);
					std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset,
															 it.EmitterServerID() == node.serverId, Error());
					bhvParam_.OnUpdateSucceed(node.uid, updatePtr->ID() + offset);
					continue;
				}
				const bool isOutdatedRecord = !it.ExtLSN().HasNewerCounterThan(nsData.latestLsn) || nsData.latestLsn.IsEmpty();
				if ((!it.IsDbRecord() && isOutdatedRecord) || it.IsEmptyRecord()) {
					logTrace(
						"{}:{}:{} Skipping update with type {}, id: {}, ns version: {}, lsn: {}, last synced ns "
						"version: {}, last synced lsn: {}",
						serverId_, node.uid, nsName, int(it.Type()), updatePtr->ID() + offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
						nsData.latestLsn.NsVersion(), nsData.latestLsn.LSN());
					std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset,
															 it.EmitterServerID() == node.serverId, Error());
					bhvParam_.OnUpdateSucceed(node.uid, updatePtr->ID() + offset);
					continue;
				}
				if (nsData.tx.IsFree() && it.IsRequiringTx()) {
					res = UpdateApplyStatus(Error(errTxDoesNotExist, "Update requires tx. ID: {}, lsn: {}, type: {}",
												  updatePtr->ID() + offset, it.ExtLSN().LSN(), int(it.Type())));
					--node.nextUpdateId;  // Have to read this update again
					break;
				}
				if (nsData.requiresTmUpdate && (it.IsBatchingAllowed() || it.IsTxBeginning())) {
					nsData.requiresTmUpdate = false;
					// Explicitly update tm for this namespace
					// TODO: Find better solution?
					logTrace("{}:{}:{} Executing select to update tm...", serverId_, node.uid, nsName);
					{
						client::CoroQueryResults qr;
						const auto q = Query(nsName).Limit(0);
						res = node.client.WithShardId(ShardingKeyType::ProxyOff, false).Select(q, qr);
						if (!res.err.ok()) {
							--node.nextUpdateId;  // Have to read this update again
							break;
						}
					}
				}
				if (it.IsBatchingAllowed()) {
					res = batcher.Batch(upd, Context{updatePtr, &nsData, offset});
					if (!res.err.ok()) {
						--node.nextUpdateId;  // Have to read this update again
						break;
					}
					continue;
				} else {
					res = batcher.AwaitBatchedUpdates();
					if (!res.err.ok()) {
						--node.nextUpdateId;  // Have to read this update again
						break;
					}

					logTrace(
						"{}:{}:{} Applying update with type {} (no batching), id: {}, ns version: {}, lsn: {}, "
						"last synced ns "
						"version: {}, last synced lsn: {}",
						serverId_, node.uid, nsName, int(it.Type()), updatePtr->ID() + offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
						nsData.latestLsn.NsVersion(), nsData.latestLsn.LSN());
					res = applyUpdate(it, node, nsData);
					logTrace("{}:{}:{} Apply update result (id: {}, ns version: {}, lsn: {}): {}. Replicas: {}", serverId_, node.uid,
							 nsName, updatePtr->ID() + offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
							 (res.err.ok() ? "OK" : "ERROR:" + res.err.whatStr()), upd.GetCounters().replicas + 1);

					const auto replRes = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset,
																	it.EmitterServerID() == node.serverId, res.err);
					if (res.err.ok()) {
						nsData.UpdateLsnOnRecord(it);
						bhvParam_.OnUpdateSucceed(node.uid, updatePtr->ID() + offset);
						nsData.requiresTmUpdate = it.IsRequiringTmUpdate();
					} else {
						requireReelections = requireReelections || (replRes == updates::ReplicationResult::Error);
					}
				}

				if (!res.err.ok()) {
					break;
				} else if (res.NeedsSingleNsResync<BehaviourParamT>()) {
					assertrx(batcher.BatchedUpdatesCount() == 0);  // Batched updates have to be drained before resync
					logInfo("{}:{} Resync was requested for '{}'", serverId_, node.uid, nsName);
					Error err = resyncSingleNamespace(node, nsName);
					if (!err.ok()) {
						logError("{}:{}:{} Single-namespace resync error: {}", serverId_, node.uid, nsName, err.whatStr());
						res = UpdateApplyStatus(std::move(err));
						break;
					}
					logInfo("{}:{}:{} Single-namespace resync completed", serverId_, node.uid, nsName);
					// Must clear type: leftover Generic/LeaderInit would re-enter full nodeReplicationImpl.
					res = UpdateApplyStatus();
					continue;
				}
			}

			if (batcher.BatchedUpdatesCount()) {
				assertrx(!res.IsHaveToResync<BehaviourParamT>());  // In this cases batchedUpdatesCount has to be 0
				auto batchedRes = batcher.AwaitBatchedUpdates();
				if (res.err.ok()) {
					res = std::move(batchedRes);
				}
			}

			if (requireReelections) {
				logWarn("{}:{} Requesting leader reelection on error: {}", serverId_, node.uid, res.err.whatStr());
				requireReelections = false;
				bhvParam_.OnUpdateReplicationFailure();
				return res;
			}
			if (!res.err.ok() || res.IsHaveToResync<BehaviourParamT>()) {
				return res;
			}
		} while (!terminate_);

		if (updatesNotifier.empty()) {
			bhvParam_.OnAllUpdatesReplicated(node.uid, int64_t(node.nextUpdateId) - 1);
			logTrace("{}:{} Awaiting updates...", serverId_, node.uid);
		}
		std::ignore = updatesNotifier.pop();
	}
	if (terminate_) {
		logTrace("{}: updates handling loop was terminated", serverId_);
	}
	return Error();
}

template <typename BehaviourParamT>
bool ReplThread<BehaviourParamT>::handleUpdatesWithError(Node& node, const Error& err) {
	auto& updatesNotifier = *node.updateNotifier;
	UpdatesQueueT::UpdatePtr updatePtr;
	bool hadErrorOnLastUpdate = false;

	if (!updatesNotifier.empty()) {
		std::ignore = updatesNotifier.pop();
	}
	do {
		updatePtr = updates_->Read(node.nextUpdateId, std::this_thread::get_id());
		if (!updatePtr) {
			break;
		}
		if (updatePtr->IsUpdatesDropBlock) {
			node.nextUpdateId = updatePtr->ID() + 1;
			continue;
		}
		node.nextUpdateId = updatePtr->ID() > node.nextUpdateId ? updatePtr->ID() : node.nextUpdateId;
		for (uint16_t offset = node.nextUpdateId - updatePtr->ID(); offset < updatePtr->Count(); ++offset) {
			++node.nextUpdateId;

			auto& upd = updatePtr->GetUpdate(offset);
			auto& it = upd.Data();
			if (it.IsNetworkCheckRecord()) {
				const auto [hadActualNetworkCheck, res] = handleNetworkCheckRecord(node, updatePtr, offset, false, it);
				if (hadActualNetworkCheck && res.err.ok()) {
					return true;  // Retry sync after succeed network check
				}
				continue;
			}
			const auto& nsName = it.NsName();
			if (updatePtr->IsInvalidated()) {
				logTrace("{}:{}:{} Update {} was invalidated", serverId_, node.uid, nsName, updatePtr->ID());
				break;
			}
			if (!nsName.empty() && !bhvParam_.IsNamespaceInConfig(node.uid, nsName)) {
				std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset, false, Error());
				continue;
			}
			if (!nsName.empty()) {
				// Reconciliation on the reconnect drops the namespace on the node, if it is absent here
				std::ignore = node.NsData(nsName);
			}

			if (it.Type() == updates::URType::AddNamespace || it.Type() == updates::URType::DropNamespace) {
				bhvParam_.OnNewNsAppearance(nsName);
			}

			assert(it.EmitterServerID() != serverId_);
			const bool isEmitter = it.EmitterServerID() == node.serverId;
			// FIXME: #2552 This check works incorrectly for async replication: node.serverId and EmitterServerID of most async updates are
			// -1. Fast resync is kept for the target RAFT-clusters only: it detects their leader's switch
			const bool isOwnUpdate = isEmitter && (isClusterReplThread() || node.IsClusterTarget());
			if (isOwnUpdate) {
				switch (node.unavailabilityCheck) {
					case Node::UnavailabilityCheck::NotStarted:
						node.unavailabilityCheck = Node::UnavailabilityCheck::SyncRetried;
						--node.nextUpdateId;
						return true;  // Retry sync after receiving update from offline node
					case Node::UnavailabilityCheck::SyncRetried:
						node.unavailabilityCheck = Node::UnavailabilityCheck::Confirmed;
						logInfo("{}:{} Node is still unavailable after the sync retry, failing the updates proxied through it: {}",
								serverId_, node.uid, err.whatStr());
						break;
					case Node::UnavailabilityCheck::Confirmed:
						break;
				}
			}
			const auto replRes = updatePtr->OnUpdateHandled(
				node.uid, consensusCnt_, requiredReplicas_, offset, isEmitter,
				isOwnUpdate ? Error(errUpdateReplication, "{}: {}", updates::kNotReplicatedToEmitterMsg, err.whatStr())
							: Error(errUpdateReplication, "{}. Last error: {}", updates::kNotEnoughReplicasMsg, err.whatStr()));

			if (replRes == updates::ReplicationResult::Error && !hadErrorOnLastUpdate) {
				hadErrorOnLastUpdate = true;
				logWarn("{}:{} Requesting leader reelection on error: {}", serverId_, node.uid, err.whatStr());
				bhvParam_.OnUpdateReplicationFailure();
			}

			log_.Trace(
				[&] {
					const auto counters = upd.GetCounters();
					rtfmt(
						"{}:{}:{} Dropping update with error: {}. Type {}, ns version: {}, lsn: {}, emitter: {}. "
						"Required: "
						"{}, succeed: "
						"{}, failed: {}, replicas: {}",
						serverId_, node.uid, nsName, err.whatStr(), int(it.Type()), it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
						(isEmitter ? node.serverId : it.EmitterServerID()), consensusCnt_, counters.approves, counters.errors,
						counters.replicas + 1);
				},
				__FILE__, __LINE__, __FUNCTION__);
		}
	} while (!terminate_);
	return false;
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::prepareReplicationTarget(Node& node) noexcept {
	if constexpr (!isClusterReplThread()) {
		if (node.IsClusterTarget()) {
			return node.isLeaderValidated ? Error() : connectToClusterTargetLeader(node);
		}
		return checkTargetNotInSyncCluster(node);
	} else {
		(void)node;
		return Error();
	}
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::checkTargetNotInSyncCluster(Node& node) noexcept {
	try {
		logWarn("{}:{} Checking if replication is allowed for this node", serverId_, node.uid);
		const Query q = Query(kReplicationStatsNamespace).Where("type", CondEq, Variant(cluster::kClusterReplStatsType));
		client::CoroQueryResults qr;
		auto err = node.client.Select(q, qr);
		if (!err.ok()) {
			return err;
		}

		if (qr.Count() == 1) {
			WrSerializer wser;
			err = qr.begin().GetJSON(wser, false);
			if (!err.ok()) {
				return err;
			}

			ReplicationStats stats;
			err = stats.FromJSON(giftStr(wser.Slice()));
			if (!err.ok()) {
				return err;
			}

			if (stats.nodeStats.size()) {
				if (stats.nodeStats[0].namespaces.size()) {
					for (const auto& ns : stats.nodeStats[0].namespaces) {
						if (bhvParam_.IsNamespaceInConfig(node.uid, ns)) {
							return Error(
								errParams,
								"Replication namespace '{}' is present on target node in sync cluster config. Target namespace can "
								"not be a part of sync cluster",
								ns);
						}
					}
				} else {
					return Error(errParams,
								 "Target node has sync cluster config over all the namespaces. Target namespace can "
								 "not be a part of sync cluster");
				}
			}
		}
		return Error();
	} CATCH_AND_RETURN
}

template <typename BehaviourParamT>
UpdateApplyStatus ReplThread<BehaviourParamT>::applyUpdate(const updates::UpdateRecord& rec, ReplThread::Node& node,
														   ReplThread::NamespaceData& nsData) noexcept {
	auto lsn = rec.ExtLSN().LSN();
	std::string_view nsName = rec.NsName();
	auto& client = node.client;
	try {
		switch (rec.Type()) {
			case updates::URType::ItemUpdate: {
				auto& data = std::get<ItemReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).Update(nsName, std::string_view{data.ch}), rec.Type());
			}
			case updates::URType::ItemUpsert: {
				auto& data = std::get<ItemReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).Upsert(nsName, std::string_view{data.ch}), rec.Type());
			}
			case updates::URType::ItemDelete: {
				auto& data = std::get<ItemReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).Delete(nsName, std::string_view{data.ch}), rec.Type());
			}
			case updates::URType::ItemInsert: {
				auto& data = std::get<ItemReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).Insert(nsName, std::string_view{data.ch}), rec.Type());
			}
			case updates::URType::IndexAdd: {
				auto& data = std::get<IndexReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).AddIndex(nsName, *data.idef), rec.Type());
			}
			case updates::URType::IndexDrop: {
				auto& data = std::get<IndexReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).DropIndex(nsName, *data.idef), rec.Type());
			}
			case updates::URType::IndexUpdate: {
				auto& data = std::get<IndexReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).UpdateIndex(nsName, *data.idef), rec.Type());
			}
			case updates::URType::PutMeta: {
				auto& data = std::get<MetaReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).PutMeta(nsName, data.key, data.value), rec.Type());
			}
			case updates::URType::PutMetaTx: {
				if (nsData.tx.IsFree()) {
					return UpdateApplyStatus(Error(errLogic, "Tx is empty"), rec.Type());
				}
				auto& data = std::get<MetaReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(nsData.tx.PutMeta(data.key, data.value, lsn), rec.Type());
			}
			case updates::URType::DeleteMeta: {
				auto& data = std::get<MetaReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).DeleteMeta(nsName, data.key), rec.Type());
			}
			case updates::URType::UpdateQuery: {
				auto& data = std::get<QueryReplicationRecord>(*rec.Data());
				client::CoroQueryResults qr;
				return UpdateApplyStatus(client.WithLSN(lsn).Update(Query::FromSQL(data.sql), qr), rec.Type());
			}
			case updates::URType::DeleteQuery: {
				auto& data = std::get<QueryReplicationRecord>(*rec.Data());
				client::CoroQueryResults qr;
				return UpdateApplyStatus(client.WithLSN(lsn).Delete(Query::FromSQL(data.sql), qr), rec.Type());
			}
			case updates::URType::SetSchema: {
				auto& data = std::get<SchemaReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(client.WithLSN(lsn).SetSchema(nsName, data.schema), rec.Type());
			}
			case updates::URType::Truncate: {
				return UpdateApplyStatus(client.WithLSN(lsn).TruncateNamespace(nsName), rec.Type());
			}
			case updates::URType::BeginTx: {
				if (!nsData.tx.IsFree()) {
					return UpdateApplyStatus(Error(errLogic, "Tx is not empty"), rec.Type());
				}
				nsData.tx = node.client.WithLSN(lsn).NewTransaction(nsName);
				return UpdateApplyStatus(Error(nsData.tx.Status()), rec.Type());
			}
			case updates::URType::CommitTx: {
				if (nsData.tx.IsFree()) {
					return UpdateApplyStatus(Error(errLogic, "Tx is empty"), rec.Type());
				}
				client::CoroQueryResults qr;
				return UpdateApplyStatus(node.client.WithLSN(lsn).CommitTransaction(nsData.tx, qr), rec.Type());
			}
			case updates::URType::ItemUpdateTx:
			case updates::URType::ItemUpsertTx:
			case updates::URType::ItemDeleteTx:
			case updates::URType::ItemInsertTx: {
				if (nsData.tx.IsFree()) {
					return UpdateApplyStatus(Error(errLogic, "Tx is empty"), rec.Type());
				}
				auto& data = std::get<ItemReplicationRecord>(*rec.Data());
				switch (rec.Type()) {
					case updates::URType::ItemUpdateTx:
						return UpdateApplyStatus(nsData.tx.Update(std::string_view{data.ch}, lsn), rec.Type());
					case updates::URType::ItemUpsertTx:
						return UpdateApplyStatus(nsData.tx.Upsert(std::string_view{data.ch}, lsn), rec.Type());
					case updates::URType::ItemDeleteTx:
						return UpdateApplyStatus(nsData.tx.Delete(std::string_view{data.ch}, lsn), rec.Type());
					case updates::URType::ItemInsertTx:
						return UpdateApplyStatus(nsData.tx.Insert(std::string_view{data.ch}, lsn), rec.Type());
					case updates::URType::None:
					case updates::URType::ItemUpdate:
					case updates::URType::ItemUpsert:
					case updates::URType::ItemDelete:
					case updates::URType::ItemInsert:
					case updates::URType::IndexAdd:
					case updates::URType::IndexDrop:
					case updates::URType::IndexUpdate:
					case updates::URType::PutMeta:
					case updates::URType::PutMetaTx:
					case updates::URType::DeleteMeta:
					case updates::URType::UpdateQuery:
					case updates::URType::DeleteQuery:
					case updates::URType::UpdateQueryTx:
					case updates::URType::DeleteQueryTx:
					case updates::URType::SetSchema:
					case updates::URType::Truncate:
					case updates::URType::BeginTx:
					case updates::URType::CommitTx:
					case updates::URType::AddNamespace:
					case updates::URType::DropNamespace:
					case updates::URType::CloseNamespace:
					case updates::URType::RenameNamespace:
					case updates::URType::ResyncNamespaceGeneric:
					case updates::URType::ResyncNamespaceLeaderInit:
					case updates::URType::ResyncOnUpdatesDrop:
					case updates::URType::EmptyUpdate:
					case updates::URType::NodeNetworkCheck:
					case updates::URType::SetTagsMatcher:
					case updates::URType::SetTagsMatcherTx:
					case updates::URType::SaveShardingConfig:
					case updates::URType::ApplyShardingConfig:
					case updates::URType::ResetOldShardingConfig:
					case updates::URType::ResetCandidateConfig:
					case updates::URType::RollbackCandidateConfig:
					case updates::URType::ApplyAsyncReplicationConfig:
						break;
				}
				std::abort();
			}
			case updates::URType::UpdateQueryTx:
			case updates::URType::DeleteQueryTx: {
				if (nsData.tx.IsFree()) {
					return UpdateApplyStatus(Error(errLogic, "Tx is empty"), rec.Type());
				}
				auto& data = std::get<QueryReplicationRecord>(*rec.Data());
				return UpdateApplyStatus(nsData.tx.Modify(Query::FromSQL(data.sql), lsn), rec.Type());
			}
			case updates::URType::SetTagsMatcherTx: {
				if (nsData.tx.IsFree()) {
					return UpdateApplyStatus(Error(errLogic, "Tx is empty"), rec.Type());
				}
				auto& data = std::get<TagsMatcherReplicationRecord>(*rec.Data());
				TagsMatcher tm = data.tm;
				return UpdateApplyStatus(nsData.tx.SetTagsMatcher(std::move(tm), lsn), rec.Type());
			}
			case updates::URType::AddNamespace: {
				auto& data = std::get<AddNamespaceReplicationRecord>(*rec.Data());
				const lsn_t recNsVersion = rec.ExtLSN().NsVersion();
				if (nsData.isClosed) {
					// Reopen after CloseNamespace. The record itself identifies the target incarnation
					lsn_t followerNsVersion = nsData.latestLsn.NsVersion();
					if (followerNsVersion.isEmpty()) {
						ReplicationStateV2 followerState;
						auto err = client.GetReplState(nsName, followerState);
						if (err.ok()) {
							followerNsVersion = followerState.nsVersion;
						} else if (err.code() != errNotFound) {
							return UpdateApplyStatus(std::move(err), rec.Type());
						}
					}
					if (followerNsVersion.isEmpty()) {
						// Missing on the follower — create it, then snapshot-resync.
						auto err =
							client.WithLSN(lsn_t(0, serverId_)).AddNamespace(*data.def, NsReplicationOpts{{data.stateToken}, recNsVersion});
						if (!err.ok()) {
							return UpdateApplyStatus(std::move(err), rec.Type());
						}
						logTrace("{}:{}:{} Namespace was reopened on leader. Scheduling resync for followers", serverId_, node.uid, nsName);
						return UpdateApplyStatus(Error(), updates::URType::ResyncNamespaceGeneric);
					}
					if (followerNsVersion == recNsVersion) {
						// Same incarnation already present (storage Close does not drop the follower).
						logTrace("{}:{}:{} Namespace already exists on follower with the same incarnation. Scheduling resync", serverId_,
								 node.uid, nsName);
						return UpdateApplyStatus(Error(), updates::URType::ResyncNamespaceGeneric);
					}
					// New incarnation (in-memory reopen)
					rollbackNamespaceTx(node, nsData);
					if (auto err = client.WithLSN(lsn_t(0, serverId_)).DropNamespace(nsName); !err.ok() && err.code() != errNotFound) {
						return UpdateApplyStatus(std::move(err), rec.Type());
					}
					nsData.syncedAtQueueId = 0;
				}
				auto err = client.WithLSN(lsn_t(0, serverId_)).AddNamespace(*data.def, NsReplicationOpts{{data.stateToken}, recNsVersion});
				if (err.ok()) {
					if constexpr (isClusterReplThread()) {
						// Namespace, replicated from the primary cluster, accepts its updates only after the ownership is set.
						// The follower creates the namespace in the primary state
						err = syncNamespaceOwnership(node, nsName, IsPrimary_True);
					} else {
						err = markNewRemoteNs(node, client, nsName);
					}
				}
				if (err.ok()) {
					nsData.isClosed = false;
				}
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::DropNamespace: {
				lsn.SetServer(serverId_);
				auto err = client.WithLSN(lsn).DropNamespace(nsName);
				nsData.isClosed = false;
				nsData.syncedAtQueueId = 0;
				if (!err.ok() && err.code() == errNotFound) {
					return UpdateApplyStatus(Error(), rec.Type());
				}
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::CloseNamespace: {
				// Record has no storage vs in-memory flag. Keep follower data here; in-memory Close ≡ Drop
				// is reconstructed on FullSync (GetNamespacePresence -> Absent).
				nsData.isClosed = true;
				logTrace("{}:{}:{} Namespace was closed on leader", serverId_, node.uid, nsName);
				return UpdateApplyStatus(Error(), rec.Type());
			}
			case updates::URType::RenameNamespace: {
				assert(false);	// TODO: Rename is not supported yet
				//				auto& data = std::get<RenameNamespaceReplicationRecord>(*rec.data);
				//				lsn.SetServer(serverId);
				//				return client.WithLSN(lsn).RenameNamespace(nsName, data->dstNsName);
				return UpdateApplyStatus(Error(), rec.Type());
			}
			case updates::URType::ResyncNamespaceGeneric:
			case updates::URType::ResyncNamespaceLeaderInit:
				return UpdateApplyStatus(Error(), rec.Type());
			case updates::URType::SetTagsMatcher: {
				auto& data = std::get<TagsMatcherReplicationRecord>(*rec.Data());
				TagsMatcher tm = data.tm;
				return UpdateApplyStatus(client.WithLSN(lsn).SetTagsMatcher(nsName, std::move(tm)), rec.Type());
			}
			case updates::URType::SaveShardingConfig: {
				auto& data = std::get<SaveNewShardingCfgRecord>(*rec.Data());
				sharding::ShardingControlResponseData res;
				auto err = client.WithLSN(lsn_t(0, serverId_))
							   .ShardingControlRequest({sharding::ControlCmdType::SaveCandidate, data.config, data.sourceId}, res);
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::ApplyShardingConfig: {
				auto& data = std::get<ApplyNewShardingCfgRecord>(*rec.Data());
				sharding::ShardingControlResponseData res;
				auto err =
					client.WithLSN(lsn_t(0, serverId_)).ShardingControlRequest({sharding::ControlCmdType::ApplyNew, data.sourceId}, res);
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::ResetOldShardingConfig: {
				auto& data = std::get<ResetShardingCfgRecord>(*rec.Data());
				sharding::ShardingControlResponseData res;
				auto err = client.WithLSN(lsn_t(0, serverId_))
							   .ShardingControlRequest({sharding::ControlCmdType::ResetOldSharding, data.sourceId}, res);
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::ResetCandidateConfig: {
				auto& data = std::get<ResetShardingCfgRecord>(*rec.Data());
				sharding::ShardingControlResponseData res;
				auto err = client.WithLSN(lsn_t(0, serverId_))
							   .ShardingControlRequest({sharding::ControlCmdType::ResetCandidate, data.sourceId}, res);
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::RollbackCandidateConfig: {
				auto& data = std::get<ResetShardingCfgRecord>(*rec.Data());
				sharding::ShardingControlResponseData res;
				auto err = client.WithLSN(lsn_t(0, serverId_))
							   .ShardingControlRequest({sharding::ControlCmdType::RollbackCandidate, data.sourceId}, res);
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::ApplyAsyncReplicationConfig:
				if constexpr (isClusterReplThread()) {
					const auto& data = std::get<AsyncReplicationCfgRecord>(*rec.Data());
					return UpdateApplyStatus(applyAsyncReplicationConfig(node, {.config = data.config, .sourceId = data.sourceId}),
											 rec.Type());
				} else {
					return UpdateApplyStatus(Error(), rec.Type());
				}

			case updates::URType::None:
			case updates::URType::EmptyUpdate:
			case updates::URType::ResyncOnUpdatesDrop:
			case updates::URType::NodeNetworkCheck:
				std::abort();
		}
	} catch (std::bad_variant_access& e) {
		assert(false);
		return Error(errLogic, "Bad variant access: {}", e.what());
	} catch (std::exception& err) {
		return Error(std::move(err), errLogic);
	}
	return Error();
}

template class ReplThread<ClusterThreadParam>;
template void ReplThread<ClusterThreadParam>::Run<ClusterNodeConfig>(ReplThreadConfig,
																	 const std::vector<std::pair<uint32_t, ClusterNodeConfig>>&, size_t,
																	 size_t);
template class ReplThread<AsyncThreadParam>;
template void ReplThread<AsyncThreadParam>::Run<AsyncReplNodeConfig>(ReplThreadConfig,
																	 const std::vector<std::pair<uint32_t, AsyncReplNodeConfig>>&, size_t,
																	 size_t);

}  // namespace cluster
}  // namespace reindexer
