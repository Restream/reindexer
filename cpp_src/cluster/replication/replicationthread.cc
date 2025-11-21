#include "asyncreplthread.h"
#include "cluster/consts.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "clusterreplthread.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "estl/gift_str.h"
#include "sharedsyncstate.h"
#include "tools/catch_and_return.h"
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
using updates::RenameNamespaceReplicationRecord;
using updates::NodeNetworkCheckRecord;
using updates::SaveNewShardingCfgRecord;
using updates::ApplyNewShardingCfgRecord;
using updates::ResetShardingCfgRecord;

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
	if (connObserverId.has_value()) {
		auto err = client.RemoveConnectionStateObserver(*connObserverId);
		(void)err;	// ignored
		connObserverId.reset();
	}
	client.Stop();
	const auto opts = client::ConnectOpts().CreateDBIfMissing().WithExpectedClusterID(config.ClusterID);
	auto err = client.Connect(dsn, loop, opts);
	(void)err;	// ignored; Error will be checked during the further requests
}
}  // namespace repl_thread_impl

template <typename BehaviourParamT>
bool UpdateApplyStatus::IsHaveToResync() const noexcept {
	static_assert(std::is_same_v<BehaviourParamT, AsyncThreadParam> || std::is_same_v<BehaviourParamT, ClusterThreadParam>,
				  "Unexpected param type");
	if constexpr (std::is_same_v<BehaviourParamT, ClusterThreadParam>) {
		return type == updates::URType::ResyncNamespaceGeneric || type == updates::URType::ResyncOnUpdatesDrop;
	} else {
		return type == updates::URType::ResyncNamespaceGeneric || type == updates::URType::ResyncNamespaceLeaderInit ||
			   type == updates::URType::ResyncOnUpdatesDrop;
	}
}

template <typename BehaviourParamT>
ReplThread<BehaviourParamT>::ReplThread(int serverId, ReindexerImpl& _thisNode, std::shared_ptr<UpdatesQueueT> shard,
										BehaviourParamT&& bhvParam, ReplicationStatsCollector statsCollector, const Logger& l)
	: thisNode(_thisNode),
	  serverId_(serverId),
	  bhvParam_(std::move(bhvParam)),
	  updates_(std::move(shard)),
	  statsCollector_(statsCollector),
	  log_(l) {
	assert(updates_);
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
		log_.Trace([this] { rtfmt("{}: new updates notification (on timer)", serverId_); });
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
					log_.Trace([this] { rtfmt("{}: new updates notification (delaying...)", serverId_); });
					updatesTimer_.start(config_.OnlineUpdatesDelaySec);
				} else {
					log_.Trace([this] { rtfmt("{}: new updates notification (on async)", serverId_); });
					spawnNotifierRoutine();
				}
			}
		} else {
			notificationInProgress_ = true;
			log_.Trace([this] { rtfmt("{}: new terminate notification", serverId_); });
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

	// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
	loop.spawn([this, &nodesList]() noexcept {
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
		for (const auto& nodeP : nodesList) {
			nodes.emplace_back(nodeP.second.GetServerID(), nodeP.first, rpcCfg);
			nodes.back().dsn = nodeP.second.GetRPCDsn();
		}

		bhvParam_.AwaitReplPermission();
		if (!terminate_) {
			log_.Info([this] {
				std::string nodesString;
				for (size_t i = 0; i < nodes.size(); ++i) {
					if (i > 0) {
						nodesString.append(", ");
					}
					nodesString.append(fmt::format("Node {} - server ID {}", nodes[i].uid, nodes[i].serverId));
				}
				rtfmt("{}: starting dataReplicationThread. Nodes:'{}'", serverId_, nodesString);
			});
			updates_->AddDataNotifier(std::this_thread::get_id(), [this] { updatesAsync_.send(); });

			for (size_t i = 0; i < nodes.size(); ++i) {
				// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
				loop.spawn(wg, [this, i]() noexcept {
					// 3) Perform wal-sync/force-sync for each follower
					nodeReplicationRoutine(nodes[i]);
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

	log_.Info([this] {
		rtfmt("{}: Replication thread was terminated. TID: {}", serverId_,
			  static_cast<size_t>(std::hash<std::thread::id>()(std::this_thread::get_id())));
	});
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
		loop.spawn(
			swg,
			[&node]() noexcept {
				if (node.connObserverId.has_value()) {
					auto err = node.client.RemoveConnectionStateObserver(*node.connObserverId);
					(void)err;	// ignore
					node.connObserverId.reset();
				}
				node.client.Stop();
			},
			k16kCoroStack);
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
void ReplThread<BehaviourParamT>::nodeReplicationRoutine(Node& node) {
	Error err;
	bool expectingReconnect = true;
	while (!terminate_) {
		statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::AwaitingResync);
		bhvParam_.AwaitReplPermission();
		if (terminate_) {
			break;
		}
		if (expectingReconnect && (!err.ok() || !node.client.WithTimeout(kStatusCmdTimeout).Status(true).ok())) {
			logInfo("{}:{} Reconnecting... Reason: {}", serverId_, node.uid, err.ok() ? "Not connected yet" : ("Error: " + err.whatStr()));
			node.Reconnect(loop, config_);
		}
		LogLevel logLevel = LogTrace;
		err = checkIfReplicationAllowed(node, logLevel);
		statsCollector_.SaveNodeError(node.uid, err);  // Reset last node error after checking node replication allowance
		if (err.ok()) {
			expectingReconnect = true;
			if (!node.connObserverId.has_value()) {
				// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
				auto connObserverId = node.client.AddConnectionStateObserver([this, &node](const Error& err) noexcept {
					if (!err.ok() && updates_ && !terminate_) {
						logInfo("{}:{} Connection error: {}", serverId_, node.uid, err.whatStr());
						UpdatesContainer recs;
						recs.emplace_back(updates::URType::NodeNetworkCheck, node.uid, false);
						node.requireResync = true;
						std::pair<Error, bool> res = updates_->template PushAsync<true>(std::move(recs));
						if (!res.first.ok()) {
							logWarn("Error while Pushing updates queue: {}", res.first.what());
						}
					}
				});
				if (connObserverId.has_value()) {
					node.connObserverId = connObserverId.value();
				} else {
					err = connObserverId.error();
				}
			}
			if (err.ok()) {
				err = nodeReplicationImpl(node);
			}
			statsCollector_.SaveNodeError(node.uid, err);
		} else {
			expectingReconnect = false;
			log_.Log(logLevel, [&] { rtfmt("{}:{} Replication is not allowed: {}", serverId_, node.uid, err.whatStr()); });
		}
		// Wait before next sync retry
		constexpr auto kGranularSleepInterval = std::chrono::milliseconds(150);
		auto awaitTime = isTxCopyError(err) ? kAwaitNsCopyInterval : std::chrono::milliseconds(config_.RetrySyncIntervalMSec);
		if (!terminate_) {
			if (err.ok()) {
				logTrace("{}:{} Doing resync...", serverId_, node.uid);
				continue;
			}
			bhvParam_.OnNodeBecameUnsynchonized(node.uid);
			updateNodeStatus(node.uid, NodeStats::Status::Offline);
			statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::AwaitingResync);
		}
		while (!terminate_ && awaitTime.count() > 0) {
			const auto diff = std::min(awaitTime, kGranularSleepInterval);
			awaitTime -= diff;
			loop.sleep(diff);

			if (isTimeoutError(err)) {
				break;
			}
			if (isNetworkError(err) || isLeaderChangedError(err)) {
				const bool retrySync = handleUpdatesWithError(node, err);
				if (retrySync) {
					break;
				}
			}
		}
	}
	if (terminate_) {
		logTrace("{}:{} Node replication routine was terminated", serverId_, node.uid);
	}
	if (node.connObserverId.has_value()) {
		auto err = node.client.RemoveConnectionStateObserver(*node.connObserverId);
		(void)err;	// ignore; Error does not matter here
		node.connObserverId.reset();
	}
	node.client.Stop();
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
	}
	CATCH_AND_RETURN
}

template <typename BehaviourParamT>
bool ReplThread<BehaviourParamT>::needForceSyncOnLogicError(const Error& err) const noexcept {
	return config_.ForceSyncOnLogicError && !isNetworkError(err) && !isTimeoutError(err);
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::nodeReplicationImpl(Node& node) {
	std::vector<NamespaceDef> nsList;
	node.requireResync = false;
	logTrace("{}:{} Trying to collect local namespaces...", serverId_, node.uid);
	const RdxDeadlineContext deadlineCtx(kLocalCallsTimeout);
	auto integralError = thisNode.EnumNamespaces(nsList, EnumNamespacesOpts().OnlyNames().HideSystem().HideTemporary(),
												 RdxContext().WithCancelCtx(deadlineCtx));
	if (!integralError.ok()) {
		logWarn("{}:{} Unable to enum local namespaces in node replication routine: {}", serverId_, node.uid, integralError.what());
		return integralError;
	}

	logTrace("{}:{} Performing ns data cleanup...", serverId_, node.uid);
	for (auto nsDataIt = node.namespaceData.begin(); nsDataIt != node.namespaceData.end();) {
		if (!nsDataIt->second.tx.IsFree()) {
			auto err = node.client.WithLSN(lsn_t(0, serverId_)).RollBackTransaction(nsDataIt->second.tx);
			logInfo("{}:{} Rollback transaction result: {}", serverId_, node.uid,
					err.ok() ? "OK" : ("Error:" + std::to_string(err.code()) + ". " + err.whatStr()));
			nsDataIt->second.tx = client::CoroTransaction();
		}
		if (nsDataIt->second.isClosed) {
			nsDataIt->second.requiresTmUpdate = true;
			++nsDataIt;
		} else {
			nsDataIt = node.namespaceData.erase(nsDataIt);
		}
	}

	if constexpr (isClusterReplThread()) {
		integralError = syncShardingConfig(node);
		if (!integralError.ok()) {
			logWarn("{}", integralError.what());
			return integralError;
		}
	}

	logInfo("{}:{} Creating {} sync routines", serverId_, node.uid, nsList.size());
	coroutine::wait_group localWg;
	for (const auto& ns : nsList) {
		if (!bhvParam_.IsNamespaceInConfig(node.uid, ns.name)) {
			continue;
		}
		logTrace("{}:{} Creating sync routine for {}", serverId_, node.uid, ns.name);
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop.spawn(localWg, [this, &integralError, &node, &ns]() mutable noexcept {
			// 3.1) Perform wal-sync/force-sync for specified namespace in separated routine
			ReplicationStateV2 replState;
			Error err;
			size_t i = 0;
			for (i = 0; i < kMaxRetriesOnRoleSwitchAwait; ++i) {
				err = node.client.GetReplState(ns.name, replState);
				bool nsExists = true;
				if (err.code() == errNotFound) {
					nsExists = false;
					logInfo("{}:{} Namespace does not exist on remote node. Trying to get repl state for whole DB", serverId_, node.uid);
					err = node.client.GetReplState(std::string_view(), replState);
				}
				if (!bhvParam_.IsLeader() && integralError.ok()) {
					integralError = Error(errParams, "Leader was switched");
					return;
				} else if (!integralError.ok()) {
					return;
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
					} else {
						if (nsExists && (replState.clusterStatus.role != ClusterOperationStatus::Role::SimpleReplica ||
										 replState.clusterStatus.leaderId != serverId_)) {
							logTrace("{}:{} Switching role for '{}' on remote node", serverId_, node.uid, ns.name);
							err = node.client.WithEmitterServerId(serverId_).SetClusterOperationStatus(
								ns.name, ClusterOperationStatus{serverId_, ClusterOperationStatus::Role::SimpleReplica});
							if (err.ok()) {
								// Renew remote replState after role switch to avoid races between remote's users and replication
								err = node.client.GetReplState(ns.name, replState);
							}
						}
					}
				} else {
					logWarn("{}:{} Unable to get repl state: {}", serverId_, node.uid, err.whatStr());
				}

				if (err.ok()) {
					if (!nsExists) {
						replState = ReplicationStateV2();
					}
					const NamespaceName nsName(ns.name);
					err = syncNamespace(node, nsName, replState);
					if (!err.ok()) {
						logWarn("{}:{}:{} Namespace sync error: {}", serverId_, node.uid, nsName, err.whatStr());
						if (err.code() == errNotFound) {
							err = Error();
							logWarn("{}:{} Expecting drop namespace record for '{}'", serverId_, node.uid, nsName);
						} else if (err.code() == errDataHashMismatch || needForceSyncOnLogicError(err)) {
							replState = ReplicationStateV2();
							err = syncNamespace(node, nsName, replState);
							if (!err.ok()) {
								logWarn("{}:{}:{} Namespace sync error (resync due to {}): {}", serverId_, node.uid, nsName,
										err.code() == errDataHashMismatch ? "datahash missmatch" : "logic error", err.whatStr());
							}
						}
					}
				}
				if (!err.ok() && integralError.ok()) {
					integralError = std::move(err);
				}
				return;
			}

			if (integralError.ok()) {
				integralError = Error(errTimeout, "{}:{}:{} Unable to sync namespace", serverId_, node.uid, ns.name);
				return;
			}
		});
	}
	localWg.wait();
	if (!integralError.ok()) {
		logError("{}:{} Unable to sync remote namespaces: {}", serverId_, node.uid, integralError.what());
		return integralError;
	}
	updateNodeStatus(node.uid, NodeStats::Status::Online);
	statsCollector_.OnSyncStateChanged(node.uid, NodeStats::SyncState::OnlineReplication);

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
Error ReplThread<BehaviourParamT>::syncNamespace(Node& node, const NamespaceName& nsName, const ReplicationStateV2& followerState) {
	try {
		class [[nodiscard]] TmpNsGuard {
		public:
			TmpNsGuard(client::CoroReindexer& client, int serverId, const Logger& log) : client_(client), serverId_(serverId), log_(log) {}
			// NOLINTNEXTLINE(bugprone-exception-escape) Exceptions in logging are unlikely
			~TmpNsGuard() {
				if (tmpNsName.size()) {
					logWarn("{}: Dropping tmp ns on error: '{}'", serverId_, tmpNsName);
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

		coroutine::tokens_pool<bool>::token syncToken;
		if (nsSyncTokens_) {
			logTrace("{}:{}:{} Awaiting sync token", serverId_, node.uid, nsName);
			syncToken = nsSyncTokens_->await_token();
			logTrace("{}:{}:{} Got sync token", serverId_, node.uid, nsName);
		}
		if (!bhvParam_.IsLeader()) {
			return Error(errParams, "Leader was switched");
		}
		SyncTimeCounter timeCounter(SyncTimeCounter::Type::WalSync, statsCollector_);

		ReplicationStateV2 localState;
		Snapshot snapshot;
		ExtendedLsn requiredLsn(followerState.nsVersion, followerState.lastLsn);
		bool createTmpNamespace = false;
		auto client = node.client.WithTimeout(std::chrono::seconds(config_.SyncTimeoutSec));
		TmpNsGuard tmpNsGuard{client, serverId_, log_};

		RdxDeadlineContext deadlineCtx(kLocalCallsTimeout);
		auto err = thisNode.GetReplState(nsName, localState, RdxContext().WithCancelCtx(deadlineCtx));
		if (!err.ok()) {
			if (err.code() == errNotFound) {
				if (requiredLsn.IsEmpty()) {
					logInfo("{}:{}: Namespace '{}' does not exist on both follower and leader", serverId_, node.uid, nsName);
					return Error();
				}
				if (node.namespaceData[nsName].isClosed) {
					logInfo("{}:{} Namespace '{}' is closed on leader. Skipping it", serverId_, node.uid, nsName);
					return Error();
				} else {
					logInfo(
						"{}:{} Namespace '{}' does not exist on leader, but exist on follower. Trying to "
						"remove it...",
						serverId_, node.uid, nsName);
					auto dropRes = client.WithLSN(lsn_t(0, serverId_)).DropNamespace(nsName);
					if (dropRes.ok()) {
						logInfo("{}:{} Namespace '{}' was removed", serverId_, node.uid, nsName);
					} else {
						logInfo("{}:{} Unable to remove namespace '{}': {}", serverId_, node.uid, nsName, dropRes.what());
						return dropRes;
					}
				}
			}
			return err;
		}
		const ExtendedLsn localLsn(localState.nsVersion, localState.lastLsn);

		logInfo(
			"{}:{} ReplState for '{}': {{ local: {{ ns_version: {}, lsn: {}, data_hash: {} }}, remote: {{ "
			"ns_version: {}, lsn: {}, data_hash: {} }} }}",
			serverId_, node.uid, nsName, localState.nsVersion, localState.lastLsn, localState.dataHash, followerState.nsVersion,
			followerState.lastLsn, followerState.dataHash);

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
			} else if (requiredLsn.LSN() == localLsn.LSN() && followerState.dataHash != localState.dataHash) {
				logWarn("{}:{}:{} Datahash missmatch. Expected: {}, actual: {}", serverId_, node.uid, nsName, localState.dataHash,
						followerState.dataHash);
				requiredLsn = ExtendedLsn();
			}
		}

		deadlineCtx = RdxDeadlineContext(kLocalCallsTimeout);
		err = thisNode.GetSnapshot(nsName, SnapshotOpts(requiredLsn, config_.MaxWALDepthOnForceSync), snapshot,
								   RdxContext().WithCancelCtx(deadlineCtx));
		if (!err.ok()) {
			return err;
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
			if constexpr (std::is_same_v<BehaviourParamT, AsyncThreadParam>) {
				err = client.WithEmitterServerId(serverId_).SetClusterOperationStatus(
					tmpNsGuard.tmpNsName, ClusterOperationStatus{serverId_, ClusterOperationStatus::Role::SimpleReplica});
				if (!err.ok()) {
					return err;
				}
			}
			replNsName = tmpNsGuard.tmpNsName;
		} else {
			replNsName = nsName;
		}
		logInfo("{}:{}:{} Target ns name: {}", serverId_, node.uid, nsName, replNsName);
		for (auto& it : snapshot) {
			if (terminate_) {
				logInfo("{}:{}:{} Terminated, while syncing namespace", serverId_, node.uid, nsName);
				return Error();
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
			"{}:{}:{} Sync done. {{ snapshot: {{ ns_version: {}, lsn: {}, data_hash: {}, data_count: {} }}, remote: {{ ns_version: {}, "
			"lsn: "
			"{}, data_hash: {}, data_count: {} }} }}",
			serverId_, node.uid, nsName, snapshot.NsVersion(), snapshot.LastLSN(), snapshot.ExpectedDataHash(),
			snapshot.ExpectedDataCount(), replState.nsVersion, replState.lastLsn, replState.dataHash, replState.dataCount);

		const bool versionMissmatch = (!snapshot.LastLSN().isEmpty() && snapshot.LastLSN() != replState.lastLsn) ||
									  (!snapshot.NsVersion().isEmpty() && snapshot.NsVersion() != replState.nsVersion);
		if (versionMissmatch || snapshot.ExpectedDataHash() != replState.dataHash ||
			(replState.HasDataCount() && snapshot.ExpectedDataCount() != uint64_t(replState.dataCount))) {
			logInfo("{}:{}:{} Snapshot dump on data missmatch: {}", serverId_, node.uid, nsName, snapshot.Dump());
			return Error(
				errDataHashMismatch,
				"{}:{}:{}: Datahash or datacount missmatcher after sync. Actual: {{ data_hash: {}, data_count: {}, ns_version: {}, "
				"lsn: {} }}; expected: {{ data_hash: {}, data_count: {}, ns_version: {}, lsn: {} }}",
				serverId_, node.uid, nsName, replState.dataHash, replState.dataCount, replState.nsVersion, replState.lastLsn,
				snapshot.ExpectedDataHash(), snapshot.ExpectedDataCount(), snapshot.NsVersion(), snapshot.LastLSN());
		}

		node.namespaceData[nsName].latestLsn = ExtendedLsn(replState.nsVersion, replState.lastLsn);
		if (createTmpNamespace) {
			logInfo("{}:{}:{} Renaming: {} -> {}", serverId_, node.uid, nsName, replNsName, nsName);
			err = client.WithLSN(lsn_t(0, serverId_)).RenameNamespace(replNsName, nsName);
			if (!err.ok()) {
				return err;
			}
			tmpNsGuard.tmpNsName.clear();
		}
	} catch (Error& err) {
		return err;
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
		log_.Trace([&] {
			auto& nsName = it.NsName();
			auto& nsData = *ctx.nsData;
			rtfmt(
				"{}:{}:{} Applying update with type {} (batched), id: {}, ns version: {}, lsn: {}, last synced ns "
				"version: {}, last synced lsn: {}",
				serverId_, node.uid, nsName, int(it.Type()), ctx.updPtr->ID() + ctx.offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
				nsData.latestLsn.NsVersion(), nsData.latestLsn.LSN());
		});
		return applyUpdate(it, node, *ctx.nsData);
	};
	auto onUpdateResF = [this, &node, &requireReelections](const UpdatesQueueT::UpdateT::Value& upd, const UpdateApplyStatus& res,
														   Context&& ctx) {
		auto& it = upd.Data();
		ctx.nsData->UpdateLsnOnRecord(it);
		log_.Trace([&] {
			const auto counters = upd.GetCounters();
			rtfmt("{}:{}:{} Apply update (lsn: {}, id: {}) result: {}. Replicas: {}", serverId_, node.uid, it.NsName(), it.ExtLSN().LSN(),
				  ctx.updPtr->ID() + ctx.offset, (res.err.ok() ? "OK" : "ERROR:" + res.err.whatStr()), counters.replicas + 1);
		});
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

				auto& nsData = node.namespaceData[nsName];
				const bool isOutdatedRecord = !it.ExtLSN().HasNewerCounterThan(nsData.latestLsn) || nsData.latestLsn.IsEmpty();
				if ((!it.IsDbRecord() && isOutdatedRecord) || it.IsEmptyRecord()) {
					logTrace(
						"{}:{}:{} Skipping update with type {}, id: {}, ns version: {}, lsn: {}, last synced ns "
						"version: {}, last synced lsn: {}",
						serverId_, node.uid, nsName, int(it.Type()), updatePtr->ID() + offset, it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
						nsData.latestLsn.NsVersion(), nsData.latestLsn.LSN());
					std::ignore = updatePtr->OnUpdateHandled(node.uid, consensusCnt_, requiredReplicas_, offset,
															 it.EmitterServerID() == node.serverId, Error());
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
					client::CoroQueryResults qr;
					res = node.client.WithShardId(ShardingKeyType::ProxyOff, false).Select(Query(nsName).Limit(0), qr);
					if (!res.err.ok()) {
						--node.nextUpdateId;  // Have to read this update again
						break;
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
				} else if (res.IsHaveToResync<BehaviourParamT>()) {
					logTrace("{}:{} Resync was requested", serverId_, node.uid);
					break;
				}
			}

			if (batcher.BatchedUpdatesCount()) {
				assert(!res.IsHaveToResync<BehaviourParamT>());	 // In this cases batchedUpdatesCount has to be 0
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
			if (!bhvParam_.IsNamespaceInConfig(node.uid, nsName)) {
				continue;
			}

			if (it.Type() == updates::URType::AddNamespace || it.Type() == updates::URType::DropNamespace) {
				node.namespaceData[nsName].isClosed = false;
				bhvParam_.OnNewNsAppearance(nsName);
			} else if (it.Type() == updates::URType::CloseNamespace) {
				node.namespaceData[nsName].isClosed = true;
			}

			if (updatePtr->IsInvalidated()) {
				logTrace("{}:{}:{} Update {} was invalidated", serverId_, node.uid, nsName, updatePtr->ID());
				break;
			}

			assert(it.EmitterServerID() != serverId_);
			const bool isEmitter = it.EmitterServerID() == node.serverId;
			if (isEmitter) {
				--node.nextUpdateId;
				return true;  // Retry sync after receiving update from offline node
			}
			const auto replRes = updatePtr->OnUpdateHandled(
				node.uid, consensusCnt_, requiredReplicas_, offset, isEmitter,
				Error(errUpdateReplication, "Unable to send update to enough amount of replicas. Last error: {}", err.whatStr()));

			if (replRes == updates::ReplicationResult::Error && !hadErrorOnLastUpdate) {
				hadErrorOnLastUpdate = true;
				logWarn("{}:{} Requesting leader reelection on error: {}", serverId_, node.uid, err.whatStr());
				bhvParam_.OnUpdateReplicationFailure();
			}

			log_.Trace([&] {
				const auto counters = upd.GetCounters();
				rtfmt(
					"{}:{}:{} Dropping update with error: {}. Type {}, ns version: {}, lsn: {}, emitter: {}. "
					"Required: "
					"{}, succeed: "
					"{}, failed: {}, replicas: {}",
					serverId_, node.uid, nsName, err.whatStr(), int(it.Type()), it.ExtLSN().NsVersion(), it.ExtLSN().LSN(),
					(isEmitter ? node.serverId : it.EmitterServerID()), consensusCnt_, counters.approves, counters.errors,
					counters.replicas + 1);
			});
		}
	} while (!terminate_);
	return false;
}

template <typename BehaviourParamT>
Error ReplThread<BehaviourParamT>::checkIfReplicationAllowed(Node& node, LogLevel& logLevel) {
	if constexpr (!isClusterReplThread()) {
		auto err = bhvParam_.CheckReplicationMode(node.uid);
		if (!err.ok()) {
			logLevel = LogTrace;
			return err;
		}
		logLevel = LogError;
		logWarn("{}:{} Checking if replication is allowed for this node", serverId_, node.uid);
		const Query q = Query(std::string(kReplicationStatsNamespace)).Where("type", CondEq, Variant(cluster::kClusterReplStatsType));
		client::CoroQueryResults qr;
		err = node.client.Select(q, qr);
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
	} else {
		(void)node;
		(void)logLevel;
	}
	return Error();
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
				const auto sid = rec.ExtLSN().NsVersion().Server();
				auto err =
					client.WithLSN(lsn_t(0, sid)).AddNamespace(*data.def, NsReplicationOpts{{data.stateToken}, rec.ExtLSN().NsVersion()});
				if (err.ok() && nsData.isClosed) {
					nsData.isClosed = false;
					logTrace("{}:{}:{} Namespace is closed on leader. Scheduling resync for followers", serverId_, node.uid, nsName);
					return UpdateApplyStatus(Error(), updates::URType::ResyncNamespaceGeneric);	 // Perform resync on ns reopen
				}
				nsData.isClosed = false;
				if constexpr (!isClusterReplThread()) {
					if (err.ok()) {
						err = client.WithEmitterServerId(serverId_).SetClusterOperationStatus(
							nsName, ClusterOperationStatus{serverId_, ClusterOperationStatus::Role::SimpleReplica});
					}
				}
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::DropNamespace: {
				lsn.SetServer(serverId_);
				auto err = client.WithLSN(lsn).DropNamespace(nsName);
				nsData.isClosed = false;
				if (!err.ok() && err.code() == errNotFound) {
					return UpdateApplyStatus(Error(), rec.Type());
				}
				return UpdateApplyStatus(std::move(err), rec.Type());
			}
			case updates::URType::CloseNamespace: {
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

			case updates::URType::None:
			case updates::URType::EmptyUpdate:
			case updates::URType::ResyncOnUpdatesDrop:
			case updates::URType::NodeNetworkCheck:
				std::abort();
		}
	} catch (std::bad_variant_access& e) {
		assert(false);
		return Error(errLogic, "Bad variant access: {}", e.what());
	} catch (const std::exception& err) {
		return Error(err, errLogic);
	} catch (...) {
		assert(false);
		return Error(errLogic, "Unknown exception during UpdateRecord handling");
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
