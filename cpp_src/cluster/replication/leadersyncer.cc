#include "leadersyncer.h"
#include "client/snapshot.h"
#include "cluster/logger.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "estl/gift_str.h"
#include "vendor/gason/gason.h"

namespace reindexer::cluster {

// Some large value to avoid endless replicator lock in case of some core issues
constexpr static auto kLocalCallsTimeout = std::chrono::seconds(300);

Error LeaderSyncer::Sync(elist<LeaderSyncQueue::Entry>&& entries, SharedSyncState& sharedSyncState, ReindexerImpl& thisNode,
						 ReplicationStatsCollector statsCollector) {
	Error err;
	const LeaderSyncThread::Config thCfg{cfg_.dsns,		cfg_.maxWALDepthOnForceSync, cfg_.clusterId,
										 cfg_.serverId, cfg_.enableCompression,		 cfg_.netTimeout};
	unique_lock lck(mtx_);
	syncQueue_.Refill(std::move(entries));
	assert(threads_.empty());
	std::once_flag onceUpdShardingCfg;
	for (size_t i = 0; i < cfg_.threadsCount; ++i) {
		threads_.emplace_back(thCfg, syncQueue_, sharedSyncState, thisNode, statsCollector, log_, onceUpdShardingCfg);
	}
	lck.unlock();

	for (auto& th : threads_) {
		th.Join();
		if (err.ok()) {
			err = th.LastError();
		}
	}

	bool wasTerminated = false;
	for (auto& th : threads_) {
		if (th.IsTerminated()) {
			wasTerminated = true;
			break;
		}
	}
	if (!wasTerminated) {
		assert(syncQueue_.Size() == 0);
	}

	lck.lock();
	threads_.clear();
	return err;
}

template <typename T>
static auto makeClusterSet(const std::vector<T>& cluster) {
	std::vector<std::reference_wrapper<const DSN>> res;
	res.reserve(cluster.size());
	for (size_t i = 0; i < cluster.size(); ++i) {
		if constexpr (std::is_same_v<T, cluster::ClusterNodeConfig>) {
			res.emplace_back(cluster[i].dsn);
		} else {
			res.emplace_back(cluster[i]);
		}
	}
	std::sort(res.begin(), res.end(), DSN::RefWrapperCompare{});
	return res;
}

void LeaderSyncThread::actualizeShardingConfig() {
	auto cluster = makeClusterSet(thisNode_.clusterConfig_->nodes);
	auto isClusterEqualSomeShard = [&cluster](const cluster::ShardingConfig& config) {
		return std::any_of(config.shards.begin(), config.shards.end(),
						   [&cluster](const auto& pair) { return cluster == makeClusterSet(pair.second); });
	};

	cluster::ShardingConfig config;
	if (auto configPtr = thisNode_.shardingConfig_.Get()) {
		config = *configPtr;
	}

	bool updated = false;
	client::ReindexerConfig clCfg;
	clCfg.AppName = "leader_sync-sharding_cfg_syncer";
	clCfg.EnableCompression = false;
	clCfg.RequestDedicatedThread = true;
	for (const auto& dsn : cfg_.dsns) {
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
		loop_.spawn([&]() noexcept {
			try {
				client::CoroReindexer client(clCfg);
				auto err = client.Connect(dsn, loop_, client::ConnectOpts().WithExpectedClusterID(cfg_.clusterId));
				if (!err.ok()) {
					logWarn("{}: Actualization sharding config error: {}", dsn, err.what());
					return;
				}

				sharding::ShardingControlResponseData response;
				err = client.WithLSN(lsn_t{0}).ShardingControlRequest({sharding::ControlCmdType::GetNodeConfig}, response);
				if (!err.ok()) {
					logWarn("{}: Actualization sharding config error: {}", dsn, err.what());
					return;
				}

				cluster::ShardingConfig nodeConfig = std::get<sharding::GetNodeConfigCommand>(response.data).config;

				if (!isClusterEqualSomeShard(nodeConfig)) {
					logWarn("{}: Different sets of nodes of the obtained config and the current cluster", dsn);
					return;
				}

				if ((nodeConfig.sourceId & ~cluster::ShardingConfig::serverIdMask) >
					(config.sourceId & ~cluster::ShardingConfig::serverIdMask)) {
					config = std::move(nodeConfig);
					updated = true;
				}
			} catch (const Error& err) {
				logWarn("{}: Actualization sharding config error: {}", dsn, err.what());
			} catch (...) {
				logWarn("{}: Unexpected exception during actualization sharding config", dsn);
			}
		});
	}

	loop_.run();

	if (updated) {
		using CallbackT = ReindexerImpl::CallbackT;
		gason::JsonParser parser;
		this->thisNode_.proxyCallbacks_.at({"leader_config_process", CallbackT::Type::System})(
			parser.Parse(giftStr(config.GetJSON(cluster::MaskingDSN::Disabled))), CallbackT::SourceIdT{config.sourceId}, {});
	}
}

LeaderSyncThread::LeaderSyncThread(const Config& cfg, LeaderSyncQueue& syncQueue, SharedSyncState& sharedSyncState, ReindexerImpl& thisNode,
								   ReplicationStatsCollector statsCollector, const Logger& l, std::once_flag& actShardingCfg)
	: cfg_(cfg),
	  syncQueue_(syncQueue),
	  sharedSyncState_(sharedSyncState),
	  thisNode_(thisNode),
	  statsCollector_(statsCollector),
	  client_(client::ReindexerConfig{10000, 0, cfg_.netTimeout, cfg_.enableCompression, true, "cluster_leader_syncer"}),
	  log_(l),
	  actShardingCfg_(actShardingCfg) {
	terminateAsync_.set(loop_);
	terminateAsync_.set([this](net::ev::async&) noexcept { client_.Stop(); });
	// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Currently there are no good ways to recover, crash is intended
	thread_ = std::thread([this]() noexcept { sync(); });
}

void LeaderSyncThread::sync() {
	std::call_once(actShardingCfg_, &LeaderSyncThread::actualizeShardingConfig, this);

	loop_.spawn([this] {
		LeaderSyncQueue::Entry entry;
		uint32_t idx = 0;
		int32_t preferredNodeId = -1;
		while (syncQueue_.TryToGetEntry(preferredNodeId, entry, idx)) {
			const uint32_t nodeId = entry.nodes[idx];
			if (preferredNodeId != int32_t(nodeId)) {
				preferredNodeId = int32_t(nodeId);
				client_.Stop();
			}
			const auto& node = entry.data[idx];
			const uint64_t expectedDataHash = node.hash;
			const int64_t expectedDataCount = node.count;
			logInfo("{}: Trying to sync ns '{}' from {} (TID: {})", cfg_.serverId, entry.nsName, nodeId,
					static_cast<size_t>(std::hash<std::thread::id>()(std::this_thread::get_id())));
			std::string tmpNsName;
			auto tryDropTmpNamespace = [this, &tmpNsName] {
				if (!tmpNsName.empty()) {
					logError("{}: Dropping '{}'...", cfg_.serverId, tmpNsName);
					if (auto err = thisNode_.DropNamespace(tmpNsName, RdxContext()); err.ok()) {
						logError("{}: '{}' was dropped", cfg_.serverId, tmpNsName);
					} else {
						logError("{}: '{}' drop error: {}", cfg_.serverId, tmpNsName, err.what());
					}
				}
			};
			try {
				// 2) Recv most recent data
				auto err = client_.Connect(cfg_.dsns[nodeId], loop_, client::ConnectOpts().WithExpectedClusterID(cfg_.clusterId));
				if (!err.ok()) {
					throw err;
				}
				for (int retry = 0; retry < 2; ++retry) {
					const bool fullResync = retry > 0;
					syncNamespaceImpl(fullResync, entry, tmpNsName);
					ReplicationStateV2 state;
					err = thisNode_.GetReplState(tmpNsName.empty() ? std::string_view(entry.nsName) : tmpNsName, state, RdxContext());
					if (!err.ok()) {
						throw err;
					}
					const auto localLsn = ExtendedLsn(state.nsVersion, state.lastLsn);
					if (state.dataHash == expectedDataHash && (expectedDataCount < 0 || expectedDataCount == state.dataCount)) {
						if (!tmpNsName.empty()) {
							err = thisNode_.renameNamespace(tmpNsName, std::string(entry.nsName), true, true);
							if (!err.ok()) {
								throw err;
							}
						}
						logInfo("{}: Local namespace '{}' was updated from node {} (ns version: {}, lsn: {})", cfg_.serverId, entry.nsName,
								nodeId, localLsn.NsVersion(), localLsn.LSN());
						break;
					}

					if (fullResync) {
						throw Error(
							errDataHashMismatch,
							"{}: Datahash or datacount missmatch after full resync for local namespace '{}'. Expected: {{ datahash: "
							"{}, datacount: {} }}; actual: {{ datahash: {}, datacount: {} }}",
							cfg_.serverId, entry.nsName, expectedDataHash, expectedDataCount, state.dataHash, state.dataCount);
					}
					logWarn(
						"{}: Datahash missmatch after local namespace '{}' sync. Expected: {{ datahash: {}, datacount: {} }}; actual: {{ "
						"datahash: {}, datacount: {} }}. Forcing full resync...",
						cfg_.serverId, entry.nsName, expectedDataHash, expectedDataCount, state.dataHash, state.dataCount);
					tryDropTmpNamespace();
				}
				sharedSyncState_.MarkSynchronized(entry.nsName);
			} catch (const Error& err) {
				lastError_ = err;
				logError("{}: Unable to sync local namespace '{}': {}", cfg_.serverId, entry.nsName, lastError_.what());
				tryDropTmpNamespace();
			} catch (...) {
				lastError_ = Error(errLogic, "Unexpected exception");
				logError("{}: Unable to sync local namespace '{}': {}", cfg_.serverId, entry.nsName, lastError_.what());
				tryDropTmpNamespace();
			}
			syncQueue_.SyncDone(nodeId);
			client_.Stop();
		}
	});
	loop_.run();
}

void LeaderSyncThread::syncNamespaceImpl(bool forced, const LeaderSyncQueue::Entry& syncEntry, std::string& tmpNsName) {
	logInfo("{}: '{}'. Trying to synchronize namespace {}", cfg_.serverId, syncEntry.nsName, forced ? "forced" : "by wal");
	SyncTimeCounter timeCounter(SyncTimeCounter::Type::InitialWalSync, statsCollector_);
	client::Snapshot snapshot;
	auto err = client_.GetSnapshot(syncEntry.nsName, SnapshotOpts(forced ? ExtendedLsn() : syncEntry.localLsn, cfg_.maxWALDepthOnForceSync),
								   snapshot);
	if (!err.ok()) {
		throw err;
	}
	if (const auto clStat = snapshot.ClusterOperationStat(); clStat.has_value()) {
		if (clStat->role != ClusterOperationStatus::Role::ClusterReplica) {
			throw Error(errReplParams, "Unable to sync leader's namespace {} via snapshot - target namespace has unexpected role: '{}'",
						syncEntry.nsName, clStat->RoleStr());
		}
		if (clStat->leaderId != cfg_.serverId) {
			throw Error(errReplParams,
						"Unable to sync leader's namespace {} via snapshot - target namespace has unexpected leader: {} ({} was expected)",
						syncEntry.nsName, clStat->leaderId, cfg_.serverId);
		}
	}

	RdxContext ctx;
	ctx.WithNoWaitSync();
	const RdxDeadlineContext deadlineCtx(kLocalCallsTimeout);
	auto ns = thisNode_.getNamespaceNoThrow(syncEntry.nsName, ctx.WithCancelCtx(deadlineCtx));
	if (!ns || snapshot.HasRawData()) {
		timeCounter.SetType(SyncTimeCounter::Type::InitialForceSync);
		// TODO: Allow tmp ns without storage via config
		err = thisNode_.CreateTemporaryNamespace(syncEntry.nsName, tmpNsName, StorageOpts().Enabled(), syncEntry.latestLsn.NsVersion(),
												 RdxContext());
		if (!err.ok()) {
			throw err;
		}
		ns = thisNode_.getNamespaceNoThrow(tmpNsName, ctx.WithCancelCtx(deadlineCtx));
		assert(ns);
	}

	for (auto& ch : snapshot) {
		if (terminate_) {
			return;
		}
		ns->ApplySnapshotChunk(ch.Chunk(), true, ctx.WithCancelCtx(deadlineCtx));
	}
}

}  // namespace reindexer::cluster
