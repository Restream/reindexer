#include "shardingproxy.h"
#include "cluster/sharding/locatorserviceadapter.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "cluster/stats/replicationstats.h"
#include "core/defnsconfigs.h"
#include "parallelexecutor.h"
#include "tools/catch_and_return.h"
#include "tools/compiletimemap.h"
#include "tools/jsontools.h"
#include "tools/randomgenerator.h"

namespace reindexer {

void ShardingProxy::ShutdownCluster() {
	impl_.ShutdownCluster();
	if (auto lockedShardingRouter = shardingRouter_.UniqueLock()) {
		lockedShardingRouter->Shutdown();
	}
}

auto ShardingProxy::isWithSharding(const Query &q, const RdxContext &ctx, int &actualShardId) const {
	using ret_type = std::optional<decltype(shardingRouter_.SharedLock(ctx))>;

	if (q.local_) {
		if (q.Type() != QuerySelect) {
			throw Error{errParams, "Only SELECT query could be LOCAL"};
		}
		return ret_type{};
	}
	if (q.count == 0 && q.calcTotal == ModeNoTotal && !q.joinQueries_.size() && !q.mergeQueries_.size()) {
		return ret_type{};	// Special case for tagsmatchers selects
	}
	if (q.IsWALQuery()) {
		return ret_type{};	// WAL queries are always local
	}

	if (!shardingInitialized_.load(std::memory_order_acquire)) {
		return ret_type{};
	}

	auto lockedShardingRouter = shardingRouter_.SharedLock(ctx);
	if (!lockedShardingRouter || !isSharderQuery(q, lockedShardingRouter)) {
		return ret_type{};
	}

	actualShardId = lockedShardingRouter->ActualShardId();

	if (!isWithSharding(ctx)) {
		return ret_type{};
	}

	return ret_type(std::move(lockedShardingRouter));
}

auto ShardingProxy::isWithSharding(std::string_view nsName, const RdxContext &ctx) const {
	using ret_type = std::optional<decltype(shardingRouter_.SharedLock(ctx))>;

	if (nsName.size() && (nsName[0] == '#' || nsName[0] == '@')) {
		return ret_type{};
	}

	if (!shardingInitialized_.load(std::memory_order_acquire)) {
		return ret_type{};
	}

	auto lockedShardingRouter = shardingRouter_.SharedLock(ctx);
	if (!lockedShardingRouter || !isSharded(nsName, lockedShardingRouter)) {
		return ret_type{};
	}

	return isWithSharding(ctx) ? ret_type(std::move(lockedShardingRouter)) : ret_type{};
}

bool ShardingProxy::isWithSharding(const RdxContext &ctx) const noexcept {
	if (int(ctx.ShardId()) == ShardingKeyType::ProxyOff) {
		return false;
	}
	return ctx.GetOriginLSN().isEmpty() && !ctx.HasEmmiterServer() && impl_.GetShardingConfig() &&
		   int(ctx.ShardId()) == ShardingKeyType::NotSetShard;
}

ShardingProxy::ShardingProxy(ReindexerConfig cfg)
	: impl_(std::move(cfg), activities_, {{"apply_sharding_config", [this](const gason::JsonNode &action, const RdxContext &ctx) {
											   auto localy = action["locally"];
											   Error (ShardingProxy::*method)(const gason::JsonNode &, const RdxContext &) noexcept =
												   &ShardingProxy::handleNewShardingConfigLocaly;
											   auto err = std::invoke(localy.empty() ? &ShardingProxy::handleNewShardingConfig : method,
																	  this, action["config"], ctx);

											   if (!err.ok()) throw err;
										   }}}) {}

Error ShardingProxy::Connect(const std::string &dsn, ConnectOpts opts) {
	try {
		Error err = impl_.Connect(dsn, opts);
		if (!err.ok()) {
			return err;
		}
		// Expecting for the first time Connect is being called under exlusive lock.
		// And all the subsequent calls will be perfomed under shared locks.
		auto configPtr = impl_.GetShardingConfig();
		if (auto lockedShardingRouter = shardingRouter_.UniqueLock(); !lockedShardingRouter && configPtr) {
			lockedShardingRouter = std::make_shared<sharding::LocatorService>(impl_, *configPtr);
			err = lockedShardingRouter->Start();
			if (err.ok()) {
				shardingInitialized_.store(true, std::memory_order_release);
			} else {
				lockedShardingRouter.Reset();
				shardingInitialized_.store(false, std::memory_order_release);
			}
		}
		return err;
	} catch (Error &e) {
		return e;
	}
}

template <ShardingProxy::ConfigResetFlag resetFlag>
[[nodiscard]] Error ShardingProxy::resetShardingConfigs(int64_t sourceId, const RdxContext &ctx) noexcept {
	try {
		// To avoid connection errors, it is necessary to copy the connections from the current ShardingConfig
		// since ParallelExecutor consistently calls the functions that disconnect shards
		std::optional<cluster::ShardingConfig> config = std::nullopt;
		obtainConfigForResetRouting(config, resetFlag, ctx);

		if (!config) return Error();

		auto router = sharding::LocatorService(impl_, *config);
		auto err = router.Start();
		if (!err.ok()) return err;

		auto oldConnections = router.GetAllShardsConnections(err);
		if (!err.ok()) return err;

		ParallelExecutor exec(router.ActualShardId());
		return exec.Exec(
			ctx, std::move(oldConnections),
			resetFlag == ConfigResetFlag::RollbackApplied ? &client::Reindexer::RollbackShardingConfigCandidate
														  : &client::Reindexer::ResetOldShardingConfig,
			[&ctx, this](int64_t sourceId) {
				resetOrRollbackShardingConfig<resetFlag>({sourceId}, ctx);
				return Error();
			},
			sourceId);
	}
	CATCH_AND_RETURN
}

void ShardingProxy::obtainConfigForResetRouting(std::optional<cluster::ShardingConfig> &config, ConfigResetFlag resetFlag,
												const RdxContext &ctx) const {
	auto lockedShardingRouter = shardingRouter_.SharedLock(ctx);

	if (!shardingInitialized_.load(std::memory_order_acquire)) {
		if (resetFlag == ConfigResetFlag::RollbackApplied) {
			auto lockedConfigCandidate = configCandidate_.SharedLock(ctx);
			if (auto &cfg = lockedConfigCandidate.Config()) {
				config = *cfg;
				return;
			} else {
				static constexpr auto errMessage =
					"Sharding config rollback error: config candidate is not available. Unable to perform rollback";
				logPrintf(LogInfo, errMessage);
				throw Error(errParams, errMessage);
			}
		} else {
			return;
		}
	}

	if (auto cfgPtr = impl_.GetShardingConfig()) {
		config = *cfgPtr;
		return;
	} else {
		static constexpr auto errMessage =
			"Sharding is initialized on this node, but the config for resetting it on others is not available";
		logPrintf(LogInfo, errMessage);
		throw Error(errParams, errMessage);
	}
}

[[nodiscard]] Error ShardingProxy::handleNewShardingConfig(const gason::JsonNode &configJSON, const RdxContext &ctx) noexcept {
	try {
		if (stringifyJson(configJSON).empty()) return Error(errParams, "New sharding config is empty");

		cluster::ShardingConfig config;
		auto err = config.FromJSON(configJSON);
		if (!err.ok()) return err;
		config.thisShardId = ShardingKeyType::NotSetShard;

		checkSyncCluster(config);

		auto router = sharding::LocatorService(impl_, config);
		err = router.Start();
		if (!err.ok()) return err;

		auto connections = router.GetAllShardsConnections(err);
		if (!err.ok()) return err;

		int64_t sourceId = tools::RandomGenerator::gets64(int64_t{0}, std::numeric_limits<int64_t>::max());

		ParallelExecutor execNodes(router.ActualShardId());

		std::unordered_map<int, std::string> cfgsStorage;

		auto SaveNewShardingConfigWrapper = [&cfgsStorage](client::Reindexer &conn, cluster::ShardingConfig config, int64_t sourceId,
														   int shardId) {
			config.thisShardId = shardId;
			cfgsStorage[shardId] = config.GetJSON();
			return conn.SaveNewShardingConfig(cfgsStorage[shardId], sourceId);
		};

		logPrintf(LogInfo, "Start attempt applying new sharding config. Source - %d", sourceId);

		// Saving config candidates in shards obtained from received config
		err = execNodes.Exec(
			ctx, sharding::ConnectionsPtr(connections), SaveNewShardingConfigWrapper,
			[](const cluster::ShardingConfig &, int64_t) { return Error(); }, config, sourceId);

		if (!err.ok()) {
			// Resetting sent config candidates due to errors
			auto errReset = execNodes.Exec(
				ctx, sharding::ConnectionsPtr(connections), &client::Reindexer::ResetShardingConfigCandidate,
				[](int64_t) { return Error(); }, sourceId);

			return Error(err.code(), err.what() + (!errReset.ok() ? ".\n" + errReset.what() : ""));
		}

		// Resetting existing shardings in case of successful saving of all candidates
		err = resetShardingConfigs<ConfigResetFlag::ResetExistent>(sourceId, ctx);
		if (!err.ok()) return err;

		// Applying previously saved config candidates
		err = execNodes.Exec(
			ctx, std::move(connections), &client::Reindexer::ApplyNewShardingConfig, [](int64_t) { return Error(); }, sourceId);

		// Rollback config candidates or applied configs if any errors occured on ApplyNewShardingConfig stage
		if (!err.ok())
			return Error(err.code(), err.what() + ".\n" + resetShardingConfigs<ConfigResetFlag::RollbackApplied>(sourceId, ctx).what());

		return Error();
	}
	CATCH_AND_RETURN
}

[[nodiscard]] Error ShardingProxy::handleNewShardingConfigLocaly(const gason::JsonNode &configJSON, const RdxContext &ctx) noexcept {
	try {
		if (configJSON.empty()) {
			if (auto lockedConfigCandidate = configCandidate_.SharedLock(ctx); lockedConfigCandidate.Config())
				return Error(errParams,
							 "Attempt to reset the config if there is an unprocessed config candidate with sourceId - %d. Try later.",
							 lockedConfigCandidate.SourceId());

			auto lockedShardingRouter = shardingRouter_.UniqueLock(ctx);
			[[maybe_unused]] auto err = impl_.ResetShardingConfig();
			lockedShardingRouter.Reset();
			shardingInitialized_.store(false, std::memory_order_release);
			logPrintf(LogInfo, "Sharding config successfully reseted locally");
			return Error();
		}

		return handleNewShardingConfigLocaly<>(configJSON, ctx);
	}
	CATCH_AND_RETURN
}

template <typename ConfigType>
[[nodiscard]] Error ShardingProxy::handleNewShardingConfigLocaly(const ConfigType &rawConfig, const RdxContext &ctx) noexcept {
	try {
		cluster::ShardingConfig config;
		auto err = config.FromJSON(rawConfig);
		if (!err.ok()) return err;

		int64_t sourceId = tools::RandomGenerator::gets64(int64_t{0}, std::numeric_limits<int64_t>::max());
		logPrintf(LogInfo, "Start attempt applying sharding config locally. Source - %d", sourceId);

		saveShardingCfgCandidateImpl(std::move(config), sourceId, ctx);
		applyNewShardingConfig({sourceId}, ctx);
		return Error();
	}
	CATCH_AND_RETURN
}

bool ShardingProxy::needProxyWithinCluster(const RdxContext &ctx) {
	if (!ctx.GetOriginLSN().isEmpty()) {
		return false;
	}

	cluster::RaftInfo info;
	auto err = impl_.GetRaftInfo(info, ctx);
	if (!err.ok()) {
		if (err.code() == errTimeout || err.code() == errCanceled) {
			err = Error(err.code(), "Unable to get cluster's leader: %s", err.what());
			throw err;
		}
	}

	if (info.role == cluster::RaftInfo::Role::None) {
		return false;
	}

	return true;
}

namespace {
using ShardingReqType = sharding::ShardingControlRequestData::Type;
using EnumsOrder = meta::Values2Types<ShardingReqType::SaveCandidate, ShardingReqType::ResetOldSharding, ShardingReqType::ApplyNew,
									  ShardingReqType::ResetCandidate, ShardingReqType::RollbackCandidate>;

using ClusterProxyMethods =
	meta::Map<EnumsOrder, meta::Values2Types<&ClusterProxy::SaveShardingCfgCandidate, &ClusterProxy::ResetOldShardingConfig,
											 &ClusterProxy::ApplyShardingCfgCandidate, &ClusterProxy::ResetShardingConfigCandidate,
											 &ClusterProxy::RollbackShardingConfigCandidate>>;

using RequestEnum2Types =
	meta::Map<EnumsOrder, std::tuple<sharding::SaveConfigCommand, sharding::ResetConfigCommand, sharding::ApplyConfigCommand,
									 sharding::ResetConfigCommand, sharding::ResetConfigCommand>>;

}  // namespace

struct ShardingProxy::ShardingProxyMethods {
	using Map = meta::Map<EnumsOrder, meta::Values2Types<&ShardingProxy::saveShardingCfgCandidate,
														 &ShardingProxy::resetOrRollbackShardingConfig<ConfigResetFlag::ResetExistent>,
														 &ShardingProxy::applyNewShardingConfig, &ShardingProxy::resetConfigCandidate,
														 &ShardingProxy::resetOrRollbackShardingConfig<ConfigResetFlag::RollbackApplied>>>;
};

template <auto RequestType, typename Request>
void ShardingProxy::shardingControlRequestAction(const Request &request, const RdxContext &ctx) {
	auto data = std::get<RequestEnum2Types::GetType<RequestType>>(request.data);

	if (needProxyWithinCluster(ctx)) {
		Error err;
		auto clusterMethod = ClusterProxyMethods::GetValue<RequestType>();
		if constexpr (std::is_invocable_v<decltype(clusterMethod), ClusterProxy *, std::string_view, int64_t, const RdxContext &>) {
			err = (impl_.*clusterMethod)(data.config, data.sourceId, ctx);
		} else {
			err = (impl_.*clusterMethod)(data.sourceId, ctx);
		}
		if (!err.ok()) {
			throw err;
		}
	}

	(this->*ShardingProxyMethods::Map::GetValue<RequestType>())(std::move(data), ctx);
}

[[nodiscard]] Error ShardingProxy::ShardingControlRequest(const sharding::ShardingControlRequestData &request,
														  const RdxContext &ctx) noexcept {
	try {
		using Type = sharding::ShardingControlRequestData::Type;

		switch (request.type) {
			case Type::SaveCandidate: {
				shardingControlRequestAction<Type::SaveCandidate>(request, ctx);
				break;
			}
			case Type::ResetOldSharding: {
				shardingControlRequestAction<Type::ResetOldSharding>(request, ctx);
				break;
			}
			case Type::ResetCandidate: {
				shardingControlRequestAction<Type::ResetCandidate>(request, ctx);
				break;
			}
			case Type::RollbackCandidate: {
				shardingControlRequestAction<Type::RollbackCandidate>(request, ctx);
				break;
			}
			case Type::ApplyNew: {
				shardingControlRequestAction<Type::ApplyNew>(request, ctx);
				break;
			}
			case Type::ApplyLeaderConfig: {
				assertrx(!ctx.GetOriginLSN().isEmpty());
				const auto &data = std::get<sharding::SaveConfigCommand>(request.data);
				return data.config.empty() ? handleNewShardingConfigLocaly(gason::JsonNode::EmptyNode(), ctx)
										   : handleNewShardingConfigLocaly(data.config, ctx);
			}
			default:
				throw Error(errLogic, "Unsupported sharding request command");
		}
	}
	CATCH_AND_RETURN
	return Error();
}

void ShardingProxy::saveShardingCfgCandidate(const sharding::SaveConfigCommand &data, const RdxContext &ctx) {
	cluster::ShardingConfig config;
	auto err = config.FromJSON(data.config);
	if (!err.ok()) throw err;

	saveShardingCfgCandidateImpl(std::move(config), data.sourceId, ctx);
}

void ShardingProxy::saveShardingCfgCandidateImpl(cluster::ShardingConfig config, int64_t sourceId, const RdxContext &ctx) {
	if (ctx.GetOriginLSN().isEmpty()) {
		checkNamespacesEmpty(config, ctx);
	}

	auto lockedConfigCandidate = configCandidate_.UniqueLock(ctx);

	if (lockedConfigCandidate.Config()) {
		if (lockedConfigCandidate.SourceId() == sourceId) {
			logPrintf(LogInfo, "This config candidate has already been saved previously. Source - %d.", sourceId);
			return;
		}
		throw Error(errParams,
					"Config candidate is busy already (when trying to save a new config). Received Source - %d. Current sourceId - %d",
					sourceId, lockedConfigCandidate.SourceId());
	}

	lockedConfigCandidate.EnableReseter(false);
	if (lockedConfigCandidate.Reseter().joinable()) lockedConfigCandidate.Reseter().join();
	lockedConfigCandidate.EnableReseter();

	impl_.SaveNewShardingConfigFile(config);
	lockedConfigCandidate.SourceId() = sourceId;
	lockedConfigCandidate.Config() = std::move(config);

	logPrintf(LogInfo, "New sharding config candidate saved. Source - %d", sourceId);

	lockedConfigCandidate.Reseter() = std::thread([this]() {
		using std::this_thread::sleep_for;
		const auto timeout = std::chrono::seconds(30);
		const auto period = std::chrono::milliseconds(100);
		auto iters = timeout / period;

		for (auto i = 0; i < iters; ++i) {
			sleep_for(period);
			if (configCandidate_.NeedStopReseter()) return;
		}

		while (!configCandidate_.TryResetConfig()) sleep_for(period);
	});
}

void ShardingProxy::checkNamespacesEmpty(const cluster::ShardingConfig &config, const RdxContext &ctx) {
	for (const auto &ns : config.namespaces) {
		if (auto nsPtr = impl_.GetNamespacePtrNoThrow(ns.ns, ctx))
			if (nsPtr->getMainNs()->GetItemsCount())
				throw Error(errParams, "Namespace %s in shard %d not empty", ns.ns, config.thisShardId);
	}
}

void ShardingProxy::checkSyncCluster(const cluster::ShardingConfig &shardingConfig) {
	client::ReindexerConfig cfg;
	cfg.AppName = "sharding_proxy_check_cluster_dsns";
	cfg.SyncRxCoroCount = 1;
	cfg.EnableCompression = true;
	cfg.RequestDedicatedThread = true;
	for (const auto &[shardId, hosts] : shardingConfig.shards) {
		auto dsn = hosts.front();
		auto connection = std::make_shared<client::Reindexer>(client::Reindexer(cfg, 1, 1));

		auto status = connection->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
		if (!status.ok()) {
			throw Error(errLogic, "Error connecting to node [%s]: %s", dsn, status.what());
		}

		const Query q = Query(std::string(kReplicationStatsNamespace)).Where("type", CondEq, Variant(cluster::kClusterReplStatsType));

		client::QueryResults qr;
		auto err = connection->WithTimeout(shardingConfig.reconnectTimeout).Select(q, qr);
		if (!err.ok()) throw err;

		WrSerializer wser;
		err = qr.begin().GetJSON(wser, false);
		if (!err.ok()) throw err;

		cluster::ReplicationStats stats;
		err = stats.FromJSON(wser.Slice());
		if (!err.ok()) throw err;

		if (!stats.nodeStats.empty() && stats.nodeStats.size() != hosts.size()) {
			throw Error(errLogic, "Not equal count of dsns in cluster and sharding config[%s]", dsn);
		}

		for (const auto &nodeStat : stats.nodeStats) {
			if (auto it = std::find(hosts.begin(), hosts.end(), nodeStat.dsn); it == hosts.end()) {
				throw Error(errLogic, "Different sets of DSNs in cluster and sharding config");
			}
		}
	}
}

void ShardingProxy::applyNewShardingConfig(const sharding::ApplyConfigCommand &data, const RdxContext &ctx) {
	auto lockedConfigCandidate = configCandidate_.UniqueLock(ctx);
	int64_t sourceId = data.sourceId;
	auto &config = lockedConfigCandidate.Config();

	if (!config) {
		if (sourceId == lockedConfigCandidate.SourceId()) {
			logPrintf(LogInfo, "Empty sharding config candidate. Probably it was already successfully applied. Source - %d", sourceId);
			return;
		} else {
			throw Error(errParams, "Attempt to apply empty sharding config candidate. Source - %d", sourceId);
		}
	}

	if (sourceId != lockedConfigCandidate.SourceId())
		throw Error(errParams, "Attempt to apply a config with a different sourceId - %d. Current sourceId - %d", sourceId,
					lockedConfigCandidate.SourceId());

	if (auto oldConfig = impl_.GetShardingConfig()) {
		if (*oldConfig == *config) {
			logPrintf(LogInfo, "New sharding config is same as old sharding config. Source - %d", sourceId);
			config = std::nullopt;
			return;
		}
	}

	auto lockedShardingRouter = shardingRouter_.UniqueLock(ctx);

	auto err = impl_.ResetShardingConfig(std::move(*config));
	// TODO: after allowing actions to upsert #config namespace, make ApplyNewShardingConfig returned void, allow except here
	// if (!err.ok()) return err;

	lockedShardingRouter = std::make_shared<sharding::LocatorService>(impl_, *impl_.GetShardingConfig());
	config = std::nullopt;

	err = lockedShardingRouter->Start();
	if (err.ok()) {
		shardingInitialized_.store(true, std::memory_order_release);
		logPrintf(LogInfo, "New sharding config successfully applied. Source - %d", sourceId);
	} else {
		logPrintf(LogError, "ERROR start sharding router:\n{}\nSource - %d", err.what(), sourceId);
		lockedShardingRouter.Reset();
		shardingInitialized_.store(false, std::memory_order_release);
		throw err;
	}
}

template <ShardingProxy::ConfigResetFlag resetFlag>
void ShardingProxy::resetOrRollbackShardingConfig(const sharding::ResetConfigCommand &data, const RdxContext &ctx) {
	int64_t sourceId = data.sourceId;
	auto lockedConfigCandidate = configCandidate_.UniqueLock(ctx);
	if (lockedConfigCandidate.Config() && sourceId != lockedConfigCandidate.SourceId())
		throw Error(errParams, "Attempt to %s a config with a different sourceId - %d. Current sourceId - %d",
					resetFlag == ConfigResetFlag::RollbackApplied ? "rollback" : "reset", sourceId, lockedConfigCandidate.SourceId());

	auto lockedShardingRouter = shardingRouter_.UniqueLock(ctx);

	[[maybe_unused]] auto err = impl_.ResetShardingConfig();
	// TODO: after allowing actions to upsert #config namespace, make ApplyNewShardingConfig returned void, allow except here
	// if (!err.ok()) return err;

	lockedShardingRouter.Reset();
	shardingInitialized_.store(false, std::memory_order_release);
	logPrintf(LogInfo, "%s sharding config successfully reseted. Source - %d",
			  resetFlag == ConfigResetFlag::RollbackApplied ? "Candidate in" : "Old", sourceId);
}

void ShardingProxy::resetConfigCandidate(const sharding::ResetConfigCommand &data, const RdxContext &ctx) {
	int64_t sourceId = data.sourceId;
	auto lockedConfigCandidate = configCandidate_.UniqueLock(ctx);

	if (!lockedConfigCandidate.Config()) return;

	if (lockedConfigCandidate.SourceId() != sourceId)
		throw Error(errParams, "Attempt to reset candidate with a different sourceId - %d. Current sourceId - %d",
					lockedConfigCandidate.SourceId(), sourceId);

	lockedConfigCandidate.EnableReseter(false);
	if (lockedConfigCandidate.Reseter().joinable()) lockedConfigCandidate.Reseter().join();
	lockedConfigCandidate.EnableReseter();

	lockedConfigCandidate.Config() = std::nullopt;
	logPrintf(LogInfo, "Sharding config candidate was reseted. Source - %d", sourceId);
}

Error ShardingProxy::OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts,
								   const RdxContext &ctx) {
	try {
		auto localOpen = [this, &ctx](std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts) {
			return impl_.OpenNamespace(nsName, opts, replOpts, ctx);
		};

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::OpenNamespace, localOpen, nsName, opts, replOpts);
		}
		return localOpen(nsName, opts, replOpts);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const RdxContext &ctx) {
	try {
		auto localAdd = [this, &ctx](const NamespaceDef &nsDef, const NsReplicationOpts &replOpts) {
			return impl_.AddNamespace(nsDef, replOpts, ctx);
		};
		if (auto lckRouterOpt = isWithSharding(nsDef.name, ctx)) {
			Error status = (*lckRouterOpt)->AwaitShards(ctx);
			if (!status.ok()) return status;

			auto connections = (*lckRouterOpt)->GetShardsConnections(status);

			lckRouterOpt->Unlock();

			if (!status.ok()) return status;
			for (auto &connection : *connections) {
				if (connection) {
					status = connection->AddNamespace(nsDef, replOpts);
				} else {
					status = localAdd(nsDef, replOpts);
				}
				if (!status.ok()) return status;
			}
			return status;
		}
		return localAdd(nsDef, replOpts);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::CloseNamespace(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localClose = [this, &ctx](std::string_view nsName) { return impl_.CloseNamespace(nsName, ctx); };

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;
			return delegateToShardsByNs(*lckRouterOpt, ctx, &client::Reindexer::CloseNamespace, localClose, nsName);
		}
		return localClose(nsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::DropNamespace(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localDrop = [this, &ctx](std::string_view nsName) { return impl_.DropNamespace(nsName, ctx); };

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::DropNamespace, localDrop, nsName);
		}
		return localDrop(nsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::TruncateNamespace(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localTruncate = [this, &ctx](std::string_view nsName) { return impl_.TruncateNamespace(nsName, ctx); };
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::TruncateNamespace, localTruncate, nsName);
		}
		return localTruncate(nsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const RdxContext &ctx) {
	try {
		auto localRename = [this, &ctx](std::string_view srcNsName, const std::string &dstNsName) {
			return impl_.RenameNamespace(srcNsName, dstNsName, ctx);
		};
		if (auto lckRouterOpt = isWithSharding(srcNsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::RenameNamespace, localRename, srcNsName, dstNsName);
		}
		return localRename(srcNsName, dstNsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::AddIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
	try {
		auto localAddIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) { return impl_.AddIndex(nsName, index, ctx); };

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::AddIndex, localAddIndex, nsName, index);
		}
		return localAddIndex(nsName, index);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::UpdateIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
	try {
		auto localUpdateIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) {
			return impl_.UpdateIndex(nsName, index, ctx);
		};

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::UpdateIndex, localUpdateIndex, nsName, index);
		}
		return localUpdateIndex(nsName, index);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::DropIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
	try {
		auto localDropIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) { return impl_.DropIndex(nsName, index, ctx); };
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::DropIndex, localDropIndex, nsName, index);
		}
		return localDropIndex(nsName, index);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::SetSchema(std::string_view nsName, std::string_view schema, const RdxContext &ctx) {
	try {
		auto localSetSchema = [this, &ctx](std::string_view nsName, std::string_view schema) {
			return impl_.SetSchema(nsName, schema, ctx);
		};
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::SetSchema, localSetSchema, nsName, schema);
		}
		return localSetSchema(nsName, schema);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Insert(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto insertFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Insert(nsName, item, ctx); };

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Insert>(*lckRouterOpt, ctx, nsName, item, insertFn);
		}
		return insertFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Insert(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto insertFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Insert(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Insert>(*lckRouterOpt, ctx, nsName, item, result, insertFn);
		}
		return insertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Update(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto updateFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Update(nsName, item, ctx); };

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Update>(*lckRouterOpt, ctx, nsName, item, updateFn);
		}
		return updateFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Update(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto updateFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Update(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Update>(*lckRouterOpt, ctx, nsName, item, result, updateFn);
		}
		return updateFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Update(const Query &query, QueryResults &result, const RdxContext &ctx) {
	try {
		auto updateFn = [this](const Query &q, LocalQueryResults &qr, const RdxContext &ctx) { return impl_.Update(q, qr, ctx); };

		int actualShardId = ShardingKeyType::NotSharded;
		result.SetQuery(&query);
		if (auto lckRouterOpt = isWithSharding(query, ctx, actualShardId)) {
			return executeQueryOnShard(*lckRouterOpt, query, result, 0, ctx, std::move(updateFn));
		}
		result.AddQr(LocalQueryResults{}, actualShardId);
		return updateFn(query, result.ToLocalQr(false), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Upsert(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto upsertFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Upsert(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Upsert>(*lckRouterOpt, ctx, nsName, item, result, upsertFn);
		}
		return upsertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Upsert(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto upsertFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Upsert(nsName, item, ctx); };

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Upsert>(*lckRouterOpt, ctx, nsName, item, upsertFn);
		}
		return upsertFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Delete(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto deleteFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Delete(nsName, item, ctx); };

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Delete>(*lckRouterOpt, ctx, nsName, item, deleteFn);
		}
		return deleteFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Delete(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto deleteFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Delete(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Delete>(*lckRouterOpt, ctx, nsName, item, result, deleteFn);
		}
		return deleteFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Delete(const Query &query, QueryResults &result, const RdxContext &ctx) {
	try {
		auto deleteFn = [this](const Query &q, LocalQueryResults &qr, const RdxContext &ctx) { return impl_.Delete(q, qr, ctx); };

		result.SetQuery(&query);
		int actualShardId = ShardingKeyType::NotSharded;
		if (auto lckRouterOpt = isWithSharding(query, ctx, actualShardId)) {
			return executeQueryOnShard(*lckRouterOpt, query, result, 0, ctx, std::move(deleteFn));
		}
		result.AddQr(LocalQueryResults{}, actualShardId);
		return deleteFn(query, result.ToLocalQr(false), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Select(std::string_view sql, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &ctx) {
	try {
		Query query;
		query.FromSQL(sql);
		switch (query.type_) {
			case QuerySelect: {
				return Select(query, result, proxyFetchLimit, ctx);
			}
			case QueryDelete: {
				return Delete(query, result, ctx);
			}
			case QueryUpdate: {
				return Update(query, result, ctx);
			}
			case QueryTruncate: {
				return TruncateNamespace(query.Namespace(), ctx);
			}
			default:
				return Error(errLogic, "Incorrect sql type %d", query.type_);
		}
	} catch (const Error &err) {
		return err;
	}
}

Error ShardingProxy::Select(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &ctx) {
	try {
		if (query.Type() != QuerySelect) {
			return Error(errLogic, "'Select' call request type is not equal to 'QuerySelect'.");
		}

		result.SetQuery(&query);
		int actualShardId = ShardingKeyType::NotSharded;
		if (auto lckRouterOpt = isWithSharding(query, ctx, actualShardId)) {
			return executeQueryOnShard(
				*lckRouterOpt, query, result, proxyFetchLimit, ctx,
				[this](const Query &q, LocalQueryResults &qr, const RdxContext &ctx) { return impl_.Select(q, qr, ctx); });
		}
		result.AddQr(LocalQueryResults{}, actualShardId);
		return impl_.Select(query, result.ToLocalQr(false), ctx);
	} catch (const Error &err) {
		return err;
	}
}

Error ShardingProxy::Commit(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localCommit = [this](std::string_view nsName) { return impl_.Commit(nsName); };
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			Error err = (*lckRouterOpt)->AwaitShards(ctx);
			if (!err.ok()) return err;
			return delegateToShardsByNs(*lckRouterOpt, ctx, &client::Reindexer::Commit, localCommit, nsName);
		}
		return localCommit(nsName);
	} catch (Error &e) {
		return e;
	}
}

auto ShardingProxy::ShardingRouter::SharedPtr(const RdxContext &ctx) const {
	auto lk = SharedLock(ctx);
	return locatorService_;
}

Transaction ShardingProxy::NewTransaction(std::string_view nsName, const RdxContext &ctx) {
	try {
		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return Transaction(impl_.NewTransaction(nsName, ctx), sharding::LocatorServiceAdapter(shardingRouter_.SharedPtr(ctx)));
		}
		return impl_.NewTransaction(nsName, ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::CommitTransaction(Transaction &tr, QueryResults &result, const RdxContext &ctx) {
	if (!tr.Status().ok()) {
		return Error(tr.Status().code(), "Unable to commit tx with error status: '%s'", tr.Status().what());
	}

	try {
		result.SetQuery(nullptr);
		return impl_.CommitTransaction(tr, result, (bool)isWithSharding(tr.GetNsName(), ctx), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::GetMeta(std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data, const RdxContext &ctx) {
	try {
		auto localGetMeta = [this, &ctx](std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data, int shardId) {
			data.emplace_back(shardId, std::string());
			return impl_.GetMeta(nsName, key, data.back().data, ctx);
		};

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			auto predicate = [](const ShardedMeta &) noexcept { return true; };
			Error err = collectFromShardsByNs(
				*lckRouterOpt, ctx,
				static_cast<Error (client::Reindexer::*)(std::string_view, const std::string &, std::vector<ShardedMeta> &)>(
					&client::Reindexer::GetMeta),
				localGetMeta, data, predicate, nsName, key);
			return err;
		}

		return localGetMeta(nsName, key, data, ctx.ShardId());
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const RdxContext &ctx) {
	try {
		auto localPutMeta = [this, &ctx](std::string_view nsName, const std::string &key, std::string_view data) {
			return impl_.PutMeta(nsName, key, data, ctx);
		};

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			return delegateToShards(*lckRouterOpt, ctx, &client::Reindexer::PutMeta, localPutMeta, nsName, key, data);
		}
		return localPutMeta(nsName, key, data);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const RdxContext &ctx) {
	try {
		auto localEnumMeta = [this, &ctx](std::string_view nsName, std::vector<std::string> &keys, [[maybe_unused]] int shardId) {
			return impl_.EnumMeta(nsName, keys, ctx);
		};

		if (auto lckRouterOpt = isWithSharding(nsName, ctx)) {
			fast_hash_set<std::string> allKeys;
			Error err = collectFromShardsByNs(
				*lckRouterOpt, ctx, &client::Reindexer::EnumMeta, localEnumMeta, keys,
				[&allKeys](const std::string &key) { return allKeys.emplace(key).second; }, nsName);

			return err;
		}
		return localEnumMeta(nsName, keys, ShardingKeyType::NotSetShard /*not used*/);
	} catch (Error &e) {
		return e;
	}
}

template <typename ShardingRouterLock>
bool ShardingProxy::isSharderQuery(const Query &q, const ShardingRouterLock &shLockShardingRouter) const {
	if (isSharded(q.Namespace(), shLockShardingRouter)) {
		return true;
	}
	for (const auto &jq : q.joinQueries_) {
		if (isSharded(jq.Namespace(), shLockShardingRouter)) {
			return true;
		}
	}
	for (const auto &mq : q.mergeQueries_) {
		if (isSharded(mq.Namespace(), shLockShardingRouter)) {
			return true;
		}
	}
	return false;
}

template <typename ShardingRouterLock>
bool ShardingProxy::isSharded(std::string_view nsName, const ShardingRouterLock &shLockShardingRouter) const noexcept {
	return shLockShardingRouter->IsSharded(nsName);
}

void ShardingProxy::calculateNewLimitOfsset(size_t count, size_t totalCount, unsigned &limit, unsigned &offset) {
	if (limit != UINT_MAX) {
		if (count >= limit) {
			limit = 0;
		} else {
			limit -= count;
		}
	}
	if (totalCount >= offset) {
		offset = 0;
	} else {
		offset -= totalCount;
	}
}

reindexer::client::Item ShardingProxy::toClientItem(std::string_view ns, client::Reindexer *connection, reindexer::Item &item) {
	assertrx(connection);
	Error err;
	reindexer::client::Item clientItem = connection->NewItem(ns);
	if (clientItem.Status().ok()) {
		err = clientItem.FromJSON(item.GetJSON());
		if (err.ok() && clientItem.Status().ok()) {
			clientItem.SetPrecepts(item.impl_->GetPrecepts());
			if (item.impl_->tagsMatcher().isUpdated()) {
				// Add new names missing in JSON from tm
				clientItem.impl_->addTagNamesFrom(item.impl_->tagsMatcher());
			}
			return clientItem;
		}
	}
	if (!err.ok()) throw err;
	throw clientItem.Status();
}

template <typename LockedRouter, typename ClientF, typename LocalF, typename T, typename Predicate, typename... Args>
Error ShardingProxy::collectFromShardsByNs(LockedRouter &lockedShardingRouter, const RdxContext &rdxCtx, const ClientF &clientF,
										   const LocalF &localF, std::vector<T> &result, const Predicate &predicate,
										   std::string_view nsName, Args &&...args) {
	try {
		Error status;

		auto [actualShardId, connections] =
			std::make_tuple(lockedShardingRouter->ActualShardId(), lockedShardingRouter->GetShardsConnections(nsName, -1, status));

		lockedShardingRouter.Unlock();

		if (!status.ok()) return status;

		ParallelExecutor execNodes(actualShardId);
		return execNodes.ExecCollect(rdxCtx, std::move(connections), clientF, localF, result, predicate, nsName,
									 std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}
}

template <typename LockedRouter, typename Func, typename FLocal, typename... Args>
Error ShardingProxy::delegateToShards(LockedRouter &lockedShardingRouter, const RdxContext &rdxCtx, const Func &f, const FLocal &local,
									  Args &&...args) {
	Error status;
	try {
		auto [actualShardId, connections] =
			std::make_tuple(lockedShardingRouter->ActualShardId(), lockedShardingRouter->GetShardsConnections(status));

		lockedShardingRouter.Unlock();

		if (!status.ok()) return status;

		ParallelExecutor execNodes(actualShardId);
		return execNodes.Exec(rdxCtx, std::move(connections), f, local, std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}

	return status;
}

template <typename LockedRouter, typename Func, typename FLocal, typename... Args>
Error ShardingProxy::delegateToShardsByNs(LockedRouter &lockedShardingRouter, const RdxContext &rdxCtx, const Func &f, const FLocal &local,
										  std::string_view nsName, Args &&...args) {
	Error status;
	try {
		auto [actualShardId, connections] =
			std::make_tuple(lockedShardingRouter->ActualShardId(), lockedShardingRouter->GetShardsConnections(nsName, -1, status));

		lockedShardingRouter.Unlock();

		if (!status.ok()) return status;

		ParallelExecutor execNodes(actualShardId);
		return execNodes.Exec(rdxCtx, std::move(connections), f, local, nsName, std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}
	return status;
}

template <ShardingProxy::ItemModifyFT fn, typename LockedRouter, typename LocalFT>
Error ShardingProxy::modifyItemOnShard(LockedRouter &lockedShardingRouter, const RdxContext &ctx, std::string_view nsName, Item &item,
									   const LocalFT &localFn) {
	try {
		Error status;

		auto [actualShardId, connection] =
			std::make_tuple(lockedShardingRouter->ActualShardId(), lockedShardingRouter->GetShardConnectionWithId(nsName, item, status));

		lockedShardingRouter.Unlock();

		if (!status.ok()) {
			return status;
		}
		if (connection) {
			client::Item clientItem = toClientItem(nsName, connection.get(), item);
			if (!clientItem.Status().ok()) return clientItem.Status();
			const auto timeout = ctx.GetRemainingTimeout();
			if (timeout.has_value() && timeout->count() <= 0) {
				return Error(errTimeout, "Item modify request timeout");
			}
			auto conn = timeout.has_value() ? connection->WithContext(ctx.GetCancelCtx()).WithTimeout(*timeout)
											: connection->WithContext(ctx.GetCancelCtx());
			const auto ward = ctx.BeforeShardingProxy();
			status = (conn.*fn)(nsName, clientItem);
			if (!status.ok()) {
				return status;
			}
			*item.impl_ = ItemImpl(clientItem.impl_->Type(), clientItem.impl_->tagsMatcher());
			Error err = item.impl_->FromJSON(clientItem.GetJSON());
			assertrx(err.ok());
			item.setID(clientItem.GetID());
			item.setShardID(connection.ShardId());
			item.setLSN(clientItem.GetLSN());
			return status;
		}
		status = localFn(nsName, item);
		if (!status.ok()) {
			return status;
		}
		item.setShardID(actualShardId);
	} catch (const Error &err) {
		return err;
	}
	return Error();
}

template <ShardingProxy::ItemModifyQrFT fn, typename LockedRouter, typename LocalFT>
Error ShardingProxy::modifyItemOnShard(LockedRouter &lockedShardingRouter, const RdxContext &ctx, std::string_view nsName, Item &item,
									   QueryResults &result, const LocalFT &localFn) {
	Error status;

	auto [actualShardId, connection] =
		std::make_tuple(lockedShardingRouter->ActualShardId(), lockedShardingRouter->GetShardConnectionWithId(nsName, item, status));

	lockedShardingRouter.Unlock();

	if (!status.ok()) {
		return status;
	}
	if (connection) {
		client::Item clientItem = toClientItem(nsName, connection.get(), item);
		client::QueryResults qrClient(result.Flags(), 0);
		if (!clientItem.Status().ok()) return clientItem.Status();

		const auto timeout = ctx.GetRemainingTimeout();
		if (timeout.has_value() && timeout->count() <= 0) {
			return Error(errTimeout, "Item modify request timeout");
		}
		auto conn = timeout.has_value() ? connection->WithContext(ctx.GetCancelCtx()).WithTimeout(*timeout)
										: connection->WithContext(ctx.GetCancelCtx());
		const auto ward = ctx.BeforeShardingProxy();
		status = (conn.*fn)(nsName, clientItem, qrClient);
		if (!status.ok()) {
			return status;
		}
		result.AddQr(std::move(qrClient), connection.ShardId());
		return status;
	}
	result.AddQr(LocalQueryResults(), actualShardId);
	status = localFn(nsName, item, result.ToLocalQr(false));
	if (!status.ok()) {
		return status;
	}
	return status;
}

template <typename LockedRouter, typename LocalFT>
Error ShardingProxy::executeQueryOnShard(LockedRouter &lockedShardingRouter, const Query &query, QueryResults &result,
										 unsigned proxyFetchLimit, const RdxContext &ctx, LocalFT &&localAction) noexcept {
	Error status;
	try {
		auto [actualShardId, connectionsPtr] =
			std::make_tuple(lockedShardingRouter->ActualShardId(), lockedShardingRouter->GetShardsConnectionsWithId(query, status));

		lockedShardingRouter.Unlock();

		if (!status.ok()) return status;

		assertrx(connectionsPtr);
		sharding::ConnectionsVector &connections = *connectionsPtr;

		if (query.Type() == QueryUpdate && connections.size() != 1) {
			return Error(errLogic, "Update request can be executed on one node only.");
		}

		if (query.Type() == QueryDelete && connections.size() != 1) {
			return Error(errLogic, "Delete request can be executed on one node only.");
		}

		const bool isDistributedQuery = connections.size() > 1;
		if (!isDistributedQuery) {
			assert(connections.size() == 1);

			if (connections[0]) {
				const auto timeout = ctx.GetRemainingTimeout();
				if (timeout.has_value() && timeout->count() <= 0) {
					return Error(errTimeout, "Sharded request timeout");
				}

				auto connection =
					timeout.has_value()
						? connections[0]->WithShardingParallelExecution(false).WithContext(ctx.GetCancelCtx()).WithTimeout(*timeout)
						: connections[0]->WithShardingParallelExecution(false).WithContext(ctx.GetCancelCtx());
				client::QueryResults qrClient(result.Flags(), proxyFetchLimit, client::LazyQueryResultsMode{});
				status = executeQueryOnClient(connection, query, qrClient, [](size_t, size_t) {});
				if (status.ok()) {
					result.AddQr(std::move(qrClient), connections[0].ShardId(), true);
				}

			} else {
				const auto shCtx = ctx.WithShardId(actualShardId, false);
				LocalQueryResults lqr;

				status = localAction(query, lqr, shCtx);
				if (status.ok()) {
					result.AddQr(std::move(lqr), actualShardId, true);
				}
			}
			return status;
		} else if (query.count == UINT_MAX && query.start == 0 && query.sortingEntries_.empty()) {
			ParallelExecutor exec(actualShardId);
			return exec.ExecSelect(query, result, connections, ctx, std::forward<LocalFT>(localAction));
		} else {
			unsigned limit = query.count;
			unsigned offset = query.start;

			Query distributedQuery(query);
			if (!distributedQuery.sortingEntries_.empty()) {
				const auto ns = impl_.GetNamespacePtr(distributedQuery.Namespace(), ctx)->getMainNs();
				result.SetOrdering(distributedQuery, *ns, ctx);
			}
			if (distributedQuery.count != UINT_MAX && !distributedQuery.sortingEntries_.empty()) {
				distributedQuery.Limit(distributedQuery.start + distributedQuery.count);
			}
			if (distributedQuery.start != 0) {
				if (distributedQuery.sortingEntries_.empty()) {
					distributedQuery.ReqTotal();
				} else {
					distributedQuery.Offset(0);
				}
			}

			size_t strictModeErrors = 0;
			for (size_t i = 0; i < connections.size(); ++i) {
				if (connections[i]) {
					const auto timeout = ctx.GetRemainingTimeout();
					if (timeout.has_value() && timeout->count() <= 0) {
						return Error(errTimeout, "Sharded request timeout");
					}
					auto connection =
						timeout.has_value()
							? connections[i]
								  ->WithShardingParallelExecution(connections.size() > 1)
								  .WithContext(ctx.GetCancelCtx())
								  .WithTimeout(*timeout)
							: connections[i]->WithShardingParallelExecution(connections.size() > 1).WithContext(ctx.GetCancelCtx());
					client::QueryResults qrClient(result.Flags(), proxyFetchLimit);

					if (distributedQuery.sortingEntries_.empty()) {
						distributedQuery.Limit(limit);
						distributedQuery.Offset(offset);
					}
					status = executeQueryOnClient(connection, distributedQuery, qrClient,
												  [&limit, &offset, this](size_t count, size_t totalCount) {
													  calculateNewLimitOfsset(count, totalCount, limit, offset);
												  });
					if (status.ok()) {
						result.AddQr(std::move(qrClient), connections[i].ShardId(), (i + 1) == connections.size());
					}
				} else {
					assertrx(i == 0);
					const auto shCtx = ctx.WithShardId(actualShardId, true);
					LocalQueryResults lqr;
					status = localAction(distributedQuery, lqr, shCtx);
					if (status.ok()) {
						if (distributedQuery.sortingEntries_.empty()) {
							calculateNewLimitOfsset(lqr.Count(), lqr.TotalCount(), limit, offset);
						}
						result.AddQr(std::move(lqr), actualShardId, (i + 1) == connections.size());
					}
				}
				if (!status.ok()) {
					if (status.code() != errStrictMode || ++strictModeErrors == connections.size()) {
						return status;
					}
				}
				if (distributedQuery.calcTotal == ModeNoTotal && limit == 0 && (i + 1) != connections.size()) {
					result.RebuildMergedData();
					break;
				}
			}
		}
	} catch (const Error &e) {
		return e;
	}
	return Error{};
}

template <typename CalucalteFT>
Error ShardingProxy::executeQueryOnClient(client::Reindexer &connection, const Query &q, client::QueryResults &qrClient,
										  const CalucalteFT &limitOffsetCalc) {
	Error status;
	switch (q.Type()) {
		case QuerySelect: {
			status = connection.Select(q, qrClient);
			if (q.sortingEntries_.empty()) {
				limitOffsetCalc(qrClient.Count(), qrClient.TotalCount());
			}
			break;
		}
		case QueryUpdate: {
			status = connection.Update(q, qrClient);
			break;
		}
		case QueryDelete: {
			status = connection.Delete(q, qrClient);
			break;
		}
		case QueryTruncate:
			std::abort();
	}
	return status;
}

}  // namespace reindexer
