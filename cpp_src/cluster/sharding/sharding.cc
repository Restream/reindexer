#include "sharding.h"
#include "cluster/consts.h"
#include "cluster/stats/replicationstats.h"
#include "core/clusterproxy.h"
#include "core/item.h"
#include "core/type_consts.h"
#include "estl/gift_str.h"
#include "tools/logger.h"

namespace reindexer {
namespace sharding {

constexpr size_t kMaxShardingProxyConnCount = 64;
constexpr size_t kMaxShardingProxyConnConcurrency = 1024;

RoutingStrategy::RoutingStrategy(const cluster::ShardingConfig& config) : keys_(config) {}

bool RoutingStrategy::getHostIdForQuery(const Query& q, int& hostId, Variant& shardKey) const {
	bool containsKey = false;
	std::string_view ns = q.NsName();
	for (auto it = q.Entries().cbegin(), next = it, end = q.Entries().cend(); it != end; ++it) {
		++next;
		it->Visit(
			Skip<AlwaysTrue, AlwaysFalse, JoinQueryEntry, SubQueryEntry, MultiDistinctQueryEntry, KnnQueryEntry>{},
			[&](const QueryEntry& qe) {
				if (containsKey) {
					if (keys_.IsShardIndex(ns, qe.FieldName())) {
						throw Error(errLogic, "Duplication of shard key condition in the query");
					}
				} else {
					bool isShardKey = false;
					int id = keys_.GetShardId(ns, qe.FieldName(), qe.Values(), isShardKey);
					if (isShardKey) {
						containsKey = true;
						if (qe.Condition() != CondEq) {
							throw Error(errLogic, "Shard key condition can only be 'Eq'");
						}
						if (hostId == ShardingKeyType::ProxyOff) {
							hostId = id;
							shardKey = qe.Values()[0];
							std::ignore = shardKey.EnsureHold();
						} else if (hostId != id) {
							throw Error(errLogic, "Shard key from other node");
						}
						if (it->operation == OpOr || (next != end && next->operation == OpOr)) {
							throw Error(errLogic, "Shard key condition cannot be connected with other conditions by operator OR");
						}
						if (it->operation == OpNot) {
							throw Error(errLogic, "Shard key condition cannot be negative");
						}
					}
				}
			},
			[&](const SubQueryFieldEntry& sqe) {
				if (keys_.IsShardIndex(ns, sqe.FieldName())) {
					throw Error(errLogic, "Subqueries can not be used in the conditions with sharding key ({})", sqe.FieldName());
				}
			},
			[&](const BetweenFieldsQueryEntry& qe) {
				if (keys_.IsShardIndex(ns, qe.LeftFieldName()) || keys_.IsShardIndex(ns, qe.RightFieldName())) {
					throw Error(errLogic, "Shard key cannot be compared with another field");
				}
			},
			[&](const Bracket&) {
				for (auto i = it.cbegin().PlainIterator(), end = it.cend().PlainIterator(); i != end; ++i) {
					i->Visit(
						Skip<AlwaysFalse, AlwaysTrue, JoinQueryEntry, Bracket, SubQueryEntry, MultiDistinctQueryEntry, KnnQueryEntry>{},
						[&](const QueryEntry& qe) {
							if (keys_.IsShardIndex(ns, qe.FieldName())) {
								throw Error(errLogic, "Shard key condition cannot be included in bracket");
							}
						},
						[&](const SubQueryFieldEntry& sqe) {
							if (keys_.IsShardIndex(ns, sqe.FieldName())) {
								throw Error(errLogic, "Subqueries can not be used in the conditions with sharding key ({})",
											sqe.FieldName());
							}
						},
						[&](const BetweenFieldsQueryEntry& qe) {
							if (keys_.IsShardIndex(ns, qe.LeftFieldName()) || keys_.IsShardIndex(ns, qe.RightFieldName())) {
								throw Error(errLogic, "Shard key cannot be compared with another field");
							}
						});
				}
			});
	}

	// TODO check not update shard key
	//	for (const UpdateEntry &entry : q.UpdateFields()) {
	//	}
	return containsKey;
}

std::pair<ShardIDsContainer, Variant> RoutingStrategy::GetHostsIdsKeyPair(const Query& q) const {
	int hostId = ShardingKeyType::ProxyOff;
	Variant shardKey;
	const std::string_view mainNs = q.NsName();
	const bool mainNsIsSharded = keys_.IsSharded(mainNs);
	bool hasShardingKeys = mainNsIsSharded && getHostIdForQuery(q, hostId, shardKey);
	const bool mainQueryToAllShards = mainNsIsSharded && !hasShardingKeys;

	for (const auto& jq : q.GetJoinQueries()) {
		if (!keys_.IsSharded(jq.NsName())) {
			continue;
		}
		if (getHostIdForQuery(jq, hostId, shardKey)) {
			hasShardingKeys = true;
		} else {
			if (hasShardingKeys) {
				throw Error(errLogic, "Join query must contain shard key");
			}
		}
	}
	for (const auto& mq : q.GetMergeQueries()) {
		if (!keys_.IsSharded(mq.NsName())) {
			continue;
		}
		if (getHostIdForQuery(mq, hostId, shardKey)) {
			hasShardingKeys = true;
		} else {
			if (hasShardingKeys) {
				throw Error(errLogic, "Merge query must contain shard key");
			}
		}
	}
	for (const auto& sq : q.GetSubQueries()) {
		if (!keys_.IsSharded(sq.NsName())) {
			continue;
		}
		if (getHostIdForQuery(sq, hostId, shardKey)) {
			hasShardingKeys = true;
		} else {
			if (hasShardingKeys) {
				throw Error(errLogic, "Subquery must contain shard key");
			}
		}
	}
	if (mainQueryToAllShards || !hasShardingKeys) {
		if (!q.GetJoinQueries().empty() || !q.GetMergeQueries().empty() || !q.GetSubQueries().empty()) {
			throw Error(errLogic, "Query to all shard can't contain JOIN, MERGE or SUBQUERY");
		}
		for (const auto& agg : q.aggregations_) {
			if (agg.Type() == AggAvg || agg.Type() == AggFacet || agg.Type() == AggDistinct || agg.Type() == AggUnknown) {
				throw Error(errLogic, "Query to all shard can't contain aggregations AVG, Facet or Distinct");
			}
		}
		return {{keys_.GetShardsIds(mainNs)}, shardKey};
	}
	return {{hostId}, shardKey};
}

std::pair<int, Variant> RoutingStrategy::GetHostIdKeyPair(std::string_view ns, const Item& item) const {
	const auto nsIndex = keys_.GetIndex(ns);
	const VariantArray v = item[nsIndex.name];
	if (!v.IsNullValue() && !v.empty()) {
		if (v.empty() || v.size() > 1) {
			throw Error(errLogic, "Sharding key value cannot be empty or an array");
		}
		assert(nsIndex.values);

		return {keys_.GetShardId(ns, v[0]), Variant(v[0]).EnsureHold()};
	}
	throw Error(errLogic, "Item does not contain proper sharding key for '{}' (key with name '{}' is expected)", ns, nsIndex.name);
}

ConnectStrategy::ConnectStrategy(const cluster::ShardingConfig& config, Connections& connections, int thisShard) noexcept
	: config_(config), connections_(connections), thisShard_(thisShard) {}

std::shared_ptr<client::Reindexer> ConnectStrategy::Connect(int shardId, Error& reconnectStatus) {
	shared_lock rlk(connections_.m);

	if (connections_.shutdown) {
		reconnectStatus = connections_.status;
		return {};
	}
	if (connections_.actualIndex.has_value()) {
		const size_t index = connections_.actualIndex.value();
		assert(index < connections_.size());
		reconnectStatus = connections_[index]->Status();
		if (reconnectStatus.ok()) {
			return connections_[index];
		}
	}
	const auto reconnectTs = connections_.reconnectTs;
	rlk.unlock();

	unique_lock wlk(connections_.m);
	if (connections_.shutdown) {
		reconnectStatus = connections_.status;
		return {};
	}
	if (reconnectTs == connections_.reconnectTs) {
		logFmt(LogInfo, "[sharding proxy] Performing reconnect...");
		auto conn = doReconnect(shardId, reconnectStatus);
		connections_.status = reconnectStatus;
		connections_.reconnectTs = steady_clock_w::now();
		const auto reconnectTs2 = connections_.reconnectTs;
		logFmt(LogInfo, "[sharding proxy] Reconnect result: {}", reconnectStatus.ok() ? "OK" : reconnectStatus.what());

		if (reconnectStatus.ok()) {
			wlk.unlock();
			rlk.lock();
			// Stop unused connections
			if (!connections_.shutdown && reconnectTs2 == connections_.reconnectTs) {
				for (size_t i = 0; i < connections_.size(); ++i) {
					if (i != connections_.actualIndex) {
						connections_[i]->Stop();
					}
				}
			}
		}
		return conn;
	}
	reconnectStatus = connections_.status;
	if (connections_.status.ok() && connections_.actualIndex.has_value()) {
		return connections_[connections_.actualIndex.value()];
	}
	return {};
}

std::shared_ptr<client::Reindexer> ConnectStrategy::doReconnect(int shardID, Error& reconnectStatus) {
	const auto& dsns = config_.shards.at(shardID);
	const size_t size = connections_.size();

	for (size_t i = 0; i < size; ++i) {
		auto err = connections_[i]->Connect(dsns[i]);
		(void)err;	// ignore; Errors will be checked during the further requests
	}

	std::shared_ptr<SharedStatusData> stData = std::make_shared<SharedStatusData>(size);
	for (size_t i = 0; i < size; ++i) {
		auto conn = connections_[i];
		auto& c = *conn;
		auto err = c.WithCompletion([i, stData, conn = std::move(conn)](const Error& err) {
						lock_guard lck(stData->mtx);
						++stData->completed;
						if (err.ok()) {
							stData->onlineIdx = i;
							stData->statuses[i] = true;
							stData->cv.notify_one();
						} else if (stData->completed == stData->statuses.size()) {
							stData->cv.notify_one();
						}
					}).Status(true);
		if (!err.ok()) {
			lock_guard lck(stData->mtx);
			++stData->completed;
		}
	}
	const Query q = Query(std::string(kReplicationStatsNamespace)).Where("type", CondEq, Variant(cluster::kClusterReplStatsType));
	unique_lock lck(stData->mtx);
	stData->cv.wait(lck, [stData] { return stData->onlineIdx >= 0 || stData->completed == stData->statuses.size(); });
	const int idx = stData->onlineIdx;
	lck.unlock();
	if (idx < 0) {
		reconnectStatus = Error(errNetwork, "No shards available");
		return {};
	}

	auto& conn = *connections_[idx];
	client::QueryResults qr;
	auto err = conn.WithTimeout(config_.reconnectTimeout).Select(q, qr);
	if (err.code() == errNetwork) {
		conn.Stop();

		logFmt(LogTrace, "[sharding proxy] Shard {} reconnects to shard {} via {}", thisShard_, shardID, dsns[idx]);
		err = conn.Connect(dsns[idx]);
		qr = client::QueryResults();
		if (err.ok()) {
			err = conn.WithTimeout(config_.reconnectTimeout).Select(q, qr);
		}
	}
	reconnectStatus = std::move(err);
	if (!reconnectStatus.ok()) {
		return {};
	}

	switch (qr.Count()) {
		case 1: {
			WrSerializer wser;
			reconnectStatus = qr.begin().GetJSON(wser, false);
			if (!reconnectStatus.ok()) {
				return {};
			}

			cluster::ReplicationStats stats;
			reconnectStatus = stats.FromJSON(giftStr(wser.Slice()));
			if (!reconnectStatus.ok()) {
				return {};
			}

			if (stats.nodeStats.size()) {
				auto res = tryConnectToLeader(dsns, stats, reconnectStatus);
				if (reconnectStatus.ok()) {
					logFmt(
						LogTrace, "[sharding proxy] Shard {} will proxy data to shard {} (cluster) via {}", thisShard_, shardID,
						connections_.actualIndex.has_value() ? fmt::format("{}", dsns[connections_.actualIndex.value()]) : "'Unknown DSN'");
					return res;
				}
				break;
			}
		}
			[[fallthrough]];
		case 0:
			// Single node without cluster
			connections_.actualIndex = idx;
			reconnectStatus = Error();
			logFmt(LogTrace, "[sharding proxy] Shard {} will proxy data to shard {} (single node) via {}", thisShard_, shardID,
				   dsns[connections_.actualIndex.value()]);
			return connections_[idx];
		default:
			reconnectStatus = Error(errLogic, "Unexpected results count for #replicationstats: {}", qr.Count());
	}

	return {};
}

std::shared_ptr<client::Reindexer> ConnectStrategy::tryConnectToLeader(const std::vector<DSN>& dsns, const cluster::ReplicationStats& stats,
																	   Error& reconnectStatus) {
	auto tryGetConnection = [this, &reconnectStatus](const std::vector<DSN>& dsns,
													 const DSN& dsn) -> std::shared_ptr<reindexer::client::Reindexer> {
		for (size_t i = 0; i < dsns.size(); ++i) {
			if (RelaxCompare(dsns[i], dsn)) {
				auto& conn = *connections_[i];
				reconnectStatus = conn.WithTimeout(config_.reconnectTimeout).Status(true);
				connections_.actualIndex = i;
				return connections_[i];
			}
		}
		return {};
	};

	auto leaderIt = std::find_if(stats.nodeStats.begin(), stats.nodeStats.end(),
								 [](const cluster::NodeStats& node) { return node.role == cluster::RaftInfo::Role::Leader; });

	std::shared_ptr<reindexer::client::Reindexer> connection;
	if (leaderIt != stats.nodeStats.end() && (connection = tryGetConnection(dsns, leaderIt->dsn), connection)) {
		return connection;
	}

	for (auto& node : stats.nodeStats) {
		if (node.role == cluster::RaftInfo::Role::Leader) {
			continue;
		}
		if (auto conn = tryGetConnection(dsns, node.dsn)) {
			return conn;
		}
	}

	reconnectStatus = Error(errLogic, "Unable to connect to either the leader-shard or the allowed followers");
	return {};
}

LocatorService::LocatorService(ClusterProxy& rx, cluster::ShardingConfig config)
	: rx_(rx), config_(std::move(config)), routingStrategy_(config_), actualShardId(config_.thisShardId) {}

Error LocatorService::convertShardingKeysValues(KeyValueType fieldType, std::vector<cluster::ShardingConfig::Key>& keys) {
	return fieldType.EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String, KeyValueType::Bool,
							KeyValueType::Int, KeyValueType::Uuid> auto) -> Error {
			try {
				for (auto& k : keys) {
					for (auto& [l, r, _] : k.values) {
						(void)_;
						std::ignore = l.convert(fieldType, nullptr, nullptr);
						std::ignore = r.convert(fieldType, nullptr, nullptr);
					}
				}
			} catch (const Error& err) {
				return err;
			}
			return Error();
		},
		[](concepts::OneOf<KeyValueType::Composite, KeyValueType::Tuple> auto) {
			return Error{errLogic, "Sharding by composite index is unsupported"};
		},
		[fieldType](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Null, KeyValueType::FloatVector> auto) {
			return Error{errLogic, "Unsupported field type: {}", fieldType.Name()};
		});
}

Error LocatorService::validateConfig() {
	for (auto& ns : config_.namespaces) {
		if (rx_.IsFulltextOrVector(ns.ns, ns.index)) {
			return Error(errLogic, "Sharding by full text or vector index is not supported: {}", ns.index);
		}
		int field = ShardingKeyType::NotSetShard;
		PayloadType pt = rx_.GetPayloadType(ns.ns);
		if (!pt) {
			continue;
		}
		if (pt.FieldByName(ns.index, field)) {
			const KeyValueType fieldType{pt.Field(field).Type()};
			Error err = convertShardingKeysValues(fieldType, ns.keys);
			if (!err.ok()) {
				return err;
			}
		} else {
			return Error(errLogic, "Sharding field is supposed to have index: '{}:{}'", ns.ns, ns.index);
		}
	}
	return errOK;
}

Error LocatorService::Start() {
	Error status = validateConfig();
	if (!status.ok()) {
		return status;
	}

	hostsConnections_.clear();

	client::ReindexerConfig cfg;
	cfg.AppName = "sharding_proxy_from_shard_" + std::to_string(ActualShardId());
	cfg.SyncRxCoroCount = cluster::kDefaultShardingProxyCoroPerConn;
	if (config_.proxyConnConcurrency > 0) {
		if (size_t(config_.proxyConnConcurrency) < kMaxShardingProxyConnConcurrency) {
			cfg.SyncRxCoroCount = config_.proxyConnConcurrency;
		} else {
			cfg.SyncRxCoroCount = kMaxShardingProxyConnConcurrency;
		}
	}
	cfg.EnableCompression = true;
	cfg.RequestDedicatedThread = true;

	uint32_t proxyConnCount = cluster::kDefaultShardingProxyConnCount;
	if (config_.proxyConnCount > 0) {
		if (size_t(config_.proxyConnCount) < kMaxShardingProxyConnCount) {
			proxyConnCount = config_.proxyConnCount;
		} else {
			proxyConnCount = kMaxShardingProxyConnCount;
		}
	}
	const uint32_t proxyConnThreads = config_.proxyConnThreads > 0 ? config_.proxyConnThreads : cluster::kDefaultShardingProxyConnThreads;

	for (const auto& [shardId, hosts] : config_.shards) {
		Connections connections;
		if (ActualShardId() == shardId) {
			// We do not need connections to current shard
			continue;
		}

		connections.reserve(hosts.size());
		for (const auto& dsn : hosts) {
			auto& connection = connections.emplace_back(std::make_shared<client::Reindexer>(
				client::Reindexer(cfg, proxyConnCount, proxyConnThreads).WithShardId(int(shardId), true)));

			logFmt(LogInfo, "[sharding proxy] Shard {} connects to shard {} via {}. Conns count: {}, threads count: {}", ActualShardId(),
				   shardId, dsn, proxyConnCount, proxyConnThreads);
			status = connection->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
			if (!status.ok()) {
				return Error(errLogic, "Error connecting to shard [{}]: {}", dsn, status.what());
			}
		}

		hostsConnections_.emplace(shardId, std::move(connections));
	}
	networkMonitor_.Configure(hostsConnections_, config_.shardsAwaitingTimeout, config_.reconnectTimeout);
	return status;
}

ConnectionsPtr LocatorService::GetShardsConnections(std::string_view ns, int shardId, Error& status) {
	bool rebuildCache = false;
	shared_lock lk(m_);
	ConnectionsPtr connections = getConnectionsFromCache(ns, shardId, rebuildCache);
	lk.unlock();
	if (rebuildCache) {
		unique_lock lck(m_);
		connections = getConnectionsFromCache(ns, shardId, rebuildCache);
		if (rebuildCache) {
			connections = rebuildConnectionsVector(ns, shardId, status);
			if (!status.ok()) {
				return connections;
			}
			auto rowIt = connectionsCache_.find(ns);
			if (rowIt == connectionsCache_.end()) {
				auto insPair = connectionsCache_.emplace(ns, ConnectionTable());
				rowIt = insPair.first;
			}
			rowIt->second.emplace(shardId, connections);
		}
	}
	return connections;
}

ConnectionsPtr LocatorService::GetAllShardsConnections(Error& status) {
	ConnectionsPtr connections = std::make_shared<h_vector<ShardConnection, kHvectorConnStack>>();
	connections->reserve(hostsConnections_.size());
	for (const auto& [shardID, con] : hostsConnections_) {
		connections->emplace_back(getShardConnection(shardID, status), shardID);
		if (!status.ok()) {
			return {};
		}
		if (connections->back().IsOnThisShard() && connections->size() > 1) {
			connections->back() = std::move(connections->front());
			connections->front() = {nullptr, shardID};
		}
	}
	return connections;
}

ConnectionsPtr LocatorService::GetShardsConnectionsWithId(const Query& q, Error& status) {
	ShardIDsContainer ids = routingStrategy_.GetHostsIdsKeyPair(q).first;
	assert(ids.size() > 0);
	if (ids.size() > 1) {
		return GetShardsConnections(q.NsName(), ShardingKeyType::NotSetShard, status);
	} else {
		return GetShardsConnections(q.NsName(), ids[0], status);
	}
}

void LocatorService::Shutdown() {
	for (auto& conns : hostsConnections_) {
		conns.second.Shutdown();
	}
	networkMonitor_.Shutdown();
}

ConnectionsPtr LocatorService::rebuildConnectionsVector(std::string_view ns, int shardId, Error& status) {
	ConnectionsPtr connections = std::make_shared<h_vector<ShardConnection, kHvectorConnStack>>();
	ShardIDsContainer ids;
	const bool emptyNsName = ns == "";
	if (shardId == ShardingKeyType::NotSetShard) {
		if (emptyNsName) {
			ids = routingStrategy_.GetHostsIds();
		} else {
			ids = routingStrategy_.GetHostsIds(ns);
		}
	} else {
		ids.push_back(shardId);
	}
	connections->reserve(ids.size());
	for (const int shardID : ids) {
		if (emptyNsName) {
			connections->emplace_back(getShardConnection(shardID, status), shardID);
		} else {
			connections->emplace_back(GetShardConnection(ns, shardID, status), shardID);
		}
		if (!status.ok()) {
			return {};
		}
		if (connections->back().IsOnThisShard() && connections->size() > 1) {
			connections->back() = std::move(connections->front());
			connections->front() = {nullptr, shardID};
		}
	}

	return connections;
}

ConnectionsPtr LocatorService::getConnectionsFromCache(std::string_view ns, int shardId, bool& requiresRebuild) {
	ConnectionsPtr connections;
	const auto it = connectionsCache_.find(ns);
	if (it != connectionsCache_.end()) {
		const auto nsConnectTableRow = it->second.find(shardId);
		if (nsConnectTableRow != it->second.end()) {
			connections = nsConnectTableRow->second;
			for (auto& connection : *connections) {
				if (!connection.IsOnThisShard() && !connection.IsConnected()) {
					requiresRebuild = true;
					break;
				}
			}
		} else {
			requiresRebuild = true;
		}
	} else {
		requiresRebuild = true;
	}
	return connections;
}

std::shared_ptr<client::Reindexer> LocatorService::GetShardConnection(std::string_view ns, int shardId, Error& status) {
	if (shardId == ShardingKeyType::NotSetShard) {
		const auto defaultShard = routingStrategy_.GetDefaultHost(ns);
		if (actualShardId == defaultShard) {
			return {};
		}
		shardId = defaultShard;
	}
	auto it = hostsConnections_.find(shardId);
	if (actualShardId == shardId || it == hostsConnections_.end()) {
		return {};
	}

	return peekHostForShard(it->second, shardId, status);
}

std::shared_ptr<client::Reindexer> LocatorService::getShardConnection(int shardId, Error& status) {
	if (shardId == ShardingKeyType::NotSetShard) {
		throw Error(errLogic, "Cannot get connection for undefined shard");
	}
	if (actualShardId == shardId) {
		return {};
	}
	return peekHostForShard(hostsConnections_[shardId], shardId, status);
}

Connections::Connections(Connections&& obj) noexcept
	: base(static_cast<base&&>(obj)),
	  actualIndex(std::move(obj.actualIndex)),
	  reconnectTs(obj.reconnectTs),
	  status(std::move(obj.status)),
	  shutdown(obj.shutdown) {}

Connections::Connections(const Connections& obj) noexcept
	: base(obj), actualIndex(obj.actualIndex), reconnectTs(obj.reconnectTs), status(obj.status), shutdown(obj.shutdown) {}

void Connections::Shutdown() {
	lock_guard lck(m);
	if (!shutdown) {
		for (auto& conn : *this) {
			conn->Stop();
		}
		shutdown = true;
		status = Error(errTerminated, "Sharding proxy is already shut down");
	}
}

}  // namespace sharding
}  // namespace reindexer
