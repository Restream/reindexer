#include "sharding.h"
#include "cluster/stats/replicationstats.h"
#include "core/clusterproxy.h"
#include "core/defnsconfigs.h"
#include "core/item.h"
#include "core/type_consts.h"
#include "tools/logger.h"

namespace reindexer {
namespace sharding {

constexpr size_t kMaxShardingProxyConnCount = 64;
constexpr size_t kMaxShardingProxyConnConcurrency = 1024;

RoutingStrategy::RoutingStrategy(const cluster::ShardingConfig &config) : keys_(config) {}

bool RoutingStrategy::getHostIdForQuery(const Query &q, int &hostId) const {
	bool containsKey = false;
	std::string_view ns = q.NsName();
	for (auto it = q.Entries().cbegin(), next = it, end = q.Entries().cend(); it != end; ++it) {
		++next;
		it->InvokeAppropriate<void>(
			Skip<AlwaysTrue, AlwaysFalse, JoinQueryEntry, SubQueryEntry>{},
			[&](const QueryEntry &qe) {
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
			[&](const SubQueryFieldEntry &sqe) {
				if (keys_.IsShardIndex(ns, sqe.FieldName())) {
					throw Error(errLogic, "Subqueries can not be used in the conditions with sharding key (%s)", sqe.FieldName());
				}
			},
			[&](const BetweenFieldsQueryEntry &qe) {
				if (keys_.IsShardIndex(ns, qe.LeftFieldName()) || keys_.IsShardIndex(ns, qe.RightFieldName())) {
					throw Error(errLogic, "Shard key cannot be compared with another field");
				}
			},
			[&](const Bracket &) {
				for (auto i = it.cbegin().PlainIterator(), end = it.cend().PlainIterator(); i != end; ++i) {
					i->InvokeAppropriate<void>(
						Skip<AlwaysFalse, AlwaysTrue, JoinQueryEntry, Bracket, SubQueryEntry>{},
						[&](const QueryEntry &qe) {
							if (keys_.IsShardIndex(ns, qe.FieldName())) {
								throw Error(errLogic, "Shard key condition cannot be included in bracket");
							}
						},
						[&](const SubQueryFieldEntry &sqe) {
							if (keys_.IsShardIndex(ns, sqe.FieldName())) {
								throw Error(errLogic, "Subqueries can not be used in the conditions with sharding key (%s)",
											sqe.FieldName());
							}
						},
						[&](const BetweenFieldsQueryEntry &qe) {
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

ShardIDsContainer RoutingStrategy::GetHostsIds(const Query &q) const {
	int hostId = ShardingKeyType::ProxyOff;
	const std::string_view mainNs = q.NsName();
	const bool mainNsIsSharded = keys_.IsSharded(mainNs);
	bool hasShardingKeys = mainNsIsSharded && getHostIdForQuery(q, hostId);
	const bool mainQueryToAllShards = mainNsIsSharded && !hasShardingKeys;

	for (const auto &jq : q.GetJoinQueries()) {
		if (!keys_.IsSharded(jq.NsName())) continue;
		if (getHostIdForQuery(jq, hostId)) {
			hasShardingKeys = true;
		} else {
			if (hasShardingKeys) {
				throw Error(errLogic, "Join query must contain shard key");
			}
		}
	}
	for (const auto &mq : q.GetMergeQueries()) {
		if (!keys_.IsSharded(mq.NsName())) continue;
		if (getHostIdForQuery(mq, hostId)) {
			hasShardingKeys = true;
		} else {
			if (hasShardingKeys) {
				throw Error(errLogic, "Merge query must contain shard key");
			}
		}
	}
	for (const auto &sq : q.GetSubQueries()) {
		if (!keys_.IsSharded(sq.NsName())) continue;
		if (getHostIdForQuery(sq, hostId)) {
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
		for (const auto &agg : q.aggregations_) {
			if (agg.Type() == AggAvg || agg.Type() == AggFacet || agg.Type() == AggDistinct || agg.Type() == AggUnknown) {
				throw Error(errLogic, "Query to all shard can't contain aggregations AVG, Facet or Distinct");
			}
		}
		return keys_.GetShardsIds(mainNs);
	}
	return {hostId};
}

int RoutingStrategy::GetHostId(std::string_view ns, const Item &item) const {
	const auto nsIndex = keys_.GetIndex(ns);
	const VariantArray v = item[nsIndex.name];
	if (!v.IsNullValue() && !v.empty()) {
		if (v.empty() || v.size() > 1) {
			throw Error(errLogic, "Sharding key value cannot be empty or an array");
		}
		assert(nsIndex.values);

		return keys_.GetShardId(ns, v[0]);
	}
	throw Error(errLogic, "Item does not contain proper sharding key for '%s' (key with name '%s' is expected)", ns, nsIndex.name);
}

ConnectStrategy::ConnectStrategy(const cluster::ShardingConfig &config, Connections &connections, int thisShard) noexcept
	: config_(config), connections_(connections), thisShard_(thisShard) {}

std::shared_ptr<client::Reindexer> ConnectStrategy::Connect(int shardId, Error &reconnectStatus) {
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

	std::unique_lock wlk(connections_.m);
	if (connections_.shutdown) {
		reconnectStatus = connections_.status;
		return {};
	}
	if (reconnectTs == connections_.reconnectTs) {
		logPrintf(LogInfo, "[sharding proxy] Performing reconnect...");
		auto conn = doReconnect(shardId, reconnectStatus);
		connections_.status = reconnectStatus;
		connections_.reconnectTs = std::chrono::steady_clock::now();
		const auto reconnectTs = connections_.reconnectTs;
		logPrintf(LogInfo, "[sharding proxy] Reconnect result: %s", reconnectStatus.ok() ? "OK" : reconnectStatus.what());

		if (reconnectStatus.ok()) {
			wlk.unlock();
			rlk.lock();
			// Stop unused connections
			if (!connections_.shutdown && reconnectTs == connections_.reconnectTs) {
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

std::shared_ptr<client::Reindexer> ConnectStrategy::doReconnect(int shardID, Error &reconnectStatus) {
	const auto &dsns = config_.shards.at(shardID);
	const size_t size = connections_.size();

	for (size_t i = 0; i < size; ++i) {
		connections_[i]->Connect(dsns[i]);
	}

	std::shared_ptr<SharedStatusData> stData = std::make_shared<SharedStatusData>(size);
	for (size_t i = 0; i < size; ++i) {
		auto conn = connections_[i];
		auto &c = *conn;
		auto err = c.WithCompletion([i, stData, conn = std::move(conn)](const Error &err) {
						std::lock_guard lck(stData->mtx);
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
			std::lock_guard lck(stData->mtx);
			++stData->completed;
		}
	}
	const Query q = Query(std::string(kReplicationStatsNamespace)).Where("type", CondEq, Variant(cluster::kClusterReplStatsType));
	std::unique_lock lck(stData->mtx);
	stData->cv.wait(lck, [stData] { return stData->onlineIdx >= 0 || stData->completed == stData->statuses.size(); });
	const int idx = stData->onlineIdx;
	lck.unlock();
	if (idx < 0) {
		reconnectStatus = Error(errNetwork, "No shards available");
		return {};
	}

	auto &conn = *connections_[idx];
	client::QueryResults qr;
	auto err = conn.WithTimeout(config_.reconnectTimeout).Select(q, qr);
	if (err.code() == errNetwork) {
		conn.Stop();

		logPrintf(LogTrace, "[sharding proxy] Shard %d reconnects to shard %d via %s", thisShard_, shardID, dsns[idx]);
		conn.Connect(dsns[idx]);
		qr = client::QueryResults();
		err = conn.WithTimeout(config_.reconnectTimeout).Select(q, qr);
	}
	reconnectStatus = std::move(err);
	if (!reconnectStatus.ok()) return {};

	switch (qr.Count()) {
		case 1: {
			WrSerializer wser;
			reconnectStatus = qr.begin().GetJSON(wser, false);
			if (!reconnectStatus.ok()) return {};

			cluster::ReplicationStats stats;
			reconnectStatus = stats.FromJSON(wser.Slice());
			if (!reconnectStatus.ok()) return {};

			if (stats.nodeStats.size()) {
				auto res = tryConnectToLeader(dsns, stats, reconnectStatus);
				if (reconnectStatus.ok()) {
					logPrintf(LogTrace, "[sharding proxy] Shard %d will proxy data to shard %d (cluster) via %s", thisShard_, shardID,
							  connections_.actualIndex.has_value() ? dsns[connections_.actualIndex.value()] : "'Unknow DSN'");
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
			logPrintf(LogTrace, "[sharding proxy] Shard %d will proxy data to shard %d (single node) via %s", thisShard_, shardID,
					  dsns[connections_.actualIndex.value()]);
			return connections_[idx];
		default:
			reconnectStatus = Error(errLogic, "Unexpected results count for #replicationstats: %d", qr.Count());
	}

	return {};
}

std::shared_ptr<client::Reindexer> ConnectStrategy::tryConnectToLeader(const std::vector<std::string> &dsns,
																	   const cluster::ReplicationStats &stats, Error &reconnectStatus) {
	auto tryGetConnection = [this, &reconnectStatus](const std::vector<std::string> &dsns,
													 const std::string &dsn) -> std::shared_ptr<reindexer::client::Reindexer> {
		for (size_t i = 0; i < dsns.size(); ++i) {
			if (dsns[i] == dsn) {
				auto &conn = *connections_[i];
				reconnectStatus = conn.WithTimeout(config_.reconnectTimeout).Status(true);
				connections_.actualIndex = i;
				return connections_[i];
			}
		}
		return {};
	};

	auto leaderIt = std::find_if(stats.nodeStats.begin(), stats.nodeStats.end(),
								 [](const cluster::NodeStats &node) { return node.role == cluster::RaftInfo::Role::Leader; });

	std::shared_ptr<reindexer::client::Reindexer> connection;
	if (leaderIt != stats.nodeStats.end() && (connection = tryGetConnection(dsns, leaderIt->dsn), connection)) return connection;

	for (auto &node : stats.nodeStats) {
		if (node.role == cluster::RaftInfo::Role::Leader) continue;
		if (auto conn = tryGetConnection(dsns, node.dsn)) return conn;
	}

	reconnectStatus = Error(errLogic, "Unable to connect to either the leader-shard or the allowed followers");
	return {};
}

LocatorService::LocatorService(ClusterProxy &rx, cluster::ShardingConfig config)
	: rx_(rx), config_(std::move(config)), routingStrategy_(config_), actualShardId(config_.thisShardId) {}

Error LocatorService::convertShardingKeysValues(KeyValueType fieldType, std::vector<cluster::ShardingConfig::Key> &keys) {
	return fieldType.EvaluateOneOf(
		[&](OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::String, KeyValueType::Bool, KeyValueType::Int,
				  KeyValueType::Uuid>) -> Error {
			try {
				for (auto &k : keys) {
					for (auto &[l, r, _] : k.values) {
						(void)_;
						l.convert(fieldType, nullptr, nullptr);
						r.convert(fieldType, nullptr, nullptr);
					}
				}
			} catch (const Error &err) {
				return err;
			}
			return Error();
		},
		[](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) {
			return Error{errLogic, "Sharding by composite index is unsupported"};
		},
		[fieldType](OneOf<KeyValueType::Undefined, KeyValueType::Null>) {
			return Error{errLogic, "Unsupported field type: %s", fieldType.Name()};
		});
}

Error LocatorService::validateConfig() {
	for (auto &ns : config_.namespaces) {
		const auto ftIndexes = rx_.GetFTIndexes(ns.ns);
		if (ftIndexes.find(ns.index) != ftIndexes.cend()) {
			return Error(errLogic, "Sharding by full text index is not supported: %s", ns.index);
		}
		int field = ShardingKeyType::NotSetShard;
		PayloadType pt = rx_.GetPayloadType(ns.ns);
		if (!pt) continue;
		if (pt.FieldByName(ns.index, field)) {
			const KeyValueType fieldType{pt.Field(field).Type()};
			Error err = convertShardingKeysValues(fieldType, ns.keys);
			if (!err.ok()) {
				return err;
			}
		} else {
			return Error(errLogic, "Sharding field is supposed to have index: '%s:%s'", ns.ns, ns.index);
		}
	}
	return errOK;
}

Error LocatorService::Start() {
	Error status = validateConfig();
	if (!status.ok()) return status;

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

	for (const auto &[shardId, hosts] : config_.shards) {
		Connections connections;
		if (ActualShardId() == shardId) {
			// We do not need connections to current shard
			continue;
		}

		connections.reserve(hosts.size());
		for (const std::string &dsn : hosts) {
			auto &connection = connections.emplace_back(std::make_shared<client::Reindexer>(
				client::Reindexer(cfg, proxyConnCount, proxyConnThreads).WithShardId(int(shardId), true)));

			logPrintf(LogInfo, "[sharding proxy] Shard %d connects to shard %d via %s. Conns count: %d, threads count: %d", ActualShardId(),
					  shardId, dsn, proxyConnCount, proxyConnThreads);
			status = connection->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
			if (!status.ok()) {
				return Error(errLogic, "Error connecting to shard [%s]: %s", dsn, status.what());
			}
		}

		hostsConnections_.emplace(shardId, std::move(connections));
	}
	networkMonitor_.Configure(hostsConnections_, config_.shardsAwaitingTimeout, config_.reconnectTimeout);
	return status;
}

ConnectionsPtr LocatorService::GetShardsConnections(std::string_view ns, int shardId, Error &status) {
	bool rebuildCache = false;
	shared_lock lk(m_);
	ConnectionsPtr connections = getConnectionsFromCache(ns, shardId, rebuildCache);
	lk.unlock();
	if (rebuildCache) {
		std::unique_lock lck(m_);
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

ConnectionsPtr LocatorService::GetAllShardsConnections(Error &status) {
	ConnectionsPtr connections = std::make_shared<h_vector<ShardConnection, kHvectorConnStack>>();
	connections->reserve(hostsConnections_.size());
	for (const auto &[shardID, con] : hostsConnections_) {
		connections->emplace_back(getShardConnection(shardID, status), shardID);
		if (!status.ok()) return {};
		if (connections->back().IsOnThisShard() && connections->size() > 1) {
			connections->back() = std::move(connections->front());
			connections->front() = {nullptr, shardID};
		}
	}
	return connections;
}

ConnectionsPtr LocatorService::GetShardsConnectionsWithId(const Query &q, Error &status) {
	ShardIDsContainer ids = routingStrategy_.GetHostsIds(q);
	assert(ids.size() > 0);
	if (ids.size() > 1) {
		return GetShardsConnections(q.NsName(), ShardingKeyType::NotSetShard, status);
	} else {
		return GetShardsConnections(q.NsName(), ids[0], status);
	}
}

void LocatorService::Shutdown() {
	for (auto &conns : hostsConnections_) {
		conns.second.Shutdown();
	}
	networkMonitor_.Shutdown();
}

ConnectionsPtr LocatorService::rebuildConnectionsVector(std::string_view ns, int shardId, Error &status) {
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
		if (!status.ok()) return {};
		if (connections->back().IsOnThisShard() && connections->size() > 1) {
			connections->back() = std::move(connections->front());
			connections->front() = {nullptr, shardID};
		}
	}

	return connections;
}

ConnectionsPtr LocatorService::getConnectionsFromCache(std::string_view ns, int shardId, bool &requiresRebuild) {
	ConnectionsPtr connections;
	auto it = connectionsCache_.find(ns);
	if (it != connectionsCache_.end()) {
		auto nsConnnectTableRow = it->second.find(shardId);
		if (nsConnnectTableRow != it->second.end()) {
			connections = nsConnnectTableRow->second;
			for (auto &connection : *connections) {
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

std::shared_ptr<client::Reindexer> LocatorService::GetShardConnection(std::string_view ns, int shardId, Error &status) {
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

std::shared_ptr<client::Reindexer> LocatorService::getShardConnection(int shardId, Error &status) {
	if (shardId == ShardingKeyType::NotSetShard) {
		throw Error(errLogic, "Cannot get connection for undefined shard");
	}
	if (actualShardId == shardId) {
		return {};
	}
	return peekHostForShard(hostsConnections_[shardId], shardId, status);
}

}  // namespace sharding
}  // namespace reindexer
