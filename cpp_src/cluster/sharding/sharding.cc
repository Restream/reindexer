#include "sharding.h"
#include "core/item.h"
#include "core/reindexerimpl.h"

namespace reindexer {
namespace sharding {

constexpr size_t kShardingProxyConnCount = 8;
constexpr size_t kShardingProxyCoroPerConn = 2;

RoutingStrategy::RoutingStrategy(const cluster::ShardingConfig &config) : config_(config), keys_(config_) {}

bool RoutingStrategy::getHostIdForQuery(const Query &q, int &hostId) const {
	bool containsKey = false;
	std::string_view ns = q.Namespace();
	for (auto it = q.entries.begin(); it != q.entries.end(); ++it) {
		if (it->HoldsOrReferTo<QueryEntry>()) {
			bool isShardKey = false;
			const QueryEntry &qe = it->Value<QueryEntry>();
			int id = keys_.GetShardId(ns, qe.index, qe.values, isShardKey);
			if (isShardKey) {
				containsKey = true;
				if (qe.condition != CondEq) {
					throw Error(errLogic, "Shard key condition can only be 'Eq'");
				}
				if (hostId == int(ShardIdType::NotInit)) {
					hostId = id;
				}
				if (hostId != id) {
					throw Error(errLogic, "Shard key from other node");
				}
			}
		}
	}
	for (const UpdateEntry &entry : q.UpdateFields()) {
		if (entry.mode == FieldModeSet || entry.mode == FieldModeSetJson) {
			bool isShardKey = false;
			int id = keys_.GetShardId(ns, entry.column, entry.values, isShardKey);
			if (isShardKey) {
				if (id == int(ShardIdType::NotSet)) return false;
				if (hostId != int(ShardIdType::NotSet) && hostId != id) return false;
				hostId = id;
			}
		} else if (entry.mode == FieldModeDrop) {
			int id = keys_.GetShardId(ns, entry.column);
			if (id != int(ShardIdType::NotSet)) {
				if (hostId != int(ShardIdType::NotSet) && hostId != id) return false;
				hostId = id;
			}
		}
	}
	return containsKey;
}

std::vector<int> RoutingStrategy::GetHostsIds(const Query &q) const {
	int hostId = int(ShardIdType::NotInit);
	bool result = getHostIdForQuery(q, hostId);
	if (!result && q.joinQueries_.size() != 0) {
		throw Error(errLogic, "Query to all shard can't contain join");
	}
	for (size_t i = 0; result && i < q.joinQueries_.size(); ++i) {
		result = getHostIdForQuery(q.joinQueries_[i], hostId);
		if (!result) {
			throw Error(errLogic, "Join query must contain shard key.");
		}
	}
	if (!result) {
		if (q.count != 0 || q.calcTotal == ModeAccurateTotal) {
			return keys_.GetShardsIds(q.Namespace());
		} else {  // special case for limit =0 and calcTotal != ModeAccurateTotal (update tagsMatcher)
			return {int(ShardIdType::NotSet)};
		}
	}
	return {hostId};
}

int RoutingStrategy::GetHostId(std::string_view ns, const Item &item) const {
	bool clientPt = (item.NumFields() == 1);
	for (std::string_view index : keys_.GetIndexes(ns)) {
		int shardId = int(ShardIdType::NotSet);
		if (clientPt) {
			VariantArray v = item[index];
			if (!v.IsNullValue() && !v.empty()) {
				bool isShardKey = false;
				shardId = keys_.GetShardId(ns, index, v, isShardKey);
			}
		} else {
			int field = item.GetFieldIndex(index);
			if (field == int(ShardIdType::NotSet)) continue;
			bool isShardKey = false;
			VariantArray v = item[field];
			shardId = keys_.GetShardId(ns, index, v, isShardKey);
		}
		if (shardId != int(ShardIdType::NotSet)) {
			assert(config_.shards.find(shardId) != config_.shards.cend());
			return shardId;
		}
	}
	return int(ShardIdType::NotSet);
}

std::vector<int> RoutingStrategy::GetHostsIds(std::string_view ns) const { return keys_.GetShardsIds(ns); }
std::vector<int> RoutingStrategy::GetHostsIds() const { return keys_.GetShardsIds(); }
bool RoutingStrategy::IsShardingKey(std::string_view ns, std::string_view index) const {
	return keys_.GetShardId(ns, index) != int(ShardIdType::NotSet);
}

ConnectStrategy::ConnectStrategy(const cluster::ShardingConfig &config, Connections &connections) noexcept
	: config_(config), connections_(connections) {}

std::shared_ptr<client::SyncCoroReindexer> ConnectStrategy::Connect(int shardId, bool writeOperation, Error &reconnectStatus,
																	std::optional<std::string_view> ns) {
	std::shared_lock<std::shared_mutex> rlk(connections_.m);

	if (connections_.shutdown) {
		reconnectStatus = connections_.status;
		return {};
	}
	const int index = connections_.actualIndex;
	assert(index < int(connections_.size()));
	reconnectStatus = connections_[index]->Status();
	if (reconnectStatus.ok()) {
		return connections_[index];
	}

	rlk.unlock();
	std::lock_guard wlk(connections_.m);

	if (connections_.shutdown) {
		reconnectStatus = connections_.status;
		return {};
	}
	if (index == connections_.actualIndex) {
		return doReconnect(shardId, writeOperation, reconnectStatus, ns);
	}
	reconnectStatus = connections_.status;
	if (connections_.status.ok()) {
		return connections_[connections_.actualIndex];
	}
	return {};
}

std::shared_ptr<client::SyncCoroReindexer> ConnectStrategy::doReconnect(int shardID, bool writeOperation, Error &reconnectStatus,
																		std::optional<std::string_view> ns) {
	bool isProxyShard = (shardID == 0);
	const int size = int(connections_.size());

	connections_.status = errOK;
	std::map<int64_t, int, std::greater<>> orderByLsn;
	if (!ns && config_.namespaces.size() == 1) {
		ns = config_.namespaces[0].ns;
	}
	for (int i = 0; i < size; ++i) {
		connections_.status = connections_[i]->Status();
		if (!connections_.status.ok()) {
			connections_[i]->Stop();
			const string &dsn = isProxyShard ? config_.proxyDsns[i] : config_.shards.at(shardID)[i];
			connections_[i]->Connect(dsn);
			connections_.status = connections_[i]->Status();
		}
		if (connections_.status.ok()) {
			ReplicationStateV2 replState;
			if (ns && connections_[i]->GetReplState(*ns, replState).ok()) {
				orderByLsn.emplace(int64_t(replState.lastLsn), i);
			} else {
				size_t mirroredIndex = connections_.size() - i - 1;
				orderByLsn.emplace(mirroredIndex, i);
			}
		}
	}

	if (!orderByLsn.empty()) {
		int index = orderByLsn.begin()->second;
		connections_.actualIndex = index;
		if (!writeOperation) {
			reconnectStatus = errOK;
			connections_.status = errOK;
			return connections_[index];
		}
	}

	return {};
}

LocatorService::LocatorService(ReindexerImpl &rx, const cluster::ShardingConfig &config)
	: rx_(rx),
	  config_(config),
	  routingStrategy_(std::make_unique<RoutingStrategy>(config)),
	  actualShardId(config.thisShardId),
	  // shardID == 0 is proxy shard
	  // (should always be set like this in config)
	  isProxy_(actualShardId == 0) {}

Error LocatorService::convertShardingKeysValues(KeyValueType fieldType, std::vector<cluster::ShardingConfig::Key> &keys) {
	switch (fieldType) {
		case KeyValueInt64:
		case KeyValueDouble:
		case KeyValueString:
		case KeyValueBool:
		case KeyValueInt:
			try {
				for (auto &k : keys) {
					for (Variant &v : k.values) {
						v.convert(fieldType, nullptr, nullptr);
					}
				}
			} catch (const Error &err) {
				return err;
			}
			return errOK;
		case KeyValueComposite:
		case KeyValueTuple:
			return Error{errLogic, "Sharding by composite index is unsupported"};
		default:
			return Error{errLogic, "Unsupported field type: %s", KeyValueTypeToStr(fieldType)};
	}
}

Error LocatorService::validateConfig() {
	InternalRdxContext ctx;
	for (auto &ns : config_.namespaces) {
		bool nsExists = false;
		FieldsSet pk = rx_.getPK(ns.ns, nsExists, ctx);
		if (nsExists) {
						const auto ftIndexes = rx_.GetFTIndexes(ns.ns, ctx);
			if (ftIndexes.find(ns.index) != ftIndexes.cend()) {
				return Error(errLogic, "Sharding by full text index is not supported: %s", ns.index);
			}
			int field = int(ShardIdType::NotSet);
			PayloadType pt = rx_.getPayloadType(ns.ns, ctx);
			if (pt.FieldByName(ns.index, field)) {
				if (!pk.contains(field)) {
					return Error(errLogic, "Sharding key can only be PK: %s:%s", ns.ns, ns.index);
				}
				for (const auto &k : ns.keys) {
					if (k.shardId == 0) {
						return Error{errParams, "Shard id 0 is reserved for proxy"};
					}
				}
				const KeyValueType fieldType{pt.Field(field).Type()};
				const Error err{convertShardingKeysValues(fieldType, ns.keys)};
				if (!err.ok()) {
					return err;
				}
			} else {
				return Error(errLogic, "Sharding field is supposed to have index: '%s:%s'", ns.ns, ns.index);
			}
		}
	}
	return errOK;
}

Error LocatorService::Start() {
	Error status = validateConfig();
	if (!status.ok()) return status;

	if (config_.proxyDsns.empty()) {
		return Error(errLogic, "Sharding config should contain proxy-host data");
	}

	hostsConnections_.clear();

	Connections proxyConnections;
	proxyConnections.reserve(config_.proxyDsns.size());
	client::CoroReindexerConfig cfg;
	cfg.AppName = "sharding_proxy";
	cfg.SyncRxCoroCount = kShardingProxyCoroPerConn;

	for (size_t i = 0; i != config_.proxyDsns.size(); ++i) {
		proxyConnections.emplace_back(
			std::make_shared<client::SyncCoroReindexer>(client::SyncCoroReindexer(cfg, kShardingProxyConnCount).WithShardId(0, true)));
		status = proxyConnections.back()->Connect(config_.proxyDsns[i], client::ConnectOpts().CreateDBIfMissing());
		if (!status.ok()) {
			return Error(errLogic, "Error connecting to sharding proxy host [%s]: %s", config_.proxyDsns[i], status.what());
		}
	}
	hostsConnections_.emplace(0, std::move(proxyConnections));

	for (const auto &[shardId, hosts] : config_.shards) {
		Connections connections;
		connections.reserve(hosts.size());
		for (const string &dsn : hosts) {
			auto &connection = connections.emplace_back(std::make_shared<client::SyncCoroReindexer>(
				client::SyncCoroReindexer(cfg, kShardingProxyConnCount).WithShardId(int(shardId), true)));
			status = connection->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
			if (!status.ok()) {
				return Error(errLogic, "Error connecting to shard [%s]: %s", dsn, status.what());
			}
		}
		hostsConnections_.emplace(shardId, std::move(connections));
	}
	return status;
}

vector<int> LocatorService::GetShardId(const Query &q) const { return routingStrategy_->GetHostsIds(q); }

int LocatorService::GetShardId(std::string_view ns, const Item &item) const { return routingStrategy_->GetHostId(ns, item); }

ConnectionsPtr LocatorService::GetShardsConnections(std::string_view ns, Error &status) {
	ConnectionsPtr connections;
	std::shared_lock<std::shared_mutex> lk(m_);
	auto it = connectionsCache_.find(std::string(ns));
	if (it != connectionsCache_.end()) {
		connections = it->second;
		lk.unlock();
		for (auto connection : *connections) {
			if (!connection.IsOnThisShard() && !connection.IsConnected()) {
				ConnectStrategy connectStrategy(config_, hostsConnections_[connection.ShardId()]);
				auto newConnection = connectStrategy.Connect(connection.ShardId(), true, status, ns);
				if (status.ok()) {
					connection.Update(newConnection);
				} else {
					return {};
				}
			}
		}
	} else {
		lk.unlock();
		connections = std::make_shared<std::vector<ShardConnection>>();
		auto ids = routingStrategy_->GetHostsIds(ns);
		if (ids.empty()) {
			*connections = {{GetShardConnection(int(ShardIdType::NotSet), true, status, ns), 0}};
		} else {
			connections->reserve(ids.size());
			for (const int shardID : ids) {
				connections->emplace_back(GetShardConnection(shardID, true, status, ns), shardID);
				if (!status.ok()) return {};
				if (connections->back().IsOnThisShard() && connections->size() > 1) {
					connections->back() = std::move(connections->front());
					connections->front() = {nullptr, shardID};
				}
			}
		}

		std::unique_lock<std::shared_mutex> lock(m_);
		connectionsCache_.emplace(ns, connections);
	}
	return connections;
}

std::vector<std::shared_ptr<client::SyncCoroReindexer>> LocatorService::GetShardsConnections(const Query &q, Error &status) {
	return getShardsConnections(routingStrategy_->GetHostsIds(q), q.Type() != QuerySelect, status, q._namespace);
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetShardConnection(std::string_view ns, const Item &item, Error &status) {
	return GetShardConnection(routingStrategy_->GetHostId(ns, item), true, status, ns);
}

std::vector<std::shared_ptr<client::SyncCoroReindexer>> LocatorService::GetShardsConnections(Error &status) {
	return getShardsConnections(routingStrategy_->GetHostsIds(), true, status, std::nullopt);
}

void LocatorService::Shutdown() {
	for (auto &conns : hostsConnections_) {
		conns.second.Shutdown();
	}
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetShardConnection(int shardId, bool writeOp, Error &status,
																			  std::optional<std::string_view> ns) {
	if (shardId == int(ShardIdType::NotSet)) {
		if (isProxy_) return {};
		return peekHostForShard(hostsConnections_[0], 0, writeOp, status, ns);
	}
	if (actualShardId == shardId) {
		return {};
	}
	return peekHostForShard(hostsConnections_[shardId], shardId, writeOp, status, ns);
}

std::vector<std::shared_ptr<client::SyncCoroReindexer>> LocatorService::getShardsConnections(std::vector<int> &&ids, bool writeOp,
																							 Error &status,
																							 std::optional<std::string_view> ns) {
	if (ids.empty()) {
		return {GetShardConnection(int(ShardIdType::NotSet), writeOp, status, ns)};
	}
	std::vector<std::shared_ptr<client::SyncCoroReindexer>> connections;
	connections.reserve(ids.size());
	for (const int shardId : ids) {
		connections.emplace_back(GetShardConnection(shardId, writeOp, status, ns));
		if (!status.ok()) break;
		if (connections.back().get() == nullptr && connections.size() > 1) {
			connections.back() = std::move(connections.front());
			connections.front() = nullptr;
		}
	}
	return connections;
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::peekHostForShard(Connections &connections, int shardId, bool writeOp,
																			Error &status, std::optional<std::string_view> ns) {
	ConnectStrategy connectStrategy(config_, connections);
	return connectStrategy.Connect(shardId, writeOp, status, ns);
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetProxyShardConnection(bool writeOp, Error &status) {
	return GetShardConnection(0, writeOp, status, std::nullopt);
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetConnection(int shardId, Error &status, std::optional<std::string_view> ns) {
	return peekHostForShard(hostsConnections_[shardId], shardId, true, status, ns);
}

}  // namespace sharding
}  // namespace reindexer
