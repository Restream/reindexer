#include "sharding.h"
#include "core/item.h"
#include "core/reindexerimpl.h"

namespace reindexer {
namespace sharding {

RoutingStrategy::RoutingStrategy(const cluster::ShardingConfig &config) : config_(config), keys_(config_) {}

bool RoutingStrategy::getHostIdForQuery(const Query &q, int &hostId) const {
	std::string_view ns = q.Namespace();
	for (auto it = q.entries.begin(); it != q.entries.end(); ++it) {
		if (it->IsLeaf()) {
			bool isShardKey = false;
			const QueryEntry &qe = it->Value();
			int id = keys_.GetShardId(ns, qe.index, qe.values, isShardKey);
			if (isShardKey) {
				if (id == IndexValueType::NotSet) return false;
				if (hostId != IndexValueType::NotSet && hostId != id) return false;
				hostId = id;
			}
		}
	}
	for (const UpdateEntry &entry : q.UpdateFields()) {
		if (entry.mode == FieldModeSet || entry.mode == FieldModeSetJson) {
			bool isShardKey = false;
			int id = keys_.GetShardId(ns, entry.column, entry.values, isShardKey);
			if (isShardKey) {
				if (id == IndexValueType::NotSet) return false;
				if (hostId != IndexValueType::NotSet && hostId != id) return false;
				hostId = id;
			}
		} else if (entry.mode == FieldModeDrop) {
			int id = keys_.GetShardId(ns, entry.column);
			if (id != IndexValueType::NotSet) {
				if (hostId != IndexValueType::NotSet && hostId != id) return false;
				hostId = id;
			}
		}
	}
	assert(hostId <= (int)config_.keys.size());
	return true;
}

std::vector<int> RoutingStrategy::GetHostsIds(const Query &q) const {
	int hostId = IndexValueType::NotSet;
	bool result = getHostIdForQuery(q, hostId);
	for (size_t i = 0; result && i < q.joinQueries_.size(); ++i) {
		result = getHostIdForQuery(q.joinQueries_[i], hostId);
	}
	if (result && hostId == IndexValueType::NotSet && q.count != 0) {
		return keys_.GetShardsIds(q.Namespace());
	}
	return {hostId};
}

int RoutingStrategy::GetHostId(std::string_view ns, const Item &item) const {
	bool clientPt = (item.NumFields() == 1);
	for (std::string_view index : keys_.GetIndexes(ns)) {
		int shardId = IndexValueType::NotSet;
		if (clientPt) {
			VariantArray v = item[index];
			if (!v.IsNullValue() && !v.empty()) {
				bool isShardKey = false;
				shardId = keys_.GetShardId(ns, index, v, isShardKey);
			}
		} else {
			int field = item.GetFieldIndex(index);
			if (field == IndexValueType::NotSet) continue;
			bool isShardKey = false;
			VariantArray v = item[field];
			shardId = keys_.GetShardId(ns, index, v, isShardKey);
		}
		if (shardId != IndexValueType::NotSet) {
			assert(shardId <= int(config_.keys.size()));
			return shardId;
		}
	}
	return IndexValueType::NotSet;
}

std::vector<int> RoutingStrategy::GetHostsIds(std::string_view ns) const { return keys_.GetShardsIds(ns); }
std::vector<int> RoutingStrategy::GetHostsIds() const { return keys_.GetShardsIds(); }
bool RoutingStrategy::IsShardingKey(std::string_view ns, std::string_view index) const {
	return keys_.GetShardId(ns, index) != IndexValueType::NotSet;
}

ConnectStrategy::ConnectStrategy(const cluster::ShardingConfig &config, Connections &connections) noexcept
	: config_(config), connections_(connections) {}

std::shared_ptr<client::SyncCoroReindexer> ConnectStrategy::Connect(int shardId, bool writeOperation, Error &reconnectStatus) {
	std::shared_lock<std::shared_mutex> rlk(connections_.m);

	const int index = connections_.actualIndex;
	assert(index < int(connections_.size()));
	reconnectStatus = connections_[index]->Status();
	if (reconnectStatus.ok()) {
		return connections_[index];
	}

	rlk.unlock();
	std::unique_lock wlk(connections_.m);

	if (index == connections_.actualIndex) {
		return doReconnect(shardId, writeOperation, reconnectStatus);
	} else {
		reconnectStatus = connections_.status;
		if (connections_.status.ok()) {
			return connections_[connections_.actualIndex];
		}
		return {};
	}
}

std::shared_ptr<client::SyncCoroReindexer> ConnectStrategy::doReconnect(int shardID, bool writeOperation, Error &reconnectStatus) {
	bool isProxyShard = (shardID == 0);
	const int size = int(connections_.size());
	const int configIndex = isProxyShard ? 0 : shardID - 1;

	connections_.status = errOK;
	std::map<int64_t, int, std::greater<>> orderByLsn;
	for (int i = 0; i < size; ++i) {
		connections_.status = connections_[i]->Status();
		if (!connections_.status.ok()) {
			connections_[i]->Stop();
			const string &dsn = isProxyShard ? config_.proxyDsns[i] : config_.keys[configIndex].hostsDsns[i];
			connections_[i]->Connect(dsn);
			connections_.status = connections_[i]->Status();
		}
		if (connections_.status.ok()) {
			ReplicationStateV2 replState;
			if (connections_[i]->GetReplState(config_.keys[configIndex].ns, replState).ok()) {
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

Error LocatorService::validateConfig() const {
	InternalRdxContext ctx;
	for (const cluster::ShardingKey &key : config_.keys) {
		bool nsExists = false;
		FieldsSet pk = rx_.getPK(key.ns, nsExists, ctx);
		if (nsExists) {
			int field = IndexValueType::NotSet;
			PayloadType pt = rx_.getPayloadType(key.ns, ctx);
			if (pt.FieldByName(key.index, field)) {
				if (!pk.contains(field)) {
					return Error(errLogic, "Sharding key can only be PK: %s:%s", key.ns, key.index);
				}
			} else {
				return Error(errLogic, "Sharding field is supposed to have index: '%s:%s'", key.ns, key.index);
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
	hostsConnections_.reserve(config_.keys.size() + 1);	 // + proxy shard

	Connections proxyConnections;
	proxyConnections.reserve(config_.proxyDsns.size());

	for (size_t i = 0; i != config_.proxyDsns.size(); ++i) {
		proxyConnections.emplace_back(std::make_shared<client::SyncCoroReindexer>(client::SyncCoroReindexer().WithShardId(0)));
		status = proxyConnections.back()->Connect(config_.proxyDsns[i], client::ConnectOpts().CreateDBIfMissing());
		if (!status.ok()) {
			return Error(errLogic, "Error connecting to sharding proxy host [%s]: %s", config_.proxyDsns[i], status.what());
		}
	}
	hostsConnections_.emplace_back(std::move(proxyConnections));

	for (const cluster::ShardingKey &key : config_.keys) {
		Connections connections;
		connections.reserve(key.hostsDsns.size());
		for (const string &dsn : key.hostsDsns) {
			size_t shardId = hostsConnections_.size();
			auto &connection = connections.emplace_back(
				std::make_shared<client::SyncCoroReindexer>(client::SyncCoroReindexer().WithShardId(int(shardId))));
			status = connection->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
			if (!status.ok()) {
				return Error(errLogic, "Error connecting to shard [%s]: %s", dsn, status.what());
			}
		}
		hostsConnections_.emplace_back(std::move(connections));
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
				auto newConnection = connectStrategy.Connect(connection.ShardId(), true, status);
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
			*connections = {{GetShardConnection(IndexValueType::NotSet, true, status), 0}};
		} else {
			connections->reserve(ids.size());
			for (const int shardID : ids) {
				connections->emplace_back(GetShardConnection(shardID, true, status), shardID);
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
	return getShardsConnections(routingStrategy_->GetHostsIds(q), q.Type() != QuerySelect, status);
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetShardConnection(std::string_view ns, const Item &item, Error &status) {
	return GetShardConnection(routingStrategy_->GetHostId(ns, item), true, status);
}

std::vector<std::shared_ptr<client::SyncCoroReindexer>> LocatorService::GetShardsConnections(Error &status) {
	return getShardsConnections(routingStrategy_->GetHostsIds(), true, status);
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetShardConnection(int shardId, bool writeOp, Error &status) {
	if (shardId == IndexValueType::NotSet) {
		if (isProxy_) return {};
		return peekHostForShard(hostsConnections_[0], 0, writeOp, status);
	}
	if (actualShardId == shardId) {
		return {};
	}
	return peekHostForShard(hostsConnections_[shardId], shardId, writeOp, status);
}

std::vector<std::shared_ptr<client::SyncCoroReindexer>> LocatorService::getShardsConnections(std::vector<int> &&ids, bool writeOp,
																							 Error &status) {
	if (ids.empty()) {
		return {GetShardConnection(IndexValueType::NotSet, writeOp, status)};
	}
	std::vector<std::shared_ptr<client::SyncCoroReindexer>> connections;
	connections.reserve(ids.size());
	for (const int shardId : ids) {
		connections.emplace_back(GetShardConnection(shardId, writeOp, status));
		if (!status.ok()) break;
		if (connections.back().get() == nullptr && connections.size() > 1) {
			connections.back() = std::move(connections.front());
			connections.front() = nullptr;
		}
	}
	return connections;
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::peekHostForShard(Connections &connections, int shardId, bool writeOp,
																			Error &status) {
	ConnectStrategy connectStrategy(config_, connections);
	return connectStrategy.Connect(shardId, writeOp, status);
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetProxyShardConnection(bool writeOp, Error &status) {
	return GetShardConnection(0, writeOp, status);
}

std::shared_ptr<client::SyncCoroReindexer> LocatorService::GetConnection(int shardId, Error &status) {
	return peekHostForShard(hostsConnections_[shardId], shardId, true, status);
}

}  // namespace sharding
}  // namespace reindexer
