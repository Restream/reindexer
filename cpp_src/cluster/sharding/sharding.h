#pragma once

#include <memory>
#include "client/reindexer.h"
#include "cluster/config.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "networkmonitor.h"
#include "shardingkeys.h"

namespace reindexer {

class Item;
class ReindexerImpl;

namespace cluster {
struct ReplicationStats;
}

namespace sharding {

using ShardConnectionsContainer = h_vector<std::shared_ptr<client::Reindexer>, kHvectorConnStack>;

class RoutingStrategy {
public:
	explicit RoutingStrategy(const cluster::ShardingConfig &config);
	~RoutingStrategy() = default;

	ShardIDsContainer GetHostsIds(const Query &) const;
	int GetHostId(std::string_view ns, const Item &) const;
	ShardIDsContainer GetHostsIds(std::string_view ns) const { return keys_.GetShardsIds(ns); }
	ShardIDsContainer GetHostsIds() const { return keys_.GetShardsIds(); }
	bool IsShardingKey(std::string_view ns, std::string_view index) const { return keys_.IsShardIndex(ns, index); }
	bool IsSharded(std::string_view ns) const noexcept { return keys_.IsSharded(ns); }
	int GetDefaultHost(std::string_view ns) const { return keys_.GetDefaultHost(ns); }

private:
	bool getHostIdForQuery(const Query &, int &currentId) const;
	const cluster::ShardingConfig &config_;
	ShardingKeys keys_;
};

class Connections : public std::vector<std::shared_ptr<client::Reindexer>> {
public:
	using base = std::vector<std::shared_ptr<client::Reindexer>>;
	Connections() : base() {}
	Connections(Connections &&obj) noexcept
		: base(static_cast<base &&>(obj)),
		  actualIndex(std::move(obj.actualIndex)),
		  reconnectTs(obj.reconnectTs),
		  status(std::move(obj.status)),
		  shutdown(obj.shutdown) {}
	void Shutdown() {
		std::lock_guard lck(m);
		if (!shutdown) {
			for (auto &conn : *this) {
				conn->Stop();
			}
			shutdown = true;
			status = Error(errTerminated, "Sharding proxy is already shut down");
		}
	}
	shared_timed_mutex m;
	std::optional<size_t> actualIndex;
	std::chrono::steady_clock::time_point reconnectTs;
	Error status;
	bool shutdown = false;
};

struct ShardConnection : std::shared_ptr<client::Reindexer> {
	using base = std::shared_ptr<client::Reindexer>;
	ShardConnection() = default;
	ShardConnection(base &&c, int id) : base(static_cast<base &&>(c)), shardID(id) {}
	client::Reindexer *operator->() const noexcept { return get(); }
	bool IsOnThisShard() const noexcept { return get() == nullptr; }
	bool IsConnected() const noexcept { return get() != nullptr && static_cast<const base &>(*this)->Status().ok(); }
	int ShardId() const noexcept { return shardID; }

private:
	int shardID = ShardingKeyType::ProxyOff;
};

using ConnectionsVector = h_vector<ShardConnection, kHvectorConnStack>;
using ConnectionsPtr = std::shared_ptr<ConnectionsVector>;

class IConnectStrategy {
public:
	virtual ~IConnectStrategy() = default;
	virtual std::shared_ptr<client::Reindexer> Connect(int shardID, Error &status) = 0;
};

class ConnectStrategy : public IConnectStrategy {
public:
	ConnectStrategy(const cluster::ShardingConfig &config_, Connections &connections, int thisShard) noexcept;
	~ConnectStrategy() override = default;
	std::shared_ptr<client::Reindexer> Connect(int shardID, Error &reconnectStatus) override final;

private:
	struct SharedStatusData {
		SharedStatusData(size_t cnt) : statuses(cnt, false) {}

		size_t completed = 0;
		int onlineIdx = -1;
		std::vector<bool> statuses;
		std::mutex mtx;
		std::condition_variable cv;
	};

	std::shared_ptr<client::Reindexer> doReconnect(int shardID, Error &reconnectStatus);
	std::shared_ptr<client::Reindexer> tryConnectToLeader(const std::vector<std::string> &dsns, const cluster::ReplicationStats &stats,
														  Error &reconnectStatus);
	const cluster::ShardingConfig &config_;
	Connections &connections_;
	int thisShard_;
};

class LocatorService {
public:
	LocatorService(ClusterProxy &rx, const cluster::ShardingConfig &config);
	~LocatorService() = default;
	LocatorService(const LocatorService &) = delete;
	LocatorService(LocatorService &&) = delete;
	LocatorService &operator=(const LocatorService &) = delete;
	LocatorService &operator=(LocatorService &&) = delete;

	Error Start();
	Error AwaitShards(const RdxContext &ctx) { return networkMonitor_.AwaitShards(ctx); }
	bool IsSharded(std::string_view ns) const noexcept { return routingStrategy_.IsSharded(ns); }
	int GetShardId(std::string_view ns, const Item &item) const { return routingStrategy_.GetHostId(ns, item); }
	ShardIDsContainer GetShardId(const Query &q) const { return routingStrategy_.GetHostsIds(q); }
	std::shared_ptr<client::Reindexer> GetShardConnection(std::string_view ns, int shardId, Error &status);

	ConnectionsPtr GetShardsConnections(std::string_view ns, int shardId, Error &status);
	ConnectionsPtr GetShardsConnectionsWithId(const Query &q, Error &status);
	ConnectionsPtr GetShardsConnections(Error &status) { return GetShardsConnections("", -1, status); }
	ShardConnection GetShardConnectionWithId(std::string_view ns, const Item &item, Error &status) {
		int shardId = routingStrategy_.GetHostId(ns, item);
		return ShardConnection(GetShardConnection(ns, shardId, status), shardId);
	}

	int ActualShardId() const noexcept { return actualShardId; }
	void Shutdown();

private:
	ConnectionsPtr rebuildConnectionsVector(std::string_view ns, int shardId, Error &status);
	ConnectionsPtr getConnectionsFromCache(std::string_view ns, int shardId, bool &requiresRebuild);
	std::shared_ptr<client::Reindexer> getShardConnection(int shardId, Error &status);
	std::shared_ptr<client::Reindexer> peekHostForShard(Connections &connections, int shardId, Error &status) {
		return ConnectStrategy(config_, connections, ActualShardId()).Connect(shardId, status);
	}
	Error validateConfig();
	Error convertShardingKeysValues(KeyValueType fieldType, std::vector<cluster::ShardingConfig::Key> &keys);

	ClusterProxy &rx_;
	cluster::ShardingConfig config_;
	RoutingStrategy routingStrategy_;
	const int actualShardId = ShardingKeyType::ProxyOff;
	ConnectionsMap hostsConnections_;

	shared_timed_mutex m_;
	// shardId - vector of connections
	// shardId==-1 connections to all shard for ns
	using ConnectionTable = fast_hash_map<int, ConnectionsPtr>;
	// nsName - connectionTable
	// nsName=="" for all connection
	using ConnectionsCache = fast_hash_map<std::string, ConnectionTable, nocase_hash_str, nocase_equal_str>;
	ConnectionsCache connectionsCache_;
	NetworkMonitor networkMonitor_;
};

}  // namespace sharding
}  // namespace reindexer
