#pragma once

#include <memory>
#include "client/reindexer.h"
#include "cluster/config.h"
#include "estl/condition_variable.h"
#include "estl/fast_hash_map.h"
#include "estl/mutex.h"
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

class [[nodiscard]] RoutingStrategy {
public:
	explicit RoutingStrategy(const cluster::ShardingConfig& config);
	~RoutingStrategy() = default;

	std::pair<ShardIDsContainer, Variant> GetHostsIdsKeyPair(const Query&) const;
	std::pair<int, Variant> GetHostIdKeyPair(std::string_view ns, const Item&) const;
	ShardIDsContainer GetHostsIds(std::string_view ns) const { return keys_.GetShardsIds(ns); }
	ShardIDsContainer GetHostsIds() const { return keys_.GetShardsIds(); }
	bool IsShardingKey(std::string_view ns, std::string_view index) const { return keys_.IsShardIndex(ns, index); }
	bool IsSharded(std::string_view ns) const noexcept { return keys_.IsSharded(ns); }
	int GetDefaultHost(std::string_view ns) const { return keys_.GetDefaultHost(ns); }

private:
	bool getHostIdForQuery(const Query&, int& currentId, Variant& shardKey) const;
	ShardingKeys keys_;
};

class [[nodiscard]] Connections : public std::vector<std::shared_ptr<client::Reindexer>> {
public:
	using base = std::vector<std::shared_ptr<client::Reindexer>>;
	Connections() = default;
	Connections(Connections&& obj) noexcept;
	Connections(const Connections& obj) noexcept;

	void Shutdown();
	shared_timed_mutex m;
	std::optional<size_t> actualIndex;
	steady_clock_w::time_point reconnectTs;
	Error status;
	bool shutdown = false;
};

struct [[nodiscard]] ShardConnection : std::shared_ptr<client::Reindexer> {
	using base = std::shared_ptr<client::Reindexer>;
	ShardConnection() = default;
	ShardConnection(base&& c, int id) : base(static_cast<base&&>(c)), shardID(id) {}
	client::Reindexer* operator->() const noexcept { return get(); }
	bool IsOnThisShard() const noexcept { return get() == nullptr; }
	bool IsConnected() const noexcept { return get() != nullptr && static_cast<const base&>(*this)->Status().ok(); }
	int ShardId() const noexcept { return shardID; }

private:
	int shardID = ShardingKeyType::ProxyOff;
};

using ConnectionsVector = h_vector<ShardConnection, kHvectorConnStack>;
using ConnectionsPtr = std::shared_ptr<ConnectionsVector>;

class [[nodiscard]] IConnectStrategy {
public:
	virtual ~IConnectStrategy() = default;
	virtual std::shared_ptr<client::Reindexer> Connect(int shardID, Error& status) = 0;
};

class [[nodiscard]] ConnectStrategy : public IConnectStrategy {
public:
	ConnectStrategy(const cluster::ShardingConfig& config_, Connections& connections, int thisShard) noexcept;
	~ConnectStrategy() override = default;
	std::shared_ptr<client::Reindexer> Connect(int shardID, Error& reconnectStatus) override final;

private:
	struct [[nodiscard]] SharedStatusData {
		SharedStatusData(size_t cnt) : statuses(cnt, false) {}

		size_t completed = 0;
		int onlineIdx = -1;
		std::vector<bool> statuses;
		mutex mtx;
		condition_variable cv;
	};

	std::shared_ptr<client::Reindexer> doReconnect(int shardID, Error& reconnectStatus);
	std::shared_ptr<client::Reindexer> tryConnectToLeader(const std::vector<DSN>& dsns, const cluster::ReplicationStats& stats,
														  Error& reconnectStatus);
	const cluster::ShardingConfig& config_;
	Connections& connections_;
	int thisShard_;
};

class [[nodiscard]] LocatorService {
public:
	LocatorService(ClusterProxy& rx, cluster::ShardingConfig config);
	~LocatorService() { Shutdown(); }
	LocatorService(const LocatorService&) = delete;
	LocatorService(LocatorService&&) = delete;
	LocatorService& operator=(const LocatorService&) = delete;
	LocatorService& operator=(LocatorService&&) = delete;

	Error Start();
	Error AwaitShards(const RdxContext& ctx) { return networkMonitor_.AwaitShards(ctx); }
	bool IsSharded(std::string_view ns) const noexcept { return routingStrategy_.IsSharded(ns); }
	std::pair<int, Variant> GetShardIdKeyPair(std::string_view ns, const Item& item) const {
		return routingStrategy_.GetHostIdKeyPair(ns, item);
	}
	std::pair<ShardIDsContainer, Variant> GetShardIdKeyPair(const Query& q) const { return routingStrategy_.GetHostsIdsKeyPair(q); }
	std::shared_ptr<client::Reindexer> GetShardConnection(std::string_view ns, int shardId, Error& status);

	ConnectionsPtr GetShardsConnections(std::string_view ns, int shardId, Error& status) RX_REQUIRES(!m_);
	ConnectionsPtr GetShardsConnectionsWithId(const Query& q, Error& status) RX_REQUIRES(!m_);
	ConnectionsPtr GetShardsConnections(Error& status) RX_REQUIRES(!m_) { return GetShardsConnections("", -1, status); }
	ConnectionsPtr GetAllShardsConnections(Error& status);
	ShardConnection GetShardConnectionWithId(std::string_view ns, const Item& item, Error& status) {
		int shardId = routingStrategy_.GetHostIdKeyPair(ns, item).first;
		return ShardConnection(GetShardConnection(ns, shardId, status), shardId);
	}

	int ActualShardId() const noexcept { return actualShardId; }
	int64_t SourceId() const noexcept { return config_.sourceId; }
	void Shutdown();

private:
	ConnectionsPtr rebuildConnectionsVector(std::string_view ns, int shardId, Error& status);
	ConnectionsPtr getConnectionsFromCache(std::string_view ns, int shardId, bool& requiresRebuild);
	std::shared_ptr<client::Reindexer> getShardConnection(int shardId, Error& status);
	std::shared_ptr<client::Reindexer> peekHostForShard(Connections& connections, int shardId, Error& status) const {
		return ConnectStrategy(config_, connections, ActualShardId()).Connect(shardId, status);
	}
	Error validateConfig();
	Error convertShardingKeysValues(KeyValueType fieldType, std::vector<cluster::ShardingConfig::Key>& keys);

	ClusterProxy& rx_;
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
	using ConnectionsCache = fast_hash_map<std::string, ConnectionTable, nocase_hash_str, nocase_equal_str, nocase_less_str>;
	ConnectionsCache connectionsCache_;
	NetworkMonitor networkMonitor_;
};

}  // namespace sharding
}  // namespace reindexer
