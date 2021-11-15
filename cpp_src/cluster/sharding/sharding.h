#pragma once

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include "client/synccororeindexer.h"
#include "cluster/config.h"
#include "shardingkeys.h"

namespace reindexer {

class Item;
class ReindexerImpl;

namespace sharding {

class IRoutingStrategy {
public:
	virtual ~IRoutingStrategy() = default;
	virtual bool IsShardingKey(std::string_view ns, std::string_view index) const = 0;
	virtual std::vector<int> GetHostsIds(const Query &) const = 0;
	virtual int GetHostId(std::string_view ns, const Item &) const = 0;
	virtual std::vector<int> GetHostsIds(std::string_view ns) const = 0;
	virtual std::vector<int> GetHostsIds() const = 0;
};

class RoutingStrategy : public IRoutingStrategy {
public:
	explicit RoutingStrategy(const cluster::ShardingConfig &config);
	~RoutingStrategy() override = default;

	std::vector<int> GetHostsIds(const Query &) const final;
	int GetHostId(std::string_view ns, const Item &) const final;
	std::vector<int> GetHostsIds(std::string_view ns) const final;
	std::vector<int> GetHostsIds() const final;
	bool IsShardingKey(std::string_view ns, std::string_view index) const final;

private:
	bool getHostIdForQuery(const Query &, int &currentId) const;
	const cluster::ShardingConfig &config_;
	ShardingKeys keys_;
};

struct Connections : public vector<std::shared_ptr<client::SyncCoroReindexer>> {
	using base = vector<std::shared_ptr<client::SyncCoroReindexer>>;
	Connections() : base(), actualIndex{0} {}
	Connections(Connections &&obj) noexcept : base(static_cast<base &&>(obj)) {
		std::shared_lock<std::shared_mutex> lk(m);
		actualIndex = obj.actualIndex;
	}
	std::shared_mutex m;
	int actualIndex;
	Error status;
};

struct ShardConnection : std::shared_ptr<client::SyncCoroReindexer> {
	using base = std::shared_ptr<client::SyncCoroReindexer>;
	ShardConnection(base &&c, int id) : base(static_cast<base &&>(c)), shardID(id) {}
	void Update(const base &obj) { static_cast<base &>(*this) = obj; }
	client::SyncCoroReindexer *operator->() { return get(); }
	bool IsOnThisShard() const { return get() == nullptr; }
	bool IsConnected() { return get() != nullptr && static_cast<base &>(*this)->Status().ok(); }
	int ShardId() const { return shardID; }

private:
	int shardID = IndexValueType::NotSet;
};
using ConnectionsPtr = std::shared_ptr<vector<ShardConnection>>;

class IConnectStrategy {
public:
	virtual ~IConnectStrategy() = default;
	virtual std::shared_ptr<client::SyncCoroReindexer> Connect(int shardID, bool writeOperation, Error &status) = 0;
};

class ConnectStrategy : public IConnectStrategy {
public:
	ConnectStrategy(const cluster::ShardingConfig &config_, Connections &connections) noexcept;
	~ConnectStrategy() override = default;
	std::shared_ptr<client::SyncCoroReindexer> Connect(int shardID, bool writeOperation, Error &reconnectStatus) final;

private:
	std::shared_ptr<client::SyncCoroReindexer> doReconnect(int shardID, bool writeOperation, Error &reconnectStatus);
	const cluster::ShardingConfig &config_;
	Connections &connections_;
};

class LocatorService {
public:
	LocatorService(ReindexerImpl &rx, const cluster::ShardingConfig &config);
	~LocatorService() = default;
	LocatorService(const LocatorService &) = delete;
	LocatorService(LocatorService &&) = delete;
	LocatorService &operator=(const LocatorService &) = delete;
	LocatorService &operator=(LocatorService &&) = delete;

	Error Start();

	int GetShardId(std::string_view ns, const Item &item) const;
	vector<int> GetShardId(const Query &q) const;

	std::shared_ptr<client::SyncCoroReindexer> GetShardConnection(std::string_view ns, const Item &item, Error &status);
	std::shared_ptr<client::SyncCoroReindexer> GetShardConnection(int shardId, bool writeOp, Error &status);
	std::shared_ptr<client::SyncCoroReindexer> GetProxyShardConnection(bool writeOp, Error &status);
	std::shared_ptr<client::SyncCoroReindexer> GetConnection(int shardId, Error &status);

	ConnectionsPtr GetShardsConnections(std::string_view ns, Error &status);
	std::vector<std::shared_ptr<client::SyncCoroReindexer>> GetShardsConnections(const Query &q, Error &status);
	std::vector<std::shared_ptr<client::SyncCoroReindexer>> GetShardsConnections(Error &status);

private:
	std::shared_ptr<client::SyncCoroReindexer> peekHostForShard(Connections &, int shardId, bool writeOp, Error &status);
	std::vector<std::shared_ptr<client::SyncCoroReindexer>> getShardsConnections(std::vector<int> &&ids, bool writeOp, Error &status);
	Error validateConfig() const;

	ReindexerImpl &rx_;
	cluster::ShardingConfig config_;
	std::unique_ptr<IRoutingStrategy> routingStrategy_;
	int actualShardId = IndexValueType::NotSet;
	vector<Connections> hostsConnections_;
	bool isProxy_ = false;

	std::shared_mutex m_;
	using ConnectionsCache = std::unordered_map<std::string, ConnectionsPtr, nocase_hash_str, nocase_equal_str>;
	ConnectionsCache connectionsCache_;
};

}  // namespace sharding
}  // namespace reindexer
