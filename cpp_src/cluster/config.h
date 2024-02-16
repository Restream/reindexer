#pragma once

#include <chrono>
#include <optional>
#include <variant>
#include "core/keyvalue/variant.h"
#include "estl/fast_hash_set.h"
#include "estl/span.h"
#include "sharding/ranges.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace YAML {
class Node;
}

namespace reindexer {

class JsonBuilder;
class WrSerializer;

namespace cluster {

inline uint32_t GetConsensusForN(uint32_t n) noexcept { return n / 2 + 1; }
constexpr auto kStatusCmdTimeout = std::chrono::seconds(3);
constexpr size_t kMaxRetriesOnRoleSwitchAwait = 50;
constexpr auto kRoleSwitchStepTime = std::chrono::milliseconds(150);

struct NodeData {
	int serverId = -1;
	int electionsTerm = 0;
	std::string dsn;

	Error FromJSON(span<char> json);
	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb) const;
	void GetJSON(WrSerializer& ser) const;
};

struct RaftInfo {
	enum class Role : uint8_t { None, Leader, Follower, Candidate };
	int32_t leaderId = -1;
	Role role = Role::None;

	bool operator==(const RaftInfo& rhs) const noexcept { return role == rhs.role && leaderId == rhs.leaderId; }
	bool operator!=(const RaftInfo& rhs) const noexcept { return !(*this == rhs); }

	Error FromJSON(span<char> json);
	Error FromJSON(const gason::JsonNode& root);
	void GetJSON(JsonBuilder& jb) const;
	void GetJSON(WrSerializer& ser) const;
	static std::string_view RoleToStr(Role);
	static Role RoleFromStr(std::string_view);
};

struct ClusterNodeConfig {
	int GetServerID() const noexcept { return serverId; }
	const std::string& GetRPCDsn() const noexcept { return dsn; }
	const std::string& GetManagementDsn() const noexcept { return dsn; }
	void FromYAML(const YAML::Node& yaml);

	bool operator==(const ClusterNodeConfig& rdata) const noexcept { return (serverId == rdata.serverId) && (dsn == rdata.dsn); }

	int serverId = -1;
	std::string dsn;
};

struct AsyncReplConfigData;
enum class AsyncReplicationMode { Default, FromClusterLeader };

class AsyncReplNodeConfig {
public:
	class NamespaceListImpl {
	public:
		NamespaceListImpl() {}
		NamespaceListImpl(fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str>&& n) : data(std::move(n)) {}
		bool IsInList(std::string_view ns, size_t hash) const noexcept { return data.empty() || (data.find(ns, hash) != data.end()); }
		bool IsInList(std::string_view ns) const noexcept { return data.empty() || (data.find(ns) != data.end()); }
		bool Empty() const noexcept { return data.empty(); }
		bool operator==(const NamespaceListImpl& r) const noexcept { return data == r.data; }

		const fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> data;
	};
	using NamespaceList = intrusive_atomic_rc_wrapper<NamespaceListImpl>;

	AsyncReplNodeConfig() = default;
	AsyncReplNodeConfig(std::string _dsn) : dsn(std::move(_dsn)) {}

	int GetServerID() const noexcept { return -1; }
	const std::string& GetRPCDsn() const { return dsn; }
	void FromYAML(const YAML::Node& yaml);
	void FromJSON(const gason::JsonNode& root);
	void GetJSON(JsonBuilder& jb) const;
	void GetYAML(YAML::Node& yaml) const;

	bool HasOwnNsList() const noexcept { return hasOwnNsList_; }
	void SetOwnNamespaceList(fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> nss) {
		namespaces_ = make_intrusive<NamespaceList>(std::move(nss));
		hasOwnNsList_ = true;
	}
	void SetNamespaceListFromConfig(const AsyncReplConfigData& config);
	intrusive_ptr<NamespaceList> Namespaces() const noexcept {
		assert(namespaces_);
		return namespaces_;
	}
	std::vector<std::string> GetNssVector() const {
		std::vector<std::string> nss;
		auto nssPtr = Namespaces();
		const auto& data = nssPtr->data;
		nss.reserve(data.size());
		for (auto& ns : data) {
			nss.emplace_back(ns);
		}
		return nss;
	}
	void SetReplicationMode(AsyncReplicationMode mode) noexcept { replicationMode_ = mode; }
	const std::optional<AsyncReplicationMode>& GetReplicationMode() const noexcept { return replicationMode_; }

	bool operator==(const AsyncReplNodeConfig& rdata) const noexcept {
		return (dsn == rdata.dsn) && nsListsAreEqual(rdata) && replicationModesAreEqual(rdata);
	}

	std::string dsn;

private:
	bool nsListsAreEqual(const AsyncReplNodeConfig& rdata) const noexcept {
		return (!hasOwnNsList_ && !rdata.hasOwnNsList_) || (!rdata.namespaces_ && !namespaces_) ||
			   (hasOwnNsList_ && rdata.hasOwnNsList_ && rdata.namespaces_ && namespaces_ && *rdata.namespaces_ == *namespaces_);
	}
	bool replicationModesAreEqual(const AsyncReplNodeConfig& rdata) const noexcept {
		return (!replicationMode_.has_value() && !rdata.replicationMode_.has_value()) || replicationMode_ == rdata.replicationMode_;
	}

	intrusive_ptr<NamespaceList> namespaces_;
	bool hasOwnNsList_ = false;
	std::optional<AsyncReplicationMode> replicationMode_;
};

constexpr size_t kDefaultClusterProxyConnCount = 8;
constexpr size_t kDefaultClusterProxyCoroPerConn = 4;
constexpr size_t kDefaultClusterProxyConnThreads = 2;

struct ClusterConfigData {
	Error FromYAML(const std::string& yaml);

	bool operator==(const ClusterConfigData& rdata) const noexcept {
		return (nodes == rdata.nodes) && (retrySyncIntervalMSec == rdata.retrySyncIntervalMSec) &&
			   (onlineUpdatesTimeoutSec == rdata.onlineUpdatesTimeoutSec) && (enableCompression == rdata.enableCompression) &&
			   (appName == rdata.appName) && (replThreadsCount == rdata.replThreadsCount) &&
			   (parallelSyncsPerThreadCount == rdata.parallelSyncsPerThreadCount) &&
			   (batchingRoutinesCount == rdata.batchingRoutinesCount) && (leaderSyncThreads == rdata.leaderSyncThreads) &&
			   (leaderSyncConcurrentSnapshotsPerNode == rdata.leaderSyncConcurrentSnapshotsPerNode) &&
			   (syncTimeoutSec == rdata.syncTimeoutSec) && (proxyConnCount == rdata.proxyConnCount) &&
			   (proxyConnConcurrency == rdata.proxyConnConcurrency) && (proxyConnThreads == rdata.proxyConnThreads) &&
			   (logLevel == rdata.logLevel);
	}
	bool operator!=(const ClusterConfigData& rdata) const noexcept { return !operator==(rdata); }

	unsigned int GetNodeIndexForServerId(int serverId) const {
		for (unsigned int i = 0; i < nodes.size(); ++i) {
			if (nodes[i].serverId == serverId) return i;
		}
		throw Error(errLogic, "Cluster config. Cannot find node index for ServerId(%d)", serverId);
	}

	std::vector<ClusterNodeConfig> nodes;
	fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> namespaces;
	std::string appName = "rx_cluster_node";
	int onlineUpdatesTimeoutSec = 20;
	int syncTimeoutSec = 60;
	int retrySyncIntervalMSec = 3000;
	int replThreadsCount = 4;
	int parallelSyncsPerThreadCount = 2;
	int batchingRoutinesCount = 100;
	bool enableCompression = true;
	int maxWALDepthOnForceSync = 1000;
	int leaderSyncThreads = 8;
	int leaderSyncConcurrentSnapshotsPerNode = 2;
	int proxyConnCount = kDefaultClusterProxyConnCount;
	int proxyConnConcurrency = kDefaultClusterProxyCoroPerConn;
	int proxyConnThreads = kDefaultClusterProxyConnThreads;
	LogLevel logLevel = LogInfo;
};

constexpr uint32_t kDefaultShardingProxyConnCount = 8;
constexpr uint32_t kDefaultShardingProxyCoroPerConn = 8;
constexpr uint32_t kDefaultShardingProxyConnThreads = 4;

struct ShardingConfig {
	static constexpr unsigned serverIdPos = 53;
	static constexpr int64_t serverIdMask = (((1ll << 10) - 1) << serverIdPos);	 // 01111111111000...000
	static constexpr auto kDefaultRollbackTimeout = std::chrono::seconds(30);

	struct Key {
		Error FromYAML(const YAML::Node& yaml, const std::map<int, std::vector<std::string>>& shards, KeyValueType& valuesType,
					   std::vector<sharding::Segment<Variant>>& checkVal);
		Error FromJSON(const gason::JsonNode&, KeyValueType& valuesType, std::vector<sharding::Segment<Variant>>& checkVal);

		void GetYAML(YAML::Node&) const;
		void GetJSON(JsonBuilder&) const;
		int shardId = ShardingKeyType::ProxyOff;
		ShardingAlgorithmType algorithmType = ByValue;

		sharding::Segment<Variant> SegmentFromYAML(const YAML::Node& yaml);
		sharding::Segment<Variant> SegmentFromJSON(const gason::JsonNode& json);
		int RelaxCompare(const std::vector<sharding::Segment<Variant>>&, const CollateOpts& collateOpts = CollateOpts()) const;
		std::vector<sharding::Segment<Variant>> values{};

	private:
		Error checkValue(const sharding::Segment<Variant>& val, KeyValueType& valuesType,
						 const std::vector<sharding::Segment<Variant>>& checkVal);
	};
	struct Namespace {
		Error FromYAML(const YAML::Node& yaml, const std::map<int, std::vector<std::string>>& shards);
		Error FromJSON(const gason::JsonNode&);
		void GetYAML(YAML::Node&) const;
		void GetJSON(JsonBuilder&) const;
		std::string ns;
		std::string index;
		std::vector<Key> keys;
		int defaultShard = ShardingKeyType::ProxyOff;
	};

	Error FromYAML(const std::string& yaml);
	Error FromJSON(span<char> json);
	Error FromJSON(const gason::JsonNode&);
	std::string GetYAML() const;
	YAML::Node GetYAMLObj() const;
	std::string GetJSON() const;
	void GetJSON(WrSerializer&) const;
	void GetJSON(JsonBuilder&) const;
	Error Validate() const;
	std::vector<Namespace> namespaces;
	std::map<int, std::vector<std::string>> shards;
	int thisShardId = ShardingKeyType::ProxyOff;
	std::chrono::milliseconds reconnectTimeout = std::chrono::milliseconds(3000);
	std::chrono::seconds shardsAwaitingTimeout = std::chrono::seconds(30);
	std::chrono::seconds configRollbackTimeout = kDefaultRollbackTimeout;
	int proxyConnCount = kDefaultShardingProxyConnCount;
	int proxyConnConcurrency = kDefaultShardingProxyCoroPerConn;
	int proxyConnThreads = kDefaultShardingProxyConnThreads;
	int64_t sourceId = ShardingSourceId::NotSet;
};
bool operator==(const ShardingConfig&, const ShardingConfig&);
inline bool operator!=(const ShardingConfig& l, const ShardingConfig& r) { return !(l == r); }
bool operator==(const ShardingConfig::Key&, const ShardingConfig::Key&);
bool operator==(const ShardingConfig::Namespace&, const ShardingConfig::Namespace&);

struct AsyncReplConfigData {
	using NamespaceList = AsyncReplNodeConfig::NamespaceList;
	enum class Role { None, Leader, Follower };

	Error FromYAML(const std::string& yml);
	Error FromJSON(std::string_view json);
	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb) const;
	void GetYAML(WrSerializer& ser) const;
	static Role Str2role(std::string_view role) noexcept;
	static std::string Role2str(Role) noexcept;
	static AsyncReplicationMode Str2mode(std::string_view mode);
	static std::string Mode2str(AsyncReplicationMode) noexcept;

	std::string appName = "rx_repl_leader";
	Role role = Role::None;
	AsyncReplicationMode mode = AsyncReplicationMode::Default;
	int replThreadsCount = 4;
	int parallelSyncsPerThreadCount = 2;
	int onlineUpdatesTimeoutSec = 20;
	int syncTimeoutSec = 60;
	int retrySyncIntervalMSec = 20 * 1000;
	bool forceSyncOnLogicError = false;		// TODO: Use this for test purposes
	bool forceSyncOnWrongDataHash = false;	// TODO: Use this for test purposes
	intrusive_ptr<NamespaceList> namespaces = make_intrusive<NamespaceList>();
	bool enableCompression = true;
	int batchingRoutinesCount = 100;
	int maxWALDepthOnForceSync = 1000;
	std::vector<AsyncReplNodeConfig> nodes;
	int onlineUpdatesDelayMSec = 100;
	LogLevel logLevel = LogNone;

	bool operator==(const AsyncReplConfigData& rdata) const noexcept {
		return (role == rdata.role) && (mode == rdata.mode) && (replThreadsCount == rdata.replThreadsCount) &&
			   (parallelSyncsPerThreadCount == rdata.parallelSyncsPerThreadCount) &&
			   (forceSyncOnLogicError == rdata.forceSyncOnLogicError) && (forceSyncOnWrongDataHash == rdata.forceSyncOnWrongDataHash) &&
			   (retrySyncIntervalMSec == rdata.retrySyncIntervalMSec) && (onlineUpdatesTimeoutSec == rdata.onlineUpdatesTimeoutSec) &&
			   (namespaces == rdata.namespaces || (namespaces && rdata.namespaces && *namespaces == *rdata.namespaces)) &&
			   (enableCompression == rdata.enableCompression) && (appName == rdata.appName) &&
			   (batchingRoutinesCount == rdata.batchingRoutinesCount) && (maxWALDepthOnForceSync == rdata.maxWALDepthOnForceSync) &&
			   (syncTimeoutSec == rdata.syncTimeoutSec) && (onlineUpdatesDelayMSec == rdata.onlineUpdatesDelayMSec) &&
			   (logLevel == rdata.logLevel) && (nodes == rdata.nodes);
	}
	bool operator!=(const AsyncReplConfigData& rdata) const noexcept { return !operator==(rdata); }
};

}  // namespace cluster
}  // namespace reindexer
