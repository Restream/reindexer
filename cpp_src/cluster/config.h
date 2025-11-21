#pragma once

#include <chrono>
#include <optional>
#include <span>
#include "core/keyvalue/variant.h"
#include "core/namespace/namespacenamesets.h"
#include "sharding/ranges.h"
#include "tools/dsn.h"
#include "tools/errors.h"

namespace gason {
struct JsonNode;
}

namespace YAML {
class Node;
}

namespace reindexer {

class WrSerializer;

constexpr char kAsyncReplicationCfgName[] = "async_replication";
constexpr char kAsyncReplicationNodesCfgName[] = "nodes";
constexpr char kAsyncReplicationDSNCfgName[] = "dsn";
constexpr char kShardingCfgName[] = "sharding";
constexpr char kShardingShardsCfgName[] = "shards";
constexpr char kShardingDSNsCfgName[] = "dsns";

namespace cluster {

inline uint32_t GetConsensusForN(uint32_t n) noexcept { return n / 2 + 1; }
constexpr auto kStatusCmdTimeout = std::chrono::seconds(3);
constexpr size_t kMaxRetriesOnRoleSwitchAwait = 50;
constexpr auto kRoleSwitchStepTime = std::chrono::milliseconds(150);

struct [[nodiscard]] NodeData {
	int serverId = -1;
	int electionsTerm = 0;
	DSN dsn;

	Error FromJSON(std::span<char> json);
	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb) const;
	void GetJSON(WrSerializer& ser) const;
};

struct [[nodiscard]] RaftInfo {
	enum class [[nodiscard]] Role : uint8_t { None, Leader, Follower, Candidate };
	int32_t leaderId = -1;
	Role role = Role::None;

	bool operator==(const RaftInfo& rhs) const noexcept { return role == rhs.role && leaderId == rhs.leaderId; }
	bool operator!=(const RaftInfo& rhs) const noexcept { return !(*this == rhs); }

	Error FromJSON(std::span<char> json);
	Error FromJSON(const gason::JsonNode& root);
	void GetJSON(JsonBuilder& jb) const;
	void GetJSON(WrSerializer& ser) const;
	static std::string_view RoleToStr(Role);
	static Role RoleFromStr(std::string_view);
};

struct [[nodiscard]] ClusterNodeConfig {
	int GetServerID() const noexcept { return serverId; }
	const DSN& GetRPCDsn() const noexcept { return dsn; }
	const DSN& GetManagementDsn() const noexcept { return dsn; }
	void FromYAML(const YAML::Node& yaml);

	bool operator==(const ClusterNodeConfig& rdata) const noexcept { return (serverId == rdata.serverId) && (dsn == rdata.dsn); }

	int serverId = -1;
	DSN dsn;
};

struct AsyncReplConfigData;
enum class [[nodiscard]] AsyncReplicationMode { Default, FromClusterLeader };
enum class [[nodiscard]] MaskingDSN : bool { Disabled = false, Enabled = true };

class [[nodiscard]] AsyncReplNodeConfig {
public:
	class [[nodiscard]] NamespaceListImpl {
	public:
		NamespaceListImpl() {}
		NamespaceListImpl(NsNamesHashSetT&& n) : data(std::move(n)) {}
		bool IsInList(const NamespaceName& ns) const noexcept { return data.empty() || (data.find(ns) != data.end()); }
		bool IsInList(std::string_view ns) const noexcept { return data.empty() || (data.find(ns) != data.end()); }
		bool Empty() const noexcept { return data.empty(); }
		bool operator==(const NamespaceListImpl& r) const noexcept { return data == r.data; }

		const NsNamesHashSetT data;
	};
	using NamespaceList = intrusive_atomic_rc_wrapper<NamespaceListImpl>;

	AsyncReplNodeConfig() = default;
	AsyncReplNodeConfig(DSN dsn) : dsn_(std::move(dsn)) {}

	int GetServerID() const noexcept { return -1; }
	const DSN& GetRPCDsn() const& { return dsn_; }
	auto GetRPCDsn() const&& = delete;
	void SetRPCDsn(const DSN& dsn) { dsn_ = dsn; }
	void FromYAML(const YAML::Node& yaml);
	void FromJSON(const gason::JsonNode& root);
	void GetJSON(JsonBuilder& jb, MaskingDSN) const;
	void GetYAML(YAML::Node& yaml) const;

	bool HasOwnNsList() const noexcept { return hasOwnNsList_; }
	void SetOwnNamespaceList(NsNamesHashSetT nss) {
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
		return (dsn_ == rdata.dsn_) && nsListsAreEqual(rdata) && replicationModesAreEqual(rdata);
	}

private:
	bool nsListsAreEqual(const AsyncReplNodeConfig& rdata) const noexcept {
		return (!hasOwnNsList_ && !rdata.hasOwnNsList_) || (!rdata.namespaces_ && !namespaces_) ||
			   (hasOwnNsList_ && rdata.hasOwnNsList_ && rdata.namespaces_ && namespaces_ && *rdata.namespaces_ == *namespaces_);
	}
	bool replicationModesAreEqual(const AsyncReplNodeConfig& rdata) const noexcept {
		return (!replicationMode_.has_value() && !rdata.replicationMode_.has_value()) || replicationMode_ == rdata.replicationMode_;
	}

	DSN dsn_;
	intrusive_ptr<NamespaceList> namespaces_;
	bool hasOwnNsList_ = false;
	std::optional<AsyncReplicationMode> replicationMode_;
};

constexpr size_t kDefaultClusterProxyConnCount = 8;
constexpr size_t kDefaultClusterProxyCoroPerConn = 4;
constexpr size_t kDefaultClusterProxyConnThreads = 2;

struct [[nodiscard]] ClusterConfigData {
	Error FromYAML(const std::string& yaml);

	bool operator==(const ClusterConfigData& rdata) const noexcept = default;

	bool NodeIndexExistsForServerId(int serverId) const noexcept {
		for (unsigned int i = 0; i < nodes.size(); ++i) {
			if (nodes[i].serverId == serverId) {
				return true;
			}
		}
		return false;
	}

	std::vector<ClusterNodeConfig> nodes;
	NsNamesHashSetT namespaces;
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
	std::string selfReplToken;
};

constexpr uint32_t kDefaultShardingProxyConnCount = 8;
constexpr uint32_t kDefaultShardingProxyCoroPerConn = 8;
constexpr uint32_t kDefaultShardingProxyConnThreads = 4;

struct [[nodiscard]] ShardingConfig {
	static constexpr unsigned serverIdPos = 53;
	static constexpr int64_t serverIdMask = (((1ll << 10) - 1) << serverIdPos);	 // 01111111111000...000
	static constexpr auto kDefaultRollbackTimeout = std::chrono::seconds(30);

	struct [[nodiscard]] Key {
		Error FromYAML(const YAML::Node& yaml, const std::map<int, std::vector<DSN>>& shards, KeyValueType& valuesType,
					   std::vector<sharding::Segment<Variant>>& checkVal);
		Error FromJSON(const gason::JsonNode&, KeyValueType& valuesType, std::vector<sharding::Segment<Variant>>& checkVal);

		void GetYAML(YAML::Node&) const;
		void GetJSON(JsonBuilder&) const;
		int shardId = ShardingKeyType::ProxyOff;
		ShardingAlgorithmType algorithmType = ByValue;

		sharding::Segment<Variant> SegmentFromYAML(const YAML::Node& yaml, int shardId);
		sharding::Segment<Variant> SegmentFromJSON(const gason::JsonNode& json, int shardId);
		ComparationResult RelaxCompare(const std::vector<sharding::Segment<Variant>>&,
									   const CollateOpts& collateOpts = CollateOpts()) const;
		std::vector<sharding::Segment<Variant>> values{};

	private:
		Error checkValue(const sharding::Segment<Variant>& val, KeyValueType& valuesType,
						 const std::vector<sharding::Segment<Variant>>& checkVal);
	};
	struct [[nodiscard]] Namespace {
		Error FromYAML(const YAML::Node& yaml, const std::map<int, std::vector<DSN>>& shards);
		Error FromJSON(const gason::JsonNode&);
		void GetYAML(YAML::Node&) const;
		void GetJSON(JsonBuilder&) const;
		std::string ns;
		std::string index;
		std::vector<Key> keys;
		int defaultShard = ShardingKeyType::ProxyOff;
	};

	Error FromYAML(const std::string& yaml);
	Error FromJSON(std::string_view json);
	Error FromJSON(std::span<char> json);
	Error FromJSON(const gason::JsonNode&);
	std::string GetYAML() const;
	YAML::Node GetYAMLObj() const;

	std::string GetJSON(MaskingDSN) const;
	void GetJSON(WrSerializer&, MaskingDSN) const;
	void GetJSON(JsonBuilder&, MaskingDSN) const;
	Error Validate() const;
	std::vector<Namespace> namespaces;
	std::map<int, std::vector<DSN>> shards;
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

struct [[nodiscard]] AsyncReplConfigData {
	using NamespaceList = AsyncReplNodeConfig::NamespaceList;
	enum class [[nodiscard]] Role { None, Leader, Follower };

	Error FromDefault() noexcept;
	Error FromYAML(const std::string& yml);
	Error FromJSON(std::string_view json);
	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb, MaskingDSN) const;
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
	std::string selfReplToken;

	bool operator==(const AsyncReplConfigData& rdata) const noexcept {
		return (role == rdata.role) && (mode == rdata.mode) && (replThreadsCount == rdata.replThreadsCount) &&
			   (parallelSyncsPerThreadCount == rdata.parallelSyncsPerThreadCount) &&
			   (forceSyncOnLogicError == rdata.forceSyncOnLogicError) && (forceSyncOnWrongDataHash == rdata.forceSyncOnWrongDataHash) &&
			   (retrySyncIntervalMSec == rdata.retrySyncIntervalMSec) && (onlineUpdatesTimeoutSec == rdata.onlineUpdatesTimeoutSec) &&
			   (namespaces == rdata.namespaces || (namespaces && rdata.namespaces && *namespaces == *rdata.namespaces)) &&
			   (enableCompression == rdata.enableCompression) && (appName == rdata.appName) &&
			   (batchingRoutinesCount == rdata.batchingRoutinesCount) && (maxWALDepthOnForceSync == rdata.maxWALDepthOnForceSync) &&
			   (syncTimeoutSec == rdata.syncTimeoutSec) && (onlineUpdatesDelayMSec == rdata.onlineUpdatesDelayMSec) &&
			   (logLevel == rdata.logLevel) && (nodes == rdata.nodes) && (selfReplToken == rdata.selfReplToken);
	}
	bool operator!=(const AsyncReplConfigData& rdata) const noexcept { return !operator==(rdata); }
};

}  // namespace cluster
}  // namespace reindexer
