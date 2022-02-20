#pragma once

#include <chrono>
#include "core/keyvalue/variant.h"
#include "estl/fast_hash_set.h"
#include "estl/span.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace Yaml {
class Node;
}

namespace reindexer {

class JsonBuilder;
class WrSerializer;

namespace cluster {

inline uint32_t GetConsensusForN(uint32_t n) noexcept { return n / 2 + 1; }
constexpr auto kStatusCmdTimeout = std::chrono::seconds(3);

struct NodeData {
	int serverId = -1;
	int electionsTerm = 0;
	std::string dsn;

	Error FromJSON(span<char> json);
	Error FromJSON(const gason::JsonNode &v);
	void GetJSON(JsonBuilder &jb) const;
	void GetJSON(WrSerializer &ser) const;
};

struct RaftInfo {
	enum class Role : uint8_t { None, Leader, Follower, Candidate };
	int32_t leaderId = -1;
	Role role = Role::None;

	bool operator==(const RaftInfo &rhs) const noexcept { return role == rhs.role && leaderId == rhs.leaderId; }
	bool operator!=(const RaftInfo &rhs) const noexcept { return !(*this == rhs); }

	Error FromJSON(span<char> json);
	Error FromJSON(const gason::JsonNode &root);
	void GetJSON(JsonBuilder &jb) const;
	void GetJSON(WrSerializer &ser) const;
	static std::string_view RoleToStr(Role);
	static Role RoleFromStr(std::string_view);
};

struct ClusterNodeConfig {
	int GetServerID() const noexcept { return serverId; }
	const std::string &GetRPCDsn() const noexcept { return dsn; }
	const std::string &GetManagementDsn() const noexcept { return dsn; }
	void FromYML(Yaml::Node &yaml);

	bool operator==(const ClusterNodeConfig &rdata) const noexcept { return (serverId == rdata.serverId) && (dsn == rdata.dsn); }

	int serverId = -1;
	std::string dsn;
};

struct AsyncReplConfigData;

class AsyncReplNodeConfig {
public:
	class NamespaceListImpl {
	public:
		NamespaceListImpl() = default;
		NamespaceListImpl(fast_hash_set<string, nocase_hash_str, nocase_equal_str> &&n) : data(std::move(n)) {}
		bool IsInList(std::string_view ns, size_t hash) const noexcept { return data.empty() || (data.find(ns, hash) != data.end()); }
		bool IsInList(std::string_view ns) const noexcept { return data.empty() || (data.find(ns) != data.end()); }
		bool Empty() const noexcept { return data.empty(); }
		bool operator==(const NamespaceListImpl &r) const noexcept { return data == r.data; }

		const fast_hash_set<string, nocase_hash_str, nocase_equal_str> data;
	};
	using NamespaceList = intrusive_atomic_rc_wrapper<NamespaceListImpl>;

	AsyncReplNodeConfig() = default;
	AsyncReplNodeConfig(std::string _dsn) : dsn(std::move(_dsn)) {}

	int GetServerID() const noexcept { return -1; }
	const std::string &GetRPCDsn() const { return dsn; }
	void FromYML(Yaml::Node &yaml);
	void FromJSON(const gason::JsonNode &root);
	void GetJSON(JsonBuilder &jb) const;
	void GetYAML(Yaml::Node &yaml) const;

	bool HasOwnNsList() const noexcept { return hasOwnNsList_; }
	void SetOwnNamespaceList(fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss) {
		namespaces_ = make_intrusive<NamespaceList>(std::move(nss));
		hasOwnNsList_ = true;
	}
	void SetNamespaceListFromConfig(const AsyncReplConfigData &config);
	intrusive_ptr<NamespaceList> Namespaces() const noexcept {
		assert(namespaces_);
		return namespaces_;
	}
	std::vector<std::string> GetNssVector() const {
		std::vector<std::string> nss;
		auto nssPtr = Namespaces();
		const auto &data = nssPtr->data;
		nss.reserve(data.size());
		for (auto &ns : data) {
			nss.emplace_back(ns);
		}
		return nss;
	}

	bool operator==(const AsyncReplNodeConfig &rdata) const noexcept { return (dsn == rdata.dsn) && nsListsAreEqual(rdata); }

	std::string dsn;

private:
	bool nsListsAreEqual(const AsyncReplNodeConfig &rdata) const noexcept {
		return (hasOwnNsList_ == rdata.hasOwnNsList_) || (!rdata.namespaces_ && !namespaces_) ||
			   (rdata.namespaces_ && namespaces_ && *rdata.namespaces_ == *namespaces_);
	}

	intrusive_ptr<NamespaceList> namespaces_;
	bool hasOwnNsList_ = false;
};

struct ClusterConfigData {
	Error FromYML(const std::string &yaml);

	bool operator==(const ClusterConfigData &rdata) const noexcept {
		return (nodes == rdata.nodes) && (retrySyncIntervalMSec == rdata.retrySyncIntervalMSec) &&
			   (onlineUpdatesTimeoutSec == rdata.onlineUpdatesTimeoutSec) && (enableCompression == rdata.enableCompression) &&
			   (appName == rdata.appName) && (replThreadsCount == rdata.replThreadsCount) &&
			   (parallelSyncsPerThreadCount == rdata.parallelSyncsPerThreadCount) &&
			   (batchingRoutinesCount == rdata.batchingRoutinesCount) && (leaderSyncThreads == rdata.leaderSyncThreads) &&
			   (leaderSyncConcurrentSnapshotsPerNode == rdata.leaderSyncConcurrentSnapshotsPerNode) &&
			   (syncTimeoutSec == rdata.syncTimeoutSec);
	}
	bool operator!=(const ClusterConfigData &rdata) const noexcept { return !operator==(rdata); }

	unsigned int GetNodeIndexForServerId(int serverId) const {
		for (unsigned int i = 0; i < nodes.size(); ++i) {
			if (nodes[i].serverId == serverId) return i;
		}
		throw Error(errLogic, "Cluster config. Cannot find node index for ServerId(%d)", serverId);
	}

	std::vector<ClusterNodeConfig> nodes;
	fast_hash_set<string, nocase_hash_str, nocase_equal_str> namespaces;
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
};

constexpr size_t kDefaultShardingProxyConnCount = 6;
constexpr size_t kDefaultShardingProxyCoroPerConn = 16;

struct ShardingConfig {
	struct Key {
		Error FromYML(Yaml::Node &yaml, const std::map<int, std::vector<std::string>> &shards, KeyValueType &valuesType,
					  fast_hash_set<Variant> &checkVal);
		Error FromJson(const gason::JsonNode &, KeyValueType &valuesType, fast_hash_set<Variant> &checkVal);
		void GetYml(std::stringstream &) const;
		void GetJson(JsonBuilder &) const;
		int shardId = ShardingKeyType::ProxyOff;
		ShardingAlgorithmType algorithmType = ByValue;
		VariantArray values;

	private:
		Error checkValue(Variant val, KeyValueType &valuesType, fast_hash_set<Variant> &checkVal);
	};
	struct Namespace {
		Error FromYML(Yaml::Node &yaml, const std::map<int, std::vector<std::string>> &shards);
		Error FromJson(const gason::JsonNode &);
		void GetYml(std::stringstream &) const;
		void GetJson(JsonBuilder &) const;
		std::string ns;
		std::string index;
		std::vector<Key> keys;
		int defaultShard = ShardingKeyType::ProxyOff;
	};

	Error FromYML(const std::string &yaml);
	Error FromJson(span<char> json);
	Error FromJson(const gason::JsonNode &);
	std::string GetYml() const;
	std::string GetJson() const;
	void GetJson(WrSerializer &) const;
	void GetJson(JsonBuilder &) const;
	Error Validate() const;
	std::vector<Namespace> namespaces;
	std::map<int, std::vector<std::string>> shards;
	int thisShardId = ShardingKeyType::ProxyOff;
	std::chrono::milliseconds reconnectTimeout = std::chrono::milliseconds(3000);
	std::chrono::seconds shardsAwaitingTimeout = std::chrono::seconds(30);
	int proxyConnCount = kDefaultShardingProxyConnCount;
	int proxyConnConcurrency = kDefaultShardingProxyCoroPerConn;
};
bool operator==(const ShardingConfig &, const ShardingConfig &);
bool operator==(const ShardingConfig::Key &, const ShardingConfig::Key &);
bool operator==(const ShardingConfig::Namespace &, const ShardingConfig::Namespace &);

struct AsyncReplConfigData {
	using NamespaceList = AsyncReplNodeConfig::NamespaceList;
	enum class Role { None, Leader, Follower };

	Error FromYML(const string &yml);
	Error FromJSON(std::string_view json);
	Error FromJSON(const gason::JsonNode &v);
	void GetJSON(JsonBuilder &jb) const;
	void GetYAML(WrSerializer &ser) const;
	static Role Str2role(std::string_view role) noexcept;
	static std::string Role2str(Role) noexcept;

	std::string appName = "rx_repl_leader";
	Role role = Role::None;
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

	bool operator==(const AsyncReplConfigData &rdata) const noexcept {
		return (replThreadsCount == rdata.replThreadsCount) && (parallelSyncsPerThreadCount == rdata.parallelSyncsPerThreadCount) &&
			   (forceSyncOnLogicError == rdata.forceSyncOnLogicError) && (forceSyncOnWrongDataHash == rdata.forceSyncOnWrongDataHash) &&
			   (retrySyncIntervalMSec == rdata.retrySyncIntervalMSec) && (onlineUpdatesTimeoutSec == rdata.onlineUpdatesTimeoutSec) &&
			   (namespaces == rdata.namespaces || (namespaces && rdata.namespaces && *namespaces == *rdata.namespaces)) &&
			   (enableCompression == rdata.enableCompression) && (appName == rdata.appName) &&
			   (batchingRoutinesCount == rdata.batchingRoutinesCount) && (maxWALDepthOnForceSync == rdata.maxWALDepthOnForceSync) &&
			   (syncTimeoutSec == rdata.syncTimeoutSec) && (nodes == rdata.nodes);
	}
	bool operator!=(const AsyncReplConfigData &rdata) const noexcept { return !operator==(rdata); }
};

}  // namespace cluster
}  // namespace reindexer
