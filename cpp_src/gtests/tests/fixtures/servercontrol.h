#pragma once

#include "client/reindexer.h"
#include "cluster/stats/replicationstats.h"
#include "core/cjson/jsonbuilder.h"
#include "core/namespace/namespacestat.h"
#include "estl/shared_mutex.h"
#include "reindexertestapi.h"
#include "server/server.h"
#include "tools/fsops.h"

#ifdef REINDEXER_WITH_SC_AS_PROCESS
const bool kTestServersInSeparateProcesses = true;
#else
const bool kTestServersInSeparateProcesses = false;
#endif

struct AsyncReplicationConfigTest {
	using NsSet = std::unordered_set<std::string, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;

	struct Node {
		Node(std::string _dsn, std::optional<NsSet> _nsList = std::optional<NsSet>()) : dsn(std::move(_dsn)), nsList(std::move(_nsList)) {}
		bool operator==(const Node& node) const noexcept { return dsn == node.dsn && nsList == node.nsList; }
		bool operator!=(const Node& node) const noexcept { return !(*this == node); }

		void GetJSON(reindexer::JsonBuilder& jb) const {
			jb.Put("dsn", dsn);
			auto arrNode = jb.Array("namespaces");
			if (nsList) {
				for (const auto& ns : *nsList) arrNode.Put(nullptr, ns);
			}
		}

		std::string dsn;
		std::optional<NsSet> nsList;
	};

	AsyncReplicationConfigTest(std::string _role, std::vector<Node> _followers = std::vector<Node>(), std::string _appName = std::string(),
							   std::string _mode = std::string())
		: role(std::move(_role)),
		  mode(std::move(_mode)),
		  nodes(std::move(_followers)),
		  forceSyncOnLogicError(false),
		  forceSyncOnWrongDataHash(true),
		  appName(std::move(_appName)),
		  serverId(0) {}
	AsyncReplicationConfigTest(std::string _role, std::vector<Node> _followers, bool _forceSyncOnLogicError, bool _forceSyncOnWrongDataHash,
							   int _serverId = 0, std::string _appName = std::string(), NsSet _namespaces = NsSet(),
							   std::string _mode = std::string(), int _onlineUpdatesDelayMSec = 100)
		: role(std::move(_role)),
		  mode(std::move(_mode)),
		  nodes(std::move(_followers)),
		  forceSyncOnLogicError(_forceSyncOnLogicError),
		  forceSyncOnWrongDataHash(_forceSyncOnWrongDataHash),
		  appName(std::move(_appName)),
		  namespaces(std::move(_namespaces)),
		  serverId(_serverId),
		  onlineUpdatesDelayMSec(_onlineUpdatesDelayMSec) {}

	bool operator==(const AsyncReplicationConfigTest& config) const {
		return role == config.role && mode == config.mode && nodes == config.nodes &&
			   forceSyncOnLogicError == config.forceSyncOnLogicError && forceSyncOnWrongDataHash == config.forceSyncOnWrongDataHash &&
			   appName == config.appName && namespaces == config.namespaces && serverId == config.serverId &&
			   syncThreads == config.syncThreads && concurrentSyncsPerThread == config.concurrentSyncsPerThread &&
			   onlineUpdatesDelayMSec == config.onlineUpdatesDelayMSec;
	}
	bool operator!=(const AsyncReplicationConfigTest& config) const { return !(this->operator==(config)); }

	std::string GetJSON() const {
		reindexer::WrSerializer wser;
		reindexer::JsonBuilder jb(wser);
		GetJSON(jb);
		return std::string(wser.Slice());
	}
	void GetJSON(reindexer::JsonBuilder& jb) const {
		jb.Put("app_name", appName);
		jb.Put("server_id", serverId);
		jb.Put("role", role);
		jb.Put("mode", mode);
		jb.Put("force_sync_on_logic_error", forceSyncOnLogicError);
		jb.Put("force_sync_on_wrong_data_hash", forceSyncOnWrongDataHash);
		jb.Put("sync_threads", syncThreads);
		jb.Put("syncs_per_thread", concurrentSyncsPerThread);
		jb.Put("online_updates_delay_msec", onlineUpdatesDelayMSec);
		{
			auto arrNode = jb.Array("namespaces");
			for (const auto& ns : namespaces) arrNode.Put(nullptr, ns);
		}
		{
			auto arrNode = jb.Array("nodes");
			for (const auto& node : nodes) {
				auto obj = arrNode.Object();
				node.GetJSON(obj);
			}
		}
	}

	std::string role;
	std::string mode;
	std::vector<Node> nodes;
	bool forceSyncOnLogicError;
	bool forceSyncOnWrongDataHash;
	int syncThreads = 2;
	int concurrentSyncsPerThread = 2;
	std::string appName;
	NsSet namespaces;
	int serverId;
	int onlineUpdatesDelayMSec = 100;

	friend std::ostream& operator<<(std::ostream& os, const AsyncReplicationConfigTest& cfg) {
		reindexer::WrSerializer ser;
		{
			reindexer::JsonBuilder jb(ser);
			cfg.GetJSON(jb);
		}
		return os << ser.Slice();
	}
};

struct ReplicationStateApi {
	reindexer::lsn_t lsn;
	reindexer::lsn_t nsVersion;
	uint64_t dataHash = 0;
	size_t dataCount = 0;
	std::optional<int> tmVersion;
	std::optional<int> tmStatetoken;
	reindexer::ClusterizationStatus::Role role = reindexer::ClusterizationStatus::Role::None;
};

using BaseApi = ReindexerTestApi<reindexer::client::Reindexer>;

void WriteConfigFile(const std::string& path, const std::string& configYaml);

struct ServerControlConfig {
	ServerControlConfig(size_t _id, unsigned short _rpcPort, unsigned short _httpPort, std::string _storagePath, std::string _dbName,
						bool _enableStats = true, size_t _maxUpdatesSize = 0, bool _asServerProcess = kTestServersInSeparateProcesses)
		: id(_id),
		  storagePath(std::move(_storagePath)),
		  httpPort(_httpPort),
		  rpcPort(_rpcPort),
		  dbName(std::move(_dbName)),
		  enableStats(_enableStats),
		  maxUpdatesSize(_maxUpdatesSize),
		  asServerProcess(_asServerProcess) {}
	size_t id;
	std::string storagePath;
	unsigned short httpPort;
	unsigned short rpcPort;
	std::string dbName;
	bool enableStats = false;
	size_t maxUpdatesSize = 0;
	bool asServerProcess = kTestServersInSeparateProcesses;
	bool disableNetworkTimeout = false;
};

class ServerControl {
public:
	const std::string kConfigNs = "#config";
	const std::string kStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex_repl_test");
	const unsigned short kDefaultHttpPort = 5555;

	const size_t kMaxServerStartTimeSec = 20;
	enum class ConfigType { File, Namespace };

	static std::string getTestLogPath() {
		const char* testSetName = ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name();
		const char* testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
		std::string name;
		name = name + "logs/" + testSetName + "/" + testName + "/";
		return name;
	}

	ServerControl(ServerControl&& rhs) noexcept;
	ServerControl& operator=(ServerControl&&) noexcept;
	ServerControl(ServerControl& rhs) = delete;
	ServerControl& operator=(ServerControl& rhs) = delete;
	ServerControl();
	~ServerControl();
	void Stop();

	struct Interface {
		typedef std::shared_ptr<Interface> Ptr;
		Interface(std::atomic_bool& stopped, ServerControlConfig config);
		Interface(std::atomic_bool& stopped, ServerControlConfig config, const YAML::Node& ReplicationConfig,
				  const YAML::Node& ClusterConfig, const YAML::Node& ShardingConfig, const YAML::Node& AsyncReplicationConfig);
		~Interface();
		void Init();
		// Stop server
		void Stop();

		void MakeLeader(const AsyncReplicationConfigTest& config = AsyncReplicationConfigTest("leader"));
		void MakeFollower();

		void SetReplicationConfig(const AsyncReplicationConfigTest& config);
		void AddFollower(const std::string& dsn,
						 std::optional<std::vector<std::string>>&& nsList = std::optional<std::vector<std::string>>(),
						 reindexer::cluster::AsyncReplicationMode replMode = reindexer::cluster::AsyncReplicationMode::Default);
		// check with master or slave that sync complete
		ReplicationStateApi GetState(const std::string& ns);
		// Force sync (restart leader's replicator)
		void ForceSync();
		// Reset replication role for the node
		void ResetReplicationRole(const std::string& ns = std::string());
		reindexer::Error TryResetReplicationRole(const std::string& ns);
		// Set cluster leader
		void SetClusterLeader(int leaderId);
		void SetReplicationLogLevel(LogLevel, std::string_view type);
		BaseApi::ItemType CreateClusterChangeLeaderItem(int leaderId);
		// get server config from file
		AsyncReplicationConfigTest GetServerConfig(ConfigType type);
		// write general replication config file
		void WriteReplicationConfig(const std::string& configYaml);
		// write async replication config to file
		void WriteAsyncReplicationConfig(const std::string& configYaml);
		// write cluster config to file
		void WriteClusterConfig(const std::string& configYaml);
		// write sharding config to file
		void WriteShardingConfig(const std::string& configYaml);
		// set server's WAL size
		void SetWALSize(int64_t size, std::string_view nsName);
		// set optimization sort workers count
		void SetOptmizationSortWorkers(size_t cnt, std::string_view nsName);
		void SetTxAlwaysCopySize(int64_t size, std::string_view nsName);
		void EnableAllProfilings();
		// get replication stats for specified replication type
		reindexer::cluster::ReplicationStats GetReplicationStats(std::string_view type);

		size_t Id() const noexcept { return config_.id; }
		unsigned short RpcPort() { return config_.rpcPort; }
		unsigned short HttpPort() { return config_.httpPort; }
		std::string GetReplicationConfigFilePath() const {
			return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kReplicationConfigFilename);
		}
		std::string GetAsyncReplicationConfigFilePath() const {
			return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kAsyncReplicationConfigFilename);
		}
		std::string GetClusterConfigFilePath() const {
			return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kClusterConfigFilename);
		}
		std::string GetShardingConfigFilePath() const {
			return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kClusterShardingFilename);
		}

		reindexer_server::Server srv;
#ifndef _WIN32
		pid_t reindexerServerPID = -1;
		pid_t reindexerServerPIDWait = -1;
#endif
		BaseApi api;

		const std::string kRPCDsn;

	private:
		template <typename ValueT>
		void setNamespaceConfigItem(std::string_view nsName, std::string_view paramName, ValueT&& value);
		template <typename ValueT>
		void upsertConfigItemFromObject(std::string_view type, const ValueT& object);

		std::vector<std::string> getCLIParamArray(bool enableStats, size_t maxUpdatesSize);
		std::string getLogName(const std::string& log, bool core = false);

		std::unique_ptr<std::thread> tr;
		std::atomic_bool& stopped_;

		const ServerControlConfig config_;

	public:
		const std::string kAsyncReplicationConfigFilename = "async_replication.conf";
		const std::string kStorageTypeFilename = ".reindexer.storage";
		const std::string kReplicationConfigFilename = "replication.conf";
		const std::string kClusterConfigFilename = "cluster.conf";
		const std::string kClusterShardingFilename = "sharding.conf";
		const std::string kConfigNs = "#config";
	};
	// Get server - wait means wait until server starts if no server
	Interface::Ptr Get(bool wait = true);
	void InitServer(ServerControlConfig config);
	void InitServerWithConfig(ServerControlConfig config, const YAML::Node& ReplicationConfig, const YAML::Node& ClusterConfig,
							  const YAML::Node& ShardingConfig, const YAML::Node& AsyncReplicationConfig);
	void Drop();
	bool IsRunning();
	bool DropAndWaitStop();
	static void WaitSync(const Interface::Ptr& s1, const Interface::Ptr& s2, const std::string& nsName);

	static constexpr std::chrono::seconds kMaxSyncTime = std::chrono::seconds(15);

private:
	typedef reindexer::shared_lock<reindexer::shared_timed_mutex> RLock;
	typedef std::unique_lock<reindexer::shared_timed_mutex> WLock;

	reindexer::shared_timed_mutex mtx_;
	std::shared_ptr<Interface> interface;
	std::atomic_bool* stopped_;
};
