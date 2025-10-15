#pragma once

#include "auth_tools.h"

#include "cluster/stats/replicationstats.h"
#include "core/namespace/namespacenamemap.h"
#include "estl/lock.h"
#include "estl/shared_mutex.h"
#include "reindexertestapi.h"
#include "server/server.h"

#ifdef REINDEXER_WITH_SC_AS_PROCESS
const bool kTestServersInSeparateProcesses = true;
#else
const bool kTestServersInSeparateProcesses = false;
#endif

struct [[nodiscard]] AsyncReplicationConfigTest {
	using NsSet = std::unordered_set<std::string, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;

	struct [[nodiscard]] Node {
		Node(reindexer::DSN _dsn, std::optional<NsSet> _nsList = std::optional<NsSet>());
		bool operator==(const Node& node) const noexcept { return dsn == node.dsn && nsList == node.nsList; }
		bool operator!=(const Node& node) const noexcept { return !(*this == node); }

		void GetJSON(reindexer::JsonBuilder& jb) const;

		reindexer::DSN dsn;
		std::optional<NsSet> nsList;
	};

	AsyncReplicationConfigTest(std::string _role, std::vector<Node> _followers = std::vector<Node>(), std::string _appName = std::string(),
							   std::string _mode = std::string());
	AsyncReplicationConfigTest(std::string _role, std::vector<Node> _followers, bool _forceSyncOnLogicError, bool _forceSyncOnWrongDataHash,
							   int _serverId = 0, std::string _appName = std::string(), NsSet _namespaces = NsSet(),
							   std::string _mode = std::string(), int _onlineUpdatesDelayMSec = 100);

	bool operator==(const AsyncReplicationConfigTest& config) const;
	bool operator!=(const AsyncReplicationConfigTest& config) const { return !(this->operator==(config)); }

	std::string GetJSON() const;
	void GetJSON(reindexer::JsonBuilder& jb) const;

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
	std::string selfReplicationToken;
	reindexer::NsNamesHashMapT<std::string> admissibleTokens;
};

using BaseApi = ReindexerTestApi<reindexer::client::Reindexer>;

void WriteConfigFile(const std::string& path, const std::string& configYaml);

struct [[nodiscard]] ServerControlConfig {
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
	bool enableStats = true;
	size_t maxUpdatesSize = 0;
	bool asServerProcess = kTestServersInSeparateProcesses;
	bool disableNetworkTimeout = false;
};

class [[nodiscard]] ServerControl {
public:
	const unsigned short kDefaultHttpPort = 5555;

	const size_t kMaxServerStartTimeSec = 20;
	enum class [[nodiscard]] ConfigType { File, Namespace };

	static std::string getTestLogPath();

	ServerControl(ServerControl&& rhs) noexcept;
	ServerControl& operator=(ServerControl&&) noexcept;
	ServerControl(ServerControl& rhs) = delete;
	ServerControl& operator=(ServerControl& rhs) = delete;
	ServerControl();
	~ServerControl();
	void Stop();

	struct [[nodiscard]] Interface {
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
		void AddFollower(const ServerControl::Interface::Ptr& follower,
						 std::optional<std::vector<std::string>>&& nsList = std::optional<std::vector<std::string>>(),
						 reindexer::cluster::AsyncReplicationMode replMode = reindexer::cluster::AsyncReplicationMode::Default);
		// check with master or slave that sync complete
		ReplicationTestState GetState(std::string_view ns) { return api.GetReplicationState(ns); }
		// Force sync (restart leader's replicator)
		void ForceSync();
		// Reset replication role for the node
		void ResetReplicationRole(std::string_view ns = std::string_view());
		reindexer::Error TryResetReplicationRole(std::string_view ns);
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
		// write user auth data to file
		void WriteUsersYAMLFile(const std::string& usersYml);
		// set server's WAL size
		void SetWALSize(int64_t size, std::string_view nsName);
		// set optimization sort workers count
		void SetOptmizationSortWorkers(size_t cnt, std::string_view nsName);
		void SetTxAlwaysCopySize(int64_t size, std::string_view nsName);
		void EnableAllProfilings();
		// get replication stats for specified replication type
		reindexer::cluster::ReplicationStats GetReplicationStats(std::string_view type);

		size_t Id() const noexcept { return config_.id; }
		unsigned short RpcPort() const noexcept { return config_.rpcPort; }
		unsigned short HttpPort() const noexcept { return config_.httpPort; }
		std::string DbName() const noexcept { return config_.dbName; }
		std::string GetReplicationConfigFilePath() const;
		std::string GetAsyncReplicationConfigFilePath() const;
		std::string GetClusterConfigFilePath() const;
		std::string GetShardingConfigFilePath() const;
		std::string GetUsersYAMLFilePath() const;

		template <typename T>
		void UpdateConfigReplTokens(const T&);

		reindexer_server::Server srv;
#ifndef _WIN32
		pid_t reindexerServerPID = -1;
		pid_t reindexerServerPIDWait = -1;
#endif
		BaseApi api;

	private:
		template <typename ValueT>
		void setNamespaceConfigItem(std::string_view nsName, std::string_view paramName, ValueT&& value);
		template <typename ValueT>
		void upsertConfigItemFromObject(const ValueT& object);

		std::vector<std::string> getCLIParamArray(bool enableStats, size_t maxUpdatesSize);
		std::string getLogName(const std::string& log, bool core = false);

		std::unique_ptr<std::thread> tr;
		std::atomic_bool& stopped_;

		const ServerControlConfig config_;
		std::string dumpUserRecYAML() const;

		template <typename T>
		T getConfigByType() const;
		Error mergeAdmissibleTokens(reindexer::NsNamesHashMapT<std::string>&) const;
		Error checkSelfToken(const std::string&) const;

	public:
		const reindexer::DSN kRPCDsn;
		constexpr static std::string_view kAsyncReplicationConfigFilename = "async_replication.conf";
		constexpr static std::string_view kStorageTypeFilename = ".reindexer.storage";
		constexpr static std::string_view kReplicationConfigFilename = "replication.conf";
		constexpr static std::string_view kClusterConfigFilename = "cluster.conf";
		constexpr static std::string_view kClusterShardingFilename = "sharding.conf";
		constexpr static std::string_view kUsersYAMLFilename = "users.yml";
	};
	// Get server - wait means wait until server starts if no server
	Interface::Ptr Get(bool wait = true) RX_REQUIRES(!mtx_);
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
	typedef reindexer::unique_lock<reindexer::shared_timed_mutex> WLock;

	reindexer::shared_timed_mutex mtx_;
	std::shared_ptr<Interface> interface;
	std::atomic_bool* stopped_;
};

extern template void ServerControl::Interface::UpdateConfigReplTokens(const std::string&);
extern template void ServerControl::Interface::UpdateConfigReplTokens(const reindexer::NsNamesHashMapT<std::string>&);
