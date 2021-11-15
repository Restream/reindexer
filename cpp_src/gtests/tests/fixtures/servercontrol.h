#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <unordered_set>
#include "client/synccororeindexer.h"
#include "cluster/stats/replicationstats.h"
#include "core/dbconfig.h"
#include "core/namespace/namespacestat.h"
#include "estl/shared_mutex.h"
#include "reindexertestapi.h"
#include "server/server.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"

struct AsyncReplicationConfigTest {
	using NsSet = std::unordered_set<std::string, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;

	struct Node {
		Node(std::string _dsn, std::optional<NsSet> _nsList = std::optional<NsSet>()) : dsn(std::move(_dsn)), nsList(std::move(_nsList)) {}
		bool operator==(const Node& node) const noexcept { return dsn == node.dsn && nsList == node.nsList; }
		bool operator!=(const Node& node) const noexcept { return !(*this == node); }

		std::string dsn;
		std::optional<NsSet> nsList;
	};

	AsyncReplicationConfigTest(std::string role, std::vector<Node> followers = std::vector<Node>(), std::string appName = std::string())
		: role_(std::move(role)),
		  nodes_(std::move(followers)),
		  forceSyncOnLogicError_(false),
		  forceSyncOnWrongDataHash_(true),
		  appName_(std::move(appName)),
		  serverId_(0) {}
	AsyncReplicationConfigTest(std::string role, std::vector<Node> followers, bool forceSyncOnLogicError, bool forceSyncOnWrongDataHash,
							   int serverId = 0, std::string appName = std::string(), NsSet namespaces = NsSet())
		: role_(std::move(role)),
		  nodes_(std::move(followers)),
		  forceSyncOnLogicError_(forceSyncOnLogicError),
		  forceSyncOnWrongDataHash_(forceSyncOnWrongDataHash),
		  appName_(std::move(appName)),
		  namespaces_(std::move(namespaces)),
		  serverId_(serverId) {}

	bool operator==(const AsyncReplicationConfigTest& config) const {
		return role_ == config.role_ && nodes_ == config.nodes_ && forceSyncOnLogicError_ == config.forceSyncOnLogicError_ &&
			   forceSyncOnWrongDataHash_ == config.forceSyncOnWrongDataHash_ && appName_ == config.appName_ &&
			   namespaces_ == config.namespaces_ && serverId_ == config.serverId_ && syncThreads_ == config.syncThreads_ &&
			   concurrentSyncsPerThread_ == config.concurrentSyncsPerThread_;
	}

	std::string role_;
	std::vector<Node> nodes_;
	bool forceSyncOnLogicError_;
	bool forceSyncOnWrongDataHash_;
	int syncThreads_ = 2;
	int concurrentSyncsPerThread_ = 2;
	std::string appName_;
	NsSet namespaces_;
	int serverId_;
};

struct ReplicationStateApi {
	reindexer::lsn_t lsn;
	reindexer::lsn_t nsVersion;
	uint64_t dataHash = 0;
	size_t dataCount = 0;
};

using BaseApi = ReindexerTestApi<reindexer::client::SyncCoroReindexer>;

void WriteConfigFile(const std::string& path, const std::string& configYaml);

class ServerControl {
public:
	const std::string kConfigNs = "#config";
	const std::string kStoragePath = "/tmp/reindex_repl_test/";
	const unsigned short kDefaultHttpPort = 5555;

	const size_t kMaxServerStartTimeSec = 20;
	enum class ConfigType { File, Namespace };

	ServerControl(ServerControl&& rhs);
	ServerControl& operator=(ServerControl&&);
	ServerControl(ServerControl& rhs) = delete;
	ServerControl& operator=(ServerControl& rhs) = delete;
	ServerControl();
	~ServerControl();
	void Stop();

	struct Interface {
		typedef std::shared_ptr<Interface> Ptr;
		Interface(size_t id, std::atomic_bool& stopped, const std::string& StoragePath, unsigned short httpPort, unsigned short rpcPort,
				  const std::string& dbName, bool enableStats, size_t maxUpdatesSize = 0);
		~Interface();
		// Stop server
		void Stop();

		void MakeLeader(const AsyncReplicationConfigTest& config = AsyncReplicationConfigTest("leader"));
		void MakeFollower();

		void SetReplicationConfig(const AsyncReplicationConfigTest& config);
		void AddFollower(const std::string& dsn,
						 std::optional<std::vector<std::string>>&& nsList = std::optional<std::vector<std::string>>());
		// check with master or slave that sync complete
		ReplicationStateApi GetState(const std::string& ns);
		// Force sync (restart leader's replicator)
		void ForceSync();
		// Reset replication role for the node
		void ResetReplicationRole(const std::string& ns = std::string());
		reindexer::Error TryResetReplicationRole(const std::string& ns);
		// Set cluster leader
		void SetClusterLeader(int lederId);
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
		// get replication stats for specified replication type
		reindexer::cluster::ReplicationStats GetReplicationStats(std::string_view type);

		size_t Id() const noexcept { return id_; }
		std::string GetReplicationConfigFilePath() const {
			return reindexer::fs::JoinPath(reindexer::fs::JoinPath(kStoragePath, dbName_), kReplicationConfigFilename);
		}
		std::string GetAsyncReplicationConfigFilePath() const {
			return reindexer::fs::JoinPath(reindexer::fs::JoinPath(kStoragePath, dbName_), kAsyncReplicationConfigFilename);
		}
		std::string GetClusterConfigFilePath() const {
			return reindexer::fs::JoinPath(reindexer::fs::JoinPath(kStoragePath, dbName_), kClusterConfigFilename);
		}

		reindexer_server::Server srv;
		BaseApi api;

		const std::string kClusterManagementDsn;

	private:
		template <typename ValueT>
		void setNamespaceConfigItem(std::string_view nsName, std::string_view paramName, ValueT&& value);
		template <typename ValueT>
		void upsertConfigItemFromObject(std::string_view type, const ValueT& object);

		size_t id_;
		std::unique_ptr<std::thread> tr;
		std::atomic_bool& stopped_;

	public:
		const std::string kAsyncReplicationConfigFilename = "async_replication.conf";
		const std::string kReplicationConfigFilename = "replication.conf";
		const std::string kClusterConfigFilename = "cluster.conf";
		const std::string kClusterShardingFilename = "sharding.conf";
		const std::string kConfigNs = "#config";
		const std::string kStoragePath;
		const unsigned short kRpcPort;
		const unsigned short kHttpPort;
		const std::string dbName_;
	};
	// Get server - wait means wait until server starts if no server
	Interface::Ptr Get(bool wait = true);
	void InitServer(size_t id, unsigned short rpcPort, unsigned short httpPort, const std::string& storagePath, const std::string& dbName,
					bool enableStats, size_t maxUpdatesSize = 0);
	void Drop();
	bool IsRunning();
	bool DropAndWaitStop();

private:
	typedef reindexer::shared_lock<reindexer::shared_timed_mutex> RLock;
	typedef std::unique_lock<reindexer::shared_timed_mutex> WLock;

	reindexer::shared_timed_mutex mtx_;
	std::shared_ptr<Interface> interface;
	std::atomic_bool* stopped_;
};
