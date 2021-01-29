#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <unordered_set>
#include "client/reindexer.h"
#include "core/dbconfig.h"
#include "estl/shared_mutex.h"
#include "reindexertestapi.h"
#include "server/server.h"
#include "tools/stringstools.h"

struct ReplicationConfigTest {
	using NsSet = std::unordered_set<std::string, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;

	ReplicationConfigTest(std::string role)
		: role_(std::move(role)), forceSyncOnLogicError_(false), forceSyncOnWrongDataHash_(true), serverId_(0) {}
	ReplicationConfigTest(std::string role, std::string appName)
		: role_(std::move(role)),
		  forceSyncOnLogicError_(false),
		  forceSyncOnWrongDataHash_(true),
		  appName_(std::move(appName)),
		  serverId_(0) {}
	ReplicationConfigTest(std::string role, bool forceSyncOnLogicError, bool forceSyncOnWrongDataHash, int serverId = 0,
						  std::string dsn = std::string(), std::string appName = std::string(), NsSet namespaces = NsSet())
		: role_(std::move(role)),
		  forceSyncOnLogicError_(forceSyncOnLogicError),
		  forceSyncOnWrongDataHash_(forceSyncOnWrongDataHash),
		  dsn_(std::move(dsn)),
		  appName_(std::move(appName)),
		  namespaces_(std::move(namespaces)),
		  serverId_(serverId) {}

	bool operator==(const ReplicationConfigTest& config) const {
		return role_ == config.role_ && forceSyncOnLogicError_ == config.forceSyncOnLogicError_ &&
			   forceSyncOnWrongDataHash_ == config.forceSyncOnWrongDataHash_ && dsn_ == config.dsn_ && appName_ == config.appName_ &&
			   namespaces_ == config.namespaces_ && serverId_ == config.serverId_;
	}

	std::string role_;
	bool forceSyncOnLogicError_;
	bool forceSyncOnWrongDataHash_;
	std::string dsn_;
	std::string appName_;
	NsSet namespaces_;
	int serverId_;
};

struct ReplicationStateApi {
	reindexer::lsn_t lsn;
	reindexer::lsn_t ownLsn;
	uint64_t dataHash;
	size_t dataCount;
	bool slaveMode;
};

using BaseApi = ReindexerTestApi<reindexer::client::Reindexer>;

class ServerControl {
public:
	const std::string kReplicationConfigFilename = "replication.conf";
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

	struct Interface {
		typedef std::shared_ptr<Interface> Ptr;
		Interface(size_t id, std::atomic_bool& stopped, const std::string& ReplicationConfigFilename, const std::string& StoragePath,
				  unsigned short httpPort, unsigned short rpcPort, const std::string& dbName, bool enableStats, size_t maxUpdatesSize = 0);
		~Interface();
		// Stop server
		void Stop();

		// Make this server master
		void MakeMaster(const ReplicationConfigTest& config = ReplicationConfigTest("master"));
		// Make this server slave
		void MakeSlave(size_t masterId, const ReplicationConfigTest& config);
		// check with master or slave that sync complete
		ReplicationStateApi GetState(const std::string& ns);
		// Force sync (restart slave's replicator)
		void ForceSync();
		// get server config from file
		ReplicationConfigTest GetServerConfig(ConfigType type);
		// write server config to file
		void WriteServerConfig(const std::string& configYaml);
		// set server's WAL size
		void SetWALSize(int64_t size, reindexer::string_view nsName);

		reindexer_server::Server srv;
		BaseApi api;

	private:
		void setReplicationConfig(size_t masterId, const ReplicationConfigTest& config);

		size_t id_;
		std::unique_ptr<std::thread> tr;
		std::atomic_bool& stopped_;

		const std::string kReplicationConfigFilename;
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

private:
	typedef reindexer::shared_lock<reindexer::shared_timed_mutex> RLock;
	typedef std::unique_lock<reindexer::shared_timed_mutex> WLock;

	reindexer::shared_timed_mutex mtx_;
	std::shared_ptr<Interface> interface;
	std::atomic_bool* stopped_;
};
