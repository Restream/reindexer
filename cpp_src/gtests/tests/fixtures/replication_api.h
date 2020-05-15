#pragma once

#include <mutex>
#include "client/reindexer.h"
#include "debug/backtrace.h"
#include "estl/fast_hash_set.h"
#include "reindexer_api.h"
#include "server/dbmanager.h"
#include "server/server.h"
#include "thread"
#include "tools/logger.h"
#include "tools/serializer.h"

using namespace reindexer_server;
using std::shared_ptr;

using BaseApi = ReindexerTestApi<reindexer::client::Reindexer>;

// for now in default serve 0  always master - other slave - inited in ReplicationApi::SetUp
const size_t kDefaultServerCount = 4;
const size_t kDefaultRpcPort = 4444;
const size_t kDefaultHttpPort = 5555;
const size_t kMaxServerStartTimeSec = 20;
const size_t kMaxSyncTimeSec = 20;

struct ReplicationConfig {
	using NsSet = std::unordered_set<std::string, nocase_hash_str, nocase_equal_str>;

	ReplicationConfig(std::string role) : role_(std::move(role)), forceSyncOnLogicError_(false), forceSyncOnWrongDataHash_(true) {}
	ReplicationConfig(std::string role, std::string appName)
		: role_(std::move(role)), forceSyncOnLogicError_(false), forceSyncOnWrongDataHash_(true), appName_(std::move(appName)) {}
	ReplicationConfig(std::string role, bool forceSyncOnLogicError, bool forceSyncOnWrongDataHash, std::string dsn = std::string(),
					  std::string appName = std::string(), NsSet namespaces = NsSet())
		: role_(std::move(role)),
		  forceSyncOnLogicError_(forceSyncOnLogicError),
		  forceSyncOnWrongDataHash_(forceSyncOnWrongDataHash),
		  dsn_(std::move(dsn)),
		  appName_(std::move(appName)),
		  namespaces_(std::move(namespaces)) {}

	bool operator==(const ReplicationConfig& config) const {
		return role_ == config.role_ && forceSyncOnLogicError_ == config.forceSyncOnLogicError_ &&
			   forceSyncOnWrongDataHash_ == config.forceSyncOnWrongDataHash_ && dsn_ == config.dsn_ && appName_ == config.appName_ &&
			   namespaces_ == config.namespaces_;
	}

	std::string role_;
	bool forceSyncOnLogicError_;
	bool forceSyncOnWrongDataHash_;
	std::string dsn_;
	std::string appName_;
	NsSet namespaces_;
};

struct ReplicationState {
	int64_t lsn;
	uint64_t dataHash;
	size_t dataCount;
};

class ServerControl {
public:
	enum class ConfigType { File, Namespace };

	ServerControl(ServerControl&& rhs);
	ServerControl& operator=(ServerControl&&);
	ServerControl(ServerControl& rhs) = delete;
	ServerControl& operator=(ServerControl& rhs) = delete;
	ServerControl();
	~ServerControl();

	struct Interface {
		typedef std::shared_ptr<Interface> Ptr;
		Interface(size_t id, std::atomic_bool& stopped);
		~Interface();
		// Stop server
		void Stop();

		// Make this server master
		void MakeMaster(const ReplicationConfig& config = ReplicationConfig("master"));
		// Make this server slave
		void MakeSlave(size_t masterId, const ReplicationConfig& config);
		// check with master or slave that sync complete
		ReplicationState GetState(const std::string& ns);
		// Force sync (restart slave's replicator)
		void ForceSync();
		// get server config from file
		ReplicationConfig GetServerConfig(ConfigType type);
		// write server config to file
		void WriteServerConfig(const std::string& configYaml);

		Server srv;
		BaseApi api;

	private:
		std::string getStotageRoot();
		void setReplicationConfig(size_t masterId, const ReplicationConfig& config);

		size_t id_;
		std::unique_ptr<std::thread> tr;
		std::atomic_bool& stopped_;
	};
	// Get server - wait means wait until server starts if no server
	Interface::Ptr Get(bool wait = true);
	void InitServer(size_t id);
	void Drop();
	bool IsRunning();

private:
	typedef shared_lock<shared_timed_mutex> RLock;
	typedef std::unique_lock<shared_timed_mutex> WLock;

	shared_timed_mutex mtx_;
	std::shared_ptr<Interface> interface;
	std::atomic_bool* stopped_;
};

class ReplicationApi : public ::testing::Test {
public:
	static const std::string kStoragePath;
	static const std::string kReplicationConfigFilename;
	static const std::string kConfigNs;

	void SetUp();
	void TearDown();

	// stop is sync
	bool StopServer(size_t id);
	// start is sync
	bool StartServer(size_t id);
	// restart is sync
	void RestartServer(size_t id);
	// get server
	ServerControl::Interface::Ptr GetSrv(size_t id);
	// wait sync for ns
	void WaitSync(const std::string& ns);
	// force resync
	void ForceSync();
	// Switch master
	void SwitchMaster(size_t id);
	//

	size_t masterId_ = 0;

private:
	vector<ServerControl> svc_;
	std::mutex m_;
};
