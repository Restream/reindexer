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

typedef reindexer::client::Reindexer CppClient;
typedef ReindexerTestApi<shared_ptr<CppClient>, reindexer::client::Item> BaseApi;

// for now in default serve 0  always master - other slave - inited in ReplicationApi::SetUp
const size_t kDefaultServerCount = 4;
const size_t kDefaultRpcPort = 4444;
const size_t kDefaultHttpPort = 5555;
const size_t kMaxServerStartTimeSec = 20;

class ServerConfig {
public:
	using NsSet = std::unordered_set<std::string, nocase_hash_str, nocase_equal_str>;

	ServerConfig(std::string role) : role_(std::move(role)), forceSyncOnLogicError_(false), forceSyncOnWrongDataHash_(true) {}
	ServerConfig(std::string role, bool forceSyncOnLogicError, bool forceSyncOnWrongDataHash, std::string dsn = std::string(),
				 NsSet namespaces = NsSet())
		: role_(std::move(role)),
		  forceSyncOnLogicError_(forceSyncOnLogicError),
		  forceSyncOnWrongDataHash_(forceSyncOnWrongDataHash),
		  dsn_(std::move(dsn)),
		  namespaces_(std::move(namespaces)) {}

	bool operator==(const ServerConfig& config) const {
		return role_ == config.role_ && forceSyncOnLogicError_ == config.forceSyncOnLogicError_ &&
			   forceSyncOnWrongDataHash_ == config.forceSyncOnWrongDataHash_ && dsn_ == config.dsn_ && namespaces_ == config.namespaces_;
	}

	std::string role_;
	bool forceSyncOnLogicError_;
	bool forceSyncOnWrongDataHash_;
	std::string dsn_;
	NsSet namespaces_;
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
		Interface(size_t id, std::atomic_bool& stopped, bool dropDb = false);
		~Interface();

		// Make this server master
		void MakeMaster(const ServerConfig& config = ServerConfig("master"));
		// Make this server slave
		void MakeSlave(size_t masterId, const ServerConfig& config = ServerConfig("slave"));
		// check with master or slave that sync complete
		bool CheckForSyncCompletion(Ptr rhs);
		// drop db from hdd
		void SetNeedDrop(bool dropDb);
		// get server config from file
		ServerConfig GetServerConfig(ConfigType type);
		// write server config to file
		void WriteServerConfig(const std::string& configYaml);
		//

		Server srv;
		BaseApi api;

	private:
		std::string getStotageRoot();
		void setServerConfig(size_t masterId, const ServerConfig& config);

		size_t id_;
		bool dropDb_;
		std::unique_ptr<std::thread> tr;
		std::atomic_bool& stopped_;
	};
	// Get server - wait means wait until server starts if no server
	Interface::Ptr Get(bool wait = true);
	void InitServer(size_t id, bool dropDb = false);
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
	bool StopServer(size_t id, bool dropDb = false);
	// start is sync
	bool StartServer(size_t id, bool dropDb = false);
	// restart is sync
	void RestartServer(size_t id, bool dropDb = false);
	// get server
	ServerControl::Interface::Ptr GetSrv(size_t id);

private:
	vector<ServerControl> svc;
	std::mutex m_;
};
