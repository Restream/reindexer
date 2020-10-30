#pragma once

#include <mutex>
#include "client/reindexer.h"
#include "debug/backtrace.h"
#include "estl/fast_hash_set.h"
#include "reindexer_api.h"
#include "reindexertestapi.h"
#include "server/dbmanager.h"
#include "server/server.h"
#include "thread"
#include "tools/logger.h"
#include "tools/serializer.h"

using namespace reindexer_server;
using std::shared_ptr;

// for now in default serve 0  always master - other slave - inited in ReplicationApi::SetUp
const size_t kDefaultServerCount = 4;
const size_t kDefaultRpcPort = 4444;
const size_t kDefaultHttpPort = 5555;
const auto kMaxServerStartTime = std::chrono::seconds(15);
const auto kMaxSyncTime = std::chrono::seconds(15);
const auto kMaxForceSyncCmdTime = std::chrono::seconds(10);

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
	void WaitSync(const std::string &ns);
	// force resync
	void ForceSync();
	// Switch master
	void SwitchMaster(size_t id);
	// Set WAL size
	void SetWALSize(size_t id, int64_t size, string_view nsName);
	//

	size_t masterId_ = 0;
	shared_timed_mutex restartMutex_;

private:
	vector<ServerControl> svc_;
	std::mutex m_;
};
