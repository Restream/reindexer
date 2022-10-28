#pragma once

#include <mutex>
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
	void WaitSync(const std::string& ns);
	// force resync
	void ForceSync();
	// Switch master
	void SwitchMaster(size_t id, const ReplicationConfigTest::NsSet& namespaces);
	// Set WAL size
	void SetWALSize(size_t id, int64_t size, std::string_view nsName);
	// Get servers count
	size_t GetServersCount() const;
	// Set optimiation sort workers on ns config
	void SetOptmizationSortWorkers(size_t id, size_t cnt, std::string_view nsName);

	size_t masterId_ = 0;
	shared_timed_mutex restartMutex_;

private:
	std::vector<ServerControl> svc_;
	mutable std::mutex m_;
};
