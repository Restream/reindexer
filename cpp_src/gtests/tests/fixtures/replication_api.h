#pragma once

#include "estl/shared_mutex.h"
#include "servercontrol.h"

using namespace reindexer_server;

// for now in default serve 0  always master - other slave - inited in ReplicationApi::SetUp
const size_t kDefaultServerCount = 4;
const size_t kDefaultRpcPort = 4444;
const size_t kDefaultHttpPort = 5555;
const auto kMaxServerStartTime = std::chrono::seconds(15);
const auto kMaxSyncTime = std::chrono::seconds(15);
const auto kMaxForceSyncCmdTime = std::chrono::seconds(10);

class [[nodiscard]] ReplicationApi : public ::testing::Test {
public:
	ReplicationApi();

	void SetUp() override;
	void TearDown() override;

	// stop is sync
	bool StopServer(size_t id);
	// start is sync
	bool StartServer(size_t id);
	// restart is sync
	void RestartServer(size_t id);
	// get server
	ServerControl::Interface::Ptr GetSrv(size_t id);
	// wait sync for ns
	void WaitSync(std::string_view ns, reindexer::lsn_t expectedLsn = reindexer::lsn_t());
	// force resync
	void ForceSync();
	// Switch master
	void SwitchMaster(
		size_t id, const AsyncReplicationConfigTest::NsSet& namespaces,
		std::string replMode = reindexer::cluster::AsyncReplConfigData::Mode2str(reindexer::cluster::AsyncReplicationMode::Default));
	// Set WAL size
	void SetWALSize(size_t id, int64_t size, std::string_view nsName);
	// Get servers count
	size_t GetServersCount() const;
	// Set optimiation sort workers on ns config
	void SetOptmizationSortWorkers(size_t id, size_t cnt, std::string_view nsName);

	size_t masterId_ = 0;
	reindexer::shared_timed_mutex restartMutex_;

private:
	const std::string kStoragePath;
	std::vector<ServerControl> svc_;
	mutable reindexer::mutex m_;
};
