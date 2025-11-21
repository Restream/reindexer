#include "replication_api.h"
#include "estl/lock.h"
#include "tools/fsops.h"

ReplicationApi::ReplicationApi() : kStoragePath(reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex_repl_test/")) {}

bool ReplicationApi::StopServer(size_t id) {
	reindexer::lock_guard lock(m_);

	assertrx(id < svc_.size());
	if (!svc_[id].Get()) {
		return false;
	}
	svc_[id].Drop();
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(10);
	while (svc_[id].IsRunning()) {
		now += pause;
		EXPECT_TRUE(now < kMaxServerStartTime);
		assertrx(now < kMaxServerStartTime);

		std::this_thread::sleep_for(pause);
	}
	return true;
}

bool ReplicationApi::StartServer(size_t id) {
	reindexer::lock_guard lock(m_);

	assertrx(id < svc_.size());
	if (svc_[id].IsRunning()) {
		return false;
	}
	svc_[id].InitServer(ServerControlConfig(id, kDefaultRpcPort + id, kDefaultHttpPort + id, kStoragePath + "node/" + std::to_string(id),
											"node" + std::to_string(id)));
	return true;
}
void ReplicationApi::RestartServer(size_t id) {
	assertrx(id < svc_.size());

	reindexer::lock_guard lock(m_);
	reindexer::unique_lock restartLock{restartMutex_, std::defer_lock};
	if (id == 0) {
		restartLock.lock();
	}
	if (svc_[id].Get()) {
		svc_[id].Drop();
		size_t counter = 0;
		while (svc_[id].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			ASSERT_LT(counter, 1000);

			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	svc_[id].InitServer(ServerControlConfig(id, kDefaultRpcPort + id, kDefaultHttpPort + id, kStoragePath + "node/" + std::to_string(id),
											"node" + std::to_string(id)));
}

void ReplicationApi::WaitSync(std::string_view ns, reindexer::lsn_t expectedLsn) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(10);
	ReplicationTestState state;
	while (state.lsn.isEmpty()) {
		now += pause;
		ASSERT_LT(now, kMaxSyncTime);
		ReplicationTestState xstate = GetSrv(masterId_)->GetState(ns);	// get an reference state and then compare all with it
		for (size_t i = 0; i < svc_.size(); i++) {
			if (i != masterId_) {
				state = GetSrv(i)->GetState(ns);
				if (xstate.lsn != state.lsn || xstate.nsVersion != state.nsVersion || xstate.tmVersion != state.tmVersion ||
					xstate.tmStatetoken != state.tmStatetoken) {
					state.lsn = reindexer::lsn_t();
					break;
				} else if (!state.lsn.isEmpty()) {
					if (!expectedLsn.isEmpty()) {
						ASSERT_EQ(state.lsn, expectedLsn)
							<< "name: " << ns << ", actual lsn: " << int64_t(state.lsn) << " expected lsn: " << int64_t(expectedLsn)
							<< " i = " << i << " masterId_ = " << masterId_;
					}
					ASSERT_EQ(state.dataHash, xstate.dataHash) << "name: " << ns << ", lsns: " << int64_t(state.lsn) << " "
															   << int64_t(xstate.lsn) << " i = " << i << " masterId_ = " << masterId_;
					ASSERT_EQ(state.dataCount, xstate.dataCount);
				}
			}
		}
		std::this_thread::sleep_for(pause);
	}
}

void ReplicationApi::ForceSync() {
	std::atomic<bool> done{false};
	std::thread awaitForceSync([&done]() {
		auto now = std::chrono::milliseconds(0);
		const auto pause = std::chrono::milliseconds(10);
		while (!done.load()) {
			now += pause;
			ASSERT_TRUE(now < kMaxSyncTime);
			std::this_thread::sleep_for(pause);
		}
	});
	GetSrv(masterId_)->ForceSync();
	done = true;
	awaitForceSync.join();
}

void ReplicationApi::SwitchMaster(size_t id, const AsyncReplicationConfigTest::NsSet& namespaces, std::string replMode) {
	if (id == masterId_) {
		return;
	}
	masterId_ = id;

	using ReplNode = AsyncReplicationConfigTest::Node;
	std::vector<ReplNode> followers;
	followers.reserve(svc_.size() - 1);
	for (size_t i = 0; i < svc_.size(); ++i) {
		if (i != id) {
			auto srv = svc_[i].Get();
			auto conf = srv->GetServerConfig(ServerControl::ConfigType::Namespace);
			conf.nodes.clear();
			conf.role = "follower";
			srv->SetReplicationConfig(conf);
			followers.emplace_back(ReplNode{MakeDsn(UserRole::kRoleReplication, srv)});
		}
	}
	AsyncReplicationConfigTest config("leader", std::move(followers), false, true, id, "node" + std::to_string(masterId_), namespaces,
									  std::move(replMode));
	GetSrv(masterId_)->SetReplicationConfig(config);
}

void ReplicationApi::SetWALSize(size_t id, int64_t size, std::string_view nsName) { GetSrv(id)->SetWALSize(size, nsName); }

size_t ReplicationApi::GetServersCount() const {
	reindexer::lock_guard lock(m_);
	return svc_.size();
}

void ReplicationApi::SetOptmizationSortWorkers(size_t id, size_t cnt, std::string_view nsName) {
	GetSrv(id)->SetOptmizationSortWorkers(cnt, nsName);
}

ServerControl::Interface::Ptr ReplicationApi::GetSrv(size_t id) {
	reindexer::lock_guard lock(m_);
	assertrx(id < svc_.size());
	auto srv = svc_[id].Get();
	assertrx(srv);
	return srv;
}

void ReplicationApi::SetUp() {
	reindexer::lock_guard lock(m_);
	std::ignore = reindexer::fs::RmDirAll(kStoragePath + "node");

	svc_.push_back(ServerControl());
	std::vector<AsyncReplicationConfigTest::Node> followers;
	for (size_t i = 1; i < kDefaultServerCount; ++i) {
		const uint16_t rpcPort = uint16_t(kDefaultRpcPort) + uint16_t(i);
		const auto dbName = "node" + std::to_string(i);
		svc_.push_back(ServerControl());
		svc_.back().InitServer(ServerControlConfig(i, rpcPort, kDefaultHttpPort + i, kStoragePath + "node/" + std::to_string(i), dbName));
		svc_.back().Get()->MakeFollower();
		followers.emplace_back(AsyncReplicationConfigTest::Node{MakeDsn(UserRole::kRoleReplication, svc_.back().Get())});
	}

	svc_.front().InitServer(ServerControlConfig(0, kDefaultRpcPort, kDefaultHttpPort, kStoragePath + "node/0", "node0"));
	svc_.front().Get()->MakeLeader(AsyncReplicationConfigTest("leader", std::move(followers), true, true));
}

void ReplicationApi::TearDown() {
	reindexer::lock_guard lock(m_);
	for (auto& server : svc_) {
		if (server.Get(false)) {
			server.Get(false)->Stop();
		}
	}
	for (auto& server : svc_) {
		if (!server.Get(false)) {
			continue;
		}
		server.Drop();
		auto now = std::chrono::milliseconds(0);
		const auto pause = std::chrono::milliseconds(10);
		while (server.IsRunning()) {
			now += pause;
			ASSERT_TRUE(now < kMaxServerStartTime);
			std::this_thread::sleep_for(pause);
		}
	}
	svc_.clear();
	std::ignore = reindexer::fs::RmDirAll(kStoragePath + "node");
}
