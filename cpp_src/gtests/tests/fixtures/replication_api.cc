#include "replication_api.h"

const std::string ReplicationApi::kConfigNs = "#config";

bool ReplicationApi::StopServer(size_t id) {
	std::lock_guard<std::mutex> lock(m_);

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
	std::lock_guard<std::mutex> lock(m_);

	assertrx(id < svc_.size());
	if (svc_[id].IsRunning()) {
		return false;
	}
	svc_[id].InitServer(id, kDefaultRpcPort + id, kDefaultHttpPort + id, kStoragePath + "node/" + std::to_string(id),
						"node" + std::to_string(id), true);
	return true;
}
void ReplicationApi::RestartServer(size_t id) {
	std::lock_guard<std::mutex> lock(m_);

	assertrx(id < svc_.size());
	if (id == 0) {
		restartMutex_.lock();
	}
	if (svc_[id].Get()) {
		svc_[id].Drop();
		size_t counter = 0;
		while (svc_[id].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assertrx(counter < 1000);

			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	svc_[id].InitServer(id, kDefaultRpcPort + id, kDefaultHttpPort + id, kStoragePath + "node/" + std::to_string(id),
						"node" + std::to_string(id), true);
	if (id == 0) {
		restartMutex_.unlock();
	}
}

void ReplicationApi::WaitSync(std::string_view ns) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(10);
	ReplicationTestState state;
	while (state.lsn.isEmpty()) {
		now += pause;
		ASSERT_TRUE(now < kMaxSyncTime);
		ReplicationTestState xstate = GetSrv(masterId_)->GetState(ns);	// get an reference state and then compare all with it
		for (size_t i = 0; i < svc_.size(); i++) {
			if (i != masterId_) {
				state = GetSrv(i)->GetState(ns);
				if (xstate.lsn != state.lsn) {
					state.lsn = reindexer::lsn_t();
					break;
				} else if (!state.lsn.isEmpty()) {
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
	for (size_t i = 0; i < svc_.size(); i++) {
		if (i != masterId_) {
			GetSrv(i)->ForceSync();
		}
	}
	done = true;
	awaitForceSync.join();
}

void ReplicationApi::SwitchMaster(size_t id, const ReplicationConfigTest::NsSet& namespaces) {
	if (id == masterId_) {
		return;
	}
	masterId_ = id;
	ReplicationConfigTest config("master", false, true, id);
	GetSrv(masterId_)->MakeMaster(config);
	for (size_t i = 0; i < svc_.size(); i++) {
		std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + masterId_) + "/node" + std::to_string(masterId_);
		ReplicationConfigTest config("slave", false, true, i, masterDsn, "server_" + std::to_string(i), namespaces);
		if (i != masterId_) {
			GetSrv(i)->MakeSlave(config);
		}
	}
}

void ReplicationApi::SetWALSize(size_t id, int64_t size, std::string_view nsName) { GetSrv(id)->SetWALSize(size, nsName); }

size_t ReplicationApi::GetServersCount() const {
	std::lock_guard<std::mutex> lock(m_);
	return svc_.size();
}

void ReplicationApi::SetOptmizationSortWorkers(size_t id, size_t cnt, std::string_view nsName) {
	GetSrv(id)->SetOptmizationSortWorkers(cnt, nsName);
}

ServerControl::Interface::Ptr ReplicationApi::GetSrv(size_t id) {
	std::lock_guard<std::mutex> lock(m_);
	assertrx(id < svc_.size());
	auto srv = svc_[id].Get();
	assertrx(srv);
	return srv;
}

void ReplicationApi::SetUp() {
	std::lock_guard<std::mutex> lock(m_);
	reindexer::fs::RmDirAll(kStoragePath + "node");

	for (size_t i = 0; i < kDefaultServerCount; i++) {
		svc_.push_back(ServerControl());
		svc_.back().InitServer(i, kDefaultRpcPort + i, kDefaultHttpPort + i, kStoragePath + "node/" + std::to_string(i),
							   "node" + std::to_string(i), true);
		if (i == 0) {
			svc_.back().Get()->MakeMaster();
		} else {
			std::string masterDsn = "cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + 0) + "/node" + std::to_string(0);
			ReplicationConfigTest config("slave", false, true, i, masterDsn);
			svc_.back().Get()->MakeSlave(config);
		}
	}
}

void ReplicationApi::TearDown() {
	std::lock_guard<std::mutex> lock(m_);
	for (auto& server : svc_) {
		if (server.Get()) {
			server.Get()->Stop();
		}
	}
	for (auto& server : svc_) {
		if (!server.Get()) {
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
}
