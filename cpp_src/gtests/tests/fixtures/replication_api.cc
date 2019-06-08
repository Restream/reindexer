#include "replication_api.h"
#include <thread>
#include "tools/fsops.h"

const std::string kStoragePath = "/tmp/reindex_repl_test/";

ServerControl::Interface::~Interface() {
	srv.Stop();
	tr->join();
	stopped_ = true;
}

ServerControl::ServerControl() { stopped_ = new std::atomic_bool(false); }
ServerControl::~ServerControl() {
	WLock lock(mtx_);
	interface = std::shared_ptr<ServerControl::Interface>();
	delete stopped_;
}

ServerControl::ServerControl(ServerControl&& rhs) {
	WLock lock(rhs.mtx_);
	interface = move(rhs.interface);
	stopped_ = rhs.stopped_;
	rhs.stopped_ = nullptr;
}
ServerControl& ServerControl::operator=(ServerControl&& rhs) {
	WLock lock(rhs.mtx_);
	interface = move(rhs.interface);
	stopped_ = rhs.stopped_;
	rhs.stopped_ = nullptr;
	return *this;
}

void ServerControl::Interface::SetNeedDrop(bool dropDb) { dropDb_ = dropDb; }

ServerControl::Interface::Interface(size_t id, std::atomic_bool& stopped, bool dropDb)
	: api(std::make_shared<CppClient>()), id_(id), dropDb_(dropDb), stopped_(stopped) {
	// Init server in thread
	stopped_ = false;
	// clang-format off
    string yaml =
        "storage:\n"
        "    path: " + kStoragePath + "node/" + std::to_string(id_) + "\n"
        "logger:\n"
        "   loglevel: none\n"
        "   rpclog: \n"
        "   serverlog: \n"
        "net:\n"
        "   httpaddr: 0.0.0.0:" + std::to_string(kDefaultHttpPort + id_) + "\n"
        "   rpcaddr: 0.0.0.0:" + std::to_string(kDefaultRpcPort + id_) + "\n";
	// clang-format on

	auto err = srv.InitFromYAML(yaml);
	EXPECT_TRUE(err.ok()) << err.what();

	tr = std::unique_ptr<std::thread>(new std::thread([this]() {
		auto res = this->srv.Start();
		(void)res;
		assert(res == EXIT_SUCCESS);
	}));
	while (!srv.IsReady()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	// init client
	string dsn;
	dsn = "cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + id_) + "/node" + std::to_string(id_);

	err = api.reindexer->Connect(dsn);
	EXPECT_TRUE(err.ok()) << err.what();
}

void ServerControl::Interface::MakeMaster() { setServerConfig("master", id_); }
void ServerControl::Interface::MakeSlave(size_t masterId) { setServerConfig("slave", masterId); }

void ServerControl::Interface::setServerConfig(const string& role, size_t masterId) {
	const char* const configNs = "#config";

	auto item = api.NewItem(configNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	// clang-format off
    string replicationConfig = "{\n"
                       "\"type\":\"replication\",\n"
                       "\"replication\":{\n"
                       "\"role\":\"" + role + "\",\n"
                       "\"master_dsn\":\"cproto://127.0.0.1:" +std::to_string(kDefaultRpcPort+masterId)+"/node" + std::to_string(masterId)+ "\",\n"
                       "\"cluster_id\":2,\n"
                       "\"force_sync_on_logic_error\": false,\n"
                       "\"force_sync_on_wrong_data_hash\": true,\n"
                       "\"namespaces\":[]\n"
                       "}\n"
                       "}\n";
	// clang-format on

	auto err = item.FromJSON(replicationConfig);
	ASSERT_TRUE(err.ok()) << err.what();
	api.Upsert(configNs, item);
	err = api.Commit(configNs);
	ASSERT_TRUE(err.ok()) << err.what();
}

bool ServerControl::IsRunning() { return !stopped_->load(); }

ServerControl::Interface::Ptr ServerControl::Get(bool wait) {
	RLock lock(mtx_);
	if (wait) {
		size_t counter = 0;
		// just simple wait until server restart's
		while (!interface || stopped_->load()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter / 1000 < kMaxServerStartTimeSec);
			assert(counter / 1000 < kMaxServerStartTimeSec);

			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		};
	}
	return interface;
}

void ServerControl::InitServer(size_t id, bool dropDb) {
	WLock lock(mtx_);
	auto srvInterface = std::make_shared<ServerControl::Interface>(id, *stopped_, dropDb);
	interface = srvInterface;
}
void ServerControl::Drop() {
	WLock lock(mtx_);
	interface = std::shared_ptr<ServerControl::Interface>();
}

bool ServerControl::Interface::CheckForSyncCompletion(Ptr) {
	/* Query qr = Query("#memstats").Where("name", CondEq, ns);
	// reindexer::client::QueryResults res;
	 auto err = rts[num].reindexer->Select(qr, res);
	 EXPECT_TRUE(err.ok()) << err.what();
	 for (auto it : res) {
		 auto item = it.GetItem();

		 // std::cout << ser.c_str() << std::endl;
	 }*/
	return false;
}

bool ReplicationApi::StopServer(size_t id, bool dropDb) {
	std::lock_guard<std::mutex> lock(m_);

	assert(id < svc.size());
	if (!svc[id].Get()) return false;
	svc[id].Get()->SetNeedDrop(dropDb);
	svc[id].Drop();
	size_t counter = 0;
	while (svc[id].IsRunning()) {
		counter++;
		// we have only 10sec timeout to restart server!!!!
		EXPECT_TRUE(counter / 100 < kMaxServerStartTimeSec);
		assert(counter / 100 < kMaxServerStartTimeSec);

		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	return true;
}

bool ReplicationApi::StartServer(size_t id, bool dropDb) {
	std::lock_guard<std::mutex> lock(m_);

	assert(id < svc.size());
	if (svc[id].Get()) return false;
	svc[id].InitServer(id, dropDb);
	return true;
}
void ReplicationApi::RestartServer(size_t id, bool dropDb) {
	std::lock_guard<std::mutex> lock(m_);

	assert(id < svc.size());
	if (svc[id].Get()) {
		svc[id].Get()->SetNeedDrop(dropDb);
		svc[id].Drop();
		size_t counter = 0;
		while (svc[id].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assert(counter < 1000);

			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	svc[id].InitServer(id, dropDb);
}
// get server
ServerControl::Interface::Ptr ReplicationApi::GetSrv(size_t id) {
	std::lock_guard<std::mutex> lock(m_);
	assert(id < svc.size());
	auto srv = svc[id].Get();
	assert(srv);
	return srv;
}

void ReplicationApi::SetUp() {
	// reindexer::logInstallWriter([&](int level, char* buf) {
	// 	(void)buf;
	// 	(void)level;
	// 	if (/*strstr(buf, "repl") ||*/ level <= LogError) {
	// 		std::cout << std::this_thread::get_id() << " " << buf << std::endl;
	// 	}
	// });
	std::lock_guard<std::mutex> lock(m_);
	reindexer::fs::RmDirAll(kStoragePath + "node");

	for (size_t i = 0; i < kDefaultServerCount; i++) {
		svc.push_back(ServerControl());
		svc.back().InitServer(i);
		if (i == 0) {
			svc.back().Get()->MakeMaster();
		} else {
			svc.back().Get()->MakeSlave(0);
		}
	}
}

void ReplicationApi::TearDown() {
	std::lock_guard<std::mutex> lock(m_);
	for (auto& server : svc) {
		if (!server.Get()) continue;
		server.Get()->SetNeedDrop(true);
		server.Drop();
		size_t counter = 0;
		while (server.IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter / 100 < kMaxServerStartTimeSec);
			assert(counter / 100 < kMaxServerStartTimeSec);

			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	svc.clear();
}
