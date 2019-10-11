#include "replication_api.h"
#include <fstream>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"

const std::string ReplicationApi::kStoragePath = "/tmp/reindex_repl_test/";
const std::string ReplicationApi::kReplicationConfigFilename = "replication.conf";
const std::string ReplicationApi::kConfigNs = "#config";

ServerControl::Interface::~Interface() {
	srv.Stop();
	tr->join();
	stopped_ = true;
}

void ServerControl::Interface::Stop() { srv.Stop(); }

ServerControl::ServerControl() { stopped_ = new std::atomic_bool(false); }
ServerControl::~ServerControl() {
	WLock lock(mtx_);
	interface.reset();
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

ReplicationConfig ServerControl::Interface::GetServerConfig(ConfigType type) {
	reindexer::ReplicationConfigData config;
	switch (type) {
		case ConfigType::File: {
			std::string replConfYaml;
			int read = fs::ReadFile(getStotageRoot() + "/node" + std::to_string(id_) + "/" + ReplicationApi::kReplicationConfigFilename,
									replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			auto err = config.FromYML(replConfYaml);
			EXPECT_TRUE(err.ok()) << err.what();
			break;
		}
		case ConfigType::Namespace: {
			reindexer::client::QueryResults results;
			auto err = api.reindexer->Select(Query(ReplicationApi::kConfigNs), results);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_TRUE(results.Status().ok()) << results.Status().what();
			for (auto it : results) {
				WrSerializer ser;
				err = it.GetJSON(ser, false);
				EXPECT_TRUE(err.ok()) << err.what();
				try {
					gason::JsonParser parser;
					gason::JsonNode configJson = parser.Parse(ser.Slice());
					if (configJson["type"].As<std::string>() != "replication") {
						continue;
					}
					auto& replConfig = configJson["replication"];
					auto err = config.FromJSON(replConfig);
					EXPECT_TRUE(err.ok()) << err.what();
					break;
				} catch (const Error&) {
					assert(false);
				}
			}
			break;
		}
		default:
			break;
	}

	EXPECT_TRUE(config.role == ReplicationMaster || config.role == ReplicationSlave);
	ReplicationConfig::NsSet namespaces;
	for (auto& ns : config.namespaces) {
		namespaces.insert(ns);
	}
	return ReplicationConfig(config.role == ReplicationMaster ? "master" : "slave", config.forceSyncOnLogicError,
							 config.forceSyncOnWrongDataHash, std::move(config.masterDSN), std::move(namespaces));
}

void ServerControl::Interface::WriteServerConfig(const std::string& configYaml) {
	std::ofstream file(getStotageRoot() + "/node" + std::to_string(id_) + "/" + ReplicationApi::kReplicationConfigFilename,
					   std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

std::string ServerControl::Interface::getStotageRoot() { return ReplicationApi::kStoragePath + "node/" + std::to_string(id_); }

ServerControl::Interface::Interface(size_t id, std::atomic_bool& stopped) : id_(id), stopped_(stopped) {
	// Init server in thread
	stopped_ = false;
	// clang-format off
    string yaml =
        "storage:\n"
        "    path: " + getStotageRoot() + "\n"
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
	string dsn = "cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + id_) + "/node" + std::to_string(id_);

	err = api.reindexer->Connect(dsn);
	EXPECT_TRUE(err.ok()) << err.what();
}

void ServerControl::Interface::MakeMaster(const ReplicationConfig& config) {
	assert(config.role_ == "master");
	setReplicationConfig(id_, config);
}
void ServerControl::Interface::MakeSlave(size_t masterId, const ReplicationConfig& config) {
	assert(config.role_ == "slave");
	setReplicationConfig(masterId, config);
}

void ServerControl::Interface::setReplicationConfig(size_t masterId, const ReplicationConfig& config) {
	auto item = api.NewItem(ReplicationApi::kConfigNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	WrSerializer ser;
	JsonBuilder jb(ser);
	jb.Put("type", "replication");
	auto replConf = jb.Object("replication");
	replConf.Put("role", config.role_);
	replConf.Put("master_dsn", config.dsn_.empty() ? ("cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + masterId) + "/node" +
													  std::to_string(masterId))
												   : config.dsn_);
	replConf.Put("cluster_id", 2);
	replConf.Put("force_sync_on_logic_error", config.forceSyncOnLogicError_);
	replConf.Put("force_sync_on_wrong_data_hash", config.forceSyncOnWrongDataHash_);

	auto nsArray = replConf.Array("namespaces");
	for (auto& ns : config.namespaces_) nsArray.Put(nullptr, ns);
	nsArray.End();
	replConf.End();
	jb.End();

	auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	api.Upsert(ReplicationApi::kConfigNs, item);
	err = api.Commit(ReplicationApi::kConfigNs);
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

void ServerControl::InitServer(size_t id) {
	WLock lock(mtx_);
	auto srvInterface = std::make_shared<ServerControl::Interface>(id, *stopped_);
	interface = srvInterface;
}
void ServerControl::Drop() {
	WLock lock(mtx_);
	interface.reset();
}

ReplicationState ServerControl::Interface::GetState(const std::string& ns) {
	Query qr = Query("#memstats").Where("name", CondEq, ns);
	reindexer::client::QueryResults res;
	auto err = api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	ReplicationState state{-1, 0, 0};
	for (auto it : res) {
		WrSerializer ser;
		err = it.GetJSON(ser, false);
		EXPECT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		auto root = parser.Parse(ser.Slice());
		state.lsn = root["replication"]["last_lsn"].As<int64_t>();
		state.dataCount = root["replication"]["data_count"].As<int64_t>();
		state.dataHash = root["replication"]["data_hash"].As<uint64_t>();

		// std::cout << state.lsn << " " << state.dataCount << " " << state.dataHash /*<< " " << ser.c_str()*/ << std::endl;
	}
	return state;
}

void ServerControl::Interface::ForceSync() {
	reindexer::client::QueryResults res;
	Error err;
	auto item = api.NewItem("#config");
	EXPECT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(R"json({"type":"action","action":{"command":"restart_replication"}})json");
	EXPECT_TRUE(err.ok()) << err.what();
	api.Upsert("#config", item);
}

bool ReplicationApi::StopServer(size_t id) {
	std::lock_guard<std::mutex> lock(m_);

	assert(id < svc_.size());
	if (!svc_[id].Get()) return false;
	svc_[id].Drop();
	size_t counter = 0;
	while (svc_[id].IsRunning()) {
		counter++;
		// we have only 10sec timeout to restart server!!!!
		EXPECT_TRUE(counter / 100 < kMaxServerStartTimeSec);
		assert(counter / 100 < kMaxServerStartTimeSec);

		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	return true;
}

bool ReplicationApi::StartServer(size_t id) {
	std::lock_guard<std::mutex> lock(m_);

	assert(id < svc_.size());
	if (svc_[id].IsRunning()) return false;
	svc_[id].InitServer(id);
	return true;
}
void ReplicationApi::RestartServer(size_t id) {
	std::lock_guard<std::mutex> lock(m_);

	assert(id < svc_.size());
	if (svc_[id].Get()) {
		svc_[id].Drop();
		size_t counter = 0;
		while (svc_[id].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assert(counter < 1000);

			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	svc_[id].InitServer(id);
}

void ReplicationApi::WaitSync(const std::string& ns) {
	size_t counter = 0;
	// we have only 10sec timeout to restart server!!!!

	ReplicationState state{-1, 0, 0};
	while (state.lsn == -1) {
		counter++;
		EXPECT_TRUE(counter / 100 < kMaxSyncTimeSec);
		for (size_t i = 0; i < svc_.size(); i++) {
			ReplicationState xstate = GetSrv((i + masterId_) % svc_.size())->GetState(ns);
			if (i == 0)
				state = xstate;
			else if (xstate.lsn != state.lsn)
				state.lsn = -1;
			else if (state.lsn != -1) {
				ASSERT_EQ(state.dataHash, xstate.dataHash) << "name: " << ns << ", lsns: " << state.lsn << " " << xstate.lsn;
				ASSERT_EQ(state.dataCount, xstate.dataCount);
			}
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

void ReplicationApi::ForceSync() {
	for (size_t i = 0; i < svc_.size(); i++) {
		if (i != masterId_) GetSrv(i)->ForceSync();
	}
}
void ReplicationApi::SwitchMaster(size_t id) {
	if (id == masterId_) return;
	masterId_ = id;
	GetSrv(masterId_)->MakeMaster();
	for (size_t i = 0; i < svc_.size(); i++) {
		if (i != masterId_) GetSrv(i)->MakeSlave(masterId_);
	}
}

ServerControl::Interface::Ptr ReplicationApi::GetSrv(size_t id) {
	std::lock_guard<std::mutex> lock(m_);
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	assert(srv);
	return srv;
}

void ReplicationApi::SetUp() {
	// reindexer::logInstallWriter([&](int level, char* buf) {
	// 	(void)buf;
	// 	(void)level;
	// 	if (strstr(buf, "repl") || level <= LogError) {
	// 		std::cout << std::this_thread::get_id() << " " << buf << std::endl;
	// 	}
	// });
	std::lock_guard<std::mutex> lock(m_);
	reindexer::fs::RmDirAll(kStoragePath + "node");

	for (size_t i = 0; i < kDefaultServerCount; i++) {
		svc_.push_back(ServerControl());
		svc_.back().InitServer(i);
		if (i == 0) {
			svc_.back().Get()->MakeMaster();
		} else {
			svc_.back().Get()->MakeSlave(0);
		}
	}
}

void ReplicationApi::TearDown() {
	std::lock_guard<std::mutex> lock(m_);
	for (auto& server : svc_) {
		if (server.Get()) server.Get()->Stop();
	}
	for (auto& server : svc_) {
		if (!server.Get()) continue;
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
	svc_.clear();
}
