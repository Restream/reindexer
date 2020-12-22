#include "servercontrol.h"
#include <fstream>
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"

using namespace reindexer;

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

ReplicationConfigTest ServerControl::Interface::GetServerConfig(ConfigType type) {
	reindexer::ReplicationConfigData config;
	switch (type) {
		case ConfigType::File: {
			std::string replConfYaml;
			int read = fs::ReadFile(kStoragePath + "/" + dbName_ + "/" + kReplicationConfigFilename, replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			auto err = config.FromYML(replConfYaml);
			EXPECT_TRUE(err.ok()) << err.what();
			break;
		}
		case ConfigType::Namespace: {
			reindexer::client::QueryResults results;
			auto err = api.reindexer->Select(Query(kConfigNs), results);
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
	ReplicationConfigTest::NsSet namespaces;
	for (auto& ns : config.namespaces) {
		namespaces.insert(ns);
	}
	return ReplicationConfigTest(config.role == ReplicationMaster ? "master" : "slave", config.forceSyncOnLogicError,
								 config.forceSyncOnWrongDataHash, config.serverId, std::move(config.masterDSN), std::move(config.appName),
								 std::move(namespaces));
}

void ServerControl::Interface::WriteServerConfig(const std::string& configYaml) {
	std::ofstream file(kStoragePath + "/" + dbName_ + "/" + kReplicationConfigFilename, std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

void ServerControl::Interface::SetWALSize(int64_t size, string_view nsName) {
	reindexer::WrSerializer ser;
	reindexer::JsonBuilder jb(ser);

	jb.Put("type", "namespaces");
	auto nsArray = jb.Array("namespaces");
	auto ns = nsArray.Object();
	ns.Put("namespace", nsName);
	ns.Put("wal_size", size);
	ns.End();
	nsArray.End();
	jb.End();

	auto item = api.NewItem(kConfigNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();

	api.Upsert(kConfigNs, item);
	err = api.Commit(kConfigNs);
	ASSERT_TRUE(err.ok()) << err.what();
}

ServerControl::Interface::Interface(size_t id, std::atomic_bool& stopped, const std::string& ReplicationConfigFilename,
									const std::string& StoragePath, unsigned short httpPort, unsigned short rpcPort,
									const std::string& dbName, bool enableStats)
	: id_(id),
	  stopped_(stopped),
	  kReplicationConfigFilename(ReplicationConfigFilename),
	  kStoragePath(StoragePath),
	  kRpcPort(rpcPort),
	  kHttpPort(httpPort),
	  dbName_(dbName) {
	// Init server in thread
	stopped_ = false;
	string testSetName = ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name();
	auto getLogName = [&testSetName, &id](const string& log, bool core = false) {
		string name("logs/" + testSetName + "/" + log + "_");
		if (!core) name += std::to_string(id);
		name += ".log";
		return name;
	};
	// clang-format off
    string yaml =
        "storage:\n"
		"    path: " + kStoragePath + "\n"
		"metrics:\n"
		"   clientsstats: " + (enableStats ? "true" : "false") + "\n"
        "logger:\n"
        "   loglevel: trace\n"
        "   rpclog: " + getLogName("rpc") + "\n"
        "   serverlog: " + getLogName("server") + "\n"
        "   corelog: " + getLogName("core", true) + "\n"
        "net:\n"
        "   httpaddr: 0.0.0.0:" + std::to_string(kHttpPort) + "\n"
        "   rpcaddr: 0.0.0.0:" + std::to_string(kRpcPort);
	// clang-format on

	auto err = srv.InitFromYAML(yaml);
	EXPECT_TRUE(err.ok()) << err.what();

	tr = std::unique_ptr<std::thread>(new std::thread([this]() {
		auto res = this->srv.Start();
		if (res != EXIT_SUCCESS) {
			std::cerr << "Exit code: " << res << std::endl;
		}
		assert(res == EXIT_SUCCESS);
	}));
	while (!srv.IsRunning() || !srv.IsReady()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	// init client
	string dsn = "cproto://127.0.0.1:" + std::to_string(kRpcPort) + "/" + dbName_;
	err = api.reindexer->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
	EXPECT_TRUE(err.ok()) << err.what();
	while (!api.reindexer->Status().ok()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

void ServerControl::Interface::MakeMaster(const ReplicationConfigTest& config) {
	assert(config.role_ == "master");
	setReplicationConfig(id_, config);
}
void ServerControl::Interface::MakeSlave(size_t masterId, const ReplicationConfigTest& config) {
	assert(config.role_ == "slave");
	assert(!config.dsn_.empty());
	setReplicationConfig(masterId, config);
}

void ServerControl::Interface::setReplicationConfig(size_t masterId, const ReplicationConfigTest& config) {
	(void)masterId;
	auto item = api.NewItem(kConfigNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	WrSerializer ser;
	JsonBuilder jb(ser);
	jb.Put("type", "replication");
	auto replConf = jb.Object("replication");
	replConf.Put("role", config.role_);
	replConf.Put("master_dsn", config.dsn_);
	replConf.Put("app_name", config.appName_);
	replConf.Put("cluster_id", 2);
	replConf.Put("force_sync_on_logic_error", config.forceSyncOnLogicError_);
	replConf.Put("force_sync_on_wrong_data_hash", config.forceSyncOnWrongDataHash_);
	replConf.Put("server_id", config.serverId_);

	auto nsArray = replConf.Array("namespaces");
	for (auto& ns : config.namespaces_) nsArray.Put(nullptr, ns);
	nsArray.End();
	replConf.End();
	jb.End();

	auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	api.Upsert(kConfigNs, item);
	err = api.Commit(kConfigNs);
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
		}
	}
	return interface;
}

void ServerControl::InitServer(size_t id, unsigned short rpcPort, unsigned short httpPort, const std::string& storagePath,
							   const std::string& dbName, bool enableStats) {
	WLock lock(mtx_);
	auto srvInterface = std::make_shared<ServerControl::Interface>(id, *stopped_, kReplicationConfigFilename, storagePath, httpPort,
																   rpcPort, dbName, enableStats);
	interface = srvInterface;
}
void ServerControl::Drop() {
	WLock lock(mtx_);
	interface.reset();
}

ReplicationStateApi ServerControl::Interface::GetState(const std::string& ns) {
	Query qr = Query("#memstats").Where("name", CondEq, ns);
	reindexer::client::QueryResults res;
	auto err = api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	ReplicationStateApi state{lsn_t(), lsn_t(), 0, 0, false};
	for (auto it : res) {
		WrSerializer ser;
		err = it.GetJSON(ser, false);
		EXPECT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		auto root = parser.Parse(ser.Slice());
		bool isSlave = root["replication"]["slave_mode"].As<bool>();
		state.ownLsn.FromJSON(root["replication"]["last_lsn_v2"]);
		if (!isSlave) {
			state.lsn.FromJSON(root["replication"]["last_lsn_v2"]);
		} else
			state.lsn.FromJSON(root["replication"]["origin_lsn"]);

		state.dataCount = root["replication"]["data_count"].As<int64_t>();
		state.dataHash = root["replication"]["data_hash"].As<uint64_t>();
		state.slaveMode = root["replication"]["slave_mode"].As<bool>();

		/*		std::cout << "\n"
					  << std::hex << "lsn = " << int64_t(state.lsn) << std::dec << " dataCount = " << state.dataCount
					  << " dataHash = " << state.dataHash << " [" << ser.c_str() << "] is slave = " << state.slaveMode << "\n"
					  << std::endl;
		*/
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
