#include "servercontrol.h"
#include <fstream>
#include "core/cjson/jsonbuilder.h"
#include "systemhelpers.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"
#include "yaml-cpp/yaml.h"

#ifndef REINDEXER_SERVER_PATH
#define REINDEXER_SERVER_PATH ""
#endif

using namespace reindexer;

ServerControl::Interface::~Interface() {
	Stop();
	if (tr) {
		tr->join();
	}
	if (reindexerServerPIDWait != -1) {
		auto err = reindexer::WaitEndProcess(reindexerServerPIDWait);
		assertf(err.ok(), "WaitEndProcess error: %s", err.what());
	}
	stopped_ = true;
}

void ServerControl::Interface::Stop() {
	if (asServerProcess) {
		if (reindexerServerPID != -1) {
			auto err = reindexer::EndProcess(reindexerServerPID);
			assertf(err.ok(), "EndProcess error: %s", err.what());
			reindexerServerPIDWait = reindexerServerPID;
			reindexerServerPID = -1;
		}
	} else {
		srv.Stop();
	}
}
std::string ServerControl::Interface::getLogName(const std::string& log, bool core) {
	std::string name = getTestLogPath();
	name += (log + "_");
	if (!core) name += std::to_string(id_);
	name += ".log";
	return name;
}

ServerControl::ServerControl() { stopped_ = new std::atomic_bool(false); }
ServerControl::~ServerControl() {
	WLock lock(mtx_);
	interface.reset();
	delete stopped_;
}
void ServerControl::Stop() { interface->Stop(); }

ServerControl::ServerControl(ServerControl&& rhs) noexcept {
	WLock lock(rhs.mtx_);
	interface = std::move(rhs.interface);
	stopped_ = rhs.stopped_;
	rhs.stopped_ = nullptr;
}
ServerControl& ServerControl::operator=(ServerControl&& rhs) noexcept {
	WLock lock(rhs.mtx_);
	interface = std::move(rhs.interface);
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
			BaseApi::QueryResultsType results(api.reindexer.get());
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
					assertrx(false);
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

void ServerControl::Interface::SetWALSize(int64_t size, std::string_view nsName) { setNamespaceConfigItem(nsName, "wal_size", size); }

void ServerControl::Interface::SetOptmizationSortWorkers(size_t cnt, std::string_view nsName) {
	setNamespaceConfigItem(nsName, "optimization_sort_workers", cnt);
}
void ServerControl::Interface::Init() {
	stopped_ = false;
	YAML::Node y;
	y["storage"]["path"] = kStoragePath;
	y["metrics"]["clientsstats"] = enableStats_;
	y["logger"]["loglevel"] = "trace";
	y["logger"]["rpclog"] = getLogName("rpc");
	y["logger"]["serverlog"] = getLogName("server");
	y["logger"]["corelog"] = getLogName("core", true);
	y["net"]["httpaddr"] = "0.0.0.0:" + std::to_string(kHttpPort);
	y["net"]["rpcaddr"] = "0.0.0.0:" + std::to_string(kRpcPort);
	if (maxUpdatesSize_) {
		y["net"]["maxupdatessize"] = maxUpdatesSize_;
	}

	if (asServerProcess) {
		try {
			std::vector<std::string> paramArray = getCLIParamArray(enableStats_, maxUpdatesSize_);
			std::string reindexerServerPath(REINDEXER_SERVER_PATH);
			if (reindexerServerPath.empty()) {	// -V547
				throw Error(errLogic, "REINDEXER_SERVER_PATH empty");
			}
			reindexerServerPID = reindexer::StartProcess(reindexerServerPath, paramArray);
		} catch (Error& e) {
			EXPECT_TRUE(false) << e.what();
		}
	} else {
		auto err = srv.InitFromYAML(YAML::Dump(y));
		EXPECT_TRUE(err.ok()) << err.what();

		tr = std::unique_ptr<std::thread>(new std::thread([this]() {
			auto res = this->srv.Start();
			if (res != EXIT_SUCCESS) {
				std::cerr << "Exit code: " << res << std::endl;
			}
			assertrx(res == EXIT_SUCCESS);
		}));
		while (!srv.IsRunning() || !srv.IsReady()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}

	// init client
	std::string dsn = "cproto://127.0.0.1:" + std::to_string(kRpcPort) + "/" + dbName_;
	Error err;
	err = api.reindexer->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
	EXPECT_TRUE(err.ok()) << err.what();

	while (!api.reindexer->Status().ok()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

std::vector<std::string> ServerControl::Interface::getCLIParamArray(bool enableStats, size_t maxUpdatesSize) {
	std::vector<std::string> paramArray;
	paramArray.push_back("reindexer_server");
	paramArray.push_back("--db=" + kStoragePath);
	if (enableStats) {
		paramArray.push_back("--clientsstats");
	}
	paramArray.push_back("--loglevel=trace");
	paramArray.push_back("--rpclog=" + getLogName("rpc"));
	paramArray.push_back("--serverlog=" + getLogName("server"));
	// paramArray.push_back("--httplog");
	paramArray.push_back("--corelog=" + getLogName("core"));

	paramArray.push_back("--httpaddr=0.0.0.0:" + std::to_string(kHttpPort));
	paramArray.push_back("--rpcaddr=0.0.0.0:" + std::to_string(kRpcPort));
	if (maxUpdatesSize) {
		paramArray.push_back("--updatessize=" + std::to_string(maxUpdatesSize));
	}

	return paramArray;
}

ServerControl::Interface::Interface(size_t id, std::atomic_bool& stopped, const std::string& ReplicationConfigFilename,
									const std::string& StoragePath, unsigned short httpPort, unsigned short rpcPort,
									const std::string& dbName, bool enableStats, size_t maxUpdatesSize)
	: id_(id),
	  stopped_(stopped),
	  kReplicationConfigFilename(ReplicationConfigFilename),
	  kStoragePath(StoragePath),
	  kRpcPort(rpcPort),
	  kHttpPort(httpPort),
	  dbName_(dbName),
	  enableStats_(enableStats),
	  maxUpdatesSize_(maxUpdatesSize) {
	stopped_ = false;
	Init();
}

void ServerControl::Interface::MakeMaster(const ReplicationConfigTest& config) {
	assertrx(config.role_ == "master");
	setReplicationConfig(id_, config);
}
void ServerControl::Interface::MakeSlave(size_t masterId, const ReplicationConfigTest& config) {
	assertrx(config.role_ == "slave");
	assertrx(!config.dsn_.empty());
	setReplicationConfig(masterId, config);
}

template <typename ValueT>
void ServerControl::Interface::setNamespaceConfigItem(std::string_view nsName, std::string_view paramName, ValueT&& value) {
	reindexer::WrSerializer ser;
	reindexer::JsonBuilder jb(ser);

	jb.Put("type", "namespaces");
	auto nsArray = jb.Array("namespaces");
	auto ns = nsArray.Object();
	ns.Put("namespace", nsName);
	ns.Put(paramName, value);

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
			assertrx(counter / 1000 < kMaxServerStartTimeSec);

			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
	return interface;
}

void ServerControl::InitServer(size_t id, unsigned short rpcPort, unsigned short httpPort, const std::string& storagePath,
							   const std::string& dbName, bool enableStats, size_t maxUpdatesSize) {
	WLock lock(mtx_);
	auto srvInterface = std::make_shared<ServerControl::Interface>(id, *stopped_, kReplicationConfigFilename, storagePath, httpPort,
																   rpcPort, dbName, enableStats, maxUpdatesSize);
	interface = srvInterface;
}
void ServerControl::Drop() {
	WLock lock(mtx_);
	interface.reset();
}

ReplicationStateApi ServerControl::Interface::GetState(const std::string& ns) {
	Query qr = Query("#memstats").Where("name", CondEq, ns);
	BaseApi::QueryResultsType res(api.reindexer.get());
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
	Error err;
	auto item = api.NewItem("#config");
	EXPECT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(R"json({"type":"action","action":{"command":"restart_replication"}})json");
	EXPECT_TRUE(err.ok()) << err.what();
	api.Upsert("#config", item);
}
