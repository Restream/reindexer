#include "servercontrol.h"
#include <fstream>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "systemhelpers.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"
#include "yaml-cpp/yaml.h"

#ifndef REINDEXER_SERVER_PATH
#define REINDEXER_SERVER_PATH ""
#endif

using namespace reindexer;

void WriteConfigFile(const std::string& path, const std::string& configYaml) {
	std::ofstream file(path, std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

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
	if (config_.asServerProcess) {
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

AsyncReplicationConfigTest ServerControl::Interface::GetServerConfig(ConfigType type) {
	SCOPED_TRACE(fmt::format("Loading server config from: {}", (type == ConfigType::File) ? "File" : "Namespace"));
	cluster::AsyncReplConfigData asyncReplConf;
	ReplicationConfigData replConf;
	switch (type) {
		case ConfigType::File: {
			std::string replConfYaml;
			int read = fs::ReadFile(config_.storagePath + "/" + config_.dbName + "/" + kAsyncReplicationConfigFilename, replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			auto err = asyncReplConf.FromYAML(replConfYaml);
			EXPECT_TRUE(err.ok()) << err.what();
			replConfYaml.clear();
			read = fs::ReadFile(config_.storagePath + "/" + config_.dbName + "/" + kReplicationConfigFilename, replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			err = replConf.FromYAML(replConfYaml);
			EXPECT_TRUE(err.ok()) << err.what();
			break;
		}
		case ConfigType::Namespace: {
			BaseApi::QueryResultsType results;
			auto err = api.reindexer->Select(
				Query(kConfigNs).Where("type", CondEq, "async_replication").Or().Where("type", CondEq, "replication"), results);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_TRUE(results.Status().ok()) << results.Status().what();
			for (auto it : results) {
				WrSerializer ser;
				err = it.GetJSON(ser, false);
				EXPECT_TRUE(err.ok()) << err.what();
				try {
					gason::JsonParser parser;
					gason::JsonNode configJson = parser.Parse(ser.Slice());
					auto confType = configJson["type"].As<std::string_view>();
					if (confType == "replication") {
						auto& replConfigJson = configJson["replication"];
						err = replConf.FromJSON(replConfigJson);
						EXPECT_TRUE(err.ok()) << err.what();
					} else if (confType == "async_replication") {
						auto& replConfigJson = configJson["async_replication"];
						err = asyncReplConf.FromJSON(replConfigJson);
						EXPECT_TRUE(err.ok()) << err.what();
					}
				} catch (const Error&) {
					assertrx(false);
				}
			}
			break;
		}
		default:
			assert(false);
			break;
	}

	AsyncReplicationConfigTest::NsSet namespaces;
	for (auto& ns : asyncReplConf.namespaces->data) {
		namespaces.insert(ns);
	}
	std::vector<AsyncReplicationConfigTest::Node> followers;
	for (auto& node : asyncReplConf.nodes) {
		followers.emplace_back(AsyncReplicationConfigTest::Node{std::move(node.dsn)});
		if (node.HasOwnNsList()) {
			AsyncReplicationConfigTest::NsSet nss;
			for (auto& ns : node.Namespaces()->data) {
				nss.emplace(ns);
			}
			followers.back().nsList.emplace(std::move(nss));
		}
	}
	return AsyncReplicationConfigTest(cluster::AsyncReplConfigData::Role2str(asyncReplConf.role), std::move(followers),
									  asyncReplConf.forceSyncOnLogicError, asyncReplConf.forceSyncOnWrongDataHash, replConf.serverID,
									  std::move(asyncReplConf.appName), std::move(namespaces),
									  cluster::AsyncReplConfigData::Mode2str(asyncReplConf.mode), asyncReplConf.onlineUpdatesDelayMSec);
}

void ServerControl::Interface::WriteReplicationConfig(const std::string& configYaml) {
	WriteConfigFile(GetReplicationConfigFilePath(), configYaml);
}

void ServerControl::Interface::WriteAsyncReplicationConfig(const std::string& configYaml) {
	WriteConfigFile(GetAsyncReplicationConfigFilePath(), configYaml);
}

void ServerControl::Interface::WriteClusterConfig(const std::string& configYaml) {
	WriteConfigFile(GetClusterConfigFilePath(), configYaml);
}

void ServerControl::Interface::WriteShardingConfig(const std::string& configYaml) {
	std::ofstream file(config_.storagePath + "/" + config_.dbName + "/" + kClusterShardingFilename, std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

void ServerControl::Interface::SetWALSize(int64_t size, std::string_view nsName) { setNamespaceConfigItem(nsName, "wal_size", size); }

void ServerControl::Interface::SetTxAlwaysCopySize(int64_t size, std::string_view nsName) {
	setNamespaceConfigItem(nsName, "tx_size_to_always_copy", size);
}

void ServerControl::Interface::SetOptmizationSortWorkers(size_t cnt, std::string_view nsName) {
	setNamespaceConfigItem(nsName, "optimization_sort_workers", cnt);
}

void ServerControl::Interface::EnableAllProfilings() {
	constexpr std::string_view kJsonCfgProfiling = R"json({
		"type":"profiling",
		"profiling":{
			"queriesperfstats":true,
			"queries_threshold_us":0,
			"perfstats":true,
			"memstats":true
		}
	})json";
	auto item = api.reindexer->NewItem(kConfigNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(kJsonCfgProfiling);
	ASSERT_TRUE(err.ok()) << err.what();
	err = api.reindexer->Upsert(kConfigNs, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

cluster::ReplicationStats ServerControl::Interface::GetReplicationStats(std::string_view type) {
	Query qr = Query("#replicationstats").Where("type", CondEq, Variant(type));
	BaseApi::QueryResultsType res;
	auto err = api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	assertf(res.Count() == 1, "Qr.Count()==%d\n", res.Count());
	WrSerializer wser;
	err = res.begin().GetJSON(wser, false);
	EXPECT_TRUE(err.ok()) << err.what();
	cluster::ReplicationStats stats;
	err = stats.FromJSON(wser.Slice());
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(stats.type, type);
	return stats;
}

std::string ServerControl::Interface::getLogName(const std::string& log, bool core) {
	std::string name = getTestLogPath();
	name += (log + "_");
	if (!core) name += std::to_string(config_.id);
	name += ".log";
	return name;
}

ServerControl::Interface::Interface(std::atomic_bool& stopped, ServerControlConfig config, const YAML::Node& ReplicationConfig,
									const YAML::Node& ClusterConfig, const YAML::Node& ShardingConfig,
									const YAML::Node& AsyncReplicationConfig)
	: api(client::ReindexerConfig(10000, 0,
								  config.disableNetworkTimeout ? std::chrono::milliseconds(100000000) : std::chrono::milliseconds(0))),
	  kRPCDsn("cproto://127.0.0.1:" + std::to_string(config.rpcPort) + "/" + config.dbName),
	  stopped_(stopped),
	  config_(std::move(config)) {
	std::string path = reindexer::fs::JoinPath(config_.storagePath, config_.dbName);
	if (reindexer::fs::MkDirAll(path) < 0) {
		assertf(false, "Unable to remove %s", path);
	}
	WriteConfigFile(reindexer::fs::JoinPath(path, kStorageTypeFilename), "leveldb");
	if (!ReplicationConfig.IsNull()) {
		WriteReplicationConfig(YAML::Dump(ReplicationConfig));
	}
	if (!AsyncReplicationConfig.IsNull()) {
		WriteAsyncReplicationConfig(YAML::Dump(AsyncReplicationConfig));
	}
	if (!ClusterConfig.IsNull()) {
		WriteClusterConfig(YAML::Dump(ClusterConfig));
	}
	if (!ShardingConfig.IsNull()) {
		WriteShardingConfig(YAML::Dump(ShardingConfig));
	}
	Init();
}

ServerControl::Interface::Interface(std::atomic_bool& stopped, ServerControlConfig config)
	: api(client::ReindexerConfig(10000, 0,
								  config.disableNetworkTimeout ? std::chrono::milliseconds(100000000) : std::chrono::milliseconds(0))),
	  kRPCDsn("cproto://127.0.0.1:" + std::to_string(config.rpcPort) + "/" + config.dbName),
	  stopped_(stopped),
	  config_(std::move(config)) {
	Init();
}

void ServerControl::Interface::Init() {
	stopped_ = false;
	YAML::Node y;
	y["storage"]["path"] = config_.storagePath;
	y["metrics"]["clientsstats"] = config_.enableStats;
	y["logger"]["loglevel"] = "trace";
	y["logger"]["rpclog"] = getLogName("rpc");
	y["logger"]["serverlog"] = getLogName("server");
	y["logger"]["corelog"] = getLogName("core", true);
	y["net"]["httpaddr"] = "0.0.0.0:" + std::to_string(config_.httpPort);
	y["net"]["rpcaddr"] = "0.0.0.0:" + std::to_string(config_.rpcPort);
	y["net"]["enable_cluster"] = true;
	if (config_.maxUpdatesSize) {
		y["net"]["maxupdatessize"] = config_.maxUpdatesSize;
	}
	if (config_.asServerProcess) {
		try {
			std::vector<std::string> paramArray = getCLIParamArray(config_.enableStats, config_.maxUpdatesSize);
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
	}

	// init client
	std::string dsn = "cproto://127.0.0.1:" + std::to_string(config_.rpcPort) + "/" + config_.dbName;
	Error err;
	err = api.reindexer->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
	EXPECT_TRUE(err.ok()) << err.what();

	while (!api.reindexer->Status().ok()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

void ServerControl::Interface::MakeLeader(const AsyncReplicationConfigTest& config) {
	assert(config.serverId >= 0);
	if (config.nodes.empty()) {
		AsyncReplicationConfigTest cfg("leader");
		cfg.serverId = config_.id;
		SetReplicationConfig(cfg);
	} else {
		assert(size_t(config.serverId) == config_.id);
		assert(config.role == "leader");
		SetReplicationConfig(config);
	}
}

void ServerControl::Interface::MakeFollower() {
	AsyncReplicationConfigTest config("follower", std::vector<AsyncReplicationConfigTest::Node>{});
	config.serverId = config_.id;
	assert(config.serverId >= 0);
	SetReplicationConfig(config);
}

void ServerControl::Interface::SetReplicationConfig(const AsyncReplicationConfigTest& config) {
	cluster::AsyncReplConfigData asyncReplConf;
	asyncReplConf.appName = config.appName;
	asyncReplConf.role = cluster::AsyncReplConfigData::Str2role(config.role);
	fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> nss;
	for (auto& ns : config.namespaces) {
		nss.emplace(ns);
	}
	asyncReplConf.namespaces = make_intrusive<cluster::AsyncReplConfigData::NamespaceList>(std::move(nss));
	asyncReplConf.onlineUpdatesTimeoutSec = 20;	 // -V1048
	asyncReplConf.replThreadsCount = 2;
	asyncReplConf.forceSyncOnLogicError = config.forceSyncOnLogicError;
	asyncReplConf.forceSyncOnWrongDataHash = config.forceSyncOnWrongDataHash;
	asyncReplConf.onlineUpdatesDelayMSec = config.onlineUpdatesDelayMSec;
	asyncReplConf.retrySyncIntervalMSec = 1000;
	asyncReplConf.logLevel = LogTrace;
	asyncReplConf.mode = cluster::AsyncReplConfigData::Str2mode(config.mode);
	for (auto& node : config.nodes) {
		asyncReplConf.nodes.emplace_back(cluster::AsyncReplNodeConfig{node.dsn});
		if (node.nsList.has_value()) {
			fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> nss;
			for (auto& ns : node.nsList.value()) {
				nss.emplace(ns);
			}
			asyncReplConf.nodes.back().SetOwnNamespaceList(std::move(nss));
		}
	}

	ReplicationConfigData replConf;
	replConf.serverID = config.serverId;
	replConf.clusterID = 2;

	upsertConfigItemFromObject("replication", replConf);
	upsertConfigItemFromObject("async_replication", asyncReplConf);
	auto err = api.Commit(kConfigNs);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ServerControl::Interface::AddFollower(const std::string& dsn, std::optional<std::vector<std::string>>&& nsList,
										   cluster::AsyncReplicationMode replMode) {
	auto getCurConf = [this](cluster::AsyncReplConfigData& curConf) {
		BaseApi::QueryResultsType qr;
		auto err = api.reindexer->Select(Query(kConfigNs).Where("type", CondEq, "async_replication"), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 1);
		WrSerializer ser;
		qr.begin().GetJSON(ser, false);
		curConf = cluster::AsyncReplConfigData();
		err = curConf.FromJSON(gason::JsonParser().Parse(ser.Slice())["async_replication"]);
		ASSERT_TRUE(err.ok()) << err.what();
	};
	cluster::AsyncReplConfigData curConf;
	getCurConf(curConf);
	if (curConf.role != cluster::AsyncReplConfigData::Role::Leader) {
		MakeLeader();
		getCurConf(curConf);
	}
	cluster::AsyncReplNodeConfig newNode;
	newNode.dsn = dsn;
	if (replMode != cluster::AsyncReplicationMode::Default) {
		newNode.SetReplicationMode(replMode);
	}
	auto found = std::find_if(curConf.nodes.begin(), curConf.nodes.end(),
							  [&dsn](const cluster::AsyncReplNodeConfig& node) { return node.GetRPCDsn() == dsn; });
	ASSERT_TRUE(found == curConf.nodes.end());
	curConf.nodes.emplace_back(std::move(newNode));
	if (nsList.has_value()) {
		fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> nss;
		for (auto&& ns : nsList.value()) {
			nss.emplace(std::move(ns));
		}
		curConf.nodes.back().SetOwnNamespaceList(std::move(nss));
	}

	curConf.onlineUpdatesTimeoutSec = 20;
	curConf.replThreadsCount = 2;
	curConf.retrySyncIntervalMSec = 1000;
	curConf.logLevel = LogTrace;
	curConf.mode = cluster::AsyncReplicationMode::Default;

	upsertConfigItemFromObject("async_replication", curConf);

	auto err = api.Commit(kConfigNs);
	ASSERT_TRUE(err.ok()) << err.what();
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

void ServerControl::InitServer(ServerControlConfig config) {
	WLock lock(mtx_);
	interface = std::make_shared<ServerControl::Interface>(*stopped_, std::move(config));
}

void ServerControl::InitServerWithConfig(ServerControlConfig config, const YAML::Node& ReplicationConfig, const YAML::Node& ClusterConfig,
										 const YAML::Node& ShardingConfig, const YAML::Node& AsyncReplicationConfig) {
	WLock lock(mtx_);
	interface = std::make_shared<ServerControl::Interface>(*stopped_, std::move(config), ReplicationConfig, ClusterConfig, ShardingConfig,
														   AsyncReplicationConfig);
}

void ServerControl::Drop() {
	WLock lock(mtx_);
	interface.reset();
}

bool ServerControl::DropAndWaitStop() {
	Drop();
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(10);
	while (IsRunning()) {
		now += pause;
		if (now > std::chrono::milliseconds(std::chrono::seconds(kMaxServerStartTimeSec))) {
			return false;
		}
		std::this_thread::sleep_for(pause);
	}
	return true;
}

void ServerControl::WaitSync(const ServerControl::Interface::Ptr& s1, const ServerControl::Interface::Ptr& s2, const std::string& nsName) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(50);
	ReplicationStateApi state1, state2;
	while (true) {
		now += pause;
		const std::string tmStateToken1 = state1.tmStatetoken.has_value() ? std::to_string(state1.tmStatetoken.value()) : "<none>";
		const std::string tmStateToken2 = state2.tmStatetoken.has_value() ? std::to_string(state2.tmStatetoken.value()) : "<none>";
		const std::string tmVersion1 = state1.tmVersion.has_value() ? std::to_string(state1.tmVersion.value()) : "<none>";
		const std::string tmVersion2 = state2.tmVersion.has_value() ? std::to_string(state2.tmVersion.value()) : "<none>";
		ASSERT_TRUE(now < kMaxSyncTime) << "Wait sync is too long. s1 lsn: " << state1.lsn << "; s2 lsn: " << state2.lsn
										<< "; s1 count: " << state1.dataCount << "; s2 count: " << state2.dataCount
										<< " s1 hash: " << state1.dataHash << "; s2 hash: " << state2.dataHash
										<< " s1 tm_token: " << tmStateToken1 << "; s2 tm_token: " << tmStateToken2
										<< " s1 tm_version: " << tmVersion1 << "; s2 tm_version: " << tmVersion2;
		state1 = s1->GetState(nsName);
		state2 = s2->GetState(nsName);

		if (state1.tmStatetoken.has_value() && state2.tmStatetoken.has_value() && state1.tmVersion.has_value() &&
			state2.tmVersion.has_value()) {
			const bool hasSameTms =
				state1.tmStatetoken.value() == state2.tmStatetoken.value() && state1.tmVersion.value() == state2.tmVersion.value();
			const bool hasSameLSN = state1.lsn == state2.lsn && state1.nsVersion == state2.nsVersion;

			if (hasSameTms && hasSameLSN) {
				ASSERT_EQ(state1.dataHash, state2.dataHash);
				return;
			}
		}
		std::this_thread::sleep_for(pause);
	}
}

ReplicationStateApi ServerControl::Interface::GetState(const std::string& ns) {
	ReplicationStateApi state;
	{
		Query qr = Query("#memstats").Where("name", CondEq, ns);
		BaseApi::QueryResultsType res;
		auto err = api.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();
		for (auto it : res) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			EXPECT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(ser.Slice());
			state.lsn.FromJSON(root["replication"]["last_lsn_v2"]);

			state.dataCount = root["replication"]["data_count"].As<int64_t>();
			state.dataHash = root["replication"]["data_hash"].As<uint64_t>();
			state.nsVersion.FromJSON(root["replication"]["ns_version"]);
			try {
				reindexer::ClusterizationStatus clStatus;
				clStatus.FromJSON(root["replication"]["clusterization_status"]);
				state.role = clStatus.role;
			} catch (...) {
				EXPECT_TRUE(false) << "Unable to parse cluster status: " << ser.Slice();
			}

			/*		TestCout() << "\n"
						  << std::hex << "lsn = " << int64_t(state.lsn) << std::dec << " dataCount = " << state.dataCount
						  << " dataHash = " << state.dataHash << " [" << ser.c_str() << "]\n"
						  << std::endl;
			*/
		}
	}
	{
		Query qr = Query(ns).Limit(0);
		BaseApi::QueryResultsType res;
		auto err = api.reindexer->Select(qr, res);
		if (err.ok()) {
			auto tm = res.GetTagsMatcher(ns);
			state.tmVersion = tm.version();
			state.tmStatetoken = tm.stateToken();
		}
	}
	return state;
}

void ServerControl::Interface::ForceSync() {
	Error err;
	auto item = api.NewItem("#config");
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(R"json({"type":"action","action":{"command":"restart_replication"}})json");
	ASSERT_TRUE(err.ok()) << err.what();
	api.Upsert("#config", item);
}

void ServerControl::Interface::ResetReplicationRole(const std::string& ns) {
	auto err = TryResetReplicationRole(ns);
	ASSERT_TRUE(err.ok()) << err.what();
}

Error ServerControl::Interface::TryResetReplicationRole(const std::string& ns) {
	BaseApi::QueryResultsType res;
	Error err;
	auto item = api.NewItem("#config");
	if (!item.Status().ok()) return item.Status();
	if (ns.size()) {
		err =
			item.FromJSON(fmt::sprintf(R"json({"type":"action","action":{"command":"reset_replication_role","namespace":"%s"}})json", ns));
	} else {
		err = item.FromJSON(R"json({"type":"action","action":{"command":"reset_replication_role"}})json");
	}
	if (!err.ok()) return err;
	return api.reindexer->Upsert("#config", item);
}

void ServerControl::Interface::SetClusterLeader(int leaderId) {
	auto item = CreateClusterChangeLeaderItem(leaderId);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	api.Upsert("#config", item);
}

void ServerControl::Interface::SetReplicationLogLevel(LogLevel level, std::string_view type) {
	BaseApi::QueryResultsType res;
	auto item = api.NewItem("#config");
	auto err = item.Status();
	ASSERT_TRUE(err.ok()) << err.what();
	err = item.FromJSON(fmt::sprintf(R"json({"type":"action","action":{"command":"set_log_level","type":"%s","level":"%s"}})json", type,
									 reindexer::logLevelToString(level)));
	ASSERT_TRUE(err.ok()) << err.what();
	err = api.reindexer->Upsert("#config", item);
	ASSERT_TRUE(err.ok()) << err.what();
}

BaseApi::ItemType ServerControl::Interface::CreateClusterChangeLeaderItem(int leaderId) {
	auto item = api.NewItem("#config");
	if (item.Status().ok()) {
		WrSerializer ser;
		JsonBuilder jb(ser);
		jb.Put("type", "action");
		auto actionObject = jb.Object("action");
		actionObject.Put("command", "set_leader_node");
		actionObject.Put("server_id", leaderId);
		actionObject.End();
		jb.End();
		Error err = item.FromJSON(ser.Slice());
		EXPECT_TRUE(err.ok()) << err.what();
	}
	return item;
}

std::vector<std::string> ServerControl::Interface::getCLIParamArray(bool enableStats, size_t maxUpdatesSize) {
	std::vector<std::string> paramArray;
	paramArray.push_back("reindexer_server");
	paramArray.push_back("--db=" + config_.storagePath);
	if (enableStats) {
		paramArray.push_back("--clientsstats");
	}
	paramArray.push_back("--loglevel=trace");
	paramArray.push_back("--rpclog=" + getLogName("rpc"));
	paramArray.push_back("--serverlog=" + getLogName("server"));
	// paramArray.push_back("--httplog");
	paramArray.push_back("--corelog=" + getLogName("core"));

	paramArray.push_back("--httpaddr=0.0.0.0:" + std::to_string(config_.httpPort));
	paramArray.push_back("--rpcaddr=0.0.0.0:" + std::to_string(config_.rpcPort));
	paramArray.push_back("--enable-cluster");
	if (maxUpdatesSize) {
		paramArray.push_back("--updatessize=" + std::to_string(maxUpdatesSize));
	}

	return paramArray;
}

template <typename ValueT>
void ServerControl::Interface::upsertConfigItemFromObject(std::string_view type, const ValueT& object) {
	WrSerializer ser;
	JsonBuilder jb(ser);
	jb.Put("type", type);
	{
		auto objBuilder = jb.Object(type);
		object.GetJSON(objBuilder);
		objBuilder.End();
	}
	jb.End();

	auto item = api.NewItem(kConfigNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	api.Upsert(kConfigNs, item);
}
