#include "servercontrol.h"
#include <fstream>
#include "cluster/config.h"
#include "core/cjson/jsonbuilder.h"
#include "core/queryresults/queryresults.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"

using namespace reindexer;

void WriteConfigFile(const std::string& path, const std::string& configYaml) {
	std::ofstream file(path, std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

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
void ServerControl::Stop() { interface->Stop(); }

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

AsyncReplicationConfigTest ServerControl::Interface::GetServerConfig(ConfigType type) {
	cluster::AsyncReplConfigData asyncReplConf;
	ReplicationConfigData replConf;
	switch (type) {
		case ConfigType::File: {
			std::string replConfYaml;
			int read = fs::ReadFile(kStoragePath + "/" + dbName_ + "/" + kAsyncReplicationConfigFilename, replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			auto err = asyncReplConf.FromYML(replConfYaml);
			EXPECT_TRUE(err.ok()) << err.what();
			replConfYaml.clear();
			read = fs::ReadFile(kStoragePath + "/" + dbName_ + "/" + kReplicationConfigFilename, replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			err = replConf.FromYML(replConfYaml);
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
					assert(false);
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
									  std::move(asyncReplConf.appName), std::move(namespaces));
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
	std::ofstream file(kStoragePath + "/" + dbName_ + "/" + kClusterShardingFilename, std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

void ServerControl::Interface::SetWALSize(int64_t size, std::string_view nsName) { setNamespaceConfigItem(nsName, "wal_size", size); }

void ServerControl::Interface::SetOptmizationSortWorkers(size_t cnt, std::string_view nsName) {
	setNamespaceConfigItem(nsName, "optimization_sort_workers", cnt);
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

ServerControl::Interface::Interface(size_t id, std::atomic_bool& stopped, const std::string& StoragePath, unsigned short httpPort,
									unsigned short rpcPort, const std::string& dbName, bool enableStats, size_t maxUpdatesSize)
	: kClusterManagementDsn("cproto://127.0.0.1:" + std::to_string(rpcPort) + "/" + dbName),
	  id_(id),
	  stopped_(stopped),
	  kStoragePath(StoragePath),
	  kRpcPort(rpcPort),
	  kHttpPort(httpPort),
	  dbName_(dbName) {
	// Init server in thread
	stopped_ = false;
	string testSetName = ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name();
	string testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
	auto getLogName = [&testSetName, &testName, &id](const string& log, bool core = false) {
		string name("logs/" + testSetName + "/" + testName + "/" + log + "_");
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
		"   replicationstats: true\n"
        "logger:\n"
        "   loglevel: trace\n"
        "   rpclog: " + getLogName("rpc") + "\n"
        "   serverlog: " + getLogName("server") + "\n"
        "   corelog: " + getLogName("core", true) + "\n"
        "net:\n"
        "   httpaddr: 0.0.0.0:" + std::to_string(kHttpPort) + "\n"
		"   rpcaddr: 0.0.0.0:" + std::to_string(kRpcPort) + "\n" +
		"   enable_cluster: true\n" +
		(maxUpdatesSize ?
		"   maxupdatessize:" + std::to_string(maxUpdatesSize)+"\n" : "");
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
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	// init client
	string dsn = "cproto://127.0.0.1:" + std::to_string(kRpcPort) + "/" + dbName_;
	err = api.reindexer->Connect(dsn, client::ConnectOpts().CreateDBIfMissing());
	EXPECT_TRUE(err.ok()) << err.what();
	while (!api.reindexer->Status().ok()) {
		std::cerr << api.reindexer->Status().what() << std::endl;
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

void ServerControl::Interface::MakeLeader(const AsyncReplicationConfigTest& config) {
	assert(config.serverId_ >= 0);
	if (config.nodes_.empty()) {
		AsyncReplicationConfigTest cfg("leader");
		cfg.serverId_ = id_;
		SetReplicationConfig(cfg);
	} else {
		assert(size_t(config.serverId_) == id_);
		assert(config.role_ == "leader");
		SetReplicationConfig(config);
	}
}

void ServerControl::Interface::MakeFollower() {
	AsyncReplicationConfigTest config("follower", std::vector<AsyncReplicationConfigTest::Node>{});
	config.serverId_ = id_;
	assert(config.serverId_ >= 0);
	SetReplicationConfig(config);
}

void ServerControl::Interface::SetReplicationConfig(const AsyncReplicationConfigTest& config) {
	cluster::AsyncReplConfigData asyncReplConf;
	asyncReplConf.appName = config.appName_;
	asyncReplConf.role = cluster::AsyncReplConfigData::Str2role(config.role_);
	fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss;
	for (auto& ns : config.namespaces_) {
		nss.emplace(ns);
	}
	asyncReplConf.namespaces = make_intrusive<cluster::AsyncReplConfigData::NamespaceList>(std::move(nss));
	asyncReplConf.onlineUpdatesTimeoutSec = 20;	 // -V1048
	asyncReplConf.replThreadsCount = 2;
	asyncReplConf.forceSyncOnLogicError = config.forceSyncOnLogicError_;
	asyncReplConf.forceSyncOnWrongDataHash = config.forceSyncOnWrongDataHash_;
	asyncReplConf.retrySyncIntervalMSec = 1000;
	for (auto& node : config.nodes_) {
		asyncReplConf.nodes.emplace_back(cluster::AsyncReplNodeConfig{node.dsn});
		if (node.nsList.has_value()) {
			fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss;
			for (auto& ns : node.nsList.value()) {
				nss.emplace(ns);
			}
			asyncReplConf.nodes.back().SetOwnNamespaceList(std::move(nss));
		}
	}

	ReplicationConfigData replConf;
	replConf.serverID = config.serverId_;
	replConf.clusterID = 2;

	upsertConfigItemFromObject("replication", replConf);
	upsertConfigItemFromObject("async_replication", asyncReplConf);
	auto err = api.Commit(kConfigNs);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ServerControl::Interface::AddFollower(const std::string& dsn, std::optional<std::vector<std::string>>&& nsList) {
	BaseApi::QueryResultsType qr;
	auto err = api.reindexer->Select(Query(kConfigNs).Where("type", CondEq, "async_replication"), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	WrSerializer ser;
	qr.begin().GetJSON(ser, false);
	cluster::AsyncReplConfigData curConf;
	err = curConf.FromJSON(gason::JsonParser().Parse(ser.Slice())["async_replication"]);
	ASSERT_TRUE(err.ok()) << err.what();
	cluster::AsyncReplNodeConfig newNode;
	newNode.dsn = dsn;
	auto found = std::find_if(curConf.nodes.begin(), curConf.nodes.end(),
							  [&dsn](const cluster::AsyncReplNodeConfig& node) { return node.GetRPCDsn() == dsn; });
	ASSERT_TRUE(found == curConf.nodes.end());
	curConf.nodes.emplace_back(std::move(newNode));
	if (nsList.has_value()) {
		fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss;
		for (auto&& ns : nsList.value()) {
			nss.emplace(std::move(ns));
		}
		curConf.nodes.back().SetOwnNamespaceList(std::move(nss));
	}

	curConf.onlineUpdatesTimeoutSec = 20;
	curConf.replThreadsCount = 2;
	curConf.retrySyncIntervalMSec = 1000;

	upsertConfigItemFromObject("async_replication", curConf);

	err = api.Commit(kConfigNs);
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
			assert(counter / 1000 < kMaxServerStartTimeSec);

			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
	return interface;
}

void ServerControl::InitServer(size_t id, unsigned short rpcPort, unsigned short httpPort, const std::string& storagePath,
							   const std::string& dbName, bool enableStats, size_t maxUpdatesSize) {
	WLock lock(mtx_);
	interface =
		std::make_shared<ServerControl::Interface>(id, *stopped_, storagePath, httpPort, rpcPort, dbName, enableStats, maxUpdatesSize);
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

ReplicationStateApi ServerControl::Interface::GetState(const std::string& ns) {
	Query qr = Query("#memstats").Where("name", CondEq, ns);
	BaseApi::QueryResultsType res;
	auto err = api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	ReplicationStateApi state;
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

		/*		std::cout << "\n"
					  << std::hex << "lsn = " << int64_t(state.lsn) << std::dec << " dataCount = " << state.dataCount
					  << " dataHash = " << state.dataHash << " [" << ser.c_str() << "]\n"
					  << std::endl;
		*/
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
		err = item.FromJSON("{\"type\":\"action\",\"action\":{\"command\":\"reset_replication_role\", \"namespace\": \"" + ns + "\"}}");
	} else {
		err = item.FromJSON(R"json({"type":"action","action":{"command":"reset_replication_role"}})json");
	}
	if (!err.ok()) return err;
	return api.reindexer->Upsert("#config", item);
}

void ServerControl::Interface::SetClusterLeader(int lederId) {
	auto item = api.NewItem("#config");
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	WrSerializer ser;
	JsonBuilder jb(ser);
	jb.Put("type", "action");
	auto actionObject = jb.Object("action");
	actionObject.Put("command", "set_leader_node");
	actionObject.Put("server_id", lederId);
	actionObject.End();
	jb.End();
	Error err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	api.Upsert("#config", item);
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
