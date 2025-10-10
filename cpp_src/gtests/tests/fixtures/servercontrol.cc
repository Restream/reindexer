#include "servercontrol.h"
#include <fstream>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "core/formatters/lsn_fmt.h"
#include "core/system_ns_names.h"
#include "estl/gift_str.h"
#include "estl/lock.h"
#include "systemhelpers.h"
#include "tools/crypt.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"
#include "vendor/yaml-cpp/yaml.h"

#ifndef REINDEXER_SERVER_PATH
#define REINDEXER_SERVER_PATH ""
#endif

using namespace reindexer;

void WriteConfigFile(const std::string& path, const std::string& configYaml) {
	std::ofstream file(path, std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

static constexpr auto kDefaultSSLCertFile = "cert.pem";
static constexpr auto kDefaultSSLKeyFile = "key.pem";

AsyncReplicationConfigTest::Node::Node(reindexer::DSN _dsn, std::optional<NsSet> _nsList)
	: dsn(std::move(_dsn)), nsList(std::move(_nsList)) {}

void AsyncReplicationConfigTest::Node::GetJSON(reindexer::JsonBuilder& jb) const {
	jb.Put("dsn", dsn);
	auto arrNode = jb.Array("namespaces");
	if (nsList) {
		for (const auto& ns : *nsList) {
			arrNode.Put(reindexer::TagName::Empty(), ns);
		}
	}
}

AsyncReplicationConfigTest::AsyncReplicationConfigTest(std::string _role, std::vector<Node> _followers, std::string _appName,
													   std::string _mode)
	: role(std::move(_role)),
	  mode(std::move(_mode)),
	  nodes(std::move(_followers)),
	  forceSyncOnLogicError(false),
	  forceSyncOnWrongDataHash(true),
	  appName(std::move(_appName)),
	  serverId(0) {}

AsyncReplicationConfigTest::AsyncReplicationConfigTest(std::string _role, std::vector<Node> _followers, bool _forceSyncOnLogicError,
													   bool _forceSyncOnWrongDataHash, int _serverId, std::string _appName,
													   NsSet _namespaces, std::string _mode, int _onlineUpdatesDelayMSec)
	: role(std::move(_role)),
	  mode(std::move(_mode)),
	  nodes(std::move(_followers)),
	  forceSyncOnLogicError(_forceSyncOnLogicError),
	  forceSyncOnWrongDataHash(_forceSyncOnWrongDataHash),
	  appName(std::move(_appName)),
	  namespaces(std::move(_namespaces)),
	  serverId(_serverId),
	  onlineUpdatesDelayMSec(_onlineUpdatesDelayMSec) {}

bool AsyncReplicationConfigTest::operator==(const AsyncReplicationConfigTest& config) const {
	return role == config.role && mode == config.mode && nodes == config.nodes && forceSyncOnLogicError == config.forceSyncOnLogicError &&
		   forceSyncOnWrongDataHash == config.forceSyncOnWrongDataHash && appName == config.appName && namespaces == config.namespaces &&
		   serverId == config.serverId && syncThreads == config.syncThreads &&
		   concurrentSyncsPerThread == config.concurrentSyncsPerThread && onlineUpdatesDelayMSec == config.onlineUpdatesDelayMSec;
}

std::string AsyncReplicationConfigTest::GetJSON() const {
	reindexer::WrSerializer wser;
	reindexer::JsonBuilder jb(wser);
	GetJSON(jb);
	return std::string(wser.Slice());
}

void AsyncReplicationConfigTest::GetJSON(reindexer::JsonBuilder& jb) const {
	jb.Put("app_name", appName);
	jb.Put("server_id", serverId);
	jb.Put("role", role);
	jb.Put("mode", mode);
	jb.Put("force_sync_on_logic_error", forceSyncOnLogicError);
	jb.Put("force_sync_on_wrong_data_hash", forceSyncOnWrongDataHash);
	jb.Put("sync_threads", syncThreads);
	jb.Put("syncs_per_thread", concurrentSyncsPerThread);
	jb.Put("online_updates_delay_msec", onlineUpdatesDelayMSec);
	{
		auto arrNode = jb.Array("namespaces");
		for (const auto& ns : namespaces) {
			arrNode.Put(reindexer::TagName::Empty(), ns);
		}
	}
	{
		auto arrNode = jb.Array("nodes");
		for (const auto& node : nodes) {
			auto obj = arrNode.Object();
			node.GetJSON(obj);
		}
	}
}

ServerControl::Interface::~Interface() {
	try {
		Stop();
		if (tr) {
			tr->join();
		}
		if (reindexerServerPIDWait != -1) {
			auto err = reindexer::WaitEndProcess(reindexerServerPIDWait);
			assertf(err.ok(), "WaitEndProcess error: {}", err.what());
		}
		stopped_ = true;
	} catch (const std::exception& err) {
		fprintf(stderr, "reindexer error: unexpected exception in ~Interface: %s\n", err.what());
		std::abort();
	}
}

void ServerControl::Interface::Stop() {
	if (config_.asServerProcess) {
		if (reindexerServerPID != -1) {
			auto err = reindexer::EndProcess(reindexerServerPID);
			assertf(err.ok(), "EndProcess error: {}", err.what());
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

std::string ServerControl::getTestLogPath() {
	const char* testSetName = ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name();
	const char* testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
	std::string name;
	name = name + "logs/" + testSetName + "/" + testName + "/";
	return name;
}

ServerControl::ServerControl(ServerControl&& rhs) noexcept {
	WLock lock(rhs.mtx_);
	interface = std::move(rhs.interface);
	stopped_ = rhs.stopped_;
	rhs.stopped_ = nullptr;
}
ServerControl& ServerControl::operator=(ServerControl&& rhs) noexcept {
	if (this != &rhs) {
		WLock lock(rhs.mtx_);
		interface = std::move(rhs.interface);
		delete stopped_;
		stopped_ = rhs.stopped_;
		rhs.stopped_ = nullptr;
	}
	return *this;
}

AsyncReplicationConfigTest ServerControl::Interface::GetServerConfig(ConfigType type) {
	SCOPED_TRACE(fmt::format("Loading server config from: {}", (type == ConfigType::File) ? "File" : "Namespace"));
	cluster::AsyncReplConfigData asyncReplConf;
	ReplicationConfigData replConf;
	switch (type) {
		case ConfigType::File: {
			const auto dbPath = fs::JoinPath(config_.storagePath, config_.dbName);
			std::string replConfYaml;
			int read = fs::ReadFile(fs::JoinPath(dbPath, kAsyncReplicationConfigFilename), replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			auto err = asyncReplConf.FromYAML(replConfYaml);
			EXPECT_TRUE(err.ok()) << err.what();
			replConfYaml.clear();
			read = fs::ReadFile(fs::JoinPath(dbPath, kReplicationConfigFilename), replConfYaml);
			EXPECT_TRUE(read > 0) << "Repl config read error";
			err = replConf.FromYAML(replConfYaml);
			EXPECT_TRUE(err.ok()) << err.what();
			break;
		}
		case ConfigType::Namespace: {
			BaseApi::QueryResultsType results;
			auto err = api.reindexer->Select(
				Query(kConfigNamespace).Where("type", CondEq, "async_replication").Or().Where("type", CondEq, "replication"), results);
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
		namespaces.insert(std::string(ns));
	}
	std::vector<AsyncReplicationConfigTest::Node> followers;
	for (auto& node : asyncReplConf.nodes) {
		followers.emplace_back(AsyncReplicationConfigTest::Node{node.GetRPCDsn()});
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
	const auto dbPath = fs::JoinPath(config_.storagePath, config_.dbName);
	std::ofstream file(fs::JoinPath(dbPath, kClusterShardingFilename), std::ios_base::trunc);
	file << configYaml;
	file.flush();
}

std::string ServerControl::Interface::dumpUserRecYAML() const {
	YAML::Emitter res;
	res << YAML::BeginMap;
	for (const auto& [role, rec] : TestUserDataFactory::Get(config_.id)) {
		res << YAML::Key << rec.login << YAML::Value;
		res << YAML::BeginMap;
		res << YAML::Key << "hash" << YAML::Value << fmt::format("$1${}${}", "rdx_salt", reindexer::MD5crypt(rec.password, "rdx_salt"));
		res << YAML::Key << "roles" << YAML::Value;
		res << YAML::BeginMap;
		res << YAML::Key << YAML::Alias("") << YAML::Value << std::string(reindexer_server::UserRoleName(role));
		res << YAML::EndMap;
		res << YAML::EndMap;
	}
	res << YAML::EndMap;
	return res.c_str();
}

void ServerControl::Interface::WriteUsersYAMLFile(const std::string& usersYml) { WriteConfigFile(GetUsersYAMLFilePath(), usersYml); }

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
	auto item = api.reindexer->NewItem(kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(kJsonCfgProfiling);
	ASSERT_TRUE(err.ok()) << err.what();
	err = api.reindexer->Upsert(kConfigNamespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

cluster::ReplicationStats ServerControl::Interface::GetReplicationStats(std::string_view type) {
	Query qr = Query(kReplicationStatsNamespace).Where("type", CondEq, Variant(type));
	BaseApi::QueryResultsType res;
	auto err = api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	assertf(res.Count() == 1, "Qr.Count()=={}\n", res.Count());
	WrSerializer wser;
	err = res.begin().GetJSON(wser, false);
	EXPECT_TRUE(err.ok()) << err.what();
	cluster::ReplicationStats stats;
	err = stats.FromJSON(reindexer::giftStr(wser.Slice()));
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(stats.type, type);
	return stats;
}

std::string ServerControl::Interface::GetReplicationConfigFilePath() const {
	return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kReplicationConfigFilename);
}

std::string ServerControl::Interface::GetAsyncReplicationConfigFilePath() const {
	return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kAsyncReplicationConfigFilename);
}

std::string ServerControl::Interface::GetClusterConfigFilePath() const {
	return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kClusterConfigFilename);
}

std::string ServerControl::Interface::GetShardingConfigFilePath() const {
	return reindexer::fs::JoinPath(reindexer::fs::JoinPath(config_.storagePath, config_.dbName), kClusterShardingFilename);
}

std::string ServerControl::Interface::GetUsersYAMLFilePath() const {
	return reindexer::fs::JoinPath(config_.storagePath, kUsersYAMLFilename);
}

std::string ServerControl::Interface::getLogName(const std::string& log, bool core) {
	std::string name = getTestLogPath();
	name += (log + "_");
	if (!core) {
		name += std::to_string(config_.id);
	}
	name += ".log";
	return name;
}

ServerControl::Interface::Interface(std::atomic_bool& stopped, ServerControlConfig config, const YAML::Node& ReplicationConfig,
									const YAML::Node& ClusterConfig, const YAML::Node& ShardingConfig,
									const YAML::Node& AsyncReplicationConfig)
	: api(client::ReindexerConfig(10000, 0,
								  config.disableNetworkTimeout ? std::chrono::milliseconds(100000000) : std::chrono::milliseconds(0))),
	  stopped_(stopped),
	  config_(std::move(config)),
	  kRPCDsn(MakeDsn(reindexer_server::UserRole::kRoleDBAdmin, config_.id, config_.rpcPort, config_.dbName)) {
	std::string path = reindexer::fs::JoinPath(config_.storagePath, config_.dbName);
	if (reindexer::fs::MkDirAll(path) < 0) {
		assertf(false, "Unable to remove {}", path);
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
	if (WithSecurity()) {
		WriteUsersYAMLFile(dumpUserRecYAML());
	}
	Init();
}

ServerControl::Interface::Interface(std::atomic_bool& stopped, ServerControlConfig config)
	: api(client::ReindexerConfig(10000, 0,
								  config.disableNetworkTimeout ? std::chrono::milliseconds(100000000) : std::chrono::milliseconds(0))),
	  stopped_(stopped),
	  config_(std::move(config)),
	  kRPCDsn(MakeDsn(reindexer_server::UserRole::kRoleDBAdmin, config_.id, config_.rpcPort, config_.dbName)) {
	if (WithSecurity()) {
		std::string path = reindexer::fs::JoinPath(config_.storagePath, config_.dbName);
		if (reindexer::fs::MkDirAll(path) < 0) {
			assertf(false, "Unable to remove {}", path);
		}
		WriteUsersYAMLFile(dumpUserRecYAML());
	}
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
	for (const auto& p : {"httpaddr", "rpcaddr", "httpsaddr", "rpcsaddr"}) {
		y["net"][p] = "none";
	}
	y["net"][fmt::format("http{}addr", WithSecurity() ? "s" : "")] = "0.0.0.0:" + std::to_string(config_.httpPort);
	y["net"][fmt::format("rpc{}addr", WithSecurity() ? "s" : "")] = "0.0.0.0:" + std::to_string(config_.rpcPort);
	y["net"]["enable_cluster"] = true;
	y["net"]["security"] = WithSecurity();
	if (WithSecurity()) {
		y["net"]["ssl_cert"] = reindexer::fs::JoinPath(TLSPath(), kDefaultSSLCertFile);
		y["net"]["ssl_key"] = reindexer::fs::JoinPath(TLSPath(), kDefaultSSLKeyFile);
	}
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
	Error err;
	err = api.reindexer->Connect(kRPCDsn, client::ConnectOpts().CreateDBIfMissing());
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
	NsNamesHashSetT nss;
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
	asyncReplConf.selfReplToken = config.selfReplicationToken;
	auto err = checkSelfToken(asyncReplConf.selfReplToken);
	ASSERT_TRUE(err.ok()) << err.what();
	asyncReplConf.mode = cluster::AsyncReplConfigData::Str2mode(config.mode);
	for (auto& node : config.nodes) {
		asyncReplConf.nodes.emplace_back(cluster::AsyncReplNodeConfig{node.dsn});
		if (node.nsList.has_value()) {
			NsNamesHashSetT nss;
			for (auto& ns : node.nsList.value()) {
				nss.emplace(ns);
			}
			asyncReplConf.nodes.back().SetOwnNamespaceList(std::move(nss));
		}
	}

	ReplicationConfigData replConf;
	replConf.serverID = config.serverId;
	replConf.clusterID = 2;
	replConf.admissibleTokens = config.admissibleTokens;
	err = mergeAdmissibleTokens(replConf.admissibleTokens);
	ASSERT_TRUE(err.ok()) << err.what();

	upsertConfigItemFromObject(replConf);
	upsertConfigItemFromObject(asyncReplConf);
}

template <typename T>
T ServerControl::Interface::getConfigByType() const {
	const std::string type = std::is_same_v<T, reindexer::cluster::AsyncReplConfigData> ? "async_replication" : "replication";
	BaseApi::QueryResultsType qr;
	auto err = api.reindexer->Select(reindexer::Query(reindexer::kConfigNamespace).Where("type", CondEq, type), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 1);
	reindexer::WrSerializer ser;
	err = qr.begin().GetJSON(ser, false);
	EXPECT_TRUE(err.ok()) << err.what();
	T config;
	gason::JsonParser parser;
	err = config.FromJSON(parser.Parse(ser.Slice())[type]);
	EXPECT_TRUE(err.ok()) << err.what();
	return config;
}

template <typename T>
void ServerControl::Interface::UpdateConfigReplTokens(const T& tok) {
	constexpr bool isAdmissibleTokens = std::is_same_v<T, NsNamesHashMapT<std::string>>;
	using ConfigT = std::conditional_t<isAdmissibleTokens, ReplicationConfigData, reindexer::cluster::AsyncReplConfigData>;

	auto config = getConfigByType<ConfigT>();
	if constexpr (isAdmissibleTokens) {
		config.admissibleTokens = tok;
		auto err = mergeAdmissibleTokens(config.admissibleTokens);
		ASSERT_TRUE(err.ok()) << err.what();
	} else {
		config.selfReplToken = tok;
		auto err = checkSelfToken(config.selfReplToken);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	upsertConfigItemFromObject(config);
}
template void ServerControl::Interface::UpdateConfigReplTokens(const NsNamesHashMapT<std::string>&);
template void ServerControl::Interface::UpdateConfigReplTokens(const std::string&);

Error ServerControl::Interface::mergeAdmissibleTokens(reindexer::NsNamesHashMapT<std::string>& tokens) const {
	auto curConfig = getConfigByType<ReplicationConfigData>();

	for (const auto& [ns, token] : curConfig.admissibleTokens) {
		if (tokens.count(ns) != 0) {
			auto newToken = tokens.at(ns);
			if (newToken != token) {
				return Error(errParams, "Replication config contain another token '{}' for namespace '{}'. Got - '{}'", token,
							 std::string_view(ns), newToken);
			}
			continue;
		}
		tokens.insert({ns, token});
	}
	return {};
}

Error ServerControl::Interface::checkSelfToken(const std::string& token) const {
	auto config = getConfigByType<cluster::AsyncReplConfigData>();
	if (config.selfReplToken.empty()) {
		return {};
	}

	if (config.selfReplToken != token) {
		return Error(errParams, "Replication config contain another self replication token '{}'. Got - '{}'", config.selfReplToken, token);
	}
	return {};
}

void ServerControl::Interface::AddFollower(const ServerControl::Interface::Ptr& follower, std::optional<std::vector<std::string>>&& nsList,
										   cluster::AsyncReplicationMode replMode) {
	cluster::AsyncReplConfigData curConf = getConfigByType<cluster::AsyncReplConfigData>();
	if (curConf.role != cluster::AsyncReplConfigData::Role::Leader) {
		MakeLeader();
		curConf = getConfigByType<cluster::AsyncReplConfigData>();
	}
	cluster::AsyncReplNodeConfig newNode;
	newNode.SetRPCDsn(MakeDsn(reindexer_server::UserRole::kRoleReplication, follower));
	if (replMode != cluster::AsyncReplicationMode::Default) {
		newNode.SetReplicationMode(replMode);
	}
	auto found = std::find_if(curConf.nodes.begin(), curConf.nodes.end(),
							  [&newNode](const cluster::AsyncReplNodeConfig& node) { return node.GetRPCDsn() == newNode.GetRPCDsn(); });
	ASSERT_TRUE(found == curConf.nodes.end());
	curConf.nodes.emplace_back(std::move(newNode));
	if (nsList.has_value()) {
		NsNamesHashSetT nss;
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

	upsertConfigItemFromObject(curConf);
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

	api.UpsertJSON(kConfigNamespace, ser.Slice());
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
	ReplicationTestState state1, state2;
	while (true) {
		now += pause;
		const std::string tmStateToken1 = state1.tmStatetoken.has_value() ? std::to_string(state1.tmStatetoken.value()) : "<none>";
		const std::string tmStateToken2 = state2.tmStatetoken.has_value() ? std::to_string(state2.tmStatetoken.value()) : "<none>";
		const std::string tmVersion1 = state1.tmVersion.has_value() ? std::to_string(state1.tmVersion.value()) : "<none>";
		const std::string tmVersion2 = state2.tmVersion.has_value() ? std::to_string(state2.tmVersion.value()) : "<none>";
		ASSERT_TRUE(now < kMaxSyncTime) << fmt::format(
			"Wait sync is too long. s1 lsn: {}; s2 lsn: {}; s1 count: {}; s2 count: {}; s1 hash: {}; s2 hash: {}; s1 tm_token: {}; s2 "
			"tm_token: {}; s1 tm_version: {}; s2 tm_version: {}",
			state1.lsn, state2.lsn, state1.dataCount, state2.dataCount, state1.dataHash, state2.dataHash, tmStateToken1, tmStateToken2,
			tmVersion1, tmVersion2);
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

void ServerControl::Interface::ForceSync() {
	Error err;
	auto item = api.NewItem(kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(R"json({"type":"action","action":{"command":"restart_replication"}})json");
	ASSERT_TRUE(err.ok()) << err.what();
	api.Upsert(kConfigNamespace, item);
}

void ServerControl::Interface::ResetReplicationRole(std::string_view ns) {
	auto err = TryResetReplicationRole(ns);
	ASSERT_TRUE(err.ok()) << err.what();
}

Error ServerControl::Interface::TryResetReplicationRole(std::string_view ns) {
	BaseApi::QueryResultsType res;
	Error err;
	auto item = api.NewItem(kConfigNamespace);
	if (!item.Status().ok()) {
		return item.Status();
	}
	if (ns.size()) {
		err = item.FromJSON(
			fmt::format(R"json({{"type":"action","action":{{"command":"reset_replication_role","namespace":"{}"}}}})json", ns));
	} else {
		err = item.FromJSON(R"json({"type":"action","action":{"command":"reset_replication_role"}})json");
	}
	if (!err.ok()) {
		return err;
	}
	return api.reindexer->Upsert(kConfigNamespace, item);
}

void ServerControl::Interface::SetClusterLeader(int leaderId) {
	auto item = CreateClusterChangeLeaderItem(leaderId);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	api.Upsert(kConfigNamespace, item);
}

void ServerControl::Interface::SetReplicationLogLevel(LogLevel level, std::string_view type) {
	BaseApi::QueryResultsType res;
	auto item = api.NewItem(kConfigNamespace);
	auto err = item.Status();
	ASSERT_TRUE(err.ok()) << err.what();
	err = item.FromJSON(fmt::format(R"json({{"type":"action","action":{{"command":"set_log_level","type":"{}","level":"{}" }} }})json",
									type, reindexer::logLevelToString(level)));
	ASSERT_TRUE(err.ok()) << err.what();
	err = api.reindexer->Upsert(kConfigNamespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

BaseApi::ItemType ServerControl::Interface::CreateClusterChangeLeaderItem(int leaderId) {
	auto item = api.NewItem(kConfigNamespace);
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

	if (WithSecurity()) {
		paramArray.push_back("--ssl-cert=" + reindexer::fs::JoinPath(TLSPath(), kDefaultSSLCertFile));
		paramArray.push_back("--ssl-key=" + reindexer::fs::JoinPath(TLSPath(), kDefaultSSLKeyFile));
		paramArray.push_back("--security");
	}

	if (enableStats) {
		paramArray.push_back("--clientsstats");
	}
	paramArray.push_back("--loglevel=trace");
	paramArray.push_back("--rpclog=" + getLogName("rpc"));
	paramArray.push_back("--serverlog=" + getLogName("server"));
	// paramArray.push_back("--httplog");
	paramArray.push_back("--corelog=" + getLogName("core"));

	paramArray.push_back(fmt::format("--http{}addr=0.0.0.0:{}", WithSecurity() ? "s" : "", config_.httpPort));
	paramArray.push_back(fmt::format("--http{}addr=none", WithSecurity() ? "" : "s"));
	paramArray.push_back(fmt::format("--rpc{}addr=0.0.0.0:{}", WithSecurity() ? "s" : "", config_.rpcPort));
	paramArray.push_back(fmt::format("--rpc{}addr=none", WithSecurity() ? "" : "s"));
	paramArray.push_back("--enable-cluster");
	if (maxUpdatesSize) {
		paramArray.push_back("--updatessize=" + std::to_string(maxUpdatesSize));
	}

	return paramArray;
}

template <typename ValueT>
void ServerControl::Interface::upsertConfigItemFromObject(const ValueT& object) {
	const std::string type = std::is_same_v<ValueT, reindexer::cluster::AsyncReplConfigData> ? "async_replication" : "replication";

	WrSerializer ser;
	JsonBuilder jb(ser);
	jb.Put("type", type);
	{
		auto objBuilder = jb.Object(type);
		if constexpr (std::is_same_v<ValueT, cluster::AsyncReplConfigData>) {
			object.GetJSON(objBuilder, cluster::MaskingDSN::Disabled);
		} else {
			object.GetJSON(objBuilder);
		}
		objBuilder.End();
	}
	jb.End();

	api.UpsertJSON(kConfigNamespace, ser.Slice());
}
