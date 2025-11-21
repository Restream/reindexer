#include "cluster/config.h"

#include "core/cjson/jsonbuilder.h"
#include "core/defnsconfigs.h"
#include "core/indexdef.h"
#include "core/type_consts.h"
#include "gason/gason.h"
#include "tools/catch_and_return.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "vendor/urlparser/urlparser.h"
#include "yaml-cpp/yaml.h"

using namespace std::string_view_literals;

namespace reindexer::cluster {

static void ValidateDSN(const DSN& dsn) {
	if (dsn.Parser().scheme() != "cproto" && dsn.Parser().scheme() != "cprotos") {
		throw Error(errParams, "DSN must start with cproto:// or cprotos://. Actual DSN is {}", dsn);
	}
}

Error NodeData::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "NodeData: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return {};
}

Error NodeData::FromJSON(const gason::JsonNode& root) {
	try {
		serverId = root["server_id"].As<int>(serverId);
		electionsTerm = root["elections_term"].As<int>(electionsTerm);
		dsn = DSN(root["dsn"].As<std::string>());
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "NodeData: {}", ex.what());
	}
	return {};
}

void NodeData::GetJSON(JsonBuilder& jb) const {
	jb.Put("server_id", serverId);
	jb.Put("elections_term", electionsTerm);
	jb.Put("dsn", dsn);
}

void NodeData::GetJSON(WrSerializer& ser) const {
	JsonBuilder jb(ser);
	GetJSON(jb);
}

Error RaftInfo::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "RaftInfo: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return {};
}

Error RaftInfo::FromJSON(const gason::JsonNode& root) {
	try {
		leaderId = root["leader_id"].As<int>(leaderId);
		role = RoleFromStr(root["role"].As<std::string_view>());
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "RaftInfo: {}", ex.what());
	}
	return {};
}

void RaftInfo::GetJSON(JsonBuilder& jb) const {
	jb.Put("leader_id", leaderId);
	jb.Put("role", RoleToStr(role));
}

void RaftInfo::GetJSON(WrSerializer& ser) const {
	JsonBuilder jb(ser);
	GetJSON(jb);
}

std::string_view RaftInfo::RoleToStr(RaftInfo::Role role) {
	using namespace std::string_view_literals;
	switch (role) {
		case RaftInfo::Role::None:
			return "none"sv;
		case RaftInfo::Role::Leader:
			return "leader"sv;
		case RaftInfo::Role::Candidate:
			return "candidate"sv;
		case RaftInfo::Role::Follower:
			return "follower"sv;
		default:
			return "unknown"sv;
	}
}

RaftInfo::Role RaftInfo::RoleFromStr(std::string_view role) {
	using namespace std::string_view_literals;
	if (role == "leader"sv) {
		return RaftInfo::Role::Leader;
	} else if (role == "candidate"sv) {
		return RaftInfo::Role::Candidate;
	} else if (role == "follower"sv) {
		return RaftInfo::Role::Follower;
	} else {
		return RaftInfo::Role::None;
	}
}

void ClusterNodeConfig::FromYAML(const YAML::Node& root) {
	serverId = root["server_id"].as<int>(serverId);
	auto tmpDsn = DSN(root["dsn"].as<std::string>());
	ValidateDSN(tmpDsn);
	dsn = std::move(tmpDsn);
}

void AsyncReplNodeConfig::FromYAML(const YAML::Node& root) {
	auto tmpDsn = DSN(root[kAsyncReplicationDSNCfgName].as<std::string>());
	ValidateDSN(tmpDsn);
	dsn_ = std::move(tmpDsn);
	auto node = root["namespaces"];
	namespaces_.reset();
	if (node.IsSequence()) {
		NsNamesHashSetT nss;
		for (const auto& ns : node) {
			nss.emplace(ns.as<std::string>());
		}
		SetOwnNamespaceList(std::move(nss));
	}
	replicationMode_.reset();
	auto modeStr = root["replication_mode"].as<std::string>();
	if (!modeStr.empty()) {
		replicationMode_ = AsyncReplConfigData::Str2mode(modeStr);
	}
}

void AsyncReplNodeConfig::FromJSON(const gason::JsonNode& root) {
	auto tmpDsn = DSN(root[kAsyncReplicationDSNCfgName].As<std::string>());
	ValidateDSN(tmpDsn);
	dsn_ = std::move(tmpDsn);
	{
		auto& node = root["namespaces"];
		if (!node.empty()) {
			NsNamesHashSetT nss;
			for (auto& objNode : node) {
				nss.emplace(objNode.As<std::string>());
			}
			SetOwnNamespaceList(std::move(nss));
		}
	}
	replicationMode_.reset();
	auto modeStr = root["replication_mode"].As<std::string_view>();
	if (!modeStr.empty()) {
		replicationMode_ = AsyncReplConfigData::Str2mode(modeStr);
	}
}

void AsyncReplNodeConfig::GetJSON(JsonBuilder& jb, MaskingDSN maskingDSN) const {
	if (maskingDSN == MaskingDSN::Disabled) {
		jb.Put(kAsyncReplicationDSNCfgName, dsn_.dsn_);
	} else if (maskingDSN == MaskingDSN::Enabled) {
		jb.Put(kAsyncReplicationDSNCfgName, dsn_);
	}
	if (hasOwnNsList_) {
		auto arrNode = jb.Array("namespaces");
		for (const auto& ns : namespaces_->data) {
			arrNode.Put(TagName::Empty(), ns);
		}
	}
	if (replicationMode_.has_value()) {
		jb.Put("replication_mode", AsyncReplConfigData::Mode2str(replicationMode_.value()));
	}
}

void AsyncReplNodeConfig::GetYAML(YAML::Node& yaml) const {
	yaml[kAsyncReplicationDSNCfgName] = dsn_;
	if (hasOwnNsList_) {
		yaml["namespaces"] = YAML::Node(YAML::NodeType::Sequence);
		if (namespaces_ && !namespaces_->Empty()) {
			for (auto& ns : namespaces_->data) {
				yaml["namespaces"].push_back(std::string_view(ns));
			}
		}
	}
	if (replicationMode_.has_value()) {
		yaml["replication_mode"] = AsyncReplConfigData::Mode2str(replicationMode_.value());
	}
}

void AsyncReplNodeConfig::SetNamespaceListFromConfig(const AsyncReplConfigData& config) {
	assert(config.namespaces);
	namespaces_ = config.namespaces;
	hasOwnNsList_ = false;
}

Error ClusterConfigData::FromYAML(const std::string& yaml) {
	try {
		YAML::Node root = YAML::Load(yaml);
		appName = root["app_name"].as<std::string>(appName);
		replThreadsCount = root["sync_threads"].as<int>(replThreadsCount);
		parallelSyncsPerThreadCount = root["syncs_per_thread"].as<int>(parallelSyncsPerThreadCount);
		onlineUpdatesTimeoutSec = root["online_updates_timeout_sec"].as<int>(onlineUpdatesTimeoutSec);
		syncTimeoutSec = root["sync_timeout_sec"].as<int>(syncTimeoutSec);
		leaderSyncThreads = root["leader_sync_threads"].as<int>(leaderSyncThreads);
		leaderSyncConcurrentSnapshotsPerNode =
			root["leader_sync_concurrent_snapshots_per_node"].as<int>(leaderSyncConcurrentSnapshotsPerNode);
		retrySyncIntervalMSec = root["retry_sync_interval_msec"].as<int>(retrySyncIntervalMSec);
		enableCompression = root["enable_compression"].as<bool>(enableCompression);
		batchingRoutinesCount = root["batching_routines_count"].as<int>(batchingRoutinesCount);
		maxWALDepthOnForceSync = root["max_wal_depth_on_force_sync"].as<int>(maxWALDepthOnForceSync);
		proxyConnCount = root["proxy_conn_count"].as<int>(proxyConnCount);
		proxyConnConcurrency = root["proxy_conn_concurrency"].as<int>(proxyConnConcurrency);
		proxyConnThreads = root["proxy_conn_threads"].as<int>(proxyConnThreads);
		selfReplToken = root["self_replication_token"].as<std::string>();
		logLevel = logLevelFromString(root["log_level"].as<std::string>("info"));
		{
			auto node = root["namespaces"];
			namespaces.clear();
			for (const auto& ns : node) {
				namespaces.insert(NamespaceName(ns.as<std::string>()));
			}
		}
		{
			auto node = root["nodes"];
			nodes.clear();
			for (const auto& n : node) {
				ClusterNodeConfig conf;
				conf.FromYAML(n);
				nodes.emplace_back(std::move(conf));
			}
		}
		return {};
	} catch (const YAML::Exception& ex) {
		return Error(errParseYAML, "ClusterConfigData: yaml parsing error: '{}'", ex.what());
	} catch (const Error& err) {
		return err;
	}
}

Error AsyncReplConfigData::FromDefault() noexcept {
	try {
		gason::JsonParser parser;
		gason::JsonNode configJson = parser.Parse(kDefAsyncReplicationConfig);
		auto& asyncReplicationJson = configJson[kAsyncReplicationCfgName];
		if (!asyncReplicationJson.isObject()) {
			return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefAsyncReplicationConfig");
		}

		Error err = FromJSON(asyncReplicationJson);
		if (!err.ok()) {
			return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefAsyncReplicationConfig: {}", err.what());
		}
	}
	CATCH_AND_RETURN

	return {};
}

Error AsyncReplConfigData::FromYAML(const std::string& yaml) {
	try {
		YAML::Node root = YAML::Load(yaml);
		role = Str2role(root["role"].as<std::string>("none"));
		mode = Str2mode(root["replication_mode"].as<std::string>("default"));
		appName = root["app_name"].as<std::string>(appName);
		replThreadsCount = root["sync_threads"].as<int>(replThreadsCount);
		parallelSyncsPerThreadCount = root["syncs_per_thread"].as<int>(parallelSyncsPerThreadCount);
		onlineUpdatesTimeoutSec = root["online_updates_timeout_sec"].as<int>(onlineUpdatesTimeoutSec);
		syncTimeoutSec = root["sync_timeout_sec"].as<int>(syncTimeoutSec);
		forceSyncOnLogicError = root["force_sync_on_logic_error"].as<bool>(forceSyncOnLogicError);
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].as<bool>(forceSyncOnWrongDataHash);
		retrySyncIntervalMSec = root["retry_sync_interval_msec"].as<int>(retrySyncIntervalMSec);
		enableCompression = root["enable_compression"].as<bool>(enableCompression);
		batchingRoutinesCount = root["batching_routines_count"].as<int>(batchingRoutinesCount);
		maxWALDepthOnForceSync = root["max_wal_depth_on_force_sync"].as<int>(maxWALDepthOnForceSync);
		onlineUpdatesDelayMSec = root["online_updates_delay_msec"].as<int>(onlineUpdatesDelayMSec);
		logLevel = logLevelFromString(root["log_level"].as<std::string>("info"));
		selfReplToken = root["self_replication_token"].as<std::string>();
		{
			auto node = root["namespaces"];
			NsNamesHashSetT nss;
			for (const auto& n : node) {
				nss.emplace(n.as<std::string>());
			}
			namespaces = make_intrusive<NamespaceList>(std::move(nss));
		}
		{
			auto node = root["nodes"];
			nodes.clear();
			for (const auto& n : node) {
				AsyncReplNodeConfig conf;
				conf.FromYAML(n);
				if (!conf.HasOwnNsList()) {
					conf.SetNamespaceListFromConfig(*this);
				}
				nodes.emplace_back(std::move(conf));
			}
		}
		return {};
	} catch (const YAML::Exception& ex) {
		return Error(errParseYAML, "AsyncReplConfigData: yaml parsing error: '{}'", ex.what());
	} catch (const Error& err) {
		return err;
	}
}

Error AsyncReplConfigData::FromJSON(std::string_view json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "AsyncReplConfigData: {}", ex.what());
	}
}

Error AsyncReplConfigData::FromJSON(const gason::JsonNode& root) {
	using namespace std::string_view_literals;
	std::string errorString;

	{
		if (std::string roleStr = Role2str(role); tryReadOptionalJsonValue(&errorString, root, "role"sv, roleStr).ok()) {
			role = Str2role(roleStr);
		}

		if (std::string modeStr = Mode2str(mode); tryReadOptionalJsonValue(&errorString, root, "replication_mode"sv, modeStr).ok()) {
			try {
				mode = Str2mode(modeStr);
			} catch (Error& err) {
				ensureEndsWith(errorString, "\n") += err.what();
			}
		}

		auto err = tryReadOptionalJsonValue(&errorString, root, "app_name"sv, appName);

		err = tryReadOptionalJsonValue(&errorString, root, "sync_threads"sv, replThreadsCount);
		err = tryReadOptionalJsonValue(&errorString, root, "syncs_per_thread"sv, parallelSyncsPerThreadCount);
		err = tryReadOptionalJsonValue(&errorString, root, "online_updates_timeout_sec"sv, onlineUpdatesTimeoutSec);
		err = tryReadOptionalJsonValue(&errorString, root, "sync_timeout_sec"sv, syncTimeoutSec);
		err = tryReadOptionalJsonValue(&errorString, root, "force_sync_on_logic_error"sv, forceSyncOnLogicError);
		err = tryReadOptionalJsonValue(&errorString, root, "force_sync_on_wrong_data_hash"sv, forceSyncOnWrongDataHash);
		err = tryReadOptionalJsonValue(&errorString, root, "retry_sync_interval_msec"sv, retrySyncIntervalMSec);
		err = tryReadOptionalJsonValue(&errorString, root, "enable_compression"sv, enableCompression);
		err = tryReadOptionalJsonValue(&errorString, root, "batching_routines_count"sv, batchingRoutinesCount);
		err = tryReadOptionalJsonValue(&errorString, root, "max_wal_depth_on_force_sync"sv, maxWALDepthOnForceSync);
		err = tryReadOptionalJsonValue(&errorString, root, "online_updates_delay_msec"sv, onlineUpdatesDelayMSec);
		err = tryReadOptionalJsonValue(&errorString, root, "self_replication_token"sv, selfReplToken);
		(void)err;	// Read errors do not matter here

		if (std::string_view levelStr = logLevelToString(logLevel);
			tryReadOptionalJsonValue(&errorString, root, "log_level"sv, levelStr).ok()) {
			logLevel = logLevelFromString(levelStr);
		}
	}

	try {
		NsNamesHashSetT nss;
		for (auto& objNode : root["namespaces"]) {
			nss.emplace(objNode.As<std::string>());
		}
		namespaces = make_intrusive<NamespaceList>(std::move(nss));
	} catch (const Error& err) {
		ensureEndsWith(errorString, "\n") += err.what();
	} catch (const gason::Exception& ex) {
		ensureEndsWith(errorString, "\n") += ex.what();
	}

	try {
		nodes.clear();
		for (auto& objNode : root[kAsyncReplicationNodesCfgName]) {
			AsyncReplNodeConfig conf;
			conf.FromJSON(objNode);
			if (!conf.HasOwnNsList()) {
				conf.SetNamespaceListFromConfig(*this);
			}
			nodes.emplace_back(std::move(conf));
		}
	} catch (const Error& err) {
		ensureEndsWith(errorString, "\n") += err.what();
	} catch (const gason::Exception& ex) {
		ensureEndsWith(errorString, "\n") += ex.what();
	}

	if (!errorString.empty()) {
		return Error(errParseJson, "AsyncReplConfigData: JSON parsing error: '{}'", errorString);
	}
	return {};
}

void AsyncReplConfigData::GetJSON(JsonBuilder& jb, MaskingDSN maskingDSN) const {
	jb.Put("role", Role2str(role));
	jb.Put("replication_mode", Mode2str(mode));
	jb.Put("app_name", appName);
	jb.Put("sync_threads", replThreadsCount);
	jb.Put("syncs_per_thread", parallelSyncsPerThreadCount);
	jb.Put("sync_timeout_sec", syncTimeoutSec);
	jb.Put("online_updates_timeout_sec", onlineUpdatesTimeoutSec);
	jb.Put("enable_compression", enableCompression);
	jb.Put("force_sync_on_logic_error", forceSyncOnLogicError);
	jb.Put("force_sync_on_wrong_data_hash", forceSyncOnWrongDataHash);
	jb.Put("retry_sync_interval_msec", retrySyncIntervalMSec);
	jb.Put("batching_routines_count", batchingRoutinesCount);
	jb.Put("max_wal_depth_on_force_sync", maxWALDepthOnForceSync);
	jb.Put("online_updates_delay_msec", onlineUpdatesDelayMSec);
	jb.Put("self_replication_token", selfReplToken);
	jb.Put("log_level", logLevelToString(logLevel));
	{
		auto arrNode = jb.Array("namespaces");
		for (const auto& ns : namespaces->data) {
			arrNode.Put(TagName::Empty(), ns);
		}
	}
	{
		auto arrNode = jb.Array("nodes");
		for (const auto& node : nodes) {
			auto obj = arrNode.Object();
			node.GetJSON(obj, maskingDSN);
		}
	}
}

void AsyncReplConfigData::GetYAML(WrSerializer& ser) const {
	YAML::Node nss;
	nss["namespaces"] = YAML::Node(YAML::NodeType::Sequence);
	if (namespaces && !namespaces->Empty()) {
		for (auto& ns : namespaces->data) {
			nss["namespaces"].push_back(std::string_view(ns));
		}
	}
	YAML::Node nds;
	nds["nodes"] = YAML::Node(YAML::NodeType::Sequence);
	if (!nodes.empty()) {
		for (auto& node : nodes) {
			YAML::Node r;
			node.GetYAML(r);
			nds["nodes"].push_back(r);
		}
	}
	// clang-format off
	ser <<	"# Replication role. May be one of\n"
			"# none - replication is disabled;\n"
			"# follower - replication as follower;\n"
			"# leader - replication as leader.\n"
			"role: " + Role2str(role) + "\n"
			"\n"
			"# Replication mode. Allows to configure async replication from sync raft-cluster. This option may be set for each target node individually or globally for all the nodes from config.\n"
			"# Possible values:\n"
			"# default - async replication from this node is always enabled, if there are any target nodes to replicate on;\n"
			"# from_sync_leader - async replication will be enabled only when current node is synchronous RAFT-cluster leader (or if this node does not have any sync cluster config)\n"
			"mode: " + Mode2str(mode) + "\n"
			"\n"
			"# Application name used by replicator as login tag\n"
			"app_name: " + appName + "\n"
			"\n"
			"# Force resync on logic error conditions\n"
			"force_sync_on_logic_error: " + (forceSyncOnLogicError ? "true" : "false") + "\n"
			"\n"
			"# Node response timeout for online-replication (seconds)\n"
			"online_updates_timeout_sec: " + std::to_string(onlineUpdatesTimeoutSec) + "\n"
			"\n"
			"# Node response timeout for wal/force syncs (seconds)\n"
			"sync_timeout_sec: " + std::to_string(syncTimeoutSec) + "\n"
			"\n"
			"# Force resync on wrong data hash conditions\n"
			"force_sync_on_wrong_data_hash: " + (forceSyncOnWrongDataHash ? "true" : "false") + "\n"
			"\n"
			"# Resync timeout on network errors\n"
			"retry_sync_interval_msec: " + std::to_string(retrySyncIntervalMSec) + "\n"
			"\n"
			"# Number of data replication threads\n"
			"sync_threads: " + std::to_string(replThreadsCount) + "\n"
			"\n"
			"# Max number of concurrent force/wal sync's per thread\n"
			"syncs_per_thread: " +  std::to_string(parallelSyncsPerThreadCount) + "\n"
			"\n"
			"# Number of coroutines for updates batching (per namespace). Higher value here may help to reduce\n"
			"# networks trip-around await time, but will require more RAM\n"
			"batching_routines_count: " + std::to_string(batchingRoutinesCount) + "\n"
			"\n"
			"# Maximum number of WAL-records, which may be gained from force-sync.\n"
			"# Increasing this value may help to avoid force-syncs after leader's switch, however it also increases RAM consumption during syncs\n"
			"max_wal_depth_on_force_sync: " + std::to_string(maxWALDepthOnForceSync) + "\n"
			"\n"
			"# Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering, but also will provide\n"
			"# more effective network batching and CPU utilization\n"
			"# 0 - disables additional delay\n"
			"online_updates_delay_msec: " + std::to_string(onlineUpdatesDelayMSec) + "\n"
			"# Replication log level on replicator's startup. May be changed either via this config (with replication restart) or via config-action\n"
			"# (upsert '{ \"type\":\"action\", \"action\": { \"command\": \"set_log_level\", \"type\": \"async_replication\", \"level\": \"info\" } }' into #config-namespace).\n"
			"# Possible values: none, error, warning, info, trace.\n"
			"log_level: " + std::string(logLevelToString(logLevel)) + "\n"
			"# Synchronization token for asynchronous replication\n"
			+ selfReplToken + "\n"
			"# List of namespaces for replication. If empty, all namespaces\n"
			"# All replicated namespaces will become read only for followers\n"
			"# It should be written as YAML sequence, JSON-style arrays are not supported\n"
			+ YAML::Dump(nss) + "\n"
			"# List of nodes for replication.\n"
			"# It should be written as YAML sequence, JSON-style arrays are not supported\n"
			+ YAML::Dump(nds) + "\n\n";
	// clang-format on
}

AsyncReplConfigData::Role AsyncReplConfigData::Str2role(std::string_view role) noexcept {
	using namespace std::string_view_literals;
	if (role == "leader"sv) {
		return Role::Leader;
	}
	if (role == "follower"sv) {
		return Role::Follower;
	}
	return Role::None;
}

std::string AsyncReplConfigData::Role2str(AsyncReplConfigData::Role role) noexcept {
	switch (role) {
		case Role::Leader:
			return "leader";
		case Role::Follower:
			return "follower";
		case Role::None:
			return "none";
		default:
			std::abort();
	}
}

AsyncReplicationMode AsyncReplConfigData::Str2mode(std::string_view mode) {
	using namespace std::string_view_literals;
	if (mode == "from_sync_leader"sv) {
		return AsyncReplicationMode::FromClusterLeader;
	}
	if (mode == "default"sv || mode.empty()) {
		return AsyncReplicationMode::Default;
	}
	throw Error(errParams, "Unexpected replication mode value: '{}'", mode);
}

std::string AsyncReplConfigData::Mode2str(AsyncReplicationMode mode) noexcept {
	switch (mode) {
		case AsyncReplicationMode::Default:
			return "default";
		case AsyncReplicationMode::FromClusterLeader:
			return "from_sync_leader";
		default:
			std::abort();
	}
}

Error ShardingConfig::Namespace::FromYAML(const YAML::Node& yaml, const std::map<int, std::vector<DSN>>& shards) {
	if (!yaml["namespace"].IsScalar()) {
		return Error(errParams, "'namespace' node must be scalar.");
	}

	ns = yaml["namespace"].as<std::string>();
	if (!validateObjectName(ns, false)) {
		return Error(errParams, "Namespace name incorrect '{}'.", ns);
	}
	if (!yaml["index"].IsScalar()) {
		return Error(errParams, "'index' node must be scalar.");
	}

	index = yaml["index"].as<std::string>();
	if (!validateIndexName(index, IndexCompositeHash)) {
		return Error(errParams, "Index name incorrect '{}'.", index);
	}
	const auto defaultShardNode = yaml["default_shard"];
	if (!defaultShardNode.IsDefined()) {
		return Error(errParams, "Default shard id is not specified for namespace '{}'", ns);
	}
	defaultShard = defaultShardNode.as<int>();
	auto keysNode = yaml["keys"];
	keys.clear();
	keys.resize(keysNode.size());
	std::vector<sharding::Segment<Variant>> checkVal;
	KeyValueType valuesType(KeyValueType::Null{});
	for (size_t i = 0; i < keys.size(); ++i) {
		Error err = keys[i].FromYAML(keysNode[i], shards, valuesType, checkVal);
		if (!err.ok()) {
			return err;
		}
	}
	return {};
}

Error ShardingConfig::Namespace::FromJSON(const gason::JsonNode& root) {
	try {
		ns = root["namespace"].As<std::string>();
		if (!validateObjectName(ns, false)) {
			return Error(errParams, "Namespace name incorrect '{}'.", ns);
		}
		defaultShard = root["default_shard"].As<int>();
		index = root["index"].As<std::string>();
		if (!validateIndexName(index, IndexCompositeHash)) {
			return Error(errParams, "Index name incorrect '{}'.", index);
		}
		const auto& keysNode = root["keys"];
		keys.clear();
		std::vector<sharding::Segment<Variant>> checkVal;
		KeyValueType valuesType(KeyValueType::Null{});
		for (const auto& kNode : keysNode) {
			keys.emplace_back();
			Error err = keys.back().FromJSON(kNode, valuesType, checkVal);
			if (!err.ok()) {
				return err;
			}
		}
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "ShardingConfig::Namespace: {}", ex.what());
	}
	return {};
}

Error ShardingConfig::Key::checkValue(const sharding::Segment<Variant>& val, KeyValueType& valuesType,
									  const std::vector<sharding::Segment<Variant>>& checkVal) {
	if (valuesType.Is<KeyValueType::Null>()) {
		valuesType = val.left.Type();
	} else if (!valuesType.IsSame(val.left.Type())) {
		return Error(errParams, "Incorrect value '{}'. Type of first value is '{}', current type is '{}'", val.left.As<std::string>(),
					 valuesType.Name(), val.left.Type().Name());
	}
	if (sharding::intersected(checkVal, val)) {
		return Error(errParams, "Incorrect value '{}'. Value already in use.",
					 val.left == val.right ? val.left.As<std::string>()
										   : fmt::format("[{}, {}]", val.left.As<std::string>(), val.right.As<std::string>()));
	}
	return {};
}

sharding::Segment<Variant> ShardingConfig::Key::SegmentFromYAML(const YAML::Node& yaml, int shardId) {
	switch (yaml.Type()) {
		case YAML::NodeType::Scalar: {
			auto val = stringToVariant(yaml.as<std::string>());

			if (val.Type().Is<KeyValueType::Null>()) {
				throw Error(errParams, "Incorrect value '{}'. Type is equal to 'KeyValueNull'", yaml.as<std::string>());
			}

			return sharding::Segment<Variant>{val, val};
		}
		case YAML::NodeType::Sequence: {
			algorithmType = ByRange;
			if (auto dist = std::distance(std::begin(yaml), std::end(yaml)); dist != 2) {
				throw Error(errParams, "Incorrect range for sharding key. Should contain 2 numbers but {} are received", dist);
			}

			Variant left, right;
			auto getVariant = [&yaml](const std::string& str) {
				auto val = stringToVariant(str);
				if (val.Type().Is<KeyValueType::Null>()) {
					throw Error(errParams, "Incorrect value '{}'. Type is equal to 'KeyValueNull'", yaml.as<std::string>());
				}
				return val;
			};

			left = getVariant(std::begin(yaml)->as<std::string>());
			right = getVariant(std::next(std::begin(yaml))->as<std::string>());

			if (!left.Type().IsSame(right.Type())) {
				throw Error(errParams, "Incorrect segment '[{}, {}]'. Type of left value is '{}', right type is '{}'",
							left.As<std::string>(), right.As<std::string>(), left.Type().Name(), right.Type().Name());
			}

			return sharding::Segment<Variant>{left, right};
		}
		case YAML::NodeType::Undefined:
		case YAML::NodeType::Null:
		case YAML::NodeType::Map:
		default:
			throw Error(errParams, "Incorrect YAML format for values config for shard id: {}", shardId);
	}
}

sharding::Segment<Variant> ShardingConfig::Key::SegmentFromJSON(const gason::JsonNode& json, int shardId) {
	const auto& jsonValue = json.value;
	switch (jsonValue.getTag()) {
		case gason::JsonTag::JTRUE:
		case gason::JsonTag::JFALSE:
		case gason::JsonTag::STRING:
		case gason::JsonTag::DOUBLE:
		case gason::JsonTag::NUMBER: {
			auto val = stringToVariant(stringifyJson(json, false));

			if (val.Type().Is<KeyValueType::Null>()) {
				throw Error(errParams, "Incorrect value '{}'. Type is equal to 'KeyValueNull'", stringifyJson(json, false));
			}

			return sharding::Segment<Variant>{val, val};
		}
		case gason::JsonTag::OBJECT: {
			algorithmType = ByRange;
			const auto& range = json["range"];
			if (auto dist = std::distance(begin(range), end(range)); dist != 2) {
				throw Error(errParams, "Incorrect range for sharding key. Should contain 2 numbers but {} are received", dist);
			}

			auto left = stringToVariant(stringifyJson(*begin(range), false));
			auto right = stringToVariant(stringifyJson(*begin(range)->next, false));

			if (!left.Type().IsSame(right.Type())) {
				throw Error(errParams, "Incorrect segment '[{}, {}]'. Type of left value is '{}', right type is '{}'",
							left.As<std::string>(), right.As<std::string>(), left.Type().Name(), right.Type().Name());
			}

			return sharding::Segment<Variant>{std::move(left), std::move(right)};
		}
		case gason::JsonTag::ARRAY:
		case gason::JsonTag::JSON_NULL:
		case gason::JsonTag::EMPTY:
		default:
			throw Error(errParams, "Incorrect JSON format for values config for shard id: {}", shardId);
	}
}

Error ShardingConfig::Key::FromYAML(const YAML::Node& yaml, const std::map<int, std::vector<DSN>>& _shards, KeyValueType& valuesType,
									std::vector<sharding::Segment<Variant>>& checkVal) {
	values.clear();
	const auto& shardIdNode = yaml["shard_id"];
	if (!shardIdNode.IsDefined()) {
		return Error(errParams, "Shard id should be specified for every namespace's keys");
	}
	shardId = shardIdNode.as<int>();
	if (_shards.find(shardId) == _shards.cend()) {
		return Error(errParams, "Shard id {} is not specified in the config but it is used in namespace keys", shardId);
	}

	auto valuesNode = yaml["values"];
	if (valuesNode.IsDefined()) {
		algorithmType = ByValue;
		for (const auto& value : valuesNode) {
			try {
				auto segment = SegmentFromYAML(value, shardId);
				Error err = checkValue(segment, valuesType, checkVal);
				if (!err.ok()) {
					return err;
				}
				values.push_back(std::move(segment));
			} catch (const Error& err) {
				return err;
			}
		}
		values = sharding::getUnion(values);
		checkVal.insert(checkVal.end(), values.begin(), values.end());
	} else {
		return Error(errParams, "Unsupported sharding algorithm type: neither values nor range are specified");
	}

	return {};
}

Error ShardingConfig::Key::FromJSON(const gason::JsonNode& root, KeyValueType& valuesType,
									std::vector<sharding::Segment<Variant>>& checkVal) {
	try {
		shardId = root["shard_id"].As<int>();
		values.clear();
		const auto& valuesNode = root["values"];
		if (!valuesNode.empty()) {
			algorithmType = ByValue;
			for (const auto& vNode : valuesNode) {
				try {
					auto segment = SegmentFromJSON(vNode, shardId);
					Error err = checkValue(segment, valuesType, checkVal);
					if (!err.ok()) {
						return err;
					}
					values.push_back(std::move(segment));
				} catch (const Error& err) {
					return err;
				}
			}
			values = sharding::getUnion(values);
			checkVal.insert(checkVal.end(), values.begin(), values.end());
		} else {
			return Error(errParams, "Unsupported sharding algorithm type: neither values nor range are specified");
		}

	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "ShardingConfig::Key: {}", ex.what());
	}
	return {};
}

void ShardingConfig::Namespace::GetYAML(YAML::Node& yaml) const {
	YAML::Node nsNode;
	nsNode["namespace"] = ns;
	nsNode["default_shard"] = defaultShard;
	nsNode["index"] = index;
	YAML::Node keysNode(YAML::NodeType::Sequence);
	for (const auto& key : keys) {
		key.GetYAML(keysNode);
	}
	nsNode["keys"] = keysNode;
	yaml.push_back(nsNode);
}

void ShardingConfig::Namespace::GetJSON(JsonBuilder& jb) const {
	jb.Put("namespace", ns);
	jb.Put("default_shard", defaultShard);
	jb.Put("index", index);
	auto keysNode = jb.Array("keys");
	for (const auto& key : keys) {
		auto kNode = keysNode.Object();
		key.GetJSON(kNode);
	}
}

void ShardingConfig::Key::GetYAML(YAML::Node& yaml) const {
	YAML::Node key;
	key["shard_id"] = shardId;
	YAML::Node valuesNode(YAML::NodeType::Sequence);
	for (const auto& [left, right, _] : values) {
		(void)_;
		if (left == right) {
			valuesNode.push_back(left.As<std::string>());
		} else {
			YAML::Node bounds(YAML::NodeType::Sequence);
			bounds.SetStyle(YAML::EmitterStyle::Flow);
			bounds.push_back(left.As<std::string>());
			bounds.push_back(right.As<std::string>());
			valuesNode.push_back(bounds);
		}
	}
	key["values"] = valuesNode;
	yaml.push_back(key);
}

void ShardingConfig::Key::GetJSON(JsonBuilder& jb) const {
	jb.Put("shard_id", shardId);
	auto valuesNode = jb.Array("values");
	for (const auto& [left, right, _] : values) {
		(void)_;
		if (left == right) {
			valuesNode.Put(TagName::Empty(), left);
		} else {
			auto segmentNodeObj = valuesNode.Object();
			auto segmentNodeArr = segmentNodeObj.Array("range");
			segmentNodeArr.Put(TagName::Empty(), left);
			segmentNodeArr.Put(TagName::Empty(), right);
		}
	}
}

ComparationResult ShardingConfig::Key::RelaxCompare(const std::vector<sharding::Segment<Variant>>& other,
													const CollateOpts& collateOpts) const {
	auto lhsIt{values.cbegin()}, rhsIt{other.cbegin()};
	const auto lhsEnd{values.cend()}, rhsEnd{other.cend()};
	for (; lhsIt != lhsEnd && rhsIt != rhsEnd; ++lhsIt, ++rhsIt) {
		auto res = lhsIt->left.RelaxCompare<WithString::Yes, NotComparable::Throw, NullsHandling::NotComparable>(rhsIt->left, collateOpts);
		if (res != ComparationResult::Eq) {
			return res;
		}
		res = lhsIt->right.RelaxCompare<WithString::Yes, NotComparable::Throw, NullsHandling::NotComparable>(rhsIt->right, collateOpts);
		if (res != ComparationResult::Eq) {
			return res;
		}
	}
	if (lhsIt == lhsEnd) {
		return (rhsIt == rhsEnd) ? ComparationResult::Eq : ComparationResult::Lt;
	} else {
		return ComparationResult::Gt;
	}
}

Error ShardingConfig::FromYAML(const std::string& yaml) {
	namespaces.clear();
	shards.clear();

	try {
		YAML::Node root = YAML::Load(yaml);
		const auto versionNode = root["version"];
		if (!versionNode.IsDefined()) {
			return Error(errParams, "Version of sharding config file is not specified");
		}
		if (const int v{versionNode.as<int>()}; v != 1) {
			return Error(errParams, "Unsupported version of sharding config file: {}", v);
		}

		auto shardsNode = root[kShardingShardsCfgName];
		for (const auto& shNode : shardsNode) {
			const size_t shardId = shNode["shard_id"].as<int>();
			if (shards.find(shardId) != shards.end()) {
				return Error{errParams, "Dsns for shard id {} are specified twice", shardId};
			}
			const auto& hostsNode = shNode[kShardingDSNsCfgName];
			auto& shard = shards[shardId];
			shard.reserve(hostsNode.size());
			for (const auto& host : hostsNode) {
				shard.emplace_back(host.as<std::string>());
			}
		}

		auto namespacesNodes = root["namespaces"];
		namespaces.resize(namespacesNodes.size());
		for (size_t i = 0; i < namespaces.size(); ++i) {
			Error err = namespaces[i].FromYAML(namespacesNodes[i], shards);
			if (!err.ok()) {
				return err;
			}
			const std::string& newNsName = namespaces[i].ns;
			if (i > 0 && std::find_if(namespaces.begin(), namespaces.begin() + i,
									  [&newNsName](const Namespace& v) { return iequals(v.ns, newNsName); }) != namespaces.begin() + i) {
				return Error(errParams, "Namespace '{}' already specified in the config.", newNsName);
			}
		}

		thisShardId = root["this_shard_id"].as<int>();
		reconnectTimeout = std::chrono::milliseconds(root["reconnect_timeout_msec"].as<int>(reconnectTimeout.count()));
		shardsAwaitingTimeout = std::chrono::seconds(root["shards_awaiting_timeout_sec"].as<int>(shardsAwaitingTimeout.count()));
		configRollbackTimeout = std::chrono::seconds(root["config_rollback_timeout_sec"].as<int>(configRollbackTimeout.count()));
		proxyConnCount = root["proxy_conn_count"].as<int>(proxyConnCount);
		proxyConnConcurrency = root["proxy_conn_concurrency"].as<int>(proxyConnConcurrency);
		proxyConnThreads = root["proxy_conn_threads"].as<int>(proxyConnThreads);
		sourceId = root["source_id"].as<int64_t>(ShardingSourceId::NotSet);
		return Validate();
	} catch (const YAML::Exception& ex) {
		return Error(errParseYAML, "yaml parsing error: '{}'", ex.what());
	} catch (const Error& err) {
		return err;
	}
}

Error ShardingConfig::FromJSON(std::string_view json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "ShardingConfig: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
}

Error ShardingConfig::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "ShardingConfig: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
}

Error ShardingConfig::FromJSON(const gason::JsonNode& root) {
	try {
		const int v = root["version"].As<int>();
		if (v != 1) {
			return Error(errParams, "Unsupported version of sharding config file: {}", v);
		}
		namespaces.clear();
		const auto& namespacesNode = root["namespaces"];
		for (const auto& nsNode : namespacesNode) {
			namespaces.emplace_back();
			Error err = namespaces.back().FromJSON(nsNode);
			if (!err.ok()) {
				return err;
			}
			const std::string& newNsName = namespaces.back().ns;
			if (namespaces.size() > 1 && std::find_if(namespaces.begin(), namespaces.end() - 1, [&newNsName](const Namespace& v) {
											 return iequals(v.ns, newNsName);
										 }) != namespaces.end() - 1) {
				return Error(errParams, "Namespace '{}' already specified in the config.", newNsName);
			}
		}
		shards.clear();
		const auto& shardsNode = root[kShardingShardsCfgName];
		for (const auto& shrdNode : shardsNode) {
			const int shardId = shrdNode["shard_id"].As<int>();
			if (shards.find(shardId) != shards.end()) {
				return Error{errParams, "Dsns for shard id {} are specified twice", shardId};
			}
			const auto& dsnsNode = shrdNode[kShardingDSNsCfgName];
			for (const auto& dNode : dsnsNode) {
				shards[shardId].emplace_back(dNode.As<std::string>());
			}
		}
		thisShardId = root["this_shard_id"].As<int>();
		reconnectTimeout = std::chrono::milliseconds(root["reconnect_timeout_msec"].As<int>(reconnectTimeout.count()));
		shardsAwaitingTimeout = std::chrono::seconds(root["shards_awaiting_timeout_sec"].As<int>(shardsAwaitingTimeout.count()));
		configRollbackTimeout = std::chrono::seconds(root["config_rollback_timeout_sec"].As<int>(configRollbackTimeout.count()));
		proxyConnCount = root["proxy_conn_count"].As<int>(proxyConnCount);
		proxyConnConcurrency = root["proxy_conn_concurrency"].As<int>(proxyConnConcurrency);
		proxyConnThreads = root["proxy_conn_threads"].As<int>(proxyConnThreads);
		sourceId = root["source_id"].As<int64_t>(ShardingSourceId::NotSet);
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "NodeData: {}", ex.what());
	}
	return Validate();
}

bool operator==(const ShardingConfig& lhs, const ShardingConfig& rhs) {
	return lhs.namespaces == rhs.namespaces && lhs.thisShardId == rhs.thisShardId && lhs.shards == rhs.shards &&
		   lhs.reconnectTimeout == rhs.reconnectTimeout && lhs.shardsAwaitingTimeout == rhs.shardsAwaitingTimeout &&
		   lhs.configRollbackTimeout == rhs.configRollbackTimeout && lhs.proxyConnCount == rhs.proxyConnCount &&
		   lhs.proxyConnConcurrency == rhs.proxyConnConcurrency && rhs.proxyConnThreads == lhs.proxyConnThreads &&
		   rhs.sourceId == lhs.sourceId;
}
bool operator==(const ShardingConfig::Key& lhs, const ShardingConfig::Key& rhs) {
	return lhs.shardId == rhs.shardId && lhs.algorithmType == rhs.algorithmType && lhs.RelaxCompare(rhs.values) == ComparationResult::Eq;
}
bool operator==(const ShardingConfig::Namespace& lhs, const ShardingConfig::Namespace& rhs) {
	return lhs.ns == rhs.ns && lhs.defaultShard == rhs.defaultShard && lhs.index == rhs.index && lhs.keys == rhs.keys;
}

std::string ShardingConfig::GetYAML() const { return YAML::Dump(GetYAMLObj()) + '\n'; }

YAML::Node ShardingConfig::GetYAMLObj() const {
	YAML::Node yaml;
	yaml["version"] = 1;
	{
		yaml["namespaces"] = YAML::Node(YAML::NodeType::Sequence);
		auto nssNode = yaml["namespaces"];
		for (const auto& ns : namespaces) {
			ns.GetYAML(nssNode);
		}
	}
	{
		yaml[kShardingShardsCfgName] = YAML::Node(YAML::NodeType::Sequence);
		auto shardsNode = yaml[kShardingShardsCfgName];
		for (const auto& [id, dsns] : shards) {
			YAML::Node n;
			n["shard_id"] = id;
			n[kShardingDSNsCfgName] = YAML::Node(YAML::NodeType::Sequence);
			for (const auto& dsn : dsns) {
				n[kShardingDSNsCfgName].push_back(dsn);
			}
			shardsNode.push_back(n);
		}
	}
	yaml["this_shard_id"] = thisShardId;
	yaml["reconnect_timeout_msec"] = reconnectTimeout.count();
	yaml["shards_awaiting_timeout_sec"] = shardsAwaitingTimeout.count();
	yaml["config_rollback_timeout_sec"] = configRollbackTimeout.count();
	yaml["proxy_conn_count"] = proxyConnCount;
	yaml["proxy_conn_concurrency"] = proxyConnConcurrency;
	yaml["proxy_conn_threads"] = proxyConnThreads;
	if (sourceId != ShardingSourceId::NotSet) {
		yaml["source_id"] = sourceId;
	}
	return yaml;
}

std::string ShardingConfig::GetJSON(MaskingDSN masking) const {
	WrSerializer ser;
	GetJSON(ser, masking);
	return std::string{ser.Slice()};
}

void ShardingConfig::GetJSON(WrSerializer& ser, MaskingDSN masking) const {
	JsonBuilder jb(ser);
	GetJSON(jb, masking);
}

void ShardingConfig::GetJSON(JsonBuilder& jb, MaskingDSN masking) const {
	jb.Put("version", 1);
	{
		auto namespacesNode = jb.Array("namespaces");
		for (const auto& ns : namespaces) {
			auto nsNode = namespacesNode.Object();
			ns.GetJSON(nsNode);
		}
	}
	{
		auto shardsNode = jb.Array(kShardingShardsCfgName);
		for (const auto& [id, dsns] : shards) {
			auto shrdNode = shardsNode.Object();
			shrdNode.Put("shard_id", id);
			auto dsnsNode = shrdNode.Array(kShardingDSNsCfgName);
			for (const auto& d : dsns) {
				if (masking == MaskingDSN::Disabled) {
					dsnsNode.Put(TagName::Empty(), d.dsn_);
				} else if (masking == MaskingDSN::Enabled) {
					dsnsNode.Put(TagName::Empty(), d);
				}
			}
		}
	}
	jb.Put("this_shard_id", thisShardId);
	jb.Put("reconnect_timeout_msec", reconnectTimeout.count());
	jb.Put("shards_awaiting_timeout_sec", shardsAwaitingTimeout.count());
	jb.Put("config_rollback_timeout_sec", configRollbackTimeout.count());
	jb.Put("proxy_conn_count", proxyConnCount);
	jb.Put("proxy_conn_concurrency", proxyConnConcurrency);
	jb.Put("proxy_conn_threads", proxyConnThreads);
	if (sourceId != ShardingSourceId::NotSet) {
		jb.Put("source_id", sourceId);
	}
}

Error ShardingConfig::Validate() const {
	std::set<std::reference_wrapper<const DSN>, DSN::RefWrapperCompare> dsns;
	for (const auto& s : shards) {
		if (s.first < 0) {
			return Error(errParams, "Shard id should not be less than zero");
		}
		for (const auto& dsn : s.second) {
			if (!dsns.insert(std::cref(dsn)).second) {
				return Error(errParams, "DSNs in shard's config should be unique. Duplicated dsn: {}", dsn);
			}
			if (!dsn.Parser().isValid()) {
				return Error(errParams, "{} is not valid uri", dsn);
			}
			if (dsn.Parser().scheme() != "cproto" && dsn.Parser().scheme() != "cprotos") {
				return Error(errParams, "Scheme of sharding dsn must be cproto or cprotos: {}", dsn);
			}
		}
	}
	for (const auto& ns : namespaces) {
		if (shards.find(ns.defaultShard) == shards.end()) {
			return Error(errParams, "Default shard id should be defined in shards list. Undefined default shard id: {}, for namespace: {}",
						 ns.defaultShard, ns.ns);
		}
		for (const auto& k : ns.keys) {
			if (shards.find(k.shardId) == shards.end()) {
				return Error(errParams, "Shard id should be defined in shards list. Undefined shard id: {}, for namespace: {}", k.shardId,
							 ns.ns);
			}
		}
	}
	return {};
}

}  // namespace reindexer::cluster
