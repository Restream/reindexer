#include "cluster/config.h"

#include <string_view>

#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/json2kv.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "vendor/urlparser/urlparser.h"
#include "yaml/yaml.h"

using namespace std::string_view_literals;

namespace reindexer {
namespace cluster {

const std::string_view kCprotoPrefix = "cproto://";

static void ValidateDSN(std::string_view dsn) {
	if (!checkIfStartsWith(kCprotoPrefix, dsn)) {
		throw Error(errParams, "DSN must start with cproto://. Actual DSN is %s", dsn);
	}
}

Error NodeData::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NodeData: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error NodeData::FromJSON(const gason::JsonNode &root) {
	try {
		serverId = root["server_id"].As<int>(serverId);
		electionsTerm = root["elections_term"].As<int>(electionsTerm);
		dsn = root["dsn"].As<std::string>();
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NodeData: %s", ex.what());
	}
	return errOK;
}

void NodeData::GetJSON(JsonBuilder &jb) const {
	jb.Put("server_id", serverId);
	jb.Put("elections_term", electionsTerm);
	jb.Put("dsn", dsn);
}

void NodeData::GetJSON(WrSerializer &ser) const {
	JsonBuilder jb(ser);
	GetJSON(jb);
}

Error RaftInfo::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "RaftInfo: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error RaftInfo::FromJSON(const gason::JsonNode &root) {
	try {
		leaderId = root["leader_id"].As<int>(leaderId);
		role = RoleFromStr(root["role"].As<std::string_view>());
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "RaftInfo: %s", ex.what());
	}
	return errOK;
}

void RaftInfo::GetJSON(JsonBuilder &jb) const {
	jb.Put("leader_id", leaderId);
	jb.Put("role", RoleToStr(role));
}

void RaftInfo::GetJSON(WrSerializer &ser) const {
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

void ClusterNodeConfig::FromYML(Yaml::Node &root) {
	serverId = root["server_id"].As<int>(serverId);
	dsn = root["dsn"].As<std::string>(dsn);
	ValidateDSN(dsn);
}

void AsyncReplNodeConfig::FromYML(Yaml::Node &root) {
	dsn = root["dsn"].As<std::string>(dsn);
	ValidateDSN(dsn);
	auto &node = root["namespaces"];
	namespaces_.reset();
	if (node.IsSequence()) {
		fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss;
		for (unsigned i = 0; i < node.Size(); i++) {
			nss.emplace(node[i].As<std::string>());
		}
		SetOwnNamespaceList(std::move(nss));
	}
}

void AsyncReplNodeConfig::FromJSON(const gason::JsonNode &root) {
	dsn = root["dsn"].As<std::string>(dsn);
	ValidateDSN(dsn);
	auto &node = root["namespaces"];
	if (!node.empty()) {
		fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss;
		for (auto &objNode : node) {
			nss.emplace(objNode.As<string>());
		}
		SetOwnNamespaceList(std::move(nss));
	}
}

void AsyncReplNodeConfig::GetJSON(JsonBuilder &jb) const {
	jb.Put("dsn", dsn);
	if (hasOwnNsList_) {
		auto arrNode = jb.Array("namespaces");
		for (const auto &ns : namespaces_->data) arrNode.Put(nullptr, ns);
	}
}

void AsyncReplNodeConfig::GetYAML(Yaml::Node &yaml) const {
	yaml["dsn"] = dsn;
	if (hasOwnNsList_) {
		auto &nsArrayYaml = yaml["namespaces"];
		if (!namespaces_ || namespaces_->Empty()) {
			(void)nsArrayYaml[0];  // Create empty array
		} else {
			size_t i = 0;
			for (auto &ns : namespaces_->data) {
				nsArrayYaml.PushBack();
				nsArrayYaml[i++] = ns;
			}
		}
	}
}

void AsyncReplNodeConfig::SetNamespaceListFromConfig(const AsyncReplConfigData &config) {
	assert(config.namespaces);
	namespaces_ = config.namespaces;
	hasOwnNsList_ = false;
}

Error ClusterConfigData::FromYML(const std::string &yaml) {
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);
		appName = root["app_name"].As<std::string>(appName);
		replThreadsCount = root["sync_threads"].As<int>(replThreadsCount);
		parallelSyncsPerThreadCount = root["syncs_per_thread"].As<int>(parallelSyncsPerThreadCount);
		onlineUpdatesTimeoutSec = root["online_updates_timeout_sec"].As<int>(onlineUpdatesTimeoutSec);
		syncTimeoutSec = root["sync_timeout_sec"].As<int>(syncTimeoutSec);
		leaderSyncThreads = root["leader_sync_threads"].As<int>(leaderSyncThreads);
		leaderSyncConcurrentSnapshotsPerNode =
			root["leader_sync_concurrent_snapshots_per_node"].As<int>(leaderSyncConcurrentSnapshotsPerNode);
		retrySyncIntervalMSec = root["retry_sync_interval_msec"].As<int>(retrySyncIntervalMSec);
		enableCompression = root["enable_compression"].As<bool>(enableCompression);
		batchingRoutinesCount = root["batching_routines_count"].As<int>(batchingRoutinesCount);
		maxWALDepthOnForceSync = root["max_wal_depth_on_force_sync"].As<int>(maxWALDepthOnForceSync);
		{
			auto &node = root["namespaces"];
			namespaces.clear();
			for (unsigned i = 0; i < node.Size(); i++) {
				namespaces.insert(node[i].As<std::string>());
			}
		}
		{
			auto &node = root["nodes"];
			nodes.clear();
			for (unsigned i = 0; i < node.Size(); i++) {
				ClusterNodeConfig conf;
				conf.FromYML(node[i]);
				nodes.emplace_back(std::move(conf));
			}
		}
		return Error();
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "ClusterConfigData: yaml parsing error: '%s'", ex.Message());
	} catch (const Error &err) {
		return err;
	}
}

Error AsyncReplConfigData::FromYML(const std::string &yaml) {
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);
		role = Str2role(root["role"].As<std::string>("none"));
		appName = root["app_name"].As<std::string>(appName);
		replThreadsCount = root["sync_threads"].As<int>(replThreadsCount);
		parallelSyncsPerThreadCount = root["syncs_per_thread"].As<int>(parallelSyncsPerThreadCount);
		onlineUpdatesTimeoutSec = root["online_updates_timeout_sec"].As<int>(onlineUpdatesTimeoutSec);
		syncTimeoutSec = root["sync_timeout_sec"].As<int>(syncTimeoutSec);
		forceSyncOnLogicError = root["force_sync_on_logic_error"].As<bool>(forceSyncOnLogicError);
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].As<bool>(forceSyncOnWrongDataHash);
		retrySyncIntervalMSec = root["retry_sync_interval_msec"].As<int>(retrySyncIntervalMSec);
		enableCompression = root["enable_compression"].As<bool>(enableCompression);
		batchingRoutinesCount = root["batching_routines_count"].As<int>(batchingRoutinesCount);
		maxWALDepthOnForceSync = root["max_wal_depth_on_force_sync"].As<int>(maxWALDepthOnForceSync);
		{
			auto &node = root["namespaces"];
			fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss;
			for (unsigned i = 0; i < node.Size(); i++) {
				nss.emplace(node[i].As<std::string>());
			}
			namespaces = make_intrusive<NamespaceList>(std::move(nss));
		}
		{
			auto &node = root["nodes"];
			nodes.clear();
			for (unsigned i = 0; i < node.Size(); i++) {
				AsyncReplNodeConfig conf;
				conf.FromYML(node[i]);
				if (!conf.HasOwnNsList()) {
					conf.SetNamespaceListFromConfig(*this);
				}
				nodes.emplace_back(std::move(conf));
			}
		}
		return errOK;
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "AsyncReplConfigData: yaml parsing error: '%s'", ex.Message());
	} catch (const Error &err) {
		return err;
	}
}

Error AsyncReplConfigData::FromJSON(std::string_view json) {
	try {
		return FromJSON(gason::JsonParser().Parse(json));
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "AsyncReplConfigData: %s", ex.what());
	}
}

Error AsyncReplConfigData::FromJSON(const gason::JsonNode &root) {
	try {
		role = Str2role(root["role"].As<string>("none"));
		appName = root["app_name"].As<std::string>(appName);
		replThreadsCount = root["sync_threads"].As<int>(replThreadsCount);
		parallelSyncsPerThreadCount = root["syncs_per_thread"].As<int>(parallelSyncsPerThreadCount);
		onlineUpdatesTimeoutSec = root["online_updates_timeout_sec"].As<int>(onlineUpdatesTimeoutSec);
		syncTimeoutSec = root["sync_timeout_sec"].As<int>(syncTimeoutSec);
		forceSyncOnLogicError = root["force_sync_on_logic_error"].As<bool>(forceSyncOnLogicError);
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].As<bool>(forceSyncOnWrongDataHash);
		retrySyncIntervalMSec = root["retry_sync_interval_msec"].As<int>(retrySyncIntervalMSec);
		enableCompression = root["enable_compression"].As<bool>(enableCompression);
		batchingRoutinesCount = root["batching_routines_count"].As<int>(batchingRoutinesCount);
		maxWALDepthOnForceSync = root["max_wal_depth_on_force_sync"].As<int>(maxWALDepthOnForceSync);

		fast_hash_set<string, nocase_hash_str, nocase_equal_str> nss;
		for (auto &objNode : root["namespaces"]) {
			nss.emplace(objNode.As<string>());
		}
		namespaces = make_intrusive<NamespaceList>(std::move(nss));

		nodes.clear();
		for (auto &objNode : root["nodes"]) {
			AsyncReplNodeConfig conf;
			conf.FromJSON(objNode);
			if (!conf.HasOwnNsList()) {
				conf.SetNamespaceListFromConfig(*this);
			}
			nodes.emplace_back(std::move(conf));
		}
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "AsyncReplConfigData: %s", ex.what());
	}
	return errOK;
}

void AsyncReplConfigData::GetJSON(JsonBuilder &jb) const {
	jb.Put("role", Role2str(role));
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
	{
		auto arrNode = jb.Array("namespaces");
		for (const auto &ns : namespaces->data) arrNode.Put(nullptr, ns);
	}
	{
		auto arrNode = jb.Array("nodes");
		for (const auto &node : nodes) {
			auto obj = arrNode.Object();
			node.GetJSON(obj);
		}
	}
}

void AsyncReplConfigData::GetYAML(WrSerializer &ser) const {
	std::string namespacesStr, nodesStr;
	if (!namespaces || namespaces->Empty()) {
		namespacesStr = "namespaces: []";
	} else {
		size_t i = 0;
		Yaml::Node nsYaml;
		auto &nsArrayYaml = nsYaml["namespaces"];
		for (auto &ns : namespaces->data) {
			nsArrayYaml.PushBack();
			nsArrayYaml[i++] = std::move(ns);
		}
		Yaml::Serialize(nsYaml, namespacesStr);
	}
	if (nodes.empty()) {
		nodesStr = "nodes: []";
	} else {
		size_t i = 0;
		Yaml::Node nodesYaml;
		auto &nodesArrayYaml = nodesYaml["nodes"];
		for (auto &node : nodes) {
			nodesArrayYaml.PushBack();
			node.GetYAML(nodesArrayYaml[i++]);
		}
		Yaml::Serialize(nodesYaml, nodesStr);
	}
	// clang-format off
	ser <<	"# Replication role. May be one of\n"
			"# none - replication is disabled\n"
			"# follower - replication as follower\n"
			"# leader - replication as leader\n"
			"role: " + Role2str(role) + "\n"
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
			"# networks triparound await time, but will require more RAM"
			"batching_routines_count: " + std::to_string(batchingRoutinesCount) + "\n"
			"\n"
			"# Maximum number of WAL-records, which may be gained from force-sync.\n"
			"# Increasing this value may help to avoid force-syncs after leader's switch, hovewer it also increases RAM consumetion during syncs\n"
			"max_wal_depth_on_force_sync: " + std::to_string(maxWALDepthOnForceSync) + "\n"
			"\n"
			"# List of namespaces for replication. If emply, all namespaces\n"
			"# All replicated namespaces will become read only for followers\n"
			"# It should be written as YAML sequence, JSON-style arrays are not supported\n"
			+ namespacesStr + "\n"
			"# List of nodes for replication.\n"
			"# It should be written as YAML sequence, JSON-style arrays are not supported\n"
			+ nodesStr + "\n\n";
	// clang-format on
}

AsyncReplConfigData::Role AsyncReplConfigData::Str2role(std::string_view role) noexcept {
	using namespace std::string_view_literals;
	if (role == "leader"sv) return Role::Leader;
	if (role == "follower"sv) return Role::Follower;
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

Error ShardingConfig::Namespace::FromYML(Yaml::Node &yaml, const std::map<int, std::vector<std::string>> &shards) {
	ns = yaml["namespace"].As<string>();
	index = yaml["index"].As<string>();
	auto &keysNode = yaml["keys"];
	keys.clear();
	keys.resize(keysNode.Size());
	for (size_t i = 0; i < keys.size(); ++i) {
		const Error err = keys[i].FromYML(keysNode[i], shards);
		if (!err.ok()) return err;
	}
	return errOK;
}

Error ShardingConfig::Namespace::FromJson(const gason::JsonNode &root) {
	try {
		ns = root["namespace"].As<std::string>();
		index = root["index"].As<std::string>();
		const auto &keysNode = root["keys"];
		keys.clear();
		for (const auto &kNode : keysNode) {
			keys.emplace_back();
			const Error err = keys.back().FromJson(kNode);
			if (!err.ok()) return err;
		}
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ShardingConfig::Namespace: %s", ex.what());
	}
	return errOK;
}

Error ShardingConfig::Key::FromYML(Yaml::Node &yaml, const std::map<int, std::vector<std::string>> &shards) {
	values.clear();

	shardId = yaml["shard_id"].As<int>();
	if (shardId == 0) {
		return Error{errParams, "Shard id 0 is reserved for proxy"};
	}
	if (shards.find(shardId) == shards.cend()) {
		return Error(errParams, "Shard id %d is not specified in the config but it is used in namespace keys", shardId);
	}

	auto &valuesNode = yaml["values"];
	auto &fromNode = yaml["from"];
	auto &toNode = yaml["to"];
	if (!valuesNode.IsNone()) {
		if (!fromNode.IsNone() || !toNode.IsNone()) {
			return Error(errParams, "Key specifies values and range at the same time");
		}
		algorithmType = ByValue;
		for (size_t i = 0; i < valuesNode.Size(); ++i) {
			values.emplace_back(stringToVariant(valuesNode[i].AsString()));
		}
	} else if (!fromNode.IsNone() || !toNode.IsNone()) {
		if (fromNode.IsNone() || toNode.IsNone()) {
			return Error(errParams, "Key's range should be specifies by 'from' and 'to'");
		}
		algorithmType = ByRange;
		values.emplace_back(stringToVariant(fromNode.AsString()));
		values.emplace_back(stringToVariant(toNode.AsString()));
	} else {
		return Error(errParams, "Unsupported sharding algorithm type: neither values nor range are specified");
	}

	return errOK;
}

Error ShardingConfig::Key::FromJson(const gason::JsonNode &root) {
	try {
		shardId = root["shard_id"].As<int>();
		values.clear();
		const auto &valuesNode = root["values"];
		if (valuesNode.empty()) {
			const auto &fromNode = root["from"];
			const auto &toNode = root["to"];
			algorithmType = ByRange;
			values.reserve(2);
			values.emplace_back(jsonValue2Variant(fromNode.value, KeyValueUndefined));
			values.emplace_back(jsonValue2Variant(toNode.value, KeyValueUndefined));
		} else {
			algorithmType = ByValue;
			for (const auto &vNode : valuesNode) {
				values.emplace_back(jsonValue2Variant(vNode.value, KeyValueUndefined));
			}
		}
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ShardingConfig::Key: %s", ex.what());
	}
	return errOK;
}

void ShardingConfig::Namespace::GetYml(std::stringstream &yaml) const {
	yaml << "\n  - namespace: \"" << ns << "\"\n    index: \"" << index << "\"\n    keys:";
	for (const auto &key : keys) {
		key.GetYml(yaml);
	}
}

void ShardingConfig::Namespace::GetJson(JsonBuilder &jb) const {
	jb.Put("namespace", ns);
	jb.Put("index", index);
	auto keysNode = jb.Array("keys");
	for (const auto &key : keys) {
		auto kNode = keysNode.Object(0);
		key.GetJson(kNode);
	}
}

void ShardingConfig::Key::GetYml(std::stringstream &yaml) const {
	yaml << "\n      - shard_id: " << shardId << "\n        ";
	switch (algorithmType) {
		case ByValue:
			yaml << "values:";
			for (const auto &v : values) {
				yaml << "\n          - ";
				if (v.Type() == KeyValueString) yaml << '"';
				yaml << v.As<std::string>();
				if (v.Type() == KeyValueString) yaml << '"';
			}
			break;
		case ByRange:
			assert(values.size() == 2);
			yaml << "from: ";
			if (values[0].Type() == KeyValueString) yaml << '"';
			yaml << values[0].As<std::string>();
			if (values[0].Type() == KeyValueString) yaml << '"';
			yaml << "\n        to: ";
			if (values[1].Type() == KeyValueString) yaml << '"';
			yaml << values[1].As<std::string>();
			if (values[1].Type() == KeyValueString) yaml << '"';
			break;
	}
}

void ShardingConfig::Key::GetJson(JsonBuilder &jb) const {
	jb.Put("shard_id", shardId);
	switch (algorithmType) {
		case ByValue: {
			auto valuesNode = jb.Array("values");
			for (const auto &v : values) valuesNode.Put(0, v);
			break;
		}
		case ByRange:
			jb.Put("from", values[0]);
			jb.Put("to", values[1]);
			break;
	}
}

Error ShardingConfig::FromYML(const std::string &yaml) {
	proxyDsns.clear();
	namespaces.clear();
	shards.clear();

	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);

		const auto &versionNode = root["version"];
		if (versionNode.IsNone()) {
			return Error(errParams, "Version of sharding config file is not specified");
		}
		if (const int v{versionNode.As<int>()}; v != 1) {
			return Error(errParams, "Unsupported version of sharding config file: %d", v);
		}
		auto &proxyDsnsNode = root["proxy_dsns"];
		proxyDsns.reserve(proxyDsnsNode.Size());
		for (size_t i = 0; i < proxyDsnsNode.Size(); ++i) {
			Yaml::Node &dsnNode = proxyDsnsNode[i];
			proxyDsns.emplace_back(dsnNode.AsString());
		}
		if (proxyDsns.empty()) {
			return Error{errParams, "Proxy dsns are not specified"};
		}

		auto &shardsNode = root["shards"];
		for (size_t i = 0; i < shardsNode.Size(); ++i) {
			size_t shardId = shardsNode[i]["shard_id"].As<int>();
			if (shardId == 0) {
				return Error{errParams, "Shard id 0 is reserved for proxy"};
			}
			if (shards.find(shardId) != shards.end()) {
				return Error{errParams, "Dsns for shard id %u are specified twice", shardId};
			}
			auto &hostsNode = shardsNode[i]["dsns"];
			shards[shardId].reserve(hostsNode.Size());
			for (size_t i = 0; i < hostsNode.Size(); ++i) {
				shards[shardId].emplace_back(hostsNode[i].AsString());
			}
		}

		auto &namespacesNodes = root["namespaces"];
		namespaces.resize(namespacesNodes.Size());
		for (size_t i = 0; i < namespaces.size(); ++i) {
			const Error err = namespaces[i].FromYML(namespacesNodes[i], shards);
			if (!err.ok()) return err;
		}

		thisShardId = root["this_shard_id"].As<int>();
		return errOK;
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "yaml parsing error: '%s'", ex.Message());
	} catch (const Error &err) {
		return err;
	}
}

Error ShardingConfig::FromJson(span<char> json) {
	try {
		return FromJson(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ShardingConfig: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
}

Error ShardingConfig::FromJson(const gason::JsonNode &root) {
	try {
		const int v = root["version"].As<int>();
		if (v != 1) {
			return Error(errParams, "Unsupported version of sharding config file: %d", v);
		}
		proxyDsns.clear();
		const auto &prxDsnsNode = root["proxy_dsns"];
		for (const auto &prxDsnN : prxDsnsNode) {
			proxyDsns.emplace_back(prxDsnN.As<std::string>());
		}
		namespaces.clear();
		const auto &namespacesNode = root["namespaces"];
		for (const auto &nsNode : namespacesNode) {
			namespaces.emplace_back();
			const Error err = namespaces.back().FromJson(nsNode);
			if (!err.ok()) return err;
		}
		shards.clear();
		const auto &shardsNode = root["shards"];
		for (const auto &shrdNode : shardsNode) {
			const int shardId = shrdNode["shard_id"].As<int>();
			if (shards.find(shardId) != shards.end()) {
				return Error{errParams, "Dsns for shard id %u are specified twice", shardId};
			}
			const auto &dsnsNode = shrdNode["dsns"];
			for (const auto &dNode : dsnsNode) {
				shards[shardId].emplace_back(dNode.As<std::string>());
			}
		}
		thisShardId = root["this_shard_id"].As<int>();
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NodeData: %s", ex.what());
	}
	return errOK;
}

bool operator==(const ShardingConfig &lhs, const ShardingConfig &rhs) {
	return lhs.proxyDsns == rhs.proxyDsns && lhs.namespaces == rhs.namespaces && lhs.thisShardId == rhs.thisShardId &&
		   lhs.shards == rhs.shards;
}
bool operator==(const ShardingConfig::Key &lhs, const ShardingConfig::Key &rhs) {
	return lhs.shardId == rhs.shardId && lhs.algorithmType == rhs.algorithmType && lhs.values.RelaxCompare(rhs.values) == 0;
}
bool operator==(const ShardingConfig::Namespace &lhs, const ShardingConfig::Namespace &rhs) {
	return lhs.ns == rhs.ns && lhs.index == rhs.index && lhs.keys == rhs.keys;
}

std::string ShardingConfig::GetYml() const {
	std::stringstream yaml;
	yaml << "version: 1\nproxy_dsns:";
	for (size_t i = 0; i < proxyDsns.size(); ++i) {
		yaml << "\n  - \"" << proxyDsns[i] << '"';
	}
	yaml << "\nnamespaces:";
	for (const auto &ns : namespaces) {
		ns.GetYml(yaml);
	}
	yaml << "\nshards:";
	for (const auto &[id, dsns] : shards) {
		yaml << "\n  - shard_id: " << id << "\n    dsns:";
		for (const auto &d : dsns) {
			yaml << "\n      - \"" << d << '"';
		}
	}
	yaml << "\nthis_shard_id: " << thisShardId << '\n';
	return yaml.str();
}

std::string ShardingConfig::GetJson() const {
	WrSerializer ser;
	GetJson(ser);
	return std::string{ser.Slice()};
}

void ShardingConfig::GetJson(WrSerializer &ser) const {
	JsonBuilder jb(ser);
	GetJson(jb);
}

void ShardingConfig::GetJson(JsonBuilder &jb) const {
	jb.Put("version", 1);
	{
		auto proxyDsnsNode = jb.Array("proxy_dsns");
		for (const auto &prxDsn : proxyDsns) proxyDsnsNode.Put(0, prxDsn);
	}
	{
		auto namespacesNode = jb.Array("namespaces");
		for (const auto &ns : namespaces) {
			auto nsNode = namespacesNode.Object(0);
			ns.GetJson(nsNode);
		}
	}
	{
		auto shardsNode = jb.Array("shards");
		for (const auto &[id, dsns] : shards) {
			auto shrdNode = shardsNode.Object(0);
			shrdNode.Put("shard_id", id);
			auto dsnsNode = shrdNode.Array("dsns");
			for (const auto &d : dsns) {
				dsnsNode.Put(0, d);
			}
		}
	}
	jb.Put("this_shard_id", thisShardId);
}

}  // namespace cluster
}  // namespace reindexer
