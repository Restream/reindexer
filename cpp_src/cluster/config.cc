#include "cluster/config.h"

#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "vendor/urlparser/urlparser.h"
#include "yaml/yaml.h"

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

Error ShardingKey::FromYML(Yaml::Node &yaml) {
	hostsDsns.clear();
	auto &hostsNode = yaml["dsns"];
	for (size_t i = 0; i < hostsNode.Size(); ++i) {
		Yaml::Node &dsnNode = hostsNode[i];
		string dsn = "cproto://";
		dsn.append(dsnNode["host"].As<string>());
		dsn.push_back(':');
		dsn.append(dsnNode["port"].As<string>());
		dsn.push_back('/');
		dsn.append(dsnNode["database"].As<string>());
		hostsDsns.emplace_back(dsn);
	}

	auto readValue = [=](auto &algorithmOptionsNode) {
		auto &valuesNode = algorithmOptionsNode["values"];
		if (valuesNode.Type() == Yaml::Node::SequenceType) {
			for (size_t i = 0; i < valuesNode.Size(); ++i) {
				values.emplace_back(stringToVariant(valuesNode[i].AsString()));
			}
		} else {
			values.emplace_back(stringToVariant(valuesNode.AsString()));
		}
	};

	values.clear();
	auto &algorithmOptionsNode = yaml["algorithm_options"];
	if (algorithmOptionsNode.Type() == Yaml::Node::SequenceType) {
		for (size_t i = 0; i < algorithmOptionsNode.Size(); ++i) {
			auto &optionsNode = algorithmOptionsNode[i];
			ShardingAlgorithmType type = ShardingAlgorithmType(optionsNode["type"].As<int>());
			if (type == ShardingAlgorithmType::ByValue) {  // -V547
				readValue(optionsNode);
			} else {
				return Error(errParams, "Unsupported sharding algorithm type: %d", int(type));
			}
		}
	} else {
		ShardingAlgorithmType type = ShardingAlgorithmType(algorithmOptionsNode["type"].As<int>());
		if (type == ShardingAlgorithmType::ByValue) {  // -V547
			readValue(algorithmOptionsNode);
		} else {
			return Error(errParams, "Unsupported sharding algorithm type: %d", int(type));
		}
	}

	ns = yaml["namespace"].As<string>();
	index = yaml["index"].As<string>();
	shardId = yaml["shard_id"].As<int>();
	return errOK;
}

string ShardingKey::ToYml() const {
	string yaml = "  - namespace: ";
	yaml.append(ns);
	yaml.push_back('\n');

	yaml.append("    index: ");
	yaml.append(index);
	yaml.push_back('\n');

	if (algorithmType == ShardingAlgorithmType::ByValue) {	// -V547
		yaml.append("    algorithm_options: ");
		yaml.push_back('\n');
		yaml.append("        type: by_value");
		yaml.push_back('\n');
		yaml.append("        values: ");
		for (size_t i = 0; i < values.size(); ++i) {
			yaml.append("\n          - ");
			bool isString = (values[i].Type() == KeyValueString);
			if (isString) yaml.push_back('"');
			yaml.append(values[i].As<string>());
			if (isString) yaml.push_back('"');
		}
		yaml.push_back('\n');
	} else {
		throw Error(errParams, "Unsupported sharding algorithm type: %d", int(algorithmType));
	}
	yaml.append("    values:");
	for (size_t i = 0; i < values.size(); ++i) {
		yaml.append("\n      - ");
		bool isString = (values[i].Type() == KeyValueString);
		if (isString) yaml.push_back('"');
		yaml.append(values[i].As<string>());
		if (isString) yaml.push_back('"');
	}
	yaml.push_back('\n');

	yaml.append("    dsns:");
	for (size_t i = 0; i < hostsDsns.size(); ++i) {
		httpparser::UrlParser parser;
		parser.parse(hostsDsns[i]);
		yaml.append("\n      - host: " + parser.hostname());
		yaml.append("\n        port: " + parser.port());
		yaml.append("\n        database: " + parser.db());
	}

	yaml.push_back('\n');
	yaml.append("    shard_id: ");
	yaml.append(std::to_string(shardId));
	yaml.push_back('\n');
	yaml.push_back('\n');

	return yaml;
}

Error ShardingConfig::FromYML(const std::string &yaml) {
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);

		auto &proxyDsnsNode = root["proxy_dsns"];
		for (size_t i = 0; i < proxyDsnsNode.Size(); ++i) {
			Yaml::Node &dsnNode = proxyDsnsNode[i];
			string dsn = "cproto://";
			dsn.append(dsnNode["host"].As<string>());
			dsn.push_back(':');
			dsn.append(dsnNode["port"].As<string>());
			dsn.push_back('/');
			dsn.append(dsnNode["database"].As<string>());
			proxyDsns.emplace_back(dsn);
		}

		auto &values = root["keys"];
		keys.clear();
		for (size_t i = 0; i < values.Size(); ++i) {
			ShardingKey key;
			key.FromYML(values[i]);
			keys.emplace_back(std::move(key));
		}

		thisShardId = root["this_shard_id"].As<int>();
		return errOK;
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "yaml parsing error: '%s'", ex.Message());
	} catch (const Error &err) {
		return err;
	}
}

string ShardingConfig::ToYml() const {
	string yaml = "proxy_dsns:";
	for (size_t i = 0; i < proxyDsns.size(); ++i) {
		httpparser::UrlParser parser;
		parser.parse(proxyDsns[i]);
		yaml.append("\n  - host: " + parser.hostname());
		yaml.append("\n    port: " + parser.port());
		yaml.append("\n    database: " + parser.db());
	}
	yaml.append("\n\n");
	yaml.append("keys:\n");
	for (const ShardingKey &key : keys) {
		yaml.append(key.ToYml());
	}
	yaml.append("this_shard_id: ");
	yaml.append(std::to_string(thisShardId));
	yaml.push_back('\n');
	return yaml;
}

}  // namespace cluster
}  // namespace reindexer
