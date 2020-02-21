#include "dbconfig.h"
#include <limits.h>
#include <fstream>
#include "cjson/jsonbuilder.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "yaml/yaml.h"

namespace reindexer {

static CacheMode str2cacheMode(const string &mode) {
	if (mode == "on") return CacheModeOn;
	if (mode == "off" || mode == "") return CacheModeOff;
	if (mode == "aggressive") return CacheModeAggressive;

	throw Error(errParams, "Unknown cache mode %s", mode);
}

Error DBConfigProvider::FromJSON(const gason::JsonNode &root) {
	try {
		smart_lock<shared_timed_mutex> lk(mtx_, true);

		auto &profilingNode = root["profiling"];
		if (!profilingNode.empty()) {
			profilingData_ = {};
			profilingData_.queriesPerfStats = profilingNode["queriesperfstats"].As<bool>();
			profilingData_.queriedThresholdUS = profilingNode["queries_threshold_us"].As<size_t>();
			profilingData_.perfStats = profilingNode["perfstats"].As<bool>();
			profilingData_.memStats = profilingNode["memstats"].As<bool>();
			profilingData_.activityStats = profilingNode["activitystats"].As<bool>();
			auto it = handlers_.find(ProfilingConf);
			if (it != handlers_.end()) (it->second)();
		}

		auto &namespacesNode = root["namespaces"];
		if (!namespacesNode.empty()) {
			namespacesData_.clear();
			for (auto &nsNode : namespacesNode) {
				NamespaceConfigData data;
				data.lazyLoad = nsNode["lazyload"].As<bool>();
				data.noQueryIdleThreshold = nsNode["unload_idle_threshold"].As<int>();
				data.logLevel = logLevelFromString(nsNode["log_level"].As<string>("none"));
				data.cacheMode = str2cacheMode(nsNode["join_cache_mode"].As<string>("off"));
				data.startCopyPolicyTxSize = nsNode["start_copy_policy_tx_size"].As<int>(data.startCopyPolicyTxSize);
				data.copyPolicyMultiplier = nsNode["copy_policy_multiplier"].As<int>(data.copyPolicyMultiplier);
				data.txSizeToAlwaysCopy = nsNode["tx_size_to_always_copy"].As<int>(data.txSizeToAlwaysCopy);
				data.optimizationTimeout = nsNode["optimization_timeout_ms"].As<int>(data.optimizationTimeout);
				data.optimizationSortWorkers = nsNode["optimization_sort_workers"].As<int>(data.optimizationSortWorkers);
				namespacesData_.emplace(nsNode["namespace"].As<string>(), std::move(data));
			}
			auto it = handlers_.find(NamespaceDataConf);
			if (it != handlers_.end()) (it->second)();
		}

		auto &replicationNode = root["replication"];
		if (!replicationNode.empty()) {
			auto err = replicationData_.FromJSON(replicationNode);
			if (!err.ok()) return err;

			auto it = handlers_.find(ReplicationConf);
			if (it != handlers_.end()) (it->second)();
		}
		return errOK;
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "DBConfigProvider: %s", ex.what());
	}
}

void DBConfigProvider::setHandler(ConfigType cfgType, std::function<void()> handler) {
	smart_lock<shared_timed_mutex> lk(mtx_, true);
	handlers_[cfgType] = handler;
}

ProfilingConfigData DBConfigProvider::GetProfilingConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return profilingData_;
}

ReplicationConfigData DBConfigProvider::GetReplicationConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return replicationData_;
}

bool DBConfigProvider::GetNamespaceConfig(const string &nsName, NamespaceConfigData &data) {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	auto it = namespacesData_.find(nsName);
	if (it == namespacesData_.end()) {
		it = namespacesData_.find("*");
	}
	if (it == namespacesData_.end()) {
		data = {};
		return false;
	}
	data = it->second;
	return true;
}

Error ReplicationConfigData::FromYML(const string &yaml) {
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);
		masterDSN = root["master_dsn"].As<std::string>(masterDSN);
		connPoolSize = root["conn_pool_size"].As<int>(connPoolSize);
		workerThreads = root["worker_threads"].As<int>(workerThreads);
		timeoutSec = root["timeout_sec"].As<int>(timeoutSec);
		clusterID = root["cluster_id"].As<int>(clusterID);
		role = str2role(root["role"].As<std::string>(role2str(role)));
		forceSyncOnLogicError = root["force_sync_on_logic_error"].As<bool>(forceSyncOnLogicError);
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].As<bool>(forceSyncOnWrongDataHash);
		retrySyncIntervalSec = root["retry_sync_interval_sec"].As<int>(retrySyncIntervalSec);
		onlineReplErrorsThreshold = root["online_repl_errors_threshold"].As<int>(onlineReplErrorsThreshold);

		auto &node = root["namespaces"];
		namespaces.clear();
		for (unsigned i = 0; i < node.Size(); i++) {
			namespaces.insert(node[i].As<std::string>());
		}
		return errOK;
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "yaml parsing error: '%s'", ex.Message());
	} catch (const Error &err) {
		return err;
	}
}

Error ReplicationConfigData::FromJSON(const gason::JsonNode &root) {
	try {
		masterDSN = root["master_dsn"].As<string>();
		connPoolSize = root["conn_pool_size"].As<int>(connPoolSize);
		workerThreads = root["worker_threads"].As<int>(workerThreads);
		timeoutSec = root["timeout_sec"].As<int>(timeoutSec);
		clusterID = root["cluster_id"].As<int>(clusterID);
		role = str2role(root["role"].As<string>("none"));
		forceSyncOnLogicError = root["force_sync_on_logic_error"].As<bool>();
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].As<bool>();
		retrySyncIntervalSec = root["retry_sync_interval_sec"].As<int>(retrySyncIntervalSec);
		onlineReplErrorsThreshold = root["online_repl_errors_threshold"].As<int>(onlineReplErrorsThreshold);

		namespaces.clear();
		for (auto &objNode : root["namespaces"]) {
			namespaces.insert(objNode.As<string>());
		}
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ReplicationConfigData: %s", ex.what());
	}
	return errOK;
}

ReplicationRole ReplicationConfigData::str2role(const string &role) {
	if (role == "master") return ReplicationMaster;
	if (role == "slave") return ReplicationSlave;
	if (role == "none") return ReplicationNone;

	throw Error(errParams, "Unknown replication role %s", role);
}

std::string ReplicationConfigData::role2str(ReplicationRole role) noexcept {
	switch (role) {
		case ReplicationMaster:
			return "master";
		case ReplicationSlave:
			return "slave";
		case ReplicationNone:
			return "none";
		default:
			std::abort();
	}
}

void ReplicationConfigData::GetJSON(JsonBuilder &jb) const {
	jb.Put("role", role2str(role));
	jb.Put("master_dsn", masterDSN);
	jb.Put("cluster_id", clusterID);
	jb.Put("timeout_sec", timeoutSec);
	jb.Put("force_sync_on_logic_error", forceSyncOnLogicError);
	jb.Put("force_sync_on_wrong_data_hash", forceSyncOnWrongDataHash);
	jb.Put("retry_sync_interval_sec", retrySyncIntervalSec);
	jb.Put("online_repl_errors_threshold", onlineReplErrorsThreshold);
	{
		auto arrNode = jb.Array("namespaces");
		for (const auto &ns : namespaces) arrNode.Put(nullptr, ns);
	}
}

void ReplicationConfigData::GetYAML(WrSerializer &ser) const {
	std::string namespacesStr;
	if (namespaces.empty()) {
		namespacesStr = "namespaces: []";
	} else {
		size_t i = 0;
		Yaml::Node nsYaml;
		auto &nsArrayYaml = nsYaml["namespaces"];
		for (auto &ns : namespaces) {
			nsArrayYaml.PushBack();
			nsArrayYaml[i++] = std::move(ns);
		}
		Yaml::Serialize(nsYaml, namespacesStr);
	}
	// clang-format off
	ser <<	"# Replication role. May be on of\n"
			"# none - replication is disabled\n"
			"# slave - replication as slave\n"
			"# master - replication as master\n"
			"role: " + role2str(role) + "\n"
			"\n"
			"# DSN to master. Only cproto schema is supported\n"
			"master_dsn: " + masterDSN + "\n"
			"\n"
			"# Cluser ID - must be same for client and for master\n"
			"cluster_id: " + std::to_string(clusterID) + "\n"
			"# Force resync on logic error conditions\n"
			"force_sync_on_logic_error: " + (forceSyncOnLogicError ? "true" : "false") + "\n"
			"\n"
			"# Master response timeout\n"
			"timeout_sec: " + std::to_string(timeoutSec) + "\n"
			"\n"
			"# Force resync on wrong data hash conditions\n"
			"force_sync_on_wrong_data_hash: " + (forceSyncOnWrongDataHash ? "true" : "false") + "\n"
			"\n"
			"# Resync timeout on network errors"
			"retry_sync_interval_sec: " + std::to_string(retrySyncIntervalSec) + "\n"
			"\n"
			"# Count of online replication erros, which will be merged in single error message"
			"online_repl_errors_threshold: " + std::to_string(onlineReplErrorsThreshold) + "\n"
			"\n"
			"# List of namespaces for replication. If emply, all namespaces\n"
			"# All replicated namespaces will become read only for slave\n"
			"# It should be written as YAML sequence, JSON-style arrays are not supported\n"
			+ namespacesStr + '\n';
	// clang-format on
}

}  // namespace reindexer
