#include "dbconfig.h"
#include <limits.h>
#include "cjson/jsonbuilder.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
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
				data.startCopyPoliticsCount = nsNode["start_copy_politics_count"].As<int>(data.startCopyPoliticsCount);
				data.mergeLimitCount = nsNode["merge_limit_count"].As<int>(data.mergeLimitCount);
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

void DBConfigProvider::SetReplicationConfig(const ReplicationConfigData &conf) {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	replicationData_ = conf;
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
		clusterID = root["cluster_id"].As<int>(clusterID);
		role = str2role(root["role"].As<std::string>(role2str(role)));
		forceSyncOnLogicError = root["force_sync_on_logic_error"].As<bool>();
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].As<bool>();

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
		connPoolSize = root["conn_pool_size"].As<int>(1);
		workerThreads = root["worker_threads"].As<int>(1);
		clusterID = root["cluster_id"].As<int>(1);
		role = str2role(root["role"].As<string>("none"));
		forceSyncOnLogicError = root["force_sync_on_logic_error"].As<bool>();
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].As<bool>();

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

std::string ReplicationConfigData::role2str(ReplicationRole role) {
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

void ReplicationConfigData::GetJSON(JsonBuilder &jb) {
	jb.Put("role", role2str(role));
	jb.Put("master_dsn", masterDSN);
	jb.Put("cluster_id", clusterID);
	jb.Put("force_sync_on_logic_error", forceSyncOnLogicError);
	jb.Put("force_sync_on_wrong_data_hash", forceSyncOnWrongDataHash);
	{
		auto arrNode = jb.Array("namespaces");
		for (auto &ns : namespaces) arrNode.Put(nullptr, ns);
	}
}

}  // namespace reindexer
