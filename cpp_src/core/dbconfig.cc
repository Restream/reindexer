#include "dbconfig.h"
#include <limits.h>
#include "cjson/jsonbuilder.h"
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

Error DBConfigProvider::FromJSON(JsonValue &v) {
	try {
		smart_lock<shared_timed_mutex> lk(mtx_, true);
		for (auto elem : v) {
			JsonValue &jvalue = elem->value;
			if (jvalue.getTag() == JSON_NULL) continue;
			if (!strcmp(elem->key, "profiling")) {
				if (jvalue.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected object in 'profiling' key");

				profilingData_ = {};
				for (auto elem : jvalue) {
					parseJsonField("queriesperfstats", profilingData_.queriesPerfStats, elem);
					parseJsonField("queries_threshold_us", profilingData_.queriedThresholdUS, elem, 0, INT_MAX);
					parseJsonField("perfstats", profilingData_.perfStats, elem);
					parseJsonField("memstats", profilingData_.memStats, elem);
				}

				auto it = handlers_.find(ProfilingConf);
				if (it != handlers_.end()) (it->second)();
			} else if (!strcmp(elem->key, "namespaces")) {
				if (jvalue.getTag() != JSON_ARRAY) return Error(errParseJson, "Expected array in 'namespaces' key");

				namespacesData_.clear();
				for (auto elem : jvalue) {
					auto &subv = elem->value;
					if (subv.getTag() != JSON_OBJECT) {
						return Error(errParseJson, "Expected object in 'namespaces' array element");
					}

					string name, logLevel, cmode;
					NamespaceConfigData data;
					for (auto subelem : subv) {
						parseJsonField("namespace", name, subelem);
						parseJsonField("lazyload", data.lazyLoad, subelem);
						parseJsonField("unload_idle_threshold", data.noQueryIdleThreshold, subelem);
						parseJsonField("log_level", logLevel, subelem);
						parseJsonField("join_cache_mode", cmode, subelem);
					}
					data.logLevel = logLevelFromString(logLevel);
					namespacesData_.emplace(name, std::move(data));
					data.cacheMode = str2cacheMode(cmode);
				}
				auto it = handlers_.find(NamespaceDataConf);
				if (it != handlers_.end()) (it->second)();
			} else if (!strcmp(elem->key, "replication")) {
				if (jvalue.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected object in 'replication' key");
				auto err = replicationData_.FromJSON(jvalue);
				if (!err.ok()) return err;

				auto it = handlers_.find(ReplicationConf);
				if (it != handlers_.end()) (it->second)();
			}
		}
		return errOK;
	} catch (const Error &err) {
		return err;
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

Error ReplicationConfigData::FromJSON(JsonValue &jvalue) {
	try {
		string replRole = "none";
		for (auto elem : jvalue) {
			parseJsonField("master_dsn", masterDSN, elem);
			parseJsonField("conn_pool_size", connPoolSize, elem);
			parseJsonField("worker_threads", workerThreads, elem);
			parseJsonField("cluster_id", clusterID, elem);
			parseJsonField("role", replRole, elem);
			parseJsonField("force_sync_on_logic_error", forceSyncOnLogicError, elem);
			parseJsonField("force_sync_on_wrong_data_hash", forceSyncOnWrongDataHash, elem);

			if (!strcmp(elem->key, "namespaces")) {
				namespaces.clear();
				if (elem->value.getTag() != JSON_ARRAY) return Error(errParseJson, "Expected array in 'namespaces' key");
				for (auto selem : elem->value) {
					auto &subv = selem->value;
					if (subv.getTag() != JSON_STRING) {
						return Error(errParseJson, "Expected string in 'namespaces' array element");
					}
					namespaces.insert(subv.toString());
				}
			}
		}
		role = str2role(replRole);
	} catch (const Error &err) {
		return err;
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
