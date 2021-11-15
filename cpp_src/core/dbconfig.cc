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
				data.strictMode = strictModeFromString(nsNode["strict_mode"].As<string>("names"));
				data.cacheMode = str2cacheMode(nsNode["join_cache_mode"].As<string>("off"));
				data.startCopyPolicyTxSize = nsNode["start_copy_policy_tx_size"].As<int>(data.startCopyPolicyTxSize);
				data.copyPolicyMultiplier = nsNode["copy_policy_multiplier"].As<int>(data.copyPolicyMultiplier);
				data.txSizeToAlwaysCopy = nsNode["tx_size_to_always_copy"].As<int>(data.txSizeToAlwaysCopy);
				data.optimizationTimeout = nsNode["optimization_timeout_ms"].As<int>(data.optimizationTimeout);
				data.optimizationSortWorkers = nsNode["optimization_sort_workers"].As<int>(data.optimizationSortWorkers);
				int64_t walSize = nsNode["wal_size"].As<int64_t>(0);
				if (walSize > 0) {
					data.walSize = walSize;
				}
				namespacesData_.emplace(nsNode["namespace"].As<string>(), std::move(data));
			}
			auto it = handlers_.find(NamespaceDataConf);
			if (it != handlers_.end()) {
				(it->second)();
			}
		}

		auto &asyncReplicationNode = root["async_replication"];
		if (!asyncReplicationNode.empty()) {
			auto err = asyncReplicationData_.FromJSON(asyncReplicationNode);
			if (!err.ok()) return err;

			auto it = handlers_.find(AsyncReplicationConf);
			if (it != handlers_.end()) (it->second)();
		}
		auto &replicationNode = root["replication"];
		if (!replicationNode.empty()) {
			auto err = replicationData_.FromJSON(replicationNode);
			if (!err.ok()) return err;

			auto it = handlers_.find(ReplicationConf);
			if (it != handlers_.end()) {
				(it->second)();
			}
			for (auto &f : replicationConfigDataHandlers_) {
				f.second(replicationData_);
			}
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

int DBConfigProvider::setHandler(std::function<void(ReplicationConfigData)> handler) {
	smart_lock<shared_timed_mutex> lk(mtx_, true);
	HandlersCounter_++;
	replicationConfigDataHandlers_[HandlersCounter_] = handler;
	return HandlersCounter_;
}

void DBConfigProvider::unsetHandler(int id) {
	smart_lock<shared_timed_mutex> lk(mtx_, true);
	replicationConfigDataHandlers_.erase(id);
}

ProfilingConfigData DBConfigProvider::GetProfilingConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return profilingData_;
}

ReplicationConfigData DBConfigProvider::GetReplicationConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return replicationData_;
}

cluster::AsyncReplConfigData DBConfigProvider::GetAsyncReplicationConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return asyncReplicationData_;
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

Error ReplicationConfigData::FromYML(const std::string &yaml) {
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);
		clusterID = root["cluster_id"].As<int>(clusterID);
		serverID = root["server_id"].As<int>(serverID);
		return errOK;
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "ReplicationConfigData: yaml parsing error: '%s'", ex.Message());
	} catch (const Error &err) {
		return err;
	}
}

Error ReplicationConfigData::FromJSON(std::string_view json) {
	try {
		return FromJSON(gason::JsonParser().Parse(json));
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ReplicationConfigData: %s", ex.what());
	}
}

Error ReplicationConfigData::FromJSON(const gason::JsonNode &root) {
	try {
		clusterID = root["cluster_id"].As<int>(clusterID);
		serverID = root["server_id"].As<int>(serverID);
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ReplicationConfigData: %s", ex.what());
	}
	return errOK;
}

void ReplicationConfigData::GetJSON(JsonBuilder &jb) const {
	jb.Put("cluster_id", clusterID);
	jb.Put("server_id", serverID);
}

void ReplicationConfigData::GetYAML(WrSerializer &ser) const {
	// clang-format off
	ser <<	"# Cluser ID - must be same for client and for master\n"
			"cluster_id: " + std::to_string(clusterID) + "\n"
			"\n"
			"# Server ID - must be unique for all nodes\n"
			"server_id: " + std::to_string(serverID) + "\n"
			"\n";
	// clang-format on
}

}  // namespace reindexer
