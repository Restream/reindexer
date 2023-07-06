#include "dbconfig.h"
#include <limits.h>
#include <fstream>
#include "cjson/jsonbuilder.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "yaml-cpp/yaml.h"

namespace reindexer {

static CacheMode str2cacheMode(std::string_view mode) {
	using namespace std::string_view_literals;
	if (mode == "on"sv) return CacheModeOn;
	if (mode == "off"sv || mode == ""sv) return CacheModeOff;
	if (mode == "aggressive"sv) return CacheModeAggressive;

	throw Error(errParams, "Unknown cache mode %s", mode);
}

Error DBConfigProvider::FromJSON(const gason::JsonNode &root) {
	try {
		smart_lock<shared_timed_mutex> lk(mtx_, true);

		auto &profilingNode = root["profiling"];
		if (!profilingNode.empty()) {
			profilingData_ = ProfilingConfigData{};
			profilingData_.queriesPerfStats = profilingNode["queriesperfstats"].As<bool>();
			profilingData_.queriesThresholdUS = profilingNode["queries_threshold_us"].As<size_t>();
			profilingData_.perfStats = profilingNode["perfstats"].As<bool>();
			profilingData_.memStats = profilingNode["memstats"].As<bool>();
			profilingData_.activityStats = profilingNode["activitystats"].As<bool>();
			if (!profilingNode["long_queries_logging"].empty()) {
				profilingData_.longSelectLoggingParams.store(
					LongQueriesLoggingParams{profilingNode["long_queries_logging"]["select"]["threshold_us"].As<int32_t>(),
											 profilingNode["long_queries_logging"]["select"]["normalized"].As<bool>()});

				profilingData_.longUpdDelLoggingParams.store(
					LongQueriesLoggingParams{profilingNode["long_queries_logging"]["update_delete"]["threshold_us"].As<int32_t>(),
											 profilingNode["long_queries_logging"]["update_delete"]["normalized"].As<bool>()});

				profilingData_.longTxLoggingParams.store(
					LongTxLoggingParams{profilingNode["long_queries_logging"]["transaction"]["threshold_us"].As<int32_t>(),
										profilingNode["long_queries_logging"]["transaction"]["avg_step_threshold_us"].As<int32_t>()});
			}
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
				data.logLevel = logLevelFromString(nsNode["log_level"].As<std::string>("none"));
				data.strictMode = strictModeFromString(nsNode["strict_mode"].As<std::string>("names"));
				data.cacheMode = str2cacheMode(nsNode["join_cache_mode"].As<std::string_view>("off"));
				data.startCopyPolicyTxSize = nsNode["start_copy_policy_tx_size"].As<int>(data.startCopyPolicyTxSize);
				data.copyPolicyMultiplier = nsNode["copy_policy_multiplier"].As<int>(data.copyPolicyMultiplier);
				data.txSizeToAlwaysCopy = nsNode["tx_size_to_always_copy"].As<int>(data.txSizeToAlwaysCopy);
				data.optimizationTimeout = nsNode["optimization_timeout_ms"].As<int>(data.optimizationTimeout);
				data.optimizationSortWorkers = nsNode["optimization_sort_workers"].As<int>(data.optimizationSortWorkers);
				int64_t walSize = nsNode["wal_size"].As<int64_t>(0);
				if (walSize > 0) {
					data.walSize = walSize;
				}
				data.minPreselectSize = nsNode["min_preselect_size"].As<int64_t>(data.minPreselectSize, 0);
				data.maxPreselectSize = nsNode["max_preselect_size"].As<int64_t>(data.maxPreselectSize, 0);
				data.maxPreselectPart = nsNode["max_preselect_part"].As<double>(data.maxPreselectPart, 0.0, 1.0);
				data.idxUpdatesCountingMode = nsNode["index_updates_counting_mode"].As<bool>(data.idxUpdatesCountingMode);
				data.syncStorageFlushLimit = nsNode["sync_storage_flush_limit"].As<int>(data.syncStorageFlushLimit, 0);
				namespacesData_.emplace(nsNode["namespace"].As<std::string>(), std::move(data));  // NOLINT(performance-move-const-arg)
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
	std::lock_guard<shared_timed_mutex> lk(mtx_);
	handlers_[cfgType] = std::move(handler);
}

ReplicationConfigData DBConfigProvider::GetReplicationConfig() {
	shared_lock<shared_timed_mutex> lk(mtx_);
	return replicationData_;
}

bool DBConfigProvider::GetNamespaceConfig(const std::string &nsName, NamespaceConfigData &data) {
	shared_lock<shared_timed_mutex> lk(mtx_);
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
	try {
		YAML::Node root = YAML::Load(yaml);
		masterDSN = root["master_dsn"].as<std::string>(masterDSN);
		appName = root["app_name"].as<std::string>(appName);
		connPoolSize = root["conn_pool_size"].as<int>(connPoolSize);
		workerThreads = root["worker_threads"].as<int>(workerThreads);
		timeoutSec = root["timeout_sec"].as<int>(timeoutSec);
		clusterID = root["cluster_id"].as<int>(clusterID);
		role = str2role(root["role"].as<std::string>(role2str(role)));
		forceSyncOnLogicError = root["force_sync_on_logic_error"].as<bool>(forceSyncOnLogicError);
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].as<bool>(forceSyncOnWrongDataHash);
		retrySyncIntervalSec = root["retry_sync_interval_sec"].as<int>(retrySyncIntervalSec);
		onlineReplErrorsThreshold = root["online_repl_errors_threshold"].as<int>(onlineReplErrorsThreshold);
		enableCompression = root["enable_compression"].as<bool>(enableCompression);
		serverId = root["server_id"].as<int>(serverId);
		auto node = root["namespaces"];
		namespaces.clear();
		for (const auto &it : node) {
			namespaces.insert(it.as<std::string>());
		}
		return errOK;
	} catch (const YAML::Exception &ex) {
		return Error(errParams, "yaml parsing error: '%s'", ex.what());
	} catch (const Error &err) {
		return err;
	}
}

Error ReplicationConfigData::FromJSON(const gason::JsonNode &root) {
	try {
		masterDSN = root["master_dsn"].As<std::string>();
		appName = root["app_name"].As<std::string>(std::move(appName));
		connPoolSize = root["conn_pool_size"].As<int>(connPoolSize);
		workerThreads = root["worker_threads"].As<int>(workerThreads);
		timeoutSec = root["timeout_sec"].As<int>(timeoutSec);
		clusterID = root["cluster_id"].As<int>(clusterID);
		role = str2role(root["role"].As<std::string>("none"));
		forceSyncOnLogicError = root["force_sync_on_logic_error"].As<bool>();
		forceSyncOnWrongDataHash = root["force_sync_on_wrong_data_hash"].As<bool>();
		retrySyncIntervalSec = root["retry_sync_interval_sec"].As<int>(retrySyncIntervalSec);
		onlineReplErrorsThreshold = root["online_repl_errors_threshold"].As<int>(onlineReplErrorsThreshold);
		enableCompression = root["enable_compression"].As<bool>(enableCompression);
		serverId = root["server_id"].As<int>(serverId);
		namespaces.clear();
		for (auto &objNode : root["namespaces"]) {
			namespaces.insert(objNode.As<std::string>());
		}
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ReplicationConfigData: %s", ex.what());
	}
	return errOK;
}

ReplicationRole ReplicationConfigData::str2role(const std::string &role) {
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
		case ReplicationReadOnly:
		default:
			std::abort();
	}
}

void ReplicationConfigData::GetJSON(JsonBuilder &jb) const {
	jb.Put("role", role2str(role));
	jb.Put("master_dsn", masterDSN);
	jb.Put("app_name", appName);
	jb.Put("cluster_id", clusterID);
	jb.Put("timeout_sec", timeoutSec);
	jb.Put("enable_compression", enableCompression);
	jb.Put("force_sync_on_logic_error", forceSyncOnLogicError);
	jb.Put("force_sync_on_wrong_data_hash", forceSyncOnWrongDataHash);
	jb.Put("retry_sync_interval_sec", retrySyncIntervalSec);
	jb.Put("online_repl_errors_threshold", onlineReplErrorsThreshold);
	jb.Put("server_id", serverId);
	{
		auto arrNode = jb.Array("namespaces");
		for (const auto &ns : namespaces) arrNode.Put(nullptr, ns);
	}
}

void ReplicationConfigData::GetYAML(WrSerializer &ser) const {
	YAML::Node nssYaml;
	nssYaml["namespaces"] = YAML::Node(YAML::NodeType::Sequence);
	for (const auto &ns : namespaces) {
		nssYaml["namespaces"].push_back(ns);
	}
	// clang-format off
	ser <<	"# Replication role. May be one of\n"
			"# none - replication is disabled\n"
			"# slave - replication as slave\n"
			"# master - replication as master\n"
			"# master_master - replication as master master\n"
			"role: " + role2str(role) + "\n"
			"\n"
			"# DSN to master. Only cproto schema is supported\n"
			"master_dsn: " + masterDSN + "\n"
			"\n"
			"# Application name used by replicator as login tag\n"
			"app_name: " + appName + "\n"
			"\n"
			"# Cluser ID - must be same for client and for master\n"
			"cluster_id: " + std::to_string(clusterID) + "\n"
			"\n"
			"# Server Id - must be unique for all nodes\n"
			"server_id: " + std::to_string(serverId) + "\n"
			"\n"
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
			+ YAML::Dump(nssYaml) + '\n';
	// clang-format on
}

}  // namespace reindexer
