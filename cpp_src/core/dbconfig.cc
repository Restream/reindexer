#include "dbconfig.h"

#include <bitset>

#include "cjson/jsonbuilder.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "type_consts.h"
#include "vendor/yaml-cpp/yaml.h"

namespace reindexer {

static CacheMode str2cacheMode(std::string_view mode) {
	using namespace std::string_view_literals;
	if (mode == "on"sv) {
		return CacheModeOn;
	}
	if (mode == "off"sv || mode == ""sv) {
		return CacheModeOff;
	}
	if (mode == "aggressive"sv) {
		return CacheModeAggressive;
	}

	throw Error(errParams, "Unknown cache mode %s", mode);
}

Error DBConfigProvider::FromJSON(const gason::JsonNode& root, bool autoCorrect) {
	std::bitset<kConfigTypesTotalCount> typesChanged;

	std::string errLogString;
	try {
		smart_lock<shared_timed_mutex> lk(mtx_, true);

		ProfilingConfigData profilingDataSafe;
		auto& profilingNode = root["profiling"];
		if (!profilingNode.empty()) {
			profilingDataLoadResult_ = profilingDataSafe.FromJSON(profilingNode);

			if (profilingDataLoadResult_.ok()) {
				typesChanged.set(ProfilingConf);
			} else {
				errLogString += profilingDataLoadResult_.whatStr();
			}
		}

		fast_hash_map<std::string, NamespaceConfigData, hash_str, equal_str, less_str> namespacesData;
		auto& namespacesNode = root["namespaces"];
		if (!namespacesNode.empty()) {
			std::string namespacesErrLogString;
			bool nssHaveErrors = false;
			for (auto& nsNode : namespacesNode) {
				std::string nsName;
				if (auto err = tryReadRequiredJsonValue(&errLogString, nsNode, "namespace", nsName)) {
					ensureEndsWith(namespacesErrLogString, "\n") += err.whatStr();
					nssHaveErrors = true;
					continue;
				}

				NamespaceConfigData data;
				auto err = data.FromJSON(nsNode);
				if (err.ok()) {
					namespacesData.emplace(std::move(nsName),
										   std::move(data));  // NOLINT(performance-move-const-arg)
				} else {
					ensureEndsWith(namespacesErrLogString, "\n") += err.whatStr();
					nssHaveErrors = true;
				}
			}

			if (!nssHaveErrors) {
				namespacesDataLoadResult_ = errOK;
				typesChanged.set(NamespaceDataConf);
			} else {
				namespacesDataLoadResult_ = Error(errParseJson, namespacesErrLogString);
				ensureEndsWith(errLogString, "\n") += "NamespacesConfig: JSON parsing error: " + namespacesErrLogString;
			}
		}

		cluster::AsyncReplConfigData asyncReplConfigDataSafe;
		auto& asyncReplicationNode = root["async_replication"];
		if (!asyncReplicationNode.empty()) {
			asyncReplicationDataLoadResult_ = asyncReplConfigDataSafe.FromJSON(asyncReplicationNode);

			if (asyncReplicationDataLoadResult_.ok()) {
				typesChanged.set(AsyncReplicationConf);
			} else {
				ensureEndsWith(errLogString, "\n") += asyncReplicationDataLoadResult_.whatStr();
			}
		}

		ReplicationConfigData replicationDataSafe;
		auto& replicationNode = root["replication"];
		if (!replicationNode.empty()) {
			if (!autoCorrect) {
				replicationDataLoadResult_ = replicationDataSafe.FromJSON(replicationNode);

				if (replicationDataLoadResult_.ok()) {
					typesChanged.set(ReplicationConf);
				} else {
					ensureEndsWith(errLogString, "\n") += replicationDataLoadResult_.whatStr();
				}
			} else {
				replicationDataSafe.FromJSONWithCorrection(replicationNode, replicationDataLoadResult_);
				typesChanged.set(ReplicationConf);
			}
		}

		// Applying entire configuration only if no read errors
		if (errLogString.empty()) {
			if (typesChanged.test(ProfilingConf)) {
				profilingData_ = profilingDataSafe;
			}
			if (typesChanged.test(NamespaceDataConf)) {
				namespacesData_ = std::move(namespacesData);
			}
			if (typesChanged.test(AsyncReplicationConf)) {
				asyncReplicationData_ = std::move(asyncReplConfigDataSafe);
			}
			if (typesChanged.test(ReplicationConf)) {
				replicationData_ = std::move(replicationDataSafe);
			}
		}
	} catch (const std::exception& ex) {
		ensureEndsWith(errLogString, "\n") += ex.what();
	}

	Error result;
	if (!errLogString.empty()) {
		result = Error(errParseJson, "DBConfigProvider: %s", errLogString);
	} else if (typesChanged.size() > 0) {
		// notifying handlers under shared_lock so none of them go out of scope
		smart_lock<shared_timed_mutex> lk(mtx_, false);

		for (unsigned changedType = 0; changedType < typesChanged.size(); ++changedType) {
			if (!typesChanged.test(changedType)) {
				continue;
			}
			if (handlers_[changedType]) {
				try {
					handlers_[changedType]();
				} catch (const Error& err) {
					logPrintf(LogError, "DBConfigProvider: Error processing event handler: '%s'", err.whatStr());
				}
			}

			if (ReplicationConf == changedType) {
				for (auto& f : replicationConfigDataHandlers_) {
					try {
						f.second(replicationData_);
					} catch (const Error& err) {
						logPrintf(LogError, "DBConfigProvider: Error processing replication config event handler: '%s'", err.whatStr());
					}
				}
			}
		}
	}

	return result;
}

Error DBConfigProvider::GetConfigParseErrors() const {
	using namespace std::string_view_literals;
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	if (!profilingDataLoadResult_.ok() || !namespacesDataLoadResult_.ok() || !asyncReplicationDataLoadResult_.ok() ||
		!replicationDataLoadResult_.ok()) {
		std::string errLogString;
		errLogString += profilingDataLoadResult_.whatStr();
		ensureEndsWith(errLogString, "\n"sv) += namespacesDataLoadResult_.whatStr();
		ensureEndsWith(errLogString, "\n"sv) += asyncReplicationDataLoadResult_.whatStr();
		ensureEndsWith(errLogString, "\n"sv) += replicationDataLoadResult_.whatStr();

		return Error(errParseJson, "DBConfigProvider: %s", errLogString);
	}

	return Error();
}

void DBConfigProvider::setHandler(ConfigType cfgType, std::function<void()> handler) {
	std::lock_guard<shared_timed_mutex> lk(mtx_);
	handlers_[cfgType] = std::move(handler);
}

int DBConfigProvider::setHandler(std::function<void(const ReplicationConfigData&)> handler) {
	smart_lock<shared_timed_mutex> lk(mtx_, true);
	replicationConfigDataHandlers_[++handlersCounter_] = std::move(handler);
	return handlersCounter_;
}

void DBConfigProvider::unsetHandler(int id) {
	smart_lock<shared_timed_mutex> lk(mtx_, true);
	replicationConfigDataHandlers_.erase(id);
}

ReplicationConfigData DBConfigProvider::GetReplicationConfig() {
	shared_lock<shared_timed_mutex> lk(mtx_);
	return replicationData_;
}

cluster::AsyncReplConfigData DBConfigProvider::GetAsyncReplicationConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return asyncReplicationData_;
}

bool DBConfigProvider::GetNamespaceConfig(std::string_view nsName, NamespaceConfigData& data) {
	shared_lock<shared_timed_mutex> lk(mtx_);
	auto it = namespacesData_.find(nsName);
	if (it == namespacesData_.end()) {
		it = namespacesData_.find(std::string_view("*"));
	}
	if (it == namespacesData_.end()) {
		data = {};
		return false;
	}
	data = it->second;
	return true;
}

Error ProfilingConfigData::FromJSON(const gason::JsonNode& v) {
	using namespace std::string_view_literals;
	std::string errorString;
	auto err = tryReadOptionalJsonValue(&errorString, v, "queriesperfstats"sv, queriesPerfStats);
	err = tryReadOptionalJsonValue(&errorString, v, "queries_threshold_us"sv, queriesThresholdUS);
	err = tryReadOptionalJsonValue(&errorString, v, "perfstats"sv, perfStats);
	err = tryReadOptionalJsonValue(&errorString, v, "memstats"sv, memStats);
	err = tryReadOptionalJsonValue(&errorString, v, "activitystats"sv, activityStats);
	(void)err;	// ignored; Errors will be handled with errorString

	auto& longQueriesLogging = v["long_queries_logging"sv];
	if (longQueriesLogging.isObject()) {
		auto& select = longQueriesLogging["select"sv];
		if (select.isObject()) {
			const auto p = longSelectLoggingParams.load(std::memory_order_relaxed);
			int32_t thresholdUs = p.thresholdUs;
			bool normalized = p.normalized;
			err = tryReadOptionalJsonValue(&errorString, select, "threshold_us"sv, thresholdUs);
			err = tryReadOptionalJsonValue(&errorString, select, "normalized"sv, normalized);
			(void)err;	// ignored; Errors will be handled with errorString
			longSelectLoggingParams.store(LongQueriesLoggingParams(thresholdUs, normalized), std::memory_order_relaxed);
		}

		auto& updateDelete = longQueriesLogging["update_delete"sv];
		if (updateDelete.isObject()) {
			const auto p = longUpdDelLoggingParams.load(std::memory_order_relaxed);
			int32_t thresholdUs = p.thresholdUs;
			bool normalized = p.normalized;
			err = tryReadOptionalJsonValue(&errorString, updateDelete, "threshold_us"sv, thresholdUs);
			err = tryReadOptionalJsonValue(&errorString, updateDelete, "normalized"sv, normalized);
			(void)err;	// ignored; Errors will be handled with errorString
			longUpdDelLoggingParams.store(LongQueriesLoggingParams(thresholdUs, normalized), std::memory_order_relaxed);
		}

		auto& transaction = longQueriesLogging["transaction"sv];
		if (transaction.isObject()) {
			const auto p = longTxLoggingParams.load(std::memory_order_relaxed);
			int32_t thresholdUs = p.thresholdUs;
			err = tryReadOptionalJsonValue(&errorString, transaction, "threshold_us"sv, thresholdUs);

			int32_t avgTxStepThresholdUs = p.avgTxStepThresholdUs;
			err = tryReadOptionalJsonValue(&errorString, transaction, "avg_step_threshold_us"sv, avgTxStepThresholdUs);
			(void)err;	// ignored; Errors will be handled with errorString
			longTxLoggingParams.store(LongTxLoggingParams(thresholdUs, avgTxStepThresholdUs), std::memory_order_relaxed);
		}
	}

	if (!errorString.empty()) {
		return Error(errParseJson, "ProfilingConfigData: JSON parsing error: '%s'", errorString);
	}
	return {};
}

Error ReplicationConfigData::FromYAML(const std::string& yaml) {
	try {
		YAML::Node root = YAML::Load(yaml);
		clusterID = root["cluster_id"].as<int>(clusterID);
		serverID = root["server_id"].as<int>(serverID);
		if (auto err = Validate(); !err.ok()) {
			return Error(errParams, "ReplicationConfigData: YAML parsing error: '%s'", err.whatStr());
		}
	} catch (const YAML::Exception& ex) {
		return Error(errParseYAML, "ReplicationConfigData: YAML parsing error: '%s'", ex.what());
	} catch (const std::exception& err) {
		return err;
	}
	return Error();
}

Error ReplicationConfigData::FromJSON(std::string_view json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "ReplicationConfigData: %s\n", ex.what());
	} catch (const std::exception& err) {
		return err;
	}
}

Error ReplicationConfigData::FromJSON(const gason::JsonNode& root) {
	using namespace std::string_view_literals;
	std::string errorString;
	auto err = tryReadOptionalJsonValue(&errorString, root, "cluster_id"sv, clusterID);
	err = tryReadOptionalJsonValue(&errorString, root, "server_id"sv, serverID);
	(void)err;	// ignored; Errors will be handled with errorString

	if (errorString.empty()) {
		if (auto err = Validate()) {
			return Error(errParseJson, "ReplicationConfigData: JSON parsing error: '%s'", err.whatStr());
		}
	} else {
		return Error(errParseJson, "ProfilingConfigData: JSON parsing error: '%s'", errorString);
	}

	return Error();
}

void ReplicationConfigData::FromJSONWithCorrection(const gason::JsonNode& root, Error& fixedErrors) {
	using namespace std::string_view_literals;
	fixedErrors = Error();
	std::string errorString;

	if (!tryReadOptionalJsonValue(&errorString, root, "cluster_id"sv, clusterID).ok()) {
		clusterID = 1;
		ensureEndsWith(errorString, "\n") += fmt::sprintf("Fixing to default: cluster_id = %d.", clusterID);
	}

	if (!tryReadOptionalJsonValue(&errorString, root, "server_id"sv, serverID, ReplicationConfigData::kMinServerIDValue,
								  ReplicationConfigData::kMaxServerIDValue)
			 .ok()) {
		serverID = 0;
		ensureEndsWith(errorString, "\n") += fmt::sprintf("Fixing to default: server_id = %d.", serverID);
	}

	if (!errorString.empty()) {
		fixedErrors = Error(errParseJson, errorString);
	}
}

void ReplicationConfigData::GetJSON(JsonBuilder& jb) const {
	jb.Put("cluster_id", clusterID);
	jb.Put("server_id", serverID);
}

void ReplicationConfigData::GetYAML(WrSerializer& ser) const {
	// clang-format off
	ser <<	"# Cluser ID - must be same for client and for master\n"
			"cluster_id: " + std::to_string(clusterID) + "\n"
			"\n"
			"# Server ID - must be unique for all nodes value in range [0, 999]\n"
			"server_id: " + std::to_string(serverID) + "\n"
			"\n";
	// clang-format on
}

Error ReplicationConfigData::Validate() const {
	using namespace std::string_view_literals;
	if (serverID < ReplicationConfigData::kMinServerIDValue || serverID > ReplicationConfigData::kMaxServerIDValue) {
		return Error(errParams, "Value of '%s' = %d is out of bounds: [%d, %d]", "server_Id"sv, serverID,
					 ReplicationConfigData::kMinServerIDValue, ReplicationConfigData::kMaxServerIDValue);
	}
	return Error();
}

std::ostream& operator<<(std::ostream& os, const ReplicationConfigData& data) {
	return os << "{ server_id: " << data.serverID << "; cluster_id: " << data.clusterID << "}";
}

Error NamespaceConfigData::FromJSON(const gason::JsonNode& v) {
	using namespace std::string_view_literals;
	std::string errorString;
	auto err = tryReadOptionalJsonValue(&errorString, v, "lazyload"sv, lazyLoad);
	err = tryReadOptionalJsonValue(&errorString, v, "unload_idle_threshold"sv, noQueryIdleThreshold);
	(void)err;	// ignored; Errors will be handled with errorString

	std::string stringVal(logLevelToString(logLevel));
	if (tryReadOptionalJsonValue(&errorString, v, "log_level"sv, stringVal).ok()) {
		logLevel = logLevelFromString(stringVal);
	}

	stringVal = strictModeToString(strictMode);
	if (tryReadOptionalJsonValue(&errorString, v, "strict_mode"sv, stringVal).ok()) {
		strictMode = strictModeFromString(stringVal);
	}

	std::string_view stringViewVal = "off"sv;
	if (tryReadOptionalJsonValue(&errorString, v, "join_cache_mode"sv, stringViewVal).ok()) {
		try {
			cacheMode = str2cacheMode(stringViewVal);
		} catch (Error& err) {
			ensureEndsWith(errorString, "\n") += "errParams: " + err.whatStr();
		}
	}

	err = tryReadOptionalJsonValue(&errorString, v, "start_copy_policy_tx_size"sv, startCopyPolicyTxSize);
	err = tryReadOptionalJsonValue(&errorString, v, "copy_policy_multiplier"sv, copyPolicyMultiplier);
	err = tryReadOptionalJsonValue(&errorString, v, "tx_size_to_always_copy"sv, txSizeToAlwaysCopy);
	err = tryReadOptionalJsonValue(&errorString, v, "tx_vec_insertion_threads"sv, txVecInsertionThreads);
	err = tryReadOptionalJsonValue(&errorString, v, "optimization_timeout_ms"sv, optimizationTimeout);
	err = tryReadOptionalJsonValue(&errorString, v, "optimization_sort_workers"sv, optimizationSortWorkers);
	(void)err;	// ignored; Errors will be handled with errorString

	if (int64_t walSizeV = walSize; tryReadOptionalJsonValue(&errorString, v, "wal_size"sv, walSizeV, 0).ok()) {
		if (walSizeV > 0) {
			walSize = walSizeV;
		}
	}
	err = tryReadOptionalJsonValue(&errorString, v, "min_preselect_size"sv, minPreselectSize, 0);
	err = tryReadOptionalJsonValue(&errorString, v, "max_preselect_size"sv, maxPreselectSize, 0);
	err = tryReadOptionalJsonValue(&errorString, v, "max_preselect_part"sv, maxPreselectPart, 0.0, 1.0);
	err = tryReadOptionalJsonValue(&errorString, v, "max_iterations_idset_preresult"sv, maxIterationsIdSetPreResult, 0);
	err = tryReadOptionalJsonValue(&errorString, v, "index_updates_counting_mode"sv, idxUpdatesCountingMode);
	err = tryReadOptionalJsonValue(&errorString, v, "sync_storage_flush_limit"sv, syncStorageFlushLimit, 0);
	(void)err;	// ignored; Errors will be handled with errorString

	auto cacheNode = v["cache"];
	if (!cacheNode.empty()) {
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "index_idset_cache_size"sv, cacheConfig.idxIdsetCacheSize, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "index_idset_hits_to_cache"sv, cacheConfig.idxIdsetHitsToCache, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "ft_index_cache_size"sv, cacheConfig.ftIdxCacheSize, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "ft_index_hits_to_cache"sv, cacheConfig.ftIdxHitsToCache, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "joins_preselect_cache_size"sv, cacheConfig.joinCacheSize, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "joins_preselect_hit_to_cache"sv, cacheConfig.joinHitsToCache, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "query_count_cache_size"sv, cacheConfig.queryCountCacheSize, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "query_count_hit_to_cache"sv, cacheConfig.queryCountHitsToCache, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "index_idset_cache_size"sv, cacheConfig.idxIdsetCacheSize, 0);
		err = tryReadOptionalJsonValue(&errorString, cacheNode, "index_idset_cache_size"sv, cacheConfig.idxIdsetCacheSize, 0);
		(void)err;	// ignored; Errors will be handled with errorString
	}

	if (!errorString.empty()) {
		return Error(errParseJson, "NamespaceConfigData: JSON parsing error: '%s'", errorString);
	}
	return {};
}

}  // namespace reindexer
