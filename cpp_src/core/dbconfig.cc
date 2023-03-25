#include "dbconfig.h"

#include <limits.h>
#include <fstream>
#include <string>
#include <string_view>

#include "cjson/jsonbuilder.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "spdlog/fmt/fmt.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "type_consts.h"
#include "yaml-cpp/yaml.h"

namespace reindexer {

static CacheMode str2cacheMode(std::string_view mode) {
	using namespace std::string_view_literals;
	if (mode == "on"sv) return CacheModeOn;
	if (mode == "off"sv || mode == ""sv) return CacheModeOff;
	if (mode == "aggressive"sv) return CacheModeAggressive;

	throw Error(errParams, "Unknown cache mode %s", mode);
}

Error DBConfigProvider::FromJSON(const gason::JsonNode &root, bool autoCorrect) {
	std::set<ConfigType> typesChanged;

	std::string errLogString;
	try {
		smart_lock<shared_timed_mutex> lk(mtx_, true);

		ProfilingConfigData profilingDataSafe;
		auto &profilingNode = root["profiling"];
		if (!profilingNode.empty()) {
			profilingDataLoadResult_ = profilingDataSafe.FromJSON(profilingNode);

			if (profilingDataLoadResult_.ok()) {
				typesChanged.emplace(ProfilingConf);
			} else {
				errLogString += profilingDataLoadResult_.what();
			}
		}

		std::unordered_map<std::string, NamespaceConfigData> namespacesData;
		auto &namespacesNode = root["namespaces"];
		if (!namespacesNode.empty()) {
			std::string namespacesErrLogString;
			bool nssHaveErrors = false;
			for (auto &nsNode : namespacesNode) {
				std::string nsName;
				if (auto err = tryReadRequiredJsonValue(&errLogString, nsNode, "namespace", nsName)) {
					ensureEndsWith(namespacesErrLogString, "\n") += err.what();
					nssHaveErrors = true;
					continue;
				}

				NamespaceConfigData data;
				auto err = data.FromJSON(nsNode);
				if (err.ok()) {
					namespacesData.emplace(std::move(nsName),
										   std::move(data));  // NOLINT(performance-move-const-arg)
				} else {
					ensureEndsWith(namespacesErrLogString, "\n") += err.what();
					nssHaveErrors = true;
				}
			}

			if (!nssHaveErrors) {
				namespacesDataLoadResult_ = errOK;
				typesChanged.emplace(NamespaceDataConf);
			} else {
				namespacesDataLoadResult_ = Error(errParseJson, namespacesErrLogString);
				ensureEndsWith(errLogString, "\n") += "NamespacesConfig: JSON parsing error: " + namespacesErrLogString;
			}
		}

		cluster::AsyncReplConfigData asyncReplConfigDataSafe;
		auto &asyncReplicationNode = root["async_replication"];
		if (!asyncReplicationNode.empty()) {
			asyncReplicationDataLoadResult_ = asyncReplConfigDataSafe.FromJSON(asyncReplicationNode);

			if (asyncReplicationDataLoadResult_.ok()) {
				typesChanged.emplace(AsyncReplicationConf);
			} else {
				ensureEndsWith(errLogString, "\n") += asyncReplicationDataLoadResult_.what();
			}
		}

		ReplicationConfigData replicationDataSafe;
		auto &replicationNode = root["replication"];
		if (!replicationNode.empty()) {
			if (!autoCorrect) {
				replicationDataLoadResult_ = replicationDataSafe.FromJSON(replicationNode);

				if (replicationDataLoadResult_.ok()) {
					typesChanged.emplace(ReplicationConf);
				} else {
					ensureEndsWith(errLogString, "\n") += replicationDataLoadResult_.what();
				}
			} else {
				replicationDataSafe.FromJSONWithCorrection(replicationNode, replicationDataLoadResult_);
				typesChanged.emplace(ReplicationConf);
			}
		}

		// Applying entire configuration only if no read errors
		if (errLogString.empty()) {
			if (typesChanged.find(ProfilingConf) != typesChanged.end()) {
				profilingData_ = std::move(profilingDataSafe);
			}
			if (typesChanged.find(NamespaceDataConf) != typesChanged.end()) {
				namespacesData_ = std::move(namespacesData);
			}
			if (typesChanged.find(AsyncReplicationConf) != typesChanged.end()) {
				asyncReplicationData_ = std::move(asyncReplConfigDataSafe);
			}
			if (typesChanged.find(ReplicationConf) != typesChanged.end()) {
				replicationData_ = std::move(replicationDataSafe);
			}
		}

	} catch (const Error &err) {
		ensureEndsWith(errLogString, "\n") += err.what();
	} catch (const gason::Exception &ex) {
		ensureEndsWith(errLogString, "\n") += ex.what();
	}

	Error result;
	if (!errLogString.empty()) {
		result = Error(errParseJson, "DBConfigProvider: %s", errLogString);
	} else if (typesChanged.size() > 0) {
		// notifying handlers under shared_lock so none of them go out of scope
		smart_lock<shared_timed_mutex> lk(mtx_, false);

		for (auto changedType : typesChanged) {
			auto it = handlers_.find(changedType);
			if (it != handlers_.end()) {
				try {
					(it->second)();
				} catch (const Error &err) {
					logPrintf(LogError, "DBConfigProvider: Error processing event handler: '%s'", err.what());
				}
			}

			if (ReplicationConf == changedType) {
				for (auto &f : replicationConfigDataHandlers_) {
					try {
						f.second(replicationData_);
					} catch (const Error &err) {
						logPrintf(LogError, "DBConfigProvider: Error processing replication config event handler: '%s'", err.what());
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
		errLogString += profilingDataLoadResult_.what();
		ensureEndsWith(errLogString, "\n"sv) += namespacesDataLoadResult_.what();
		ensureEndsWith(errLogString, "\n"sv) += asyncReplicationDataLoadResult_.what();
		ensureEndsWith(errLogString, "\n"sv) += replicationDataLoadResult_.what();

		return Error(errParseJson, "DBConfigProvider: %s", errLogString);
	}

	return Error();
}

void DBConfigProvider::setHandler(ConfigType cfgType, std::function<void()> handler) {
	smart_lock<shared_timed_mutex> lk(mtx_, true);
	handlers_[cfgType] = std::move(handler);
}

int DBConfigProvider::setHandler(std::function<void(ReplicationConfigData)> handler) {
	smart_lock<shared_timed_mutex> lk(mtx_, true);
	HandlersCounter_++;
	replicationConfigDataHandlers_[HandlersCounter_] = std::move(handler);
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

LongQueriesLoggingParams DBConfigProvider::GetSelectLoggingParams() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return profilingData_.longSelectLoggingParams;
}

LongQueriesLoggingParams DBConfigProvider::GetUpdDelLoggingParams() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return profilingData_.longUpdDelLoggingParams;
}

LongTxLoggingParams DBConfigProvider::GetTxLoggingParams() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return profilingData_.longTxLoggingParams;
}

bool DBConfigProvider::ActivityStatsEnabled() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return profilingData_.activityStats;
}

ReplicationConfigData DBConfigProvider::GetReplicationConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return replicationData_;
}

cluster::AsyncReplConfigData DBConfigProvider::GetAsyncReplicationConfig() {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return asyncReplicationData_;
}

bool DBConfigProvider::GetNamespaceConfig(const std::string &nsName, NamespaceConfigData &data) {
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

Error ProfilingConfigData::FromJSON(const gason::JsonNode &v) {
	using namespace std::string_view_literals;
	std::string errorString;
	tryReadOptionalJsonValue(&errorString, v, "queriesperfstats"sv, queriesPerfStats);
	tryReadOptionalJsonValue(&errorString, v, "queries_threshold_us"sv, queriedThresholdUS);
	tryReadOptionalJsonValue(&errorString, v, "perfstats"sv, perfStats);
	tryReadOptionalJsonValue(&errorString, v, "memstats"sv, memStats);
	tryReadOptionalJsonValue(&errorString, v, "activitystats"sv, activityStats);

	auto &longQueriesLogging = v["long_queries_logging"sv];
	if (!longQueriesLogging.empty()) {
		auto &select = longQueriesLogging["select"sv];
		if (!select.empty()) {
			tryReadOptionalJsonValue(&errorString, select, "threshold_us"sv, longSelectLoggingParams.thresholdUs);
			tryReadOptionalJsonValue(&errorString, select, "normalized"sv, longSelectLoggingParams.normalized);
		}

		auto &updateDelete = longQueriesLogging["update_delete"sv];
		if (!updateDelete.empty()) {
			tryReadOptionalJsonValue(&errorString, updateDelete, "threshold_us"sv, longUpdDelLoggingParams.thresholdUs);
			tryReadOptionalJsonValue(&errorString, updateDelete, "normalized"sv, longUpdDelLoggingParams.normalized);
		}

		auto &transaction = longQueriesLogging["transaction"sv];
		if (!transaction.empty()) {
			int32_t value = longTxLoggingParams.thresholdUs;
			tryReadOptionalJsonValue(&errorString, transaction, "threshold_us"sv, value);
			longTxLoggingParams.thresholdUs = value;

			value = longTxLoggingParams.avgTxStepThresholdUs;
			tryReadOptionalJsonValue(&errorString, transaction, "avg_step_threshold_us"sv, value);
			longTxLoggingParams.avgTxStepThresholdUs = value;
		}
	}

	if (!errorString.empty()) {
		return Error(errParseJson, "ProfilingConfigData: JSON parsing error: '%s'", errorString);
	} else
		return Error();
}

Error ReplicationConfigData::FromYAML(const std::string &yaml) {
	try {
		YAML::Node root = YAML::Load(yaml);
		clusterID = root["cluster_id"].as<int>(clusterID);
		serverID = root["server_id"].as<int>(serverID);
		if (auto err = Validate()) {
			return Error(errParams, "ReplicationConfigData: YAML parsing error: '%s'", err.what());
		}
	} catch (const YAML::Exception &ex) {
		return Error(errParseYAML, "ReplicationConfigData: YAML parsing error: '%s'", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return Error();
}

Error ReplicationConfigData::FromJSON(std::string_view json) {
	try {
		return FromJSON(gason::JsonParser().Parse(json));
	} catch (const Error &err) {
		return err;
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ReplicationConfigData: %s\n", ex.what());
	}
}

Error ReplicationConfigData::FromJSON(const gason::JsonNode &root) {
	using namespace std::string_view_literals;
	std::string errorString;
	tryReadOptionalJsonValue(&errorString, root, "cluster_id"sv, clusterID);
	tryReadOptionalJsonValue(&errorString, root, "server_id"sv, serverID);

	if (errorString.empty()) {
		if (auto err = Validate()) {
			return Error(errParseJson, "ReplicationConfigData: JSON parsing error: '%s'", err.what());
		}
	} else {
		return Error(errParseJson, "ProfilingConfigData: JSON parsing error: '%s'", errorString);
	}

	return Error();
}

void ReplicationConfigData::FromJSONWithCorrection(const gason::JsonNode &root, Error &fixedErrors) {
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

	if (!errorString.empty()) fixedErrors = Error(errParseJson, errorString);
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

std::ostream &operator<<(std::ostream &os, const ReplicationConfigData &data) {
	return os << "{ server_id: " << data.serverID << "; cluster_id: " << data.clusterID << "}";
}

Error NamespaceConfigData::FromJSON(const gason::JsonNode &v) {
	using namespace std::string_view_literals;
	std::string errorString;
	tryReadOptionalJsonValue(&errorString, v, "lazyload"sv, lazyLoad);
	tryReadOptionalJsonValue(&errorString, v, "unload_idle_threshold"sv, noQueryIdleThreshold);

	std::string stringVal = logLevelToString(logLevel);
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
		} catch (Error &err) {
			ensureEndsWith(errorString, "\n") += "errParams: " + err.what();
		}
	}

	tryReadOptionalJsonValue(&errorString, v, "start_copy_policy_tx_size"sv, startCopyPolicyTxSize);
	tryReadOptionalJsonValue(&errorString, v, "copy_policy_multiplier"sv, copyPolicyMultiplier);
	tryReadOptionalJsonValue(&errorString, v, "tx_size_to_always_copy"sv, txSizeToAlwaysCopy);
	tryReadOptionalJsonValue(&errorString, v, "optimization_timeout_ms"sv, optimizationTimeout);
	tryReadOptionalJsonValue(&errorString, v, "optimization_sort_workers"sv, optimizationSortWorkers);

	if (int64_t walSizeV = walSize; tryReadOptionalJsonValue(&errorString, v, "wal_size"sv, walSizeV, 0).ok()) {
		if (walSizeV > 0) {
			walSize = walSizeV;
		}
	}
	tryReadOptionalJsonValue(&errorString, v, "min_preselect_size"sv, minPreselectSize, 0);
	tryReadOptionalJsonValue(&errorString, v, "max_preselect_size"sv, maxPreselectSize, 0);
	tryReadOptionalJsonValue(&errorString, v, "max_preselect_part"sv, maxPreselectPart, 0.0, 1.0);
	tryReadOptionalJsonValue(&errorString, v, "index_updates_counting_mode"sv, idxUpdatesCountingMode);
	tryReadOptionalJsonValue(&errorString, v, "sync_storage_flush_limit"sv, syncStorageFlushLimit, 0);

	if (!errorString.empty()) {
		return Error(errParseJson, "NamespaceConfigData: JSON parsing error: '%s'", errorString);
	}
	return Error();
}

}  // namespace reindexer
