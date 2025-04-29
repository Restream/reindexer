#include "dbconfig.h"
#include <bitset>
#include "cjson/jsonbuilder.h"
#include "defnsconfigs.h"
#include "estl/smart_lock.h"
#include "gason/gason.h"
#include "tools/catch_and_return.h"
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

	throw Error(errParams, "Unknown cache mode {}", mode);
}

static std::string_view cacheMode2str(CacheMode mode) {
	using namespace std::string_view_literals;
	switch (mode) {
		case CacheModeOn:
			return "on"sv;
		case CacheModeOff:
			return "off"sv;
		case CacheModeAggressive:
			return "aggressive"sv;
		default:
			throw Error(errParams, "Unknown cache mode {}", int(mode));
	}
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

		fast_hash_map<std::string, NamespaceConfigData, nocase_hash_str, nocase_equal_str, nocase_less_str> namespacesData;
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
		result = Error(errParseJson, "DBConfigProvider: {}", errLogString);
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
					logFmt(LogError, "DBConfigProvider: Error processing event handler: '{}'", err.whatStr());
				}
			}

			if (ReplicationConf == changedType) {
				for (auto& f : replicationConfigDataHandlers_) {
					try {
						f.second(replicationData_);
					} catch (const Error& err) {
						logFmt(LogError, "DBConfigProvider: Error processing replication config event handler: '{}'", err.whatStr());
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

		return Error(errParseJson, "DBConfigProvider: {}", errLogString);
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

ReplicationConfigData DBConfigProvider::GetReplicationConfig() const {
	shared_lock<shared_timed_mutex> lk(mtx_);
	return replicationData_;
}

cluster::AsyncReplConfigData DBConfigProvider::GetAsyncReplicationConfig() const {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	return asyncReplicationData_;
}

Error DBConfigProvider::CheckAsyncReplicationToken(std::string_view nsName, std::string_view token) const {
	if (auto nsToken = GetAsyncReplicationToken(nsName); !nsToken.empty() && nsToken != token) {
		return Error(errReplParams, "Different replication tokens. Expected '{}', but got '{}'", nsToken, token);
	}
	return {};
}

std::string DBConfigProvider::GetAsyncReplicationToken(std::string_view nsName) const {
	smart_lock<shared_timed_mutex> lk(mtx_, false);
	const auto& tokens = replicationData_.admissibleTokens;
	if (auto it = tokens.find(nsName); it != tokens.end()) {
		return it->second;
	} else if (it = tokens.find("*"); it != tokens.end()) {
		return it->second;
	}
	return std::string{};
}

bool DBConfigProvider::GetNamespaceConfig(std::string_view nsName, NamespaceConfigData& data) const {
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

Error ProfilingConfigData::FromDefault() noexcept {
	try {
		gason::JsonParser parser;
		gason::JsonNode configJson = parser.Parse(kDefProfilingConfig);
		auto& profilingJson = configJson["profiling"];
		if (!profilingJson.isObject()) {
			return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefProfilingConfig");
		}

		Error err = FromJSON(profilingJson);
		if (!err.ok()) {
			return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefProfilingConfig: {}", err.what());
		}
	}
	CATCH_AND_RETURN

	return ErrorCode::errOK;
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
		return Error(errParseJson, "ProfilingConfigData: JSON parsing error: '{}'", errorString);
	}
	return {};
}

static auto getAdmissibleTokens(const YAML::Node& node) {
	if (node && !node.IsSequence()) {
		throw Error{errParams, "'admissible_replication_tokens' node must be the array"};
	}

	NsNamesHashMapT<std::string> res;
	for (const auto& tokenNode : node) {
		if (!tokenNode.IsMap()) {
			throw Error{errParams, "Object with admissible tokens has incorrect type"};
		}
		auto token = tokenNode["token"];
		if (!token || !token.IsScalar()) {
			throw Error{errParams, "Field with token value is not filled in or has the incorrect type (expected string scalar)"};
		}
		const auto& tokenVal = token.as<std::string>();
		auto nss = tokenNode["namespaces"];

		if (!nss || !nss.IsSequence()) {
			throw Error{errParams,
						"Field with namespaces for currect token {} is not filled in or has the incorrect type (expected array of strings)",
						tokenVal};
		}

		for (const auto& ns : nss) {
			if (!ns.IsScalar()) {
				throw Error{errParams, "Namespace name must be the string"};
			}
			if (auto [_, wasInserted] = res.insert({NamespaceName(ns.as<std::string>()), tokenVal}); !wasInserted) {
				throw Error{errParams, "Namespace lists for different admissible tokens should not intersect"};
			}
		}
	}
	return res;
}

static auto getAdmissibleTokens(const gason::JsonNode& node) {
	if (!node.empty() && !node.isArray()) {
		throw Error{errParams, "'admissible_replication_tokens' node must be the array"};
	}

	NsNamesHashMapT<std::string> res;
	for (const auto& tokenNode : node) {
		if (!tokenNode.isObject()) {
			throw Error{errParams, "Object with admissible tokens has incorrect type"};
		}
		const auto& token = tokenNode["token"];
		if (token.empty() || (token.isObject() || token.isArray())) {
			throw Error{errParams, "Field with token value is not filled in or has the incorrect type (expected string scalar)"};
		}
		const auto& tokenVal = token.As<std::string>();
		const auto& nss = tokenNode["namespaces"];

		if (nss.empty() || !nss.isArray()) {
			throw Error{errParams,
						"Field with namespaces for currect token {} is not filled in or has the incorrect type (expected array of strings)",
						tokenVal};
		}

		for (const auto& ns : nss) {
			if (token.isObject() || token.isArray()) {
				throw Error{errParams, "Namespace name must be the string"};
			}

			if (auto [_, wasInserted] = res.insert({NamespaceName(ns.As<std::string>()), tokenVal}); !wasInserted) {
				throw Error{errParams, "Namespace lists for different admissible tokens should not intersect"};
			}
		}
	}
	return res;
}

void ProfilingConfigData::GetJSON(JsonBuilder& jb) const {
	jb.Put("queriesperfstats", queriesPerfStats.load());
	jb.Put("queries_threshold_us", queriesThresholdUS.load());
	jb.Put("perfstats", perfStats.load());
	jb.Put("memstats", memStats.load());
	jb.Put("activitystats", activityStats.load());

	const auto select_lg_params = longSelectLoggingParams.load(std::memory_order_relaxed);
	const auto update_delete_lg_params = longUpdDelLoggingParams.load(std::memory_order_relaxed);
	const auto transaction_lg_params = longTxLoggingParams.load(std::memory_order_relaxed);
	jb.Object("long_queries_logging")
		.Object("select")
		.Put("threshold_us", select_lg_params.thresholdUs)
		.Put("normalized", bool(select_lg_params.normalized))
		.End()
		.Object("update_delete")
		.Put("threshold_us", update_delete_lg_params.thresholdUs)
		.Put("normalized", bool(update_delete_lg_params.normalized))
		.End()
		.Object("transaction")
		.Put("threshold_us", transaction_lg_params.thresholdUs)
		.Put("avg_step_threshold_us", transaction_lg_params.avgTxStepThresholdUs)
		.End();
}

Error ReplicationConfigData::FromDefault() noexcept {
	try {
		gason::JsonParser parser;
		gason::JsonNode configJson = parser.Parse(kDefReplicationConfig);
		auto& replicationJson = configJson["replication"];
		if (!replicationJson.isObject()) {
			return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefReplicationConfig");
		}

		Error err = FromJSON(replicationJson);
		if (!err.ok()) {
			return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefReplicationConfig: {}", err.what());
		}
	}
	CATCH_AND_RETURN

	return ErrorCode::errOK;
}

Error ReplicationConfigData::FromYAML(const std::string& yaml) {
	try {
		YAML::Node root = YAML::Load(yaml);
		clusterID = root["cluster_id"].as<int>(clusterID);
		serverID = root["server_id"].as<int>(serverID);
		if (auto err = Validate(); !err.ok()) {
			return Error(errParams, "ReplicationConfigData: YAML parsing error: '{}'", err.whatStr());
		}
		admissibleTokens = getAdmissibleTokens(root["admissible_replication_tokens"]);
	} catch (const YAML::Exception& ex) {
		return Error(errParseYAML, "ReplicationConfigData: YAML parsing error: '{}'", ex.what());
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
		return Error(errParseJson, "ReplicationConfigData: {}\n", ex.what());
	} catch (const std::exception& err) {
		return err;
	}
}

Error ReplicationConfigData::FromJSON(const gason::JsonNode& root) {
	using namespace std::string_view_literals;
	std::string errorString;
	auto err = tryReadOptionalJsonValue(&errorString, root, "cluster_id"sv, clusterID);
	err = tryReadOptionalJsonValue(&errorString, root, "server_id"sv, serverID);
	try {
		admissibleTokens = getAdmissibleTokens(root["admissible_replication_tokens"]);
	} catch (std::exception& e) {
		ensureEndsWith(errorString, "\n") += e.what();
	}
	(void)err;	// ignored; Errors will be handled with errorString

	if (errorString.empty()) {
		if (auto err = Validate()) {
			return Error(errParseJson, "ReplicationConfigData: JSON parsing error: '{}'", err.whatStr());
		}
	} else {
		return Error(errParseJson, "ProfilingConfigData: JSON parsing error: '{}'", errorString);
	}

	return Error();
}

void ReplicationConfigData::FromJSONWithCorrection(const gason::JsonNode& root, Error& fixedErrors) {
	using namespace std::string_view_literals;
	fixedErrors = Error();
	std::string errorString;

	if (!tryReadOptionalJsonValue(&errorString, root, "cluster_id"sv, clusterID).ok()) {
		clusterID = 1;
		ensureEndsWith(errorString, "\n") += fmt::format("Fixing to default: cluster_id = {}.", clusterID);
	}

	if (!tryReadOptionalJsonValue(&errorString, root, "server_id"sv, serverID, ReplicationConfigData::kMinServerIDValue,
								  ReplicationConfigData::kMaxServerIDValue)
			 .ok()) {
		serverID = 0;
		ensureEndsWith(errorString, "\n") += fmt::format("Fixing to default: server_id = {}.", serverID);
	}

	try {
		admissibleTokens = getAdmissibleTokens(root["admissible_replication_tokens"]);
	} catch (const std::exception& err) {
		ensureEndsWith(errorString, "\n") += err.what();
		admissibleTokens.clear();
		ensureEndsWith(errorString, "\n") += "Fixing to default: admissible_replication_tokens = {}.";
	}

	if (!errorString.empty()) {
		fixedErrors = Error(errParseJson, errorString);
	}
}

void ReplicationConfigData::GetJSON(JsonBuilder& jb) const {
	jb.Put("cluster_id", clusterID);
	jb.Put("server_id", serverID);

	fast_hash_map<std::string_view, std::vector<std::string_view>> tokenNss;
	for (const auto& [nsName, token] : admissibleTokens) {
		tokenNss[token].emplace_back(nsName);
	}
	if (!tokenNss.empty()) {
		auto admissibleArr = jb.Array("admissible_replication_tokens");
		for (const auto& [token, nss] : tokenNss) {
			auto tokenObj = admissibleArr.Object(TagName::Empty());
			tokenObj.Put("token", token);
			auto nssArr = tokenObj.Array("namespaces");
			for (const auto& ns : nss) {
				nssArr.Put(TagName::Empty(), std::string_view(ns));
			}
		}
	}
}

void ReplicationConfigData::GetYAML(WrSerializer& ser) const {
	fast_hash_map<std::string_view, std::vector<std::string_view>> tokenNss;
	for (const auto& [nsName, token] : admissibleTokens) {
		tokenNss[token].emplace_back(nsName);
	}

	YAML::Node admissibleArr(YAML::NodeType::Sequence);
	if (!tokenNss.empty()) {
		for (const auto& [token, nss] : tokenNss) {
			YAML::Node tokenObj(YAML::NodeType::Map);
			tokenObj["token"] = token;
			for (const auto& ns : nss) {
				tokenObj["namespaces"].push_back(ns);
			}
			admissibleArr["admissible_replication_tokens"].push_back(tokenObj);
		}
	}

	// clang-format off
	ser <<	"# Cluser ID - must be same for client and for master\n"
			"cluster_id: " + std::to_string(clusterID) + "\n"
			"\n"
			"# Server ID - must be unique for all nodes value in range [0, 999]\n"
			"server_id: " + std::to_string(serverID) + "\n"
			"# Synchronization namespaces admissible tokens\n"
			+ (admissibleArr.size() > 0 ? YAML::Dump(admissibleArr) : std::string()) + "\n"
			"\n";
	// clang-format on
}

Error ReplicationConfigData::Validate() const {
	using namespace std::string_view_literals;
	if (serverID < ReplicationConfigData::kMinServerIDValue || serverID > ReplicationConfigData::kMaxServerIDValue) {
		return Error(errParams, "Value of '{}' = {} is out of bounds: [{}, {}]", "server_Id"sv, serverID,
					 ReplicationConfigData::kMinServerIDValue, ReplicationConfigData::kMaxServerIDValue);
	}
	return Error();
}

std::ostream& operator<<(std::ostream& os, const ReplicationConfigData& data) {
	return os << "{ server_id: " << data.serverID << "; cluster_id: " << data.clusterID << "}";
}

Error NamespaceConfigData::FromDefault(std::vector<std::string>& defaultNamespacesNames,
									   std::vector<NamespaceConfigData>& defaultNamespacesConfs) noexcept {
	using namespace std::string_view_literals;
	try {
		gason::JsonParser parser;
		gason::JsonNode configJson = parser.Parse(kDefNamespacesConfig);
		std::string errorString;
		Error err;

		auto& namespacesNode = configJson["namespaces"];
		if (!namespacesNode.isArray()) {
			return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefNamespacesConfig: no namespaces");
		}

		defaultNamespacesNames.clear();
		defaultNamespacesNames.reserve(10);
		defaultNamespacesConfs.clear();
		defaultNamespacesConfs.reserve(10);

		for (const gason::JsonNode& namespaceNode : namespacesNode.value) {
			std::string_view namespaceName = ""sv;
			if (!tryReadOptionalJsonValue(&errorString, namespaceNode, "namespace"sv, namespaceName).ok() || namespaceName.empty()) {
				return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefNamespacesConfig: no namespace name");
			}

			defaultNamespacesNames.emplace_back(namespaceName);
			defaultNamespacesConfs.emplace_back();
			Error err = defaultNamespacesConfs[defaultNamespacesConfs.size() - 1].FromJSON(namespaceNode);
			if (!err.ok()) {
				return Error(ErrorCode::errInvalidDefConfigs, "Incorrect kDefNamespacesConfig: {}", err.what());
			}
		}
	}
	CATCH_AND_RETURN

	return ErrorCode::errOK;
}

Error NamespaceConfigData::GetJSON(const std::vector<std::string>& namespacesNames, const std::vector<NamespaceConfigData>& namespacesConfs,
								   JsonBuilder& jb) noexcept {
	if (namespacesNames.size() != namespacesConfs.size()) {
		return Error(ErrorCode::errLogic, "Incorrect args for NamespaceConfigData::GetJSON");
	}

	jb.Put("type", "namespaces");
	auto arr = jb.Array("namespaces");
	for (size_t i = 0; i < namespacesConfs.size(); i++) {
		const std::string& namespaceName = namespacesNames[i];
		const NamespaceConfigData& namespaceConf = namespacesConfs[i];

		JsonBuilder obj = arr.Object();
		obj.Put("namespace", namespaceName);
		namespaceConf.GetJSON(obj);
		obj.End();
	}

	return ErrorCode::errOK;
}

Error NamespaceConfigData::FromJSON(const gason::JsonNode& v) {
	using namespace std::string_view_literals;
	std::string errorString;
	Error err;

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
	err = tryReadOptionalJsonValue(&errorString, v, "ann_storage_cache_build_timeout_ms"sv, annStorageCacheBuildTimeout, 0);
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
		(void)err;	// ignored; Errors will be handled with errorString
	}

	if (!errorString.empty()) {
		return Error(errParseJson, "NamespaceConfigData: JSON parsing error: '{}'", errorString);
	}
	return {};
}

void NamespaceConfigData::GetJSON(JsonBuilder& jb) const {
	jb.Put("log_level", logLevelToString(logLevel));
	jb.Put("strict_mode", strictModeToString(strictMode));
	jb.Put("join_cache_mode", cacheMode2str(cacheMode));

	jb.Put("start_copy_policy_tx_size", startCopyPolicyTxSize);
	jb.Put("copy_policy_multiplier", copyPolicyMultiplier);
	jb.Put("tx_size_to_always_copy", txSizeToAlwaysCopy);
	jb.Put("tx_vec_insertion_threads", txVecInsertionThreads);
	jb.Put("optimization_timeout_ms", optimizationTimeout);
	jb.Put("optimization_sort_workers", optimizationSortWorkers);
	jb.Put("wal_size", walSize);

	jb.Put("min_preselect_size", minPreselectSize);
	jb.Put("max_preselect_size", maxPreselectSize);
	jb.Put("max_preselect_part", maxPreselectPart);
	jb.Put("max_iterations_idset_preresult", maxIterationsIdSetPreResult);
	jb.Put("index_updates_counting_mode", idxUpdatesCountingMode);
	jb.Put("sync_storage_flush_limit", syncStorageFlushLimit);
	jb.Put("ann_storage_cache_build_timeout_ms", annStorageCacheBuildTimeout);

	jb.Object("cache")
		.Put("index_idset_cache_size", cacheConfig.idxIdsetCacheSize)
		.Put("index_idset_hits_to_cache", cacheConfig.idxIdsetHitsToCache)
		.Put("ft_index_cache_size", cacheConfig.ftIdxCacheSize)
		.Put("ft_index_hits_to_cache", cacheConfig.ftIdxHitsToCache)
		.Put("joins_preselect_cache_size", cacheConfig.joinCacheSize)
		.Put("joins_preselect_hit_to_cache", cacheConfig.joinHitsToCache)
		.Put("query_count_cache_size", cacheConfig.queryCountCacheSize)
		.Put("query_count_hit_to_cache", cacheConfig.queryCountHitsToCache);
}

template <class ConfigType, typename... Args>
Error GetDefaultConfigImpl(std::string_view type, JsonBuilder& builder, Args... args) {
	builder.Put("type", type);
	ConfigType cfg;
	Error err = cfg.FromDefault();
	if (!err.ok()) {
		return err;
	}
	JsonBuilder obj = builder.Object(type);
	cfg.GetJSON(obj, args...);

	builder.End();
	return err;
}

Error GetDefaultNamespacesConfigsImpl(JsonBuilder& builder) noexcept {
	std::vector<std::string> defaultNamespacesNames;
	std::vector<NamespaceConfigData> defaultNamespacesConfs;
	Error err = NamespaceConfigData::FromDefault(defaultNamespacesNames, defaultNamespacesConfs);
	if (!err.ok()) {
		return err;
	}
	err = NamespaceConfigData::GetJSON(defaultNamespacesNames, defaultNamespacesConfs, builder);
	if (!err.ok()) {
		return err;
	}
	builder.End();

	return err;
}

Error GetDefaultConfigs(std::string_view type, JsonBuilder& builder) noexcept {
	using namespace std::literals;
	try {
		if (type == "profiling"sv) {
			return GetDefaultConfigImpl<ProfilingConfigData>(type, builder);
		} else if (type == "namespaces"sv) {
			return GetDefaultNamespacesConfigsImpl(builder);
		} else if (type == "replication"sv) {
			return GetDefaultConfigImpl<ReplicationConfigData>(type, builder);
		} else if (type == "async_replication"sv) {
			return GetDefaultConfigImpl<cluster::AsyncReplConfigData>(type, builder, cluster::MaskingDSN::Disabled);
		}
	}
	CATCH_AND_RETURN

	return Error{ErrorCode::errNotFound, "Unknown default config type"};
}

}  // namespace reindexer
