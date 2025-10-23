#include "dbconfig.h"
#include <bitset>
#include "cjson/jsonbuilder.h"
#include "defnsconfigs.h"
#include "estl/lock.h"
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
	using namespace std::string_view_literals;
	std::bitset<kConfigTypesTotalCount> typesChanged;

	std::string errLogString;
	try {
		smart_lock lk(mtx_, Unique);

		ProfilingConfigData profilingDataSafe;
		const auto& profilingNode = root["profiling"sv];
		if (!profilingNode.empty()) {
			profilingDataLoadResult_ = profilingDataSafe.FromJSON(profilingNode);

			if (profilingDataLoadResult_.ok()) {
				typesChanged.set(ProfilingConf);
			} else {
				errLogString += profilingDataLoadResult_.whatStr();
			}
		}

		fast_hash_map<std::string, NamespaceConfigData, nocase_hash_str, nocase_equal_str, nocase_less_str> namespacesData;
		const auto& namespacesNode = root["namespaces"sv];
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
				const auto err = data.FromJSON(nsNode);
				if (err.ok()) {
					namespacesData.emplace(std::move(nsName),
										   std::move(data));  // NOLINT(performance-move-const-arg)
				} else {
					ensureEndsWith(namespacesErrLogString, "\n") += err.whatStr();
					nssHaveErrors = true;
				}
			}

			if (!nssHaveErrors) {
				namespacesDataLoadResult_ = ErrorCode::errOK;
				typesChanged.set(NamespaceDataConf);
			} else {
				namespacesDataLoadResult_ = Error(ErrorCode::errParseJson, namespacesErrLogString);
				ensureEndsWith(errLogString, "\n") += "NamespacesConfig: JSON parsing error: " + namespacesErrLogString;
			}
		}

		cluster::AsyncReplConfigData asyncReplConfigDataSafe;
		const auto& asyncReplicationNode = root[kAsyncReplicationCfgName];
		if (!asyncReplicationNode.empty()) {
			asyncReplicationDataLoadResult_ = asyncReplConfigDataSafe.FromJSON(asyncReplicationNode);

			if (asyncReplicationDataLoadResult_.ok()) {
				typesChanged.set(AsyncReplicationConf);
			} else {
				ensureEndsWith(errLogString, "\n") += asyncReplicationDataLoadResult_.whatStr();
			}
		}

		ReplicationConfigData replicationDataSafe;
		const auto& replicationNode = root["replication"sv];
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

		fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str> embeddersData;
		const auto& embeddersNode = root["caches"sv];
		if (!embeddersNode.empty()) {
			std::string embeddersErrLogString;
			bool embeddersHaveErrors = false;
			for (const auto& cacheNode : embeddersNode) {
				EmbeddersConfigData data;
				const auto err = data.FromJSON(cacheNode);
				if (err.ok()) {
					embeddersData.emplace(toLower(data.cacheTag), std::move(data.configData));	// NOLINT(performance-move-const-arg)
				} else {
					ensureEndsWith(embeddersErrLogString, "\n") += err.whatStr();
					embeddersHaveErrors = true;
				}
			}

			if (!embeddersHaveErrors) {
				embeddersDataLoadResult_ = ErrorCode::errOK;
				typesChanged.set(EmbeddersConf);
			} else {
				embeddersDataLoadResult_ = Error(ErrorCode::errParseJson, embeddersErrLogString);
				ensureEndsWith(errLogString, "\n") += "EmbeddersConfig: JSON parsing error: " + embeddersErrLogString;
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
			if (typesChanged.test(EmbeddersConf)) {
				embeddersData_ = std::move(embeddersData);
			}
		}
	} catch (const std::exception& ex) {
		ensureEndsWith(errLogString, "\n") += ex.what();
	}

	Error result;
	if (!errLogString.empty()) {
		result = {ErrorCode::errParseJson, "DBConfigProvider: {}", errLogString};
	} else if (typesChanged.size() > 0) {
		// notifying handlers under shared_lock so none of them go out of scope
		smart_lock lk(mtx_, NonUnique);

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
	smart_lock lk(mtx_, NonUnique);
	if (!profilingDataLoadResult_.ok() || !namespacesDataLoadResult_.ok() || !asyncReplicationDataLoadResult_.ok() ||
		!replicationDataLoadResult_.ok() || !embeddersDataLoadResult_.ok()) {
		std::string errLogString(profilingDataLoadResult_.whatStr());
		ensureEndsWith(errLogString, "\n"sv) += namespacesDataLoadResult_.whatStr();
		ensureEndsWith(errLogString, "\n"sv) += asyncReplicationDataLoadResult_.whatStr();
		ensureEndsWith(errLogString, "\n"sv) += replicationDataLoadResult_.whatStr();
		ensureEndsWith(errLogString, "\n"sv) += embeddersDataLoadResult_.whatStr();

		return {ErrorCode::errParseJson, "DBConfigProvider: {}", errLogString};
	}

	return {};
}

void DBConfigProvider::setHandler(ConfigType cfgType, std::function<void()> handler) {
	lock_guard lk(mtx_);
	handlers_[cfgType] = std::move(handler);
}

int DBConfigProvider::setHandler(std::function<void(const ReplicationConfigData&)> handler) {
	smart_lock<shared_timed_mutex> lk(mtx_, Unique);
	replicationConfigDataHandlers_[++handlersCounter_] = std::move(handler);
	return handlersCounter_;
}

void DBConfigProvider::unsetHandler(int id) {
	smart_lock<shared_timed_mutex> lk(mtx_, Unique);
	replicationConfigDataHandlers_.erase(id);
}

const fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str>& DBConfigProvider::GetEmbeddersConfig() const {
	shared_lock<shared_timed_mutex> lk(mtx_);
	return embeddersData_;
}
ReplicationConfigData DBConfigProvider::GetReplicationConfig() const {
	shared_lock<shared_timed_mutex> lk(mtx_);
	return replicationData_;
}

cluster::AsyncReplConfigData DBConfigProvider::GetAsyncReplicationConfig() const {
	smart_lock<shared_timed_mutex> lk(mtx_, NonUnique);
	return asyncReplicationData_;
}

Error DBConfigProvider::CheckAsyncReplicationToken(std::string_view nsName, std::string_view token) const {
	if (auto nsToken = GetAsyncReplicationToken(nsName); !nsToken.empty() && nsToken != token) {
		return Error(errReplParams, "Different replication tokens. Expected '{}', but got '{}'", nsToken, token);
	}
	return {};
}

std::string DBConfigProvider::GetAsyncReplicationToken(std::string_view nsName) const {
	smart_lock<shared_timed_mutex> lk(mtx_, NonUnique);
	const auto& tokens = replicationData_.admissibleTokens;
	if (auto it = tokens.find(nsName); it != tokens.end()) {
		return it->second;
	} else if (it = tokens.find("*"); it != tokens.end()) {
		return it->second;
	}
	return {};
}

void DBConfigProvider::GetNamespaceConfig(std::string_view nsName, NamespaceConfigData& data) const {
	using namespace std::string_view_literals;
	shared_lock<shared_timed_mutex> lk(mtx_);
	auto it = namespacesData_.find(nsName);
	if (it == namespacesData_.end()) {
		it = namespacesData_.find("*"sv);
	}
	if (it == namespacesData_.end()) {
		data = {};
		return;
	}
	data = it->second;
}

Error ProfilingConfigData::FromDefault() noexcept {
	using namespace std::string_view_literals;
	try {
		gason::JsonParser parser;
		const gason::JsonNode configJson = parser.Parse(kDefProfilingConfig);
		const auto& profilingJson = configJson["profiling"sv];
		if (!profilingJson.isObject()) {
			return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefProfilingConfig"};
		}

		const auto err = FromJSON(profilingJson);
		if (!err.ok()) {
			return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefProfilingConfig: {}", err.what()};
		}
	}
	CATCH_AND_RETURN

	return {};
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

	const auto& longQueriesLogging = v["long_queries_logging"sv];
	if (longQueriesLogging.isObject()) {
		const auto& select = longQueriesLogging["select"sv];
		if (select.isObject()) {
			const auto p = longSelectLoggingParams.load(std::memory_order_relaxed);
			int32_t thresholdUs = p.thresholdUs;
			bool normalized = p.normalized;
			err = tryReadOptionalJsonValue(&errorString, select, "threshold_us"sv, thresholdUs);
			err = tryReadOptionalJsonValue(&errorString, select, "normalized"sv, normalized);
			(void)err;	// ignored; Errors will be handled with errorString
			longSelectLoggingParams.store(LongQueriesLoggingParams(thresholdUs, normalized), std::memory_order_relaxed);
		}

		const auto& updateDelete = longQueriesLogging["update_delete"sv];
		if (updateDelete.isObject()) {
			const auto p = longUpdDelLoggingParams.load(std::memory_order_relaxed);
			int32_t thresholdUs = p.thresholdUs;
			bool normalized = p.normalized;
			err = tryReadOptionalJsonValue(&errorString, updateDelete, "threshold_us"sv, thresholdUs);
			err = tryReadOptionalJsonValue(&errorString, updateDelete, "normalized"sv, normalized);
			(void)err;	// ignored; Errors will be handled with errorString
			longUpdDelLoggingParams.store(LongQueriesLoggingParams(thresholdUs, normalized), std::memory_order_relaxed);
		}

		const auto& transaction = longQueriesLogging["transaction"sv];
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
	using namespace std::string_view_literals;
	jb.Put("queriesperfstats"sv, queriesPerfStats.load());
	jb.Put("queries_threshold_us"sv, queriesThresholdUS.load());
	jb.Put("perfstats"sv, perfStats.load());
	jb.Put("memstats"sv, memStats.load());
	jb.Put("activitystats"sv, activityStats.load());

	const auto select_lg_params = longSelectLoggingParams.load(std::memory_order_relaxed);
	const auto update_delete_lg_params = longUpdDelLoggingParams.load(std::memory_order_relaxed);
	const auto transaction_lg_params = longTxLoggingParams.load(std::memory_order_relaxed);
	auto lql = jb.Object("long_queries_logging"sv);

	auto select = lql.Object("select"sv);
	select.Put("threshold_us"sv, select_lg_params.thresholdUs);
	select.Put("normalized"sv, bool(select_lg_params.normalized));
	select.End();

	auto updateDelete = lql.Object("update_delete"sv);
	updateDelete.Put("threshold_us"sv, update_delete_lg_params.thresholdUs);
	updateDelete.Put("normalized"sv, bool(update_delete_lg_params.normalized));
	updateDelete.End();

	auto transaction = lql.Object("transaction"sv);
	transaction.Put("threshold_us"sv, transaction_lg_params.thresholdUs);
	transaction.Put("avg_step_threshold_us"sv, transaction_lg_params.avgTxStepThresholdUs);
	transaction.End();

	lql.End();
}

Error ReplicationConfigData::FromDefault() noexcept {
	using namespace std::string_view_literals;
	try {
		gason::JsonParser parser;
		const gason::JsonNode configJson = parser.Parse(kDefReplicationConfig);
		const auto& replicationJson = configJson["replication"sv];
		if (!replicationJson.isObject()) {
			return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefReplicationConfig"};
		}

		const auto err = FromJSON(replicationJson);
		if (!err.ok()) {
			return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefReplicationConfig: {}", err.what()};
		}
	}
	CATCH_AND_RETURN

	return {};
}

Error ReplicationConfigData::FromYAML(const std::string& yaml) {
	try {
		YAML::Node root = YAML::Load(yaml);
		clusterID = root["cluster_id"].as<int>(clusterID);
		serverID = root["server_id"].as<int>(serverID);
		if (auto err = Validate(); !err.ok()) {
			return {ErrorCode::errParams, "ReplicationConfigData: YAML parsing error: '{}'", err.whatStr()};
		}
		admissibleTokens = getAdmissibleTokens(root["admissible_replication_tokens"]);
	} catch (const std::exception& err) {
		return {ErrorCode::errParseYAML, "ReplicationConfigData: YAML parsing error: '{}'", err.what()};
	}
	return {};
}

Error ReplicationConfigData::FromJSON(std::string_view json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const std::exception& err) {
		return {ErrorCode::errParseJson, "ReplicationConfigData: {}\n", err.what()};
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
		if (err = Validate(); !err.ok()) {
			return {ErrorCode::errParseJson, "ReplicationConfigData: JSON parsing error: '{}'", err.whatStr()};
		}
	} else {
		return {ErrorCode::errParseJson, "ProfilingConfigData: JSON parsing error: '{}'", errorString};
	}

	return {};
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
		fixedErrors = Error(ErrorCode::errParseJson, errorString);
	}
}

void ReplicationConfigData::GetJSON(JsonBuilder& jb) const {
	using namespace std::string_view_literals;
	jb.Put("cluster_id"sv, clusterID);
	jb.Put("server_id"sv, serverID);

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
	ser <<	"# Cluster ID - must be same for client and for master\n"
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
		return {ErrorCode::errParams, "Value of 'server_id' = {} is out of bounds: [{}, {}]", serverID,
				ReplicationConfigData::kMinServerIDValue, ReplicationConfigData::kMaxServerIDValue};
	}
	return {};
}

std::ostream& operator<<(std::ostream& os, const ReplicationConfigData& data) {
	return os << "{ server_id: " << data.serverID << "; cluster_id: " << data.clusterID << "}";
}

Error NamespaceConfigData::FromDefault(std::vector<std::string>& defaultNamespacesNames,
									   std::vector<NamespaceConfigData>& defaultNamespacesConfs) noexcept {
	using namespace std::string_view_literals;
	try {
		gason::JsonParser parser;
		const gason::JsonNode configJson = parser.Parse(kDefNamespacesConfig);
		std::string errorString;

		const auto& namespacesNode = configJson["namespaces"sv];
		if (!namespacesNode.isArray()) {
			return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefNamespacesConfig: no namespaces"};
		}

		defaultNamespacesNames.resize(0);
		defaultNamespacesNames.reserve(10);
		defaultNamespacesConfs.resize(0);
		defaultNamespacesConfs.reserve(10);

		std::string namespaceName;
		for (const gason::JsonNode& namespaceNode : namespacesNode.value) {
			if (!tryReadOptionalJsonValue(&errorString, namespaceNode, "namespace"sv, namespaceName).ok() || namespaceName.empty()) {
				return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefNamespacesConfig: no namespace name"};
			}

			defaultNamespacesNames.emplace_back(namespaceName);
			defaultNamespacesConfs.emplace_back();
			const auto err = defaultNamespacesConfs[defaultNamespacesConfs.size() - 1].FromJSON(namespaceNode);
			if (!err.ok()) {
				return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefNamespacesConfig: {}", err.what()};
			}
		}
	}
	CATCH_AND_RETURN

	return {};
}

Error NamespaceConfigData::GetJSON(const std::vector<std::string>& namespacesNames, const std::vector<NamespaceConfigData>& namespacesConfs,
								   JsonBuilder& jb) noexcept {
	using namespace std::string_view_literals;
	if (namespacesNames.size() != namespacesConfs.size()) {
		return {ErrorCode::errLogic, "Incorrect args for NamespaceConfigData::GetJSON"};
	}

	jb.Put("type"sv, "namespaces"sv);
	auto arr = jb.Array("namespaces"sv);
	for (size_t i = 0; i < namespacesConfs.size(); i++) {
		const std::string& namespaceName = namespacesNames[i];
		const NamespaceConfigData& namespaceConf = namespacesConfs[i];

		JsonBuilder obj = arr.Object();
		obj.Put("namespace"sv, namespaceName);
		namespaceConf.GetJSON(obj);
		obj.End();
	}

	return {};
}

Error NamespaceConfigData::FromJSON(const gason::JsonNode& v) {
	using namespace std::string_view_literals;
	std::string errorString;

	std::string stringVal(logLevelToString(logLevel));
	if (tryReadOptionalJsonValue(&errorString, v, "log_level"sv, stringVal).ok()) {
		logLevel = logLevelFromString(stringVal);
	}

	stringVal = strictModeToString(strictMode);
	if (tryReadOptionalJsonValue(&errorString, v, "strict_mode"sv, stringVal).ok()) {
		strictMode = strictModeFromString(stringVal);
	}

	auto stringViewVal = "off"sv;
	if (tryReadOptionalJsonValue(&errorString, v, "join_cache_mode"sv, stringViewVal).ok()) {
		try {
			cacheMode = str2cacheMode(stringViewVal);
		} catch (Error& err) {
			ensureEndsWith(errorString, "\n") += "errParams: " + err.whatStr();
		}
	}

	auto err = tryReadOptionalJsonValue(&errorString, v, "start_copy_policy_tx_size"sv, startCopyPolicyTxSize);
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

	const auto cacheNode = v["cache"];
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
		return {ErrorCode::errParseJson, "NamespaceConfigData: JSON parsing error: '{}'", errorString};
	}
	return {};
}

void NamespaceConfigData::GetJSON(JsonBuilder& jb) const {
	using namespace std::string_view_literals;
	jb.Put("log_level"sv, logLevelToString(logLevel));
	jb.Put("strict_mode"sv, strictModeToString(strictMode));
	jb.Put("join_cache_mode"sv, cacheMode2str(cacheMode));

	jb.Put("start_copy_policy_tx_size"sv, startCopyPolicyTxSize);
	jb.Put("copy_policy_multiplier"sv, copyPolicyMultiplier);
	jb.Put("tx_size_to_always_copy"sv, txSizeToAlwaysCopy);
	jb.Put("tx_vec_insertion_threads"sv, txVecInsertionThreads);
	jb.Put("optimization_timeout_ms"sv, optimizationTimeout);
	jb.Put("optimization_sort_workers"sv, optimizationSortWorkers);
	jb.Put("wal_size"sv, walSize);

	jb.Put("min_preselect_size"sv, minPreselectSize);
	jb.Put("max_preselect_size"sv, maxPreselectSize);
	jb.Put("max_preselect_part"sv, maxPreselectPart);
	jb.Put("max_iterations_idset_preresult"sv, maxIterationsIdSetPreResult);
	jb.Put("index_updates_counting_mode"sv, idxUpdatesCountingMode);
	jb.Put("sync_storage_flush_limit"sv, syncStorageFlushLimit);
	jb.Put("ann_storage_cache_build_timeout_ms"sv, annStorageCacheBuildTimeout);

	auto c = jb.Object("cache"sv);
	c.Put("index_idset_cache_size"sv, cacheConfig.idxIdsetCacheSize);
	c.Put("index_idset_hits_to_cache"sv, cacheConfig.idxIdsetHitsToCache);
	c.Put("ft_index_cache_size"sv, cacheConfig.ftIdxCacheSize);
	c.Put("ft_index_hits_to_cache"sv, cacheConfig.ftIdxHitsToCache);
	c.Put("joins_preselect_cache_size"sv, cacheConfig.joinCacheSize);
	c.Put("joins_preselect_hit_to_cache"sv, cacheConfig.joinHitsToCache);
	c.Put("query_count_cache_size"sv, cacheConfig.queryCountCacheSize);
	c.Put("query_count_hit_to_cache"sv, cacheConfig.queryCountHitsToCache);
	c.End();
}

Error EmbeddersConfigData::FromDefault(std::vector<EmbeddersConfigData>& defaultEmbeddersConfs) noexcept {
	using namespace std::string_view_literals;
	try {
		defaultEmbeddersConfs.resize(0);

		gason::JsonParser parser;
		const gason::JsonNode configJson = parser.Parse(kDefEmbeddersConfig);

		const auto& cachesNode = configJson["caches"sv];
		if (cachesNode.empty()) {
			return {};	// NOTE: optional
		}

		if (!cachesNode.isArray()) {
			return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefEmbeddersConfig: no caches"};
		}

		for (const auto& cacheNode : cachesNode) {
			EmbeddersConfigData data;
			const auto err = data.FromJSON(cacheNode);
			if (!err.ok()) {
				return {ErrorCode::errInvalidDefConfigs, "Incorrect kDefEmbeddersConfig: {}", err.what()};
			}
			defaultEmbeddersConfs.emplace_back(data);
		}
	}
	CATCH_AND_RETURN

	return {};
}

Error EmbeddersConfigData::GetJSON(const std::vector<EmbeddersConfigData>& embeddersConfs, JsonBuilder& jb) noexcept {
	using namespace std::string_view_literals;
	jb.Put("type"sv, "embedders");
	auto arr = jb.Array("caches"sv);
	for (const auto& cache : embeddersConfs) {
		JsonBuilder obj = arr.Object();
		cache.GetJSON(obj);
		obj.End();
	}

	return {};
}

Error EmbeddersConfigData::FromJSON(const gason::JsonNode& root) {
	using namespace std::string_view_literals;
	std::string errorString;
	auto err = tryReadRequiredJsonValue(&errorString, root, "cache_tag"sv, cacheTag);
	err = tryReadOptionalJsonValue(&errorString, root, "max_cache_items"sv, configData.maxCacheItems, 0);
	err = tryReadOptionalJsonValue(&errorString, root, "hit_to_cache"sv, configData.hitToCache, 0);
	(void)err;	// ignored; Errors will be handled with errorString
	if (!errorString.empty()) {
		return {ErrorCode::errParseJson, "EmbeddersConfigData: JSON parsing error: '{}'", errorString};
	}
	return {};
}

void EmbeddersConfigData::GetJSON(JsonBuilder& jb) const {
	using namespace std::string_view_literals;
	jb.Put("cache_tag"sv, cacheTag);
	jb.Put("max_cache_items"sv, configData.maxCacheItems);
	jb.Put("hit_to_cache"sv, configData.hitToCache);
}

template <class ConfigType, typename... Args>
Error GetDefaultConfigImpl(std::string_view type, JsonBuilder& builder, Args... args) {
	using namespace std::string_view_literals;
	builder.Put("type"sv, type);
	ConfigType cfg;
	auto err = cfg.FromDefault();
	if (!err.ok()) {
		return err;
	}
	JsonBuilder obj = builder.Object(type);
	cfg.GetJSON(obj, args...);

	builder.End();
	return {};
}

Error GetDefaultNamespacesConfigsImpl(JsonBuilder& builder) noexcept {
	std::vector<std::string> defaultNamespacesNames;
	std::vector<NamespaceConfigData> defaultNamespacesConfs;
	auto err = NamespaceConfigData::FromDefault(defaultNamespacesNames, defaultNamespacesConfs);
	if (err.ok()) {
		err = NamespaceConfigData::GetJSON(defaultNamespacesNames, defaultNamespacesConfs, builder);
	}
	if (!err.ok()) {
		return err;
	}
	builder.End();

	return {};
}

Error GetDefaultEmbeddersConfigImpl(JsonBuilder& builder) noexcept {
	std::vector<EmbeddersConfigData> defaultEmbeddersConfs;
	auto err = EmbeddersConfigData::FromDefault(defaultEmbeddersConfs);
	if (err.ok()) {
		err = EmbeddersConfigData::GetJSON(defaultEmbeddersConfs, builder);
	}
	if (!err.ok()) {
		return err;
	}
	builder.End();

	return {};
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
		} else if (type == "embedders"sv) {
			return GetDefaultEmbeddersConfigImpl(builder);
		}
	}
	CATCH_AND_RETURN

	return {ErrorCode::errNotFound, "Unknown default config type"};
}

}  // namespace reindexer
