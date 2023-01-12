#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include "estl/fast_hash_set.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {
class JsonBuilder;
class RdxContext;
class WrSerializer;

enum ConfigType { ProfilingConf, NamespaceDataConf, ReplicationConf };

struct LongQueriesLoggingParams {
	int32_t thresholdUs = -1;
	bool normalized = false;
};

struct LongTxLoggingParams {
	LongTxLoggingParams() noexcept : thresholdUs(-1), avgTxStepThresholdUs(-1) {}

	// Do not using 2 int32's here due to MSVC compatibility reasons (alignof should not be less than sizeof in this case to use it in
	// atomic).
	// Starting from C++14 both of the bit fields will be signed.
	int64_t thresholdUs : 32;
	int64_t avgTxStepThresholdUs : 32;
};

struct ProfilingConfigData {
	bool queriesPerfStats = false;
	size_t queriedThresholdUS = 10;
	bool perfStats = false;
	bool memStats = false;
	bool activityStats = false;
	LongQueriesLoggingParams longSelectLoggingParams;
	LongQueriesLoggingParams longUpdDelLoggingParams;
	LongTxLoggingParams longTxLoggingParams;
};

struct NamespaceConfigData {
	bool lazyLoad = false;
	int noQueryIdleThreshold = 0;
	LogLevel logLevel = LogNone;
	CacheMode cacheMode = CacheModeOff;
	StrictMode strictMode = StrictModeNames;
	int startCopyPolicyTxSize = 10000;
	int copyPolicyMultiplier = 5;
	int txSizeToAlwaysCopy = 100000;
	int optimizationTimeout = 800;
	int optimizationSortWorkers = 4;
	int64_t walSize = 4000000;
	int64_t minPreselectSize = 1000;
	int64_t maxPreselectSize = 1000;
	double maxPreselectPart = 0.1;
	bool idxUpdatesCountingMode = false;
	int syncStorageFlushLimit = 20000;
};

enum ReplicationRole { ReplicationNone, ReplicationMaster, ReplicationSlave, ReplicationReadOnly };

struct ReplicationConfigData {
	Error FromYML(const std::string &yml);
	Error FromJSON(const gason::JsonNode &v);
	void GetJSON(JsonBuilder &jb) const;
	void GetYAML(WrSerializer &ser) const;

	ReplicationRole role = ReplicationNone;
	std::string masterDSN;
	std::string appName = "rx_slave";
	int connPoolSize = 1;
	int workerThreads = 1;
	int clusterID = 1;
	int timeoutSec = 60;
	int retrySyncIntervalSec = 20;
	int onlineReplErrorsThreshold = 100;
	bool forceSyncOnLogicError = false;
	bool forceSyncOnWrongDataHash = false;
	fast_hash_set<std::string, nocase_hash_str, nocase_equal_str> namespaces;
	bool enableCompression = true;
	int serverId = 0;

	bool operator==(const ReplicationConfigData &rdata) const noexcept {
		return (role == rdata.role) && (connPoolSize == rdata.connPoolSize) && (workerThreads == rdata.workerThreads) &&
			   (clusterID == rdata.clusterID) && (forceSyncOnLogicError == rdata.forceSyncOnLogicError) &&
			   (forceSyncOnWrongDataHash == rdata.forceSyncOnWrongDataHash) && (masterDSN == rdata.masterDSN) &&
			   (retrySyncIntervalSec == rdata.retrySyncIntervalSec) && (onlineReplErrorsThreshold == rdata.onlineReplErrorsThreshold) &&
			   (timeoutSec == rdata.timeoutSec) && (namespaces == rdata.namespaces) && (enableCompression == rdata.enableCompression) &&
			   (serverId == rdata.serverId) && (appName == rdata.appName);
	}
	bool operator!=(const ReplicationConfigData &rdata) const noexcept { return !operator==(rdata); }

protected:
	static ReplicationRole str2role(const std::string &);
	static std::string role2str(ReplicationRole) noexcept;
};

class DBConfigProvider {
public:
	DBConfigProvider() = default;
	~DBConfigProvider() = default;
	DBConfigProvider(DBConfigProvider &obj) = delete;
	DBConfigProvider &operator=(DBConfigProvider &obj) = delete;

	Error FromJSON(const gason::JsonNode &root);
	void setHandler(ConfigType cfgType, std::function<void()> handler);

	ProfilingConfigData GetProfilingConfig();
	ReplicationConfigData GetReplicationConfig();
	bool GetNamespaceConfig(const std::string &nsName, NamespaceConfigData &data);
	LongQueriesLoggingParams GetSelectLoggingParams();
	LongQueriesLoggingParams GetUpdDelLoggingParams();
	LongTxLoggingParams GetTxLoggingParams();
	bool ActivityStatsEnabled();

private:
	ProfilingConfigData profilingData_;
	ReplicationConfigData replicationData_;
	std::unordered_map<std::string, NamespaceConfigData> namespacesData_;
	std::unordered_map<int, std::function<void()>> handlers_;
	shared_timed_mutex mtx_;
};

}  // namespace reindexer
