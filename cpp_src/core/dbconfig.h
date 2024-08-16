#pragma once

#include <functional>
#include <string>
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
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

enum ConfigType {
	ProfilingConf = 0,
	NamespaceDataConf,
	ReplicationConf,
	//
	kConfigTypesTotalCount
};

class LongQueriesLoggingParams {
public:
	LongQueriesLoggingParams(int32_t t = -1, bool n = false) noexcept : thresholdUs(t), normalized(n ? 1 : 0) {}

	// Do not using int32 + bool here due to MSVC compatibility reasons (alignof should not be less than sizeof in this case to use it in
	// atomic).
	int64_t thresholdUs : 32;
	int64_t normalized : 1;
};

class LongTxLoggingParams {
public:
	LongTxLoggingParams(int32_t t = -1, int32_t a = -1) noexcept : thresholdUs(t), avgTxStepThresholdUs(a) {}

	// Do not using 2 int32's here due to MSVC compatibility reasons (alignof should not be less than sizeof in this case to use it in
	// atomic).
	// Starting from C++14 both of the bit fields will be signed.
	int64_t thresholdUs : 32;
	int64_t avgTxStepThresholdUs : 32;
};

class ProfilingConfigData {
public:
	ProfilingConfigData& operator=(const ProfilingConfigData& d) noexcept {
		queriesThresholdUS.store(d.queriesThresholdUS, std::memory_order_relaxed);
		queriesPerfStats.store(d.queriesPerfStats, std::memory_order_relaxed);
		perfStats.store(d.perfStats, std::memory_order_relaxed);
		memStats.store(d.memStats, std::memory_order_relaxed);
		activityStats.store(d.activityStats, std::memory_order_relaxed);
		longSelectLoggingParams.store(d.longSelectLoggingParams, std::memory_order_relaxed);
		longUpdDelLoggingParams.store(d.longUpdDelLoggingParams, std::memory_order_relaxed);
		longTxLoggingParams.store(d.longTxLoggingParams, std::memory_order_relaxed);
		return *this;
	}

	std::atomic<size_t> queriesThresholdUS = {10};
	std::atomic<bool> queriesPerfStats = {false};
	std::atomic<bool> perfStats = {false};
	std::atomic<bool> memStats = {false};
	std::atomic<bool> activityStats = {false};
	std::atomic<LongQueriesLoggingParams> longSelectLoggingParams;
	std::atomic<LongQueriesLoggingParams> longUpdDelLoggingParams;
	std::atomic<LongTxLoggingParams> longTxLoggingParams;
};

constexpr size_t kDefaultCacheSizeLimit = 1024 * 1024 * 128;
constexpr uint32_t kDefaultHitCountToCache = 2;

struct NamespaceCacheConfigData {
	bool IsIndexesCacheEqual(const NamespaceCacheConfigData& o) noexcept {
		return idxIdsetCacheSize == o.idxIdsetCacheSize && idxIdsetHitsToCache == o.idxIdsetHitsToCache &&
			   ftIdxCacheSize == o.ftIdxCacheSize && ftIdxHitsToCache == o.ftIdxHitsToCache;
	}
	bool IsJoinCacheEqual(const NamespaceCacheConfigData& o) noexcept {
		return joinCacheSize == o.joinCacheSize && joinHitsToCache == o.joinHitsToCache;
	}
	bool IsQueryCountCacheEqual(const NamespaceCacheConfigData& o) noexcept {
		return queryCountCacheSize == o.queryCountCacheSize && queryCountHitsToCache == o.queryCountHitsToCache;
	}

	uint64_t idxIdsetCacheSize = kDefaultCacheSizeLimit;
	uint32_t idxIdsetHitsToCache = kDefaultHitCountToCache;
	uint64_t ftIdxCacheSize = kDefaultCacheSizeLimit;
	uint32_t ftIdxHitsToCache = kDefaultHitCountToCache;
	uint64_t joinCacheSize = 2 * kDefaultCacheSizeLimit;
	uint32_t joinHitsToCache = kDefaultHitCountToCache;
	uint64_t queryCountCacheSize = kDefaultCacheSizeLimit;
	uint32_t queryCountHitsToCache = kDefaultHitCountToCache;
};

struct NamespaceConfigData {
	bool lazyLoad = false;
	int noQueryIdleThreshold = 0;
	LogLevel logLevel = LogNone;
	CacheMode cacheMode = CacheModeOff;
	StrictMode strictMode = StrictModeNames;
	int startCopyPolicyTxSize = 10'000;
	int copyPolicyMultiplier = 5;
	int txSizeToAlwaysCopy = 100'000;
	int optimizationTimeout = 800;
	int optimizationSortWorkers = 4;
	int64_t walSize = 4'000'000;
	int64_t minPreselectSize = 1'000;
	int64_t maxPreselectSize = 1'000;
	double maxPreselectPart = 0.1;
	int64_t maxIterationsIdSetPreResult = 20'000;
	bool idxUpdatesCountingMode = false;
	int syncStorageFlushLimit = 20'000;
	NamespaceCacheConfigData cacheConfig;
};

enum ReplicationRole { ReplicationNone, ReplicationMaster, ReplicationSlave, ReplicationReadOnly };
inline constexpr int format_as(ReplicationRole v) noexcept { return int(v); }

struct ReplicationConfigData {
	Error FromYML(const std::string& yml);
	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb) const;
	void GetYAML(WrSerializer& ser) const;

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
	fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> namespaces;
	bool enableCompression = true;
	int serverId = 0;

	bool operator==(const ReplicationConfigData& rdata) const noexcept {
		return (role == rdata.role) && (connPoolSize == rdata.connPoolSize) && (workerThreads == rdata.workerThreads) &&
			   (clusterID == rdata.clusterID) && (forceSyncOnLogicError == rdata.forceSyncOnLogicError) &&
			   (forceSyncOnWrongDataHash == rdata.forceSyncOnWrongDataHash) && (masterDSN == rdata.masterDSN) &&
			   (retrySyncIntervalSec == rdata.retrySyncIntervalSec) && (onlineReplErrorsThreshold == rdata.onlineReplErrorsThreshold) &&
			   (timeoutSec == rdata.timeoutSec) && (namespaces == rdata.namespaces) && (enableCompression == rdata.enableCompression) &&
			   (serverId == rdata.serverId) && (appName == rdata.appName);
	}
	bool operator!=(const ReplicationConfigData& rdata) const noexcept { return !operator==(rdata); }

protected:
	static ReplicationRole str2role(const std::string&);
	static std::string role2str(ReplicationRole) noexcept;
};

class DBConfigProvider {
public:
	DBConfigProvider() = default;
	~DBConfigProvider() = default;
	DBConfigProvider(DBConfigProvider& obj) = delete;
	DBConfigProvider& operator=(DBConfigProvider& obj) = delete;

	Error FromJSON(const gason::JsonNode& root);
	void setHandler(ConfigType cfgType, std::function<void()> handler);

	ReplicationConfigData GetReplicationConfig();
	bool GetNamespaceConfig(std::string_view nsName, NamespaceConfigData& data);
	LongQueriesLoggingParams GetSelectLoggingParams() const noexcept {
		return profilingData_.longSelectLoggingParams.load(std::memory_order_relaxed);
	}
	LongQueriesLoggingParams GetUpdDelLoggingParams() const noexcept {
		return profilingData_.longUpdDelLoggingParams.load(std::memory_order_relaxed);
	}
	LongTxLoggingParams GetTxLoggingParams() const noexcept { return profilingData_.longTxLoggingParams.load(std::memory_order_relaxed); }
	bool ActivityStatsEnabled() const noexcept { return profilingData_.activityStats.load(std::memory_order_relaxed); }
	bool MemStatsEnabled() const noexcept { return profilingData_.memStats.load(std::memory_order_relaxed); }
	bool PerfStatsEnabled() const noexcept { return profilingData_.perfStats.load(std::memory_order_relaxed); }
	bool QueriesPerfStatsEnabled() const noexcept { return profilingData_.queriesPerfStats.load(std::memory_order_relaxed); }
	unsigned QueriesThresholdUS() const noexcept { return profilingData_.queriesThresholdUS.load(std::memory_order_relaxed); }

private:
	ProfilingConfigData profilingData_;
	ReplicationConfigData replicationData_;
	fast_hash_map<std::string, NamespaceConfigData, hash_str, equal_str, less_str> namespacesData_;
	std::array<std::function<void()>, kConfigTypesTotalCount> handlers_;
	shared_timed_mutex mtx_;
};

}  // namespace reindexer
