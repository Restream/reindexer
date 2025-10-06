#pragma once

#include <functional>
#include <ostream>
#include <string>
#include "cluster/config.h"
#include "core/namespace/namespacenamemap.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "estl/thread_annotation_attributes.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {
class RdxContext;
class WrSerializer;

enum ConfigType {
	ProfilingConf = 0,
	NamespaceDataConf,
	AsyncReplicationConf,
	ReplicationConf,
	EmbeddersConf,
	//
	kConfigTypesTotalCount
};

class [[nodiscard]] LongQueriesLoggingParams {
public:
	LongQueriesLoggingParams(int32_t t = -1, bool n = false) noexcept : thresholdUs(t), normalized(n ? 1 : 0) {}

	// Don't use int32 + bool here due to MSVC compatibility reasons (alignof should not be less than sizeof in this case to use it in
	// atomic).
	int64_t thresholdUs : 32;
	int64_t normalized : 1;
};

class [[nodiscard]] LongTxLoggingParams {
public:
	LongTxLoggingParams(int32_t t = -1, int32_t a = -1) noexcept : thresholdUs(t), avgTxStepThresholdUs(a) {}

	// Don't use 2 int32's here due to MSVC compatibility reasons (alignof should not be less than sizeof in this case to use it in
	// atomic).
	// Starting from C++14 both of the bit fields will be signed.
	int64_t thresholdUs : 32;
	int64_t avgTxStepThresholdUs : 32;
};

class [[nodiscard]] ProfilingConfigData {
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

	Error FromDefault() noexcept;
	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb) const;

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

struct [[nodiscard]] NamespaceCacheConfigData {
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

struct [[nodiscard]] NamespaceConfigData {
	LogLevel logLevel = LogNone;
	CacheMode cacheMode = CacheModeOff;
	StrictMode strictMode = StrictModeNames;
	int startCopyPolicyTxSize = 10'000;
	int copyPolicyMultiplier = 5;
	int txSizeToAlwaysCopy = 100'000;
	int txVecInsertionThreads = 4;
	int optimizationTimeout = 800;
	int optimizationSortWorkers = 4;
	int64_t walSize = 4'000'000;
	int64_t minPreselectSize = 1'000;
	int64_t maxPreselectSize = 1'000;
	double maxPreselectPart = 0.1;
	int64_t maxIterationsIdSetPreResult = 20'000;
	bool idxUpdatesCountingMode = false;
	int syncStorageFlushLimit = 20'000;
	int annStorageCacheBuildTimeout = 5'000;
	NamespaceCacheConfigData cacheConfig;

	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb) const;

	static Error FromDefault(std::vector<std::string>& defaultNamespacesNames,
							 std::vector<NamespaceConfigData>& defaultNamespacesConfs) noexcept;
	static Error GetJSON(const std::vector<std::string>& namespacesNames, const std::vector<NamespaceConfigData>& namespacesConfs,
						 JsonBuilder& jb) noexcept;
};

struct [[nodiscard]] ReplicationConfigData {
	static constexpr int16_t kMinServerIDValue = 0;
	static constexpr int16_t kMaxServerIDValue = 999;

	int serverID = 0;
	int clusterID = 1;
	NsNamesHashMapT<std::string> admissibleTokens;

	Error FromDefault() noexcept;
	Error FromYAML(const std::string& yml);
	Error FromJSON(std::string_view json);
	Error FromJSON(const gason::JsonNode& v);
	void FromJSONWithCorrection(const gason::JsonNode& v, Error& fixedErrors);
	Error Validate() const;

	void GetJSON(JsonBuilder& jb) const;
	void GetYAML(WrSerializer& ser) const;

	bool operator==(const ReplicationConfigData& rdata) const noexcept = default;
};

std::ostream& operator<<(std::ostream& os, const ReplicationConfigData& data);

struct [[nodiscard]] EmbedderConfigData {
	int maxCacheItems = 0;	// Don't touch, disabled by default (=0)
	int hitToCache = 1;
	bool operator==(const EmbedderConfigData&) const noexcept = default;
};
struct [[nodiscard]] EmbeddersConfigData {
	std::string cacheTag;
	EmbedderConfigData configData;

	Error FromJSON(const gason::JsonNode& v);
	void GetJSON(JsonBuilder& jb) const;

	static Error FromDefault(std::vector<EmbeddersConfigData>& defaultEmbeddersConfs) noexcept;
	static Error GetJSON(const std::vector<EmbeddersConfigData>& embeddersConfs, JsonBuilder& jb) noexcept;
};

class [[nodiscard]] DBConfigProvider {
public:
	DBConfigProvider() = default;
	~DBConfigProvider() = default;
	DBConfigProvider(DBConfigProvider& obj) = delete;
	DBConfigProvider& operator=(DBConfigProvider& obj) = delete;
	/**
	 * @brief Safely reads config or config part from JSON
	 * @param root JSON node, that contain config data for one or multiple sections
	 * @param autoCorrect - fail-safe switch.
	 *   - If \c true - function will try to read as much as possible from \c root, replacing failed
	 * values with default-ones and logging every fail section-wise. Final config read in this mode will be in a consistent state in any
	 * case.
	 *   - If \c false - function will try to parse whole JSON. If the read is successful, config will be applied at once. In case of read
	 * errors, all of them will be recorded section-wise and then joined to one composite error of type \c errParseJSON and returned to
	 * caller; the entire configuration will not be changed and will remain in the state prior to this call.
	 * @returns Error
	 * - If Error.ok() - either read is successful and configuration is updated, either JSON is correct, but nothing to read and
	 * nothing to change.
	 * Otherwise - read errors occured, config is not changed.
	 */
	Error FromJSON(const gason::JsonNode& root, bool autoCorrect = false) RX_REQUIRES(!mtx_);

	/// @brief Returns aggregated error state generated by previous calls to FromJSON
	Error GetConfigParseErrors() const RX_REQUIRES(!mtx_);

	void setHandler(ConfigType cfgType, std::function<void()> handler) RX_REQUIRES(!mtx_);
	int setHandler(std::function<void(const ReplicationConfigData&)> handler) RX_REQUIRES(!mtx_);
	void unsetHandler(int id) RX_REQUIRES(!mtx_);

	const fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str>& GetEmbeddersConfig() const RX_REQUIRES(!mtx_);
	cluster::AsyncReplConfigData GetAsyncReplicationConfig() const RX_REQUIRES(!mtx_);
	ReplicationConfigData GetReplicationConfig() const RX_REQUIRES(!mtx_);
	void GetNamespaceConfig(std::string_view nsName, NamespaceConfigData& data) const RX_REQUIRES(!mtx_);
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

	Error CheckAsyncReplicationToken(std::string_view nsName, std::string_view token) const RX_REQUIRES(!mtx_);
	std::string GetAsyncReplicationToken(std::string_view nsName) const RX_REQUIRES(!mtx_);

private:
	ProfilingConfigData profilingData_;
	cluster::AsyncReplConfigData asyncReplicationData_;
	ReplicationConfigData replicationData_;
	Error profilingDataLoadResult_;
	Error namespacesDataLoadResult_;
	Error asyncReplicationDataLoadResult_;
	Error replicationDataLoadResult_;
	Error embeddersDataLoadResult_;
	fast_hash_map<std::string, NamespaceConfigData, nocase_hash_str, nocase_equal_str, nocase_less_str> namespacesData_;
	fast_hash_map<std::string, EmbedderConfigData, hash_str, equal_str, less_str> embeddersData_;
	std::array<std::function<void()>, kConfigTypesTotalCount> handlers_;
	fast_hash_map<int, std::function<void(const ReplicationConfigData&)>> replicationConfigDataHandlers_;
	int handlersCounter_ = 0;
	mutable shared_timed_mutex mtx_;
};

Error GetDefaultConfigs(std::string_view type, JsonBuilder& builder) noexcept;

}  // namespace reindexer
