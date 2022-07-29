#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include "cluster/config.h"
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

enum ConfigType { ProfilingConf, NamespaceDataConf, AsyncReplicationConf, ReplicationConf };

struct ProfilingConfigData {
	bool queriesPerfStats = false;
	size_t queriedThresholdUS = 10;
	bool perfStats = false;
	bool memStats = false;
	bool activityStats = false;
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
};

struct ReplicationConfigData {
	int serverID = 0;
	int clusterID = 1;

	Error FromYML(const string &yml);
	Error FromJSON(std::string_view json);
	Error FromJSON(const gason::JsonNode &v);
	void GetJSON(JsonBuilder &jb) const;
	void GetYAML(WrSerializer &ser) const;

	bool operator==(const ReplicationConfigData &rdata) const noexcept {
		return (clusterID == rdata.clusterID) && (serverID == rdata.serverID);
	}
	bool operator!=(const ReplicationConfigData &rdata) const noexcept { return !operator==(rdata); }
};

class DBConfigProvider {
public:
	DBConfigProvider() = default;
	~DBConfigProvider() = default;
	DBConfigProvider(DBConfigProvider &obj) = delete;
	DBConfigProvider &operator=(DBConfigProvider &obj) = delete;

	Error FromJSON(const gason::JsonNode &root);
	void setHandler(ConfigType cfgType, std::function<void()> handler);
	int setHandler(std::function<void(ReplicationConfigData)> handler);
	void unsetHandler(int id);

	ProfilingConfigData GetProfilingConfig();
	cluster::AsyncReplConfigData GetAsyncReplicationConfig();
	ReplicationConfigData GetReplicationConfig();
	bool GetNamespaceConfig(const string &nsName, NamespaceConfigData &data);

private:
	ProfilingConfigData profilingData_;
	cluster::AsyncReplConfigData asyncReplicationData_;
	ReplicationConfigData replicationData_;
	std::unordered_map<string, NamespaceConfigData> namespacesData_;
	std::unordered_map<int, std::function<void()>> handlers_;
	std::unordered_map<int, std::function<void(ReplicationConfigData)>> replicationConfigDataHandlers_;
	int HandlersCounter_ = 0;
	shared_timed_mutex mtx_;
};

}  // namespace reindexer
