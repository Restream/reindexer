#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include "estl/fast_hash_set.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

union JsonValue;

namespace reindexer {
class JsonBuilder;

enum ConfigType { ProfilingConf, NamespaceDataConf, ReplicationConf };

struct ProfilingConfigData {
	bool queriesPerfStats = false;
	size_t queriedThresholdUS = 10;
	bool perfStats = false;
	bool memStats = false;
};

struct NamespaceConfigData {
	bool lazyLoad = false;
	int noQueryIdleThreshold = 0;
	LogLevel logLevel = LogNone;
	CacheMode cacheMode = CacheModeOff;
};

enum ReplicationRole { ReplicationNone, ReplicationMaster, ReplicationSlave };

struct ReplicationConfigData {
	Error FromYML(const string &yml);
	Error FromJSON(JsonValue &v);
	void GetJSON(JsonBuilder &jb);

	ReplicationRole role = ReplicationNone;
	std::string masterDSN;
	int connPoolSize = 1;
	int workerThreads = 1;
	int clusterID = 1;
	bool forceSyncOnLogicError = false;
	bool forceSyncOnWrongDataHash = false;
	fast_hash_set<string, nocase_hash_str, nocase_equal_str> namespaces;

protected:
	ReplicationRole str2role(const string &);
	std::string role2str(ReplicationRole);
};

class DBConfigProvider {
public:
	DBConfigProvider() = default;
	~DBConfigProvider() = default;
	DBConfigProvider(DBConfigProvider &obj) = delete;
	DBConfigProvider &operator=(DBConfigProvider &obj) = delete;

	Error FromJSON(JsonValue &v);
	void SetReplicationConfig(const ReplicationConfigData &conf);
	void setHandler(ConfigType cfgType, std::function<void()> handler);

	ProfilingConfigData GetProfilingConfig();
	ReplicationConfigData GetReplicationConfig();
	bool GetNamespaceConfig(const string &nsName, NamespaceConfigData &data);

private:
	ProfilingConfigData profilingData_;
	ReplicationConfigData replicationData_;
	std::unordered_map<string, NamespaceConfigData> namespacesData_;
	std::unordered_map<int, std::function<void()>> handlers_;
	shared_timed_mutex mtx_;
};

}  // namespace reindexer
