#pragma once

#include <stdlib.h>
#include <chrono>
#include <mutex>
#include <string>
#include <vector>
#include "estl/span.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/lsn.h"

namespace reindexer {

class WrSerializer;
class JsonBuilder;

struct LRUCacheMemStat {
	void GetJSON(JsonBuilder &builder);

	size_t totalSize = 0;
	size_t itemsCount = 0;
	size_t emptyCount = 0;
	size_t hitCountLimit = 0;
};

struct IndexMemStat {
	void GetJSON(JsonBuilder &builder);
	std::string name;
	size_t uniqKeysCount = 0;
	size_t dataSize = 0;
	size_t idsetBTreeSize = 0;
	size_t idsetPlainSize = 0;
	size_t sortOrdersSize = 0;
	size_t fulltextSize = 0;
	size_t columnSize = 0;
	size_t trackedUpdatesCount = 0;
	size_t trackedUpdatesBuckets = 0;
	size_t trackedUpdatesSize = 0;
	size_t trackedUpdatesOveflow = 0;
	LRUCacheMemStat idsetCache;
	size_t GetIndexStructSize() const noexcept {
		return idsetPlainSize + idsetBTreeSize + sortOrdersSize + fulltextSize + columnSize + trackedUpdatesSize;
	}
};

struct ClusterizationStatus {
	void GetJSON(WrSerializer &ser) const;
	void GetJSON(JsonBuilder &builder) const;
	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode &root);

	enum class Role { None, ClusterReplica, SimpleReplica };

	int leaderId = -1;
	Role role = Role::None;
};

struct ReplicationState {
	enum class Status { None, Idle, Error, Fatal, Syncing };

	void GetJSON(JsonBuilder &builder);
	void FromJSON(span<char>);

	// LSN of last change
	// updated from WAL when querying the structure
	lsn_t lastLsn;
	// Temporary namespace flag
	bool temporary = false;
	// Incarnation counter
	int incarnationCounter = 0;
	// Data hash
	uint64_t dataHash = 0;
	// Data count
	int dataCount = 0;
	// Data updated
	uint64_t updatedUnixNano = 0;
	// Namespace version
	lsn_t nsVersion;
	// Clusterization status
	ClusterizationStatus clusterStatus;
	// Shows, that namespaces was replicated in v3.
	// Required for the transition process only
	bool wasV3ReplicatedNS = false;
};

// TODO: Rename this
struct ReplicationStateV2 {
	void GetJSON(JsonBuilder &builder);
	void FromJSON(span<char>);

	// LSN of last change
	// updated from WAL when querying the structure
	lsn_t lastLsn;
	uint64_t dataHash = 0;
	lsn_t nsVersion;
	//
	ClusterizationStatus clusterStatus;
};

struct ReplicationStat : public ReplicationState {
	void GetJSON(JsonBuilder &builder);
	size_t walCount = 0;
	size_t walSize = 0;
	int16_t serverId = 0;
};

struct NamespaceMemStat {
	void GetJSON(WrSerializer &ser);

	std::string name;
	std::string storagePath;
	bool storageOK = false;
	bool storageEnabled = false;
	std::string storageStatus;
	bool storageLoaded = true;
	bool optimizationCompleted = false;
	size_t itemsCount = 0;
	size_t emptyItemsCount = 0;
	size_t stringsWaitingToBeDeletedSize = 0;
	struct {
		size_t dataSize = 0;
		size_t indexesSize = 0;
		size_t cacheSize = 0;
		size_t indexOptimizerMemory = 0;
	} Total;
	ReplicationStat replication;
	LRUCacheMemStat joinCache;
	LRUCacheMemStat queryCache;
	std::vector<IndexMemStat> indexes;
};

struct PerfStat {
	void GetJSON(JsonBuilder &builder);

	size_t totalHitCount;
	size_t totalTimeUs;
	size_t totalLockTimeUs;
	size_t avgHitCount;
	size_t avgTimeUs;
	size_t avgLockTimeUs;
	double stddev;
	size_t minTimeUs;
	size_t maxTimeUs;
};

struct TxPerfStat {
	void GetJSON(JsonBuilder &builder);

	size_t totalCount;
	size_t totalCopyCount;
	size_t avgStepsCount;
	size_t minStepsCount;
	size_t maxStepsCount;
	size_t avgPrepareTimeUs;
	size_t minPrepareTimeUs;
	size_t maxPrepareTimeUs;
	size_t avgCommitTimeUs;
	size_t minCommitTimeUs;
	size_t maxCommitTimeUs;
	size_t avgCopyTimeUs;
	size_t minCopyTimeUs;
	size_t maxCopyTimeUs;
};

struct IndexPerfStat {
	IndexPerfStat() = default;
	IndexPerfStat(const std::string &n, const PerfStat &s, const PerfStat &c) : name(n), selects(s), commits(c) {}

	void GetJSON(JsonBuilder &builder);

	std::string name;
	PerfStat selects;
	PerfStat commits;
};

struct NamespacePerfStat {
	void GetJSON(WrSerializer &ser);

	std::string name;
	PerfStat updates;
	PerfStat selects;
	TxPerfStat transactions;
	std::vector<IndexPerfStat> indexes;
};

}  // namespace reindexer
