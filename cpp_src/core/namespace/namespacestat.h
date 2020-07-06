#pragma once

#include <stdlib.h>
#include <chrono>
#include <mutex>
#include <string>
#include <vector>
#include "core/lsn.h"
#include "estl/span.h"
#include "gason/gason.h"
#include "tools/errors.h"

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
	LRUCacheMemStat idsetCache;
};

struct MasterState {
	void GetJSON(JsonBuilder &builder);
	void FromJSON(span<char>);
	void FromJSON(const gason::JsonNode &root);

	// LSN of last change
	lsn_t lastUpstreamLSNm;
	// Data hash
	uint64_t dataHash = 0;
	// Data count
	int dataCount = 0;
	// Data updated
	uint64_t updatedUnixNano = 0;
};

struct ReplicationState {
	enum class Status { None, Idle, Error, Fatal, Syncing };

	void GetJSON(JsonBuilder &builder);
	void FromJSON(span<char>);

	// LSN of last change
	// updated from WAL when querying the structure
	lsn_t lastLsn;
	// Slave mode flag (only read operation enabled)
	bool slaveMode = false;
	// enable replication
	bool replicatorEnabled = false;
	// Temporary namespace flag
	bool temporary = false;
	// Replication error
	Error replError = errOK;
	// Incarnation counter
	int incarnationCounter = 0;
	// Data hash
	uint64_t dataHash = 0;
	// Data count
	int dataCount = 0;
	// Data updated
	uint64_t updatedUnixNano = 0;
	// Current replication status
	Status status = Status::None;
	// Current master state
	MasterState masterState;

	lsn_t originLSN;
	lsn_t lastSelfLSN;
	lsn_t lastUpstreamLSN;
};

struct ReplicationStat : public ReplicationState {
	void GetJSON(JsonBuilder &builder);
	size_t walCount = 0;
	size_t walSize = 0;
};

struct NamespaceMemStat {
	void GetJSON(WrSerializer &ser);

	std::string name;
	std::string storagePath;
	bool storageOK = false;
	bool storageLoaded = true;
	bool optimizationCompleted = false;
	size_t itemsCount = 0;
	size_t emptyItemsCount = 0;
	size_t dataSize = 0;
	struct {
		size_t dataSize = 0;
		size_t indexesSize = 0;
		size_t cacheSize = 0;
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
