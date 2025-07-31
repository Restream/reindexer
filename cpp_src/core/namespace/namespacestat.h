#pragma once

#include <stdlib.h>
#include <span>
#include <string>
#include <vector>
#include "namespacename.h"
#include "tools/errors.h"
#include "tools/lsn.h"

namespace reindexer {

class WrSerializer;

struct LRUCacheMemStat {
	void GetJSON(JsonBuilder& builder) const;

	size_t totalSize = 0;
	size_t itemsCount = 0;
	size_t emptyCount = 0;
	size_t hitCountLimit = 0;
};

struct IndexMemStat {
	void GetJSON(JsonBuilder& builder) const;
	std::string name;
	size_t uniqKeysCount = 0;
	size_t dataSize = 0;
	size_t idsetBTreeSize = 0;
	size_t idsetPlainSize = 0;
	size_t sortOrdersSize = 0;
	size_t indexingStructSize = 0;
	size_t columnSize = 0;
	size_t trackedUpdatesCount = 0;
	size_t trackedUpdatesBuckets = 0;
	size_t trackedUpdatesSize = 0;
	size_t trackedUpdatesOverflow = 0;
	std::optional<bool> isBuilt;  // KNN-indexes|fast-text indexes only
	LRUCacheMemStat idsetCache;
	size_t GetFullIndexStructSize() const noexcept {
		return idsetPlainSize + idsetBTreeSize + sortOrdersSize + columnSize + trackedUpdatesSize + indexingStructSize;
	}
};

struct EmbeddersCacheMemStat {
	void GetJSON(JsonBuilder& builder) const;
	std::string tag;
	size_t capacity = 0;
	LRUCacheMemStat cache;
	bool storageOK = false;
	bool storageEnabled = false;
	std::string storageStatus;
	std::string storagePath;
	uint64_t storageSize = 0;
};

struct ClusterOperationStatus {
	void GetJSON(WrSerializer& ser) const;
	void GetJSON(JsonBuilder& builder) const;
	Error FromJSON(std::span<char> json);
	void FromJSON(const gason::JsonNode& root);

	enum class Role { None, ClusterReplica, SimpleReplica };

	std::string_view RoleStr() const noexcept {
		using namespace std::string_view_literals;
		switch (role) {
			case Role::None:
				return "none"sv;
			case Role::ClusterReplica:
				return "cluster_replica"sv;
			case Role::SimpleReplica:
				return "simple_replica"sv;
		}
		return "<unknown>"sv;
	}

	int leaderId = -1;
	Role role = Role::None;
};

struct ReplicationState {
	enum class Status { None, Idle, Error, Fatal, Syncing };

	virtual ~ReplicationState() = default;

	virtual void GetJSON(JsonBuilder& builder) const;
	void FromJSON(std::span<char>);

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
	// ClusterOperation status
	ClusterOperationStatus clusterStatus;
	// Shows, that namespaces was replicated in v3.
	// Required for the transition process only
	bool wasV3ReplicatedNS = false;
	// Admissible synchronization token
	std::string token;
};

// TODO: Rename this
struct ReplicationStateV2 {
	constexpr static int64_t kNoDataCount = -1;

	bool HasDataCount() const noexcept { return dataCount != kNoDataCount; }
	void GetJSON(JsonBuilder& builder) const;
	void FromJSON(std::span<char>);

	// LSN of last change
	// updated from WAL when querying the structure
	lsn_t lastLsn;
	uint64_t dataHash = 0;
	// This field is optional - older rx versions do not have it
	int64_t dataCount = kNoDataCount;
	lsn_t nsVersion;
	//
	ClusterOperationStatus clusterStatus;
};

struct ReplicationStat final : public ReplicationState {
	void GetJSON(JsonBuilder& builder) const override;

	size_t walCount = 0;
	size_t walSize = 0;
	int16_t serverId = 0;
};

struct NamespaceMemStat {
	static constexpr std::string_view kNamespaceStatType{"namespace"};
	static constexpr std::string_view kEmbeddersStatType{"embedders"};

	void GetJSON(WrSerializer& ser) const;

	NamespaceName name;
	std::string type;
	std::string storagePath;
	bool storageOK = false;
	bool storageEnabled = false;
	std::string storageStatus;
	bool optimizationCompleted = false;
	size_t itemsCount = 0;
	size_t emptyItemsCount = 0;
	size_t stringsWaitingToBeDeletedSize = 0;
	struct {
		size_t dataSize = 0;
		size_t indexesSize = 0;
		size_t cacheSize = 0;
		size_t indexOptimizerMemory = 0;
		size_t inmemoryStorageSize = 0;
	} Total;
	struct {
		size_t proxySize = 0;
		// TODO: Uncomment affter calculation async batches size in the AsyncStorage + add in calc of Total.inmemoryStorageSize
		// size_t asyncBatchesSize = 0;
	} Storage;
	ReplicationStat replication;
	LRUCacheMemStat joinCache;
	LRUCacheMemStat queryCache;
	std::vector<IndexMemStat> indexes;
	std::vector<EmbeddersCacheMemStat> embedders;
};

struct LRUCachePerfStat {
	enum class State { DoesNotExist, Active, Inactive };

	void GetJSON(JsonBuilder& builder) const;
	uint64_t TotalQueries() const noexcept;
	double HitRate() const noexcept;

	State state = State::DoesNotExist;
	uint64_t hits = 0;
	uint64_t misses = 0;
};

struct PerfStat {
	void GetJSON(JsonBuilder& builder) const;

	size_t totalHitCount;
	size_t totalAvgTimeUs;
	size_t totalAvgLockTimeUs;
	size_t lastSecHitCount;
	size_t lastSecAvgTimeUs;
	size_t lastSecAvgLockTimeUs;
	double stddev;
	size_t minTimeUs;
	size_t maxTimeUs;
};

struct TxPerfStat {
	void GetJSON(JsonBuilder& builder) const;
	void FromJSON(const gason::JsonNode& node);

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

struct EmbedderCachePerfStat : LRUCachePerfStat {
	void GetJSON(JsonBuilder& builder) const;

	std::string tag;
};

struct IndexPerfStat {
	IndexPerfStat() = default;
	IndexPerfStat(const std::string& n, PerfStat&& s, PerfStat&& c) : name(n), selects(std::move(s)), commits(std::move(c)) {}

	void GetJSON(JsonBuilder& builder) const;

	std::string name;
	PerfStat selects;
	PerfStat commits;
	LRUCachePerfStat cache;
	EmbedderCachePerfStat upsertEmbedderCache;
	EmbedderCachePerfStat queryEmbedderCache;
};

struct NamespacePerfStat {
	void GetJSON(WrSerializer& ser) const;

	NamespaceName name;
	PerfStat updates;
	PerfStat selects;
	TxPerfStat transactions;
	std::vector<IndexPerfStat> indexes;
	LRUCachePerfStat joinCache;
	LRUCachePerfStat queryCountCache;
};

}  // namespace reindexer
