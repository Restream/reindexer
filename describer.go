package reindexer

import (
	"context"
	"strings"

	"github.com/restream/reindexer/v5/bindings"
)

const (
	ConfigNamespaceName           = "#config"
	MemstatsNamespaceName         = "#memstats"
	NamespacesNamespaceName       = "#namespaces"
	PerfstatsNamespaceName        = "#perfstats"
	QueriesperfstatsNamespaceName = "#queriesperfstats"
	ClientsStatsNamespaceName     = "#clientsstats"
	ReplicationStatsNamespaceName = "#replicationstats"
)

const (
	// Reconnect to the next node in the list
	ReconnectStrategyNext = ReconnectStrategy("next")
	// Reconnect to the random node in the list
	ReconnectStrategyRandom = ReconnectStrategy("random")
	// Reconnect to the synchronized node (which was the part of the last consensus in synchronous cluster)
	ReconnectStrategySynchronized = ReconnectStrategy("synchronized")
	// Always choose cluster's leader
	ReconnectStrategyPrefferWrite = ReconnectStrategy("preffer_write")
	// Always choose cluster's follower
	ReconnectStrategyReadOnly = ReconnectStrategy("read_only")
	// Choose follower, when it's possible. Otherwise reconnect to leader
	ReconnectStrategyPrefferRead = ReconnectStrategy("preffer_read")
)

type ReconnectStrategy string

// Map from cond name to index type
var queryTypes = map[string]int{
	"EQ":      EQ,
	"GT":      GT,
	"LT":      LT,
	"GE":      GE,
	"LE":      LE,
	"SET":     SET,
	"RANGE":   RANGE,
	"ANY":     ANY,
	"EMPTY":   EMPTY,
	"ALLSET":  ALLSET,
	"MATCH":   EQ,
	"LIKE":    LIKE,
	"DWITHIN": DWITHIN,
}

func GetCondType(name string) (int, error) {
	cond, ok := queryTypes[strings.ToUpper(name)]
	if ok {
		return cond, nil
	} else {
		return 0, ErrCondType
	}
}

type IndexDescription struct {
	IndexDef

	// Extra index description. Provides some info for user-side DSL-validation logic
	IsSortable bool     `json:"is_sortable"`
	Conditions []string `json:"conditions"`
}

type NamespaceDescription struct {
	Name           string             `json:"name"`
	Indexes        []IndexDescription `json:"indexes"`
	StorageEnabled bool               `json:"storage_enabled"`
}

// CacheMemStat information about reindexer's cache memory consumption
type CacheMemStat struct {
	// Total memory consumption by this cache
	TotalSize int64 `json:"total_size"`
	// Count of used elements stored in this cache
	ItemsCount int64 `json:"items_count"`
	// Count of empty elements slots in this cache
	EmptyCount int64 `json:"empty_count"`
	// Number of hits of queries, to store results in cache
	HitCountLimit int64 `json:"hit_count_limit"`
}

// Embedder status
type EmbedderStatus struct {
	// Last request execution status
	LastRequestResult string `json:"last_request_result"`
	// Last error
	LastError EmbedderError `json:"last_error"`
}

type EmbedderError struct {
	// Error code
	LastErrorCode int64 `json:"code"`
	// Error message
	LastErrorMessage string `json:"message"`
}

// Embeder info
type EmbedderInfo struct {
	// Embedder status
	Status EmbedderStatus `json:"status"`
}

// Operation counter and server id
type LsnT = bindings.LsnT

func CreateLSNFromInt64(v int64) LsnT {
	return bindings.CreateLSNFromInt64(v)
}

func CreateInt64FromLSN(v LsnT) int64 {
	return bindings.CreateInt64FromLSN(v)
}

// NamespaceMemStat information about reindexer's namespace memory statistics
// and located in '#memstats' system namespace
type NamespaceMemStat struct {
	// Name of namespace
	Name string `json:"name"`
	// Type of namespace ["namespace"|"embedders"]
	Type string `json:"type"`
	// Filesystem path to namespace storage
	StoragePath string `json:"storage_path"`
	// Status of disk storage (true, if storage is enabled and writable)
	StorageOK bool `json:"storage_ok"`
	// Shows if storage is enabled (however it may still be unavailable)
	StorageEnabled bool `json:"storage_enabled"`
	// More detailed info about storage status. May contain 'OK', 'DISABLED', 'NO SPACE LEFT' or last error description"
	StorageStatus string `json:"storage_status"`
	// Background indexes optimization has been completed
	OptimizationCompleted bool `json:"optimization_completed"`
	// Total count of documents in namespace
	ItemsCount int64 `json:"items_count,omitempty"`
	// Count of emopy(unused) slots in namespace
	EmptyItemsCount int64 `json:"empty_items_count"`
	// Size of strings deleted from namespace, but still used in queryResults
	StringsWaitingToBeDeletedSize int64 `json:"strings_waiting_to_be_deleted_size"`
	// Summary of total namespace memory consumption
	Total struct {
		// Total memory size of stored documents, including system structures
		DataSize int64 `json:"data_size"`
		// Total memory consumption of namespace's indexes
		IndexesSize int64 `json:"indexes_size"`
		// Total memory consumption of namespace's caches. e.g. idset and join caches
		CacheSize int64 `json:"cache_size"`
		// Total memory size, occupied by index optimizer (in bytes)
		IndexOptimizerMemory int64 `json:"index_optimizer_memory"`
		// Total memory size, occupied by the AsyncStorage (in bytes)
		InmemoryStorageSize int64 `json:"inmemory_storage_size"`
	} `json:"total"`
	// Summary of total async storage memory consumption
	Storage struct {
		// Total memory size, occupied by synchronous proxy map of the AsyncStorage (in bytes)
		ProxySize int64 `json:"proxy_size"`
		// Total memory consumption of async batches in the AsyncStorage (in bytes)
		// TODO: Uncomment affter calculation async batches size in the AsyncStorage
		// AsyncBatchesSize int64 `json:"async_batches_size"`
	} `json:"storage"`

	// Replication status of namespace
	Replication *struct {
		// Last Log Sequence Number (LSN) of applied namespace modification
		LastLSN LsnT `json:"last_lsn_v2"`
		// Namespace version counter
		NSVersion LsnT `json:"ns_version"`
		// Number of storage's master <-> slave switches
		IncarnationCounter int64 `json:"incarnation_counter"`
		// Hashsum of all records in namespace
		DataHash uint64 `json:"data_hash"`
		// Data count
		DataCount int `json:"data_count"`
		// Write Ahead Log (WAL) records count
		WalCount int64 `json:"wal_count"`
		// Total memory consumption of Write Ahead Log (WAL)
		WalSize int64 `json:"wal_size"`
		// Data updated timestamp
		UpdatedUnixNano int64 `json:"updated_unix_nano"`
		// Cluster info
		ClusterizationStatus struct {
			// Current leader server ID (for RAFT-cluster only)
			LeaderID int `json:"leader_id"`
			// Current role in cluster: cluster_replica, simple_replica or none
			Role string `json:"role"`
		} `json:"clusterization_status"`
	} `json:"replication,omitempty"`
	// Indexes memory statistic
	Indexes []struct {
		// Name of index. There are special index with name `-tuple`. It's stores original document's json structure with non-indexed fields
		Name string `json:"name"`
		// Count of unique keys values stored in index
		UniqKeysCount int64 `json:"unique_keys_count"`
		// Total memory consumption (in bytes) of documents's data, held by index
		DataSize int64 `json:"data_size"`
		// Total memory consumption (in bytes) of SORT statement and `GT`, `LT` conditions optimized structures. Applicable only to `tree` indexes
		SortOrdersSize int64 `json:"sort_orders_size"`
		// Total memory consumption (in bytes) of reverse index vectors. For `store` indexes always 0
		IDSetPlainSize int64 `json:"idset_plain_size"`
		// Total memory consumption (in bytes) of reverse index b-tree structures. For `dense` and `store` indexes always 0
		IDSetBTreeSize int64 `json:"idset_btree_size"`
		// Total memory consumption (in bytes) of the main indexing structures (fulltext, ANN, etc.)
		IndexingStructSize int64 `json:"indexing_struct_size"`
		// Total memory consumation (in bytes) of shared vectors keeper structures (ANN indexes only)
		VectorsKeeperSize int64 `json:"vectors_keeper_size"`
		// Idset cache stats. Stores merged reverse index results of SELECT field IN(...) by IN(...) keys
		IDSetCache CacheMemStat `json:"idset_cache"`
		// Updates count, pending in index updates tracker
		TrackedUpdatesCount int64 `json:"tracked_updates_count"`
		// Buckets count in index updates tracker map
		TrackedUpdatesBuckets int64 `json:"tracked_updates_buckets"`
		// Updates tracker map size in bytes
		TrackedUpdatesSize int64 `json:"tracked_updates_size"`
		// Updates tracker map overflow (number of elements, stored outside of the main buckets)
		TrackedUpdatesOverflow int64 `json:"tracked_updates_overflow"`
		// Shows whether KNN/fulltext indexing structure is fully built. If this field is nil, index does not require any specific build steps
		IsBuilt *bool `json:"is_built,omitempty"`
		// Upsert embedder status
		UpsertEmbedder *EmbedderInfo `json:"upsert_embedder,omitempty"`
		// Query embedder status
		QueryEmbedder *EmbedderInfo `json:"query_embedder,omitempty"`
	} `json:"indexes,omitempty"`
	// Join cache stats. Stores results of selects to right table by ON condition
	JoinCache *CacheMemStat `json:"join_cache,omitempty"`
	// Query cache stats. Stores results of SELECT COUNT(*) by Where conditions
	QueryCache *CacheMemStat `json:"query_cache,omitempty"`
	// Embedders memory statistic
	EmbeddingСaches []struct {
		// Tag of cache from configuration
		CacheTag string `json:"cache_tag"`
		// Capacity of cache
		Capacity int64 `json:"capacity"`
		// Cache stats
		Cache CacheMemStat `json:"cache"`
		// Status of disk storage (true, if storage is enabled and writable)
		StorageOK bool `json:"storage_ok"`
		// More detailed info about storage status. May contain 'OK', 'DISABLED', 'FAILED' or last error description"
		StorageStatus string `json:"storage_status"`
		// Filesystem path to namespace storage
		StoragePath string `json:"storage_path"`
		// Disk space occupied by storage
		StorageSize int64 `json:"storage_size"`
	} `json:"embedding_caches,omitempty"`
	// Status of tags matcher
	TagsMatcher struct {
		// Current count of tags in tags matcher
		TagsCount uint32 `json:"tags_count"`
		// Maximum count of tags
		MaxTagsCount uint32 `json:"max_tags_count"`
		// Tags matcher version
		Version int32 `json:"version"`
		// Tags matcher state token
		StateToken uint32 `json:"state_token"`
	} `json:"tags_matcher"`
}

// PerfStat is information about different reinexer's objects performance statistics
type PerfStat struct {
	// Total count of queries to this object
	TotalQueriesCount int64 `json:"total_queries_count"`
	// Average latency (execution time) for queries to this object
	TotalAvgLatencyUs int64 `json:"total_avg_latency_us"`
	// Average waiting time for acquiring lock to this object
	TotalAvgLockTimeUs int64 `json:"total_avg_lock_time_us"`
	// Count of queries to this object, requested at last second
	LastSecQPS int64 `json:"last_sec_qps"`
	// Average latency (execution time) for queries to this object at last second
	LastSecAvgLatencyUs int64 `json:"last_sec_avg_latency_us"`
	// Average waiting time for acquiring lock to this object at last second
	LastSecAvgLockTimeUs int64 `json:"last_sec_avg_lock_time_us"`
	// Minimal latency value
	MinLatencyUs int64 `json:"min_latency_us"`
	// Maximum latency value
	MaxLatencyUs int64 `json:"max_latency_us"`
	// Standard deviation of latency values
	LatencyStddev int64 `json:"latency_stddev"`
}

// TxPerfStat is information about transactions performance statistics
type TxPerfStat struct {
	// Total transactions count for namespace
	TotalCount int64 `json:"total_count"`
	// Total namespace copy operations
	TotalCopyCount int64 `json:"total_copy_count"`
	// Average steps count in transactions for this namespace
	AvgStepsCount int64 `json:"avg_steps_count"`
	// Minimum steps count in transactions for this namespace
	MinStepsCount int64 `json:"min_steps_count"`
	// Maximum steps count in transactions for this namespace
	MaxStepsCount int64 `json:"max_steps_count"`
	// Average transaction preparation time usec
	AvgPrepareTimeUs int64 `json:"avg_prepare_time_us"`
	// Minimum transaction preparation time usec
	MinPrepareTimeUs int64 `json:"min_prepare_time_us"`
	// Maximum transaction preparation time usec
	MaxPrepareTimeUs int64 `json:"max_prepare_time_us"`
	// Average transaction commit time usec
	AvgCommitTimeUs int64 `json:"avg_commit_time_us"`
	// Minimum transaction commit time usec
	MinCommitTimeUs int64 `json:"min_commit_time_us"`
	// Maximum transaction commit time usec
	MaxCommitTimeUs int64 `json:"max_commit_time_us"`
	// Average namespace copy time usec
	AvgCopyTimeUs int64 `json:"avg_copy_time_us"`
	// Maximum namespace copy time usec
	MinCopyTimeUs int64 `json:"min_copy_time_us"`
	// Minimum namespace copy time usec
	MaxCopyTimeUs int64 `json:"max_copy_time_us"`
}

// LRUCachePerfStat is information about LRU cache efficiency
type LRUCachePerfStat struct {
	// Total queries to cache
	TotalQueries uint64 `json:"total_queries"`
	// Cache hit rate (CacheHits / TotalQueries)
	CacheHitRate float64 `json:"cache_hit_rate"`
	// Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
	IsActive bool `json:"is_active"`
}

type EmbedderPerfStat struct {
	// Total number of calls to a specific embedder
	TotalQueriesCount uint64 `json:"total_queries_count"`
	// Total number of requested vectors
	TotalEmbedDocumentsCount uint64 `json:"total_embed_documents_count"`
	// Number of calls to the embedder in the last second
	LastSecQps uint64 `json:"last_sec_qps"`
	// Number of embedded documents in the last second
	LastSecDps uint64 `json:"last_sec_dps"`
	// Total number of errors accessing the embedder
	TotalErrorsCount uint64 `json:"total_errors_count"`
	// Number of errors in the last second
	LastSecErrorsCount uint64 `json:"last_sec_errors_count"`
	// Current number of connections in use
	ConnInUse uint64 `json:"conn_in_use"`
	// Average number of connections used over the last second
	LastSecAvgConnInUse uint64 `json:"last_sec_avg_conn_in_use"`
	// Average overall autoembedding latency (over all time)
	TotalAvgLatencyUs uint64 `json:"total_avg_latency_us"`
	// Average autoembedding latency (over the last second)
	LastSecAvgLatencyUs uint64 `json:"last_sec_avg_latency_us"`
	// Maximum total autoembedding latency (all time)
	MaxLatencyUs uint64 `json:"max_latency_us"`
	// Minimum overall autoembedding latency (all time)
	MinLatencyUs uint64 `json:"min_latency_us"`
	// Average latency of waiting for a connection from the pool (over all time)
	TotalAvgConnAwaitLatencyUs uint64 `json:"total_avg_conn_await_latency_us"`
	// Average latency of waiting for a connection from the pool (over the last second)
	LastSecAvgConnAwaitLatencyUs uint64 `json:"last_sec_avg_conn_await_latency_us"`
	// Average auto-embedding latency on cache miss (over all time)
	TotalAvgEmbedLatencyUs uint64 `json:"total_avg_embed_latency_us"`
	// Average auto-embedding latency for cache misses (last second)
	LastSecAvgEmbedLatencyUs uint64 `json:"last_sec_avg_embed_latency_us"`
	// Maximum auto-embedding latency on cache miss (all time)
	MaxEmbedLatencyUs uint64 `json:"max_embed_latency_us"`
	// Minimum auto-embedding latency on cache miss (all time)
	MinEmbedLatencyUs uint64 `json:"min_embed_latency_us"`
	// Average auto-embedding latency for cache hits (over all time)
	TotalAvgCacheLatencyUs uint64 `json:"total_avg_cache_latency_us"`
	// Average auto-embedding latency for cache hits (last second)
	LastSecAvgCacheLatencyUs uint64 `json:"last_sec_avg_cache_latency_us"`
	// Maximum auto-embedding latency on a cache hit (all time)
	MaxCacheLatencyUs uint64 `json:"max_cache_latency_us"`
	// Minimum auto-embedding latency for a cache hit (all time)
	MinCacheLatencyUs uint64 `json:"min_cache_latency_us"`
	// Cache statistics
	CacheStat *EmbedderCachePerfStat `json:"cache,omitempty"`
}

// EmbedderCachePerfStat is information about specific embedder performance statistics
type EmbedderCachePerfStat struct {
	// Tag of cache from configuration
	CacheTag string `json:"cache_tag"`
	// Total queries to cache
	TotalQueries uint64 `json:"total_queries"`
	// Cache hit rate (CacheHits / TotalQueries)
	CacheHitRate float64 `json:"cache_hit_rate"`
	// Determines if cache is currently in use. Usually it has 'false' value for uncommitted indexes
	IsActive bool `json:"is_active"`
}

// IndexPerfStat is information about specific index performance statistics
type IndexPerfStat struct {
	// Name of index
	Name string `json:"name"`
	// Performance statistics for index commit operations
	Commits PerfStat `json:"commits"`
	// Performance statistics for index select operations
	Selects PerfStat `json:"selects"`
	// Performance statistics for LRU IdSets index cache (or fulltext cache for text indexes).
	// Nil-value means, that index does not use cache at all
	Cache *LRUCachePerfStat `json:"cache,omitempty"`
	// Performance statistics for upsert embedder
	UpsertEmbedder EmbedderPerfStat `json:"upsert_embedder"`
	// Performance statistics for query embedder
	QueryEmbedder EmbedderPerfStat `json:"query_embedder"`
}

// NamespacePerfStat is information about namespace's performance statistics
// and located in '#perfstats' system namespace
type NamespacePerfStat struct {
	// Name of namespace
	Name string `json:"name"`
	// Performance statistics for update operations
	Updates PerfStat `json:"updates"`
	// Performance statistics for select operations
	Selects PerfStat `json:"selects"`
	// Performance statistics for transactions
	Transactions TxPerfStat `json:"transactions"`
	// Performance statistics for JOINs cache
	JoinCache LRUCachePerfStat `json:"join_cache"`
	// Performance statistics for CountCached aggregation cache
	QueryCountCache LRUCachePerfStat `json:"query_count_cache"`
	// Performance statistics for each namespace index
	Indexes []IndexPerfStat `json:"indexes"`
}

// ClientConnectionStat is information about client connection
type ClientConnectionStat struct {
	// Connection identifier
	ConnectionId int64 `json:"connection_id"`
	// client ip address
	Ip string `json:"ip"`
	// User name
	UserName string `json:"user_name"`
	// User right
	UserRights string `json:"user_rights"`
	// Database name
	DbName string `json:"db_name"`
	// Current activity
	CurrentActivity string `json:"current_activity"`
	// Server start time in unix timestamp
	StartTime int64 `json:"start_time"`
	// Receive bytes
	RecvBytes int64 `json:"recv_bytes"`
	// Sent bytes
	SentBytes int64 `json:"sent_bytes"`
	// Client version string
	ClientVersion string `json:"client_version"`
	// Send buffer size
	SendBufBytes int64 `json:"send_buf_bytes"`
	// Timestamp of last send operation (ms)
	LastSendTs int64 `json:"last_send_ts"`
	// Timestamp of last recv operation (ms)
	LastRecvTs int64 `json:"last_recv_ts"`
	// Current send rate (bytes/s)
	SendRate int `json:"send_rate"`
	// Current recv rate (bytes/s)
	RecvRate int `json:"recv_rate"`
	// Active transactions count
	TxCount int `json:"tx_count"`
}

// ReplicationSyncStat WAL/force sync statistic
type ReplicationSyncStat struct {
	// Syncs count
	Count int64 `json:"count"`
	// Average sync time
	AvgTimeUs int64 `json:"avg_time_us"`
	// Max sync time
	MaxTimeUs int64 `json:"max_time_us"`
}

// ReplicationStat replication statistic
type ReplicationStat struct {
	// Replication type: either "async" or "cluster"
	Type string `json:"type"`
	// Global WAL-syncs' stats
	WALSync ReplicationSyncStat `json:"wal_sync"`
	// Global force-syncs' stats
	ForceSync ReplicationSyncStat `json:"force_sync"`
	// Leader's initial sync statistic (for "cluster" type only)
	InitialSyncStat struct {
		// WAL-syncs' stats
		WALSync ReplicationSyncStat `json:"wal_sync"`
		// Force-syncs' stats
		ForceSync ReplicationSyncStat `json:"force_sync"`
		// Total initial sync time
		TotalTimeUs int64 `json:"total_time_us"`
	} `json:"initial_sync"`
	// Count of online updates, awaiting replication
	PendingUpdatesCount int64 `json:"pending_updates_count"`
	// Total allocated online updates count (including those, which already were replicated, but was not deallocated yet)
	AllocatedUpdatesCount int64 `json:"allocated_updates_count"`
	// Total allocated online updates size in bytes
	AllocatedUpdatesSize int64 `json:"allocated_updates_size"`
	// Info about each node
	ReplicationNodeStat []struct {
		// Node's DSN
		DSN string `json:"dsn"`
		// Node's server ID
		ServerID int `json:"server_id"`
		// Online updates, awaiting replication to this node
		PendingUpdatesCount int64 `json:"pending_updates_count"`
		// Network status: "none", "online", "offline", "raft_error"
		Status string `json:"status"`
		// Replication role: "none", "follower", "leader", "candidate"
		Role string `json:"role"`
		// Node's sync state: "none", "syncing", "awaiting_resync", "online_replication", "initial_leader_sync"
		SyncState string `json:"sync_state"`
		// Shows synchronization state for raft-cluster node (false if node is outdated)
		IsSynchronized bool `json:"is_synchronized"`
	} `json:"nodes"`
}

// QueryPerfStat is information about query's performance statistics
// and located in '#queriesperfstats' system namespace
type QueryPerfStat struct {
	Query string `json:"query"`
	PerfStat
}

// DBConfigItem is structure stored in system '#config` namespace
type DBConfigItem struct {
	Type             string                    `json:"type"`
	Profiling        *DBProfilingConfig        `json:"profiling,omitempty"`
	Namespaces       *[]DBNamespacesConfig     `json:"namespaces,omitempty"`
	Replication      *DBReplicationConfig      `json:"replication,omitempty"`
	AsyncReplication *DBAsyncReplicationConfig `json:"async_replication,omitempty"`
	Embedders        *[]DBEmbeddersConfig      `json:"caches,omitempty"`
}

// Section of long_queries_logging related to logging of SELECT, UPDATE and DELETE queries
type LongQueryLoggingItem struct {
	// Threshold value for logging SELECT, UPDATE or DELETE queries, if -1 logging is disabled
	ThresholdUS int `json:"threshold_us"`
	// Output the query in a normalized form
	Normalized bool `json:"normalized"`
}

// Section of long_queries_logging related to logging of transactions
type LongTxLoggingItem struct {
	// Threshold value for total transaction commit time, if -1 logging based on commit time is disabled
	ThresholdUS int `json:"threshold_us"`
	// Threshold value for the average step duration time in a transaction, if -1 logging based on step duration is disabled
	AverageTxStepThresholdUs int `json:"avg_step_threshold_us"`
}

// Section of ProfilingConfig related to logging of long queries
type LongQueryLoggingConfig struct {
	// Relates to logging of long select queries
	SelectItem LongQueryLoggingItem `json:"select"`
	// Relates to logging of long update and delete queries
	UpdDelItem LongQueryLoggingItem `json:"update_delete"`
	// Relates to logging of transactions
	TxItem LongTxLoggingItem `json:"transaction"`
}

// DBProfilingConfig is part of reindexer configuration contains profiling options
type DBProfilingConfig struct {
	// Minimum query execution time to be recorded in #queriesperfstats namespace
	QueriesThresholdUS int `json:"queries_threshold_us"`
	// Enables tracking memory statistics
	MemStats bool `json:"memstats"`
	// Enables tracking overall performance statistics
	PerfStats bool `json:"perfstats"`
	// Enables recording of queries performance statistics
	QueriesPerfStats bool `json:"queriesperfstats"`
	// Enables recording of activity statistics into #activitystats namespace
	ActivityStats bool `json:"activitystats"`

	// Configured console logging of long queries
	LongQueryLogging *LongQueryLoggingConfig `json:"long_queries_logging,omitempty"`
}

type NamespaceCacheConfig struct {
	// Max size of the index IdSets cache in bytes (per index)
	// Each index has it's own independent cache
	// This cache is used in any selections to store resulting sets of internal document IDs (it does not stores documents' content itself)
	// Default value is 134217728 (128 MB). Min value is 0
	IdxIdsetCacheSize uint64 `json:"index_idset_cache_size"`
	// Default 'hits to cache' for index IdSets caches
	// This value determines how many requests required to put results into cache
	// For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results
	// This value may be automatically increased if cache is invalidation too fast
	// Default value is 2. Min value is 0
	IdxIdsetHitsToCache uint32 `json:"index_idset_hits_to_cache"`
	// Max size of the fulltext indexes IdSets cache in bytes (per index)
	// Each fulltext index has it's own independent cache
	// This cache is used in any selections to store resulting sets of internal document IDs, FT ranks and highlighted areas (it does not stores documents' content itself)
	// Default value is 134217728 (128 MB). Min value is 0
	FTIdxCacheSize uint64 `json:"ft_index_cache_size"`
	// Default 'hits to cache' for fulltext index IdSets caches.
	// This value determines how many requests required to put results into cache
	// For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
	// Default value is 2. Min value is 0
	FTIdxHitsToCache uint32 `json:"ft_index_hits_to_cache"`
	// Max size of the index IdSets cache in bytes for each namespace
	// This cache will be enabled only if 'join_cache_mode' property is not 'off'
	// It stores resulting IDs and any other 'preselect' information for the JOIN queries (when target namespace is right namespace of the JOIN)
	// Default value is 268435456 (256 MB). Min value is 0
	JoinCacheSize uint64 `json:"joins_preselect_cache_size"`
	// Default 'hits to cache' for joins preselect cache of the current namespace
	// This value determines how many requests required to put results into cache
	// For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results
	// This value may be automatically increased if cache is invalidation too fast
	// Default value is 2. Min value is 0
	JoinHitsToCache uint32 `json:"joins_preselect_hit_to_cache"`
	// Max size of the cache for COUNT_CACHED() aggregation in bytes for each namespace
	// This cache stores resulting COUNTs and serialized queries for the COUNT_CACHED() aggregations
	// Default value is 134217728 (128 MB). Min value is 0
	QueryCountCacheSize uint64 `json:"query_count_cache_size"`
	// Default 'hits to cache' for COUNT_CACHED() aggregation of the current namespace
	// This value determines how many requests required to put results into cache
	// For example with value of 2: first request will be executed without caching, second request will generate cache entry and put results into the cache and third request will get cached results. This value may be automatically increased if cache is invalidation too fast
	// Default value is 2. Min value is 0
	QueryCountHitsToCache uint32 `json:"query_count_hit_to_cache"`
}

// DBNamespacesConfig is part of reindexer configuration contains namespaces options
type DBNamespacesConfig struct {
	// Name of namespace, or `*` for setting to all namespaces
	Namespace string `json:"namespace"`
	// Log level of queries core logger
	LogLevel string `json:"log_level"`
	// Join cache mode. Can be one of on, off, aggressive
	JoinCacheMode string `json:"join_cache_mode"`
	// Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
	StartCopyPolicyTxSize int `json:"start_copy_policy_tx_size"`
	// Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
	CopyPolicyMultiplier int `json:"copy_policy_multiplier"`
	// Force namespace copying for transaction with steps count greater than this value
	TxSizeToAlwaysCopy int `json:"tx_size_to_always_copy"`
	// Count of threads, that will be created during transaction's commit to insert data into multithread ANN-indexes
	TxVecInsertionThreads int `json:"tx_vec_insertion_threads"`
	// Timeout before background indexes optimization start after last update. 0 - disable optimizations
	OptimizationTimeout int `json:"optimization_timeout_ms"`
	// Maximum number of background threads of sort indexes optimization. 0 - disable sort optimizations
	OptimizationSortWorkers int `json:"optimization_sort_workers"`
	// Maximum WAL size for this namespace (maximum count of WAL records)
	WALSize int64 `json:"wal_size"`
	// Minimum preselect size for optimization of inner join by injection of filters. It is using if (MaxPreselectPart * ns.size) is less than this value
	MinPreselectSize int64 `json:"min_preselect_size"`
	// Maximum preselect size for optimization of inner join by injection of filters
	MaxPreselectSize int64 `json:"max_preselect_size"`
	// Maximum preselect part of namespace's items for optimization of inner join by injection of filters
	MaxPreselectPart float64 `json:"max_preselect_part"`
	// Maximum number of IdSet iterations of namespace preliminary result size for optimization
	// expected values [201..2.147.483.647], default value 20.000
	MaxIterationsIdSetPreResult int64 `json:"max_iterations_idset_preresult"`
	// Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
	IndexUpdatesCountingMode bool `json:"index_updates_counting_mode"`
	// Enables synchronous storage flush inside write-calls, if async updates count is more than SyncStorageFlushLimit
	// 0 - disables synchronous storage flush. In this case storage will be flushed in background thread only
	// Default value is 20000
	SyncStorageFlushLimit int `json:"sync_storage_flush_limit"`
	// Delay between last namespace update and background ANN-indexes storage cache creation. Storage cache is required for ANN-indexes for faster startup
	// 0 - disables background cache creation (cache will still be created on the database shutdown)
	// Default value is 5000 ms
	ANNStorageCacheBuildTimeoutMs int `json:"ann_storage_cache_build_timeout_ms"`
	// Strict mode for queries. Adds additional check for fields('names')/indexes('indexes') existence in sorting and filtering conditions"
	// Default value - 'names'
	// Possible values: 'indexes','names','none'
	StrictMode string `json:"strict_mode,omitempty"`
	// Namespaces' cache configs
	CacheConfig *NamespaceCacheConfig `json:"cache,omitempty"`
}

// DBAsyncReplicationNode
type DBAsyncReplicationNode struct {
	// Node's DSN. It must to have cproto format (e.g. 'cproto://<ip>:<port>/<db>')
	DSN string `json:"dsn"`
	// List of namespaces to replicate on this specific node. If nil, list from main replication config will be used
	Namespaces []string `json:"namespaces"`
}

// DBAsyncReplicationConfig is part of reindexer configuration contains async replication options
type DBAsyncReplicationConfig struct {
	// Replication role. One of: none, leader, follower
	Role string `json:"role"`
	// Replication mode for mixed 'sync cluster + async replication' configs. One of: default, from_sync_leader
	ReplicationMode string `json:"replication_mode,omitempty"`
	// force resync on logic error conditions
	ForceSyncOnLogicError bool `json:"force_sync_on_logic_error,omitempty"`
	// force resync on wrong data hash conditions
	ForceSyncOnWrongDataHash bool `json:"force_sync_on_wrong_data_hash,omitempty"`
	// Network timeout for online updates (s)
	UpdatesTimeout int `json:"online_updates_timeout_sec,omitempty"`
	// Network timeout for wal/force syncs (s)
	SyncTimeout int `json:"sync_timeout_sec,omitempty"`
	// Number of parallel replication threads
	SyncThreads int `json:"sync_threads,omitempty"`
	// Max number of concurrent force/wal syncs per replication thread
	ConcurrentSyncsPerThread int `json:"syncs_per_thread,omitempty"`
	// Number of coroutines for online-updates batching (per each namespace of each node)
	BatchingReoutines int `json:"batching_routines_count,omitempty"`
	// Enable compression for replication network operations
	EnableCompression bool `json:"enable_compression,omitempty"`
	// Delay between write operation and replication. Larger values here will leader to higher replication latency and buffering,"
	// but also will provide more effective network batching and CPU utilization
	OnlineUpdatesDelayMSec int `json:"online_updates_delay_msec,omitempty"`
	// Replication log level on replicator's startup. Possible values: none, error, warning, info, trace ('info' is default)
	LogLevel string `json:"log_level,omitempty"`
	// List of namespaces for replication. If empty, all namespaces. All replicated namespaces will become read only for slave
	Namespaces []string `json:"namespaces"`
	// Replication token of the current node that it sends to the follower for verification
	SelfReplicationToken string `json:"self_replication_token,omitempty"`
	// Reconnect interval after replication error (ms)
	RetrySyncInterval int `json:"retry_sync_interval_msec,omitempty"`
	// List of follower-nodes for async replication
	Nodes []DBAsyncReplicationNode `json:"nodes"`
}

type AdmissibleToken struct {
	// Admissible token
	Token string `json:"token"`
	// Namespaces which can only be replicated by a leader with the same token
	Namespaces []string `json:"namespaces,omitempty"`
}

// DBReplicationConfig is part of reindexer configuration contains general node settings for replication
type DBReplicationConfig struct {
	// Server ID - must be unique for each node (available values: 0-999)
	ServerID int `json:"server_id"`
	// Cluster ID - must be same for client and for master
	ClusterID int `json:"cluster_id"`
	//  Lists of namespaces with their admissible tokens
	AdmissibleTokens []AdmissibleToken `json:"admissible_replication_tokens,omitempty"`
}

// DBEmbeddersConfig is part of reindexer configuration contains Embedders cache settings
type DBEmbeddersConfig struct {
	// Name, used to access the cache. Optional, if not specified, caching is not used
	CacheTag string `json:"cache_tag"`
	// Maximum size of the embedding results cache in items.
	// This cache will only be enabled if the 'max_cache_items' property is not 'off' (value 0).
	// It stores the results of the embedding calculation. Minimum 0, default 1000000
	MaxCacheItems int `json:"max_cache_items"`
	// This value determines how many requests required to put results into cache.
	// For example with value of 2: first request will be executed without caching,
	// second request will generate cache entry and put results into the cache
	// and third request will get cached results. 0 and 1 mean - when value added goes straight to the cache
	HitToCache int `json:"hit_to_cache"`
}

// DescribeNamespaces makes a 'SELECT * FROM #namespaces' query to database.
// Return NamespaceDescription results, error
func (db *Reindexer) DescribeNamespaces() ([]*NamespaceDescription, error) {
	result := []*NamespaceDescription{}

	descs, err := db.Query(NamespacesNamespaceName).ExecCtx(db.ctx).FetchAll()
	if err != nil {
		return nil, err
	}

	for _, desc := range descs {
		nsdesc, ok := desc.(*NamespaceDescription)
		if ok {
			result = append(result, nsdesc)
		}
	}

	return result, nil
}

// DescribeNamespace makes a 'SELECT * FROM #namespaces' query to database.
// Return NamespaceDescription results, error
func (db *Reindexer) DescribeNamespace(namespace string) (*NamespaceDescription, error) {
	return db.impl.describeNamespace(db.ctx, namespace)
}

func (db *reindexerImpl) describeNamespace(ctx context.Context, namespace string) (*NamespaceDescription, error) {
	desc, err := db.query(NamespacesNamespaceName).Where("name", EQ, namespace).ExecCtx(ctx).FetchOne()
	if err != nil {
		return nil, err
	}
	return desc.(*NamespaceDescription), nil
}

// GetNamespacesMemStat makes a 'SELECT * FROM #memstats' query to database.
// Return NamespaceMemStat results, error
func (db *Reindexer) GetNamespacesMemStat() ([]*NamespaceMemStat, error) {
	result := []*NamespaceMemStat{}

	descs, err := db.Query(MemstatsNamespaceName).ExecCtx(db.ctx).FetchAll()
	if err != nil {
		return nil, err
	}

	for _, desc := range descs {
		nsdesc, ok := desc.(*NamespaceMemStat)
		if ok {
			result = append(result, nsdesc)
		}
	}

	return result, nil
}

// GetNamespaceMemStat makes a 'SELECT * FROM #memstat' query to database.
// Return NamespaceMemStat results, error
func (db *Reindexer) GetNamespaceMemStat(namespace string) (*NamespaceMemStat, error) {
	desc, err := db.Query(MemstatsNamespaceName).Where("name", EQ, namespace).ExecCtx(db.ctx).FetchOne()
	if err != nil {
		return nil, err
	}
	return desc.(*NamespaceMemStat), nil
}

func DefaultDBNamespaceConfig(namespace string) *DBNamespacesConfig {
	const kDefaultCacheSizeLimit = 1024 * 1024 * 128
	const kDefaultHitCountToCache = 2

	cacheCfg := &NamespaceCacheConfig{
		IdxIdsetCacheSize:     kDefaultCacheSizeLimit,
		IdxIdsetHitsToCache:   kDefaultHitCountToCache,
		FTIdxCacheSize:        kDefaultCacheSizeLimit,
		FTIdxHitsToCache:      kDefaultHitCountToCache,
		JoinCacheSize:         2 * kDefaultCacheSizeLimit,
		JoinHitsToCache:       kDefaultHitCountToCache,
		QueryCountCacheSize:   kDefaultCacheSizeLimit,
		QueryCountHitsToCache: kDefaultHitCountToCache,
	}

	return &DBNamespacesConfig{
		Namespace:                     namespace,
		LogLevel:                      "none",
		JoinCacheMode:                 "off",
		StrictMode:                    "names",
		StartCopyPolicyTxSize:         10000,
		CopyPolicyMultiplier:          5,
		TxSizeToAlwaysCopy:            100000,
		TxVecInsertionThreads:         4,
		OptimizationTimeout:           800,
		OptimizationSortWorkers:       4,
		WALSize:                       4000000,
		MinPreselectSize:              1000,
		MaxPreselectSize:              1000,
		MaxPreselectPart:              0.1,
		MaxIterationsIdSetPreResult:   20000,
		ANNStorageCacheBuildTimeoutMs: 5000,
		SyncStorageFlushLimit:         20000,
		CacheConfig:                   cacheCfg,
	}
}
