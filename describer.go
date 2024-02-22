package reindexer

import (
	"context"
	"strings"
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
	// Reconnect to the synchnized node (which was the part of the last consensus in synchronous cluster)
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

// Map from index type to cond name
var queryNames = map[int]string{
	EQ:      "EQ",
	GT:      "GT",
	LT:      "LT",
	GE:      "GE",
	LE:      "LE",
	SET:     "SET",
	RANGE:   "RANGE",
	ANY:     "ANY",
	EMPTY:   "EMPTY",
	LIKE:    "LIKE",
	DWITHIN: "DWITHIN",
}

type IndexDescription struct {
	IndexDef

	IsSortable bool     `json:"is_sortable"`
	IsFulltext bool     `json:"is_fulltext"`
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

// Operation counter and server id
type LsnT struct {
	// Operation counter
	Counter int64 `json:"counter"`
	// Node identifier
	ServerId int `json:"server_id"`
}

const kLSNCounterBitsCount = 48
const kLSNCounterMask int64 = (int64(1) << kLSNCounterBitsCount) - int64(1)
const kLSNDigitCountMult int64 = 1000000000000000

func (lsn *LsnT) IsCompatibleWith(o LsnT) bool {
	return lsn.ServerId == o.ServerId
}

func (lsn *LsnT) IsNewerThen(o LsnT) bool {
	return lsn.Counter > o.Counter
}

func (lsn *LsnT) IsEmpty() bool { return lsn.Counter == kLSNDigitCountMult-1 }

func CreateLSNFromInt64(v int64) LsnT {
	if (v & kLSNCounterMask) == kLSNCounterMask {
		return LsnT{Counter: kLSNDigitCountMult - 1, ServerId: -1}
	}

	server := v / kLSNDigitCountMult
	return LsnT{Counter: v - server*kLSNDigitCountMult, ServerId: int(server)}
}

func CreateInt64FromLSN(v LsnT) int64 {
	return int64(v.ServerId)*kLSNDigitCountMult + v.Counter
}

// NamespaceMemStat information about reindexer's namespace memory statisctics
// and located in '#memstats' system namespace
type NamespaceMemStat struct {
	// Name of namespace
	Name string `json:"name"`
	// [[deperecated]]. do not use
	StorageError string `json:"storage_error"`
	// Filesystem path to namespace storage
	StoragePath string `json:"storage_path"`
	// Status of disk storage (true, if storage is enabled and writable)
	StorageOK bool `json:"storage_ok"`
	// Shows if storage is enabled (hovewer it may still be unavailable)
	StorageEnabled bool `json:"storage_enabled"`
	// More detailed info about storage status. May contain 'OK', 'DISABLED', 'NO SPACE LEFT' or last error descrition"
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
		// Total memory size, occupated by index optimizer (in bytes)
		IndexOptimizerMemory int64 `json:"index_optimizer_memory"`
	} `json:"total"`
	// Replication status of namespace
	Replication struct {
		// Last Log Sequence Number (LSN) of applied namespace modification
		LastLSN LsnT `json:"last_lsn_v2"`
		// Namespace version counter
		NSVersion LsnT `json:"ns_version"`
		// Temporary namespace flag
		Temporary bool `json:"temporary"`
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
			LeadeID int `json:"leader_id"`
			// Current role in cluster: cluster_replica, simple_replica or none
			Role string `json:"role"`
		} `json:"clusterization_status"`
	} `json:"replication"`

	// Indexes memory statistic
	Indexes []struct {
		// Name of index. There are special index with name `-tuple`. It's stores original document's json structure with non indexe fields
		Name string `json:"name"`
		// Count of unique keys values stored in index
		UniqKeysCount int64 `json:"unique_keys_count"`
		// Total memory consumption of documents's data, holded by index
		DataSize int64 `json:"data_size"`
		// Total memory consumption of SORT statement and `GT`, `LT` conditions optimized structures. Applicabe only to `tree` indexes
		SortOrdresSize int64 `json:"sort_orders_size"`
		// Total memory consumption of reverse index vectors. For `store` ndexes always 0
		IDSetPlainSize int64 `json:"idset_plain_size"`
		// Total memory consumption of reverse index b-tree structures. For `dense` and `store` indexes always 0
		IDSetBTreeSize int64 `json:"idset_btree_size"`
		// Total memory consumption of fulltext search structures
		FulltextSize int64 `json:"fulltext_size"`
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
	} `json:"indexes"`
	// Join cache stats. Stores results of selects to right table by ON condition
	JoinCache CacheMemStat `json:"join_cache"`
	// Query cache stats. Stores results of SELECT COUNT(*) by Where conditions
	QueryCache CacheMemStat `json:"query_cache"`
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
	// Count of online updates, awaiting rpelication
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
		// Shows synchroniztion state for raft-cluster node (false if node is outdated)
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
	// Minimum query execution time to be recoreded in #queriesperfstats namespace
	QueriesThresholdUS int `json:"queries_threshold_us"`
	// Enables tracking memory statistics
	MemStats bool `json:"memstats"`
	// Enables tracking overal perofrmance statistics
	PerfStats bool `json:"perfstats"`
	// Enables recording of queries perofrmance statistics
	QueriesPerfStats bool `json:"queriesperfstats"`
	// Enables recording of activity statistics into #activitystats namespace
	ActivityStats bool `json:"activitystats"`

	// Configured console logging of long queries
	LongQueryLogging *LongQueryLoggingConfig `json:"long_queries_logging,omitempty"`
}

type NamespaceCacheConfig struct {
	// Max size of the index IdSets cache in bytes (per index)
	// Each index has it's own independant cache
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
	// Each fulltext index has it's own independant cache
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
	// Enable namespace lazy load (namespace shoud be loaded from disk on first call, not at reindexer startup)
	Lazyload bool `json:"lazyload"`
	// Unload namespace data from RAM after this idle timeout in seconds. If 0, then data should not be unloaded
	UnloadIdleThreshold int `json:"unload_idle_threshold"`
	// Enable namespace copying for transaction with steps count greater than this value (if copy_politics_multiplier also allows this)
	StartCopyPolicyTxSize int `json:"start_copy_policy_tx_size"`
	// Disables copy policy if namespace size is greater than copy_policy_multiplier * start_copy_policy_tx_size
	CopyPolicyMultiplier int `json:"copy_policy_multiplier"`
	// Force namespace copying for transaction with steps count greater than this value
	TxSizeToAlwaysCopy int `json:"tx_size_to_always_copy"`
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
	// Enables 'simple counting mode' for index updates tracker. This will increase index optimization time, however may reduce insertion time
	IndexUpdatesCountingMode bool `json:"index_updates_counting_mode"`
	// Enables synchronous storage flush inside write-calls, if async updates count is more than SyncStorageFlushLimit
	// 0 - disables synchronous storage flush. In this case storage will be flushed in background thread only
	// Default value is 20000
	SyncStorageFlushLimit int `json:"sync_storage_flush_limit"`
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
	ReplicationMode string `json:"replication_mode"`
	// force resync on logic error conditions
	ForceSyncOnLogicError bool `json:"force_sync_on_logic_error"`
	// force resync on wrong data hash conditions
	ForceSyncOnWrongDataHash bool `json:"force_sync_on_wrong_data_hash"`
	// Network timeout for online updates (s)
	UpdatesTimeout int `json:"online_updates_timeout_sec"`
	// Network timeout for wal/force syncs (s)
	SyncTimeout int `json:"sync_timeout_sec"`
	// Number of parallel replication threads
	SyncThreads int `json:"sync_threads"`
	// Max number of concurrent force/wal syncs per replication thread
	ConcurrentSyncsPerThread int `json:"syncs_per_thread"`
	// Number of coroutines for online-updates batching (per each namespace of each node)
	BatchingReoutines int `json:"batching_routines_count"`
	// Enable compression for replication network operations
	EnableCompression bool `json:"enable_compression"`
	// Delay between write operation and replication. Larger values here will leader to higher replication latency and bufferization,"
	// but also will provide more effective network batching and CPU untilization
	OnlineUpdatesDelayMSec int `json:"online_updates_delay_msec,omitempty"`
	// Replication log level on replicator's startup. Possible values: none, error, warning, info, trace ('info' is default)
	LogLevel string `json:"log_level,omitempty"`
	// List of namespaces for replication. If emply, all namespaces. All replicated namespaces will become read only for slave
	Namespaces []string `json:"namespaces"`
	// Reconnect interval after replication error (ms)
	RetrySyncInterval int `json:"retry_sync_interval_msec"`
	// List of follower-nodes for async replication
	Nodes []DBAsyncReplicationNode `json:"nodes"`
}

// DBReplicationConfig is part of reindexer configuration contains general node settings for replication
type DBReplicationConfig struct {
	// Server ID - must be unique for each node (available values: 0-999)
	ServerID int `json:"server_id"`
	// Cluster ID - must be same for client and for master
	ClusterID int `json:"cluster_id"`
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
