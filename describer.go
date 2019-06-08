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
)

// Map from cond name to index type
var queryTypes = map[string]int{
	"EQ":     EQ,
	"GT":     GT,
	"LT":     LT,
	"GE":     GE,
	"LE":     LE,
	"SET":    SET,
	"RANGE":  RANGE,
	"ANY":    ANY,
	"EMPTY":  EMPTY,
	"ALLSET": ALLSET,
	"MATCH":  EQ,
	"LIKE":   LIKE,
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
	EQ:    "EQ",
	GT:    "GT",
	LT:    "LT",
	GE:    "GE",
	LE:    "LE",
	SET:   "SET",
	RANGE: "RANGE",
	ANY:   "ANY",
	EMPTY: "EMPTY",
	LIKE:  "LIKE",
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

// NamespaceMemStat information about reindexer's namespace memory statisctics
// and located in '#memstats' system namespace
type NamespaceMemStat struct {
	// Name of namespace
	Name string `json:"name"`
	// [[deperecated]]. do not use
	StorageError string `json:"storage_error"`
	// Filesystem path to namespace storage
	StoragePath string `json:"storage_path"`
	// Status of disk storage
	StorageOK bool `json:"storage_ok"`
	// [[deperecated]]. do not use
	UpdatedUnixNano int64 `json:"updated_unix_nano"`
	// Total count of documents in namespace
	ItemsCount int64 `json:"items_count,omitempty"`
	// Count of emopy(unused) slots in namespace
	EmptyItemsCount int64 `json:"empty_items_count"`
	// Raw size of documents, stored in the namespace, except string fields
	DataSize int64 `json:"data_size"`
	// Summary of total namespace memory consumption
	Total struct {
		// Total memory size of stored documents, including system structures
		DataSize int64 `json:"data_size"`
		// Total memory consumption of namespace's indexes
		IndexesSize int64 `json:"indexes_size"`
		// Total memory consumption of namespace's caches. e.g. idset and join caches
		CacheSize int64 `json:"cache_size"`
	} `json:"total"`
	// Replication status of namespace
	Replication struct {
		// Last Log Sequence Number (LSN) of applied namespace modification
		LastLSN int64 `json:"last_lsn"`
		// Cluster ID - must be same for client and for master
		ClusterID int64 `json:"cluster_id"`
		// If true, then namespace is in slave mode
		SlaveMode bool `json:"slave_mode"`
		// Number of storage's master <-> slave switches
		IncarnationCounter int64 `json:"incarnation_counter"`
		// Hashsum of all records in namespace
		DataHash uint64 `json:"data_hash"`
		// Write Ahead Log (WAL) records count
		WalCount int64 `json:"wal_count"`
		// Total memory consumption of Write Ahead Log (WAL)
		WalSize int64 `json:"wal_size"`
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

// NamespacePerfStat is information about namespace's performance statistics
// and located in '#perfstats' system namespace
type NamespacePerfStat struct {
	// Name of namespace
	Name string `json:"name"`
	// Performance statistics for update operations
	Updates PerfStat `json:"updates"`
	// Performance statistics for select operations
	Selects PerfStat `json:"selects"`
}

// QueryPerfStat is information about query's performance statistics
// and located in '#queriesperfstats' system namespace
type QueryPerfStat struct {
	Query string `json:"query"`
	PerfStat
}

// DBConfigItem is structure stored in system '#config` namespace
type DBConfigItem struct {
	Type        string                `json:"type"`
	Profiling   *DBProfilingConfig    `json:"profiling,omitempty"`
	Namespaces  *[]DBNamespacesConfig `json:"namespaces,omitempty"`
	Replication *DBReplicationConfig  `json:"replication,omitempty"`
}

// DBProfilingConfig is part of reindexer configuration contains profiling options
type DBProfilingConfig struct {
	// Minimum query execution time to be recoreded in #queriesperfstats namespace
	QueriesThresholdUS int `json:"queries_threshold_us"`
	// Enables tracking memory statistics
	MemStats bool `json:"memstats"`
	// Enables tracking overal perofrmance statistics
	PerfStats bool `json:"perfstats"`
	// Enables record queries perofrmance statistics
	QueriesPerfStats bool `json:"queriesperfstats"`
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
	// Copy namespce policts will start only after item's count become greater in this param
	StartCopyPoliticsCount int `json:"start_copy_politics_count"`
	// Merge write namespace after get thi count of operations
	MergeLimitCount int `json:"merge_limit_count"`
}

// DBReplicationConfig is part of reindexer configuration contains replication options
type DBReplicationConfig struct {
	// Replication role. One of  none, slave, master
	Role string `json:"role"`
	// DSN to master. Only cproto schema is supported
	MasterDSN string `json:"master_dsn"`
	// Cluser ID - must be same for client and for master
	ClusteID int `json:"cluster_id"`
	// force resync on logic error conditions
	ForceSyncOnLogicError bool `json:"force_sync_on_logic_error"`
	// force resync on wrong data hash conditions
	ForceSyncOnWrongDataHash bool `json:"force_sync_on_wrong_data_hash"`
	// List of namespaces for replication. If emply, all namespaces. All replicated namespaces will become read only for slave
	Namespaces []string `json:"namespaces"`
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
