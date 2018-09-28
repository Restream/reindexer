package reindexer

import (
	"strings"

	"github.com/restream/reindexer/bindings"
)

const (
	ConfigNamespaceName           = "#config"
	MemstatsNamespaceName         = "#memstats"
	NamespacesNamespaceName       = "#namespaces"
	PerfstatsNamespaceName        = "#perfstats"
	QueriesperfstatsNamespaceName = "#quriesperfstats"
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
}

type IndexDescription struct {
	bindings.IndexDef

	IsSortable bool     `json:"is_sortable"`
	IsFulltext bool     `json:"is_fulltext"`
	Conditions []string `json:"conditions"`
}

type NamespaceDescription struct {
	Name           string             `json:"name"`
	Indexes        []IndexDescription `json:"indexes"`
	StorageEnabled bool               `json:"storage_enabled"`
}

type NamespaceMemStat struct {
	Name            string `json:"name"`
	StorageError    string `json:"storage_error"`
	StoragePath     string `json:"storage_path"`
	StorageOK       bool   `json:"storage_ok"`
	UpdatedUnixNano int64  `json:"updated_unix_nano"`
	ItemsCount      int64  `json:"items_count,omitempty"`
	EmptyItemsCount int64  `json:"empty_items_count"`
	DataSize        int64  `json:"data_size"`
	Total           struct {
		DataSize    int `json:"data_size"`
		IndexesSize int `json:"indexes_size"`
		CacheSize   int `json:"cache_size"`
	}
}

type PerfStat struct {
	TotalQueriesCount    int64 `json:"total_queries_count"`
	TotalAvgLatencyUs    int64 `json:"total_avg_latency_us"`
	TotalAvgLockTimeUs   int64 `json:"total_avg_lock_time_us"`
	LastSecQPS           int64 `json:"last_sec_qps"`
	LastSecAvgLatencyUs  int64 `json:"last_sec_avg_latency_us"`
	LastSecAvgLockTimeUs int64 `json:"last_sec_avg_lock_time_us"`
}

type NamespacePerfStat struct {
	Name    string   `json:"name"`
	Updates PerfStat `json:"updates"`
	Selects PerfStat `json:"selects"`
}

type QueryPerfStat struct {
	Query string `json:"query"`
	PerfStat
}

type DBConfigItem struct {
	Type       string                `json:"type"`
	Profiling  *DBProfilingConfig    `json:"profiling,omitempty"`
	LogQueries *[]DBLogQueriesConfig `json:"log_queries,omitempty"`
}

type DBProfilingConfig struct {
	QueriesThresholdUS int  `json:"queries_threshold_us"`
	MemStats           bool `json:"memstats"`
	PerfStats          bool `json:"perfstats"`
	QueriesPerfStats   bool `json:"queriesperfstats"`
}

type DBLogQueriesConfig struct {
	Namespace string `json:"namespace"`
	LogLevel  string `json:"log_level"`
}

// DescribeNamespaces makes a 'SELECT * FROM #namespaces' query to database.
// Return NamespaceDescription results, error
func (db *Reindexer) DescribeNamespaces() ([]*NamespaceDescription, error) {
	result := []*NamespaceDescription{}

	descs, err := db.Query(NamespacesNamespaceName).Exec().FetchAll()
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
	desc, err := db.Query(NamespacesNamespaceName).Where("name", EQ, namespace).Exec().FetchOne()
	if err != nil {
		return nil, err
	}
	return desc.(*NamespaceDescription), nil
}

// GetNamespacesMemStat makes a 'SELECT * FROM #memstats' query to database.
// Return NamespaceMemStat results, error
func (db *Reindexer) GetNamespacesMemStat() ([]*NamespaceMemStat, error) {
	result := []*NamespaceMemStat{}

	descs, err := db.Query(MemstatsNamespaceName).Exec().FetchAll()
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
	desc, err := db.Query(MemstatsNamespaceName).Where("name", EQ, namespace).Exec().FetchOne()
	if err != nil {
		return nil, err
	}
	return desc.(*NamespaceMemStat), nil
}
