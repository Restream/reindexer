package reindexer

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
	"github.com/restream/reindexer/dsl"

	_ "github.com/restream/reindexer/bindings/cproto"
)

// Condition types
const (
	// Equal '='
	EQ = bindings.EQ
	// Greater '>'
	GT = bindings.GT
	// Lower '<'
	LT = bindings.LT
	// Greater or equal '>=' (GT|EQ)
	GE = bindings.GE
	// Lower or equal '<'
	LE = bindings.LE
	// One of set 'IN []'
	SET = bindings.SET
	// All of set
	ALLSET = bindings.ALLSET
	// In range
	RANGE = bindings.RANGE
	// Any value
	ANY = bindings.ANY
	// Empty value (usualy zero len array)
	EMPTY = bindings.EMPTY
)

const (
	// ERROR Log level
	ERROR = bindings.ERROR
	// WARNING Log level
	WARNING = bindings.WARNING
	// INFO Log level
	INFO = bindings.INFO
	// TRACE Log level
	TRACE = bindings.TRACE
)

const (
	AggAvg = bindings.AggAvg
	AggSum = bindings.AggSum
)

var logger Logger = &nullLogger{}

// Reindexer The reindxer state struct
type Reindexer struct {
	lock          sync.RWMutex
	ns            map[string]*reindexerNamespace
	storagePath   string
	binding       bindings.RawBinding
	debugLevels   map[string]int
	nsHashCounter int
}

type cacheItem struct {
	item interface{}
	// version of item, for cachins
	version int
}

type reindexerNamespace struct {
	cacheItems    map[int]cacheItem
	cacheLock     sync.RWMutex
	joined        map[string][]int
	rtype         reflect.Type
	deepCopyIface bool
	name          string
	opts          NamespaceOptions
	cjsonState    cjson.State
	nsHash        int
}

// Interface for append joined items
type Joinable interface {
	Join(field string, subitems []interface{}, context interface{})
}

// JoinHandler it's function for handle join results.
// Returns bool, that indicates is values will be applied to structs.
type JoinHandler func(field string, item interface{}, subitems []interface{}) (isContinue bool)

type DeepCopy interface {
	DeepCopy() interface{}
}

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

type nullLogger struct {
}

func (nullLogger) Printf(level int, fmt string, msg ...interface{}) {
}

var (
	errNsNotFound             = errors.New("rq: Namespace is not found")
	errNsExists               = errors.New("rq: Namespace is already exists")
	errInvalidReflection      = errors.New("rq: Invalid reflection type of index")
	errStorageNotEnabled      = errors.New("rq: Storage is not enabled, can't save")
	errIteratorNotReady       = errors.New("rq: Iterator not ready. Next() must be called before")
	errJoinUnexpectedField    = errors.New("rq: Unexpected join field")
	ErrEmptyNamespace         = errors.New("rq: empty namespace name")
	ErrEmptyFieldName         = errors.New("rq: empty field name in filter")
	ErrCondType               = errors.New("rq: cond type not found")
	ErrOpInvalid              = errors.New("rq: op is invalid")
	ErrNoPK                   = errors.New("rq: No pk field in struct")
	ErrWrongType              = errors.New("rq: Wrong type of item")
	ErrMustBePointer          = errors.New("rq: Argument must be a pointer to element, not element")
	ErrMergeAndJoinInOneQuery = errors.New("rq: Can't be merge and join in one query")
	ErrNotFound               = errors.New("rq: Not found")
	ErrDeepCopyType           = errors.New("rq: DeepCopy() returns wrong type")
)

// NewReindex Create new instanse of Reindexer DB
// Returns pointer to created instance
func NewReindex(dsn string, options ...interface{}) *Reindexer {

	if dsn == "builtin" {
		dsn += "://"
	}

	u, err := url.Parse(dsn)
	if err != nil {
		panic(fmt.Errorf("Can't parse DB DSN '%s'", dsn))
	}

	binding := bindings.GetBinding(u.Scheme)

	if binding == nil {
		panic(fmt.Errorf("Reindex binding '%s' is not available, can't create DB", u.Scheme))
	}

	if err = binding.Init(u, options...); err != nil {
		panic(fmt.Errorf("Reindex binding '%s' init error: %s", u.Scheme, err.Error()))
		return nil
	}

	rx := &Reindexer{
		ns:      make(map[string]*reindexerNamespace, 100),
		binding: binding,
	}

	rx.openSystemNamespace(NamespacesNamespaceName, NamespaceDescription{})
	rx.openSystemNamespace(PerfstatsNamespaceName, NamespacePerfStat{})
	rx.openSystemNamespace(MemstatsNamespaceName, NamespaceMemStat{})
	rx.openSystemNamespace(QueriesperfstatsNamespaceName, QueryPerfStat{})
	rx.openSystemNamespace(ConfigNamespaceName, DBConfigItem{})

	return rx
}

func (db *Reindexer) openSystemNamespace(name string, itype interface{}) {
	db.ns[name] = &reindexerNamespace{
		name:       name,
		rtype:      reflect.TypeOf(itype),
		cjsonState: cjson.NewState(),
		nsHash:     db.nsHashCounter,
	}
	db.Query(name).Limit(0).Exec().Close()
	db.nsHashCounter++
}

// SetLogger sets logger interface for output reindexer logs
func (db *Reindexer) SetLogger(log Logger) {
	if log != nil {
		logger = log
		db.binding.EnableLogger(log)
	} else {
		logger = &nullLogger{}
		db.binding.DisableLogger()
	}
}

// Ping checks connection with reindexer
func (db *Reindexer) Ping() error {
	return db.binding.Ping()
}

// NamespaceOptions is options for namespace
type NamespaceOptions struct {
	// Only in memory namespace
	enableStorage bool
	// Drop ns on index mismatch error
	dropOnIndexesConflict bool
	// Drop on file errors
	dropOnFileFormatError bool
	// Cached mode options
	cachedMode uint8
}

func (opts *NamespaceOptions) NoStorage() *NamespaceOptions {
	opts.enableStorage = false
	return opts
}

func (opts *NamespaceOptions) DropOnIndexesConflict() *NamespaceOptions {
	opts.dropOnIndexesConflict = true
	return opts
}

func (opts *NamespaceOptions) DropOnFileFormatError() *NamespaceOptions {
	opts.dropOnFileFormatError = true
	return opts
}

func (opts *NamespaceOptions) CacheOff() *NamespaceOptions {
	opts.cachedMode = bindings.CacheModeOff
	return opts
}

func (opts *NamespaceOptions) CacheOn() *NamespaceOptions {
	opts.cachedMode = bindings.CacheModeOn
	return opts
}

func (opts *NamespaceOptions) CacheAggressive() *NamespaceOptions {
	opts.cachedMode = bindings.CacheModeAggressive
	return opts
}

// DefaultNamespaceOptions return defailt namespace options
func DefaultNamespaceOptions() *NamespaceOptions {
	return &NamespaceOptions{enableStorage: true, cachedMode: bindings.CacheModeOn}
}

// OpenNamespace Open or create new namespace and indexes based on passed struct.
// IndexDescription fields of struct are marked by `reindex:` tag
func (db *Reindexer) OpenNamespace(namespace string, opts *NamespaceOptions, s interface{}) (err error) {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	db.lock.Lock()
	oldNs, ok := db.ns[namespace]
	if ok {
		db.lock.Unlock()
		// Ns exists, and have different type
		if oldNs.rtype.Name() != t.Name() {
			panic(errNsExists)
		}
		// Ns exists, and have the same type.
		return nil
	}

	enableStorage := opts.enableStorage

	copier, haveDeepCopy := reflect.New(t).Interface().(DeepCopy)
	if haveDeepCopy {
		cpy := copier.DeepCopy()
		cpyType := reflect.TypeOf(reflect.Indirect(reflect.ValueOf(cpy)).Interface())
		if cpyType != reflect.TypeOf(s) {
			db.lock.Unlock()
			return ErrDeepCopyType
		}
	}

	ns := &reindexerNamespace{
		cacheItems:    make(map[int]cacheItem, 100),
		rtype:         t,
		name:          namespace,
		joined:        make(map[string][]int),
		opts:          *opts,
		cjsonState:    cjson.NewState(),
		deepCopyIface: haveDeepCopy,
		nsHash:        db.nsHashCounter,
	}

	enc := ns.cjsonState.NewEncoder()
	db.nsHashCounter++
	db.ns[namespace] = ns
	db.lock.Unlock()

	for retry := 0; retry < 2; retry++ {
		if err = db.binding.OpenNamespace(namespace, enableStorage, opts.dropOnFileFormatError, opts.cachedMode); err != nil {
			break
		}

		if err = enc.Validate(s); err != nil {
			break
		}

		db.Query(namespace).Limit(0).Exec().Close()

		if err = db.createIndex(namespace, t, false, "", "", &ns.joined); err != nil {
			rerr, ok := err.(bindings.Error)
			if ok && rerr.Code() == bindings.ErrConflict && opts.dropOnIndexesConflict {
				db.binding.DropNamespace(namespace)
				continue
			}
			db.binding.CloseNamespace(namespace)
			break
		}

		// Initial query to update payloadType
		db.Query(namespace).Limit(0).Exec().Close()
		break
	}
	if err != nil {
		db.lock.Lock()
		delete(db.ns, namespace)
		db.lock.Unlock()
	}

	return err
}

// DropNamespace - drop whole namespace from DB
func (db *Reindexer) DropNamespace(namespace string) error {
	db.lock.Lock()
	delete(db.ns, namespace)
	db.lock.Unlock()

	return db.binding.DropNamespace(namespace)
}

// CloseNamespace - close namespace, but keep storage
func (db *Reindexer) CloseNamespace(namespace string) error {
	db.lock.Lock()
	_, ok := db.ns[namespace]
	if !ok {
		db.lock.Unlock()
		return errNsNotFound
	}
	delete(db.ns, namespace)
	db.lock.Unlock()

	return db.binding.CloseNamespace(namespace)
}

// Upsert (Insert or Update) item to index
// Item must be the same type as item passed to OpenNamespace, or []byte with json
func (db *Reindexer) Upsert(namespace string, item interface{}, precepts ...string) error {
	_, err := db.modifyItem(namespace, nil, item, nil, modeUpsert, precepts...)
	return err
}

// Insert item to namespace.
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
// Return 0, if no item was inserted, 1 if item was inserted
func (db *Reindexer) Insert(namespace string, item interface{}, precepts ...string) (int, error) {
	return db.modifyItem(namespace, nil, item, nil, modeInsert, precepts...)
}

// Update item to namespace.
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
// Return 0, if no item was updated, 1 if item was updated
func (db *Reindexer) Update(namespace string, item interface{}, precepts ...string) (int, error) {
	return db.modifyItem(namespace, nil, item, nil, modeUpdate, precepts...)
}

// Delete - remove item  from namespace
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
func (db *Reindexer) Delete(namespace string, item interface{}, precepts ...string) error {
	_, err := db.modifyItem(namespace, nil, item, nil, modeDelete, precepts...)
	return err
}

// ConfigureIndex - congigure index.
// config argument must be struct with index configuration
func (db *Reindexer) ConfigureIndex(namespace, index string, config interface{}) error {
	json, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return db.binding.ConfigureIndex(namespace, index, string(json))
}

// DropIndex - drop index.
func (db *Reindexer) DropIndex(namespace, index string) error {
	return db.binding.DropIndex(namespace, index)
}

func loglevelToString(logLevel int) string {
	switch logLevel {
	case INFO:
		return "info"
	case TRACE:
		return "trace"
	case ERROR:
		return "error"
	case WARNING:
		return "warning"
	case 0:
		return "none"
	default:
		return ""
	}
}

// SetDefaultQueryDebug sets default debug level for queries to namespaces
func (db *Reindexer) SetDefaultQueryDebug(namespace string, level int) {

	citem := &DBConfigItem{Type: "profiling"}
	if item, ok := db.Query(ConfigNamespaceName).WhereString("type", EQ, "profiling").Get(); ok {
		citem = item.(*DBConfigItem)
	}

	found := false

	if citem.LogQueries == nil {
		logQueries := make([]DBLogQueriesConfig, 0, 1)
		citem.LogQueries = &logQueries
	}

	for i := range *citem.LogQueries {
		if (*citem.LogQueries)[i].Namespace == namespace {
			(*citem.LogQueries)[i].LogLevel = loglevelToString(level)
			found = true
		}
	}
	if !found {
		*citem.LogQueries = append(*citem.LogQueries, DBLogQueriesConfig{namespace, loglevelToString(level)})
	}
	db.Upsert(ConfigNamespaceName, citem)
}

// Query Create new Query for building request
func (db *Reindexer) Query(namespace string) *Query {
	return newQuery(db, namespace)
}

// ExecSQL make query to database. Query is SQL statement
// Return Iterator
func (db *Reindexer) ExecSQL(query string) *Iterator {
	// TODO: do not parse query string twice in go and cpp
	namespace := ""
	querySlice := strings.Fields(strings.ToLower(query))

	for i := range querySlice {
		if querySlice[i] == "from" && i+1 < len(querySlice) {
			namespace = querySlice[i+1]
			break
		}
	}

	return db.execSQL(namespace, query)
}

func (db *Reindexer) ExecSQLToJSON(query string) *JSONIterator {
	// TODO: do not parse query string twice in go and cpp
	namespace := ""
	querySlice := strings.Fields(strings.ToLower(query))

	for i := range querySlice {
		if querySlice[i] == "from" && i+1 < len(querySlice) {
			namespace = querySlice[i+1]
			break
		}
	}

	return db.execSQLAsJSON(namespace, query)
}

// BeginTx - start update transaction
func (db *Reindexer) BeginTx(namespace string) (*Tx, error) {
	return newTx(db, namespace)
}

// MustBeginTx - start update transaction, panic on error
func (db *Reindexer) MustBeginTx(namespace string) *Tx {
	tx, err := newTx(db, namespace)
	if err != nil {
		panic(err)
	}
	return tx
}

// TODO make func as void
// setUpdatedAt - set updated at time for namespace
func (db *Reindexer) setUpdatedAt(ns *reindexerNamespace, updatedAt time.Time) error {
	str := strconv.FormatInt(updatedAt.UnixNano(), 10)

	db.putMeta(ns.name, "updated", []byte(str))

	return nil
}

// GetUpdatedAt - get updated at time of namespace
func (db *Reindexer) GetUpdatedAt(namespace string) (*time.Time, error) {
	b, err := db.getMeta(namespace, "updated")
	if err != nil {
		return nil, err
	}

	updatedAtUnixNano, err := strconv.ParseInt(string(b), 10, 64)

	// will return 1970-01-01 on parser error
	updatedAt := time.Unix(0, updatedAtUnixNano).UTC()

	return &updatedAt, nil
}

func (db *Reindexer) QueryFrom(d dsl.DSL) (*Query, error) {
	if d.Namespace == "" {
		return nil, ErrEmptyNamespace
	}

	q := db.Query(d.Namespace).Offset(d.Offset)

	if d.Limit > 0 {
		q.Limit(d.Limit)
	}

	if d.Distinct != "" {
		q.Distinct(d.Distinct)
	}
	if d.Sort.Field != "" {
		q.Sort(d.Sort.Field, d.Sort.Desc, d.Sort.Values...)
	}

	for _, filter := range d.Filters {
		if filter.Field == "" {
			return nil, ErrEmptyFieldName
		}
		if filter.Value == nil {
			continue
		}

		cond, err := GetCondType(filter.Cond)
		if err != nil {
			return nil, err
		}

		switch strings.ToUpper(filter.Op) {
		case "":
			q.Where(filter.Field, cond, filter.Value)
		case "NOT":
			q.Not().Where(filter.Field, cond, filter.Value)
		default:
			return nil, ErrOpInvalid
		}
	}

	return q, nil
}

// GetStats Get local thread reindexer usage stats
// [[deprecated]]
func (db *Reindexer) GetStats() bindings.Stats {
	log.Println("Deprecated function reindexer.GetStats call. Use SELECT * FROM '#perfstats' to get performance statistics")
	return bindings.Stats{}
}

// ResetStats Reset local thread reindexer usage stats
// [[deprecated]]
func (db *Reindexer) ResetStats() {
}

// EnableStorage enables persistent storage of data
// [[deprecated]] storage path should be passed as DSN part to reindexer.NewReindex (""), e.g. reindexer.NewReindexer ("builtin:///tmp/reindex")
func (db *Reindexer) EnableStorage(storagePath string) error {
	log.Println("Deprecated function reindexer.EnableStorage call")
	return db.binding.EnableStorage(storagePath)
}
