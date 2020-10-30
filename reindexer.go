package reindexer

import (
	"context"

	"github.com/restream/reindexer/bindings"
	_ "github.com/restream/reindexer/bindings/cproto"
	"github.com/restream/reindexer/dsl"
	// _ "github.com/restream/reindexer/bindings/builtinserver"
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
	// String like pattern
	LIKE = bindings.LIKE
	// Geometry DWithin
	DWITHIN = bindings.DWITHIN
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

// Aggregation funcs
const (
	AggAvg      = bindings.AggAvg
	AggSum      = bindings.AggSum
	AggFacet    = bindings.AggFacet
	AggMin      = bindings.AggMin
	AggMax      = bindings.AggMax
	AggDistinct = bindings.AggDistinct
)

// Reindexer error codes
const (
	ErrCodeOK               = bindings.ErrOK
	ErrCodeParseSQL         = bindings.ErrParseSQL
	ErrCodeQueryExec        = bindings.ErrQueryExec
	ErrCodeParams           = bindings.ErrParams
	ErrCodeLogic            = bindings.ErrLogic
	ErrCodeParseJson        = bindings.ErrParseJson
	ErrCodeParseDSL         = bindings.ErrParseDSL
	ErrCodeConflict         = bindings.ErrConflict
	ErrCodeParseBin         = bindings.ErrParseBin
	ErrCodeForbidden        = bindings.ErrForbidden
	ErrCodeWasRelock        = bindings.ErrWasRelock
	ErrCodeNotValid         = bindings.ErrNotValid
	ErrCodeNetwork          = bindings.ErrNetwork
	ErrCodeNotFound         = bindings.ErrNotFound
	ErrCodeStateInvalidated = bindings.ErrStateInvalidated
	ErrCodeTimeout          = bindings.ErrTimeout
)

var logger Logger = &nullLogger{}

// Reindexer The reindxer state struct
type Reindexer struct {
	impl *reindexerImpl
	ctx  context.Context
}

// IndexDef - Inddex  definition struct
type IndexDef bindings.IndexDef

// Error - reindexer Error interface
type Error interface {
	Error() string
	Code() int
}

// Joinable is an interface for append joined items
type Joinable interface {
	Join(field string, subitems []interface{}, context interface{})
}

// JoinHandler it's function for handle join results.
// Returns bool, that indicates whether automatic join strategy still needs to be applied.
// If `useAutomaticJoinStrategy` is false - it means that JoinHandler takes full responsibility of performing join.
// If `useAutomaticJoinStrategy` is true - it means JoinHandler will perform only part of the work, required during join, the rest will be done using automatic join strategy.
// Automatic join strategy is defined as:
// - use Join method to perform join (in case item implements Joinable interface)
// - use reflection to perform join otherwise
type JoinHandler func(field string, item interface{}, subitems []interface{}) (useAutomaticJoinStrategy bool)

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
	errNsNotFound          = bindings.NewError("rq: Namespace is not found", ErrCodeNotFound)
	errNsExists            = bindings.NewError("rq: Namespace is already exists", ErrCodeParams)
	errInvalidReflection   = bindings.NewError("rq: Invalid reflection type of index", ErrCodeParams)
	errStorageNotEnabled   = bindings.NewError("rq: Storage is not enabled, can't save", ErrCodeLogic)
	errIteratorNotReady    = bindings.NewError("rq: Iterator not ready. Next() must be called before", ErrCodeLogic)
	errJoinUnexpectedField = bindings.NewError("rq: Unexpected join field", ErrCodeParams)
	ErrEmptyNamespace      = bindings.NewError("rq: empty namespace name", ErrCodeParams)
	ErrEmptyFieldName      = bindings.NewError("rq: empty field name in filter", ErrCodeParams)
	ErrEmptyAggFieldName   = bindings.NewError("rq: empty field name in aggregation", ErrCodeParams)
	ErrCondType            = bindings.NewError("rq: cond type not found", ErrCodeParams)
	ErrOpInvalid           = bindings.NewError("rq: op is invalid", ErrCodeParams)
	ErrAggInvalid          = bindings.NewError("rq: agg is invalid", ErrCodeParams)
	ErrNoPK                = bindings.NewError("rq: No pk field in struct", ErrCodeParams)
	ErrWrongType           = bindings.NewError("rq: Wrong type of item", ErrCodeParams)
	ErrMustBePointer       = bindings.NewError("rq: Argument must be a pointer to element, not element", ErrCodeParams)
	ErrNotFound            = bindings.NewError("rq: Not found", ErrCodeNotFound)
	ErrDeepCopyType        = bindings.NewError("rq: DeepCopy() returns wrong type", ErrCodeParams)
)

type AggregationResult struct {
	Fields []string `json:"fields"`
	Type   string   `json:"type"`
	Value  float64  `json:"value,omitempty"`
	Facets []struct {
		Values []string `json:"values"`
		Count  int      `json:"count"`
	} `json:"facets,omitempty"`
	Distincts []string `json:"distincts,omitempty"`
}

// NewReindex Create new instanse of Reindexer DB
// Returns pointer to created instance
func NewReindex(dsn interface{}, options ...interface{}) *Reindexer {
	rx := &Reindexer{
		impl: newReindexImpl(dsn, options...),
		ctx:  context.TODO(),
	}
	return rx
}

// Status will return current db status
func (db *Reindexer) Status() bindings.Status {
	return db.impl.getStatus(db.ctx)
}

// SetLogger sets logger interface for output reindexer logs
func (db *Reindexer) SetLogger(log Logger) {
	db.impl.setLogger(log)
}

// ReopenLogFiles reopens log files
func (db *Reindexer) ReopenLogFiles() error {
	return db.impl.reopenLogFiles()
}

// Ping checks connection with reindexer
func (db *Reindexer) Ping() error {
	return db.impl.ping(db.ctx)
}

func (db *Reindexer) Close() {
	db.impl.close()
}

func (db *Reindexer) RenameNs(srcNsName string, dstNsName string) {
	db.impl.lock.Lock()
	defer db.impl.lock.Unlock()
	srcNs, ok := (db.impl).ns[srcNsName]
	if ok {
		delete(db.impl.ns, srcNsName)
		db.impl.ns[dstNsName] = srcNs
	} else {
		delete(db.impl.ns, dstNsName)
	}

}

// NamespaceOptions is options for namespace
type NamespaceOptions struct {
	// Only in memory namespace
	enableStorage bool
	// Drop ns on index mismatch error
	dropOnIndexesConflict bool
	// Drop on file errors
	dropOnFileFormatError bool
	// Disable object cache
	disableObjCache bool
	// Object cache items count
	objCacheItemsCount uint64
}

// DefaultNamespaceOptions return default namespace options
func DefaultNamespaceOptions() *NamespaceOptions {
	return &NamespaceOptions{
		enableStorage:      true,
		objCacheItemsCount: 256000,
	}
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

func (opts *NamespaceOptions) DisableObjCache() *NamespaceOptions {
	opts.disableObjCache = true
	return opts
}

// Set maximum items count in Object Cache. Default is 256000
func (opts *NamespaceOptions) ObjCacheSize(count int) *NamespaceOptions {
	opts.objCacheItemsCount = uint64(count)
	return opts
}

// OpenNamespace Open or create new namespace and indexes based on passed struct.
// IndexDef fields of struct are marked by `reindex:` tag
func (db *Reindexer) OpenNamespace(namespace string, opts *NamespaceOptions, s interface{}) (err error) {
	return db.impl.openNamespace(db.ctx, namespace, opts, s)
}

// RegisterNamespace Register go type against namespace. There are no data and indexes changes will be performed
func (db *Reindexer) RegisterNamespace(namespace string, opts *NamespaceOptions, s interface{}) (err error) {
	return db.impl.registerNamespace(namespace, opts, s)
}

// DropNamespace - drop whole namespace from DB
func (db *Reindexer) DropNamespace(namespace string) error {
	return db.impl.dropNamespace(db.ctx, namespace)
}

// TruncateNamespace - delete all items from namespace
func (db *Reindexer) TruncateNamespace(namespace string) error {
	return db.impl.truncateNamespace(db.ctx, namespace)
}

// RenameNamespace - Rename namespace. If namespace with dstNsName exists, then it is replaced.
func (db *Reindexer) RenameNamespace(srcNsName string, dstNsName string) error {
	return db.impl.renameNamespace(db.ctx, srcNsName, dstNsName)
}

// CloseNamespace - close namespace, but keep storage
func (db *Reindexer) CloseNamespace(namespace string) error {
	return db.impl.closeNamespace(db.ctx, namespace)
}

// Upsert (Insert or Update) item to index
// Item must be the same type as item passed to OpenNamespace, or []byte with json
// If the precepts are provided and the item is a pointer, the value pointed by item will be updated
func (db *Reindexer) Upsert(namespace string, item interface{}, precepts ...string) error {
	return db.impl.upsert(db.ctx, namespace, item, precepts...)
}

// Insert item to namespace by PK
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
// Return 0, if no item was inserted, 1 if item was inserted
// If the precepts are provided and the item is a pointer, the value pointed by item will be updated
func (db *Reindexer) Insert(namespace string, item interface{}, precepts ...string) (int, error) {
	return db.impl.insert(db.ctx, namespace, item, precepts...)
}

// Update item to namespace by PK
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
// Return 0, if no item was updated, 1 if item was updated
// If the precepts are provided and the item is a pointer, the value pointed by item will be updated
func (db *Reindexer) Update(namespace string, item interface{}, precepts ...string) (int, error) {
	return db.impl.update(db.ctx, namespace, item, precepts...)
}

// Delete - remove single item from namespace by PK
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
// If the precepts are provided and the item is a pointer, the value pointed by item will be updated
func (db *Reindexer) Delete(namespace string, item interface{}, precepts ...string) error {
	return db.impl.delete(db.ctx, namespace, item, precepts...)
}

// ConfigureIndex - congigure index.
// config argument must be struct with index configuration
// Deprecated: Use UpdateIndex instead.
func (db *Reindexer) ConfigureIndex(namespace, index string, config interface{}) error {
	return db.impl.configureIndex(db.ctx, namespace, index, config)
}

// AddIndex - add index.
func (db *Reindexer) AddIndex(namespace string, indexDef ...IndexDef) error {
	return db.impl.addIndex(db.ctx, namespace, indexDef...)
}

// UpdateIndex - update index.
func (db *Reindexer) UpdateIndex(namespace string, indexDef IndexDef) error {
	return db.impl.updateIndex(db.ctx, namespace, indexDef)
}

// DropIndex - drop index.
func (db *Reindexer) DropIndex(namespace, index string) error {
	return db.impl.dropIndex(db.ctx, namespace, index)
}

// SetDefaultQueryDebug sets default debug level for queries to namespaces
func (db *Reindexer) SetDefaultQueryDebug(namespace string, level int) error {
	return db.impl.setDefaultQueryDebug(db.ctx, namespace, level)
}

// Query Create new Query for building request
func (db *Reindexer) Query(namespace string) *Query {
	return db.impl.query(namespace)
}

// ExecSQL make query to database. Query is a SQL statement.
// Return Iterator.
func (db *Reindexer) ExecSQL(query string) *Iterator {
	return db.impl.execSQL(db.ctx, query)
}

// ExecSQLToJSON make query to database. Query is a SQL statement.
// Return JSONIterator.
func (db *Reindexer) ExecSQLToJSON(query string) *JSONIterator {
	return db.impl.execSQLToJSON(db.ctx, query)
}

// BeginTx - start update transaction. Please not:
// 1. Returned transaction object is not thread safe and can't be used from different goroutines.
// 2. Transaction object holds Reindexer's resources, therefore application should explicitly
//    call Rollback or Commit, otherwise resources will leak

func (db *Reindexer) BeginTx(namespace string) (*Tx, error) {
	return db.impl.beginTx(db.ctx, namespace)
}

// MustBeginTx - start update transaction, panic on error
func (db *Reindexer) MustBeginTx(namespace string) *Tx {
	return db.impl.mustBeginTx(db.ctx, namespace)
}

// QueryFrom - create query from DSL and execute it
func (db *Reindexer) QueryFrom(d dsl.DSL) (*Query, error) {
	return db.impl.queryFrom(d)
}

// GetStats Get local thread reindexer usage stats
// Deprecated: Use SELECT * FROM '#perfstats' to get performance statistics.
func (db *Reindexer) GetStats() bindings.Stats {
	return db.impl.getStats()
}

// ResetStats Reset local thread reindexer usage stats
// Deprecated: no longer used.
func (db *Reindexer) ResetStats() {
	db.impl.resetStats()
}

// EnableStorage enables persistent storage of data
// Deprecated: storage path should be passed as DSN part to reindexer.NewReindex (""), e.g. reindexer.NewReindexer ("builtin:///tmp/reindex").
func (db *Reindexer) EnableStorage(storagePath string) error {
	return db.impl.enableStorage(db.ctx, storagePath)
}

func (db *Reindexer) PutMeta(namespace, key string, data []byte) error {
	return db.impl.putMeta(db.ctx, namespace, key, data)
}

func (db *Reindexer) GetMeta(namespace, key string) ([]byte, error) {
	return db.impl.getMeta(db.ctx, namespace, key)
}

// WithContext Add context to next method call
func (db *Reindexer) WithContext(ctx context.Context) *Reindexer {
	dbC := &Reindexer{
		impl: db.impl,
		ctx:  ctx,
	}
	return dbC
}
