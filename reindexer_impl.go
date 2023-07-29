package reindexer

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	otel "go.opentelemetry.io/otel"
	otelattr "go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/restream/reindexer/v3/bindings"
	"github.com/restream/reindexer/v3/cjson"
	"github.com/restream/reindexer/v3/dsl"
)

type reindexerNamespace struct {
	cacheItems    *cacheItems
	cacheLock     sync.RWMutex
	joined        map[string][]int
	indexes       []bindings.IndexDef
	schema        bindings.SchemaDef
	rtype         reflect.Type
	deepCopyIface bool
	name          string
	opts          NamespaceOptions
	cjsonState    cjson.State
	nsHash        int
	opened        bool
}

// reindexerImpl The reindxer state struct
type reindexerImpl struct {
	lock          sync.RWMutex
	ns            map[string]*reindexerNamespace
	storagePath   string
	binding       bindings.RawBinding
	debugLevels   map[string]int
	nsHashCounter int
	status        error

	promMetrics *reindexerPrometheusMetrics

	otelTracer           oteltrace.Tracer
	otelCommonTraceAttrs []otelattr.KeyValue
}

type cacheItems struct {
	// cached items
	items *lru.Cache
}

func (ci *cacheItems) Reset() {
	if ci.items == nil {
		return
	}
	ci.items.Purge()
}

func (ci *cacheItems) Remove(key int) {
	if ci.items == nil {
		return
	}
	ci.items.Remove(key)
}

func (ci *cacheItems) Add(key int, item *cacheItem) {
	if ci.items == nil {
		return
	}
	ci.items.Add(key, item)
}

func (ci *cacheItems) Len() int {
	if ci.items == nil {
		return 0
	}
	return ci.items.Len()
}

func (ci *cacheItems) Get(key int) (*cacheItem, bool) {
	if ci.items == nil {
		return nil, false
	}

	item, ok := ci.items.Get(key)
	if ok {
		return item.(*cacheItem), ok
	}
	return nil, false
}

type cacheItem struct {
	// cached data
	item interface{}
	// version of item
	version int
}

func newCacheItems(count uint64) (*cacheItems, error) {
	cache, err := lru.New(int(count))
	if err != nil {
		return nil, err
	}
	return &cacheItems{
		items: cache,
	}, nil
}

// NewReindexImpl Create new instanse of Reindexer DB
// Returns pointer to created instance
func newReindexImpl(dsn interface{}, options ...interface{}) *reindexerImpl {
	scheme, dsnParsed := dsnParse(dsn)

	binding := bindings.GetBinding(scheme)
	if binding == nil {
		panic(fmt.Errorf("Reindex binding '%s' is not available, can't create DB", scheme))
	}

	binding = binding.Clone()

	rx := &reindexerImpl{
		ns:      make(map[string]*reindexerNamespace, 100),
		binding: binding,
	}

	for _, opt := range options {
		switch v := opt.(type) {
		case bindings.OptionPrometheusMetrics:
			if v.EnablePrometheusMetrics {
				rx.promMetrics = newPrometheusMetrics(dsnParsed)
			}

		case bindings.OptionOpenTelemetry:
			if v.EnableTracing {
				rx.otelTracer = otel.Tracer(
					"reindexer/v3",
					oteltrace.WithInstrumentationVersion(bindings.ReindexerVersion),
				)
				rx.otelCommonTraceAttrs = []otelattr.KeyValue{
					otelattr.String("rx.dsn", dsnString(dsnParsed)),
				}
			}
		}
	}

	if err := binding.Init(dsnParsed, options...); err != nil {
		rx.status = err
	}

	if changing, ok := binding.(bindings.RawBindingChanging); ok {
		changing.OnChangeCallback(rx.resetCaches)
	}

	opts := &NamespaceOptions{
		disableObjCache: true,
	}
	rx.registerNamespaceImpl(NamespacesNamespaceName, opts, NamespaceDescription{})
	rx.registerNamespaceImpl(PerfstatsNamespaceName, opts, NamespacePerfStat{})
	rx.registerNamespaceImpl(MemstatsNamespaceName, opts, NamespaceMemStat{})
	rx.registerNamespaceImpl(QueriesperfstatsNamespaceName, opts, QueryPerfStat{})
	rx.registerNamespaceImpl(ConfigNamespaceName, opts, DBConfigItem{})
	rx.registerNamespaceImpl(ClientsStatsNamespaceName, opts, ClientConnectionStat{})
	return rx
}

// getStatus will return current db status
func (db *reindexerImpl) getStatus(ctx context.Context) bindings.Status {
	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.Status").End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("Status", "")).ObserveDuration()
	}

	status := db.binding.Status(ctx)
	if db.status != nil {
		status.Err = db.status // If reindexerImpl has an error, return it.
	}

	db.lock.RLock()
	nsArray := make([]*reindexerNamespace, 0, len(db.ns))
	for _, ns := range db.ns {
		nsArray = append(nsArray, ns)
	}
	db.lock.RUnlock()

	for _, ns := range nsArray {
		status.Cache.CurSize += int64(ns.cacheItems.Len())
	}

	return status
}

// setLogger sets logger interface for output reindexer logs
func (db *reindexerImpl) setLogger(log Logger) {
	if log != nil {
		logger = log
		db.binding.EnableLogger(log)
	} else {
		logger = &nullLogger{}
		db.binding.DisableLogger()
	}
}

func (db *reindexerImpl) reopenLogFiles() error {
	return db.binding.ReopenLogFiles()
}

// ping checks connection with reindexer
func (db *reindexerImpl) ping(ctx context.Context) error {
	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.Ping").End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("Ping", "")).ObserveDuration()
	}

	return db.binding.Ping(ctx)
}

func (db *reindexerImpl) close() {
	if err := db.binding.Finalize(); err != nil {
		panic(err)
	}
}

// openNamespace Open or create new namespace and indexes based on passed struct.
// IndexDef fields of struct are marked by `reindex:` tag
func (db *reindexerImpl) openNamespace(ctx context.Context, namespace string, opts *NamespaceOptions, s interface{}) (err error) {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.OpenNamespace", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("OpenNamespace", namespace)).ObserveDuration()
	}

	if err = db.registerNamespaceImpl(namespace, opts, s); err != nil {
		return err
	}

	ns, err := db.getNS(namespace)
	if err != nil {
		return err
	}

	for retry := 0; retry < 2; retry++ {
		if err = db.binding.OpenNamespace(ctx, namespace, opts.enableStorage, opts.dropOnFileFormatError); err != nil {
			break
		}

		for _, indexDef := range ns.indexes {
			if err = db.binding.AddIndex(ctx, namespace, indexDef); err != nil {
				break
			}
		}

		if err == nil {
			if err = db.binding.SetSchema(ctx, namespace, ns.schema); err != nil {
				if rerr, ok := err.(bindings.Error); ok && rerr.Code() == bindings.ErrParams {
					// Ignore error from old server which doesn't support SetSchema
					err = nil
				} else {
					break
				}
			}
		}

		if err != nil {
			rerr, ok := err.(bindings.Error)
			if ok && rerr.Code() == bindings.ErrConflict && opts.dropOnIndexesConflict {
				db.binding.DropNamespace(ctx, namespace)
				continue
			}
			db.binding.CloseNamespace(ctx, namespace)
			break
		}

		break
	}

	return err
}

// RegisterNamespace Register go type against namespace. There are no data and indexes changes will be performed
func (db *reindexerImpl) registerNamespace(ctx context.Context, namespace string, opts *NamespaceOptions, s interface{}) (err error) {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.RegisterNamespace", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("RegisterNamespace", namespace)).ObserveDuration()
	}

	return db.registerNamespaceImpl(namespace, opts, s)
}

// registerNamespace Register go type against namespace. There are no data and indexes changes will be performed
func (db *reindexerImpl) registerNamespaceImpl(namespace string, opts *NamespaceOptions, s interface{}) (err error) {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	namespace = strings.ToLower(namespace)

	db.lock.Lock()
	defer db.lock.Unlock()

	oldNs, ok := db.ns[namespace]
	if ok {
		// Ns exists, and have different type
		if oldNs.rtype.Name() != t.Name() {
			return errNsExists
		}
		// Ns exists, and have the same type.
		return nil
	}
	haveDeepCopy := false
	cacheItems := &cacheItems{}

	if !opts.disableObjCache {
		var copier DeepCopy
		copier, haveDeepCopy = reflect.New(t).Interface().(DeepCopy)
		if haveDeepCopy {
			cpy := copier.DeepCopy()
			cpyType := reflect.TypeOf(reflect.Indirect(reflect.ValueOf(cpy)).Interface())
			if cpyType != reflect.TypeOf(s) {
				return ErrDeepCopyType
			}
		}
		cacheItems, err = newCacheItems(opts.objCacheItemsCount)
		if err != nil {
			return err
		}
	}

	ns := &reindexerNamespace{
		cacheItems:    cacheItems,
		rtype:         t,
		name:          namespace,
		joined:        make(map[string][]int),
		opts:          *opts,
		cjsonState:    cjson.NewState(),
		deepCopyIface: haveDeepCopy,
		nsHash:        db.nsHashCounter,
		opened:        false,
	}

	validator := cjson.Validator{}

	if err = validator.Validate(s); err != nil {
		return err
	}
	if ns.indexes, err = parseIndexes(namespace, ns.rtype, &ns.joined); err != nil {
		return err
	}
	if schema := parseSchema(namespace, ns.rtype); schema != nil {
		ns.schema = *schema
	}

	db.nsHashCounter++
	db.ns[namespace] = ns
	return nil
}

// dropNamespace - drop whole namespace from DB
func (db *reindexerImpl) dropNamespace(ctx context.Context, namespace string) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.DropNamespace", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("DropNamespace", namespace)).ObserveDuration()
	}

	db.lock.Lock()
	delete(db.ns, namespace)
	db.lock.Unlock()

	return db.binding.DropNamespace(ctx, namespace)
}

// truncateNamespace - delete all items from namespace
func (db *reindexerImpl) truncateNamespace(ctx context.Context, namespace string) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.TruncateNamespace", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("TruncateNamespace", namespace)).ObserveDuration()
	}

	return db.binding.TruncateNamespace(ctx, namespace)
}

// RenameNamespace - Rename namespace. If namespace with dstNsName exists, then it is replaced.
func (db *reindexerImpl) renameNamespace(ctx context.Context, srcNsName string, dstNsName string) error {
	srcNsName = strings.ToLower(srcNsName)
	dstNsName = strings.ToLower(dstNsName)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.RenameNamespace", otelattr.String("rx.ns", srcNsName)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("RenameNamespace", srcNsName)).ObserveDuration()
	}

	err := db.binding.RenameNamespace(ctx, srcNsName, dstNsName)
	if err != nil {
		return err
	}
	db.lock.Lock()
	defer db.lock.Unlock()

	srcNs, ok := db.ns[srcNsName]
	if ok {
		delete(db.ns, srcNsName)
		db.ns[dstNsName] = srcNs
	} else {
		delete(db.ns, dstNsName)
	}
	return err
}

// closeNamespace - close namespace, but keep storage
func (db *reindexerImpl) closeNamespace(ctx context.Context, namespace string) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.CloseNamespace", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("CloseNamespace", namespace)).ObserveDuration()
	}

	db.lock.Lock()
	delete(db.ns, namespace)
	db.lock.Unlock()

	return db.binding.CloseNamespace(ctx, namespace)
}

// upsert (Insert or Update) item to index
// Item must be the same type as item passed to OpenNamespace, or []byte with json
func (db *reindexerImpl) upsert(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.Upsert", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("Upsert", namespace)).ObserveDuration()
	}

	_, err := db.modifyItem(ctx, namespace, nil, item, nil, modeUpsert, precepts...)
	return err
}

// insert item to namespace by PK
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
// Return 0, if no item was inserted, 1 if item was inserted
func (db *reindexerImpl) insert(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.Insert", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("Insert", namespace)).ObserveDuration()
	}

	return db.modifyItem(ctx, namespace, nil, item, nil, modeInsert, precepts...)
}

// update item to namespace by PK
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
// Return 0, if no item was updated, 1 if item was updated
func (db *reindexerImpl) update(ctx context.Context, namespace string, item interface{}, precepts ...string) (int, error) {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.Update", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("Update", namespace)).ObserveDuration()
	}

	return db.modifyItem(ctx, namespace, nil, item, nil, modeUpdate, precepts...)
}

// delete - remove single item from namespace by PK
// Item must be the same type as item passed to OpenNamespace, or []byte with json data
func (db *reindexerImpl) delete(ctx context.Context, namespace string, item interface{}, precepts ...string) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.Delete", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("Delete", namespace)).ObserveDuration()
	}

	_, err := db.modifyItem(ctx, namespace, nil, item, nil, modeDelete, precepts...)
	return err
}

// configureIndex - configure an index.
// config argument must be struct with index configuration
// Deprecated: Use UpdateIndex instead.
func (db *reindexerImpl) configureIndex(ctx context.Context, namespace, index string, config interface{}) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.ConfigureIndex", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("ConfigureIndex", namespace)).ObserveDuration()
	}

	nsDef, err := db.describeNamespace(ctx, namespace)
	if err != nil {
		return err
	}

	index = strings.ToLower(index)
	for _, iDef := range nsDef.Indexes {
		if strings.ToLower(iDef.Name) == index {
			iDef.Config = config
			return db.binding.UpdateIndex(ctx, namespace, bindings.IndexDef(iDef.IndexDef))
		}
	}
	return fmt.Errorf("rq: Index '%s' not found in namespace %s", index, namespace)
}

// addIndex - add an index.
func (db *reindexerImpl) addIndex(ctx context.Context, namespace string, indexDef ...IndexDef) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.AddIndex", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("AddIndex", namespace)).ObserveDuration()
	}

	for _, index := range indexDef {
		if err := db.binding.AddIndex(ctx, namespace, bindings.IndexDef(index)); err != nil {
			return err
		}
	}
	return nil
}

// updateIndex - update an index.
func (db *reindexerImpl) updateIndex(ctx context.Context, namespace string, indexDef IndexDef) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.UpdateIndex", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("UpdateIndex", namespace)).ObserveDuration()
	}

	return db.binding.UpdateIndex(ctx, namespace, bindings.IndexDef(indexDef))
}

// dropIndex - drop index.
func (db *reindexerImpl) dropIndex(ctx context.Context, namespace, index string) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.DropIndex", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("DropIndex", namespace)).ObserveDuration()
	}

	return db.binding.DropIndex(ctx, namespace, index)
}

func (db *reindexerImpl) putMeta(ctx context.Context, namespace, key string, data []byte) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.PutMeta", otelattr.String("rx.ns", namespace), otelattr.String("rx.meta.key", key)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("PutMeta", namespace)).ObserveDuration()
	}

	return db.binding.PutMeta(ctx, namespace, key, string(data))
}

func (db *reindexerImpl) getMeta(ctx context.Context, namespace, key string) ([]byte, error) {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.GetMeta", otelattr.String("rx.ns", namespace), otelattr.String("rx.meta.key", key)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("GetMeta", namespace)).ObserveDuration()
	}

	out, err := db.binding.GetMeta(ctx, namespace, key)
	if err != nil {
		return nil, err
	}

	defer out.Free()

	ret := make([]byte, len(out.GetBuf()))
	copy(ret, out.GetBuf())

	return ret, nil
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

// setDefaultQueryDebug sets default debug level for queries to namespaces
func (db *reindexerImpl) setDefaultQueryDebug(ctx context.Context, namespace string, level int) error {
	citem := &DBConfigItem{Type: "namespaces"}
	item, err := db.query(ConfigNamespaceName).WhereString("type", EQ, "namespaces").ExecCtx(ctx).FetchOne()
	if err != nil {
		return err
	}

	citem = item.(*DBConfigItem)
	defaultCfg := DBNamespacesConfig{
		JoinCacheMode:           "off",
		StartCopyPolicyTxSize:   10000,
		CopyPolicyMultiplier:    5,
		TxSizeToAlwaysCopy:      100000,
		OptimizationTimeout:     800,
		OptimizationSortWorkers: 4,
		WALSize:                 4000000,
	}
	found := false

	if citem.Namespaces == nil {
		namespaces := make([]DBNamespacesConfig, 0, 1)
		citem.Namespaces = &namespaces
	}

	for i := range *citem.Namespaces {
		switch (*citem.Namespaces)[i].Namespace {
		case namespace:
			(*citem.Namespaces)[i].LogLevel = loglevelToString(level)
			found = true
		case "*":
			defaultCfg = (*citem.Namespaces)[i]
		}
	}
	if !found {
		nsCfg := defaultCfg
		nsCfg.Namespace = namespace
		nsCfg.LogLevel = loglevelToString(level)
		*citem.Namespaces = append(*citem.Namespaces, nsCfg)
	}
	return db.upsert(ctx, ConfigNamespaceName, citem)
}

// query Create new Query for building request
func (db *reindexerImpl) query(namespace string) *Query {
	namespace = strings.ToLower(namespace)

	return newQuery(db, namespace, nil)
}

func (db *reindexerImpl) queryTx(namespace string, tx *Tx) *Query {
	namespace = strings.ToLower(namespace)

	return newQuery(db, namespace, tx)
}

// execSQL make query to database. Query is a SQL statement.
// Return Iterator.
func (db *reindexerImpl) execSQL(ctx context.Context, query string) *Iterator {
	namespace := getQueryNamespace(query)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.ExecSQL", otelattr.String("rx.ns", namespace), otelattr.String("rx.sql", query)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("ExecSQL", namespace)).ObserveDuration()
	}

	result, nsArray, err := db.prepareSQL(ctx, namespace, query, false)
	if err != nil {
		return errIterator(err)
	}

	iter := newIterator(ctx, db, namespace, nil, result, nsArray, nil, nil, nil)

	return iter
}

// execSQLToJSON make query to database. Query is a SQL statement.
// Return JSONIterator.
func (db *reindexerImpl) execSQLToJSON(ctx context.Context, query string) *JSONIterator {
	namespace := getQueryNamespace(query)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.ExecSQLToJSON", otelattr.String("rx.ns", namespace), otelattr.String("rx.sql", query)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("ExecSQLToJSON", namespace)).ObserveDuration()
	}

	result, _, err := db.prepareSQL(ctx, namespace, query, true)
	if err != nil {
		return errJSONIterator(err)
	}

	defer result.Free()

	json, jsonOffsets, explain, err := db.rawResultToJson(result.GetBuf(), namespace, "total", nil, nil)
	if err != nil {
		return errJSONIterator(err)
	}

	return newJSONIterator(ctx, nil, json, jsonOffsets, explain)
}

func getQueryNamespace(query string) string {
	// TODO: do not parse query string twice in go and cpp
	namespace := ""
	querySlice := strings.Fields(strings.ToLower(query))

	for i := range querySlice {
		if querySlice[i] == "from" && i+1 < len(querySlice) {
			namespace = querySlice[i+1]
			break
		}
	}

	return namespace
}

// beginTx - start update transaction
func (db *reindexerImpl) beginTx(ctx context.Context, namespace string) (*Tx, error) {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.Tx.Begin", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("Tx.Begin", namespace)).ObserveDuration()
	}

	return newTx(db, namespace, ctx)
}

// mustBeginTx - start update transaction, panic on error
func (db *reindexerImpl) mustBeginTx(ctx context.Context, namespace string) *Tx {
	tx, err := db.beginTx(ctx, namespace)
	if err != nil {
		panic(err)
	}

	return tx
}

func (db *reindexerImpl) addFilterDSL(filter *dsl.Filter, q *Query) error {
	if filter.Field == "" {
		return ErrEmptyFieldName
	}
	cond, err := GetCondType(filter.Cond)
	if err != nil {
		return err
	}
	if filter.Value != nil {
		// Skip filter if value is nil (for backwards compatibility reasons)
		q.Where(filter.Field, cond, filter.Value)
	} else {
		q.And()
	}

	return nil
}

func (db *reindexerImpl) addJoinedDSL(joined *dsl.JoinQuery, resultField string, q *Query) error {
	if joined.Namespace == "" {
		return ErrEmptyNamespace
	}

	jq := db.query(joined.Namespace).Offset(joined.Offset)
	if joined.Limit > 0 {
		jq.Limit(joined.Limit)
	}
	if joined.Sort.Field != "" {
		jq.Sort(joined.Sort.Field, joined.Sort.Desc, joined.Sort.Values...)
	}

	_, err := db.handleFiltersDSL(joined.Filters, nil, jq)
	if err != nil {
		return err
	}

	switch strings.ToLower(joined.Type) {
	case "left":
		q.LeftJoin(jq, resultField)
	case "inner":
		q.InnerJoin(jq, resultField)
	case "orinner":
		q.Or().InnerJoin(jq, resultField)
	default:
		return bindings.NewError("rq: join type is invalid", ErrCodeParams)
	}

	for _, on := range joined.On {
		cond, err := GetCondType(on.Cond)
		if err != nil {
			return err
		}
		if on.LeftField == "" {
			return bindings.NewError("rq: dsl join on empty field (left)", ErrCodeParams)
		}
		if on.RightField == "" {
			return bindings.NewError("rq: dsl join on empty field (right)", ErrCodeParams)
		}
		switch strings.ToLower(on.Op) {
		case "":
			jq.On(on.LeftField, cond, on.RightField)
		case "and":
			jq.On(on.LeftField, cond, on.RightField)
		case "or":
			jq.Or().On(on.LeftField, cond, on.RightField)
		default:
			return bindings.NewError("rq: dsl join_query op is invalid", ErrCodeParams)
		}
	}
	q.JoinHandler(resultField, func(field string, item interface{}, subitems []interface{}) bool {
		return false // Do not handle joined data
	})

	return nil
}

func (db *reindexerImpl) handleFiltersDSL(filters []dsl.Filter, joinIDs *map[string]int, q *Query) (*Query, error) {
	for fi, _ := range filters {
		filter := &filters[fi]
		switch strings.ToLower(filters[fi].Op) {
		case "":
			q.And()
		case "and":
			q.And()
		case "or":
			q.Or()
		case "not":
			q.Not()
		default:
			return nil, bindings.NewError("rq: dsl filter op is invalid", ErrCodeParams)
		}

		if filter.Joined != nil {
			if joinIDs == nil {
				return nil, bindings.NewError("rq: nested join quieries are not supported", ErrCodeParams)
			}
			if filter.Field != "" || filter.Cond != "" {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'field' and 'join_query' at the same time", ErrCodeParams)
			}
			if len(filter.Filters) != 0 {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'fielters' and 'join_query' at the same time", ErrCodeParams)
			}

			var joinedFieldName string
			if v, found := (*joinIDs)[filter.Joined.Namespace]; found {
				joinedFieldName = fmt.Sprintf("_dsl_joined_%s_%d", filter.Joined.Namespace, v)
				(*joinIDs)[filter.Joined.Namespace] += 1
			} else {
				joinedFieldName = fmt.Sprintf("_dsl_joined_%s", filter.Joined.Namespace)
				(*joinIDs)[filter.Joined.Namespace] = 0
			}
			err := db.addJoinedDSL(filter.Joined, joinedFieldName, q)
			if err != nil {
				return nil, err
			}
			continue
		}

		if len(filter.Filters) != 0 {
			if len(filter.Field) != 0 || len(filter.Cond) != 0 {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'field' and 'filters' at the same time", ErrCodeParams)
			}
			q.OpenBracket()
			_, err := db.handleFiltersDSL(filter.Filters, joinIDs, q)
			if err != nil {
				return nil, err
			}
			q.CloseBracket()
			continue
		}
		err := db.addFilterDSL(filter, q)
		if err != nil {
			return nil, err
		}
	}
	return q, nil
}

func (db *reindexerImpl) queryFrom(d *dsl.DSL) (*Query, error) {
	if d.Namespace == "" {
		return nil, ErrEmptyNamespace
	}

	q := db.query(d.Namespace).Offset(d.Offset)
	if d.Explain {
		q.Explain()
	}
	if d.Limit > 0 {
		q.Limit(d.Limit)
	}
	if d.Distinct != "" {
		q.Distinct(d.Distinct)
	}

	for _, agg := range d.Aggregations {
		if len(agg.Fields) == 0 {
			return nil, ErrEmptyAggFieldName
		}
		switch agg.AggType {
		case AggSum:
			q.AggregateSum(agg.Fields[0])
		case AggAvg:
			q.AggregateAvg(agg.Fields[0])
		case AggFacet:
			aggReq := q.AggregateFacet(agg.Fields...).Limit(agg.Limit).Offset(agg.Offset)
			for _, sort := range agg.Sort {
				aggReq.Sort(sort.Field, sort.Desc)
			}
		case AggMin:
			q.AggregateMin(agg.Fields[0])
		case AggMax:
			q.AggregateMax(agg.Fields[0])
		case AggDistinct:
			q.Distinct(agg.Fields[0])
		default:
			return nil, ErrAggInvalid
		}
	}

	if d.Sort.Field != "" {
		q.Sort(d.Sort.Field, d.Sort.Desc, d.Sort.Values...)
	}
	if d.ReqTotal {
		q.ReqTotal()
	}
	if d.WithRank {
		q.WithRank()
	}

	joinIDs := make(map[string]int)
	return db.handleFiltersDSL(d.Filters, &joinIDs, q)
}

func dsnParse(dsn interface{}) (string, []url.URL) {
	var dsnSlice []string
	var scheme string

	switch v := dsn.(type) {
	case string:
		dsnSlice = []string{v}
	case []string:
		if len(v) == 0 {
			panic(fmt.Errorf("Empty multi DSN config. DSN: '%#v'. ", dsn))
		}
		dsnSlice = v
	default:
		panic(fmt.Errorf("DSN format not supported. Support []string or string. DSN: '%#v'. ", dsn))
	}

	dsnParsed := make([]url.URL, 0, len(dsnSlice))
	for i := range dsnSlice {
		if dsnSlice[i] == "builtin" {
			dsnSlice[i] += "://"
		}

		u, err := url.Parse(dsnSlice[i])
		if err != nil {
			panic(fmt.Errorf("Can't parse DB DSN '%s'", dsn))
		}
		if scheme != "" && scheme != u.Scheme {
			panic(fmt.Sprintf("DSN has a different schemas. %s", dsn))
		}
		dsnParsed = append(dsnParsed, *u)
		scheme = u.Scheme
	}

	return scheme, dsnParsed
}

func dsnString(dsnParsed []url.URL) string {
	dsnLabelParts := []string{}
	for _, dsn := range dsnParsed {
		dsnLabelParts = append(dsnLabelParts, strings.TrimSpace(dsn.String()))
	}

	return strings.Join(dsnLabelParts, ",")
}

// GetStats Get local thread reindexer usage stats
// Deprecated: Use SELECT * FROM '#perfstats' to get performance statistics.
func (db *reindexerImpl) getStats() bindings.Stats {
	log.Println("Deprecated function reindexer.GetStats call. Use SELECT * FROM '#perfstats' to get performance statistics")
	return bindings.Stats{}
}

// ResetStats Reset local thread reindexer usage stats
// Deprecated: no longer used.
func (db *reindexerImpl) resetStats() {
}

// enableStorage enables persistent storage of data
// Deprecated: storage path should be passed as DSN part to reindexer.NewReindex (""), e.g. reindexer.NewReindexer ("builtin:///tmp/reindex").
func (db *reindexerImpl) enableStorage(ctx context.Context, storagePath string) error {
	log.Println("Deprecated function reindexer.EnableStorage call")
	return db.binding.EnableStorage(ctx, storagePath)
}
