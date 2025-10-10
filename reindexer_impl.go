package reindexer

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	otel "go.opentelemetry.io/otel"
	otelattr "go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
	"github.com/restream/reindexer/v5/dsl"
	"github.com/restream/reindexer/v5/events"
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
}

func (ns *reindexerNamespace) renamedCopy(newName string) *reindexerNamespace {
	nsCopy := &reindexerNamespace{
		cacheItems:    &cacheItems{},
		joined:        ns.joined,
		indexes:       ns.indexes,
		schema:        ns.schema,
		rtype:         ns.rtype,
		deepCopyIface: ns.deepCopyIface,
		name:          newName,
		opts:          ns.opts,
		cjsonState:    cjson.NewState(),
	}

	if !nsCopy.opts.disableObjCache {
		var err error
		nsCopy.cacheItems, err = newCacheItems(nsCopy.opts.objCacheItemsCount)
		if err != nil {
			nsCopy.cacheItems = &cacheItems{}
			log.Printf("Unable to copy namespace's cache during rename ('%s'->'%s'). Cache was disabled for '%s'\n", ns.name, newName, newName)
		}
	}
	return nsCopy
}

// reindexerImpl The reindxer state struct
type reindexerImpl struct {
	// Fast RW lock to access namespaces list
	lock sync.RWMutex
	// Slow separate mutex to provide synchronization between separate namespaces' list modification
	nsModifySlowLock   sync.Mutex
	ns                 map[string]*reindexerNamespace
	storagePath        string
	binding            bindings.RawBinding
	debugLevels        map[string]int
	strictJoinHandlers bool
	status             error

	promMetrics *reindexerPrometheusMetrics

	otelTracer           oteltrace.Tracer
	otelCommonTraceAttrs []otelattr.KeyValue

	events *events.EventsHandler
}

type cacheItems struct {
	// cached items
	items *lru.Cache
}

// Motivation:
// Key:
// - id + shardID - unique logic identifier of the document in the sharded cluster;
// - nsTag - required to handled FORCE-syncs in the clusters. Int64 representation of the LsnT. Contains server ID and timestamp part. Using it as the part of the key to avoid
// recaching during force-sync (those syncs could apear under the heavy load). Timestamp is required to hadn;e nodes' restarts in the sharded cluster (currenty items' IDs are not consistant between restarts)
// Item:
// - itemVersion - LSN's counter (does not contain server ID). Represents incremental document's version. Outdated documents will be removed from cache
// and will not be cached multiple times.
// - shardingVersion - source ID of the sharding config. It is not incremental, so it can not work as version. Multiple recaching of the same item is possible,
// but expecting, that sharding config version should be changed very rarelly and probably even in some kind of the maintenance mode.

type cacheKey struct {
	id      int
	shardID int
	nsTag   int64
}

type cacheItem struct {
	// cached data
	item interface{}
	// version of the item
	itemVersion int64
	// version of the sharding config
	shardingVersion int64
}

func (ci *cacheItems) Reset() {
	if ci.items == nil {
		return
	}
	ci.items.Purge()
}

func (ci *cacheItems) Remove(key cacheKey) {
	if ci.items == nil {
		return
	}
	ci.items.Remove(key)
}

func (ci *cacheItems) Add(key cacheKey, item *cacheItem) {
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

func (ci *cacheItems) Get(key cacheKey) (*cacheItem, bool) {
	if ci.items == nil {
		return nil, false
	}

	item, ok := ci.items.Get(key)
	if ok {
		return item.(*cacheItem), ok
	}
	return nil, false
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
		events:  events.NewEventsHandler(binding),
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
					"reindexer/v5",
					oteltrace.WithInstrumentationVersion(bindings.ReindexerVersion),
				)
				rx.otelCommonTraceAttrs = []otelattr.KeyValue{
					otelattr.String("rx.dsn", dsnString(dsnParsed)),
				}
			}
		case bindings.OptionStrictJoinHandlers:
			rx.strictJoinHandlers = v.EnableStrictJoinHandlers
		}
	}

	if err := binding.Init(dsnParsed, rx.events, options...); err != nil {
		rx.status = err
	}

	if changing, ok := binding.(bindings.RawBindingChanging); ok {
		changing.OnChangeCallback(rx.resetCaches)
	}

	if replicationStat, ok := binding.(bindings.GetReplicationStat); ok {
		replicationStat.GetReplicationStat(rx.getReplicationStat)
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
	rx.registerNamespaceImpl(ReplicationStatsNamespaceName, opts, ReplicationStat{})
	return rx
}

func (db *reindexerImpl) getReplicationStat(ctx context.Context) (*bindings.ReplicationStat, error) {
	var stat *bindings.ReplicationStat

	stat, err := db.getClusterStat(ctx)
	if err != nil {
		return nil, err
	}

	if stat == nil {
		stat, err = db.getAsyncReplicationStat(ctx)
		if err != nil {
			return nil, err
		}
	}

	if stat == nil {
		return nil, bindings.NewError("can't use WithReconnectionStrategy without configured cluster or async replication", bindings.ErrParams)
	}

	return stat, nil
}

func (db *reindexerImpl) getAsyncReplicationStat(ctx context.Context) (*bindings.ReplicationStat, error) {
	dsns := db.binding.GetDSNs()
	var stat *bindings.ReplicationStat

	for _, dsn := range dsns {
		dsn := fmt.Sprintf("%s://%s%s", dsn.Scheme, dsn.Host, dsn.Path)
		db, err := NewReindex(dsn)
		if err != nil {
			continue
		}
		defer db.Close()
		resp, err := db.Query("#config").
			WhereString("type", EQ, bindings.ReplicationTypeAsync).
			ExecCtx(ctx).
			FetchOne()
		if err != nil {
			continue
		}
		statRx := resp.(*DBConfigItem).AsyncReplication
		if statRx.Role == "leader" {
			stat = &bindings.ReplicationStat{
				Type:  bindings.ReplicationTypeAsync,
				Nodes: make([]bindings.ReplicationNodeStat, 0, len(statRx.Nodes)),
			}
			stat.Nodes = append(stat.Nodes, bindings.ReplicationNodeStat{
				DSN:  dsn,
				Role: statRx.Role,
			})

			break
		}
	}

	return stat, nil
}

func (db *reindexerImpl) getClusterStat(ctx context.Context) (*bindings.ReplicationStat, error) {
	resp, err := db.query("#replicationstats").
		WhereString("type", EQ, "cluster").
		ExecCtx(ctx).
		FetchOne()
	if err != nil {
		return nil, err
	}

	statRx := resp.(*ReplicationStat)
	if len(statRx.ReplicationNodeStat) == 0 {
		return nil, nil
	}

	stat := &bindings.ReplicationStat{
		Type:  statRx.Type,
		Nodes: make([]bindings.ReplicationNodeStat, 0, len(statRx.ReplicationNodeStat)),
	}

	for _, node := range statRx.ReplicationNodeStat {
		stat.Nodes = append(stat.Nodes, bindings.ReplicationNodeStat{
			DSN:            node.DSN,
			Status:         node.Status,
			IsSynchronized: node.IsSynchronized,
			Role:           node.Role,
		})
	}

	return stat, nil
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
	if log != nil && (reflect.ValueOf(log).Kind() != reflect.Ptr || !reflect.ValueOf(log).IsNil()) {
		db.binding.EnableLogger(log)
	} else {
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

	db.nsModifySlowLock.Lock()
	defer db.nsModifySlowLock.Unlock()

	if err = db.registerNamespaceImpl(namespace, opts, s); err != nil {
		return err
	}

	ns, err := db.getNS(namespace)
	if err != nil {
		return err
	}

	awaitReplication := false
	const defaultMaxTries = 2
	maxTries := defaultMaxTries
	for retry := 0; retry < maxTries; retry++ {
		if awaitReplication {
			// Try to await concurrent indexes replication
			log.Printf("Awaiting concurrent replication for '%s'\n", namespace) // TODO: Remove this log, once race in replicated ns will be fixed
			err = nil
			awaitReplication = false
			time.Sleep(time.Millisecond * 300)
		} else {
			maxTries = defaultMaxTries
		}

		if err = db.binding.OpenNamespace(ctx, namespace, opts.enableStorage, opts.dropOnFileFormatError); err != nil {
			break
		}

		for _, indexDef := range ns.indexes {
			if err = db.binding.AddIndex(ctx, namespace, indexDef); err != nil {
				if rerr, ok := err.(bindings.Error); ok && rerr.Code() == bindings.ErrWrongReplicationData {
					awaitReplication = true
					maxTries = 15
					log.Printf("Replication error in '%s' during '%s' index creation\n", namespace, indexDef.Name) // TODO: Remove this log, once race in replicated ns will be fixed
				}
				break
			}
		}
		if awaitReplication {
			continue
		}

		if err == nil {
			if err = db.binding.SetSchema(ctx, namespace, ns.schema); err != nil {
				rerr, ok := err.(bindings.Error)
				if ok && rerr.Code() == bindings.ErrParams {
					// Ignore error from old server which doesn't support SetSchema
					err = nil
				} else if ok && rerr.Code() == bindings.ErrWrongReplicationData {
					awaitReplication = true
					maxTries = 15
					log.Printf("Replication error in '%s' during schema setting\n", namespace) // TODO: Remove this log, once race in replicated ns will be fixed
					continue
				}
				break
			}
		}

		if err != nil {
			rerr, ok := err.(bindings.Error)
			if ok && rerr.Code() == bindings.ErrConflict && opts.dropOnIndexesConflict {
				db.binding.DropNamespace(ctx, namespace)
				continue
			}
			db.unregisterNamespaceImpl(namespace)
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

	db.nsModifySlowLock.Lock()
	defer db.nsModifySlowLock.Unlock()

	return db.registerNamespaceImpl(namespace, opts, s)
}

func (db *reindexerImpl) unregisterNamespaceImpl(namespace string) {
	db.lock.Lock()
	defer db.lock.Unlock()
	delete(db.ns, namespace)
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
	}

	validator := cjson.Validator{}

	if err = validator.Validate(s); err != nil {
		return err
	}
	if ns.indexes, err = parseIndexes(ns.rtype, &ns.joined); err != nil {
		return err
	}
	if schema := parseSchema(ns.rtype); schema != nil {
		ns.schema = *schema
	}

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

	db.nsModifySlowLock.Lock()
	defer db.nsModifySlowLock.Unlock()

	db.unregisterNamespaceImpl(namespace)
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

	db.nsModifySlowLock.Lock()
	defer db.nsModifySlowLock.Unlock()

	if err := db.binding.RenameNamespace(ctx, srcNsName, dstNsName); err != nil {
		return err
	}

	db.lock.Lock()
	defer db.lock.Unlock()
	if srcNs, ok := db.ns[srcNsName]; ok {
		delete(db.ns, srcNsName)
		// create copy via `renamedCopy` to avoid race condition on the non-const namespace's properties
		db.ns[dstNsName] = srcNs.renamedCopy(dstNsName)
	}
	return nil
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

	db.nsModifySlowLock.Lock()
	defer db.nsModifySlowLock.Unlock()

	db.unregisterNamespaceImpl(namespace)
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

	_, err := db.modifyItem(ctx, namespace, nil, item, modeUpsert, precepts...)
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

	return db.modifyItem(ctx, namespace, nil, item, modeInsert, precepts...)
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

	return db.modifyItem(ctx, namespace, nil, item, modeUpdate, precepts...)
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

	_, err := db.modifyItem(ctx, namespace, nil, item, modeDelete, precepts...)
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

func (db *reindexerImpl) enumMeta(ctx context.Context, namespace string) ([]string, error) {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.EnumMeta", otelattr.String("rx.ns", namespace)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("EnumMeta", namespace)).ObserveDuration()
	}

	return db.binding.EnumMeta(ctx, namespace)
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

func (db *reindexerImpl) deleteMeta(ctx context.Context, namespace, key string) error {
	namespace = strings.ToLower(namespace)

	if db.otelTracer != nil {
		defer db.startTracingSpan(ctx, "Reindexer.DeleteMeta", otelattr.String("rx.ns", namespace), otelattr.String("rx.meta.key", key)).End()
	}

	if db.promMetrics != nil {
		defer prometheus.NewTimer(db.promMetrics.clientCallsLatency.WithLabelValues("DeleteMeta", namespace)).ObserveDuration()
	}

	return db.binding.DeleteMeta(ctx, namespace, key)
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
	item, err := db.query(ConfigNamespaceName).WhereString("type", EQ, "namespaces").ExecCtx(ctx).FetchOne()
	if err != nil && err != ErrNotFound {
		return err
	}
	citem := &DBConfigItem{Type: "namespaces"}
	if err == nil {
		citem = item.(*DBConfigItem)
	}

	defaultCfg := DefaultDBNamespaceConfig("*")
	found := false
	if citem.Namespaces == nil {
		namespaces := make([]DBNamespacesConfig, 0, 1)
		citem.Namespaces = &namespaces
	}

nss_loop:
	for i := range *citem.Namespaces {
		switch (*citem.Namespaces)[i].Namespace {
		case namespace:
			(*citem.Namespaces)[i].LogLevel = loglevelToString(level)
			found = true
			break nss_loop
		case "*":
			tmp := (*citem.Namespaces)[i]
			defaultCfg = &tmp
		}
	}
	if !found {
		nsCfg := defaultCfg
		nsCfg.Namespace = namespace
		nsCfg.LogLevel = loglevelToString(level)
		*citem.Namespaces = append(*citem.Namespaces, *nsCfg)
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

	json, jsonOffsets, explain, err := db.rawResultToJson(result.GetBuf(), namespace, "total", nil, nil, namespace)
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

func (db *reindexerImpl) addFilterDSL(filter *dsl.Filter, q *Query, fields *map[string]bool) error {
	if filter.Field == "" {
		return ErrEmptyFieldName
	}
	cond, err := GetCondType(filter.Cond)
	if err != nil {
		return err
	}
	if filter.Value != nil || cond == bindings.ANY || cond == bindings.EMPTY {
		// Skip filter if value is nil (for backwards compatibility reasons)
		(*fields)[filter.Field] = q.nextOp == opAND
		q.Where(filter.Field, cond, filter.Value)
	} else {
		q.And()
	}

	return nil
}

func (db *reindexerImpl) addJoinedDSL(joined *dsl.JoinQuery, resultField string, q *Query) error {
	if joined.Namespace == "" {
		return bindings.NewError("rq: empty namespace name in joined query", ErrCodeParams)
	}

	jq := db.query(joined.Namespace).Offset(joined.Offset)
	if joined.Limit > 0 {
		jq.Limit(joined.Limit)
	}
	if joined.Sort.Field != "" {
		jq.Sort(joined.Sort.Field, joined.Sort.Desc, joined.Sort.Values...)
	}
	if _, err := db.handleFiltersDSL(joined.Filters, nil, jq); err != nil {
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

func (db *reindexerImpl) addSubqueryDSL(filter *dsl.Filter, q *Query) error {
	if filter.SubQ.Namespace == "" {
		return bindings.NewError("rq: empty namespace name in subquery", ErrCodeParams)
	}
	if filter.Field != "" && filter.Value != nil {
		return bindings.NewError("rq: filter: both field name and value in filter with subquery are not empty (expecting exactly one of them)", ErrCodeParams)
	}
	cond, err := GetCondType(filter.Cond)
	if err != nil {
		return err
	}
	if len(filter.SubQ.SelectFilter) > 1 {
		return bindings.NewError(fmt.Sprintf("rq: subquery can not have multiple select_filter's: %v", filter.SubQ.SelectFilter), ErrCodeParams)
	}
	if cond == EMPTY || cond == ANY {
		if filter.SubQ.Limit > 0 {
			return bindings.NewError("rq: filter: subquery with condition Any or Empty can not contain limit", ErrCodeParams)
		}
		if filter.SubQ.Offset > 0 {
			return bindings.NewError("rq: filter: subquery with condition Any or Empty can not contain offset", ErrCodeParams)
		}
		if filter.SubQ.ReqTotal {
			return bindings.NewError("rq: filter: subquery with condition Any or Empty can not contain ReqTotal", ErrCodeParams)
		}
		if len(filter.SubQ.Aggregations) > 0 {
			return bindings.NewError("rq: filter: subquery with condition Any or Empty can not contain aggregations", ErrCodeParams)
		}
		if len(filter.SubQ.SelectFilter) > 0 {
			return bindings.NewError("rq: filter: subquery with condition Any or Empty can not contain select filter", ErrCodeParams)
		}
	} else {
		for _, agg := range filter.SubQ.Aggregations {
			switch strings.ToLower(agg.AggType) {
			case dsl.AggSum, dsl.AggAvg, dsl.AggMin, dsl.AggMax, dsl.AggCount, dsl.AggCountCached:
				// Skip
			default:
				return bindings.NewError(fmt.Sprintf("rq: filter: unsupported aggregation for subquery: '%s'", agg.AggType), ErrCodeParams)
			}
		}
		totalAggs := len(filter.SubQ.Aggregations)
		if filter.SubQ.ReqTotal {
			totalAggs++
		}
		if totalAggs > 1 {
			return bindings.NewError("rq: filter: subquery can not contain more than 1 aggregation / count request", ErrCodeParams)
		}
		if totalAggs == 1 && len(filter.SubQ.SelectFilter) > 0 {
			return bindings.NewError("rq: subquery can not have select_filter and aggregations at the same time", ErrCodeParams)
		}
		if totalAggs == 0 && len(filter.SubQ.SelectFilter) != 1 {
			return bindings.NewError(fmt.Sprintf("rq: subquery with Cond '%s' and without aggregations must have exactly 1 select_filter", filter.Cond), ErrCodeParams)
		}
	}

	sq := db.query(filter.SubQ.Namespace).Offset(filter.SubQ.Offset).Select(filter.SubQ.SelectFilter...)
	if filter.SubQ.Limit > 0 {
		sq.Limit(filter.SubQ.Limit)
	}
	if filter.SubQ.ReqTotal {
		sq.ReqTotal()
	}
	if filter.SubQ.Sort.Field != "" {
		sq.Sort(filter.SubQ.Sort.Field, filter.SubQ.Sort.Desc, filter.SubQ.Sort.Values...)
	}
	if err = db.addAggregationsDSL(sq, filter.SubQ.Aggregations); err != nil {
		return err
	}
	if _, err = db.handleFiltersDSL(filter.SubQ.Filters, nil, sq); err != nil {
		return err
	}

	if filter.Field != "" {
		// Subquery with field
		if cond == EMPTY || cond == ANY {
			return bindings.NewError("rq: filter: subquery with condition Any or Empty can not have Field in the filter", ErrCodeParams)
		}
		q.Where(filter.Field, cond, sq)
	} else {
		if (cond == EMPTY || cond == ANY) && filter.Value != nil {
			return bindings.NewError("rq: filter: subquery with condition Any or Empty can not have non-nil target Value", ErrCodeParams)
		}
		if cond != EMPTY && cond != ANY && filter.Value == nil {
			return bindings.NewError(fmt.Sprintf("rq: filter: subquery with condition %v can not have nil target Value and empty field name", filter.Cond), ErrCodeParams)
		}
		// Subquery with explicit values
		q.WhereQuery(sq, cond, filter.Value)

	}
	return nil
}

func (db *reindexerImpl) handleFiltersDSL(filters []dsl.Filter, joinIDs *map[string]int, q *Query) (*Query, error) {
	equalPositionsFilters := make([]*dsl.Filter, 0, len(filters))
	fields := make(map[string]bool)

	for fi := range filters {
		filter := &filters[fi]
		switch strings.ToLower(filter.Op) {
		case "":
			q.And()
		case "and":
			q.And()
		case "or":
			q.Or()
		case "not":
			q.Not()
		default:
			return nil, bindings.NewError(fmt.Sprintf("rq: dsl filter op is invalid: '%s'", filter.Op), ErrCodeParams)
		}

		if filter.Joined != nil {
			if joinIDs == nil {
				return nil, bindings.NewError("rq: nested join quieries are not supported", ErrCodeParams)
			}
			if filter.Field != "" {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'field' and 'join_query' at the same time", ErrCodeParams)
			}
			if filter.Cond != "" {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'cond' and 'join_query' at the same time", ErrCodeParams)
			}
			if filter.Value != nil {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'value' and 'join_query' at the same time", ErrCodeParams)
			}
			if len(filter.Filters) != 0 {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'filters' and 'join_query' at the same time", ErrCodeParams)
			}
			if filter.SubQ != nil {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'subquery' and 'join_query' at the same time", ErrCodeParams)
			}

			var joinedFieldName string
			if v, found := (*joinIDs)[filter.Joined.Namespace]; found {
				joinedFieldName = fmt.Sprintf("_dsl_joined_%s_%d", filter.Joined.Namespace, v)
				(*joinIDs)[filter.Joined.Namespace] += 1
			} else {
				joinedFieldName = fmt.Sprintf("_dsl_joined_%s", filter.Joined.Namespace)
				(*joinIDs)[filter.Joined.Namespace] = 0
			}
			if err := db.addJoinedDSL(filter.Joined, joinedFieldName, q); err != nil {
				return nil, err
			}
			continue
		} else if len(filter.Filters) != 0 {
			if len(filter.Field) != 0 || len(filter.Cond) != 0 {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'field' and 'filters' at the same time", ErrCodeParams)
			}
			if filter.SubQ != nil {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'subquery' and 'filters' at the same time", ErrCodeParams)
			}
			if filter.Cond != "" {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'cond' and 'filters' at the same time", ErrCodeParams)
			}
			if filter.Value != nil {
				return nil, bindings.NewError("rq: dsl filter can not contain both 'value' and 'filters' at the same time", ErrCodeParams)
			}
			q.OpenBracket()
			if _, err := db.handleFiltersDSL(filter.Filters, joinIDs, q); err != nil {
				return nil, err
			}
			q.CloseBracket()
			continue
		} else if filter.SubQ != nil {
			if err := db.addSubqueryDSL(filter, q); err != nil {
				return nil, err
			}
			continue
		} else if len(filter.EqualPositions) > 0 {
			equalPositionsFilters = append(equalPositionsFilters, filter)
			continue
		}
		if err := db.addFilterDSL(filter, q, &fields); err != nil {
			return nil, err
		}
	}

	if err := db.processEqualPositions(q, equalPositionsFilters, fields); err != nil {
		return nil, err
	}

	return q, nil
}

func (db *reindexerImpl) processEqualPositions(q *Query, filters []*dsl.Filter, conditionFields map[string]bool) error {
	for _, filter := range filters {
		if filter.Cond != "" ||
			filter.Op != "" ||
			filter.Field != "" ||
			filter.Value != nil ||
			filter.Filters != nil ||
			filter.Joined != nil ||
			filter.SubQ != nil {
			return bindings.NewError("rq: filter: filter with 'equal_positions'-field should not contain any other fields besides it", ErrCodeParams)
		}

		for _, EqualPositionsElement := range filter.EqualPositions {
			fieldsCount := len(EqualPositionsElement.Positions)
			if fieldsCount < 2 {
				return bindings.NewError(fmt.Sprintf("rq: filter: 'equal_positions' is supposed to have at least 2 arguments. Arguments: %v",
					EqualPositionsElement.Positions), ErrCodeParams)
			}

			fields := make([]string, 0, fieldsCount)
			for _, field := range EqualPositionsElement.Positions {
				isAndOp, exist := conditionFields[field]
				if !exist {
					return bindings.NewError("rq: filter: fields from 'equal_positions'-filter must be specified in the 'where'-conditions of other filters in the current bracket",
						ErrCodeParams)
				}
				if !isAndOp {
					return bindings.NewError(fmt.Sprintf("rq: filter: only AND operation allowed for equal position; equal position field with not AND operation: '%s'; equal position fields: %v",
						field,
						EqualPositionsElement.Positions),
						ErrCodeParams)
				}
				fields = append(fields, field)
			}

			q.EqualPosition(fields...)
		}
	}
	return nil
}

func (db *reindexerImpl) addAggregationsDSL(q *Query, aggs []dsl.Aggregation) error {
	for _, agg := range aggs {
		if len(agg.Fields) == 0 {
			return ErrEmptyAggFieldName
		}
		switch strings.ToLower(agg.AggType) {
		case dsl.AggSum:
			q.AggregateSum(agg.Fields[0])
		case dsl.AggAvg:
			q.AggregateAvg(agg.Fields[0])
		case dsl.AggFacet:
			aggReq := q.AggregateFacet(agg.Fields...).Limit(agg.Limit).Offset(agg.Offset)
			for _, sort := range agg.Sort {
				aggReq.Sort(sort.Field, sort.Desc)
			}
		case dsl.AggMin:
			q.AggregateMin(agg.Fields[0])
		case dsl.AggMax:
			q.AggregateMax(agg.Fields[0])
		case dsl.AggDistinct:
			q.Distinct(agg.Fields...)
		case dsl.AggCount:
			if len(agg.Fields) == 1 && (agg.Fields[0] == "" || agg.Fields[0] == "*") {
				q.ReqTotal()
			} else {
				q.ReqTotal(agg.Fields...)
			}
		default:
			return bindings.NewError(fmt.Sprintf("rq: unsupported aggregation type: '%s'", agg.AggType), ErrCodeParams)
		}
	}
	return nil
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

	if err := db.addAggregationsDSL(q, d.Aggregations); err != nil {
		return nil, err
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
			maskDsn := func(u *url.URL) string {
				if password, ok := u.User.Password(); ok {
					maskLogin := func(username string) string {
						if len(username) > 4 {
							return username[2:] + "..." + username[len(username)-2:]
						} else {
							return "..."
						}
					}
					u.User = url.UserPassword(maskLogin(u.User.Username()), fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(password))))
				}
				return u.String()
			}
			panic(fmt.Sprintf("DSN has a different schemas. %s", maskDsn(u)))
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

func (db *reindexerImpl) subscribe(ctx context.Context, opts *events.EventsStreamOptions) *events.EventsStream {
	return db.events.CreateStream(ctx, opts)
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

func (db *reindexerImpl) dbmsVersion() (string, error) {
	return db.binding.DBMSVersion()
}
