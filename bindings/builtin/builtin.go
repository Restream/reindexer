package builtin

// #include "core/cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
)

const defCgoLimit = 2000
const defWatchersPoolSize = 4
const defCtxWatchDelay = time.Millisecond * 100

var bufFree = newBufFreeBatcher()

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

// Separate mutexes for logger object itself and for reindexer_enable_logger call:
// logMtx provides safe access to the logger
// logEnableMtx provides atomic logic for (enable + set) and (disable + reset) procedures
// This logger is global to easily export it into CGO (however it may lead to some confusion if there are multiple builtin instances in the app)
var logMtx sync.RWMutex
var logEnableMtx sync.Mutex
var logger Logger
var emptyLogger bindings.NullLogger

var enableDebug bool

var bufPool sync.Pool

type Builtin struct {
	cgoLimiter     chan struct{}
	cgoLimiterStat *cgoLimiterStat
	rx             C.uintptr_t
	ctxWatcher     *CtxWatcher
	eventsHandler  *CGOEventsHandler
}

type RawCBuffer struct {
	cbuf         C.reindexer_resbuffer
	hasFinalizer bool
	trace        []byte
}

func (buf *RawCBuffer) FreeFinalized() {
	buf.hasFinalizer = false
	if buf.cbuf.results_ptr != 0 {
		trace := string(buf.trace)
		if trace == "" {
			trace = "To see iterator allocation trace set REINDEXER_GODEBUG=1 environment variable!"
		}

		CGoLogger(bindings.WARNING, "FreeFinalized called. Iterator.Close() was not called:\n"+trace)
	}
	buf.Free()
}

func (buf *RawCBuffer) Free() {
	bufFree.add(buf)
}

func (buf *RawCBuffer) GetBuf() []byte {
	return buf2go(buf.cbuf)
}

func newRawCBuffer() (ret *RawCBuffer) {
	obj := bufPool.Get()

	if obj != nil {
		ret = obj.(*RawCBuffer)
	} else {
		ret = &RawCBuffer{}
	}
	if enableDebug {
		if ret.trace == nil {
			ret.trace = make([]byte, 0x4000)
		}
		ret.trace = ret.trace[0:runtime.Stack(ret.trace[0:cap(ret.trace)], false)]
	}
	return ret
}

func init() {
	bindings.RegisterBinding("builtin", &Builtin{})
	enableDebug = os.Getenv("REINDEXER_GODEBUG") != ""
}

func str2c(str string) C.reindexer_string {
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&str))
	return C.reindexer_string{p: unsafe.Pointer(hdr.Data), n: C.int(hdr.Len)}
}

func ctxErr(errCode int) error {
	switch errCode {
	case bindings.ErrTimeout:
		return context.DeadlineExceeded
	case bindings.ErrCanceled:
		return context.Canceled
	}
	return nil
}

func err2go(ret C.reindexer_error) error {
	if ret.what != nil {
		defer C.free(unsafe.Pointer(ret.what))

		if err := ctxErr(int(ret.code)); err != nil {
			return err
		}
		return bindings.NewError("rq:"+C.GoString(ret.what), int(ret.code))
	}
	return nil
}

func ret2go(ret C.reindexer_ret) (*RawCBuffer, error) {
	if ret.err_code != 0 {
		defer C.free(unsafe.Pointer(uintptr(ret.out.data)))
		if err := ctxErr(int(ret.err_code)); err != nil {
			return nil, err
		}
		return nil, bindings.NewError("rq:"+C.GoString((*C.char)(unsafe.Pointer(uintptr(ret.out.data)))), int(ret.err_code))
	}

	rbuf := newRawCBuffer()
	rbuf.cbuf = ret.out
	if !rbuf.hasFinalizer {
		runtime.SetFinalizer(rbuf, (*RawCBuffer).FreeFinalized)
		rbuf.hasFinalizer = true
	}
	return rbuf, nil
}

func buf2c(buf []byte) C.reindexer_buffer {
	if len(buf) == 0 {
		return C.reindexer_buffer{data: nil, len: 0}
	}
	return C.reindexer_buffer{
		data: (*C.uint8_t)(unsafe.Pointer(&buf[0])),
		len:  C.int(len(buf)),
	}
}

func buf2go(buf C.reindexer_resbuffer) []byte {
	if buf.data == 0 || buf.len == 0 {
		return nil
	}
	length := int(buf.len)
	return (*[1 << 30]byte)(unsafe.Pointer(uintptr(buf.data)))[:length:length]
}

func bool2cint(v bool) C.int {
	if v {
		return 1
	}
	return 0
}

func (binding *Builtin) Init(u []url.URL, eh bindings.EventsHandler, options ...interface{}) error {
	if binding.rx != 0 {
		return bindings.NewError("already initialized", bindings.ErrConflict)
	}

	ctxWatchDelay := defCtxWatchDelay
	ctxWatchersPoolSize := defWatchersPoolSize
	cgoLimit := defCgoLimit
	var maxUpdatesSize uint
	var rx uintptr
	var builtinAllocatorConfig *bindings.OptionBuiltinAllocatorConfig
	connectOptions := *bindings.DefaultConnectOptions()
	connectOptions.Opts |= bindings.ConnectOptWarnVersion
	for _, option := range options {
		switch v := option.(type) {
		case bindings.OptionPrometheusMetrics:
			// nothing
		case bindings.OptionOpenTelemetry:
			// nothing
		case bindings.OptionBuiltinWithServer:
			// nothing
		case bindings.OptionStrictJoinHandlers:
			// nothing
		case bindings.OptionCgoLimit:
			cgoLimit = v.CgoLimit
		case bindings.OptionReindexerInstance:
			rx = v.Instance
		case bindings.OptionBuiltintCtxWatch:
			if v.WatchDelay > 0 {
				ctxWatchDelay = v.WatchDelay
			}
			if v.WatchersPoolSize > 0 {
				ctxWatchersPoolSize = v.WatchersPoolSize
			}
		case bindings.OptionBuiltinAllocatorConfig:
			builtinAllocatorConfig = &v
		case bindings.ConnectOptions:
			connectOptions = v
		case bindings.OptionBuiltinMaxUpdatesSize:
			maxUpdatesSize = v.MaxUpdatesSizeBytes
		default:
			fmt.Printf("Unknown builtin option: %#v\n", option)
		}
	}

	if rx == 0 {
		subDBName := u[0].Path[strings.LastIndex(u[0].Path, "/")+1:]
		if builtinAllocatorConfig != nil {
			rxConfig := C.reindexer_config{
				allocator_cache_limit:    C.int64_t(builtinAllocatorConfig.AllocatorCacheLimit),
				allocator_max_cache_part: C.float(builtinAllocatorConfig.AllocatorMaxCachePart),
				max_updates_size:         C.uint64_t(maxUpdatesSize),
				sub_db_name:              str2c(subDBName),
			}
			binding.rx = C.init_reindexer_with_config(rxConfig)
		} else {
			binding.rx = C.init_reindexer()
		}

	} else {
		binding.rx = C.uintptr_t(rx)
	}

	if cgoLimit != 0 {
		binding.cgoLimiter = make(chan struct{}, cgoLimit)
		binding.cgoLimiterStat = newCgoLimiterStat(binding)
	}

	binding.ctxWatcher = NewCtxWatcher(ctxWatchersPoolSize, ctxWatchDelay)
	binding.eventsHandler = NewCGOEventsHandler(eh)

	opts := C.ConnectOpts{
		storage: C.uint16_t(connectOptions.Storage),
		options: C.uint16_t(connectOptions.Opts),
	}

	caps := *bindings.DefaultBindingCapabilities().
		WithResultsWithShardIDs(true).
		WithQrIdleTimeouts(true).
		WithIncarnationTags(true).
		WithFloatRank(true)
	ccaps := C.BindingCapabilities{
		caps: C.int64_t(caps.Value),
	}

	return err2go(C.reindexer_connect(binding.rx, str2c(u[0].Host+u[0].Path), opts, str2c(bindings.ReindexerVersion), ccaps))
}

func (binding *Builtin) StartWatchOnCtx(ctx context.Context) (CCtxWrapper, error) {
	if binding.ctxWatcher != nil {
		return binding.ctxWatcher.StartWatchOnCtx(ctx)
	}
	return CCtxWrapper{}, bindings.NewError("rq: builtin binding is not initialized properly", bindings.ErrNotValid)
}

func (binding *Builtin) Clone() bindings.RawBinding {
	return &Builtin{}
}

func (binding *Builtin) Ping(ctx context.Context) error {
	return err2go(C.reindexer_ping(binding.rx))
}

func (binding *Builtin) awaitLimiter(ctx context.Context) (withLimiter bool, err error) {
	if binding.cgoLimiter != nil {
		select {
		case binding.cgoLimiter <- struct{}{}:
			withLimiter = true
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return
}

func (binding *Builtin) ModifyItem(ctx context.Context, namespace string, format int, data []byte, mode int, precepts []string, stateToken int) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ser1 := cjson.NewPoolSerializer()
	defer ser1.Close()
	ser1.PutVString(namespace)
	ser1.PutVarCUInt(format)
	ser1.PutVarCUInt(mode)
	ser1.PutVarCUInt(stateToken)

	ser1.PutVarCUInt(len(precepts))
	for _, precept := range precepts {
		ser1.PutVString(precept)
	}
	packedArgs := ser1.Bytes()

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_modify_item_packed(binding.rx, buf2c(packedArgs), buf2c(data), ctxInfo.cCtx))
}

func (binding *Builtin) ModifyItemTx(txCtx *bindings.TxCtx, format int, data []byte, mode int, precepts []string, stateToken int) error {
	select {
	case <-txCtx.UserCtx.Done():
		return txCtx.UserCtx.Err()
	default:
	}

	ser1 := cjson.NewPoolSerializer()
	defer ser1.Close()
	ser1.PutVarCUInt(format)
	ser1.PutVarCUInt(mode)
	ser1.PutVarCUInt(stateToken)

	ser1.PutVarCUInt(len(precepts))
	for _, precept := range precepts {
		ser1.PutVString(precept)
	}
	packedArgs := ser1.Bytes()

	return err2go(C.reindexer_modify_item_packed_tx(binding.rx, C.uintptr_t(txCtx.Id), buf2c(packedArgs), buf2c(data)))
}

func (binding *Builtin) DeleteQueryTx(txCtx *bindings.TxCtx, rawQuery []byte) error {
	select {
	case <-txCtx.UserCtx.Done():
		return txCtx.UserCtx.Err()
	default:
		return err2go(C.reindexer_delete_query_tx(binding.rx, C.uintptr_t(txCtx.Id), buf2c(rawQuery)))
	}
}
func (binding *Builtin) UpdateQueryTx(txCtx *bindings.TxCtx, rawQuery []byte) error {
	select {
	case <-txCtx.UserCtx.Done():
		return txCtx.UserCtx.Err()
	default:
		return err2go(C.reindexer_update_query_tx(binding.rx, C.uintptr_t(txCtx.Id), buf2c(rawQuery)))
	}
}

// ModifyItemTxAsync is not implemented for builtin binding
func (binding *Builtin) ModifyItemTxAsync(txCtx *bindings.TxCtx, format int, data []byte, mode int, precepts []string, stateToken int, cmpl bindings.RawCompletion) {
	err := binding.ModifyItemTx(txCtx, format, data, mode, precepts, stateToken)
	cmpl(nil, err)
}

func (binding *Builtin) OpenNamespace(ctx context.Context, namespace string, enableStorage, dropOnFormatError bool) error {
	var storageOptions bindings.StorageOptions
	storageOptions.Enabled(enableStorage).DropOnFileFormatError(dropOnFormatError)
	opts := C.StorageOpts{
		options: C.uint16_t(storageOptions),
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_open_namespace(binding.rx, str2c(namespace), opts, ctxInfo.cCtx))
}
func (binding *Builtin) CloseNamespace(ctx context.Context, namespace string) error {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_close_namespace(binding.rx, str2c(namespace), ctxInfo.cCtx))
}

func (binding *Builtin) DropNamespace(ctx context.Context, namespace string) error {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_drop_namespace(binding.rx, str2c(namespace), ctxInfo.cCtx))
}

func (binding *Builtin) TruncateNamespace(ctx context.Context, namespace string) error {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_truncate_namespace(binding.rx, str2c(namespace), ctxInfo.cCtx))
}

func (binding *Builtin) RenameNamespace(ctx context.Context, srcNs string, dstNs string) error {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_rename_namespace(binding.rx, str2c(srcNs), str2c(dstNs), ctxInfo.cCtx))
}

func (binding *Builtin) AddIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	sIndexDef := string(bIndexDef)
	return err2go(C.reindexer_add_index(binding.rx, str2c(namespace), str2c(sIndexDef), ctxInfo.cCtx))
}

func (binding *Builtin) SetSchema(ctx context.Context, namespace string, schema bindings.SchemaDef) error {
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	sSchemaJSON := string(schemaJSON)

	return err2go(C.reindexer_set_schema(binding.rx, str2c(namespace), str2c(sSchemaJSON), ctxInfo.cCtx))
}

func (binding *Builtin) UpdateIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	sIndexDef := string(bIndexDef)
	return err2go(C.reindexer_update_index(binding.rx, str2c(namespace), str2c(sIndexDef), ctxInfo.cCtx))
}

func (binding *Builtin) DropIndex(ctx context.Context, namespace, index string) error {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_drop_index(binding.rx, str2c(namespace), str2c(index), ctxInfo.cCtx))
}

func (binding *Builtin) EnumMeta(ctx context.Context, namespace string) ([]string, error) {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	data, err := ret2go(C.reindexer_enum_meta(binding.rx, str2c(namespace), ctxInfo.cCtx))
	if err != nil {
		return nil, err
	}
	defer data.Free()

	ser := cjson.NewSerializer(data.GetBuf())

	keyCnt := ser.GetVarUInt()
	keys := make([]string, keyCnt)
	for i := 0; i < int(keyCnt); i++ {
		keys[i] = ser.GetVString()
	}
	return keys, nil
}

func (binding *Builtin) PutMeta(ctx context.Context, namespace, key, data string) error {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_put_meta(binding.rx, str2c(namespace), str2c(key), str2c(data), ctxInfo.cCtx))
}

func (binding *Builtin) GetMeta(ctx context.Context, namespace, key string) (bindings.RawBuffer, error) {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_get_meta(binding.rx, str2c(namespace), str2c(key), ctxInfo.cCtx))
}

func (binding *Builtin) DeleteMeta(ctx context.Context, namespace, key string) error {
	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_delete_meta(binding.rx, str2c(namespace), str2c(key), ctxInfo.cCtx))
}

func (binding *Builtin) Select(ctx context.Context, query string, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_select(binding.rx, str2c(query), bool2cint(asJson), (*C.int32_t)(unsafe.Pointer(&ptVersions[0])), C.int(len(ptVersions)), ctxInfo.cCtx))
}
func (binding *Builtin) BeginTx(ctx context.Context, namespace string) (txCtx bindings.TxCtx, err error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return txCtx, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}
	ret := C.reindexer_start_transaction(binding.rx, str2c(namespace))
	err = err2go(ret.err)
	if err != nil {
		return
	}
	txCtx.Id = uint64(ret.tx_id)
	return
}

func (binding *Builtin) CommitTx(txCtx *bindings.TxCtx) (bindings.RawBuffer, error) {

	if withLimiter, err := binding.awaitLimiter(txCtx.UserCtx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.StartWatchOnCtx(txCtx.UserCtx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)
	txID := txCtx.Id
	txCtx.Id = 0
	return ret2go(C.reindexer_commit_transaction(binding.rx, C.uintptr_t(txID), ctxInfo.cCtx))
}

func (binding *Builtin) RollbackTx(txCtx *bindings.TxCtx) error {
	txID := txCtx.Id
	txCtx.Id = 0
	return err2go(C.reindexer_rollback_transaction(binding.rx, C.uintptr_t(txID)))
}

func (binding *Builtin) SelectQuery(ctx context.Context, data []byte, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_select_query(binding.rx, buf2c(data), bool2cint(asJson), (*C.int32_t)(unsafe.Pointer(&ptVersions[0])), C.int(len(ptVersions)), ctxInfo.cCtx))
}

func (binding *Builtin) DeleteQuery(ctx context.Context, data []byte) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_delete_query(binding.rx, buf2c(data), ctxInfo.cCtx))
}

func (binding *Builtin) UpdateQuery(ctx context.Context, data []byte) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_update_query(binding.rx, buf2c(data), ctxInfo.cCtx))
}

// CGoLogger logger function for C
//
//export CGoLogger
func CGoLogger(level int, msg string) {
	logMtx.RLock()
	defer logMtx.RUnlock()
	if logger != nil {
		logger.Printf(level, "%s", msg)
	}
}

func (binding *Builtin) setLogger(log bindings.Logger) {
	logMtx.Lock()
	defer logMtx.Unlock()
	logger = log
}

func (binding *Builtin) EnableLogger(log bindings.Logger) {
	logEnableMtx.Lock()
	defer logEnableMtx.Unlock()
	binding.setLogger(log)
	C.reindexer_enable_go_logger()
}

func (binding *Builtin) DisableLogger() {
	logEnableMtx.Lock()
	defer logEnableMtx.Unlock()
	C.reindexer_disable_go_logger()
	binding.setLogger(nil)
}

func (binding *Builtin) GetLogger() bindings.Logger {
	logMtx.RLock()
	defer logMtx.RUnlock()
	if logger != nil {
		return logger
	}
	return &emptyLogger
}

func (binding *Builtin) ReopenLogFiles() error {
	fmt.Println("builtin binding ReopenLogFiles method is dummy")
	return nil
}

func (binding *Builtin) Finalize() error {
	if binding.eventsHandler != nil && binding.rx != 0 {
		binding.eventsHandler.Unsubscribe(binding.rx)
	}
	bufFree.free_buffers_sync()
	C.destroy_reindexer(binding.rx)
	binding.rx = 0
	if binding.cgoLimiterStat != nil {
		binding.cgoLimiterStat.Stop()
	}
	return binding.ctxWatcher.Finalize()
}

func (binding *Builtin) Status(ctx context.Context) (status bindings.Status) {
	status = bindings.Status{
		Builtin: bindings.StatusBuiltin{
			CGOLimit: cap(binding.cgoLimiter),
			CGOUsage: len(binding.cgoLimiter),
		},
	}
	if binding.cgoLimiterStat != nil {
		status.Builtin.CGOUsageLastMinAvg = binding.cgoLimiterStat.LastMinAvg()
	}
	return status
}

func (binding *Builtin) GetDSNs() []url.URL {
	return nil
}

func (binding *Builtin) Subscribe(ctx context.Context, opts *bindings.SubscriptionOptions) error {
	if binding.eventsHandler == nil {
		return errors.New("Builtin events handler is not initialized")
	}
	return binding.eventsHandler.Subscribe(binding.rx, opts)
}

func (binding *Builtin) Unsubscribe(ctx context.Context) error {
	if binding.eventsHandler == nil {
		return errors.New("Builtin events handler is not initialized")
	}
	return binding.eventsHandler.Unsubscribe(binding.rx)
}

func (binding *Builtin) DBMSVersion() (string, error) {
	return C.GoString(C.reindexer_version()), nil
}

func newBufFreeBatcher() (bf *bufFreeBatcher) {
	bf = &bufFreeBatcher{
		bufs:   make([]*RawCBuffer, 0, 100),
		bufs2:  make([]*RawCBuffer, 0, 100),
		kickCh: make(chan struct{}, 1),
	}
	go bf.loop()
	return
}

type bufFreeBatcher struct {
	bufs   []*RawCBuffer
	bufs2  []*RawCBuffer
	cbufs  []C.reindexer_resbuffer
	lock   sync.Mutex
	kickCh chan struct{}

	rxTerminationLock sync.Mutex
}

func (bf *bufFreeBatcher) free_buffers_impl() {
	bf.rxTerminationLock.Lock()
	defer bf.rxTerminationLock.Unlock()
	bf.lock.Lock()
	if len(bf.bufs) == 0 {
		bf.lock.Unlock()
		return
	}
	bf.bufs, bf.bufs2 = bf.bufs2, bf.bufs
	bf.lock.Unlock()

	for _, buf := range bf.bufs2 {
		bf.cbufs = append(bf.cbufs, buf.cbuf)
	}

	C.reindexer_free_buffers(&bf.cbufs[0], C.int(len(bf.cbufs)))

	for _, buf := range bf.bufs2 {
		buf.cbuf.results_ptr = 0
		bf.toPool(buf)
	}
	bf.cbufs = bf.cbufs[:0]
	bf.bufs2 = bf.bufs2[:0]
}

func (bf *bufFreeBatcher) loop() {
	for {
		<-bf.kickCh
		bf.free_buffers_impl()
	}
}

func (bf *bufFreeBatcher) free_buffers_sync() {
	bf.free_buffers_impl()
}

func (bf *bufFreeBatcher) add(buf *RawCBuffer) {
	if buf.cbuf.results_ptr != 0 {
		bf.toFree(buf)
	} else {
		bf.toPool(buf)
	}
}

func (bf *bufFreeBatcher) toFree(buf *RawCBuffer) {
	bf.lock.Lock()
	bf.bufs = append(bf.bufs, buf)
	bf.lock.Unlock()
	select {
	case bf.kickCh <- struct{}{}:
	default:
	}
}

func (bf *bufFreeBatcher) toPool(buf *RawCBuffer) {
	bufPool.Put(buf)
}
