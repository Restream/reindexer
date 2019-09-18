package builtin

// #include "core/cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
)

const defCgoLimit = 2000
const defWatchersPoolSize = 4
const defCtxWatchDelay = time.Millisecond * 100

var bufFree = newBufFreeBatcher()

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

var logger Logger
var logMtx sync.Mutex

var bufPool sync.Pool

type Builtin struct {
	cgoLimiter chan struct{}
	rx         C.uintptr_t
	ctxWatcher *CtxWatcher
}

type RawCBuffer struct {
	cbuf         C.reindexer_resbuffer
	hasFinalizer bool
}

func (buf *RawCBuffer) FreeFinalized() {
	buf.hasFinalizer = false
	if buf.cbuf.results_ptr != 0 {
		CGoLogger(bindings.WARNING, "FreeFinalized called. Iterator.Close() was not called")
	}
	buf.Free()
}

func (buf *RawCBuffer) Free() {
	bufFree.add(buf)
}

func (buf *RawCBuffer) GetBuf() []byte {
	return buf2go(buf.cbuf)
}

func newRawCBuffer() *RawCBuffer {
	obj := bufPool.Get()
	if obj != nil {
		return obj.(*RawCBuffer)
	}
	return &RawCBuffer{}
}

func init() {
	bindings.RegisterBinding("builtin", &Builtin{})
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

func (binding *Builtin) Init(u *url.URL, options ...interface{}) error {
	if binding.rx != 0 {
		return bindings.NewError("already initialized", bindings.ErrConflict)
	}

	ctxWatchDelay := defCtxWatchDelay
	ctxWatchersPoolSize := defWatchersPoolSize
	cgoLimit := defCgoLimit
	var rx uintptr
	connectOptions := *bindings.DefaultConnectOptions()
	for _, option := range options {
		switch v := option.(type) {
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
		case bindings.ConnectOptions:
			connectOptions = v
		default:
			fmt.Printf("Unknown builtin option: %v\n", option)
		}
	}

	if rx == 0 {
		binding.rx = C.init_reindexer()
	} else {
		binding.rx = C.uintptr_t(rx)
	}

	if cgoLimit != 0 {
		binding.cgoLimiter = make(chan struct{}, cgoLimit)
	}

	binding.ctxWatcher = NewCtxWatcher(ctxWatchersPoolSize, ctxWatchDelay)

	opts := C.ConnectOpts{
		storage: C.uint16_t(connectOptions.Storage),
		options: C.uint16_t(connectOptions.Opts),
	}

	return err2go(C.reindexer_connect(binding.rx, str2c(u.Path), opts))
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
		return
	}
	return
}

func (binding *Builtin) ModifyItem(ctx context.Context, nsHash int, namespace string, format int, data []byte, mode int, precepts []string, stateToken int) (bindings.RawBuffer, error) {
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

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_modify_item_packed(binding.rx, buf2c(packedArgs), buf2c(data), ctxInfo.cCtx))
}

func (binding *Builtin) ModifyItemTx(txCtx *bindings.TxCtx, format int, data []byte, mode int, precepts []string, stateToken int) error {
	if withLimiter, err := binding.awaitLimiter(txCtx.UserCtx); err != nil {
		return err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
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

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_open_namespace(binding.rx, str2c(namespace), opts, ctxInfo.cCtx))
}
func (binding *Builtin) CloseNamespace(ctx context.Context, namespace string) error {
	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_close_namespace(binding.rx, str2c(namespace), ctxInfo.cCtx))
}

func (binding *Builtin) DropNamespace(ctx context.Context, namespace string) error {
	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_drop_namespace(binding.rx, str2c(namespace), ctxInfo.cCtx))
}

func (binding *Builtin) TruncateNamespace(ctx context.Context, namespace string) error {
	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_truncate_namespace(binding.rx, str2c(namespace), ctxInfo.cCtx))
}

func (binding *Builtin) EnableStorage(ctx context.Context, path string) error {
	l := len(path)
	if l > 0 && path[l-1] != '/' {
		path += "/"
	}

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_enable_storage(binding.rx, str2c(path), ctxInfo.cCtx))
}

func (binding *Builtin) AddIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	sIndexDef := string(bIndexDef)
	return err2go(C.reindexer_add_index(binding.rx, str2c(namespace), str2c(sIndexDef), ctxInfo.cCtx))
}

func (binding *Builtin) UpdateIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	sIndexDef := string(bIndexDef)
	return err2go(C.reindexer_update_index(binding.rx, str2c(namespace), str2c(sIndexDef), ctxInfo.cCtx))
}

func (binding *Builtin) DropIndex(ctx context.Context, namespace, index string) error {
	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_drop_index(binding.rx, str2c(namespace), str2c(index), ctxInfo.cCtx))
}

func (binding *Builtin) PutMeta(ctx context.Context, namespace, key, data string) error {
	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return err2go(C.reindexer_put_meta(binding.rx, str2c(namespace), str2c(key), str2c(data), ctxInfo.cCtx))
}

func (binding *Builtin) GetMeta(ctx context.Context, namespace, key string) (bindings.RawBuffer, error) {
	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_get_meta(binding.rx, str2c(namespace), str2c(key), ctxInfo.cCtx))
}

func (binding *Builtin) Select(ctx context.Context, query string, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_select(binding.rx, str2c(query), bool2cint(asJson), (*C.int32_t)(unsafe.Pointer(&ptVersions[0])), C.int(len(ptVersions)), ctxInfo.cCtx))
}
func (binding *Builtin) BeginTx(ctx context.Context, namespace string) (txCtx bindings.TxCtx, err error) {
	ret := C.reindexer_start_transaction(binding.rx, str2c(namespace))
	err = err2go(ret.err)
	if err != nil {
		return
	}
	txCtx.Id = uint64(ret.tx_id)
	return
}

func (binding *Builtin) CommitTx(txCtx *bindings.TxCtx) (bindings.RawBuffer, error) {
	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(txCtx.UserCtx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_commit_transaction(binding.rx, C.uintptr_t(txCtx.Id), ctxInfo.cCtx))
}

func (binding *Builtin) RollbackTx(txCtx *bindings.TxCtx) error {
	return err2go(C.reindexer_rollback_transaction(binding.rx, C.uintptr_t(txCtx.Id)))
}

func (binding *Builtin) SelectQuery(ctx context.Context, data []byte, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_select_query(binding.rx, buf2c(data), bool2cint(asJson), (*C.int32_t)(unsafe.Pointer(&ptVersions[0])), C.int(len(ptVersions)), ctxInfo.cCtx))
}

func (binding *Builtin) DeleteQuery(ctx context.Context, nsHash int, data []byte) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_delete_query(binding.rx, buf2c(data), ctxInfo.cCtx))
}

func (binding *Builtin) UpdateQuery(ctx context.Context, nsHash int, data []byte) (bindings.RawBuffer, error) {
	if withLimiter, err := binding.awaitLimiter(ctx); err != nil {
		return nil, err
	} else if withLimiter {
		defer func() { <-binding.cgoLimiter }()
	}

	ctxInfo, err := binding.ctxWatcher.StartWatchOnCtx(ctx)
	if err != nil {
		return nil, err
	}
	defer binding.ctxWatcher.StopWatchOnCtx(ctxInfo)

	return ret2go(C.reindexer_update_query(binding.rx, buf2c(data), ctxInfo.cCtx))
}

func (binding *Builtin) Commit(ctx context.Context, namespace string) error {
	return err2go(C.reindexer_commit(binding.rx, str2c(namespace)))
}

// CGoLogger logger function for C
//export CGoLogger
func CGoLogger(level int, msg string) {
	logMtx.Lock()
	defer logMtx.Unlock()
	if logger != nil {
		logger.Printf(level, "%s", msg)
	}
}

func (binding *Builtin) EnableLogger(log bindings.Logger) {
	logMtx.Lock()
	defer logMtx.Unlock()
	logger = log
	C.reindexer_enable_go_logger()
}

func (binding *Builtin) DisableLogger() {
	logMtx.Lock()
	defer logMtx.Unlock()
	C.reindexer_disable_go_logger()
	logger = nil
}

func (binding *Builtin) Finalize() error {
	C.destroy_reindexer(binding.rx)
	binding.rx = 0
	return binding.ctxWatcher.Finalize()
}

func (binding *Builtin) Status() (status bindings.Status) {
	return bindings.Status{
		Builtin: bindings.StatusBuiltin{
			CGOLimit: cap(binding.cgoLimiter),
			CGOUsage: len(binding.cgoLimiter),
		},
	}
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
}

func (bf *bufFreeBatcher) loop() {
	for {
		<-bf.kickCh

		bf.lock.Lock()
		if len(bf.bufs) == 0 {
			bf.lock.Unlock()
			continue
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
