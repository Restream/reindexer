//go:generate sh -c "cd ../.. && mkdir -p build && cd build && cmake .. && make reindexer -j8"

package builtin

// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra -I../../cpp_src
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../../cpp_src
// #cgo LDFLAGS: -lreindexer -lleveldb -lsnappy -L${SRCDIR}/../../build/cpp_src/ -lstdc++ -g
// #include "core/cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"net/url"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/restream/reindexer/bindings"
)

const cgoLimit = 2000

var cgoLimiter = make(chan struct{}, cgoLimit)

var bufFree = newBufFreeBatcher()

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

var logger Logger

var bufPool sync.Pool

type Builtin struct {
}

type RawCBuffer struct {
	cbuf         C.reindexer_buffer
	hasFinalizer bool
}

func (buf *RawCBuffer) FreeFinalized() {
	buf.hasFinalizer = false
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
	C.init_reindexer()
	bindings.RegisterBinding("builtin", &Builtin{})
}

func str2c(str string) C.reindexer_string {
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&str))
	return C.reindexer_string{p: unsafe.Pointer(hdr.Data), n: C.int(hdr.Len)}
}
func err2go(ret C.reindexer_error) error {
	if ret.what != nil {
		defer C.free(unsafe.Pointer(ret.what))
		return bindings.NewError("rq:"+C.GoString(ret.what), int(ret.code))
	}
	return nil
}

func ret2go(ret C.reindexer_ret) (*RawCBuffer, error) {
	if ret.err.what != nil {
		defer C.free(unsafe.Pointer(ret.err.what))
		defer C.free(unsafe.Pointer(ret.out.data))
		return nil, errors.New("rq:" + C.GoString(ret.err.what))
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
	return C.reindexer_buffer{
		data: (*C.uint8_t)(unsafe.Pointer(&buf[0])),
		len:  C.int(len(buf)),
	}
}

func buf2go(buf C.reindexer_buffer) []byte {
	if buf.data == nil || buf.len == 0 {
		return nil
	}
	length := int(buf.len)
	return (*[1 << 30]byte)(unsafe.Pointer(buf.data))[:length:length]
}

func bool2cint(v bool) C.int {
	if v {
		return 1
	}
	return 0
}

func (binding *Builtin) Init(u *url.URL) error {
	if len(u.Path) != 0 && u.Path != "/" {
		return binding.EnableStorage(u.Path)
	}
	return nil
}

func (binding *Builtin) Ping() error {
	return nil
}

func (binding *Builtin) ModifyItem(data []byte, mode int) (bindings.RawBuffer, error) {
	cgoLimiter <- struct{}{}
	defer func() { <-cgoLimiter }()
	return ret2go(C.reindexer_modify_item(buf2c(data), C.int(mode)))
}

func (binding *Builtin) OpenNamespace(namespace string, enableStorage, dropOnFormatError bool) error {
	var storageOptions bindings.StorageOptions
	storageOptions.Enabled(enableStorage).DropOnFileFormatError(dropOnFormatError)
	opts := C.StorageOpts{
		options: C.uint8_t(storageOptions),
	}
	return err2go(C.reindexer_open_namespace(str2c(namespace), opts))
}
func (binding *Builtin) CloseNamespace(namespace string) error {
	return err2go(C.reindexer_close_namespace(str2c(namespace)))
}

func (binding *Builtin) DropNamespace(namespace string) error {
	return err2go(C.reindexer_drop_namespace(str2c(namespace)))
}

func (binding *Builtin) EnableStorage(path string) error {
	l := len(path)
	if l > 0 && path[l-1] != '/' {
		path += "/"
	}
	return err2go(C.reindexer_enable_storage(str2c(path)))
}

func (binding *Builtin) AddIndex(namespace, index, jsonPath, indexType, fieldType string, indexOpts bindings.IndexOptions, collateMode int, sortOrderStr string) error {
	opts := C.IndexOptsC{
		options:   C.uint8_t(indexOpts),
		collate:   C.uint8_t(collateMode),
		sortOrderLetters: C.CString(sortOrderStr),
	}
	err := err2go(C.reindexer_add_index(str2c(namespace), str2c(index), str2c(jsonPath), str2c(indexType), str2c(fieldType), opts))
	C.free(unsafe.Pointer(opts.sortOrderLetters))
	return err
}

func (binding *Builtin) ConfigureIndex(namespace, index, config string) error {
	return err2go(C.reindexer_configure_index(str2c(namespace), str2c(index), str2c(config)))
}

func (binding *Builtin) PutMeta(namespace, key, data string) error {
	return err2go(C.reindexer_put_meta(str2c(namespace), str2c(key), str2c(data)))
}

func (binding *Builtin) GetMeta(namespace, key string) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_get_meta(str2c(namespace), str2c(key)))
}

func (binding *Builtin) Select(query string, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	cgoLimiter <- struct{}{}
	defer func() { <-cgoLimiter }()
	return ret2go(C.reindexer_select(str2c(query), bool2cint(withItems), (*C.int32_t)(unsafe.Pointer(&ptVersions[0]))))
}

func (binding *Builtin) SelectQuery(data []byte, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	cgoLimiter <- struct{}{}
	defer func() { <-cgoLimiter }()
	return ret2go(C.reindexer_select_query(buf2c(data), bool2cint(withItems), (*C.int32_t)(unsafe.Pointer(&ptVersions[0]))))
}

func (binding *Builtin) DeleteQuery(data []byte) (bindings.RawBuffer, error) {
	cgoLimiter <- struct{}{}
	defer func() { <-cgoLimiter }()
	return ret2go(C.reindexer_delete_query(buf2c(data)))
}

func (binding *Builtin) Commit(namespace string) error {
	return err2go(C.reindexer_commit(str2c(namespace)))
}

// CGoLogger logger function for C
//export CGoLogger
func CGoLogger(level int, msg string) {
	if logger != nil {
		logger.Printf(level, "%s", msg)
	}
}

func (binding *Builtin) EnableLogger(log bindings.Logger) {
	logger = log
	C.reindexer_enable_go_logger()

}
func (binding *Builtin) DisableLogger() {
	C.reindexer_disable_go_logger()
	logger = nil
}

func (binding *Builtin) GetStats() bindings.Stats {

	stats := C.reindexer_get_stats()

	return bindings.Stats{
		TimeInsert:   time.Microsecond * time.Duration(int(stats.time_insert)),
		CountInsert:  int(stats.count_insert),
		TimeUpdate:   time.Microsecond * time.Duration(int(stats.time_update)),
		CountUpdate:  int(stats.count_update),
		TimeUpsert:   time.Microsecond * time.Duration(int(stats.time_upsert)),
		CountUpsert:  int(stats.count_upsert),
		TimeDelete:   time.Microsecond * time.Duration(int(stats.time_delete)),
		CountDelete:  int(stats.count_delete),
		TimeSelect:   time.Microsecond * time.Duration(int(stats.time_select)),
		CountSelect:  int(stats.count_select),
		TimeGetItem:  time.Microsecond * time.Duration(int(stats.time_get_item)),
		CountGetItem: int(stats.count_get_item),
		TimeJoin:     time.Microsecond * time.Duration(int(stats.time_join)),
		CountJoin:    int(stats.count_join),
	}
}

func (binding *Builtin) ResetStats() {
	C.reindexer_reset_stats()
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
	cbufs  []C.reindexer_buffer
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

		C.reindexer_free_buffers((*C.reindexer_buffer)(unsafe.Pointer(&bf.cbufs[0])), C.int(len(bf.cbufs)))

		for _, buf := range bf.bufs2 {
			buf.cbuf.data = nil
			bf.toPool(buf)
		}
		bf.cbufs = bf.cbufs[:0]
		bf.bufs2 = bf.bufs2[:0]
	}
}

func (bf *bufFreeBatcher) add(buf *RawCBuffer) {
	if buf.cbuf.data != nil {
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
