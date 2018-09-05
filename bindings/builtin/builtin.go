package builtin

// #include "core/cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"runtime"
	"sync"
	"unsafe"

	"github.com/restream/reindexer/bindings"
)

const defCgoLimit = 2000

var bufFree = newBufFreeBatcher()

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

var logger Logger

var bufPool sync.Pool

type Builtin struct {
	cgoLimiter chan struct{}
}

type RawCBuffer struct {
	cbuf         C.reindexer_resbuffer
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
		defer C.free(unsafe.Pointer(uintptr(ret.out.data)))
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

func (binding *Builtin) SetReindexerInstance(p unsafe.Pointer) {
	C.change_reindexer_instance(p)
}

func (binding *Builtin) Init(u *url.URL, options ...interface{}) error {

	cgoLimit := defCgoLimit
	for _, option := range options {
		switch v := option.(type) {
		case bindings.OptionCgoLimit:
			cgoLimit = v.CgoLimit
		case bindings.OptionBuiltinWithServer:
		default:
			fmt.Printf("Unknown builtin option: %v\n", option)
		}
	}

	binding.cgoLimiter = make(chan struct{}, cgoLimit)
	if len(u.Path) != 0 && u.Path != "/" {
		err := binding.EnableStorage(u.Path)
		if err != nil {
			return err
		}
	}

	return err2go(C.reindexer_init_system_namespaces())
}

func (binding *Builtin) Ping() error {
	return nil
}

func (binding *Builtin) ModifyItem(nsHash int, data []byte, mode int) (bindings.RawBuffer, error) {
	binding.cgoLimiter <- struct{}{}
	defer func() { <-binding.cgoLimiter }()
	return ret2go(C.reindexer_modify_item(buf2c(data), C.int(mode)))
}

func (binding *Builtin) OpenNamespace(namespace string, enableStorage, dropOnFormatError bool, cacheMode uint8) error {
	var storageOptions bindings.StorageOptions
	storageOptions.Enabled(enableStorage).DropOnFileFormatError(dropOnFormatError)
	opts := C.StorageOpts{
		options: C.uint8_t(storageOptions),
	}
	return err2go(C.reindexer_open_namespace(str2c(namespace), opts, C.uint8_t(cacheMode)))
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
		options:          C.uint8_t(indexOpts),
		collate:          C.uint8_t(collateMode),
		sortOrderLetters: C.CString(sortOrderStr),
	}
	err := err2go(C.reindexer_add_index(str2c(namespace), str2c(index), str2c(jsonPath), str2c(indexType), str2c(fieldType), opts))
	C.free(unsafe.Pointer(opts.sortOrderLetters))
	return err
}

func (binding *Builtin) DropIndex(namespace, index string) error {
	return err2go(C.reindexer_drop_index(str2c(namespace), str2c(index)))
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
	binding.cgoLimiter <- struct{}{}
	defer func() { <-binding.cgoLimiter }()
	return ret2go(C.reindexer_select(str2c(query), bool2cint(withItems), (*C.int32_t)(unsafe.Pointer(&ptVersions[0])), C.int(len(ptVersions))))
}

func (binding *Builtin) SelectQuery(data []byte, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	binding.cgoLimiter <- struct{}{}
	defer func() { <-binding.cgoLimiter }()
	return ret2go(C.reindexer_select_query(buf2c(data), bool2cint(withItems), (*C.int32_t)(unsafe.Pointer(&ptVersions[0])), C.int(len(ptVersions))))
}

func (binding *Builtin) DeleteQuery(nsHash int, data []byte) (bindings.RawBuffer, error) {
	binding.cgoLimiter <- struct{}{}
	defer func() { <-binding.cgoLimiter }()
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

		C.reindexer_free_buffers((*C.reindexer_resbuffer)(unsafe.Pointer(&bf.cbufs[0])), C.int(len(bf.cbufs)))

		for _, buf := range bf.bufs2 {
			buf.cbuf.data = 0
			bf.toPool(buf)
		}
		bf.cbufs = bf.cbufs[:0]
		bf.bufs2 = bf.bufs2[:0]
	}
}

func (bf *bufFreeBatcher) add(buf *RawCBuffer) {
	if buf.cbuf.data != 0 {
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
