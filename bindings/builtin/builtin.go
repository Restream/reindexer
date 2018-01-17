package builtin

// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra -I../../cpp_src
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../../cpp_src
// #cgo LDFLAGS: -lreindexer -lleveldb -lsnappy -L${SRCDIR}/../../cpp_src/.build -lstdc++ -g
// #include "core/cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"reflect"
	"runtime"
	"sync"
	"unsafe"

	"time"

	"github.com/restream/reindexer/bindings"
)

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
	if buf.cbuf.data != nil {
		C.reindexer_free_buffer(buf.cbuf)
	}
	buf.cbuf.data = nil
	bufPool.Put(buf)
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
	rbuf := newRawCBuffer()
	rbuf.cbuf = ret.out
	if ret.err.what != nil {
		defer C.free(unsafe.Pointer(ret.err.what))
		return rbuf, errors.New("rq:" + C.GoString(ret.err.what))
	}
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

func (binding *Builtin) ModifyItem(data []byte, mode int) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_modify_item(buf2c(data), C.int(mode)))
}

func (binding *Builtin) OpenNamespace(namespace string, enableStorage, dropOnFormatError bool) error {
	opts := C.StorageOpts{
		IsEnabled:               bool2cint(enableStorage),
		IsDropOnFileFormatError: bool2cint(dropOnFormatError),
		IsCreateIfMissing:       bool2cint(true),
		IsDropOnIndexesConflict: bool2cint(false),
	}
	return err2go(C.reindexer_open_namespace(str2c(namespace), opts))
}
func (binding *Builtin) CloseNamespace(namespace string) error {
	return err2go(C.reindexer_close_namespace(str2c(namespace)))
}

func (binding *Builtin) DropNamespace(namespace string) error {
	return err2go(C.reindexer_drop_namespace(str2c(namespace)))
}

func (binding *Builtin) CloneNamespace(src string, dst string) error {
	return err2go(C.reindexer_clone_namespace(str2c(src), str2c(dst)))
}
func (binding *Builtin) RenameNamespace(src string, dst string) error {
	return err2go(C.reindexer_rename_namespace(str2c(src), str2c(dst)))
}

func (binding *Builtin) EnableStorage(path string) error {
	return err2go(C.reindexer_enable_storage(str2c(path)))
}

func (binding *Builtin) AddIndex(namespace, index, jsonPath, indexType, fieldType string, isArray, isPK, isDense, isAppendable bool, collateMode int) error {
	opts := C.IndexOpts{
		IsArray:       bool2cint(isArray),
		IsPK:          bool2cint(isPK),
		IsDense:       bool2cint(isDense),
		IsAppendable:  bool2cint(isAppendable),
		CollateMode:   C.int(collateMode),
	}
	return err2go(C.reindexer_add_index(str2c(namespace), str2c(index), str2c(jsonPath), str2c(indexType), str2c(fieldType), opts))
}

func (binding *Builtin) ConfigureIndex(namespace, index, config string) error {
	return err2go(C.reindexer_configure_index(str2c(namespace), str2c(index), str2c(config)))
}

func (binding *Builtin) PutMeta(data []byte) error {
	return err2go(C.reindexer_put_meta(buf2c(data)))
}

func (binding *Builtin) GetMeta(data []byte) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_get_meta(buf2c(data)))
}
func (binding *Builtin) GetPayloadType(resBuf []byte, nsid int) (bindings.RawBuffer, error) {
	cbuf := buf2c(resBuf)
	cbuf.results_flag = 1
	return ret2go(C.reindexer_get_payload_type(cbuf, C.int(nsid)))
}

func (binding *Builtin) Select(query string, withItems bool) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_select(str2c(query), bool2cint(withItems)))
}

func (binding *Builtin) SelectQuery(withItems bool, data []byte) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_select_query(bool2cint(withItems), buf2c(data)))
}

func (binding *Builtin) DeleteQuery(data []byte) (bindings.RawBuffer, error) {
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
