package builtin

// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra -I../..
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../..
// #cgo LDFLAGS: -lreindexer -lleveldb -lsnappy -L${SRCDIR}/../../.build -lstdc++
// #include "cbinding/reindexer_c.h"
// #include "reindexer_cgo.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"reflect"
	"sync"
	"unsafe"

	"time"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/bindings"
)

var logger reindexer.Logger

var bufPool sync.Pool

type Builtin struct {
}

type RawCBuffer struct {
	cbuf C.reindexer_buffer
}

func (buf *RawCBuffer) Free() {
	if buf.cbuf.data != nil {
		C.free(unsafe.Pointer(buf.cbuf.data))
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
	defer C.free(unsafe.Pointer(ret.what))
	if ret.what != nil {
		return errors.New("rq:" + C.GoString(ret.what))
	}
	return nil
}

func ret2go(ret C.reindexer_ret) (*RawCBuffer, error) {
	defer C.free(unsafe.Pointer(ret.err.what))
	rbuf := newRawCBuffer()
	rbuf.cbuf = ret.out
	if ret.err.what != nil {
		return rbuf, errors.New("rq:" + C.GoString(ret.err.what))
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

func (binding *Builtin) DeleteItem(data []byte) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_delete(buf2c(data)))
}

func (binding *Builtin) UpsertItem(data []byte) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_upsert(buf2c(data)))
}

func (binding *Builtin) AddNamespace(namespace string) error {
	return err2go(C.reindexer_addnamespace(str2c(namespace)))
}
func (binding *Builtin) DeleteNamespace(namespace string) error {
	return err2go(C.reindexer_delete_namespace(str2c(namespace)))
}
func (binding *Builtin) CloneNamespace(src string, dst string) error {
	return err2go(C.reindexer_clone_namespace(str2c(src), str2c(dst)))
}
func (binding *Builtin) RenameNamespace(src string, dst string) error {
	return err2go(C.reindexer_rename_namespace(str2c(src), str2c(dst)))
}

func (binding *Builtin) EnableStorage(namespace string, path string) error {
	return err2go(C.reindexer_enable_storage(str2c(namespace), str2c(path)))
}

func (binding *Builtin) AddIndex(namespace, index, jsonPath string, indexType int, isArray, isPK bool) error {
	opts := C.IndexOpts{IsArray: bool2cint(isArray), IsPK: bool2cint(isPK)}
	return err2go(C.reindexer_addindex(str2c(namespace), str2c(index), str2c(jsonPath), C.IndexType(indexType), opts))
}

func (binding *Builtin) PutMeta(data []byte) error {
	return err2go(C.reindexer_put_meta(buf2c(data)))
}

func (binding *Builtin) GetMeta(data []byte) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_get_meta(buf2c(data)))
}

func (binding *Builtin) GetItems(data []byte) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_get_items(buf2c(data)))
}

func (binding *Builtin) Select(query string, withItems bool) (bindings.RawBuffer, error) {
	return ret2go(C.reindexer_select(str2c(query), bool2cint(withItems)))
}

func (binding *Builtin) SelectQuery(q *bindings.Query, withItems bool, data []byte) (bindings.RawBuffer, error) {

	query := C.reindexer_query{
		limit:       C.int(q.LimitItems),
		offset:      C.int(q.StartOffset),
		debug_level: C.int(q.DebugLevel),
		calc_total:  bool2cint(q.ReqTotalCount),
	}
	return ret2go(C.reindexer_select_query(query, bool2cint(withItems), buf2c(data)))
}

func (binding *Builtin) DeleteQuery(q *bindings.Query, data []byte) (bindings.RawBuffer, error) {

	query := C.reindexer_query{
		limit:       C.int(q.LimitItems),
		offset:      C.int(q.StartOffset),
		debug_level: C.int(q.DebugLevel),
		calc_total:  bool2cint(q.ReqTotalCount),
	}

	return ret2go(C.reindexer_delete_query(query, buf2c(data)))
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
