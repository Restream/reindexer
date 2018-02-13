package cproto

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"

	"github.com/restream/reindexer/bindings"
)

const connPoolSize = 8

var bufPool sync.Pool

type NetCProto struct {
	url  url.URL
	pool [connPoolSize]*connection
	seqs chan int
}

type NetBuffer struct {
	buf   []byte
	conn  *connection
	reqID int
}

func (buf *NetBuffer) Free() {
	bufPool.Put(buf)
}

func (buf *NetBuffer) GetBuf() []byte {
	return buf.buf
}

func newNetBuffer() *NetBuffer {
	obj := bufPool.Get()
	if obj != nil {
		return obj.(*NetBuffer)
	}
	return &NetBuffer{}
}

func init() {
	nc := &NetCProto{}

	bindings.RegisterBinding("cproto", nc)
}
func (binding *NetCProto) Init(u *url.URL) (err error) {
	binding.url = *u
	binding.seqs = make(chan int, len(binding.pool))
	for i := 0; i < len(binding.pool); i++ {
		var cerr error
		binding.pool[i], cerr = newConnection(binding)
		if cerr != nil {
			err = cerr
		}
		binding.seqs <- i
	}
	return nil
}

func (binding *NetCProto) Ping() error {
	conn := binding.getConn()
	_, err := conn.rpcCall(cmdPing)
	return err
}

func (binding *NetCProto) ModifyItem(data []byte, mode int) (bindings.RawBuffer, error) {
	conn := binding.getConn()
	res, err := conn.rpcCall(cmdModifyItem, data, mode)
	if err != nil {
		return nil, err
	}

	return &NetBuffer{buf: res[0].([]byte), reqID: -1}, nil
}

type storageOpts struct {
	EnableStorage     bool `json:"enable_storage"`
	DropOnFormatError bool `json:"drop_on_file_format_error"`
	CreateIfMissing   bool `json:"create_if_missing"`
}

func (binding *NetCProto) OpenNamespace(namespace string, enableStorage, dropOnFormatError bool) error {

	sops := storageOpts{
		EnableStorage:     enableStorage,
		DropOnFormatError: dropOnFormatError,
		CreateIfMissing:   true,
	}

	bsops, err := json.Marshal(sops)

	conn := binding.getConn()
	_, err = conn.rpcCall(cmdOpenNamespace, namespace, bsops)
	return err
}

func (binding *NetCProto) CloseNamespace(namespace string) error {
	conn := binding.getConn()
	_, err := conn.rpcCall(cmdCloseNamespace, namespace)
	return err
}

func (binding *NetCProto) DropNamespace(namespace string) error {
	conn := binding.getConn()
	_, err := conn.rpcCall(cmdDropNamespace, namespace)
	return err
}

func (binding *NetCProto) CloneNamespace(src string, dst string) error {
	conn := binding.getConn()
	_, err := conn.rpcCall(cmdCloneNamespace, src, dst)
	return err
}

func (binding *NetCProto) RenameNamespace(src string, dst string) error {
	conn := binding.getConn()
	_, err := conn.rpcCall(cmdRenameNamespace, src, dst)
	return err
}

type indexDef struct {
	Name         string `json:"name"`
	JSONPath     string `json:"json_path"`
	IndexType    string `json:"index_type"`
	FieldType    string `json:"field_type"`
	IsPK         bool   `json:"is_pk"`
	IsArray      bool   `json:"is_array"`
	IsDense      bool   `json:"is_dense"`
	IsAppendable bool   `json:"is_appendable"`
	CollateMode  string `json:"collate_mode"`
}

func (binding *NetCProto) AddIndex(namespace, index, jsonPath, indexType, fieldType string, opts bindings.IndexOptions, collateMode int) error {
	conn := binding.getConn()

	cm := ""
	switch collateMode {
	case bindings.CollateASCII:
		cm = "ascii"
	case bindings.CollateUTF8:
		cm = "utf8"
	case bindings.CollateNumeric:
		cm = "numeric"
	}

	idef := indexDef{
		Name:         index,
		JSONPath:     jsonPath,
		IndexType:    indexType,
		FieldType:    fieldType,
		IsArray:      opts.IsArray(),
		IsPK:         opts.IsPK(),
		IsDense:      opts.IsDense(),
		IsAppendable: opts.IsAppendable(),
		CollateMode:  cm,
	}

	bidef, err := json.Marshal(idef)
	_, err = conn.rpcCall(cmdAddIndex, namespace, bidef)
	return err
}

func (binding *NetCProto) ConfigureIndex(namespace, index, config string) error {
	conn := binding.getConn()
	_, err := conn.rpcCall(cmdConfigureIndex, index, config)
	return err
}

func (binding *NetCProto) PutMeta(namespace, key, data string) error {
	conn := binding.getConn()
	_, err := conn.rpcCall(cmdPutMeta, namespace, key, data)
	return err
}

func (binding *NetCProto) GetMeta(namespace, key string) (bindings.RawBuffer, error) {
	conn := binding.getConn()
	res, err := conn.rpcCall(cmdGetMeta, namespace, key)
	if err != nil {
		return nil, err
	}
	return &NetBuffer{buf: res[0].([]byte), reqID: -1}, nil
}

func (binding *NetCProto) Select(query string, withItems bool, ptVersions []int32) (bindings.RawBuffer, error) {
	conn := binding.getConn()

	flags := bindings.ResultsWithPayloadTypes
	flags |= bindings.ResultsClose
	if withItems {
		flags |= bindings.ResultsWithJson
	} else {
		flags |= bindings.ResultsWithCJson
	}

	res, err := conn.rpcCall(cmdSelectSQL, query, flags, int32(0x7FFFFFFF), int64(-1), ptVersions)
	if err != nil {
		return nil, err
	}
	return &NetBuffer{buf: res[0].([]byte), reqID: res[1].(int)}, nil
}

func (binding *NetCProto) SelectQuery(data []byte, withItems bool, ptVersions []int32) (bindings.RawBuffer, error) {
	conn := binding.getConn()

	flags := bindings.ResultsWithPayloadTypes
	flags |= bindings.ResultsClose
	if withItems {
		flags |= bindings.ResultsWithJson
	} else {
		flags |= bindings.ResultsWithCJson
	}

	res, err := conn.rpcCall(cmdSelect, data, flags, int32(0x7FFFFFFF), int64(-1), ptVersions)
	if err != nil {
		return nil, err
	}
	return &NetBuffer{buf: res[0].([]byte), reqID: res[1].(int)}, nil
}

func (binding *NetCProto) DeleteQuery(data []byte) (bindings.RawBuffer, error) {
	conn := binding.getConn()

	res, err := conn.rpcCall(cmdDeleteQuery, data)
	if err != nil {
		return nil, err
	}
	b := res[0].([]byte)
	return &NetBuffer{buf: b, reqID: res[1].(int)}, nil
}

func (binding *NetCProto) Commit(namespace string) error {
	conn := binding.getConn()

	_, err := conn.rpcCall(cmdCommit, namespace)
	return err
}

func (binding *NetCProto) EnableLogger(log bindings.Logger) {
	fmt.Println("cproto binding EnableLogger method is dummy")
}
func (binding *NetCProto) DisableLogger() {
	fmt.Println("cproto binding DisableLogger method is dummy")
}

func (binding *NetCProto) GetStats() bindings.Stats {
	fmt.Println("cproto binding GetStats method is dummy")
	return bindings.Stats{}
}

func (binding *NetCProto) ResetStats() {
	fmt.Println("cproto binding ResetStats method is dummy")
}

func (binding *NetCProto) EnableStorage(path string) error {
	fmt.Println("cproto binding EnableStorage method is dummy")
	return nil
}

func (binding *NetCProto) getConn() (conn *connection) {
	index := <-binding.seqs
	if binding.pool[index].hasError() {
		binding.pool[index], _ = newConnection(binding)
	}
	conn = binding.pool[index]
	binding.seqs <- index
	return
}
