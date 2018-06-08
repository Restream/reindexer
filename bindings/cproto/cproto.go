package cproto

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"

	"github.com/restream/reindexer/bindings"
)

const connPoolSize = 8

func init() {
	bindings.RegisterBinding("cproto", new(NetCProto))
}

type NetCProto struct {
	url  url.URL
	pool chan *connection
}

func (binding *NetCProto) Init(u *url.URL) (err error) {
	binding.url = *u
	binding.pool = make(chan *connection, connPoolSize)
	for i := 0; i < connPoolSize; i++ {
		conn, cerr := newConnection(binding)
		if cerr != nil {
			err = cerr
		}
		binding.pool <- conn
	}
	return
}

func (binding *NetCProto) Ping() error {
	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdPing)
}

func (binding *NetCProto) ModifyItem(data []byte, mode int) (bindings.RawBuffer, error) {
	conn := binding.getConn()
	buf, err := conn.rpcCall(cmdModifyItem, data, mode)
	if err != nil {
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	buf.reqID = -1
	return buf, nil
}

type storageOpts struct {
	EnableStorage     bool `json:"enabled"`
	DropOnFormatError bool `json:"drop_on_file_format_error"`
	CreateIfMissing   bool `json:"create_if_missing"`
}
type nsDef struct {
	Namespace string      `json:"name"`
	SOpts     storageOpts `json:"storage"`
	CacheMode uint8       `json:"cached_mode"`
}

func (binding *NetCProto) OpenNamespace(namespace string, enableStorage, dropOnFormatError bool, cacheMode uint8) error {

	sops := storageOpts{
		EnableStorage:     enableStorage,
		DropOnFormatError: dropOnFormatError,
		CreateIfMissing:   true,
	}
	nsdef := nsDef{
		SOpts:     sops,
		CacheMode: cacheMode,
		Namespace: namespace,
	}

	bsnsdef, err := json.Marshal(nsdef)
	if err != nil {
		return err
	}

	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdOpenNamespace, bsnsdef, "")
}

func (binding *NetCProto) CloseNamespace(namespace string) error {
	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdCloseNamespace, namespace)
}

func (binding *NetCProto) DropNamespace(namespace string) error {
	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdDropNamespace, namespace)
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
	SortOrder    string `json:"sort_order_letters"`
}

func (binding *NetCProto) AddIndex(namespace, index, jsonPath, indexType, fieldType string, opts bindings.IndexOptions, collateMode int, sortOrder string) error {

	cm := ""
	switch collateMode {
	case bindings.CollateASCII:
		cm = "ascii"
	case bindings.CollateUTF8:
		cm = "utf8"
	case bindings.CollateNumeric:
		cm = "numeric"
	case bindings.CollateCustom:
		cm = "custom"
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
		SortOrder:    sortOrder,
	}

	bidef, err := json.Marshal(idef)
	if err != nil {
		return err
	}

	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdAddIndex, namespace, bidef)
}

func (binding *NetCProto) DropIndex(namespace, index string) error {
	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdDropIndex, namespace, index)
}

func (binding *NetCProto) ConfigureIndex(namespace, index, config string) error {
	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdConfigureIndex, namespace, index, config)
}

func (binding *NetCProto) PutMeta(namespace, key, data string) error {
	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdPutMeta, namespace, key, data)
}

func (binding *NetCProto) GetMeta(namespace, key string) (bindings.RawBuffer, error) {
	conn := binding.getConn()
	buf, err := conn.rpcCall(cmdGetMeta, namespace, key)
	if err != nil {
		buf.Free()
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	buf.reqID = -1
	return buf, nil
}

func (binding *NetCProto) Select(query string, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	conn := binding.getConn()
	flags := 0
	if withItems {
		flags |= bindings.ResultsWithJson
	} else {
		flags |= bindings.ResultsWithCJson | bindings.ResultsWithPayloadTypes
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := conn.rpcCall(cmdSelectSQL, query, flags, int32(fetchCount), int64(-1), ptVersions)
	if err != nil {
		buf.Free()
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	buf.reqID = buf.args[1].(int)
	buf.needClose = buf.reqID != -1
	return buf, nil
}

func (binding *NetCProto) SelectQuery(data []byte, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	conn := binding.getConn()
	flags := 0
	if withItems {
		flags |= bindings.ResultsWithJson
	} else {
		flags |= bindings.ResultsWithCJson | bindings.ResultsWithPayloadTypes
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := conn.rpcCall(cmdSelect, data, flags, int32(fetchCount), int64(-1), ptVersions)
	if err != nil {
		buf.Free()
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	buf.reqID = buf.args[1].(int)
	buf.needClose = buf.reqID != -1
	return buf, nil
}

func (binding *NetCProto) DeleteQuery(data []byte) (bindings.RawBuffer, error) {
	conn := binding.getConn()

	buf, err := conn.rpcCall(cmdDeleteQuery, data)
	if err != nil {
		buf.Free()
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	return buf, nil
}

func (binding *NetCProto) Commit(namespace string) error {
	conn := binding.getConn()
	return conn.rpcCallNoResults(cmdCommit, namespace)
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
	conn = <-binding.pool
	if conn.hasError() {
		conn, _ = newConnection(binding)
	}
	binding.pool <- conn
	return
}
