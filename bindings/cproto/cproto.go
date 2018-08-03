package cproto

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/restream/reindexer/bindings"
)

const (
	defConnPoolSize  = 8
	pingerTimeoutSec = 60

	opRd = 0
	opWr = 1
)

func init() {
	bindings.RegisterBinding("cproto", new(NetCProto))
}

type NetCProto struct {
	url              url.URL
	pool             chan *connection
	onChangeCallback func()
	serverStartTime  int64
	retryAttempts    bindings.OptionRetryAttempts
}

func (binding *NetCProto) Init(u *url.URL, options ...interface{}) (err error) {

	connPoolSize := defConnPoolSize
	for _, option := range options {
		switch v := option.(type) {
		case bindings.OptionConnPoolSize:
			connPoolSize = v.ConnPoolSize
		case bindings.OptionRetryAttempts:
			binding.retryAttempts = v
		default:
			fmt.Printf("Unknown cproto option: %v\n", option)
		}
	}

	if binding.retryAttempts.Read < 0 {
		binding.retryAttempts.Read = 0
	}
	if binding.retryAttempts.Write < 0 {
		binding.retryAttempts.Write = 0
	}

	binding.url = *u
	binding.pool = make(chan *connection, connPoolSize)
	for i := 0; i < connPoolSize; i++ {
		conn, cerr := newConnection(binding)
		if cerr != nil {
			err = cerr
		}
		binding.pool <- conn
	}
	go binding.pinger()
	return
}

func (binding *NetCProto) Ping() error {
	return binding.rpcCallNoResults(opRd, cmdPing)
}

func (binding *NetCProto) ModifyItem(nsHash int, data []byte, mode int) (bindings.RawBuffer, error) {
	buf, err := binding.rpcCall(opWr, cmdModifyItem, data, mode)
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

	return binding.rpcCallNoResults(opWr, cmdOpenNamespace, namespace, bsnsdef)
}

func (binding *NetCProto) CloseNamespace(namespace string) error {
	return binding.rpcCallNoResults(opWr, cmdCloseNamespace, namespace)
}

func (binding *NetCProto) DropNamespace(namespace string) error {
	return binding.rpcCallNoResults(opWr, cmdDropNamespace, namespace)
}

type indexDef struct {
	Name         string `json:"name"`
	JSONPath     string `json:"json_path"`
	IndexType    string `json:"index_type"`
	FieldType    string `json:"field_type"`
	IsPK         bool   `json:"is_pk"`
	IsArray      bool   `json:"is_array"`
	IsDense      bool   `json:"is_dense"`
	IsSparse     bool   `json:"is_sparse"`
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
		IsSparse:     opts.IsSparse(),
		IsAppendable: opts.IsAppendable(),
		CollateMode:  cm,
		SortOrder:    sortOrder,
	}

	bidef, err := json.Marshal(idef)
	if err != nil {
		return err
	}

	return binding.rpcCallNoResults(opWr, cmdAddIndex, namespace, bidef)
}

func (binding *NetCProto) DropIndex(namespace, index string) error {
	return binding.rpcCallNoResults(opWr, cmdDropIndex, namespace, index)
}

func (binding *NetCProto) ConfigureIndex(namespace, index, config string) error {
	return binding.rpcCallNoResults(opWr, cmdConfigureIndex, namespace, index, config)
}

func (binding *NetCProto) PutMeta(namespace, key, data string) error {
	return binding.rpcCallNoResults(opWr, cmdPutMeta, namespace, key, data)
}

func (binding *NetCProto) GetMeta(namespace, key string) (bindings.RawBuffer, error) {
	buf, err := binding.rpcCall(opRd, cmdGetMeta, namespace, key)
	if err != nil {
		buf.Free()
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	buf.reqID = -1
	return buf, nil
}

func (binding *NetCProto) Select(query string, withItems bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	flags := 0
	if withItems {
		flags |= bindings.ResultsWithJson
	} else {
		flags |= bindings.ResultsWithCJson | bindings.ResultsWithPayloadTypes
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(opRd, cmdSelectSQL, query, flags, int32(fetchCount), int64(-1), ptVersions)
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
	flags := 0
	if withItems {
		flags |= bindings.ResultsWithJson
	} else {
		flags |= bindings.ResultsWithCJson | bindings.ResultsWithPayloadTypes
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(opRd, cmdSelect, data, flags, int32(fetchCount), int64(-1), ptVersions)
	if err != nil {
		buf.Free()
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	buf.reqID = buf.args[1].(int)
	buf.needClose = buf.reqID != -1
	return buf, nil
}

func (binding *NetCProto) DeleteQuery(nsHash int, data []byte) (bindings.RawBuffer, error) {
	buf, err := binding.rpcCall(opWr, cmdDeleteQuery, data)
	if err != nil {
		buf.Free()
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	return buf, nil
}

func (binding *NetCProto) Commit(namespace string) error {
	return binding.rpcCallNoResults(opWr, cmdCommit, namespace)
}

func (binding *NetCProto) OnChangeCallback(f func()) {
	binding.onChangeCallback = f
}

func (binding *NetCProto) EnableLogger(log bindings.Logger) {
	fmt.Println("cproto binding EnableLogger method is dummy")
}
func (binding *NetCProto) DisableLogger() {
	fmt.Println("cproto binding DisableLogger method is dummy")
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

func (binding *NetCProto) rpcCall(op int, cmd int, args ...interface{}) (buf *NetBuffer, err error) {
	var attempts int
	switch op {
	case opRd:
		attempts = binding.retryAttempts.Read + 1
	default:
		attempts = binding.retryAttempts.Write + 1
	}
	for i := 0; i < attempts; i++ {
		if buf, err = binding.getConn().rpcCall(cmd, args...); err == nil {
			return
		}
		switch err.(type) {
		case net.Error, *net.OpError:
			time.Sleep(time.Second * time.Duration(i))
		default:
			return
		}
	}
	return
}

func (binding *NetCProto) rpcCallNoResults(op int, cmd int, args ...interface{}) error {
	buf, err := binding.rpcCall(op, cmd, args...)
	buf.Free()
	return err
}

func (binding *NetCProto) pinger() {
	timeout := time.Second * time.Duration(pingerTimeoutSec)
	ticker := time.NewTicker(timeout)
	for now := range ticker.C {
		for i := 0; i < cap(binding.pool); i++ {
			conn := binding.getConn()
			if !conn.hasError() && conn.lastReadTime().Add(timeout).Before(now) {
				buf, _ := conn.rpcCall(cmdPing)
				buf.Free()
			}
		}
	}
}

func (binding *NetCProto) checkServerStartTime(timestamp int64) {
	old := atomic.SwapInt64(&binding.serverStartTime, timestamp)
	if old != 0 && old != timestamp && binding.onChangeCallback != nil {
		binding.onChangeCallback()
	}
}
