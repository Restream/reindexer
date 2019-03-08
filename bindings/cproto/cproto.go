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
	"github.com/restream/reindexer/cjson"
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

func (binding *NetCProto) Clone() bindings.RawBinding {
	return &NetCProto{}
}

func (binding *NetCProto) Ping() error {
	return binding.rpcCallNoResults(opRd, cmdPing)
}

func (binding *NetCProto) BeginTx(namespace string) (ctx bindings.TxCtx, err error) {
	buf, err := binding.rpcCall(opWr, cmdStartTransaction, namespace)

	if len(buf.args) == 0 {
		return
	}
	ctx.Result = buf
	ctx.Id = uint64(buf.args[0].(int64))
	return
}

func (binding *NetCProto) CommitTx(ctx *bindings.TxCtx) (bindings.RawBuffer, error) {
	netBuffer := ctx.Result.(*NetBuffer)

	txBuf, err := netBuffer.conn.rpcCall(cmdCommitTx, int64(ctx.Id))

	defer txBuf.Free()
	defer netBuffer.close()
	netBuffer.needClose = false
	txBuf.needClose = false

	if err != nil {
		return nil, err
	}

	txBuf.buf, netBuffer.buf = netBuffer.buf, txBuf.buf
	netBuffer.result = txBuf.args[0].([]byte)
	return netBuffer, nil
}

func (binding *NetCProto) RollbackTx(ctx *bindings.TxCtx) error {
	netBuffer := ctx.Result.(*NetBuffer)

	txBuf, err := netBuffer.conn.rpcCall(cmdRollbackTx, int64(ctx.Id))

	defer txBuf.Free()
	defer netBuffer.Free()
	netBuffer.needClose = false
	txBuf.needClose = false

	if err != nil {
		return err
	}

	return nil
}

func (binding *NetCProto) ModifyItemTx(txCtx *bindings.TxCtx, format int, data []byte, mode int, precepts []string, stateToken int) error {
	var packedPercepts []byte
	if len(precepts) != 0 {
		ser1 := cjson.NewPoolSerializer()
		defer ser1.Close()

		ser1.PutVarCUInt(len(precepts))
		for _, precept := range precepts {
			ser1.PutVString(precept)
		}
		packedPercepts = ser1.Bytes()
	}

	netBuffer := txCtx.Result.(*NetBuffer)

	txBuf, err := netBuffer.conn.rpcCall(cmdAddTxItem, format, data, mode, packedPercepts, stateToken, int64(txCtx.Id))

	defer txBuf.Free()
	if err != nil {
		netBuffer.close()
		return err
	}

	return nil
}
func (binding *NetCProto) ModifyItem(nsHash int, namespace string, format int, data []byte, mode int, precepts []string, stateToken int) (bindings.RawBuffer, error) {

	var packedPercepts []byte
	if len(precepts) != 0 {
		ser1 := cjson.NewPoolSerializer()
		defer ser1.Close()

		ser1.PutVarCUInt(len(precepts))
		for _, precept := range precepts {
			ser1.PutVString(precept)
		}
		packedPercepts = ser1.Bytes()
	}

	buf, err := binding.rpcCall(opWr, cmdModifyItem, namespace, format, data, mode, packedPercepts, stateToken, 0)
	if err != nil {
		return nil, err
	}
	buf.result = buf.args[0].([]byte)
	buf.reqID = -1
	return buf, nil
}

func (binding *NetCProto) OpenNamespace(namespace string, enableStorage, dropOnFormatError bool) error {
	storageOtps := bindings.StorageOpts{
		EnableStorage:     enableStorage,
		DropOnFormatError: dropOnFormatError,
		CreateIfMissing:   true,
	}

	namespaceDef := bindings.NamespaceDef{
		StorageOpts: storageOtps,
		Namespace:   namespace,
	}

	bNamespaceDef, err := json.Marshal(namespaceDef)
	if err != nil {
		return err
	}

	return binding.rpcCallNoResults(opWr, cmdOpenNamespace, bNamespaceDef)
}

func (binding *NetCProto) CloseNamespace(namespace string) error {
	return binding.rpcCallNoResults(opWr, cmdCloseNamespace, namespace)
}

func (binding *NetCProto) DropNamespace(namespace string) error {
	return binding.rpcCallNoResults(opWr, cmdDropNamespace, namespace)
}

func (binding *NetCProto) AddIndex(namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	return binding.rpcCallNoResults(opWr, cmdAddIndex, namespace, bIndexDef)
}

func (binding *NetCProto) UpdateIndex(namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	return binding.rpcCallNoResults(opWr, cmdUpdateIndex, namespace, bIndexDef)
}

func (binding *NetCProto) DropIndex(namespace, index string) error {
	return binding.rpcCallNoResults(opWr, cmdDropIndex, namespace, index)
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
		flags |= bindings.ResultsJson
	} else {
		flags |= bindings.ResultsCJson | bindings.ResultsWithPayloadTypes | bindings.ResultsWithItemID
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(opRd, cmdSelectSQL, query, flags, int32(fetchCount), ptVersions)
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
		flags |= bindings.ResultsJson
	} else {
		flags |= bindings.ResultsCJson | bindings.ResultsWithPayloadTypes | bindings.ResultsWithItemID
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(opRd, cmdSelect, data, flags, int32(fetchCount), ptVersions)
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

func (binding *NetCProto) UpdateQuery(nsHash int, data []byte) (bindings.RawBuffer, error) {
	buf, err := binding.rpcCall(opWr, cmdUpdateQuery, data)
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

func (binding *NetCProto) Status() bindings.Status {
	var totalQueueSize, totalQueueUsage, connUsage int
	for i := 0; i < cap(binding.pool); i++ {
		conn := binding.getConn()
		totalQueueSize += cap(conn.seqs)
		queueUsage := cap(conn.seqs) - len(conn.seqs)
		totalQueueUsage += queueUsage
		if queueUsage > 0 {
			connUsage++
		}
	}
	return bindings.Status{
		CProto: bindings.StatusCProto{
			ConnPoolSize:   cap(binding.pool),
			ConnPoolUsage:  connUsage,
			ConnQueueSize:  totalQueueSize,
			ConnQueueUsage: totalQueueUsage,
		},
	}
}

func (binding *NetCProto) Finalize() error {
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
