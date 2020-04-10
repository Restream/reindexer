package cproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"sync"
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
	dsn              dsn
	pool             chan *connection
	onChangeCallback func()
	serverStartTime  int64
	retryAttempts    bindings.OptionRetryAttempts
	timeouts         bindings.OptionTimeouts
	connectOpts      bindings.OptionConnect
	compression      bindings.OptionCompression
	termCh           chan struct{}
	lock             sync.RWMutex
}

type dsn struct {
	url         []url.URL
	connVersion int
	active      int
}

func (binding *NetCProto) getActiveDSN() *url.URL {
	return &binding.dsn.url[binding.dsn.active]
}

func (binding *NetCProto) nextDSN() {
	binding.dsn.active = (binding.dsn.active + 1) % len(binding.dsn.url)
	binding.dsn.connVersion++
}

func (binding *NetCProto) Init(u []url.URL, options ...interface{}) (err error) {
	connPoolSize := defConnPoolSize

	for _, option := range options {
		switch v := option.(type) {
		case bindings.OptionConnPoolSize:
			connPoolSize = v.ConnPoolSize
		case bindings.OptionRetryAttempts:
			binding.retryAttempts = v
		case bindings.OptionTimeouts:
			binding.timeouts = v
		case bindings.OptionConnect:
			binding.connectOpts = v
		case bindings.OptionCompression:
			binding.compression = v
		default:
			fmt.Printf("Unknown cproto option: %#v\n", option)
		}
	}

	if binding.timeouts.RequestTimeout/time.Second != 0 && binding.timeouts.LoginTimeout > binding.timeouts.RequestTimeout {
		binding.timeouts.RequestTimeout = binding.timeouts.LoginTimeout
	}

	if binding.retryAttempts.Read < 0 {
		binding.retryAttempts.Read = 0
	}
	if binding.retryAttempts.Write < 0 {
		binding.retryAttempts.Write = 0
	}

	binding.dsn.url = u
	binding.newPool(context.Background(), connPoolSize)
	binding.termCh = make(chan struct{})
	go binding.pinger()
	return
}

func (binding *NetCProto) newPool(ctx context.Context, connPoolSize int) error {
	var wg sync.WaitGroup
	binding.pool = make(chan *connection, connPoolSize)
	wg.Add(connPoolSize)
	for i := 0; i < connPoolSize; i++ {
		go func(binding *NetCProto, wg *sync.WaitGroup) {
			defer wg.Done()
			conn, _ := newConnection(ctx, binding)
			binding.pool <- conn
		}(binding, &wg)
	}
	wg.Wait()
	for i := 0; i < connPoolSize; i++ {
		conn := <-binding.pool
		if conn.err != nil {
			return conn.err
		}
		binding.pool <- conn
	}

	return nil
}

func (binding *NetCProto) Clone() bindings.RawBinding {
	return &NetCProto{}
}

func (binding *NetCProto) Ping(ctx context.Context) error {
	return binding.rpcCallNoResults(ctx, opRd, cmdPing)
}

func (binding *NetCProto) BeginTx(ctx context.Context, namespace string) (txCtx bindings.TxCtx, err error) {
	buf, err := binding.rpcCall(ctx, opWr, cmdStartTransaction, namespace)
	if err != nil {
		return
	}

	txCtx.Result = buf
	if len(buf.args) > 0 {
		txCtx.Id = uint64(buf.args[0].(int64))
	}
	return
}

func (binding *NetCProto) CommitTx(txCtx *bindings.TxCtx) (bindings.RawBuffer, error) {
	return txCtx.Result.(*NetBuffer).conn.rpcCall(txCtx.UserCtx, cmdCommitTx, uint32(binding.timeouts.RequestTimeout/time.Second), int64(txCtx.Id))
}

func (binding *NetCProto) RollbackTx(txCtx *bindings.TxCtx) error {
	if txCtx.Result == nil {
		return nil
	}
	return txCtx.Result.(*NetBuffer).conn.rpcCallNoResults(txCtx.UserCtx, cmdRollbackTx, uint32(binding.timeouts.RequestTimeout/time.Second), int64(txCtx.Id))
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
	return netBuffer.conn.rpcCallNoResults(txCtx.UserCtx, cmdAddTxItem, uint32(binding.timeouts.RequestTimeout/time.Second), format, data, mode, packedPercepts, stateToken, int64(txCtx.Id))
}

func (binding *NetCProto) ModifyItemTxAsync(txCtx *bindings.TxCtx, format int, data []byte, mode int, precepts []string, stateToken int, cmpl bindings.RawCompletion) {
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
	netBuffer.conn.rpcCallAsync(txCtx.UserCtx, cmdAddTxItem, uint32(binding.timeouts.RequestTimeout/time.Second), cmpl, format, data, mode, packedPercepts, stateToken, int64(txCtx.Id))
}

func (binding *NetCProto) DeleteQueryTx(txCtx *bindings.TxCtx, rawQuery []byte) error {
	netBuffer := txCtx.Result.(*NetBuffer)
	return netBuffer.conn.rpcCallNoResults(txCtx.UserCtx, cmdDeleteQueryTx, uint32(binding.timeouts.RequestTimeout/time.Second), rawQuery, int64(txCtx.Id))

}

func (binding *NetCProto) UpdateQueryTx(txCtx *bindings.TxCtx, rawQuery []byte) error {
	netBuffer := txCtx.Result.(*NetBuffer)
	return netBuffer.conn.rpcCallNoResults(txCtx.UserCtx, cmdUpdateQueryTx, uint32(binding.timeouts.RequestTimeout/time.Second), rawQuery, int64(txCtx.Id))
}

func (binding *NetCProto) ModifyItem(ctx context.Context, nsHash int, namespace string, format int, data []byte, mode int, precepts []string, stateToken int) (bindings.RawBuffer, error) {

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

	return binding.rpcCall(ctx, opWr, cmdModifyItem, namespace, format, data, mode, packedPercepts, stateToken, 0)
}

func (binding *NetCProto) OpenNamespace(ctx context.Context, namespace string, enableStorage, dropOnFormatError bool) error {
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

	return binding.rpcCallNoResults(ctx, opWr, cmdOpenNamespace, bNamespaceDef)
}

func (binding *NetCProto) CloseNamespace(ctx context.Context, namespace string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdCloseNamespace, namespace)
}

func (binding *NetCProto) DropNamespace(ctx context.Context, namespace string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdDropNamespace, namespace)
}

func (binding *NetCProto) TruncateNamespace(ctx context.Context, namespace string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdTruncateNamespace, namespace)
}

func (binding *NetCProto) RenameNamespace(ctx context.Context, srcNamespace string, dstNamespace string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdRenameNamespace, srcNamespace, dstNamespace)
}

func (binding *NetCProto) AddIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	return binding.rpcCallNoResults(ctx, opWr, cmdAddIndex, namespace, bIndexDef)
}

func (binding *NetCProto) UpdateIndex(ctx context.Context, namespace string, indexDef bindings.IndexDef) error {
	bIndexDef, err := json.Marshal(indexDef)
	if err != nil {
		return err
	}

	return binding.rpcCallNoResults(ctx, opWr, cmdUpdateIndex, namespace, bIndexDef)
}

func (binding *NetCProto) DropIndex(ctx context.Context, namespace, index string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdDropIndex, namespace, index)
}

func (binding *NetCProto) PutMeta(ctx context.Context, namespace, key, data string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdPutMeta, namespace, key, data)
}

func (binding *NetCProto) GetMeta(ctx context.Context, namespace, key string) (bindings.RawBuffer, error) {
	return binding.rpcCall(ctx, opRd, cmdGetMeta, namespace, key)
}

func (binding *NetCProto) Select(ctx context.Context, query string, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	flags := 0
	if asJson {
		flags |= bindings.ResultsJson
	} else {
		flags |= bindings.ResultsCJson | bindings.ResultsWithPayloadTypes | bindings.ResultsWithItemID
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(ctx, opRd, cmdSelectSQL, query, flags, int32(fetchCount), ptVersions)
	if buf != nil {
		buf.reqID = buf.args[1].(int)
	}
	return buf, err
}

func (binding *NetCProto) SelectQuery(ctx context.Context, data []byte, asJson bool, ptVersions []int32, fetchCount int) (bindings.RawBuffer, error) {
	flags := 0
	if asJson {
		flags |= bindings.ResultsJson
	} else {
		flags |= bindings.ResultsCJson | bindings.ResultsWithPayloadTypes | bindings.ResultsWithItemID
	}

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(ctx, opRd, cmdSelect, data, flags, int32(fetchCount), ptVersions)
	if buf != nil {
		buf.reqID = buf.args[1].(int)
	}
	return buf, err
}

func (binding *NetCProto) DeleteQuery(ctx context.Context, nsHash int, data []byte) (bindings.RawBuffer, error) {
	return binding.rpcCall(ctx, opWr, cmdDeleteQuery, data)
}

func (binding *NetCProto) UpdateQuery(ctx context.Context, nsHash int, data []byte) (bindings.RawBuffer, error) {
	return binding.rpcCall(ctx, opWr, cmdUpdateQuery, data)
}

func (binding *NetCProto) Commit(ctx context.Context, namespace string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdCommit, namespace)
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

func (binding *NetCProto) EnableStorage(ctx context.Context, path string) error {
	fmt.Println("cproto binding EnableStorage method is dummy")
	return nil
}

func (binding *NetCProto) Status(ctx context.Context) bindings.Status {
	var totalQueueSize, totalQueueUsage, connUsage int
	binding.lock.RLock()
	poolCap := cap(binding.pool)
	binding.lock.RUnlock()
	var remoteAddr string
	for i := 0; i < poolCap; i++ {
		conn, _ := binding.getConn(ctx)
		totalQueueSize += cap(conn.seqs)
		queueUsage := cap(conn.seqs) - len(conn.seqs)
		totalQueueUsage += queueUsage
		if queueUsage > 0 {
			connUsage++
		}
		remoteAddr = conn.conn.RemoteAddr().String()
	}

	return bindings.Status{
		CProto: bindings.StatusCProto{
			ConnPoolSize:   poolCap,
			ConnPoolUsage:  connUsage,
			ConnQueueSize:  totalQueueSize,
			ConnQueueUsage: totalQueueUsage,
			ConnAddr:       remoteAddr,
		},
	}
}

func (binding *NetCProto) Finalize() error {
	if binding.termCh != nil {
		close(binding.termCh)
	}
	binding.lock.RLock()
	for i := 0; i < cap(binding.pool); i++ {
		conn := <-binding.pool
		conn.Finalize()
	}
	binding.lock.RUnlock()

	return nil
}

func (binding *NetCProto) getConn(ctx context.Context) (conn *connection, err error) {
	binding.lock.RLock()
	select {
	case conn = <-binding.pool:
		if conn.hasError() {
			currVersion := binding.dsn.connVersion
			binding.lock.RUnlock()
			binding.lock.Lock()
			if currVersion == binding.dsn.connVersion {
				conn, err = binding.reconnect(ctx)
				binding.lock.Unlock()
				if err != nil {
					return nil, err
				}
				if conn.isServerChanged {
					binding.onChangeCallback()
				}
			} else {
				binding.lock.Unlock()
				conn, err = binding.getConn(ctx)
				if err != nil {
					return nil, err
				}
			}
		} else {
			binding.lock.RUnlock()
		}
		binding.pool <- conn
	case <-ctx.Done():
		binding.lock.RUnlock()
		err = ctx.Err()
	}
	return
}

func (binding *NetCProto) reconnect(ctx context.Context) (conn *connection, err error) {
	errWrap := errors.New("failed to connect with provided dsn")
	for i := 0; i < len(binding.dsn.url); i++ {
		binding.nextDSN()
		err = binding.newPool(ctx, cap(binding.pool))
		if err != nil {
			errWrap = fmt.Errorf("%s; %s", errWrap, err)
			continue
		}

		errWrap = nil
		conn = <-binding.pool

		break
	}

	return conn, errWrap
}

func (binding *NetCProto) rpcCall(ctx context.Context, op int, cmd int, args ...interface{}) (buf *NetBuffer, err error) {
	var attempts int
	switch op {
	case opRd:
		attempts = binding.retryAttempts.Read + 1
	default:
		attempts = binding.retryAttempts.Write + 1
	}
	for i := 0; i < attempts; i++ {
		var conn *connection
		if conn, err = binding.getConn(ctx); err == nil {
			if buf, err = conn.rpcCall(ctx, cmd, uint32(binding.timeouts.RequestTimeout/time.Second), args...); err == nil {
				return
			}
		}
		switch err.(type) {
		case net.Error, *net.OpError:
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			case <-time.After(time.Second * time.Duration(i)):
			}
		default:
			return
		}
	}
	return
}

func (binding *NetCProto) rpcCallNoResults(ctx context.Context, op int, cmd int, args ...interface{}) error {
	buf, err := binding.rpcCall(ctx, op, cmd, args...)
	buf.Free()
	return err
}

func (binding *NetCProto) pinger() {
	timeout := time.Second
	ticker := time.NewTicker(timeout)
	var ticksCount uint16
	for now := range ticker.C {
		ticksCount++
		select {
		case <-binding.termCh:
			return
		default:
		}
		if ticksCount == pingerTimeoutSec {
			ticksCount = 0
			binding.lock.RLock()
			poolCap := cap(binding.pool)
			binding.lock.RUnlock()
			for i := 0; i < poolCap; i++ {
				conn, err := binding.getConn(context.TODO())
				if err != nil || conn.hasError() {
					continue
				}
				if conn.lastReadTime().Add(timeout).Before(now) {
					buf, _ := conn.rpcCall(context.TODO(), cmdPing, uint32(binding.timeouts.RequestTimeout*time.Second))
					buf.Free()
				}
			}
		}
	}
}
