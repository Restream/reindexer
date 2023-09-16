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
	"sync/atomic"
	"time"

	"github.com/restream/reindexer/v3/bindings"
	"github.com/restream/reindexer/v3/cjson"
)

const (
	defConnPoolSize        = 8
	defConnPoolLBAlgorithm = bindings.LBPowerOfTwoChoices
	pingerTimeoutSec       = uint32(60)
	pingResponseTimeoutSec = uint32(20)
	defAppName             = "Go-connector"

	opRd = 0
	opWr = 1
)

var logger Logger
var logMtx sync.RWMutex

func init() {
	bindings.RegisterBinding("cproto", new(NetCProto))
}

type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

type NetCProto struct {
	dsn              dsn
	pool             pool
	isServerChanged  int32
	onChangeCallback func()
	serverStartTime  int64
	retryAttempts    bindings.OptionRetryAttempts
	timeouts         bindings.OptionTimeouts
	connectOpts      bindings.OptionConnect
	compression      bindings.OptionCompression
	dedicatedThreads bindings.OptionDedicatedThreads
	appName          string
	termCh           chan struct{}
	lock             sync.RWMutex
}

type dsn struct {
	url         []url.URL
	connVersion int
	connTry     int
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
	connPoolLBAlgorithm := defConnPoolLBAlgorithm
	binding.appName = defAppName

	for _, option := range options {
		switch v := option.(type) {
		case bindings.OptionPrometheusMetrics:
			// nothing
		case bindings.OptionOpenTelemetry:
			// nothing
		case bindings.OptionConnPoolSize:
			connPoolSize = v.ConnPoolSize

		case bindings.OptionConnPoolLoadBalancing:
			connPoolLBAlgorithm = v.Algorithm

		case bindings.OptionRetryAttempts:
			binding.retryAttempts = v

		case bindings.OptionTimeouts:
			binding.timeouts = v

		case bindings.OptionConnect:
			binding.connectOpts = v

		case bindings.OptionCompression:
			binding.compression = v

		case bindings.OptionAppName:
			binding.appName = v.AppName

		case bindings.OptionDedicatedThreads:
			binding.dedicatedThreads = v

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
	binding.connectDSN(context.Background(), connPoolSize, connPoolLBAlgorithm)
	binding.termCh = make(chan struct{})
	go binding.pinger()
	return
}

func (binding *NetCProto) newPool(ctx context.Context, connPoolSize int, connPoolLBAlgorithm bindings.LoadBalancingAlgorithm) error {
	var wg sync.WaitGroup
	for _, conn := range binding.pool.conns {
		conn.Close()
	}

	binding.pool = pool{
		conns:       make([]*connection, connPoolSize),
		lbAlgorithm: connPoolLBAlgorithm,
	}

	wg.Add(connPoolSize)
	for i := 0; i < connPoolSize; i++ {
		go func(binding *NetCProto, wg *sync.WaitGroup, i int) {
			defer wg.Done()

			conn, _ := newConnection(ctx, binding)
			binding.pool.conns[i] = conn
		}(binding, &wg, i)
	}
	wg.Wait()

	for _, conn := range binding.pool.conns {
		if conn.err != nil {
			return conn.err
		}
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
	return txCtx.Result.(*NetBuffer).conn.rpcCallNoResults(context.TODO(), cmdRollbackTx, uint32(binding.timeouts.RequestTimeout/time.Second), int64(txCtx.Id))
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

func (binding *NetCProto) SetSchema(ctx context.Context, namespace string, schema bindings.SchemaDef) error {
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	return binding.rpcCallNoResults(ctx, opWr, cmdSetSchema, namespace, schemaJSON)
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
	flags |= bindings.ResultsSupportIdleTimeout

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(ctx, opRd, cmdSelectSQL, query, flags, int32(fetchCount), ptVersions)
	if buf != nil {
		buf.reqID = buf.args[1].(int)
		if len(buf.args) > 2 {
			buf.uid = buf.args[2].(int64)
		}
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
	flags |= bindings.ResultsSupportIdleTimeout

	if fetchCount <= 0 {
		fetchCount = math.MaxInt32
	}

	buf, err := binding.rpcCall(ctx, opRd, cmdSelect, data, flags, int32(fetchCount), ptVersions)
	if buf != nil {
		buf.reqID = buf.args[1].(int)
		if len(buf.args) > 2 {
			buf.uid = buf.args[2].(int64)
		}
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
	logMtx.Lock()
	defer logMtx.Unlock()
	logger = log
}

func (binding *NetCProto) DisableLogger() {
	logMtx.Lock()
	defer logMtx.Unlock()
	logger = nil
}
func (binding *NetCProto) ReopenLogFiles() error {
	fmt.Println("cproto binding ReopenLogFiles method is dummy")
	return nil
}

func (binding *NetCProto) EnableStorage(ctx context.Context, path string) error {
	fmt.Println("cproto binding EnableStorage method is dummy")
	return nil
}

func (binding *NetCProto) Status(ctx context.Context) bindings.Status {
	var totalQueueSize, totalQueueUsage, connUsage int
	var remoteAddr string
	conns := binding.getAllConns()
	activeConns := 0
	for _, conn := range conns {
		if conn.hasError() {
			continue
		}
		activeConns++
		totalQueueSize += cap(conn.seqs)
		queueUsage := cap(conn.seqs) - len(conn.seqs)
		totalQueueUsage += queueUsage
		if queueUsage > 0 {
			connUsage++
		}
		remoteAddr = conn.conn.RemoteAddr().String()
	}

	var err error
	if activeConns == 0 {
		_, err = binding.getConnection(ctx)
	}

	return bindings.Status{
		CProto: bindings.StatusCProto{
			ConnPoolSize:   len(conns),
			ConnPoolUsage:  connUsage,
			ConnQueueSize:  totalQueueSize,
			ConnQueueUsage: totalQueueUsage,
			ConnAddr:       remoteAddr,
		},
		Err: err,
	}
}

func (binding *NetCProto) Finalize() error {
	if binding.termCh != nil {
		close(binding.termCh)
	}
	conns := binding.getAllConns()
	for _, conn := range conns {
		conn.Finalize()
	}
	return nil
}

func (binding *NetCProto) getAllConns() []*connection {
	binding.lock.RLock()
	defer binding.lock.RUnlock()
	return binding.pool.conns
}

func (binding *NetCProto) logMsg(level int, fmt string, msg ...interface{}) {
	logMtx.RLock()
	defer logMtx.RUnlock()
	if logger != nil {
		logger.Printf(level, fmt, msg)
	}
}

func (binding *NetCProto) getConnection(ctx context.Context) (conn *connection, err error) {
	for {
		binding.lock.RLock()
		conn = binding.pool.GetConnection()
		currVersion := binding.dsn.connVersion
		binding.lock.RUnlock()

		if conn.hasError() {
			binding.lock.Lock()
			select {
			case <-ctx.Done():
				binding.lock.Unlock()
				return nil, ctx.Err()
			default:
			}
			if currVersion == binding.dsn.connVersion {
				binding.logMsg(3, "rq: reconnecting after err: %s \n", conn.curError().Error())
				conn, err = binding.reconnect(ctx)
				binding.lock.Unlock()
				if err != nil {
					return nil, err
				}

				if atomic.CompareAndSwapInt32(&binding.isServerChanged, 1, 0) && binding.onChangeCallback != nil {
					binding.onChangeCallback()
				}
				return conn, nil
			} else {
				conn = binding.pool.GetConnection()
				binding.lock.Unlock()
				if conn.hasError() {
					continue
				}
				return conn, nil
			}
		}

		return conn, nil
	}
}

func (binding *NetCProto) connectDSN(ctx context.Context, connPoolSize int, connPoolLBAlgorithm bindings.LoadBalancingAlgorithm) error {
	errWrap := errors.New("failed to connect with provided dsn")
	for i := 0; i < len(binding.dsn.url); i++ {
		err := binding.newPool(ctx, connPoolSize, connPoolLBAlgorithm)
		if err != nil {
			binding.nextDSN()
			errWrap = fmt.Errorf("%s; %s", errWrap, err)
			continue
		}

		errWrap = nil
		break
	}

	return errWrap
}

func (binding *NetCProto) reconnect(ctx context.Context) (conn *connection, err error) {
	if binding.dsn.connTry < 100 {
		binding.dsn.connTry++
	}

	err = binding.connectDSN(ctx, len(binding.pool.conns), binding.pool.lbAlgorithm)
	if err != nil {
		time.Sleep(time.Duration(binding.dsn.connTry) * time.Millisecond)
	} else {
		binding.dsn.connTry = 0
	}
	conn = binding.pool.GetConnection()

	return conn, err
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
		if conn, err = binding.getConnection(ctx); err == nil {
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
	if buf != nil {
		buf.Free()
	}
	return err
}

func (binding *NetCProto) pinger() {
	timeout := time.Second
	ticker := time.NewTicker(timeout)
	var ticksCount uint32
	for now := range ticker.C {
		ticksCount++
		select {
		case <-binding.termCh:
			return
		default:
		}
		if ticksCount == pingerTimeoutSec {
			ticksCount = 0
			conns := binding.getAllConns()
			var wg sync.WaitGroup
			cmpl := func(buf bindings.RawBuffer, err error) {
				wg.Done()
				if buf != nil {
					buf.Free()
				}
			}
			for _, conn := range conns {
				if conn.hasError() {
					continue
				}
				if !conn.lastReadTime().Add(timeout).Before(now) {
					continue
				}
				if cap(conn.seqs)-len(conn.seqs) > 0 {
					continue
				}
				timeout := pingResponseTimeoutSec
				if uint32(binding.timeouts.RequestTimeout) > timeout {
					timeout = uint32(binding.timeouts.RequestTimeout)
				}

				wg.Add(1)
				conn.rpcCallAsync(context.TODO(), cmdPing, uint32(timeout), cmpl)
			}
			wg.Wait()
		}
	}
}
