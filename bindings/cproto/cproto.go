package cproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
)

const (
	defConnPoolSize  = 8
	pingerTimeoutSec = 60
	defAppName       = "Go-connector"

	opRd = 0
	opWr = 1
)

const (
	reconnectStrategyNext         = "next"
	reconnectStrategyRandom       = "random"
	reconnectStrategySynchronized = "synchronized"
	reconnectStrategyPrefferWrite = "preffer_write"
	reconnectStrategyReadOnly     = "read_only"
	reconnectStrategyPrefferRead  = "preffer_read"
)

const (
	clusterNodeStatusNone    = "none"
	clusterNodeStatusOnline  = "online"
	clusterNodeStatusOffline = "offline"

	clusterNodeRoleNone      = "none"
	clusterNodeRoleFollower  = "follower"
	clusterNodeRoleLeader    = "leader"
	clusterNodeRoleCandidate = "candidate"
)

const (
	replicationTypeCluster = "cluster"
	replicationTypeAsync   = "async_replication"
)

var logger Logger
var logMtx sync.RWMutex

func init() {
	rand.Seed(time.Now().UnixNano())
	bindings.RegisterBinding("cproto", new(NetCProto))
}

type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

type NetCProto struct {
	dsn                dsn
	pool               pool
	isServerChanged    int32
	onChangeCallback   func()
	getReplicationStat func(ctx context.Context) (*bindings.ReplicationStat, error)
	serverStartTime    int64
	retryAttempts      bindings.OptionRetryAttempts
	timeouts           bindings.OptionTimeouts
	connectOpts        bindings.OptionConnect
	compression        bindings.OptionCompression
	dedicatedThreads   bindings.OptionDedicatedThreads
	caps               bindings.BindingCapabilities
	appName            string
	termCh             chan struct{}
	lock               sync.RWMutex
}

type pool struct {
	conns []connection
	next  uint64
}

type dsn struct {
	connFactory          connFactory
	urls                 []url.URL
	connVersion          int
	connTry              int
	active               int
	reconnectionStrategy string
	allowUnknownNodes    bool
}

func (p *pool) get() connection {
	id := atomic.AddUint64(&p.next, 1)
	for id >= uint64(len(p.conns)) {
		if atomic.CompareAndSwapUint64(&p.next, id, 0) {
			id = 0
		} else {
			id = atomic.AddUint64(&p.next, 1)
		}
	}
	return p.conns[id]
}

func (binding *NetCProto) getActiveDSN() *url.URL {
	return &binding.dsn.urls[binding.dsn.active]
}

func (binding *NetCProto) nextDSN(ctx context.Context, strategy string, stat *bindings.ReplicationStat) error {
	switch strategy {
	case reconnectStrategyNext:
		binding.dsn.active = (binding.dsn.active + 1) % len(binding.dsn.urls)
	case reconnectStrategyRandom:
		binding.dsn.active = rand.Intn(len(binding.dsn.urls))
	case reconnectStrategySynchronized:
		if err := binding.processStrategySynchronized(ctx, stat); err != nil {
			return err
		}
	case reconnectStrategyReadOnly:
		if err := binding.processStrategyReadOnly(ctx, stat); err != nil {
			return err
		}
	case reconnectStrategyPrefferWrite:
		if err := binding.processStrategyWriteReq(ctx, stat); err != nil {
			return err
		}
	case reconnectStrategyPrefferRead:
		if err := binding.processStrategyPrefferRead(ctx, stat); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown dsn connect strategy: %s", strategy)
	}
	binding.dsn.connVersion++
	return nil
}

func (binding *NetCProto) processStrategyPrefferRead(ctx context.Context, stat *bindings.ReplicationStat) error {
	switch stat.Type {
	case replicationTypeCluster:
		if err := binding.processStrategyReadOnly(ctx, stat); err != nil {
			return err
		}
	case replicationTypeAsync:
		var leaderDSN *url.URL
		var err error

		for _, node := range stat.Nodes {
			if node.Role == clusterNodeRoleLeader {
				leaderDSN, err = url.Parse(node.DSN)
				if err != nil {
					return fmt.Errorf("can't parse cluster dsn %s, err: %s", node.DSN, err)
				}
				break
			}
		}
		if err := binding.setClusterDSN(leaderDSN); err != nil {
			return err
		}
	}

	return nil
}

func (binding *NetCProto) processStrategyWriteReq(ctx context.Context, stat *bindings.ReplicationStat) error {
	var newClusterDSN *url.URL
	var err error
	for _, node := range stat.Nodes {
		if node.Role == clusterNodeRoleLeader && node.Status == clusterNodeStatusOnline {
			newClusterDSN, err = url.Parse(node.DSN)
			if err != nil {
				return fmt.Errorf("can't parse cluster dsn %s, err: %s", node.DSN, err)
			}
			break
		}
	}

	if err := binding.setClusterDSN(newClusterDSN); err != nil {
		return err
	}

	return nil
}

func (binding *NetCProto) processStrategyReadOnly(ctx context.Context, stat *bindings.ReplicationStat) error {
	var leaderDSN *url.URL
	var followersDSN []*url.URL
	var err error
	for _, node := range stat.Nodes {
		if node.Status != clusterNodeStatusOnline {
			continue
		}
		if node.Role == clusterNodeRoleLeader {
			leaderDSN, err = url.Parse(node.DSN)
			if err != nil {
				return fmt.Errorf("can't parse cluster dsn %s, err: %s", node.DSN, err)
			}
		} else if node.Role == clusterNodeRoleFollower {
			dsn, err := url.Parse(node.DSN)
			if err != nil {
				return fmt.Errorf("can't parse cluster dsn %s, err: %s", node.DSN, err)
			}
			followersDSN = append(followersDSN, dsn)
		}
	}

	var newClusterDSN *url.URL
	if len(followersDSN) > 0 {
		newClusterDSN = followersDSN[rand.Intn(len(followersDSN))]
	} else if leaderDSN != nil {
		newClusterDSN = leaderDSN
	}

	if err := binding.setClusterDSN(newClusterDSN); err != nil {
		return err
	}

	return nil
}

func (binding *NetCProto) processStrategySynchronized(ctx context.Context, stat *bindings.ReplicationStat) error {
	clusterDSNs := make([]*url.URL, 0, len(stat.Nodes))
	for _, node := range stat.Nodes {
		if node.IsSynchronized && node.Status == clusterNodeStatusOnline {
			dsn, err := url.Parse(node.DSN)
			if err != nil {
				return fmt.Errorf("can't parse cluster dsn %s, err: %s", node.DSN, err)
			}
			clusterDSNs = append(clusterDSNs, dsn)
		}
	}
	if len(clusterDSNs) == 0 {
		return errors.New("can't find cluster node for reconnect")
	}
	newClusterDSN := clusterDSNs[rand.Intn(len(clusterDSNs))]

	if err := binding.setClusterDSN(newClusterDSN); err != nil {
		return err
	}

	return nil
}

func (binding *NetCProto) setClusterDSN(dsn *url.URL) error {
	if dsn == nil {
		return errors.New("can't find cluster node for reconnect")
	}

	var dsnExist bool
	for i, d := range binding.dsn.urls {
		if d.Host == dsn.Host {
			binding.dsn.active = i
			dsnExist = true
			break
		}
	}
	if !dsnExist {
		if binding.dsn.allowUnknownNodes {
			binding.dsn.urls = append(binding.dsn.urls, *dsn)
			binding.dsn.active = len(binding.dsn.urls) - 1
		} else {
			binding.dsn.active = (binding.dsn.active + 1) % len(binding.dsn.urls)
		}
	}

	return nil
}

func (binding *NetCProto) GetDSNs() []url.URL {
	return binding.dsn.urls
}

func (binding *NetCProto) Init(u []url.URL, options ...interface{}) (err error) {
	connPoolSize := defConnPoolSize
	binding.appName = defAppName
	binding.caps = *bindings.DefaultBindingCapabilities().WithQrIdleTimeouts(true).WithResultsWithShardIDs(true)

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
		case bindings.OptionAppName:
			binding.appName = v.AppName
		case bindings.OptionDedicatedThreads:
			binding.dedicatedThreads = v
		case bindings.OptionReconnectionStrategy:
			binding.dsn.reconnectionStrategy = v.Strategy
			binding.dsn.allowUnknownNodes = v.AllowUnknownNodes
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

	binding.dsn.connFactory = &connFactoryImpl{}
	binding.dsn.urls = u
	binding.connectDSN(context.Background(), connPoolSize)
	binding.termCh = make(chan struct{})
	go binding.pinger()
	return
}

func (binding *NetCProto) newPool(ctx context.Context, connPoolSize int) error {
	var wg sync.WaitGroup
	for _, conn := range binding.pool.conns {
		conn.finalize()
	}
	binding.pool = pool{
		conns: make([]connection, connPoolSize),
	}
	wg.Add(connPoolSize)
	for i := 0; i < connPoolSize; i++ {
		go func(binding *NetCProto, wg *sync.WaitGroup, i int) {
			defer wg.Done()
			conn, serverStartTS, _ := binding.dsn.connFactory.newConnection(
				ctx,
				newConnParams{
					dsn:                    binding.getActiveDSN(),
					loginTimeout:           binding.timeouts.LoginTimeout,
					requestTimeout:         binding.timeouts.RequestTimeout,
					createDBIfMissing:      binding.connectOpts.CreateDBIfMissing,
					appName:                binding.appName,
					enableCompression:      binding.compression.EnableCompression,
					requestDedicatedThread: binding.dedicatedThreads.DedicatedThreads,
					caps:                   binding.caps,
				},
			)

			if serverStartTS > 0 {
				old := atomic.SwapInt64(&binding.serverStartTime, serverStartTS)
				if old != 0 && old != serverStartTS {
					atomic.StoreInt32(&binding.isServerChanged, 1)
				}
			}

			binding.pool.conns[i] = conn
		}(binding, &wg, i)
	}
	wg.Wait()
	for _, conn := range binding.pool.conns {
		if conn.hasError() {
			return conn.curError()
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

func (binding *NetCProto) GetReplicationStat(f func(ctx context.Context) (*bindings.ReplicationStat, error)) {
	binding.getReplicationStat = f
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
		seqs := conn.getSeqs()
		totalQueueSize += cap(seqs)
		queueUsage := cap(seqs) - len(seqs)
		totalQueueUsage += queueUsage
		if queueUsage > 0 {
			connUsage++
		}
		remoteAddr = conn.getConn().RemoteAddr().String()
	}

	var err error
	if activeConns == 0 {
		_, err = binding.getConn(ctx)
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
		conn.finalize()
	}
	return nil
}

func (binding *NetCProto) getAllConns() []connection {
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

func (binding *NetCProto) getConn(ctx context.Context) (conn connection, err error) {
	for {
		binding.lock.RLock()
		conn = binding.pool.get()
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

				if binding.dsn.reconnectionStrategy != reconnectStrategyNext && binding.dsn.reconnectionStrategy != "" {
					var stat *bindings.ReplicationStat
					if binding.dsn.reconnectionStrategy != reconnectStrategyRandom {
						stat, err = binding.getReplicationStat(ctx)
						if err != nil {
							return nil, err
						}
					}
					binding.lock.Lock()
					if currVersion+1 == binding.dsn.connVersion {
						for _, conn := range binding.pool.conns {
							conn.finalize()
						}
						binding.logMsg(3, "rq: reconnecting with strategy: %s \n", binding.dsn.reconnectionStrategy)
						if err := binding.nextDSN(ctx, binding.dsn.reconnectionStrategy, stat); err != nil {
							binding.lock.Unlock()
							return nil, err
						}
						conn, err = binding.reconnect(ctx)
						binding.lock.Unlock()
						if err != nil {
							return nil, err
						}
					} else {
						conn = binding.pool.get()
						binding.lock.Unlock()
						if conn.hasError() {
							continue
						}
					}
				}

				if atomic.CompareAndSwapInt32(&binding.isServerChanged, 1, 0) && binding.onChangeCallback != nil {
					binding.onChangeCallback()
				}
				return conn, nil
			} else {
				conn = binding.pool.get()
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

func (binding *NetCProto) connectDSN(ctx context.Context, connPoolSize int) error {
	errWrap := errors.New("failed to connect with provided dsn")
	for i := 0; i < len(binding.dsn.urls); i++ {
		err := binding.newPool(ctx, connPoolSize)
		if err != nil {
			if errDSN := binding.nextDSN(ctx, reconnectStrategyNext, nil); errDSN != nil {
				errWrap = fmt.Errorf("%s; %s", errWrap, errDSN)
				continue
			}
			errWrap = fmt.Errorf("%s; %s", errWrap, err)
			continue
		}

		errWrap = nil
		break
	}

	return errWrap
}

func (binding *NetCProto) reconnect(ctx context.Context) (conn connection, err error) {
	if binding.dsn.connTry < 100 {
		binding.dsn.connTry++
	}
	err = binding.connectDSN(ctx, len(binding.pool.conns))
	if err != nil {
		time.Sleep(time.Duration(binding.dsn.connTry) * time.Millisecond)
	} else {
		binding.dsn.connTry = 0
	}

	return binding.pool.get(), err
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
		var conn connection
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
	if buf != nil {
		buf.Free()
	}
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
			conns := binding.getAllConns()
			for _, conn := range conns {
				if conn.hasError() {
					continue
				}
				if conn.lastReadTime().Add(timeout).Before(now) {
					buf, _ := conn.rpcCall(context.TODO(), cmdPing, uint32(binding.timeouts.RequestTimeout/time.Second))
					buf.Free()
				}
			}
		}
	}
}
