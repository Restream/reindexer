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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
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

var emptyLogger bindings.NullLogger

func init() {
	rand.Seed(time.Now().UnixNano())
	bindings.RegisterBinding("cproto", new(NetCProto))
	bindings.RegisterBinding("cprotos", new(NetCProto))
	if runtime.GOOS != "windows" {
		bindings.RegisterBinding("ucproto", new(NetCProto))
	}
}

type LoggerOwner interface {
	GetLogger() bindings.Logger
}

type NetCProto struct {
	dsn                dsn
	pool               pool
	isServerChanged    int32
	onChangeCallback   func()
	getReplicationStat func(ctx context.Context) (*bindings.ReplicationStat, error)
	serverReindexerVer string
	verMtx             sync.RWMutex
	serverStartTime    int64
	retryAttempts      bindings.OptionRetryAttempts
	timeouts           bindings.OptionTimeouts
	connectOpts        bindings.OptionConnect
	compression        bindings.OptionCompression
	dedicatedThreads   bindings.OptionDedicatedThreads
	caps               bindings.BindingCapabilities
	appName            string
	eventsHandler      bindings.EventsHandler
	termCh             chan struct{}
	lock               sync.RWMutex
	logger             bindings.Logger
	logMtx             sync.RWMutex
	tls                bindings.OptionTLS
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

func (binding *NetCProto) Init(u []url.URL, eh bindings.EventsHandler, options ...interface{}) (err error) {
	connPoolSize := defConnPoolSize
	connPoolLBAlgorithm := defConnPoolLBAlgorithm
	binding.appName = defAppName
	binding.caps = *bindings.DefaultBindingCapabilities().
		WithQrIdleTimeouts(true).
		WithResultsWithShardIDs(true).
		WithIncarnationTags(true).
		WithFloatRank(true)

	for _, option := range options {
		switch v := option.(type) {
		case bindings.OptionPrometheusMetrics:
			// nothing
		case bindings.OptionOpenTelemetry:
			// nothing
		case bindings.OptionStrictJoinHandlers:
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
		case bindings.OptionReconnectionStrategy:
			binding.dsn.reconnectionStrategy = v.Strategy
			binding.dsn.allowUnknownNodes = v.AllowUnknownNodes
		case bindings.OptionTLS:
			binding.tls = v
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

	binding.eventsHandler = eh
	binding.dsn.connFactory = &connFactoryImpl{}
	binding.dsn.urls = u
	for i := 0; i < len(binding.dsn.urls); i++ {
		if binding.dsn.urls[i].Scheme == "ucproto" {
			addrs := strings.Split(binding.dsn.urls[i].Path, ":")
			if len(addrs) != 2 {
				return fmt.Errorf("rq: unexpected URL format for ucproto: '%s'. Expecting '<unix socket>:/<db name>", binding.dsn.urls[i].Path)
			}
			binding.dsn.urls[i].Host = addrs[0]
			binding.dsn.urls[i].Path = addrs[1]
		}
	}
	binding.connectDSN(context.Background(), connPoolSize, connPoolLBAlgorithm)
	binding.termCh = make(chan struct{})
	go binding.pinger()
	return
}

func (binding *NetCProto) setServerReindexerVer(serverReindexerVer string) {
	binding.verMtx.Lock()
	defer binding.verMtx.Unlock()
	binding.serverReindexerVer = serverReindexerVer
}

func (binding *NetCProto) DBMSVersion() (string, error) {
	binding.verMtx.RLock()
	defer binding.verMtx.RUnlock()
	if binding.serverReindexerVer != "" {
		return binding.serverReindexerVer, nil
	}

	return "", errors.New("login failed, DBMS version is not available")
}

func (binding *NetCProto) newPool(ctx context.Context, connPoolSize int, connPoolLBAlgorithm bindings.LoadBalancingAlgorithm) error {
	var wg sync.WaitGroup
	for _, conn := range binding.pool.sharedConns {
		conn.finalize()
	}
	if binding.pool.eventsConn != nil {
		binding.pool.eventsConn.finalize()
	}

	binding.pool = pool{
		sharedConns: make([]connection, connPoolSize),
		lbAlgorithm: connPoolLBAlgorithm,
	}

	connParams := binding.createConnParams()

	wg.Add(connPoolSize)
	for i := 0; i < connPoolSize; i++ {
		go func(binding *NetCProto, wg *sync.WaitGroup, i int) {
			defer wg.Done()

			conn, serverReindexerVer, serverStartTS, _ := binding.dsn.connFactory.newConnection(ctx, connParams, binding, nil)
			if serverStartTS > 0 {
				old := atomic.SwapInt64(&binding.serverStartTime, serverStartTS)
				if old != 0 && old != serverStartTS {
					atomic.StoreInt32(&binding.isServerChanged, 1)
				}
			}

			if serverReindexerVer != "" {
				binding.setServerReindexerVer(serverReindexerVer)
			}

			binding.pool.sharedConns[i] = conn
		}(binding, &wg, i)
	}
	wg.Wait()

	for _, conn := range binding.pool.sharedConns {
		if conn.hasError() {
			return conn.curError()
		}
	}

	return nil
}

func (binding *NetCProto) createConnParams() newConnParams {
	return newConnParams{dsn: binding.getActiveDSN(),
		loginTimeout:           binding.timeouts.LoginTimeout,
		requestTimeout:         binding.timeouts.RequestTimeout,
		createDBIfMissing:      binding.connectOpts.CreateDBIfMissing,
		appName:                binding.appName,
		enableCompression:      binding.compression.EnableCompression,
		requestDedicatedThread: binding.dedicatedThreads.DedicatedThreads,
		caps:                   binding.caps,
		tls:                    binding.tls}
}

func (binding *NetCProto) createEventConn(ctx context.Context, connParams newConnParams, eventsSubOptsJSON []byte) error {
	conn, serverReindexerVer, serverStartTS, err := binding.dsn.connFactory.newConnection(ctx, connParams, binding, binding.eventsHandler)
	if serverStartTS > 0 {
		old := atomic.SwapInt64(&binding.serverStartTime, serverStartTS)
		if old != 0 && old != serverStartTS {
			atomic.StoreInt32(&binding.isServerChanged, 1)
		}
	}

	if serverReindexerVer != "" {
		binding.setServerReindexerVer(serverReindexerVer)
	}

	if err != nil {
		return err
	}
	err = conn.rpcCallNoResults(ctx, cmdSubscribe, uint32(binding.timeouts.RequestTimeout/time.Second), 1, []byte{}, 0, eventsSubOptsJSON)
	if err != nil {
		return err
	}
	binding.pool.eventsConn = conn
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

func (binding *NetCProto) ModifyItem(ctx context.Context, namespace string, format int, data []byte, mode int, precepts []string, stateToken int) (bindings.RawBuffer, error) {

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

func (binding *NetCProto) EnumMeta(ctx context.Context, namespace string) ([]string, error) {
	buf, err := binding.rpcCall(ctx, opRd, cmdEnumMeta, namespace)
	if err != nil {
		return nil, err
	}
	defer buf.Free()

	keys := make([]string, len(buf.args))
	for i, item := range buf.args {
		keys[i] = string(item.([]byte))
	}
	return keys, nil
}

func (binding *NetCProto) PutMeta(ctx context.Context, namespace, key, data string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdPutMeta, namespace, key, data)
}

func (binding *NetCProto) GetMeta(ctx context.Context, namespace, key string) (bindings.RawBuffer, error) {
	return binding.rpcCall(ctx, opRd, cmdGetMeta, namespace, key)
}

func (binding *NetCProto) DeleteMeta(ctx context.Context, namespace, key string) error {
	return binding.rpcCallNoResults(ctx, opWr, cmdDeleteMeta, namespace, key)
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

	buf, err := binding.rpcCall(ctx, opRd, cmdExecSQL, query, flags, int32(fetchCount), ptVersions)
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

func (binding *NetCProto) DeleteQuery(ctx context.Context, data []byte) (bindings.RawBuffer, error) {
	return binding.rpcCall(ctx, opWr, cmdDeleteQuery, data)
}

func (binding *NetCProto) UpdateQuery(ctx context.Context, data []byte) (bindings.RawBuffer, error) {
	return binding.rpcCall(ctx, opWr, cmdUpdateQuery, data)
}

func (binding *NetCProto) OnChangeCallback(f func()) {
	binding.onChangeCallback = f
}

func (binding *NetCProto) GetReplicationStat(f func(ctx context.Context) (*bindings.ReplicationStat, error)) {
	binding.getReplicationStat = f
}

func (binding *NetCProto) EnableLogger(log bindings.Logger) {
	binding.logMtx.Lock()
	defer binding.logMtx.Unlock()
	binding.logger = log
}

func (binding *NetCProto) DisableLogger() {
	binding.logMtx.Lock()
	defer binding.logMtx.Unlock()
	binding.logger = nil
}

func (binding *NetCProto) GetLogger() bindings.Logger {
	binding.logMtx.RLock()
	defer binding.logMtx.RUnlock()
	if binding.logger != nil {
		return binding.logger
	}
	return &emptyLogger
}

func (binding *NetCProto) ReopenLogFiles() error {
	fmt.Println("cproto binding ReopenLogFiles method is dummy")
	return nil
}

func (binding *NetCProto) Status(ctx context.Context) bindings.Status {
	var totalQueueSize, totalQueueUsage, connUsage int
	var remoteAddr string
	conns := binding.getAllRegularConns()
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
		remoteAddr = conn.getConnection().RemoteAddr().String()
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
		conn.finalize()
	}
	return nil
}

func (binding *NetCProto) callSubscribe(ctx context.Context, eventsSubOptsJSON []byte) error {
	return binding.pool.eventsConn.rpcCallNoResults(ctx, cmdSubscribe, uint32(binding.timeouts.RequestTimeout/time.Second), 1, []byte{}, 0, eventsSubOptsJSON)
}

func (binding *NetCProto) tryReuseSubConn(ctx context.Context, eventsSubOptsJSON []byte) (bool, error) {
	binding.lock.RLock()
	defer binding.lock.RUnlock()
	if binding.pool.eventsConn != nil && !binding.pool.eventsConn.hasError() {
		return true, binding.callSubscribe(ctx, eventsSubOptsJSON)
	}
	return false, nil
}

func (binding *NetCProto) Subscribe(ctx context.Context, opts *bindings.SubscriptionOptions) error {
	eventsSubOptsJSON, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	reused, err := binding.tryReuseSubConn(ctx, eventsSubOptsJSON)
	if err != nil {
		return err
	}
	if reused {
		return nil
	}

	binding.lock.Lock()
	defer binding.lock.Unlock()
	if binding.pool.eventsConn != nil {
		if !binding.pool.eventsConn.hasError() {
			return binding.callSubscribe(ctx, eventsSubOptsJSON)
		}
		binding.pool.eventsConn.finalize()
	}
	return binding.createEventConn(ctx, binding.createConnParams(), eventsSubOptsJSON)
}

func (binding *NetCProto) unsubscribeImpl(context.Context) (connection, error) {
	binding.lock.Lock()
	defer binding.lock.Unlock()
	if binding.pool.eventsConn == nil {
		return nil, errors.New("not subscribed")
	}
	conn := binding.pool.eventsConn
	binding.pool.eventsConn = nil
	return conn, nil
}

func (binding *NetCProto) Unsubscribe(ctx context.Context) error {
	if conn, err := binding.unsubscribeImpl(ctx); err != nil {
		return err
	} else {
		return conn.finalize()
	}
}

func (binding *NetCProto) getAllRegularConns() []connection {
	binding.lock.RLock()
	defer binding.lock.RUnlock()
	return binding.pool.sharedConns
}

func (binding *NetCProto) getAllConns() []connection {
	binding.lock.RLock()
	defer binding.lock.RUnlock()
	conns := make([]connection, len(binding.pool.sharedConns))
	copy(conns, binding.pool.sharedConns)
	if binding.pool.eventsConn != nil {
		conns = append(conns, binding.pool.eventsConn)
	}
	return conns
}

func (binding *NetCProto) logMsg(level int, fmt string, msg ...interface{}) {
	binding.logMtx.RLock()
	defer binding.logMtx.RUnlock()
	if binding.logger != nil {
		binding.logger.Printf(level, fmt, msg)
	}
}

func (binding *NetCProto) getConnection(ctx context.Context) (conn connection, err error) {
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
						for _, conn := range binding.pool.sharedConns {
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
						conn = binding.pool.GetConnection()
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
	for i := 0; i < len(binding.dsn.urls); i++ {
		err := binding.newPool(ctx, connPoolSize, connPoolLBAlgorithm)
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
	err = binding.connectDSN(ctx, len(binding.pool.sharedConns), binding.pool.lbAlgorithm)
	if err != nil {
		time.Sleep(time.Duration(binding.dsn.connTry) * time.Millisecond)
	} else {
		binding.dsn.connTry = 0
	}

	return binding.pool.GetConnection(), err
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
	pingTimeoutSec := pingResponseTimeoutSec
	if uint32(binding.timeouts.RequestTimeout/time.Second) > pingTimeoutSec {
		pingTimeoutSec = uint32(binding.timeouts.RequestTimeout / time.Second)
	}
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
				seqs := conn.getSeqs()
				if cap(seqs)-len(seqs) > 0 {
					continue
				}
				wg.Add(1)
				conn.rpcCallAsync(context.TODO(), cmdPing, pingTimeoutSec, cmpl)
			}
			wg.Wait()
		}
	}
}
