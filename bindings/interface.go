package bindings

import (
	"context"
	"net/url"
	"time"

	"github.com/restream/reindexer/bindings/builtinserver/config"
	"github.com/restream/reindexer/jsonschema"
)

type IndexDef struct {
	Name        string      `json:"name"`
	JSONPaths   []string    `json:"json_paths"`
	IndexType   string      `json:"index_type"`
	FieldType   string      `json:"field_type"`
	IsPK        bool        `json:"is_pk"`
	IsArray     bool        `json:"is_array"`
	IsDense     bool        `json:"is_dense"`
	IsSparse    bool        `json:"is_sparse"`
	CollateMode string      `json:"collate_mode"`
	SortOrder   string      `json:"sort_order_letters"`
	ExpireAfter int         `json:"expire_after"`
	Config      interface{} `json:"config"`
	RTreeType   string      `json:"rtree_type"`
}

type FieldDef struct {
	JSONPath string `json:"json_path"`
	Type     string `json:"type"`
	IsArray  bool   `json:"is_array"`
}

type SchemaDef jsonschema.Schema

type StorageOpts struct {
	EnableStorage     bool `json:"enabled"`
	DropOnFormatError bool `json:"drop_on_file_format_error"`
	CreateIfMissing   bool `json:"create_if_missing"`
}

type NamespaceDef struct {
	Namespace   string      `json:"name"`
	StorageOpts StorageOpts `json:"storage"`
}

type StorageOptions uint16

func (so *StorageOptions) Enabled(value bool) *StorageOptions {
	if value {
		*so |= StorageOptions(StorageOptEnabled | StorageOptCreateIfMissing)
	} else {
		*so &= ^StorageOptions(StorageOptEnabled)
	}
	return so
}

func (so *StorageOptions) DropOnFileFormatError(value bool) *StorageOptions {
	if value {
		*so |= StorageOptions(StorageOptDropOnFileFormatError)
	} else {
		*so &= ^StorageOptions(StorageOptDropOnFileFormatError)
	}
	return so
}

type ConnectOptions struct {
	Storage uint16
	Opts    uint16
}

const (
	StorageTypeLevelDB = 0
	StorageTypeRocksDB = 1
)

func DefaultConnectOptions() *ConnectOptions {
	var so ConnectOptions
	return so.AllowNamespaceErrors(true).OpenNamespaces(false).StorageType(StorageTypeLevelDB)
}

func (so *ConnectOptions) OpenNamespaces(value bool) *ConnectOptions {
	if value {
		so.Opts |= uint16(ConnectOptOpenNamespaces)
	} else {
		so.Opts &= ^uint16(ConnectOptOpenNamespaces)
	}
	return so
}

func (so *ConnectOptions) AllowNamespaceErrors(value bool) *ConnectOptions {
	if value {
		so.Opts |= uint16(ConnectOptAllowNamespaceErrors)
	} else {
		so.Opts &= ^uint16(ConnectOptAllowNamespaceErrors)
	}
	return so
}

// Choose storage type
func (so *ConnectOptions) StorageType(value uint16) *ConnectOptions {
	if value != StorageTypeLevelDB && value != StorageTypeRocksDB {
		so.Storage = StorageTypeLevelDB
	} else {
		so.Storage = value
	}
	return so
}

// Capabilties of chosen binding. This value will affect some of the serverside functions and serialization logic
type BindingCapabilities struct {
	Value int64
}

func DefaultBindingCapabilities() *BindingCapabilities {
	return &BindingCapabilities{Value: 0}
}

// Enable Query results idle timeouts support
func (bc *BindingCapabilities) WithQrIdleTimeouts(value bool) *BindingCapabilities {
	if value {
		bc.Value |= int64(BindingCapabilityQrIdleTimeouts)
	} else {
		bc.Value &= ^int64(BindingCapabilityQrIdleTimeouts)
	}
	return bc
}

// Enable shard IDs support
func (bc *BindingCapabilities) WithResultsWithShardIDs(value bool) *BindingCapabilities {
	if value {
		bc.Value |= int64(BindingCapabilityResultsWithShardIDs)
	} else {
		bc.Value &= ^int64(BindingCapabilityResultsWithShardIDs)
	}
	return bc
}

// go interface to reindexer_c.h interface
type RawBuffer interface {
	GetBuf() []byte
	Free()
}

// go transanction context
type TxCtx struct {
	Result  RawBuffer
	Id      uint64
	UserCtx context.Context
}

// FetchMore interface for partial loading results (used in cproto)
type FetchMore interface {
	Fetch(ctx context.Context, offset, limit int, asJson bool) (err error)
}

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

func NewError(text string, code int) error {
	return Error{text, code}
}

type Error struct {
	s    string
	code int
}

func (e Error) Error() string {
	return e.s
}

func (e Error) Code() int {
	return e.code
}

type Stats struct {
	CountGetItem int
	TimeGetItem  time.Duration
	CountSelect  int
	TimeSelect   time.Duration
	CountInsert  int
	TimeInsert   time.Duration
	CountUpdate  int
	TimeUpdate   time.Duration
	CountUpsert  int
	TimeUpsert   time.Duration
	CountDelete  int
	TimeDelete   time.Duration
	CountJoin    int
	TimeJoin     time.Duration
}

// Raw binding to reindexer
type RawBinding interface {
	Init(u []url.URL, options ...interface{}) error
	Clone() RawBinding
	OpenNamespace(ctx context.Context, namespace string, enableStorage, dropOnFileFormatError bool) error
	CloseNamespace(ctx context.Context, namespace string) error
	DropNamespace(ctx context.Context, namespace string) error
	TruncateNamespace(ctx context.Context, namespace string) error
	RenameNamespace(ctx context.Context, srcNs string, dstNs string) error
	EnableStorage(ctx context.Context, namespace string) error
	AddIndex(ctx context.Context, namespace string, indexDef IndexDef) error
	SetSchema(ctx context.Context, namespace string, schema SchemaDef) error
	UpdateIndex(ctx context.Context, namespace string, indexDef IndexDef) error
	DropIndex(ctx context.Context, namespace, index string) error
	BeginTx(ctx context.Context, namespace string) (TxCtx, error)
	CommitTx(txCtx *TxCtx) (RawBuffer, error)
	RollbackTx(tx *TxCtx) error
	ModifyItemTx(txCtx *TxCtx, format int, data []byte, mode int, percepts []string, stateToken int) error
	ModifyItemTxAsync(txCtx *TxCtx, format int, data []byte, mode int, percepts []string, stateToken int, cmpl RawCompletion)
	DeleteQueryTx(txCtx *TxCtx, rawQuery []byte) error
	UpdateQueryTx(txCtx *TxCtx, rawQuery []byte) error

	PutMeta(ctx context.Context, namespace, key, data string) error
	GetMeta(ctx context.Context, namespace, key string) (RawBuffer, error)
	ModifyItem(ctx context.Context, nsHash int, namespace string, format int, data []byte, mode int, percepts []string, stateToken int) (RawBuffer, error)
	Select(ctx context.Context, query string, asJson bool, ptVersions []int32, fetchCount int) (RawBuffer, error)
	SelectQuery(ctx context.Context, rawQuery []byte, asJson bool, ptVersions []int32, fetchCount int) (RawBuffer, error)
	DeleteQuery(ctx context.Context, nsHash int, rawQuery []byte) (RawBuffer, error)
	UpdateQuery(ctx context.Context, nsHash int, rawQuery []byte) (RawBuffer, error)
	Commit(ctx context.Context, namespace string) error
	EnableLogger(logger Logger)
	DisableLogger()
	ReopenLogFiles() error
	Ping(ctx context.Context) error
	Finalize() error
	Status(ctx context.Context) Status
	GetDSNs() []url.URL
}

type RawBindingChanging interface {
	OnChangeCallback(f func())
}

type GetReplicationStat interface {
	GetReplicationStat(f func(ctx context.Context) (*ReplicationStat, error))
}

var availableBindings = make(map[string]RawBinding)

func RegisterBinding(name string, binding RawBinding) {
	availableBindings[name] = binding
}

func GetBinding(name string) RawBinding {
	b, ok := availableBindings[name]
	if !ok {
		return nil
	}
	return b
}

type OptionReindexerInstance struct {
	Instance uintptr
}

type OptionCgoLimit struct {
	CgoLimit int
}

type OptionConnPoolSize struct {
	ConnPoolSize int
}

//OptionTimeouts sets client-side network timeouts on login(connect) and requests
//Timer resolution here is 1 second
type OptionTimeouts struct {
	LoginTimeout   time.Duration
	RequestTimeout time.Duration
}

//OptionBuiltintCtxWatch - context watcher settings
//WatchDelay - minimal delay on context watcher goroutines launch. Timer resolution here is 20 ms
//WatchersPoolSize - watchers goroutine count (this doesn't affect goroutine count after WatchDelay expiration)
type OptionBuiltintCtxWatch struct {
	WatchDelay       time.Duration
	WatchersPoolSize int
}

type OptionRetryAttempts struct {
	Read  int
	Write int
}

type OptionBuiltinWithServer struct {
	StartupTimeout  time.Duration
	ShutdownTimeout time.Duration
	ServerConfig    *config.ServerConfig
}

// OptionConnect - DB connect options for server
// CreateDBIfMissing - allow to create DB on connect if DB doesn't exist already
// EnableCompression - request compress traffic by snappy library
type OptionConnect struct {
	CreateDBIfMissing bool
}

// OptionCompression - DB connect options for server
// EnableCompression - request compress traffic by snappy library
type OptionCompression struct {
	EnableCompression bool
}

// AppName - Application name, which will be used in server connect info
type OptionAppName struct {
	AppName string
}

// Strategy - Strategy used for reconnect to server on connection error
// AllowUnknownNodes - Allow add dsn from cluster node, that not exist in original dsn list
type OptionReconnectionStrategy struct {
	Strategy          string
	AllowUnknownNodes bool
}

type Status struct {
	Err     error
	CProto  StatusCProto
	Builtin StatusBuiltin
	Cache   StatusCache
}

type StatusCache struct {
	CurSize int64
}

type StatusCProto struct {
	ConnPoolSize   int
	ConnPoolUsage  int
	ConnQueueSize  int
	ConnQueueUsage int
	ConnAddr       string
}

type StatusBuiltin struct {
	CGOLimit           int
	CGOUsage           int
	CGOUsageLastMinAvg int
}

type Completion func(err error)

type RawCompletion func(buf RawBuffer, err error)

type ReplicationStat struct {
	Type  string                `json:"type"`
	Nodes []ReplicationNodeStat `json:"nodes"`
}

type ReplicationNodeStat struct {
	DSN            string `json:"dsn"`
	Status         string `json:"status"`
	Role           string `json:"role"`
	IsSynchronized bool   `json:"is_synchronized"`
}
