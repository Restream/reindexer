package bindings

import (
	"context"
	"net/url"
	"time"

	"github.com/restream/reindexer/bindings/builtinserver/config"
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
}

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
	Init(u *url.URL, options ...interface{}) error
	Clone() RawBinding
	OpenNamespace(ctx context.Context, namespace string, enableStorage, dropOnFileFormatError bool) error
	CloseNamespace(ctx context.Context, namespace string) error
	DropNamespace(ctx context.Context, namespace string) error
	EnableStorage(ctx context.Context, namespace string) error
	AddIndex(ctx context.Context, namespace string, indexDef IndexDef) error
	UpdateIndex(ctx context.Context, namespace string, indexDef IndexDef) error
	DropIndex(ctx context.Context, namespace, index string) error
	BeginTx(ctx context.Context, namespace string) (TxCtx, error)
	CommitTx(txCtx *TxCtx) (RawBuffer, error)
	RollbackTx(tx *TxCtx) error
	ModifyItemTx(txCtx *TxCtx, format int, data []byte, mode int, percepts []string, stateToken int) error
	ModifyItemTxAsync(txCtx *TxCtx, format int, data []byte, mode int, percepts []string, stateToken int, cmpl RawCompletion)

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
	Ping(ctx context.Context) error
	Finalize() error
	Status() Status
}

type RawBindingChanging interface {
	OnChangeCallback(f func())
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

type Status struct {
	Err     error
	CProto  StatusCProto
	Builtin StatusBuiltin
}

type StatusCProto struct {
	ConnPoolSize   int
	ConnPoolUsage  int
	ConnQueueSize  int
	ConnQueueUsage int
}

type StatusBuiltin struct {
	CGOLimit int
	CGOUsage int
}

type Completion func(err error)

type RawCompletion func(buf RawBuffer, err error)
