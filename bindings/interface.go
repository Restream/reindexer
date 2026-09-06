package bindings

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/restream/reindexer/v5/jsonschema"
)

const (
	MultithreadingMode_SingleThread            = 0
	MultithreadingMode_MultithreadTransactions = 1
)

type EmbedderCircuitBreakerConfig struct {
	// Consecutive failed network requests required to open the breaker.
	// Value 0 disables the circuit breaker.
	// Default: 16
	Threshold *int `json:"threshold,omitempty"`
	// Idle timeout that resets the consecutive failure counter (milliseconds). Optional
	// Value 0 disables idle reset.
	// Default: 15000
	ThresholdTimeout *int `json:"threshold_timeout_ms,omitempty"`
	// Cooldown after the breaker opens (milliseconds).
	// Value 0 disables the circuit breaker.
	// Default: 7500
	Cooldown *int `json:"cooldown_ms,omitempty"`
}

// Keep in sync with EmbedderCircuitBreakerDefaults in circuitbreaker_defaults.h.
const (
	defaultEmbedderCircuitBreakerThreshold          = 16
	defaultEmbedderCircuitBreakerThresholdTimeoutMs = 15000
	defaultEmbedderCircuitBreakerCooldownMs         = 7500
)

func DefaultEmbedderCircuitBreakerConfig() *EmbedderCircuitBreakerConfig {
	return &EmbedderCircuitBreakerConfig{
		Threshold:        intPtr(defaultEmbedderCircuitBreakerThreshold),
		ThresholdTimeout: intPtr(defaultEmbedderCircuitBreakerThresholdTimeoutMs),
		Cooldown:         intPtr(defaultEmbedderCircuitBreakerCooldownMs),
	}
}

func intPtr(v int) *int { return &v }

type EmbedderConnectionPoolConfig struct {
	// Number connections to service. Optional
	// Values range: [1,1024]
	// Default: 10
	Connections int `json:"connections,omitempty"`
	// Connection\reconnection timeout to any embedding service (milliseconds)
	// Min value: 100
	// Default: 300
	ConnectTimeout int `json:"connect_timeout_ms,omitempty"`
	// Timeout reading data from embedding service (milliseconds). Optional
	// Min value: 500
	// Default: 5000
	ReadTimeout int `json:"read_timeout_ms,omitempty"`
	// Timeout writing data from embedding service (milliseconds). Optional
	// Min value: 500
	// Default: 5000
	WriteTimeout int `json:"write_timeout_ms,omitempty"`
	// Optional circuit breaker for cache-miss embedding HTTP calls
	CircuitBreaker *EmbedderCircuitBreakerConfig `json:"circuit_breaker,omitempty"`
}

func DefaultEmbedderConnectionPoolConfig() *EmbedderConnectionPoolConfig {
	return &EmbedderConnectionPoolConfig{
		Connections:    10,
		ConnectTimeout: 300,
		ReadTimeout:    5000,
		WriteTimeout:   5000,
		CircuitBreaker: DefaultEmbedderCircuitBreakerConfig(),
	}
}

const (
	EmbeddingProtocolRX     = "rx"
	EmbeddingProtocolOpenAI = "openai"
)

// IEmbeddingProtocolOptions is implemented by protocol-specific embedder options.
type IEmbeddingProtocolOptions interface {
	Type() string
}

// EmbeddingProtocolOptionsRX configures the default Reindexer produce API.
type EmbeddingProtocolOptionsRX struct {
	// Protocol type. Must be "rx".
	ProtocolType string `json:"type"`
}

func (o *EmbeddingProtocolOptionsRX) Type() string {
	if o == nil || o.ProtocolType == "" {
		return EmbeddingProtocolRX
	}
	return o.ProtocolType
}

func (o EmbeddingProtocolOptionsRX) MarshalJSON() ([]byte, error) {
	if o.ProtocolType == "" {
		o.ProtocolType = EmbeddingProtocolRX
	}
	if o.ProtocolType != EmbeddingProtocolRX {
		return nil, fmt.Errorf("unexpected embedding protocol type %q for RX options", o.ProtocolType)
	}
	type dto EmbeddingProtocolOptionsRX
	return json.Marshal(dto(o))
}

func NewEmbeddingProtocolOptionsRX() *EmbeddingProtocolOptionsRX {
	return &EmbeddingProtocolOptionsRX{ProtocolType: EmbeddingProtocolRX}
}

// EmbeddingProtocolOptionsOpenAI configures an OpenAI-compatible embeddings endpoint.
type EmbeddingProtocolOptionsOpenAI struct {
	// Protocol type. Must be "openai".
	ProtocolType string `json:"type"`
	// Model name for OpenAI-compatible requests. Required.
	Model string `json:"model"`
	// How upsert fields are converted to OpenAI input text. Optional, upsert embedder only.
	// "stringify" (default): document fields serialized as a JSON object string
	// "join": field values joined with newline (array values within a field joined with space)
	FieldsFormat string `json:"fields_format,omitempty"`
}

func (o *EmbeddingProtocolOptionsOpenAI) Type() string {
	if o == nil || o.ProtocolType == "" {
		return EmbeddingProtocolOpenAI
	}
	return o.ProtocolType
}

func (o EmbeddingProtocolOptionsOpenAI) MarshalJSON() ([]byte, error) {
	if o.ProtocolType == "" {
		o.ProtocolType = EmbeddingProtocolOpenAI
	}
	if o.ProtocolType != EmbeddingProtocolOpenAI {
		return nil, fmt.Errorf("unexpected embedding protocol type %q for OpenAI options", o.ProtocolType)
	}
	if o.Model == "" {
		return nil, fmt.Errorf("embedding protocol %q requires model", EmbeddingProtocolOpenAI)
	}
	type dto EmbeddingProtocolOptionsOpenAI
	return json.Marshal(dto(o))
}

func NewEmbeddingProtocolOptionsOpenAI(model string) *EmbeddingProtocolOptionsOpenAI {
	return &EmbeddingProtocolOptionsOpenAI{ProtocolType: EmbeddingProtocolOpenAI, Model: model}
}

type EmbedderConfig struct {
	// Embedder name. Optional. Used in RX protocol URL path; ignored for OpenAI protocol
	Name string `json:"name,omitempty"`
	// Embed service URL. Required.
	// For protocol "rx": base URL of the service (path /api/v1/embedder/{name}/produce is appended).
	// For protocol "openai": full embeddings endpoint URL (e.g. http://127.0.0.1:8080/v1/embeddings).
	URL string `json:"URL"`
	// Protocol options. Optional, defaults to RX produce API when omitted.
	// Use *EmbeddingProtocolOptionsRX or *EmbeddingProtocolOptionsOpenAI.
	ProtocolOptions IEmbeddingProtocolOptions `json:"protocol,omitempty"`
	// List of index fields to calculate embedding. Required for UpsertEmbedder and optional for QueryEmbedder
	Fields []string `json:"fields,omitempty"`
	// Name, used to access the cache. Optional, if not specified, caching is not used
	CacheTag string `json:"cache_tag,omitempty"`
	// Embedding insertion strategy. Optional
	// `always` :       Default value, always embed
	// `empty_only` :   When the user specified any value for the embedded field (non-empty vector), then automatic embedding is not performed
	// `strict` :       When the user sets some value for the embedded field (non-empty vector), we return an error. If the field is empty, we automatically embed
	EmbeddingStrategy string `json:"embedding_strategy,omitempty"`
	// Connection pool configuration
	ConnectionPoolConfig *EmbedderConnectionPoolConfig `json:"pool,omitempty"`
}

func (c *EmbedderConfig) UnmarshalJSON(data []byte) error {
	type dto EmbedderConfig
	aux := &struct {
		*dto
		ProtocolOptions json.RawMessage `json:"protocol,omitempty"`
	}{dto: (*dto)(c)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if len(aux.ProtocolOptions) == 0 {
		c.ProtocolOptions = nil
		return nil
	}
	var raw any
	if err := json.Unmarshal(aux.ProtocolOptions, &raw); err != nil {
		return err
	}
	if raw == nil {
		c.ProtocolOptions = nil
		return nil
	}
	var typeProbe struct {
		Type         string `json:"type"`
		Model        string `json:"model"`
		FieldsFormat string `json:"fields_format"`
	}
	if err := json.Unmarshal(aux.ProtocolOptions, &typeProbe); err != nil {
		return err
	}
	if typeProbe.Type == "" {
		return fmt.Errorf("embedding protocol type is required")
	}
	switch typeProbe.Type {
	case EmbeddingProtocolOpenAI:
		var opts EmbeddingProtocolOptionsOpenAI
		if err := json.Unmarshal(aux.ProtocolOptions, &opts); err != nil {
			return err
		}
		if opts.Model == "" {
			return fmt.Errorf("embedding protocol %q requires model", EmbeddingProtocolOpenAI)
		}
		c.ProtocolOptions = &opts
	case EmbeddingProtocolRX:
		if typeProbe.Model != "" || typeProbe.FieldsFormat != "" {
			return fmt.Errorf("embedding protocol %q does not support model or fields_format", EmbeddingProtocolRX)
		}
		var opts EmbeddingProtocolOptionsRX
		if err := json.Unmarshal(aux.ProtocolOptions, &opts); err != nil {
			return err
		}
		if opts.ProtocolType == "" {
			opts.ProtocolType = EmbeddingProtocolRX
		}
		c.ProtocolOptions = &opts
	default:
		return fmt.Errorf("unknown embedding protocol type %q", typeProbe.Type)
	}
	return nil
}

func DefaultUpsertEmbedderConfig(url string, fields []string) *EmbedderConfig {
	config := new(EmbedderConfig)
	config.Name = "UpsertEmbedder"
	config.URL = url
	config.Fields = fields
	config.CacheTag = ""
	config.EmbeddingStrategy = "always"
	config.ConnectionPoolConfig = DefaultEmbedderConnectionPoolConfig()
	return config
}

func DefaultQueryEmbedderConfig(url string) *EmbedderConfig {
	config := new(EmbedderConfig)
	config.Name = "QueryEmbedder"
	config.URL = url
	config.CacheTag = ""
	config.ConnectionPoolConfig = DefaultEmbedderConnectionPoolConfig()
	return config
}

type EmbeddingConfig struct {
	// Insert\Update\Upsert embedder configuration
	UpsertEmbedder *EmbedderConfig `json:"upsert_embedder,omitempty"`
	// Query embedder configuration
	QueryEmbedder *EmbedderConfig `json:"query_embedder,omitempty"`
}
type QuantizationConfig struct {
	Type       string  `json:"quantization_type"`
	Quantile   float32 `json:"quantile,omitempty"`
	SampleSize int64   `json:"sample_size,omitempty"`
	Threshold  int64   `json:"quantization_threshold,omitempty"`
}

type FloatVectorIndexOpts struct {
	Metric             string              `json:"metric"`
	Dimension          int                 `json:"dimension"`
	M                  int                 `json:"m,omitempty"`
	EfConstruction     int                 `json:"ef_construction,omitempty"`
	StartSize          int                 `json:"start_size,omitempty"`
	CentroidsCount     int                 `json:"centroids_count,omitempty"`
	MultithreadingMode int                 `json:"multithreading,omitempty"`
	Radius             float32             `json:"radius,omitempty"`
	EmbeddingConfig    *EmbeddingConfig    `json:"embedding,omitempty"`
	QuantizationConfig *QuantizationConfig `json:"quantization_config,omitempty"`
}

type IndexDef struct {
	Name        string   `json:"name"`
	JSONPaths   []string `json:"json_paths"`
	IndexType   string   `json:"index_type"`
	FieldType   string   `json:"field_type"`
	IsPK        bool     `json:"is_pk"`
	IsArray     bool     `json:"is_array"`
	IsDense     bool     `json:"is_dense"`
	IsNoColumn  bool     `json:"is_no_column"`
	IsSparse    bool     `json:"is_sparse"`
	CollateMode string   `json:"collate_mode"`
	SortOrder   string   `json:"sort_order_letters"`
	ExpireAfter int      `json:"expire_after"`
	Config      any      `json:"config"`
	RTreeType   string   `json:"rtree_type"`
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

// Capabilities of chosen binding. This value will affect some of the serverside functions and serialization logic
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

// Enable namespaces timestamps support
func (bc *BindingCapabilities) WithIncarnationTags(value bool) *BindingCapabilities {
	if value {
		bc.Value |= int64(BindingCapabilityNamespaceIncarnations)
	} else {
		bc.Value &= ^int64(BindingCapabilityNamespaceIncarnations)
	}
	return bc
}

// Enable float rank format
func (bc *BindingCapabilities) WithFloatRank(value bool) *BindingCapabilities {
	if value {
		bc.Value |= int64(BindingCapabilityComplexRank)
	} else {
		bc.Value &= ^int64(BindingCapabilityComplexRank)
	}
	return bc
}

// Enable query serialization format V2
func (bc *BindingCapabilities) WithQueryFormatV2(value bool) *BindingCapabilities {
	if value {
		bc.Value |= int64(BindingCapabilityQueryFormatV2)
	} else {
		bc.Value &= ^int64(BindingCapabilityQueryFormatV2)
	}
	return bc
}

func (bc BindingCapabilities) HasQueryFormatV2() bool {
	return (bc.Value & int64(BindingCapabilityQueryFormatV2)) != 0
}

func (bc BindingCapabilities) QueryFormatVersion() int {
	if bc.HasQueryFormatV2() {
		return QueryFormatV2
	}
	return QueryFormatV1
}

// go interface to reindexer_c.h interface
type RawBuffer interface {
	GetBuf() []byte
	Free()
}

// go transaction context
type TxCtx struct {
	Result  RawBuffer
	Id      uint64
	UserCtx context.Context
}

// FetchMore interface for partial loading results (used in cproto/ucproto)
type FetchMore interface {
	Fetch(ctx context.Context, offset, limit int, asJson bool) (err error)
}

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...any)
}

type NullLogger struct {
}

func (NullLogger) Printf(level int, fmt string, msg ...any) {
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
	Init(u []url.URL, eh EventsHandler, options ...any) error
	QueryFormatVersion() int
	Clone() RawBinding
	OpenNamespace(ctx context.Context, namespace string, enableStorage, dropOnFileFormatError bool) error
	CloseNamespace(ctx context.Context, namespace string) error
	DropNamespace(ctx context.Context, namespace string) error
	TruncateNamespace(ctx context.Context, namespace string) error
	RenameNamespace(ctx context.Context, srcNs string, dstNs string) error
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

	EnumMeta(ctx context.Context, namespace string) ([]string, error)
	PutMeta(ctx context.Context, namespace, key, data string) error
	GetMeta(ctx context.Context, namespace, key string) (RawBuffer, error)
	DeleteMeta(ctx context.Context, namespace, key string) error
	ModifyItem(ctx context.Context, namespace string, format int, data []byte, mode int, percepts []string, stateToken int) (RawBuffer, error)
	Select(ctx context.Context, query string, asJson bool, tmVersions []int32, fetchCount int) (RawBuffer, error)
	SelectQuery(ctx context.Context, rawQuery []byte, asJson bool, tmVersions []int32, fetchCount int) (RawBuffer, error)
	DeleteQuery(ctx context.Context, rawQuery []byte) (RawBuffer, error)
	UpdateQuery(ctx context.Context, rawQuery []byte, tmVersions []int32) (RawBuffer, error)
	EnableLogger(logger Logger)
	DisableLogger()
	GetLogger() Logger
	ReopenLogFiles() error
	Ping(ctx context.Context) error
	Finalize() error
	Status(ctx context.Context) Status
	GetDSNs() []url.URL
	Subscribe(ctx context.Context, opts *SubscriptionOptions) error
	Unsubscribe(ctx context.Context) error

	DBMSVersion() (string, error)
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

type OptionBuiltinAllocatorConfig struct {
	AllocatorCacheLimit   int64
	AllocatorMaxCachePart float32
}

// MaxUpdatesSizeBytes for the internal replication/events queues
type OptionBuiltinMaxUpdatesSize struct {
	MaxUpdatesSizeBytes uint
}

type OptionCgoLimit struct {
	CgoLimit int
}

type OptionConnPoolSize struct {
	ConnPoolSize int
}

type LoadBalancingAlgorithm int

// LBRoundRobin - choose connections in round-robin fashion. Used by default
// LBRandom - choose connections randomly
// LBPowerOfTwoChoices - choose connections using "Power of Two Choices" algorithm (https://www.nginx.com/blog/nginx-power-of-two-choices-load-balancing-algorithm/)
const (
	LBRoundRobin LoadBalancingAlgorithm = iota
	LBRandom
	LBPowerOfTwoChoices
)

// OptionConnPoolLoadBalancing sets algorithm, which will be used to choose connection for cproto/ucproto requests' balancing
type OptionConnPoolLoadBalancing struct {
	Algorithm LoadBalancingAlgorithm
}

// OptionTimeouts sets client-side network timeouts on login(connect) and requests
// Timer resolution here is 1 second
type OptionTimeouts struct {
	LoginTimeout   time.Duration
	RequestTimeout time.Duration
}

// OptionBuiltintCtxWatch - context watcher settings
// WatchDelay - minimal delay on context watcher goroutines launch. Timer resolution here is 20 ms
// WatchersPoolSize - watchers goroutine count (this doesn't affect goroutine count after WatchDelay expiration)
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
type OptionConnect struct {
	CreateDBIfMissing bool
}

// OptionCompression - DB compression options for server
// EnableCompression - request compress traffic by snappy library
type OptionCompression struct {
	EnableCompression bool
}

// OptionDedicatedThreads - dedicated RPC threads options for server
// DedicatedThreads - request dedicated thread on server side for each connection of this binding
type OptionDedicatedThreads struct {
	DedicatedThreads bool
}

// AppName - Application name, which will be used in server connect info
type OptionAppName struct {
	AppName string
}

// OptionPrometheusMetrics - enables collection of Reindexer's client side metrics (for example,
// information about latency and rpc of all rx client calls like Upsert, Select, etc).
type OptionPrometheusMetrics struct {
	EnablePrometheusMetrics bool
}

// OptionOpenTelemetry - enables OpenTelemetry integration.
type OptionOpenTelemetry struct {
	EnableTracing bool
}

// OptionStrictJoinHandlers - enables join handlers check.
// If enabled, queries without required join handlers or Joinable interface will return error after execution.
type OptionStrictJoinHandlers struct {
	EnableStrictJoinHandlers bool
}

// Strategy - Strategy used for reconnect to server on connection error
// AllowUnknownNodes - Allow add dsn from cluster node, that not exist in original dsn list
type OptionReconnectionStrategy struct {
	Strategy          string
	AllowUnknownNodes bool
}

type OptionTLS struct {
	Config *tls.Config
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
