package bindings

import (
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
	CacheMode   uint8       `json:"cached_mode"`
}

type StorageOptions uint8
type CacheMode uint8

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

// FetchMore interface for partial loading results (used in cproto)
type FetchMore interface {
	Fetch(offset, limit int, withItems bool) (err error)
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
	OpenNamespace(namespace string, enableStorage, dropOnFileFormatError bool, cacheMode uint8) error
	CloseNamespace(namespace string) error
	DropNamespace(namespace string) error
	EnableStorage(namespace string) error
	AddIndex(namespace string, indexDef IndexDef) error
	UpdateIndex(namespace string, indexDef IndexDef) error
	DropIndex(namespace, index string) error
	PutMeta(namespace, key, data string) error
	GetMeta(namespace, key string) (RawBuffer, error)
	ModifyItem(nsHash int, namespace string, format int, data []byte, mode int, packedPercepts []byte, stateToken int, txID int) (RawBuffer, error)
	Select(query string, withItems bool, ptVersions []int32, fetchCount int) (RawBuffer, error)
	SelectQuery(rawQuery []byte, withItems bool, ptVersions []int32, fetchCount int) (RawBuffer, error)
	DeleteQuery(nsHash int, rawQuery []byte) (RawBuffer, error)
	Commit(namespace string) error
	EnableLogger(logger Logger)
	DisableLogger()
	Ping() error
	Finalize() error
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

type OptionRetryAttempts struct {
	Read  int
	Write int
}

type OptionBuiltinWithServer struct {
	StartupTimeout  time.Duration
	ShutdownTimeout time.Duration
	ServerConfig    *config.ServerConfig
}
