package bindings

import (
	"net/url"
	"time"
)

type IndexOptions uint8

func (indexOpts *IndexOptions) PK(value bool) *IndexOptions {
	if value {
		*indexOpts |= IndexOptions(IndexOptPK)
	} else {
		*indexOpts &= ^IndexOptions(IndexOptPK)
	}
	return indexOpts
}

func (indexOpts *IndexOptions) Array(value bool) *IndexOptions {
	if value {
		*indexOpts |= IndexOptions(IndexOptArray)
	} else {
		*indexOpts &= ^IndexOptions(IndexOptArray)
	}
	return indexOpts
}

func (indexOpts *IndexOptions) Dense(value bool) *IndexOptions {
	if value {
		*indexOpts |= IndexOptions(IndexOptDense)
	} else {
		*indexOpts &= ^IndexOptions(IndexOptDense)
	}
	return indexOpts
}

func (indexOpts *IndexOptions) Appendable(value bool) *IndexOptions {
	if value {
		*indexOpts |= IndexOptions(IndexOptAppendable)
	} else {
		*indexOpts &= ^IndexOptions(IndexOptAppendable)
	}
	return indexOpts
}

func (indexOpts *IndexOptions) IsPK() bool {
	return uint8(*indexOpts)&IndexOptPK != 0
}

func (indexOpts *IndexOptions) IsArray() bool {
	return uint8(*indexOpts)&IndexOptArray != 0
}

func (indexOpts *IndexOptions) IsDense() bool {
	return uint8(*indexOpts)&IndexOptDense != 0
}

func (indexOpts *IndexOptions) IsAppendable() bool {
	return uint8(*indexOpts)&IndexOptAppendable != 0
}

type StorageOptions uint8

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
	Init(u *url.URL) error
	OpenNamespace(namespace string, enableStorage, dropOnFileFormatError bool) error
	CloseNamespace(namespace string) error
	DropNamespace(namespace string) error
	EnableStorage(namespace string) error
	AddIndex(namespace, index, jsonPath, indexType, fieldType string, opts IndexOptions, collateMode int, sortOrderStr string) error
	ConfigureIndex(namespace, index, config string) error
	PutMeta(namespace, key, data string) error
	GetMeta(namespace, key string) (RawBuffer, error)
	ModifyItem(data []byte, mode int) (RawBuffer, error)
	Select(query string, withItems bool, ptVersions []int32, fetchCount int) (RawBuffer, error)
	SelectQuery(rawQuery []byte, withItems bool, ptVersions []int32, fetchCount int) (RawBuffer, error)
	DeleteQuery(rawQuery []byte) (RawBuffer, error)
	Commit(namespace string) error
	EnableLogger(logger Logger)
	DisableLogger()
	Ping() error

	GetStats() Stats
	ResetStats()
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
