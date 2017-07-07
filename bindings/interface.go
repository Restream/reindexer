package bindings

// #cgo CFLAGS: -I..
// #cgo CXXFLAGS: -I..
// #include "core/type_consts.h"
// #include "cbinding/reindexer_ctypes.h"
import "C"
import "time"

// public go consts from type_consts.h and reindexer_ctypes.h
const (
	IndexHash          = int(C.IndexHash)
	IndexTree          = int(C.IndexTree)
	IndexInt           = int(C.IndexInt)
	IndexIntHash       = int(C.IndexIntHash)
	IndexInt64         = int(C.IndexInt64)
	IndexInt64Hash     = int(C.IndexInt64Hash)
	IndexDouble        = int(C.IndexDouble)
	IndexFullText      = int(C.IndexFullText)
	IndexNewFullText   = int(C.IndexNewFullText)
	IndexComposite     = int(C.IndexComposite)
	IndexCompositeHash = int(C.IndexCompositeHash)
	IndexBool          = int(C.IndexBool)
	IndexIntStore      = int(C.IndexIntStore)
	IndexInt64Store    = int(C.IndexInt64Store)
	IndexStrStore      = int(C.IndexStrStore)
	IndexDoubleStore   = int(C.IndexDoubleStore)

	EQ     = int(C.CondEq)
	GT     = int(C.CondGt)
	LT     = int(C.CondLt)
	GE     = int(C.CondGe)
	LE     = int(C.CondLe)
	SET    = int(C.CondSet)
	ALLSET = int(C.CondAllSet)
	RANGE  = int(C.CondRange)
	ANY    = int(C.CondAny)
	EMPTY  = int(C.CondEmpty)

	ERROR   = int(C.LogError)
	WARNING = int(C.LogWarning)
	INFO    = int(C.LogInfo)
	TRACE   = int(C.LogTrace)
)

// private go consts from type_consts.h and reindexer_ctypes.h
const (
	OpAnd = int(C.OpAnd)
	OpOr  = int(C.OpOr)
	OpNot = int(C.OpNot)

	ValueInt    = int(C.KeyValueInt)
	ValueInt64  = int(C.KeyValueInt64)
	ValueDouble = int(C.KeyValueDouble)
	ValueString = int(C.KeyValueString)

	QueryCondition = int(C.QueryCondition)
	QueryDistinct  = int(C.QueryDistinct)
	QuerySortIndex = int(C.QuerySortIndex)
	QueryJoinOn    = int(C.QueryJoinOn)
	QueryEnd       = int(C.QueryEnd)

	LeftJoin    = int(C.LeftJoin)
	InnerJoin   = int(C.InnerJoin)
	OrInnerJoin = int(C.OrInnerJoin)
)

type CInt C.int

// go interface to reindexer_c.h interface
type RawBuffer interface {
	GetBuf() []byte
	Free()
}

// Logger interface for reindexer
type Logger interface {
	Printf(level int, fmt string, msg ...interface{})
}

type Query struct {
	Namespace     string
	ReqTotalCount bool
	LimitItems    int
	StartOffset   int
	DebugLevel    int
}

type Stats struct {
	CountGetItem int
	TimeGetItem  time.Duration
	CountSelect  int
	TimeSelect   time.Duration
	CountUpsert  int
	TimeUpsert   time.Duration
	CountDelete  int
	TimeDelete   time.Duration
	CountJoin    int
	TimeJoin     time.Duration
}

// Raw binding to reindexer
type RawBinding interface {
	AddNamespace(namespace string) error
	DeleteNamespace(namespace string) error
	CloneNamespace(src string, dst string) error
	RenameNamespace(src string, dst string) error
	EnableStorage(namespace string, path string) error
	AddIndex(namespace, index, jsonPath string, indexType int, isArray, isPK bool) error
	PutMeta(raw []byte) error
	GetMeta(raw []byte) (RawBuffer, error)
	DeleteItem([]byte) (RawBuffer, error)
	UpsertItem(data []byte) (RawBuffer, error)
	GetItems(data []byte) (RawBuffer, error)
	Select(query string, withItems bool) (RawBuffer, error)
	SelectQuery(q *Query, withItems bool, rawQuery []byte) (RawBuffer, error)
	DeleteQuery(q *Query, rawQuery []byte) (RawBuffer, error)
	Commit(namespace string) error
	EnableLogger(logger Logger)
	DisableLogger()

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
