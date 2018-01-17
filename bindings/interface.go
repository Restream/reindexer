package bindings

// #cgo CFLAGS: -I../cpp_src
// #cgo CXXFLAGS: -I../cpp_src
// #include "core/type_consts.h"
// #include "core/cbinding/reindexer_ctypes.h"
import "C"
import (
	"time"
)

const CInt32Max = int(^uint32(0) >> 1)

// public go consts from type_consts.h and reindexer_ctypes.h
const (
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

	AggAvg = int(C.AggAvg)
	AggSum = int(C.AggSum)

	CollateNone    = int(C.CollateNone)
	CollateASCII   = int(C.CollateASCII)
	CollateUTF8    = int(C.CollateUTF8)
	CollateNumeric = int(C.CollateNumeric)
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

	QueryCondition    = int(C.QueryCondition)
	QueryDistinct     = int(C.QueryDistinct)
	QuerySortIndex    = int(C.QuerySortIndex)
	QueryJoinOn       = int(C.QueryJoinOn)
	QueryEnd          = int(C.QueryEnd)
	QueryLimit        = int(C.QueryLimit)
	QueryOffset       = int(C.QueryOffset)
	QueryDebugLevel   = int(C.QueryDebugLevel)
	QueryReqTotal     = int(C.QueryReqTotal)
	QuerySelectFilter = int(C.QuerySelectFilter)
	QueryAggregation  = int(C.QueryAggregation)

	LeftJoin    = int(C.LeftJoin)
	InnerJoin   = int(C.InnerJoin)
	OrInnerJoin = int(C.OrInnerJoin)
	Merge       = int(C.Merge)

	TAG_VARINT = int(C.TAG_VARINT)
	TAG_DOUBLE = int(C.TAG_DOUBLE)
	TAG_STRING = int(C.TAG_STRING)
	TAG_ARRAY  = int(C.TAG_ARRAY)
	TAG_BOOL   = int(C.TAG_BOOL)
	TAG_NULL   = int(C.TAG_NULL)
	TAG_OBJECT = int(C.TAG_OBJECT)
	TAG_END    = int(C.TAG_END)

	FormatJson  = int(C.FormatJson)
	FormatCJson = int(C.FormatCJson)

	ModeInsert = int(C.ModeInsert)
	ModeUpdate = int(C.ModeUpdate)
	ModeUpsert = int(C.ModeUpsert)
	ModeDelete = int(C.ModeDelete)

	ModeNoCalc        = int(C.ModeNoTotal)
	ModeCachedTotal   = int(C.ModeCachedTotal)
	ModeAccurateTotal = int(C.ModeAccurateTotal)

	ErrOK        = int(C.errOK)
	ErrParseSQL  = int(C.errParseSQL)
	ErrQueryExec = int(C.errQueryExec)
	ErrParams    = int(C.errParams)
	ErrLogic     = int(C.errLogic)
	ErrParseJson = int(C.errParseJson)
	ErrParseDSL  = int(C.errParseDSL)
	ErrConflict  = int(C.errConflict)
)

type CInt C.int
type CUInt8 C.uint8_t
type CInt8 C.int8_t
type CInt16 C.int16_t

// go interface to reindexer_c.h interface
type RawBuffer interface {
	GetBuf() []byte
	Free()
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
	OpenNamespace(namespace string, enableStorage, dropOnFileFormatError bool) error
	CloseNamespace(namespace string) error
	DropNamespace(namespace string) error
	CloneNamespace(src string, dst string) error
	RenameNamespace(src string, dst string) error
	EnableStorage(namespace string) error
	AddIndex(namespace, index, jsonPath, indexType, fieldType string, isArray, isPK, isDense, isAppendable bool, mode int) error
	ConfigureIndex(namespace, index, config string) error
	PutMeta(raw []byte) error
	GetMeta(raw []byte) (RawBuffer, error)
	GetPayloadType(resBuf []byte, nsid int) (RawBuffer, error)
	ModifyItem(data []byte, mode int) (RawBuffer, error)
	Select(query string, withItems bool) (RawBuffer, error)
	SelectQuery(withItems bool, rawQuery []byte) (RawBuffer, error)
	DeleteQuery(rawQuery []byte) (RawBuffer, error)
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
