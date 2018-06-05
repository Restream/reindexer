package bindings

const CInt32Max = int(^uint32(0) >> 1)

// public go consts from type_consts.h and reindexer_ctypes.h
const (
	ANY    = 0
	EQ     = 1
	LT     = 2
	LE     = 3
	GT     = 4
	GE     = 5
	RANGE  = 6
	SET    = 7
	ALLSET = 8
	EMPTY  = 9

	ERROR   = 1
	WARNING = 2
	INFO    = 3
	TRACE   = 4

	AggSum = 0
	AggAvg = 1

	CollateNone    = 0
	CollateASCII   = 1
	CollateUTF8    = 2
	CollateNumeric = 3
	CollateCustom  = 4
)

// private go consts from type_consts.h and reindexer_ctypes.h
const (
	OpOr  = 1
	OpAnd = 2
	OpNot = 3

	ValueInt       = 0
	ValueInt64     = 1
	ValueString    = 2
	ValueDouble    = 3
	ValueComposite = 6

	QueryCondition      = 0
	QueryDistinct       = 1
	QuerySortIndex      = 2
	QueryJoinOn         = 3
	QueryLimit          = 4
	QueryOffset         = 5
	QueryReqTotal       = 6
	QueryDebugLevel     = 7
	QueryAggregation    = 8
	QuerySelectFilter   = 9
	QuerySelectFunction = 10
	QueryEnd            = 11

	LeftJoin    = 0
	InnerJoin   = 1
	OrInnerJoin = 2
	Merge       = 3

	CacheModeOn         = 0
	CacheModeAggressive = 1
	CacheModeOff        = 2

	FormatJson  = 0
	FormatCJson = 1

	ModeUpdate = 0
	ModeInsert = 1
	ModeUpsert = 2
	ModeDelete = 3

	ModeNoCalc        = 0
	ModeCachedTotal   = 1
	ModeAccurateTotal = 2

	ResultsPure             = 0
	ResultsWithPtrs         = 1
	ResultsWithCJson        = 2
	ResultsWithJson         = 3
	ResultsWithPayloadTypes = 8

	IndexOptPK         = 1 << 7
	IndexOptArray      = 1 << 6
	IndexOptDense      = 1 << 5
	IndexOptAppendable = 1 << 4

	StorageOptEnabled               = 1
	StorageOptDropOnFileFormatError = 1 << 1
	StorageOptCreateIfMissing       = 1 << 2

	ErrOK        = 0
	ErrParseSQL  = 1
	ErrQueryExec = 2
	ErrParams    = 3
	ErrLogic     = 4
	ErrParseJson = 5
	ErrParseDSL  = 6
	ErrConflict  = 7
)
