package bindings

const CInt32Max = int(^uint32(0) >> 1)

const ReindexerVersion = "v3.1.0"

// public go consts from type_consts.h and reindexer_ctypes.h
const (
	ANY     = 0
	EQ      = 1
	LT      = 2
	LE      = 3
	GT      = 4
	GE      = 5
	RANGE   = 6
	SET     = 7
	ALLSET  = 8
	EMPTY   = 9
	LIKE    = 10
	DWITHIN = 11

	ERROR   = 1
	WARNING = 2
	INFO    = 3
	TRACE   = 4

	AggSum      = 0
	AggAvg      = 1
	AggFacet    = 2
	AggMin      = 3
	AggMax      = 4
	AggDistinct = 5

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

	ValueInt64     = 0
	ValueDouble    = 1
	ValueString    = 2
	ValueBool      = 3
	ValueNull      = 4
	ValueInt       = 8
	ValueUndefined = 9
	ValueComposite = 10
	ValueTuple     = 11

	QueryCondition         = 0
	QueryDistinct          = 1
	QuerySortIndex         = 2
	QueryJoinOn            = 3
	QueryLimit             = 4
	QueryOffset            = 5
	QueryReqTotal          = 6
	QueryDebugLevel        = 7
	QueryAggregation       = 8
	QuerySelectFilter      = 9
	QuerySelectFunction    = 10
	QueryEnd               = 11
	QueryExplain           = 12
	QueryEqualPosition     = 13
	QueryUpdateField       = 14
	QueryAggregationLimit  = 15
	QueryAggregationOffset = 16
	QueryAggregationSort   = 17
	QueryOpenBracket       = 18
	QueryCloseBracket      = 19
	QueryJoinCondition     = 20
	QueryDropField         = 21
	QueryUpdateObject      = 22
	QueryWithRank          = 23
	QueryStrictMode        = 24
	QueryUpdateFieldV2     = 25

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

	QueryResultEnd         = 0
	QueryResultAggregation = 1
	QueryResultExplain     = 2

	QueryStrictModeNotSet  = 0
	QueryStrictModeNone    = 1
	QueryStrictModeNames   = 2
	QueryStrictModeIndexes = 3

	ResultsFormatMask = 0xF
	ResultsPure       = 0x0
	ResultsPtrs       = 0x1
	ResultsCJson      = 0x2
	ResultsJson       = 0x3

	ResultsWithPayloadTypes = 0x10
	ResultsWithItemID       = 0x20
	ResultsWithPercents     = 0x40
	ResultsWithNsID         = 0x80
	ResultsWithJoined       = 0x100

	IndexOptPK         = 1 << 7
	IndexOptArray      = 1 << 6
	IndexOptDense      = 1 << 5
	IndexOptAppendable = 1 << 4
	IndexOptSparse     = 1 << 3

	StorageOptEnabled               = 1
	StorageOptDropOnFileFormatError = 1 << 1
	StorageOptCreateIfMissing       = 1 << 2

	ConnectOptOpenNamespaces       = 1
	ConnectOptAllowNamespaceErrors = 1 << 1
	ConnectOptAutorepair           = 1 << 2
	ConnectOptWarnVersion          = 1 << 4

	ErrOK               = 0
	ErrParseSQL         = 1
	ErrQueryExec        = 2
	ErrParams           = 3
	ErrLogic            = 4
	ErrParseJson        = 5
	ErrParseDSL         = 6
	ErrConflict         = 7
	ErrParseBin         = 8
	ErrForbidden        = 9
	ErrWasRelock        = 10
	ErrNotValid         = 11
	ErrNetwork          = 12
	ErrNotFound         = 13
	ErrStateInvalidated = 14
	ErrTimeout          = 19
	ErrCanceled         = 20
	ErrTagsMissmatch    = 21
)
