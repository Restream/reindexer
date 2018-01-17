#pragma once

typedef enum KeyValueType {
	KeyValueInt,
	KeyValueInt64,
	KeyValueString,
	KeyValueDouble,
	KeyValueEmpty,
	KeyValueUndefined,
	KeyValueComposite,
} KeyValueType;

typedef enum IndexType {
	IndexStrHash = 0,
	IndexStrBTree = 1,
	IndexIntBTree = 2,
	IndexIntHash = 3,
	IndexInt64BTree = 4,
	IndexInt64Hash = 5,
	IndexDoubleBTree = 6,
	IndexFullText = 7,
	IndexNewFullText = 8,
	IndexCompositeBTree = 9,
	IndexCompositeHash = 10,
	IndexCompositeText = 11,
	IndexBool = 12,
	IndexIntStore = 13,
	IndexInt64Store = 14,
	IndexStrStore = 15,
	IndexDoubleStore = 16,
	IndexCompositeNewText = 17,
} IndexType;

typedef enum QueryItemType {
	QueryCondition,
	QueryDistinct,
	QuerySortIndex,
	QueryJoinOn,
	QueryLimit,
	QueryOffset,
	QueryReqTotal,
	QueryDebugLevel,
	QueryAggregation,
	QuerySelectFilter,
	QueryEnd
} QueryItemType;

typedef enum QuerySerializeMode {
	Normal = 0x0,
	SkipJoinQueries = 0x01,
	SkipMergeQueries = 0x02,
	SkipLimitOffset = 0x04
} QuerySerializeMode;

typedef enum CondType {
	CondAny = 0,
	CondEq = 1,
	CondLt = 2,
	CondLe = 3,
	CondGt = 4,
	CondGe = 5,
	CondRange = 6,
	CondSet = 7,
	CondAllSet = 8,
	CondEmpty = 9,
} CondType;

enum ErrorCode { errOK = 0, errParseSQL, errQueryExec, errParams, errLogic, errParseJson, errParseDSL, errConflict };

enum OpType { OpOr = 1, OpAnd = 2, OpNot = 3 };

enum AggType { AggSum, AggAvg };

enum { TAG_VARINT, TAG_DOUBLE, TAG_STRING, TAG_ARRAY, TAG_BOOL, TAG_NULL, TAG_OBJECT, TAG_END };

enum JoinType { LeftJoin, InnerJoin, OrInnerJoin, Merge };

enum CollateMode { CollateNone = 0, CollateASCII, CollateUTF8, CollateNumeric };

enum CalcTotalMode { ModeNoTotal, ModeCachedTotal, ModeAccurateTotal };

typedef int IdType;
typedef unsigned SortType;

static const SortType SortIdUnfilled = -1;
static const SortType SortIdUnexists = -2;

typedef enum LogLevel { LogNone, LogError, LogWarning, LogInfo, LogTrace } LogLevel;

typedef struct IndexOpts {
#ifdef __cplusplus
	IndexOpts(bool array = false, bool pk = false, bool dense = false, bool appendable = false, int collateMode = CollateNone)
		: IsArray(array), IsPK(pk), IsDense(dense), IsAppendable(appendable), CollateMode(collateMode) {}

	// [[deprecated]]
	IndexOpts(const int src[3]) : IsArray(src[0]), IsPK(src[1]), IsDense(src[2]) {
		IsAppendable = 0;
		CollateMode = CollateNone;
	};
#endif
	int IsArray;
	int IsPK;
	int IsDense;
	int IsAppendable;
	int CollateMode;
} IndexOpts;

typedef struct StorageOpts {
#ifdef __cplusplus
	StorageOpts(bool enabled = true, bool dropOnIndexesConflict = false, bool dropOnFileFormatError = false, bool createIfMissing = true)
		: IsEnabled(enabled),
		  IsDropOnIndexesConflict(dropOnIndexesConflict),
		  IsDropOnFileFormatError(dropOnFileFormatError),
		  IsCreateIfMissing(createIfMissing) {}
#endif
	int IsEnabled;
	int IsDropOnIndexesConflict;
	int IsDropOnFileFormatError;
	int IsCreateIfMissing;
} StorageOpts;

typedef struct reindexer_stat {
	int count_get_item, time_get_item;
	int count_select, time_select;
	int count_insert, time_insert;
	int count_update, time_update;
	int count_upsert, time_upsert;
	int count_delete, time_delete;
	int count_join, time_join;
} reindexer_stat;
