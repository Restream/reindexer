#pragma once

#include <stdint.h>

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
	IndexFastFT = 7,
	IndexFuzzyFT = 8,
	IndexCompositeBTree = 9,
	IndexCompositeHash = 10,
	IndexCompositeFastFT = 11,
	IndexBool = 12,
	IndexIntStore = 13,
	IndexInt64Store = 14,
	IndexStrStore = 15,
	IndexDoubleStore = 16,
	IndexCompositeFuzzyFT = 17,
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
	QuerySelectFunction,
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

enum ErrorCode {
	errOK = 0,
	errParseSQL,
	errQueryExec,
	errParams,
	errLogic,
	errParseJson,
	errParseDSL,
	errConflict,
	errParseBin,
	errForbidden,
	errWasRelock,
	errNotValid,
};

enum OpType { OpOr = 1, OpAnd = 2, OpNot = 3 };

enum AggType { AggSum, AggAvg };

enum { TAG_VARINT, TAG_DOUBLE, TAG_STRING, TAG_ARRAY, TAG_BOOL, TAG_NULL, TAG_OBJECT, TAG_END };

enum JoinType { LeftJoin, InnerJoin, OrInnerJoin, Merge };

enum CalcTotalMode { ModeNoTotal, ModeCachedTotal, ModeAccurateTotal };

enum DataFormat { FormatJson, FormatCJson };

enum CacheMode { CacheModeOn = 0, CacheModeAggressive = 1, CacheModeOff = 2 };

typedef int IdType;
typedef unsigned SortType;

static const SortType SortIdUnfilled = -1;
static const SortType SortIdUnexists = -2;

typedef enum LogLevel { LogNone, LogError, LogWarning, LogInfo, LogTrace } LogLevel;

enum {
	kResultsPure = 0x0,
	kResultsWithPtrs = 0x1,
	kResultsWithCJson = 0x2,
	kResultsWithJson = 0x3,
	kResultsWithPayloadTypes = 0x8,
};

typedef enum IndexOpt { kIndexOptPK = 1 << 7, kIndexOptArray = 1 << 6, kIndexOptDense = 1 << 5, kIndexOptAppendable = 1 << 4 } IndexOpt;

typedef enum StotageOpt {
	kStorageOptEnabled = 1 << 0,
	kStorageOptDropOnFileFormatError = 1 << 1,
	kStorageOptCreateIfMissing = 1 << 2,
	kStorageOptVerifyChecksums = 1 << 3,
	kStorageOptFillCache = 1 << 4,
	kStorageOptSync = 1 << 5
} StorageOpt;

enum CollateMode { CollateNone = 0, CollateASCII, CollateUTF8, CollateNumeric, CollateCustom };

enum { ModeUpdate = 0, ModeInsert = 1, ModeUpsert = 2, ModeDelete = 3 };

typedef struct IndexOptsC {
	uint8_t options;
	uint8_t collate;
	const char* sortOrderLetters;
} IndexOptsC;

typedef struct StorageOpts {
#ifdef __cplusplus
	StorageOpts() : options(0) {}

	bool IsEnabled() const { return options & kStorageOptEnabled; }
	bool IsDropOnFileFormatError() const { return options & kStorageOptDropOnFileFormatError; }
	bool IsCreateIfMissing() const { return options & kStorageOptCreateIfMissing; }
	bool IsVerifyChecksums() const { return options & kStorageOptVerifyChecksums; }
	bool IsFillCache() const { return options & kStorageOptFillCache; }
	bool IsSync() const { return options & kStorageOptSync; }

	StorageOpts& Enabled(bool value = true) {
		options = value ? options | kStorageOptEnabled : options & ~(kStorageOptEnabled);
		return *this;
	}

	StorageOpts& DropOnFileFormatError(bool value = true) {
		options = value ? options | kStorageOptDropOnFileFormatError : options & ~(kStorageOptDropOnFileFormatError);
		return *this;
	}

	StorageOpts& CreateIfMissing(bool value = true) {
		options = value ? options | kStorageOptCreateIfMissing : options & ~(kStorageOptCreateIfMissing);
		return *this;
	}

	StorageOpts& VerifyChecksums(bool value = true) {
		options = value ? options | kStorageOptVerifyChecksums : options & ~(kStorageOptVerifyChecksums);
		return *this;
	}

	StorageOpts& FillCache(bool value = true) {
		options = value ? options | kStorageOptFillCache : options & ~(kStorageOptFillCache);
		return *this;
	}

	StorageOpts& Sync(bool value = true) {
		options = value ? options | kStorageOptSync : options & ~(kStorageOptSync);
		return *this;
	}
#endif
	uint8_t options;
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
