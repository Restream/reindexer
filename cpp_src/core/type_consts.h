#pragma once

#include <stdint.h>

enum { TAG_VARINT, TAG_DOUBLE, TAG_STRING, TAG_BOOL, TAG_NULL, TAG_ARRAY, TAG_OBJECT, TAG_END };

typedef enum KeyValueType {
	KeyValueInt64 = TAG_VARINT,
	KeyValueDouble = TAG_DOUBLE,
	KeyValueString = TAG_STRING,
	KeyValueBool = TAG_BOOL,
	KeyValueNull = TAG_NULL,
	KeyValueInt = TAG_END + 1,
	KeyValueUndefined,
	KeyValueComposite,
	KeyValueTuple,
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
	QueryEnd,
	QueryExplain,
	QueryEqualPosition,
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
	errParseSQL = 1,
	errQueryExec = 2,
	errParams = 3,
	errLogic = 4,
	errParseJson = 5,
	errParseDSL = 6,
	errConflict = 7,
	errParseBin = 8,
	errForbidden = 9,
	errWasRelock = 10,
	errNotValid = 11,
	errNetwork = 12,
	errNotFound = 13,
	errStateInvalidated = 14
};

enum OpType { OpOr = 1, OpAnd = 2, OpNot = 3 };

enum AggType { AggSum, AggAvg, AggFacet, AggMin, AggMax };

enum JoinType { LeftJoin, InnerJoin, OrInnerJoin, Merge };

enum CalcTotalMode { ModeNoTotal, ModeCachedTotal, ModeAccurateTotal };

enum DataFormat { FormatJson, FormatCJson };

enum QueryResultItemType { QueryResultEnd, QueryResultAggregation, QueryResultExplain };

enum CacheMode { CacheModeOn = 0, CacheModeAggressive = 1, CacheModeOff = 2 };

typedef int IdType;
typedef unsigned SortType;

static const SortType SortIdUnfilled = -1;
static const SortType SortIdUnexists = -2;

typedef enum LogLevel { LogNone, LogError, LogWarning, LogInfo, LogTrace } LogLevel;

enum {
	kResultsFormatMask = 0xF,
	kResultsPure = 0x0,
	kResultsPtrs = 0x1,
	kResultsCJson = 0x2,
	kResultsJson = 0x3,

	kResultsWithPayloadTypes = 0x10,
	kResultsWithItemID = 0x20,
	kResultsWithPercents = 0x40,
	kResultsWithNsID = 0x80,
	kResultsWithJoined = 0x100
};

typedef enum IndexOpt { kIndexOptPK = 1 << 7, kIndexOptArray = 1 << 6, kIndexOptDense = 1 << 5, kIndexOptSparse = 1 << 3 } IndexOpt;

typedef enum StotageOpt {
	kStorageOptEnabled = 1 << 0,
	kStorageOptDropOnFileFormatError = 1 << 1,
	kStorageOptCreateIfMissing = 1 << 2,
	kStorageOptVerifyChecksums = 1 << 3,
	kStorageOptFillCache = 1 << 4,
	kStorageOptSync = 1 << 5
} StorageOpt;

enum CollateMode { CollateNone = 0, CollateASCII, CollateUTF8, CollateNumeric, CollateCustom };

enum ItemModifyMode { ModeUpdate = 0, ModeInsert = 1, ModeUpsert = 2, ModeDelete = 3 };

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
