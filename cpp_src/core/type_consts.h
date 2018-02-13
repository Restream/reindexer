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

enum ErrorCode { errOK = 0, errParseSQL, errQueryExec, errParams, errLogic, errParseJson, errParseDSL, errConflict, errParseBin };

enum OpType { OpOr = 1, OpAnd = 2, OpNot = 3 };

enum AggType { AggSum, AggAvg };

enum { TAG_VARINT, TAG_DOUBLE, TAG_STRING, TAG_ARRAY, TAG_BOOL, TAG_NULL, TAG_OBJECT, TAG_END };

enum JoinType { LeftJoin, InnerJoin, OrInnerJoin, Merge };

enum CollateMode { CollateNone = 0, CollateASCII, CollateUTF8, CollateNumeric };

enum CalcTotalMode { ModeNoTotal, ModeCachedTotal, ModeAccurateTotal };

enum DataFormat { FormatJson, FormatCJson };

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
	kResultsClose = 0x10
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

typedef struct IndexOpts {
#ifdef __cplusplus
	IndexOpts(uint8_t flags = 0, CollateMode mode = CollateNone) : options(flags), collate(mode) {}

	bool IsPK() const { return options & kIndexOptPK; }
	bool IsArray() const { return options & kIndexOptArray; }
	bool IsDense() const { return options & kIndexOptDense; }
	bool IsAppendable() const { return options & kIndexOptAppendable; }
	CollateMode GetCollateMode() const { return static_cast<CollateMode>(collate); }

	IndexOpts& PK(bool value = true) {
		options = value ? options | kIndexOptPK : options & ~(kIndexOptPK);
		return *this;
	}
	IndexOpts& Array(bool value = true) {
		options = value ? options | kIndexOptArray : options & ~(kIndexOptArray);
		return *this;
	}
	IndexOpts& Dense(bool value = true) {
		options = value ? options | kIndexOptDense : options & ~(kIndexOptDense);
		return *this;
	}
	IndexOpts& Appendable(bool value = true) {
		options = value ? options | kIndexOptAppendable : options & ~(kIndexOptAppendable);
		return *this;
	}
	IndexOpts& SetCollateMode(CollateMode mode) {
		collate = mode;
		return *this;
	}
#endif
	uint8_t options;
	uint8_t collate;
} IndexOpts;

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
