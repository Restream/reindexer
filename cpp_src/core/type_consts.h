#pragma once

#include <stdint.h>

typedef enum TagType {
	TAG_VARINT = 0,
	TAG_DOUBLE = 1,
	TAG_STRING = 2,
	TAG_BOOL = 3,
	TAG_NULL = 4,
	TAG_ARRAY = 5,
	TAG_OBJECT = 6,
	TAG_END = 7,
	TAG_UUID = 8,
} TagType;

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
	IndexTtl = 18,
	IndexRTree = 19,
	IndexUuidHash = 20,
	IndexUuidStore = 21,
} IndexType;

typedef enum QueryItemType {
	QueryCondition = 0,
	QueryDistinct = 1,
	QuerySortIndex = 2,
	QueryJoinOn = 3,
	QueryLimit = 4,
	QueryOffset = 5,
	QueryReqTotal = 6,
	QueryDebugLevel = 7,
	QueryAggregation = 8,
	QuerySelectFilter = 9,
	QuerySelectFunction = 10,
	QueryEnd = 11,
	QueryExplain = 12,
	QueryEqualPosition = 13,
	QueryUpdateField = 14,
	QueryAggregationLimit = 15,
	QueryAggregationOffset = 16,
	QueryAggregationSort = 17,
	QueryOpenBracket = 18,
	QueryCloseBracket = 19,
	QueryJoinCondition = 20,
	QueryDropField = 21,
	QueryUpdateObject = 22,
	QueryWithRank = 23,
	QueryStrictMode = 24,
	QueryUpdateFieldV2 = 25,
	QueryBetweenFieldsCondition = 26,
	QueryAlwaysFalseCondition = 27,
	QueryAlwaysTrueCondition = 28,
	QuerySubQueryCondition = 29,
	QueryFieldSubQueryCondition = 30,
	QueryLocal = 31,
} QueryItemType;

typedef enum QuerySerializeMode {
	Normal = 0x0,
	SkipJoinQueries = 0x01,
	SkipMergeQueries = 0x02,
	SkipLimitOffset = 0x04,
	WithJoinEntries = 0x08,
	SkipAggregations = 0x10,
	SkipSortEntries = 0x20,
	SkipExtraParams = 0x40,
	SkipLeftJoinQueries = 0x80,
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
	CondLike = 10,
	CondDWithin = 11,
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
	errStateInvalidated = 14,
	errBadTransaction = 15,
	errOutdatedWAL = 16,
	errNoWAL = 17,
	errDataHashMismatch = 18,
	errTimeout = 19,
	errCanceled = 20,
	errTagsMissmatch = 21,
	errReplParams = 22,
	errNamespaceInvalidated = 23,
	errParseMsgPack = 24,
	errParseProtobuf = 25,
	errUpdatesLost = 26,
	errWrongReplicationData = 27,
	errUpdateReplication = 28,
	errClusterConsensus = 29,
	errTerminated = 30,
	errTxDoesNotExist = 31,
	errAlreadyConnected = 32,
	errTxInvalidLeader = 33,
	errAlreadyProxied = 34,
	errStrictMode = 35,
	errQrUIDMissmatch = 36,
	errSystem = 37,
	errAssert = 38,
	errParseYAML = 39,
	errNamespaceOverwritten = 40
};

enum SchemaType { JsonSchemaType, ProtobufSchemaType };

enum QueryType { QuerySelect, QueryDelete, QueryUpdate, QueryTruncate };

enum OpType { OpOr = 1, OpAnd = 2, OpNot = 3 };

enum ArithmeticOpType { OpPlus = 0, OpMinus = 1, OpMult = 2, OpDiv = 3 };

enum AggType { AggSum, AggAvg, AggFacet, AggMin, AggMax, AggDistinct, AggCount, AggCountCached, AggUnknown = -1 };

enum JoinType { LeftJoin, InnerJoin, OrInnerJoin, Merge };

enum CalcTotalMode { ModeNoTotal, ModeCachedTotal, ModeAccurateTotal };

enum DataFormat { FormatJson, FormatCJson, FormatMsgPack };

enum QueryResultItemType {
	QueryResultEnd = 0,
	QueryResultAggregation = 1,
	QueryResultExplain = 2,
	QueryResultShardingVersion = 3,
	QueryResultShardId = 4,
	QueryResultIncarnationTags = 5,
};

enum CacheMode { CacheModeOn = 0, CacheModeAggressive = 1, CacheModeOff = 2 };

enum StrictMode { StrictModeNotSet = 0, StrictModeNone, StrictModeNames, StrictModeIndexes };

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
	kResultsMsgPack = 0x4,

	kResultsWithPayloadTypes = 0x10,
	kResultsWithItemID = 0x20,
	kResultsWithRank = 0x40,
	kResultsWithNsID = 0x80,
	kResultsWithJoined = 0x100,
	kResultsWithRaw = 0x200,
	kResultsNeedOutputRank = 0x400,
	kResultsWithShardId = 0x800,
	kResultsNeedOutputShardId = 0x1000,
	kResultsSupportIdleTimeout = 0x2000,  // Deprecated. Use BindingCapabilities instead

	kResultsFlagMaxValue
};

typedef enum IndexOpt {
	kIndexOptPK = 1 << 7,
	kIndexOptArray = 1 << 6,
	kIndexOptDense = 1 << 5,
	kIndexOptSparse = 1 << 3,
} IndexOpt;

typedef enum StotageOpt {
	kStorageOptEnabled = 1 << 0,
	kStorageOptDropOnFileFormatError = 1 << 1,
	kStorageOptCreateIfMissing = 1 << 2,
	kStorageOptVerifyChecksums = 1 << 3,
	kStorageOptFillCache = 1 << 4,
	kStorageOptSync = 1 << 5,
	kStorageOptLazyLoad = 1 << 6,
	kStorageOptAutorepair = 1 << 9,
} StorageOpt;

enum CollateMode { CollateNone = 0, CollateASCII, CollateUTF8, CollateNumeric, CollateCustom };

enum FieldModifyMode {
	FieldModeSet = 0,
	FieldModeDrop = 1,
	FieldModeSetJson = 2,
	FieldModeArrayPushBack = 3,
	FieldModeArrayPushFront = 4,
};

enum ItemModifyMode { ModeUpdate = 0, ModeInsert = 1, ModeUpsert = 2, ModeDelete = 3 };

typedef struct StorageOpts {
#ifdef __cplusplus
	constexpr StorageOpts() noexcept : options(0), noQueryIdleThresholdSec(0) {}

	bool IsEnabled() const noexcept { return options & kStorageOptEnabled; }
	bool IsDropOnFileFormatError() const noexcept { return options & kStorageOptDropOnFileFormatError; }
	bool IsCreateIfMissing() const noexcept { return options & kStorageOptCreateIfMissing; }
	bool IsVerifyChecksums() const noexcept { return options & kStorageOptVerifyChecksums; }
	bool IsFillCache() const noexcept { return options & kStorageOptFillCache; }
	bool IsSync() const noexcept { return options & kStorageOptSync; }
	bool IsLazyLoad() const noexcept { return options & kStorageOptLazyLoad; }
	bool IsAutorepair() const noexcept { return options & kStorageOptAutorepair; }

	StorageOpts& Enabled(bool value = true) noexcept {
		options = value ? options | kStorageOptEnabled : options & ~(kStorageOptEnabled);
		return *this;
	}

	StorageOpts& DropOnFileFormatError(bool value = true) noexcept {
		options = value ? options | kStorageOptDropOnFileFormatError : options & ~(kStorageOptDropOnFileFormatError);
		return *this;
	}

	StorageOpts& CreateIfMissing(bool value = true) noexcept {
		options = value ? options | kStorageOptCreateIfMissing : options & ~(kStorageOptCreateIfMissing);
		return *this;
	}

	StorageOpts& VerifyChecksums(bool value = true) {
		options = value ? options | kStorageOptVerifyChecksums : options & ~(kStorageOptVerifyChecksums);
		return *this;
	}

	StorageOpts& FillCache(bool value = true) noexcept {
		options = value ? options | kStorageOptFillCache : options & ~(kStorageOptFillCache);
		return *this;
	}

	StorageOpts& Sync(bool value = true) noexcept {
		options = value ? options | kStorageOptSync : options & ~(kStorageOptSync);
		return *this;
	}

	StorageOpts& LazyLoad(bool value = true) noexcept {
		options = value ? options | kStorageOptLazyLoad : options & ~(kStorageOptLazyLoad);
		return *this;
	}

	StorageOpts& Autorepair(bool value = true) noexcept {
		options = value ? options | kStorageOptAutorepair : options & ~(kStorageOptAutorepair);
		return *this;
	}
#endif
	uint16_t options;
	uint16_t noQueryIdleThresholdSec;
} StorageOpts;

typedef enum ConnectOpt {
	kConnectOptOpenNamespaces = 1,
	kConnectOptAllowNamespaceErrors = 1 << 1,
	kConnectOptAutorepair = 1 << 2,
	kConnectOptCheckClusterID = 1 << 3,
	kConnectOptWarnVersion = 1 << 4,
	kConnectOptDisableReplication = 1 << 5,
} ConnectOpt;

typedef enum StorageTypeOpt {
	kStorageTypeOptLevelDB = 0,
	kStorageTypeOptRocksDB = 1,
} StorageTypeOpt;

typedef struct ConnectOpts {
#ifdef __cplusplus
	constexpr ConnectOpts() noexcept : storage(kStorageTypeOptLevelDB), options(kConnectOptOpenNamespaces), expectedClusterID(-1) {}

	bool IsOpenNamespaces() const noexcept { return options & kConnectOptOpenNamespaces; }
	bool IsAllowNamespaceErrors() const noexcept { return options & kConnectOptAllowNamespaceErrors; }
	bool IsAutorepair() const noexcept { return options & kConnectOptAutorepair; }
	StorageTypeOpt StorageType() const noexcept {
		if (storage == static_cast<uint16_t>(kStorageTypeOptRocksDB)) {
			return kStorageTypeOptRocksDB;
		}
		return kStorageTypeOptLevelDB;
	}
	int ExpectedClusterID() const noexcept { return expectedClusterID; }
	bool HasExpectedClusterID() const noexcept { return options & kConnectOptCheckClusterID; }
	bool IsReplicationDisabled() const noexcept { return options & kConnectOptDisableReplication; }

	ConnectOpts& OpenNamespaces(bool value = true) noexcept {
		options = value ? options | kConnectOptOpenNamespaces : options & ~(kConnectOptOpenNamespaces);
		return *this;
	}

	ConnectOpts& AllowNamespaceErrors(bool value = true) noexcept {
		options = value ? options | kConnectOptAllowNamespaceErrors : options & ~(kConnectOptAllowNamespaceErrors);
		return *this;
	}

	ConnectOpts& Autorepair(bool value = true) noexcept {
		options = value ? options | kConnectOptAutorepair : options & ~(kConnectOptAutorepair);
		return *this;
	}

	ConnectOpts& WithStorageType(StorageTypeOpt type) noexcept {
		storage = static_cast<uint16_t>(type);
		return *this;
	}

	ConnectOpts& WithExpectedClusterID(int clusterID) noexcept {
		expectedClusterID = clusterID;
		options |= kConnectOptCheckClusterID;
		return *this;
	}

	ConnectOpts& DisableReplication(bool value = true) noexcept {
		options = value ? options | kConnectOptDisableReplication : options & ~(kConnectOptDisableReplication);
		return *this;
	}
#endif
	uint16_t storage;
	uint16_t options;
	int expectedClusterID;
} ConnectOpts;

enum IndexValueType { NotSet = -1, SetByJsonPath = -2 };
enum ShardingAlgorithmType { ByValue, ByRange };

enum BindingCapability {
	kBindingCapabilityQrIdleTimeouts = 1,
	kBindingCapabilityResultsWithShardIDs = 1 << 1,
	kBindingCapabilityIncarnationTags = 1 << 2,
};

typedef struct BindingCapabilities {
#ifdef __cplusplus
	constexpr BindingCapabilities(int64_t c = 0) noexcept : caps(c) {}

	bool HasQrIdleTimeouts() const noexcept { return caps & kBindingCapabilityQrIdleTimeouts; }
	bool HasResultsWithShardIDs() const noexcept { return caps & kBindingCapabilityResultsWithShardIDs; }
	bool HasIncarnationTags() const noexcept { return caps & kBindingCapabilityIncarnationTags; }
#endif
	int64_t caps;
} BindingCapabilities;

typedef struct RPCQrId {
#ifdef __cplusplus
	explicit RPCQrId(int m = -1, int64_t u = -1) noexcept : main(m), uid(u) {}
#endif
	int main;
	int64_t uid;
} RpcQrId;

#ifdef __cplusplus
namespace ShardingKeyType {
enum ShardingKey { ProxyOff = -2, NotSetShard = -1, NotSharded = -3 };
}
namespace ShardingSourceId {
enum SourceId { NotSet = -1 };
}
#endif

static const char kSerialPrefix[] = "_SERIAL_";

// REINDEX_WITH_V3_FOLLOWERS
enum SubscriptionOpt {
	kSubscriptionOptIncrementSubscription = 1 << 0,
};

typedef struct SubscriptionOpts {
#ifdef __cplusplus
	SubscriptionOpts() : options(0) {}

	bool IsIncrementSubscription() const { return options & kSubscriptionOptIncrementSubscription; }
	SubscriptionOpts& IncrementSubscription(bool value = true) {
		options = value ? options | kSubscriptionOptIncrementSubscription : options & ~(kSubscriptionOptIncrementSubscription);
		return *this;
	}
#endif
	uint16_t options;
} SubscriptionOpts;
// REINDEX_WITH_V3_FOLLOWERS
