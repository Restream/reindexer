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
	IndexHash,
	IndexTree,
	IndexInt,
	IndexIntHash,
	IndexInt64,
	IndexInt64Hash,
	IndexDouble,
	IndexFullText,
	IndexNewFullText,
	IndexComposite,
	IndexCompositeHash,
	IndexBool,
	IndexIntStore,
	IndexInt64Store,
	IndexStrStore,
	IndexDoubleStore
} IndexType;

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

enum OpType { OpOr = 1, OpAnd = 2, OpNot = 3 };

typedef int IdType;
typedef unsigned SortType;

static const SortType SortIdUnfilled = (SortType)-1;
static const SortType SortIdUnexists = (SortType)-2;

typedef enum LogLevel { LogNone, LogError, LogWarning, LogInfo, LogTrace } LogLevel;

typedef struct IndexOpts {
	int IsArray;
	int IsPK;
} IndexOpts;
