#pragma once

#include <cstdlib>
#include <string_view>
#include "enums.h"
#include "type_consts.h"

namespace reindexer {

CondType InvertJoinCondition(CondType);
CondType InvertNotCondition(CondType);
std::string_view CondTypeToStr(CondType);
std::string_view CondTypeToStrShort(CondType);
std::string_view TagTypeToStr(TagType);
std::string_view AggTypeToStr(AggType) noexcept;
QueryRankType ToQueryRankType(VectorMetric);

constexpr bool IsComposite(IndexType type) noexcept {
	switch (type) {
		case IndexCompositeBTree:
		case IndexCompositeFastFT:
		case IndexCompositeFuzzyFT:
		case IndexCompositeHash:
			return true;
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexFastFT:
		case IndexFuzzyFT:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
		case IndexDummy:
		default:
			return false;
	}
}

constexpr bool IsFullText(IndexType type) noexcept {
	switch (type) {
		case IndexFastFT:
		case IndexFuzzyFT:
		case IndexCompositeFastFT:
		case IndexCompositeFuzzyFT:
			return true;
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
		case IndexDummy:
		default:
			return false;
	}
}

constexpr bool IsFastFullText(IndexType type) noexcept {
	switch (type) {
		case IndexFastFT:
		case IndexCompositeFastFT:
			return true;
		case IndexFuzzyFT:
		case IndexCompositeFuzzyFT:
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
		case IndexDummy:
		default:
			return false;
	}
}

constexpr bool IsFloatVector(IndexType type) noexcept {
	switch (type) {
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
			return true;
		case IndexCompositeBTree:
		case IndexCompositeFastFT:
		case IndexCompositeFuzzyFT:
		case IndexCompositeHash:
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexFastFT:
		case IndexFuzzyFT:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
		case IndexDummy:
		default:
			return false;
	}
}

/// Get readable Join Type
/// @param type - join type
/// @returns string with join type name
std::string_view JoinTypeName(JoinType type);

}  // namespace reindexer

template <typename T>
auto& operator<<(T& os, CondType cond) {
	return os << reindexer::CondTypeToStrShort(cond);
}

template <typename T>
auto& operator<<(T& os, OpType op) {
	switch (op) {
		case OpOr:
			return os << "OR";
		case OpAnd:
			return os << "AND";
		case OpNot:
			return os << "NOT";
	}
	std::abort();
}

inline std::string_view OpTypeToStr(OpType op) {
	using namespace std::string_view_literals;
	switch (op) {
		case OpOr:
			return "OR"sv;
		case OpAnd:
			return "AND"sv;
		case OpNot:
			return "NOT"sv;
	}
	std::abort();
}

template <typename T>
auto& operator<<(T& os, JoinType jt) {
	return os << reindexer::JoinTypeName(jt);
}

template <typename T>
T& operator<<(T& os, IndexType it) {
	using namespace std::string_view_literals;
	switch (it) {
		case IndexStrHash:
			return os << "StrHash"sv;
		case IndexStrBTree:
			return os << "StrBTree"sv;
		case IndexIntBTree:
			return os << "IntBTree"sv;
		case IndexIntHash:
			return os << "IntHash"sv;
		case IndexInt64BTree:
			return os << "Int64BTree"sv;
		case IndexInt64Hash:
			return os << "Int64Hash"sv;
		case IndexDoubleBTree:
			return os << "DoubleBtree"sv;
		case IndexFastFT:
			return os << "FastFT"sv;
		case IndexFuzzyFT:
			return os << "FuzzyFT"sv;
		case IndexCompositeBTree:
			return os << "CompositeBTree"sv;
		case IndexCompositeHash:
			return os << "CompositeHash"sv;
		case IndexCompositeFastFT:
			return os << "CompositeFastHash"sv;
		case IndexBool:
			return os << "Bool"sv;
		case IndexIntStore:
			return os << "IntStore"sv;
		case IndexInt64Store:
			return os << "Int64Store"sv;
		case IndexStrStore:
			return os << "StrStore"sv;
		case IndexDoubleStore:
			return os << "DoubleStore"sv;
		case IndexCompositeFuzzyFT:
			return os << "CompositeFuzzyFT"sv;
		case IndexTtl:
			return os << "Ttl"sv;
		case ::IndexRTree:
			return os << "RTree"sv;
		case IndexUuidHash:
			return os << "UuidHash"sv;
		case IndexUuidStore:
			return os << "UuidStore"sv;
		case IndexHnsw:
			return os << "Hnsw"sv;
		case IndexVectorBruteforce:
			return os << "VectorBruteforce"sv;
		case IndexIvf:
			return os << "Ivf"sv;
		case IndexDummy:
			return os << "Dummy"sv;
	}
	std::abort();
}

template <typename T>
T& operator<<(T& os, CollateMode m) {
	switch (m) {
		case CollateNone:
			return os << "None";
		case CollateASCII:
			return os << "ASCII";
		case CollateUTF8:
			return os << "UTF8";
		case CollateNumeric:
			return os << "Numeric";
		case CollateCustom:
			return os << "Custom";
	}
	std::abort();
}
