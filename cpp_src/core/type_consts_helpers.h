#pragma once

#include <cstdlib>
#include <string_view>
#include "type_consts.h"

namespace reindexer {

[[nodiscard]] CondType InvertJoinCondition(CondType);
[[nodiscard]] CondType InvertNotCondition(CondType);
[[nodiscard]] std::string_view CondTypeToStr(CondType);
[[nodiscard]] std::string_view TagTypeToStr(TagType);
[[nodiscard]] std::string_view AggTypeToStr(AggType) noexcept;

constexpr bool IsComposite(IndexType type) noexcept {
	return type == IndexCompositeBTree || type == IndexCompositeFastFT || type == IndexCompositeFuzzyFT || type == IndexCompositeHash;
}

constexpr bool IsFullText(IndexType type) noexcept {
	return type == IndexFastFT || type == IndexFuzzyFT || type == IndexCompositeFastFT || type == IndexCompositeFuzzyFT;
}

constexpr bool IsFastFullText(IndexType type) noexcept { return type == IndexFastFT || type == IndexCompositeFastFT; }

}  // namespace reindexer

/// Get readable Join Type
/// @param type - join type
/// @returns string with join type name
[[nodiscard]] std::string_view JoinTypeName(JoinType type);

template <typename T>
auto& operator<<(T& os, CondType cond) {
	switch (cond) {
		case CondAny:
			return os << "IS NOT NULL";
		case CondEq:
			return os << '=';
		case CondLt:
			return os << '<';
		case CondLe:
			return os << "<=";
		case CondGt:
			return os << '>';
		case CondGe:
			return os << ">=";
		case CondRange:
			return os << "RANGE";
		case CondSet:
			return os << "IN";
		case CondAllSet:
			return os << "ALLSET";
		case CondEmpty:
			return os << "IS NULL";
		case CondLike:
			return os << "LIKE";
		case CondDWithin:
			return os << "DWITHIN";
	}
	std::abort();
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
	return os << JoinTypeName(jt);
}

template <typename T>
T& operator<<(T& os, IndexType it) {
	switch (it) {
		case IndexStrHash:
			return os << "StrHash";
		case IndexStrBTree:
			return os << "StrBTree";
		case IndexIntBTree:
			return os << "IntBTree";
		case IndexIntHash:
			return os << "IntHash";
		case IndexInt64BTree:
			return os << "Int64BTree";
		case IndexInt64Hash:
			return os << "Int64Hash";
		case IndexDoubleBTree:
			return os << "DoubleBtree";
		case IndexFastFT:
			return os << "FastFT";
		case IndexFuzzyFT:
			return os << "FuzzyFT";
		case IndexCompositeBTree:
			return os << "CompositeBTree";
		case IndexCompositeHash:
			return os << "CompositeHash";
		case IndexCompositeFastFT:
			return os << "CompositeFastHash";
		case IndexBool:
			return os << "Bool";
		case IndexIntStore:
			return os << "IntStore";
		case IndexInt64Store:
			return os << "Int64Store";
		case IndexStrStore:
			return os << "StrStore";
		case IndexDoubleStore:
			return os << "DoubleStore";
		case IndexCompositeFuzzyFT:
			return os << "CompositeFuzzyFT";
		case IndexTtl:
			return os << "Ttl";
		case ::IndexRTree:
			return os << "RTree";
		case IndexUuidHash:
			return os << "UuidHash";
		case IndexUuidStore:
			return os << "UuidStore";
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
