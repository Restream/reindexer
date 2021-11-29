#pragma once

#include <cstdlib>
#include <string_view>
#include "type_consts.h"

namespace reindexer {

CondType InvertJoinCondition(CondType cond);
std::string_view CondTypeToStr(CondType t);
std::string KeyValueTypeToStr(KeyValueType);

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
		default:
			abort();
	}
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
		default:
			abort();
	}
}

template <typename T>
auto& operator<<(T& os, JoinType jt) {
	switch (jt) {
		case LeftJoin:
			return os << "LEFT JOIN";
		case InnerJoin:
			return os << "INNER JOIN";
		case OrInnerJoin:
			return os << "OR INNER JOIN";
		case Merge:
			return os << "MERGE";
		default:
			abort();
	}
}

}  // namespace reindexer
