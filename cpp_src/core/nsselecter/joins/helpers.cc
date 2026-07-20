#include "helpers.h"
#include "core/type_consts_helpers.h"

namespace reindexer::joins {

CondType InvertJoinCondition(CondType cond) {
	switch (cond) {
		case CondSet:
			return CondSet;
		case CondEq:
			return CondEq;
		case CondGt:
			return CondLt;
		case CondLt:
			return CondGt;
		case CondGe:
			return CondLe;
		case CondLe:
			return CondGe;
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			break;
	}
	throw Error(errForbidden, "Not invertible conditional operator '{}({})' in query", CondTypeToStr(cond), CondTypeToStrShort(cond));
}

std::string_view JoinTypeName(JoinType type) noexcept {
	using namespace std::string_view_literals;

	switch (type) {
		case JoinType::InnerJoin:
			return "INNER JOIN"sv;
		case JoinType::OrInnerJoin:
			return "OR INNER JOIN"sv;
		case JoinType::LeftJoin:
			return "LEFT JOIN"sv;
		case JoinType::Merge:
			return "MERGE"sv;
	}
	assertrx(false);
	return "unknown"sv;
}

}  // namespace reindexer::joins
