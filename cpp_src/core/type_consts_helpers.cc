#include "type_consts_helpers.h"

#include "tools/errors.h"

namespace reindexer {

[[nodiscard]] CondType InvertJoinCondition(CondType cond) {
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
			break;
	}
	throw Error(errForbidden, "Not invertible conditional operator '%s(%d)' in query", CondTypeToStr(cond), cond);
}

[[nodiscard]] CondType InvertNotCondition(CondType cond) {
	switch (cond) {
		case CondGt:
			return CondLe;
		case CondLt:
			return CondGe;
		case CondGe:
			return CondLt;
		case CondLe:
			return CondGt;
		case CondSet:
		case CondEq:
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			break;
	}
	throw Error(errForbidden, "Not invertible conditional operator '%s(%d)' in query", CondTypeToStr(cond), cond);
}

[[nodiscard]] std::string_view CondTypeToStr(CondType t) {
	using namespace std::string_view_literals;
	switch (t) {
		case CondAny:
			return "CondAny"sv;
		case CondEq:
			return "CondEq"sv;
		case CondLt:
			return "CondLt"sv;
		case CondLe:
			return "CondLe"sv;
		case CondGt:
			return "CondGt"sv;
		case CondGe:
			return "CondGe"sv;
		case CondRange:
			return "CondRange"sv;
		case CondSet:
			return "CondSet"sv;
		case CondAllSet:
			return "CondAllSet"sv;
		case CondEmpty:
			return "CondEmpty"sv;
		case CondLike:
			return "CondLike"sv;
		case CondDWithin:
			return "CondDWithin"sv;
	}
	throw Error{errNotValid, "Invalid condition type: %d", t};
}

[[nodiscard]] std::string_view TagTypeToStr(TagType t) {
	using namespace std::string_view_literals;
	switch (t) {
		case TAG_VARINT:
			return "<varint>"sv;
		case TAG_OBJECT:
			return "<object>"sv;
		case TAG_END:
			return "<end>"sv;
		case TAG_ARRAY:
			return "<array>"sv;
		case TAG_BOOL:
			return "<bool>"sv;
		case TAG_STRING:
			return "<string>"sv;
		case TAG_DOUBLE:
			return "<double>"sv;
		case TAG_NULL:
			return "<null>"sv;
		case TAG_UUID:
			return "<uuid>"sv;
	}
	throw Error{errNotValid, "Invalid tag type: %d", t};
}

[[nodiscard]] std::string_view AggTypeToStr(AggType t) noexcept {
	using namespace std::string_view_literals;
	switch (t) {
		case AggMin:
			return "min"sv;
		case AggMax:
			return "max"sv;
		case AggSum:
			return "sum"sv;
		case AggAvg:
			return "avg"sv;
		case AggDistinct:
			return "distinct"sv;
		case AggFacet:
			return "facet"sv;
		case AggCount:
			return "count"sv;
		case AggCountCached:
			return "count_cached"sv;
		case AggUnknown:
			break;
	}
	return "unknown"sv;
}

}  // namespace reindexer

[[nodiscard]] std::string_view JoinTypeName(JoinType type) {
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
