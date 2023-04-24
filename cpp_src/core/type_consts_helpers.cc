#include "type_consts_helpers.h"
#include "tools/errors.h"

namespace reindexer {

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
		default:
			throw Error(errForbidden, "Not invertible conditional operator '%s(%d)' in query", CondTypeToStr(cond), cond);
	}
}

std::string_view CondTypeToStr(CondType t) noexcept {
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
		default:
			return "Unknown"sv;
	}
}

std::string_view AggTypeToStr(AggType t) noexcept {
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
		default:
			return "unknown"sv;
	}
}

}  // namespace reindexer
