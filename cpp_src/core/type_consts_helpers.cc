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

std::string_view CondTypeToStr(CondType t) {
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

std::string KeyValueTypeToStr(KeyValueType t) {
	using namespace std::string_literals;
	switch (t) {
		case KeyValueInt64:
			return "int64"s;
		case KeyValueDouble:
			return "double"s;
		case KeyValueString:
			return "string"s;
		case KeyValueBool:
			return "bool"s;
		case KeyValueNull:
			return "null"s;
		case KeyValueInt:
			return "int"s;
		case KeyValueUndefined:
			return "undefined"s;
		case KeyValueComposite:
			return "composite"s;
		case KeyValueTuple:
			return "tuple"s;
		default:
			abort();
	}
}

}  // namespace reindexer
