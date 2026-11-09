#include "helpers.h"
#include "core/type_consts_helpers.h"
#include "estl/charset.h"

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

bool IsSortedByJoinedField(std::string_view sortExpr, std::string_view joinedNs) {
	constexpr static estl::Charset kJoinedIndexNameSyms{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
														'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
														'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
														'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', '+'};
	std::string_view::size_type i = 0;
	const auto s = sortExpr.size();
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	bool inQuotes = false;
	if (i < s && sortExpr[i] == '"') {
		++i;
		inQuotes = true;
	}
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	std::string_view::size_type j = 0, s2 = joinedNs.size();
	for (; j < s2 && i < s; ++i, ++j) {
		if (tolower(sortExpr[i]) != tolower(joinedNs[j])) {
			return false;
		}
	}
	if (i >= s || sortExpr[i] != '.') {
		return false;
	}
	for (++i; i < s; ++i) {
		if (!kJoinedIndexNameSyms.test(sortExpr[i])) {
			if (isspace(sortExpr[i])) {
				break;
			}
			if (inQuotes && sortExpr[i] == '"') {
				inQuotes = false;
				++i;
				break;
			}
			return false;
		}
	}
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	if (inQuotes && i < s && sortExpr[i] == '"') {
		++i;
	}
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	return i == s;
}

}  // namespace reindexer::joins
