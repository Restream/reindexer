#include "condition_inversion.h"
#include "core/type_consts_helpers.h"
#include "tools/errors.h"

namespace reindexer {

std::optional<CondType> TryInvertCondition(CondType cond) noexcept {
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
	return std::nullopt;
}

CondType InvertCondition(CondType cond) {
	if (const auto inverted = TryInvertCondition(cond)) {
		return *inverted;
	}
	throw Error(errForbidden, "Not invertible conditional operator '{}({})' in query", CondTypeToStr(cond), CondTypeToStrShort(cond));
}

CondType InvertLeftValueCondition(CondType cond) {
	if (const auto inverted = TryInvertCondition(cond)) [[likely]] {
		return *inverted;
	}
	throw Error(errParams, "Condition {} is not supported with value on the left side", CondTypeToStrShort(cond));
}

}  // namespace reindexer
