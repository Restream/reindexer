#pragma once

#include <cmath>
#include "variant.h"

namespace reindexer {

RX_ALWAYS_INLINE bool IsNaN(const Variant& value) noexcept {
	return (value.Type().Is<KeyValueType::Double>() && std::isnan(static_cast<double>(value))) ||
		   (value.Type().Is<KeyValueType::Float>() && std::isnan(static_cast<float>(value)));
}

// Compare values for WHERE and JOIN ON conditions, not for sorting or ordering index keys.
// Float/double NaN is NotComparable; string conversion, incompatible types and NULL follow the template policies.
// Tuples are compared lexicographically: an earlier unequal component may determine the result before a NaN.
// Array matching and the semantics of individual conditions (IN, ALLSET, NOT, etc.) remain in the caller.
template <WithString withString, NotComparable notComparable, NullsHandling nullsHandling>
ComparationResult CompareForCondition(const Variant& lhs, const Variant& rhs, const CollateOpts& collate = CollateOpts{}) {
	if (IsNaN(lhs) || IsNaN(rhs)) [[unlikely]] {
		return ComparationResult::NotComparable;
	}
	if (lhs.Type().Is<KeyValueType::Tuple>() && rhs.Type().Is<KeyValueType::Tuple>()) {
		const auto left = lhs.As<VariantArray>();
		const auto right = rhs.As<VariantArray>();
		auto l = left.cbegin();
		auto r = right.cbegin();
		for (; l != left.cend() && r != right.cend(); ++l, ++r) {
			const auto result = CompareForCondition<withString, notComparable, nullsHandling>(*l, *r, collate);
			if (result != ComparationResult::Eq) {
				return result;
			}
		}
		return l == left.cend() ? (r == right.cend() ? ComparationResult::Eq : ComparationResult::Lt) : ComparationResult::Gt;
	}
	return lhs.RelaxCompare<withString, notComparable, nullsHandling>(rhs, collate);
}

template <NotComparable notComparable>
struct [[nodiscard]] ConditionComparator {
	static bool equal(const Variant& lhs, const Variant& rhs) {
		return CompareForCondition<WithString::Yes, notComparable, kDefaultNullsHandling>(lhs, rhs) == ComparationResult::Eq;
	}
};

}  // namespace reindexer
