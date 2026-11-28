#pragma once

#include <algorithm>
#include "core/keyvalue/condition_comparison.h"

namespace reindexer {

class [[nodiscard]] RelaxedAllSet {
public:
	explicit RelaxedAllSet(const VariantArray& values) : values_{values} { values_.EnsureHold(); }

	const VariantArray& Values() const noexcept { return values_; }

	bool Check(const VariantArray& lhs) const {
		if (values_.empty() || lhs.empty()) {
			return false;
		}
		for (const auto& rhs : values_) {
			if (std::none_of(lhs.begin(), lhs.end(),
							 [&rhs](const Variant& value) { return ConditionComparator<NotComparable::Return>::equal(value, rhs); })) {
				return false;
			}
		}
		return true;
	}

private:
	VariantArray values_;
};

}  // namespace reindexer
