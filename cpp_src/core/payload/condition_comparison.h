#pragma once

#include <algorithm>
#include "core/keyvalue/condition_comparison.h"
#include "payloadiface.h"

namespace reindexer {

namespace condition_comparison_detail {

inline void GetFieldValues(const ConstPayload& payload, int field, const FieldsSet& fields, size_t tagPathIdx, VariantArray& values) {
	if (field != IndexValueType::SetByJsonPath) {
		payload.Get(field, values);
		return;
	}
	assertrx_throw(tagPathIdx < fields.getTagsPathsLength());
	if (fields.isTagsPathIndexed(tagPathIdx)) {
		payload.GetByJsonPath(fields.getIndexedTagsPath(tagPathIdx), values, KeyValueType::Undefined{});
	} else {
		payload.GetByJsonPath(fields.getTagsPath(tagPathIdx), values, KeyValueType::Undefined{});
	}
	if (values.empty()) {
		values.Clear();
		values.emplace_back();
	}
}

inline bool ContainsNaN(const Variant& value) {
	if (IsNaN(value)) {
		return true;
	}
	if (value.Type().Is<KeyValueType::Tuple>()) {
		const auto values = value.As<VariantArray>();
		return std::any_of(values.begin(), values.end(), ContainsNaN);
	}
	return false;
}

}  // namespace condition_comparison_detail

template <NullsHandling nullsHandling = kWhereCompareNullHandling>
ComparationResult ComparePayloadsForCondition(const ConstPayload& lhs, const ConstPayload& rhs, const FieldsSet& fields,
											  const CollateOpts& collate = CollateOpts{}) {
	size_t tagPathIdx = 0;
	for (const int field : fields) {
		VariantArray left, right;
		condition_comparison_detail::GetFieldValues(lhs, field, fields, tagPathIdx, left);
		condition_comparison_detail::GetFieldValues(rhs, field, fields, tagPathIdx, right);
		const bool byJsonPath = field == IndexValueType::SetByJsonPath;
		if (byJsonPath) {
			++tagPathIdx;
		}
		const size_t length = std::min(left.size(), right.size());
		for (size_t i = 0; i < length; ++i) {
			const auto result = byJsonPath
									? CompareForCondition<WithString::Yes, NotComparable::Return, nullsHandling>(left[i], right[i], collate)
									: CompareForCondition<WithString::No, NotComparable::Return, nullsHandling>(left[i], right[i], collate);
			if (result != ComparationResult::Eq) {
				return result;
			}
		}
		if (left.size() != right.size()) {
			return left.size() < right.size() ? ComparationResult::Lt : ComparationResult::Gt;
		}
	}
	return ComparationResult::Eq;
}

inline bool PayloadHasNaN(const ConstPayload& payload, const FieldsSet& fields) {
	size_t tagPathIdx = 0;
	for (const int field : fields) {
		if (field != IndexValueType::SetByJsonPath) {
			const auto type = payload.Type().Field(field).Type();
			if (!type.Is<KeyValueType::Double>() && !type.Is<KeyValueType::Float>()) {
				continue;
			}
		}
		VariantArray values;
		condition_comparison_detail::GetFieldValues(payload, field, fields, tagPathIdx, values);
		if (field == IndexValueType::SetByJsonPath) {
			++tagPathIdx;
		}
		if (std::any_of(values.begin(), values.end(), condition_comparison_detail::ContainsNaN)) {
			return true;
		}
	}
	return false;
}

}  // namespace reindexer
