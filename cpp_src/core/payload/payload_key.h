#pragma once

#include <cstdlib>
#include <variant>
#include "core/payload/payloadiface.h"

namespace reindexer::payload_key {

inline void GetValuesByJsonPath(const ConstPayload& payload, const FieldsSet& fields, size_t pathIdx, VariantArray& values) {
	assertrx(pathIdx < fields.getTagsPathsLength());
	const FieldsPath& path = fields.getFieldsPath(pathIdx);
	if (const auto* tagsPath = std::get_if<TagsPath>(&path)) {
		payload.GetByJsonPath(*tagsPath, values, KeyValueType::Undefined{});
	} else if (const auto* indexedPath = std::get_if<IndexedTagsPath>(&path)) {
		payload.GetByJsonPath(*indexedPath, values, KeyValueType::Undefined{});
	} else {
		std::abort();
	}
}

template <typename HashPathValues>
size_t Hash(const ConstPayload& payload, const FieldsSet& fields, HashPathValues hashPathValues) {
	uint64_t hash = 0;
	VariantArray values;
	size_t pathIdx = 0;
	for (int field : fields) {
		hash *= VariantArray::kHashMagic;
		if (field == IndexValueType::SetByJsonPath) {
			GetValuesByJsonPath(payload, fields, pathIdx++, values);
			hash ^= hashPathValues(values);
		} else {
			const auto& f = payload.Type().Field(field);
			if (f.IsArray()) {
				const auto arr = payload_access::readArrayMeta(payload.Ptr(), f.Offset());
				hash ^= arr.len;
				auto* p = payload.Value()->Ptr() + arr.offset;
				for (int i = 0; i < arr.len; ++i, p += f.ElemSizeof()) {
					hash ^= PayloadFieldValue(f, p).Hash();
				}
			} else {
				assertrx_dbg(!f.Type().Is<KeyValueType::FloatVector>());
				hash ^= payload.Field(field).Hash();
			}
		}
	}
	return hash;
}

template <typename EqualPathValues>
bool Equal(const ConstPayload& lhs, const ConstPayload& rhs, const FieldsSet& fields, EqualPathValues equalPathValues) {
	VariantArray leftValues, rightValues;
	size_t pathIdx = 0;
	for (int field : fields) {
		if (field == IndexValueType::SetByJsonPath) {
			GetValuesByJsonPath(lhs, fields, pathIdx, leftValues);
			GetValuesByJsonPath(rhs, fields, pathIdx++, rightValues);
			if (!equalPathValues(leftValues, rightValues)) {
				return false;
			}
		} else {
			const auto& f = lhs.Type().Field(field);
			if (f.IsArray()) {
				const auto left = payload_access::readArrayMeta(lhs.Ptr(), f.Offset());
				const auto right = payload_access::readArrayMeta(rhs.Ptr(), f.Offset());
				if (left.len != right.len) {
					return false;
				}
				auto* lp = lhs.Value()->Ptr() + left.offset;
				auto* rp = rhs.Value()->Ptr() + right.offset;
				for (int i = 0; i < left.len; ++i, lp += f.ElemSizeof(), rp += f.ElemSizeof()) {
					if (!PayloadFieldValue(f, lp).IsEQ(PayloadFieldValue(f, rp))) {
						return false;
					}
				}
			} else if (!lhs.Field(field).IsEQ(rhs.Field(field))) {
				return false;
			}
		}
	}
	return true;
}

}  // namespace reindexer::payload_key
