#pragma once

#include "core/payload/payloadiface.h"

namespace reindexer {

template <typename T>
void buildPayloadTuple(const PayloadIface<T> &pl, const TagsMatcher *tagsMatcher, WrSerializer &wrser);

void copyCJsonValue(TagType tagType, Serializer &rdser, WrSerializer &wrser);
void copyCJsonValue(TagType tagType, Variant value, WrSerializer &wrser);
void putCJsonRef(TagType tagType, int tagName, int tagField, const VariantArray &values, WrSerializer &wrser);
void putCJsonValue(TagType tagType, int tagName, const VariantArray &values, WrSerializer &wrser);

[[nodiscard]] TagType kvType2Tag(KeyValueType kvType) noexcept;
void skipCjsonTag(ctag tag, Serializer &rdser, std::array<unsigned, kMaxIndexes> *fieldsArrayOffsets = nullptr);
[[nodiscard]] Variant cjsonValueToVariant(TagType tag, Serializer &rdser, KeyValueType dstType);

[[noreturn]] void throwUnexpectedNestedArrayError(std::string_view parserName, const PayloadFieldType &f);
[[noreturn]] void throwScalarMultipleEncodesError(const Payload &pl, const PayloadFieldType &f, int field);
RX_ALWAYS_INLINE void validateNonArrayFieldRestrictions(const ScalarIndexesSetT &scalarIndexes, const Payload &pl,
														const PayloadFieldType &f, int field, bool isInArray, std::string_view parserName) {
	if (!f.IsArray()) {
		if rx_unlikely (isInArray) {
			throwUnexpectedNestedArrayError(parserName, f);
		}
		if rx_unlikely (scalarIndexes.test(field)) {
			throwScalarMultipleEncodesError(pl, f, field);
		}
	}
}

}  // namespace reindexer
