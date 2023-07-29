#pragma once

#include "cjsonbuilder.h"
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

}  // namespace reindexer
