#pragma once

#include "cjsonbuilder.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

template <typename T>
void buildPayloadTuple(const PayloadIface<T> *pl, const TagsMatcher *tagsMatcher, WrSerializer &wrser);

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);
void copyCJsonValue(int tagType, const Variant &value, WrSerializer &wrser);
void putCJsonRef(int tagType, int tagName, int tagField, const VariantArray &values, WrSerializer &wrser);
void putCJsonValue(int tagType, int tagName, const VariantArray &values, WrSerializer &wrser);

int kvType2Tag(KeyValueType kvType);
void skipCjsonTag(ctag tag, Serializer &rdser);
Variant cjsonValueToVariant(int tag, Serializer &rdser, KeyValueType dstType, Error &err);

}  // namespace reindexer
