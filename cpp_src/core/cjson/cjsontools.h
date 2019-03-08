#pragma once

#include "cjsonbuilder.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

template <typename T>
key_string buildPayloadTuple(const PayloadIface<T> *pl, const TagsMatcher *tagsMatcher) {
	WrSerializer wrser;
	CJsonBuilder builder(wrser, CJsonBuilder::TypeObject);

	for (int field = 1; field < pl->NumFields(); ++field) {
		const PayloadFieldType &fieldType = pl->Type().Field(field);
		if (fieldType.JsonPaths().size() < 1 || fieldType.JsonPaths()[0].empty()) continue;

		VariantArray keyRefs;
		pl->Get(field, keyRefs);

		int tagName = tagsMatcher->name2tag(fieldType.JsonPaths()[0].c_str());
		assert(tagName != 0);

		if (fieldType.IsArray()) {
			builder.ArrayRef(tagName, field, keyRefs.size());
		} else {
			assert(keyRefs.size() == 1);
			builder.Ref(tagName, keyRefs[0], field);
		}
	}
	builder.End();
	return make_key_string(reinterpret_cast<const char *>(wrser.Buf()), wrser.Len());
}

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);
void copyCJsonValue(int tagType, const Variant &value, WrSerializer &wrser);
void putCJsonValue(int tagType, int tagName, const VariantArray &values, WrSerializer &wrser);

int kvType2Tag(KeyValueType kvType);
void skipCjsonTag(ctag tag, Serializer &rdser);
Variant cjsonValueToVariant(int tag, Serializer &rdser, KeyValueType dstType, Error &err);

}  // namespace reindexer
