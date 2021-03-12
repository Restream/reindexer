#include "cjsontools.h"

namespace reindexer {

int kvType2Tag(KeyValueType kvType) {
	int tagType = 0;
	switch (kvType) {
		case KeyValueInt:
		case KeyValueInt64:
			tagType = TAG_VARINT;
			break;
		case KeyValueBool:
			tagType = TAG_BOOL;
			break;
		case KeyValueDouble:
			tagType = TAG_DOUBLE;
			break;
		case KeyValueString:
			tagType = TAG_STRING;
			break;
		case KeyValueUndefined:
		case KeyValueNull:
			tagType = TAG_NULL;
			break;
		default:
			std::abort();
	}
	return tagType;
}

void copyCJsonValue(int tagType, const Variant &value, WrSerializer &wrser) {
	if (value.Type() == KeyValueNull) return;
	switch (tagType) {
		case TAG_DOUBLE:
			wrser.PutDouble(static_cast<double>(value.convert(KeyValueDouble)));
			break;
		case TAG_VARINT:
			switch (value.Type()) {
				case KeyValueInt64:
					wrser.PutVarint(static_cast<int64_t>(value.convert(KeyValueInt64)));
					break;
				case KeyValueInt:
					wrser.PutVarint(static_cast<int>(value.convert(KeyValueInt)));
					break;
				default:
					wrser.PutVarint(static_cast<int64_t>(value.convert(KeyValueInt64)));
			}
			break;
		case TAG_BOOL:
			wrser.PutBool(static_cast<bool>(value.convert(KeyValueBool)));
			break;
		case TAG_STRING:
			wrser.PutVString(static_cast<string_view>(value.convert(KeyValueString)));
			break;
		case TAG_NULL:
			break;
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", ctag(tagType).TypeName());
	}
}

void putCJsonRef(int tagType, int tagName, int tagField, const VariantArray &values, WrSerializer &wrser) {
	if (values.IsArrayValue()) {
		wrser.PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName, tagField)));
		wrser.PutVarUint(values.size());
	} else if (values.size() == 1) {
		wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, tagField)));
	}
}

void putCJsonValue(int tagType, int tagName, const VariantArray &values, WrSerializer &wrser) {
	if (values.IsArrayValue()) {
		int elemType = kvType2Tag(values.ArrayType());
		wrser.PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
		wrser.PutUInt32(int(carraytag(values.size(), elemType)));
		for (const Variant &value : values) copyCJsonValue(elemType, value, wrser);
	} else if (values.size() == 1) {
		wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName)));
		copyCJsonValue(tagType, values.front(), wrser);
	}
}

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser) {
	switch (tagType) {
		case TAG_DOUBLE:
			wrser.PutDouble(rdser.GetDouble());
			break;
		case TAG_VARINT:
			wrser.PutVarint(rdser.GetVarint());
			break;
		case TAG_BOOL:
			wrser.PutBool(rdser.GetBool());
			break;
		case TAG_STRING:
			wrser.PutVString(rdser.GetVString());
			break;
		case TAG_NULL:
			break;
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", ctag(tagType).TypeName());
	}
}

void skipCjsonTag(ctag tag, Serializer &rdser) {
	const bool embeddedField = (tag.Field() < 0);
	switch (tag.Type()) {
		case TAG_ARRAY: {
			if (embeddedField) {
				carraytag atag = rdser.GetUInt32();
				for (int i = 0; i < atag.Count(); i++) {
					ctag t = atag.Tag() != TAG_OBJECT ? atag.Tag() : rdser.GetVarUint();
					skipCjsonTag(t, rdser);
				}
			} else {
				rdser.GetVarUint();
			}
		} break;

		case TAG_OBJECT:
			for (ctag otag = rdser.GetVarUint(); otag.Type() != TAG_END; otag = rdser.GetVarUint()) {
				skipCjsonTag(otag, rdser);
			}
			break;
		default:
			if (embeddedField) rdser.GetRawVariant(KeyValueType(tag.Type()));
	}
}

Variant cjsonValueToVariant(int tag, Serializer &rdser, KeyValueType dstType, Error &err) {
	try {
		KeyValueType srcType = KeyValueType(tag);
		if (dstType == KeyValueInt && srcType == KeyValueInt64) srcType = KeyValueInt;
		return rdser.GetRawVariant(KeyValueType(srcType)).convert(dstType);
	} catch (const Error &e) {
		err = e;
	}

	return Variant();
}

template <typename T>
void buildPayloadTuple(const PayloadIface<T> *pl, const TagsMatcher *tagsMatcher, WrSerializer &wrser) {
	CJsonBuilder builder(wrser, ObjType::TypeObject);
	for (int field = 1; field < pl->NumFields(); ++field) {
		const PayloadFieldType &fieldType = pl->Type().Field(field);
		if (fieldType.JsonPaths().size() < 1 || fieldType.JsonPaths()[0].empty()) continue;

		int tagName = tagsMatcher->name2tag(fieldType.JsonPaths()[0]);
		assertf(tagName != 0, "ns=%s, field=%s", pl->Type().Name(), fieldType.JsonPaths()[0]);

		if (fieldType.IsArray()) {
			builder.ArrayRef(tagName, field, pl->GetArrayLen(field));
		} else {
			builder.Ref(tagName, pl->Get(field, 0), field);
		}
	}
}

template void buildPayloadTuple<const PayloadValue>(const PayloadIface<const PayloadValue> *pl, const TagsMatcher *tagsMatcher,
													WrSerializer &wrser);
template void buildPayloadTuple<PayloadValue>(const PayloadIface<PayloadValue> *pl, const TagsMatcher *tagsMatcher, WrSerializer &wrser);

}  // namespace reindexer
