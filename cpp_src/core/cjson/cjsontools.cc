#include "cjsontools.h"

namespace reindexer {

int kvType2Tag(KeyValueType kvType) {
	return kvType.EvaluateOneOf([&](OneOf<KeyValueType::Int, KeyValueType::Int64>) noexcept { return TAG_VARINT; },
								[&](KeyValueType::Bool) noexcept { return TAG_BOOL; },
								[&](KeyValueType::Double) noexcept { return TAG_DOUBLE; },
								[&](KeyValueType::String) noexcept { return TAG_STRING; },
								[&](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept { return TAG_NULL; },
								[&](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept -> decltype(TAG_NULL) { std::abort(); });
}

void copyCJsonValue(int tagType, const Variant &value, WrSerializer &wrser) {
	if (value.Type().Is<KeyValueType::Null>()) return;
	switch (tagType) {
		case TAG_DOUBLE:
			wrser.PutDouble(static_cast<double>(value.convert(KeyValueType::Double{})));
			break;
		case TAG_VARINT:
			value.Type().EvaluateOneOf(
				[&](KeyValueType::Int) { wrser.PutVarint(static_cast<int>(value.convert(KeyValueType::Int{}))); },
				[&](OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::Bool, KeyValueType::String, KeyValueType::Composite,
						  KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null>) {
					wrser.PutVarint(static_cast<int64_t>(value.convert(KeyValueType::Int64{})));
				});
			break;
		case TAG_BOOL:
			wrser.PutBool(static_cast<bool>(value.convert(KeyValueType::Bool{})));
			break;
		case TAG_STRING:
			wrser.PutVString(static_cast<std::string_view>(value.convert(KeyValueType::String{})));
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
			if (embeddedField) rdser.GetRawVariant(KeyValueType::FromNumber(tag.Type()));
	}
}

Variant cjsonValueToVariant(int tag, Serializer &rdser, KeyValueType dstType, Error &err) {
	try {
		KeyValueType srcType = KeyValueType::FromNumber(tag);
		if (dstType.Is<KeyValueType::Int>() && srcType.Is<KeyValueType::Int64>()) srcType = KeyValueType::Int{};
		return rdser.GetRawVariant(srcType).convert(dstType);
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
