#include "cjsontools.h"
#include "core/type_consts_helpers.h"

namespace reindexer {

TagType kvType2Tag(KeyValueType kvType) noexcept {
	return kvType.EvaluateOneOf([](OneOf<KeyValueType::Int, KeyValueType::Int64>) noexcept { return TAG_VARINT; },
								[](KeyValueType::Bool) noexcept { return TAG_BOOL; },
								[](KeyValueType::Double) noexcept { return TAG_DOUBLE; },
								[](KeyValueType::String) noexcept { return TAG_STRING; },
								[](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept { return TAG_NULL; },
								[](KeyValueType::Uuid) noexcept { return TAG_UUID; },
								[](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept -> TagType { std::abort(); });
}

void copyCJsonValue(TagType tagType, Variant value, WrSerializer &wrser) {
	if (value.Type().Is<KeyValueType::Null>()) return;
	switch (tagType) {
		case TAG_DOUBLE:
			wrser.PutDouble(static_cast<double>(value.convert(KeyValueType::Double{})));
			break;
		case TAG_VARINT:
			value.Type().EvaluateOneOf([&](KeyValueType::Int) { wrser.PutVarint(value.As<int>()); },
									   [&](KeyValueType::Int64) { wrser.PutVarint(value.As<int64_t>()); },
									   [&](OneOf<KeyValueType::Double, KeyValueType::Bool, KeyValueType::String, KeyValueType::Composite,
												 KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Uuid>) {
										   wrser.PutVarint(static_cast<int64_t>(value.convert(KeyValueType::Int64{})));
									   });
			break;
		case TAG_BOOL:
			wrser.PutBool(static_cast<bool>(value.convert(KeyValueType::Bool{})));
			break;
		case TAG_STRING:
			wrser.PutVString(static_cast<std::string_view>(value.convert(KeyValueType::String{})));
			break;
		case TAG_UUID:
			wrser.PutUuid(value.convert(KeyValueType::Uuid{}).As<Uuid>());
			break;
		case TAG_NULL:
			break;
		case TAG_ARRAY:
		case TAG_END:
		case TAG_OBJECT:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", TagTypeToStr(tagType));
	}
}

void putCJsonRef(TagType tagType, int tagName, int tagField, const VariantArray &values, WrSerializer &wrser) {
	if (values.IsArrayValue()) {
		wrser.PutCTag(ctag{TAG_ARRAY, tagName, tagField});
		wrser.PutVarUint(values.size());
	} else if (values.size() == 1) {
		wrser.PutCTag(ctag{tagType, tagName, tagField});
	}
}

void putCJsonValue(TagType tagType, int tagName, const VariantArray &values, WrSerializer &wrser) {
	if (values.IsArrayValue()) {
		const TagType elemType = kvType2Tag(values.ArrayType());
		wrser.PutCTag(ctag{TAG_ARRAY, tagName});
		wrser.PutCArrayTag(carraytag{values.size(), elemType});
		for (const Variant &value : values) copyCJsonValue(elemType, value, wrser);
	} else if (values.size() == 1) {
		wrser.PutCTag(ctag{tagType, tagName});
		copyCJsonValue(tagType, values.front(), wrser);
	}
}

void copyCJsonValue(TagType tagType, Serializer &rdser, WrSerializer &wrser) {
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
		case TAG_UUID:
			wrser.PutUuid(rdser.GetUuid());
			break;
		case TAG_END:
		case TAG_OBJECT:
		case TAG_ARRAY:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", TagTypeToStr(tagType));
	}
}

void skipCjsonTag(ctag tag, Serializer &rdser, std::array<unsigned, kMaxIndexes> *fieldsArrayOffsets) {
	const auto field = tag.Field();
	const bool embeddedField = (field < 0);
	switch (tag.Type()) {
		case TAG_ARRAY: {
			if (embeddedField) {
				const carraytag atag = rdser.GetCArrayTag();
				for (size_t i = 0, count = atag.Count(); i < count; ++i) {
					const ctag t = atag.Type() != TAG_OBJECT ? ctag{atag.Type()} : rdser.GetCTag();
					skipCjsonTag(t, rdser);
				}
			} else {
				const auto len = rdser.GetVarUint();
				if (fieldsArrayOffsets) {
					(*fieldsArrayOffsets)[field] += len;
				}
			}
		} break;

		case TAG_OBJECT:
			for (ctag otag{rdser.GetCTag()}; otag != kCTagEnd; otag = rdser.GetCTag()) {
				skipCjsonTag(otag, rdser, fieldsArrayOffsets);
			}
			break;
		case TAG_VARINT:
		case TAG_STRING:
		case TAG_DOUBLE:
		case TAG_END:
		case TAG_BOOL:
		case TAG_NULL:
		case TAG_UUID:
			if (embeddedField) {
				rdser.SkipRawVariant(KeyValueType{tag.Type()});
			} else if (fieldsArrayOffsets) {
				(*fieldsArrayOffsets)[field] += 1;
			}
	}
}

Variant cjsonValueToVariant(TagType tagType, Serializer &rdser, KeyValueType dstType) {
	return rdser
		.GetRawVariant(dstType.Is<KeyValueType::Int>() && tagType == TAG_VARINT ? KeyValueType{KeyValueType::Int{}} : KeyValueType{tagType})
		.convert(dstType);
}

template <typename T>
void buildPayloadTuple(const PayloadIface<T> &pl, const TagsMatcher *tagsMatcher, WrSerializer &wrser) {
	CJsonBuilder builder(wrser, ObjType::TypeObject);
	for (int field = 1, numFields = pl.NumFields(); field < numFields; ++field) {
		const PayloadFieldType &fieldType = pl.Type().Field(field);
		if (fieldType.JsonPaths().size() < 1 || fieldType.JsonPaths()[0].empty()) {
			continue;
		}

		int tagName = tagsMatcher->name2tag(fieldType.JsonPaths()[0]);
		assertf(tagName != 0, "ns=%s, field=%s", pl.Type().Name(), fieldType.JsonPaths()[0]);

		if (fieldType.IsArray()) {
			builder.ArrayRef(tagName, field, pl.GetArrayLen(field));
		} else {
			builder.Ref(tagName, pl.Get(field, 0), field);
		}
	}
}

template void buildPayloadTuple<const PayloadValue>(const PayloadIface<const PayloadValue> &, const TagsMatcher *, WrSerializer &);
template void buildPayloadTuple<PayloadValue>(const PayloadIface<PayloadValue> &, const TagsMatcher *, WrSerializer &);

}  // namespace reindexer
