#include "protobufparser.h"
#include "core/cjson/protobufbuilder.h"
#include "core/schema.h"

namespace reindexer {

ProtobufValue ProtobufParser::ReadValue() {
	bool isArray = false;
	const uint64_t tag = object_.ser.GetVarUInt();
	int tagType = (tag & kTypeMask);
	const TagName tagName = TagName(tag >> kTypeBit);
	TagsPath currPath{object_.tagsPath};
	currPath.push_back(tagName);
	KeyValueType itemType = object_.schema.GetFieldType(currPath, isArray);
	if (itemType.Is<KeyValueType::Undefined>()) {
		throw Error(errParseProtobuf, "Field [{}] type is unknown: [{}]", tagName.AsNumber(), itemType.Name());
	}
	switch (tagType) {
		case PBUF_TYPE_VARINT:
			if (itemType.Is<KeyValueType::Bool>()) {
				return {Variant(bool(object_.ser.GetVarUInt())), tagName, itemType, isArray};
			} else {
				return {Variant(object_.ser.GetVarint()), tagName, itemType, isArray};
			}
		case PBUF_TYPE_FLOAT32:
		case PBUF_TYPE_FLOAT64:
			return {Variant(double(object_.ser.GetDouble())), tagName, itemType, isArray};
		case PBUF_TYPE_LENGTHENCODED:
			return {Variant(p_string(object_.ser.GetPVString())), tagName, itemType, isArray};
		default:
			throw Error(errParseProtobuf, "Type [{}] unexpected while decoding Protobuf", tagType);
	}
}

Variant ProtobufParser::ReadArrayItem(KeyValueType fieldType) {
	return fieldType.EvaluateOneOf(
		[&](KeyValueType::Int64) { return Variant(int64_t(object_.ser.GetVarint())); },
		[&](KeyValueType::Int) { return Variant(int(object_.ser.GetVarint())); },
		[&](KeyValueType::Double) { return Variant(object_.ser.GetDouble()); },
		[&](KeyValueType::Float) { return Variant(object_.ser.GetFloat()); },
		[&](KeyValueType::Bool) { return Variant(object_.ser.GetBool()); },
		[&](concepts::OneOf<KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::String,
							KeyValueType::Uuid, KeyValueType::FloatVector> auto) -> Variant {
			throw Error(errParseProtobuf, "Error parsing packed indexed array: unexpected type [{}]", fieldType.Name());
		});
}

bool ProtobufParser::IsEof() const { return !(object_.ser.Pos() < object_.ser.Len()); }

ProtobufValue::ProtobufValue() : value(), tagName(0), itemType(KeyValueType::Undefined{}), isArray(false) {}
ProtobufValue::ProtobufValue(Variant&& _value, TagName _tagName, KeyValueType _itemType, bool _isArray)
	: value(std::move(_value)), tagName(_tagName), itemType(_itemType), isArray(_isArray) {}

}  // namespace reindexer
