#include "protobufparser.h"
#include <tuple>
#include "core/cjson/protobufbuilder.h"
#include "core/schema.h"

namespace reindexer {

ProtobufValue ProtobufParser::ReadValue() {
	bool isArray = false;
	auto tag = object_.ser.GetVarUint();
	int tagType = (tag & kTypeMask);
	int tagName = (tag >> kNameBit);
	TagsPath currPath{object_.tagsPath};
	currPath.push_back(tagName);
	KeyValueType itemType = object_.schema.GetFieldType(currPath, isArray);
	if (itemType == KeyValueUndefined) {
		throw Error(errParseProtobuf, "Field [%d] type is unknown: [%d]", tagName, itemType);
	}
	switch (tagType) {
		case PBUF_TYPE_VARINT:
			if (itemType == KeyValueBool) {
				return {Variant(bool(object_.ser.GetVarUint())), tagName, itemType, isArray};
			} else {
				return {Variant(int64_t(object_.ser.GetVarUint())), tagName, itemType, isArray};
			}
		case PBUF_TYPE_FLOAT32:
		case PBUF_TYPE_FLOAT64:
			return {Variant(double(object_.ser.GetDouble())), tagName, itemType, isArray};
		case PBUF_TYPE_LENGTHENCODED:
			return {Variant(p_string(object_.ser.GetPVString())), tagName, itemType, isArray};
		default:
			throw Error(errParseProtobuf, "Type [%d] unexpected while decoding Protobuf", tagType);
	}
}

Variant ProtobufParser::ReadArrayItem(int fieldType) {
	Variant v;
	switch (fieldType) {
		case KeyValueInt64:
			v = Variant(int64_t(object_.ser.GetVarUint()));
			break;
		case KeyValueInt:
			v = Variant(int(object_.ser.GetVarUint()));
			break;
		case KeyValueDouble:
			v = Variant(object_.ser.GetDouble());
			break;
		case KeyValueBool:
			v = Variant(object_.ser.GetBool());
			break;
		default:
			throw Error(errParseProtobuf, "Error parsing packed indexed array: unexpected type [%d]", fieldType);
	}
	return v;
}

bool ProtobufParser::IsEof() const { return !(object_.ser.Pos() < object_.ser.Len()); }

ProtobufValue::ProtobufValue() : value(), tagName(0), itemType(KeyValueUndefined), isArray(false) {}
ProtobufValue::ProtobufValue(Variant&& _value, int _tagName, KeyValueType _itemType, bool _isArray)
	: value(std::move(_value)), tagName(_tagName), itemType(_itemType), isArray(_isArray) {}

}  // namespace reindexer
