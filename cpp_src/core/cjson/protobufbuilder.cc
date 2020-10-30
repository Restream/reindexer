#include "protobufbuilder.h"
#include "core/schema.h"

namespace reindexer {

ProtobufBuilder::ProtobufBuilder(WrSerializer* wrser, ObjType type, const Schema* schema, const TagsMatcher* tm, const TagsPath* tagsPath,
								 int fieldIdx)
	: type_(type), ser_(wrser), tm_(tm), tagsPath_(tagsPath), schema_(schema), sizeHelper_(), itemsFieldIndex_(fieldIdx) {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObject:
			putFieldHeader(fieldIdx, PBUF_TYPE_LENGTHENCODED);
			sizeHelper_ = ser_->StartVString();
			break;
		case ObjType::TypeObjectArray:
			itemsFieldIndex_ = fieldIdx;
			break;
		default:
			break;
	}
}

void ProtobufBuilder::End() {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObject: {
			sizeHelper_.End();
			break;
		}
		case ObjType::TypeObjectArray:
			itemsFieldIndex_ = -1;
			break;
		default:
			break;
	}
	type_ = ObjType::TypePlain;
}

void ProtobufBuilder::packItem(int fieldIdx, int tagType, Serializer& rdser, ProtobufBuilder& array) {
	switch (tagType) {
		case TAG_DOUBLE:
			array.put(fieldIdx, rdser.GetDouble());
			break;
		case TAG_VARINT:
			array.put(fieldIdx, rdser.GetVarint());
			break;
		case TAG_BOOL:
			array.put(fieldIdx, rdser.GetBool());
			break;
		case TAG_STRING:
			array.put(fieldIdx, string(rdser.GetVString()));
			break;
		case TAG_NULL:
			array.Null(fieldIdx);
			break;
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", ctag(tagType).TypeName());
	}
}

int ProtobufBuilder::getFieldTag(int fieldIdx) const {
	if (type_ == ObjType::TypeObjectArray && itemsFieldIndex_ != -1) {
		return itemsFieldIndex_;
	}
	return fieldIdx;
}

bool ProtobufBuilder::getExpectedFieldType(KeyValueType& expectedType) {
	if (schema_ && tagsPath_) {
		bool isArray = false;
		expectedType = schema_->GetFieldType(*tagsPath_, isArray);
		if (expectedType != KeyValueUndefined) {
			return true;
		}
	}
	return false;
}

void ProtobufBuilder::putFieldHeader(int fieldIdx, ProtobufTypes type) { ser_->PutVarUint((getFieldTag(fieldIdx) << kNameBit) | type); }

void ProtobufBuilder::put(int fieldIdx, bool val) { return put(fieldIdx, int(val)); }

void ProtobufBuilder::put(int fieldIdx, int val) {
	KeyValueType expectedType;
	if (getExpectedFieldType(expectedType)) {
		switch (expectedType) {
			case KeyValueInt:
			case KeyValueBool:
				break;
			case KeyValueInt64:
				return put(fieldIdx, int64_t(val));
			case KeyValueDouble:
				return put(fieldIdx, double(val));
			default:
				throw Error(errParams, "Expected type '%s' for field '%s'", Variant::TypeName(expectedType), tm_->tag2name(fieldIdx));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_VARINT);
	}
	ser_->PutVarUint(val);
}

void ProtobufBuilder::put(int fieldIdx, int64_t val) {
	KeyValueType expectedType;
	if (getExpectedFieldType(expectedType)) {
		switch (expectedType) {
			case KeyValueInt64:
				break;
			case KeyValueBool:
			case KeyValueInt:
				return put(fieldIdx, int(val));
			case KeyValueDouble:
				return put(fieldIdx, double(val));
			default:
				throw Error(errParams, "Expected type '%s' for field '%s'", Variant::TypeName(expectedType), tm_->tag2name(fieldIdx));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_VARINT);
	}
	ser_->PutVarUint(val);
}

void ProtobufBuilder::put(int fieldIdx, double val) {
	KeyValueType expectedType;
	if (getExpectedFieldType(expectedType)) {
		switch (expectedType) {
			case KeyValueDouble:
				break;
			case KeyValueInt:
			case KeyValueBool:
				return put(fieldIdx, int(val));
			case KeyValueInt64:
				return put(fieldIdx, int64_t(val));
			default:
				throw Error(errParams, "Expected type '%s' for field '%s'", Variant::TypeName(expectedType), tm_->tag2name(fieldIdx));
		}
	}

	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_FLOAT64);
	}
	ser_->PutDouble(val);
}

void ProtobufBuilder::put(int fieldIdx, string_view val) {
	KeyValueType expectedType;
	if (getExpectedFieldType(expectedType)) {
		if (expectedType != KeyValueString) {
			throw Error(errParams, "Expected type 'String' for field '%s'", tm_->tag2name(fieldIdx));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutVString(val);
}

void ProtobufBuilder::put(int fieldIdx, const Variant& val) {
	switch (val.Type()) {
		case KeyValueInt64:
			put(fieldIdx, int64_t(val));
			break;
		case KeyValueInt:
			put(fieldIdx, int(val));
			break;
		case KeyValueDouble:
			put(fieldIdx, double(val));
			break;
		case KeyValueString:
			put(fieldIdx, string_view(val));
			break;
		case KeyValueBool:
			put(fieldIdx, bool(val));
			break;
		case KeyValueTuple: {
			auto arrNode = ArrayPacked(fieldIdx);
			for (auto& itVal : val.getCompositeValues()) {
				arrNode.Put(fieldIdx, itVal);
			}
			break;
		}
		case KeyValueNull:
			break;
		default:
			break;
	}
}

ProtobufBuilder ProtobufBuilder::Object(int fieldIdx, int) {
	// Main object in Protobuf is never of Object type,
	// only nested object fields are.
	if (type_ == ObjType::TypePlain && fieldIdx == 0) {
		return ProtobufBuilder(std::move(*this));
	}
	return ProtobufBuilder(ser_, ObjType::TypeObject, schema_, tm_, tagsPath_, getFieldTag(fieldIdx));
}

}  // namespace reindexer
