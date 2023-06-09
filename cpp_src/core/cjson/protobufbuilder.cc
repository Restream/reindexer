#include "protobufbuilder.h"
#include "core/schema.h"
#include "core/type_consts_helpers.h"

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
		case ObjType::TypePlain:
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
		case ObjType::TypePlain:
			break;
	}
	type_ = ObjType::TypePlain;
}

void ProtobufBuilder::packItem(int fieldIdx, TagType tagType, Serializer& rdser, ProtobufBuilder& array) {
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
			array.put(fieldIdx, rdser.GetVString());
			break;
		case TAG_UUID:
			array.put(fieldIdx, rdser.GetUuid());
			break;
		case TAG_NULL:
			array.Null(fieldIdx);
			break;
		case TAG_ARRAY:
		case TAG_OBJECT:
		case TAG_END:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", TagTypeToStr(tagType));
	}
}

int ProtobufBuilder::getFieldTag(int fieldIdx) const {
	if (type_ == ObjType::TypeObjectArray && itemsFieldIndex_ != -1) {
		return itemsFieldIndex_;
	}
	return fieldIdx;
}

std::pair<KeyValueType, bool> ProtobufBuilder::getExpectedFieldType() const {
	if (schema_ && tagsPath_) {
		bool isArray = false;
		const auto expectedType = schema_->GetFieldType(*tagsPath_, isArray);
		return {expectedType, !expectedType.Is<KeyValueType::Undefined>()};
	}
	return {KeyValueType::Undefined{}, false};
}

void ProtobufBuilder::putFieldHeader(int fieldIdx, ProtobufTypes type) { ser_->PutVarUint((getFieldTag(fieldIdx) << kTypeBit) | type); }

void ProtobufBuilder::put(int fieldIdx, bool val) { return put(fieldIdx, int(val)); }

void ProtobufBuilder::put(int fieldIdx, int val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](OneOf<KeyValueType::Int, KeyValueType::Bool>) noexcept {},
								[&](KeyValueType::Int64) {
									put(fieldIdx, int64_t(val));
									done = true;
								},
								[&](KeyValueType::Double) {
									put(fieldIdx, double(val));
									done = true;
								},
								[&](OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
										  KeyValueType::Undefined, KeyValueType::Uuid>) {
									throw Error(errParams, "Expected type '%s' for field '%s'", res.first.Name(), tm_->tag2name(fieldIdx));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(fieldIdx, PBUF_TYPE_VARINT);
		}
		ser_->PutVarUint(val);
	}
}

void ProtobufBuilder::put(int fieldIdx, int64_t val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](KeyValueType::Int64) noexcept {},
								[&](OneOf<KeyValueType::Bool, KeyValueType::Int>) {
									put(fieldIdx, int(val));
									done = true;
								},
								[&](KeyValueType::Double) {
									put(fieldIdx, double(val));
									done = true;
								},
								[&](OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
										  KeyValueType::Undefined, KeyValueType::Uuid>) {
									throw Error(errParams, "Expected type '%s' for field '%s'", res.first.Name(), tm_->tag2name(fieldIdx));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(fieldIdx, PBUF_TYPE_VARINT);
		}
		ser_->PutVarUint(val);
	}
}

void ProtobufBuilder::put(int fieldIdx, double val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](KeyValueType::Double) noexcept {},
								[&](OneOf<KeyValueType::Int, KeyValueType::Bool>) {
									put(fieldIdx, int(val));
									done = true;
								},
								[&](KeyValueType::Int64) {
									put(fieldIdx, int64_t(val));
									done = true;
								},
								[&](OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
										  KeyValueType::Undefined, KeyValueType::Uuid>) {
									throw Error(errParams, "Expected type '%s' for field '%s'", res.first.Name(), tm_->tag2name(fieldIdx));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(fieldIdx, PBUF_TYPE_FLOAT64);
		}
		ser_->PutDouble(val);
	}
}

void ProtobufBuilder::put(int fieldIdx, std::string_view val) {
	if (const auto res = getExpectedFieldType(); res.second) {
		if (!res.first.Is<KeyValueType::String>()) {
			throw Error(errParams, "Expected type 'String' for field '%s'", tm_->tag2name(fieldIdx));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutVString(val);
}

void ProtobufBuilder::put(int fieldIdx, Uuid val) {
	if (const auto res = getExpectedFieldType(); res.second) {
		if (!res.first.Is<KeyValueType::String>()) {
			throw Error(errParams, "Expected type 'String' for field '%s'", tm_->tag2name(fieldIdx));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(fieldIdx, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutStrUuid(val);
}

void ProtobufBuilder::put(int fieldIdx, const Variant& val) {
	val.Type().EvaluateOneOf([&](KeyValueType::Int64) { put(fieldIdx, int64_t(val)); }, [&](KeyValueType::Int) { put(fieldIdx, int(val)); },
							 [&](KeyValueType::Double) { put(fieldIdx, double(val)); },
							 [&](KeyValueType::String) { put(fieldIdx, std::string_view(val)); },
							 [&](KeyValueType::Bool) { put(fieldIdx, bool(val)); },
							 [&](KeyValueType::Tuple) {
								 auto arrNode = ArrayPacked(fieldIdx);
								 for (auto& itVal : val.getCompositeValues()) {
									 arrNode.Put(fieldIdx, itVal);
								 }
							 },
							 [&](KeyValueType::Uuid) { put(fieldIdx, Uuid{val}); },
							 [&](OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite>) noexcept {});
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
