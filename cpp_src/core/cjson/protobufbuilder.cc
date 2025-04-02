#include "protobufbuilder.h"
#include "core/schema.h"
#include "core/type_consts_helpers.h"

namespace reindexer {

ProtobufBuilder::ProtobufBuilder(WrSerializer* wrser, ObjType type, const Schema* schema, const TagsMatcher* tm, const TagsPath* tagsPath,
								 TagName tagName)
	: type_(type), ser_(wrser), tm_(tm), tagsPath_(tagsPath), schema_(schema), sizeHelper_(), itemsFieldIndex_(tagName) {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObject:
			putFieldHeader(tagName, PBUF_TYPE_LENGTHENCODED);
			sizeHelper_ = ser_->StartVString();
			break;
		case ObjType::TypeObjectArray:
			itemsFieldIndex_ = tagName;
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
			itemsFieldIndex_ = TagName::Empty();
			break;
		case ObjType::TypePlain:
			break;
	}
	type_ = ObjType::TypePlain;
}

void ProtobufBuilder::packItem(TagName tagName, TagType tagType, Serializer& rdser, ProtobufBuilder& array) {
	switch (tagType) {
		case TAG_DOUBLE:
			array.put(tagName, rdser.GetDouble());
			break;
		case TAG_VARINT:
			array.put(tagName, rdser.GetVarint());
			break;
		case TAG_BOOL:
			array.put(tagName, rdser.GetBool());
			break;
		case TAG_STRING:
			array.put(tagName, rdser.GetVString());
			break;
		case TAG_UUID:
			array.put(tagName, rdser.GetUuid());
			break;
		case TAG_FLOAT:
			array.put(tagName, rdser.GetFloat());
			break;
		case TAG_NULL:
			array.Null(tagName);
			break;
		case TAG_ARRAY:
		case TAG_OBJECT:
		case TAG_END:
			throw Error(errParseJson, "Unexpected cjson typeTag '{}' while parsing value", TagTypeToStr(tagType));
	}
}

TagName ProtobufBuilder::getFieldTag(TagName tagName) const {
	if (type_ == ObjType::TypeObjectArray && !itemsFieldIndex_.IsEmpty()) {
		return itemsFieldIndex_;
	}
	return tagName;
}

std::pair<KeyValueType, bool> ProtobufBuilder::getExpectedFieldType() const {
	if (schema_ && tagsPath_) {
		bool isArray = false;
		const auto expectedType = schema_->GetFieldType(*tagsPath_, isArray);
		return {expectedType, !expectedType.Is<KeyValueType::Undefined>()};
	}
	return {KeyValueType::Undefined{}, false};
}

void ProtobufBuilder::putFieldHeader(TagName tagName, ProtobufTypes type) {
	ser_->PutVarUint((unsigned(getFieldTag(tagName).AsNumber()) << kTypeBit) | type);
}

void ProtobufBuilder::put(TagName tagName, bool val) { return put(tagName, int(val)); }

void ProtobufBuilder::put(TagName tagName, int val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](OneOf<KeyValueType::Int, KeyValueType::Bool>) noexcept {},
								[&](KeyValueType::Int64) {
									put(tagName, int64_t(val));
									done = true;
								},
								[&](KeyValueType::Double) {
									put(tagName, double(val));
									done = true;
								},
								[&](KeyValueType::Float) {
									put(tagName, float(val));
									done = true;
								},
								[&](OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
										  KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector>) {
									throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tm_->tag2name(tagName));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tagName, PBUF_TYPE_VARINT);
		}
		ser_->PutVarUint(val);
	}
}

void ProtobufBuilder::put(TagName tagName, int64_t val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](KeyValueType::Int64) noexcept {},
								[&](OneOf<KeyValueType::Bool, KeyValueType::Int>) {
									put(tagName, int(val));
									done = true;
								},
								[&](KeyValueType::Double) {
									put(tagName, double(val));
									done = true;
								},
								[&](KeyValueType::Float) {
									put(tagName, float(val));
									done = true;
								},
								[&](OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
										  KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector>) {
									throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tm_->tag2name(tagName));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tagName, PBUF_TYPE_VARINT);
		}
		ser_->PutVarUint(val);
	}
}

void ProtobufBuilder::put(TagName tagName, double val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](KeyValueType::Double) noexcept {},
								[&](OneOf<KeyValueType::Int, KeyValueType::Bool>) {
									put(tagName, int(val));
									done = true;
								},
								[&](KeyValueType::Float) {
									put(tagName, float(val));
									done = true;
								},
								[&](KeyValueType::Int64) {
									put(tagName, int64_t(val));
									done = true;
								},
								[&](OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
										  KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector>) {
									throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tm_->tag2name(tagName));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tagName, PBUF_TYPE_FLOAT64);
		}
		ser_->PutDouble(val);
	}
}

void ProtobufBuilder::put(TagName tagName, float val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf(
			[&](KeyValueType::Double) noexcept {
				put(tagName, double(val));
				done = true;
			},
			[&](OneOf<KeyValueType::Int, KeyValueType::Bool>) {
				put(tagName, int(val));
				done = true;
			},
			[&](KeyValueType::Float) {},
			[&](KeyValueType::Int64) {
				put(tagName, int64_t(val));
				done = true;
			},
			[&](OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
					  KeyValueType::Uuid, KeyValueType::FloatVector>) {
				throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tm_->tag2name(tagName));
			});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tagName, PBUF_TYPE_FLOAT32);
		}
		ser_->PutFloat(val);
	}
}

void ProtobufBuilder::put(TagName tagName, std::string_view val) {
	if (const auto res = getExpectedFieldType(); res.second) {
		if (!res.first.Is<KeyValueType::String>()) {
			throw Error(errParams, "Expected type 'String' for field '{}'", tm_->tag2name(tagName));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(tagName, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutVString(val);
}

void ProtobufBuilder::put(TagName tagName, Uuid val) {
	if (const auto res = getExpectedFieldType(); res.second) {
		if (!res.first.Is<KeyValueType::String>()) {
			throw Error(errParams, "Expected type 'String' for field '{}'", tm_->tag2name(tagName));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(tagName, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutStrUuid(val);
}

void ProtobufBuilder::put(TagName tagName, const Variant& val) {
	val.Type().EvaluateOneOf(
		[&](KeyValueType::Int64) { put(tagName, int64_t(val)); }, [&](KeyValueType::Int) { put(tagName, int(val)); },
		[&](KeyValueType::Double) { put(tagName, double(val)); }, [&](KeyValueType::Float) { put(tagName, float(val)); },
		[&](KeyValueType::String) { put(tagName, std::string_view(val)); }, [&](KeyValueType::Bool) { put(tagName, bool(val)); },
		[&](KeyValueType::Tuple) {
			auto arrNode = ArrayPacked(tagName);
			for (auto& itVal : val.getCompositeValues()) {
				arrNode.Put(tagName, itVal, 0);
			}
		},
		[&](KeyValueType::Uuid) { put(tagName, Uuid{val}); },
		[&](OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::FloatVector>) noexcept {});
}

ProtobufBuilder ProtobufBuilder::Object(TagName tagName, int) {
	// Main object in Protobuf is never of Object type,
	// only nested object fields are.
	if (type_ == ObjType::TypePlain && tagName.IsEmpty()) {
		return ProtobufBuilder(std::move(*this));
	}
	return ProtobufBuilder(ser_, ObjType::TypeObject, schema_, tm_, tagsPath_, getFieldTag(tagName));
}

}  // namespace reindexer
