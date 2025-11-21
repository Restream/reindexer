#include "protobufbuilder.h"
#include <string_view>
#include "core/schema.h"
#include "core/type_consts_helpers.h"

namespace reindexer {

namespace {
bool isTagIndexOrEmptyName(TagIndex) noexcept { return true; }
bool isTagIndexOrEmptyName(TagName tagName) noexcept { return tagName.IsEmpty(); }
}  // namespace

ProtobufBuilder::ProtobufBuilder(WrSerializer* wrser, ObjType type, const Schema* schema, const TagsMatcher* tm, const TagsPath* tagsPath,
								 concepts::TagNameOrIndex auto tag)
	: type_(type), ser_(wrser), tm_(tm), tagsPath_(tagsPath), schema_(schema), sizeHelper_(), itemsFieldIndex_(toItemsFieldIndex(tag)) {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObject:
			putFieldHeader(tag, PBUF_TYPE_LENGTHENCODED);
			sizeHelper_ = ser_->StartVString();
			break;
		case ObjType::TypeObjectArray:
			itemsFieldIndex_ = toItemsFieldIndex(tag);
			break;
		case ObjType::TypePlain:
			break;
	}
}
template ProtobufBuilder::ProtobufBuilder(WrSerializer*, ObjType, const Schema*, const TagsMatcher*, const TagsPath*, TagName);
template ProtobufBuilder::ProtobufBuilder(WrSerializer*, ObjType, const Schema*, const TagsMatcher*, const TagsPath*, TagIndex);

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

void ProtobufBuilder::packItem(concepts::TagNameOrIndex auto tag, TagType tagType, Serializer& rdser, ProtobufBuilder& array) {
	switch (tagType) {
		case TAG_DOUBLE:
			array.put(tag, rdser.GetDouble());
			break;
		case TAG_VARINT:
			array.put(tag, rdser.GetVarint());
			break;
		case TAG_BOOL:
			array.put(tag, rdser.GetBool());
			break;
		case TAG_STRING:
			array.put(tag, rdser.GetVString());
			break;
		case TAG_UUID:
			array.put(tag, rdser.GetUuid());
			break;
		case TAG_FLOAT:
			array.put(tag, rdser.GetFloat());
			break;
		case TAG_NULL:
			array.Null(tag);
			break;
		case TAG_ARRAY:
		case TAG_OBJECT:
		case TAG_END:
			throw Error(errParseJson, "Unexpected cjson typeTag '{}' while parsing value", TagTypeToStr(tagType));
	}
}
template void ProtobufBuilder::packItem(TagName, TagType, Serializer&, ProtobufBuilder&);
template void ProtobufBuilder::packItem(TagIndex, TagType, Serializer&, ProtobufBuilder&);

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

void ProtobufBuilder::putFieldHeader(concepts::TagNameOrIndex auto tag, ProtobufTypes type) {
	ser_->PutVarUint((unsigned(getFieldTag(tag).AsNumber()) << kTypeBit) | type);
}

std::string_view ProtobufBuilder::tagToName(TagName tagName) { return tm_->tag2name(tagName); }
std::string_view ProtobufBuilder::tagToName(TagIndex) { return tm_->tag2name(TagName::Empty()); }

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, bool val) { return put(tag, int(val)); }
template void ProtobufBuilder::put(TagName, bool);
template void ProtobufBuilder::put(TagIndex, bool);

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, int val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](concepts::OneOf<KeyValueType::Int, KeyValueType::Bool> auto) noexcept {},
								[&](KeyValueType::Int64) {
									put(tag, int64_t(val));
									done = true;
								},
								[&](KeyValueType::Double) {
									put(tag, double(val));
									done = true;
								},
								[&](KeyValueType::Float) {
									put(tag, float(val));
									done = true;
								},
								[&](concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
													KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector> auto) {
									throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tagToName(tag));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tag, PBUF_TYPE_VARINT);
		}
		ser_->PutVarint(val);
	}
}

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, int64_t val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](KeyValueType::Int64) noexcept {},
								[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int> auto) {
									put(tag, int(val));
									done = true;
								},
								[&](KeyValueType::Double) {
									put(tag, double(val));
									done = true;
								},
								[&](KeyValueType::Float) {
									put(tag, float(val));
									done = true;
								},
								[&](concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
													KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector> auto) {
									throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tagToName(tag));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tag, PBUF_TYPE_VARINT);
		}
		ser_->PutVarint(val);
	}
}

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, double val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf([&](KeyValueType::Double) noexcept {},
								[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Bool> auto) {
									put(tag, int(val));
									done = true;
								},
								[&](KeyValueType::Float) {
									this->put(tag, float(val));
									done = true;
								},
								[&](KeyValueType::Int64) {
									put(tag, int64_t(val));
									done = true;
								},
								[&](concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
													KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector> auto) {
									throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tagToName(tag));
								});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tag, PBUF_TYPE_FLOAT64);
		}
		ser_->PutDouble(val);
	}
}

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, float val) {
	bool done = false;
	if (const auto res = getExpectedFieldType(); res.second) {
		res.first.EvaluateOneOf(
			[&](KeyValueType::Double) {
				put(tag, double(val));
				done = true;
			},
			[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Bool> auto) {
				put(tag, int(val));
				done = true;
			},
			[&](KeyValueType::Float) {},
			[&](KeyValueType::Int64) {
				put(tag, int64_t(val));
				done = true;
			},
			[&](concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
								KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector> auto) {
				throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tagToName(tag));
			});
	}
	if (!done) {
		if (type_ != ObjType::TypeArray) {
			putFieldHeader(tag, PBUF_TYPE_FLOAT32);
		}
		ser_->PutFloat(val);
	}
}

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, std::string_view val) {
	if (const auto res = getExpectedFieldType(); res.second) {
		if (!res.first.Is<KeyValueType::String>()) {
			throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tagToName(tag));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(tag, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutVString(val);
}
template void ProtobufBuilder::put(TagIndex, std::string_view);

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, Uuid val) {
	if (const auto res = getExpectedFieldType(); res.second) {
		if (!res.first.Is<KeyValueType::String>()) {
			throw Error(errParams, "Expected type '{}' for field '{}'", res.first.Name(), tagToName(tag));
		}
	}
	if (type_ != ObjType::TypeArray) {
		putFieldHeader(tag, PBUF_TYPE_LENGTHENCODED);
	}
	ser_->PutStrUuid(val);
}
template void ProtobufBuilder::put(TagIndex, Uuid);

void ProtobufBuilder::put(concepts::TagNameOrIndex auto tag, const Variant& val) {
	val.Type().EvaluateOneOf([&](KeyValueType::Int64) { put(tag, int64_t(val)); }, [&](KeyValueType::Int) { put(tag, int(val)); },
							 [&](KeyValueType::Double) { put(tag, double(val)); }, [&](KeyValueType::Float) { put(tag, float(val)); },
							 [&](KeyValueType::String) { put(tag, std::string_view(val)); },
							 [&](KeyValueType::Bool) { put(tag, bool(val)); },
							 [&](KeyValueType::Tuple) {
								 auto arrNode = ArrayPacked(tag);
								 for (auto& itVal : val.getCompositeValues()) {
									 arrNode.Put(tag, itVal, 0);
								 }
							 },
							 [&](KeyValueType::Uuid) { put(tag, Uuid{val}); },
							 [&](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
												 KeyValueType::FloatVector> auto) noexcept {});
}
template void ProtobufBuilder::put(TagName, const Variant&);
template void ProtobufBuilder::put(TagIndex, const Variant&);

ProtobufBuilder ProtobufBuilder::Object(concepts::TagNameOrIndex auto tag, int) {
	// Main object in Protobuf is never of Object type,
	// only nested object fields are.
	if (type_ == ObjType::TypePlain && isTagIndexOrEmptyName(tag)) {
		return ProtobufBuilder(std::move(*this));
	}
	return ProtobufBuilder(ser_, ObjType::TypeObject, schema_, tm_, tagsPath_, getFieldTag(tag));
}

template ProtobufBuilder ProtobufBuilder::Object(TagName, int);
template ProtobufBuilder ProtobufBuilder::Object(TagIndex, int);

}  // namespace reindexer
