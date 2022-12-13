#include "protobufschemabuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "core/schema.h"

namespace reindexer {

ProtobufSchemaBuilder::ProtobufSchemaBuilder()
	: ser_(nullptr), fieldsTypes_(nullptr), pt_(nullptr), tm_(nullptr), type_(ObjType::TypePlain) {}

ProtobufSchemaBuilder::ProtobufSchemaBuilder(WrSerializer* ser, SchemaFieldsTypes* fieldsTypes, ObjType type, std::string_view name,
											 PayloadType* pt, TagsMatcher* tm)
	: ser_(ser), fieldsTypes_(fieldsTypes), pt_(pt), tm_(tm), type_(type) {
	switch (type_) {
		case ObjType::TypePlain:
			if (ser_) ser_->Write("syntax = \"proto3\";\n\n");
			break;
		case ObjType::TypeObject:
			if (ser_) ser_->Write("message ");
			if (ser_) ser_->Write(name);
			if (ser_) ser_->Write(" {\n");
			break;
		default:
			break;
	}
}

ProtobufSchemaBuilder::ProtobufSchemaBuilder(ProtobufSchemaBuilder&& obj)
	: ser_(obj.ser_), fieldsTypes_(obj.fieldsTypes_), pt_(obj.pt_), tm_(obj.tm_), type_(obj.type_) {
	obj.type_ = ObjType::TypePlain;
}
ProtobufSchemaBuilder::~ProtobufSchemaBuilder() { End(); }

std::pair<std::string_view, KeyValueType> ProtobufSchemaBuilder::jsonSchemaTypeToProtobufType(const FieldProps& props) const {
	using namespace std::string_view_literals;
	if (props.type == "string") {
		return {"string"sv, KeyValueType::String{}};
	} else if (props.type == "integer") {
		if (tm_ && pt_) {
			TagsPath& tagsPath = fieldsTypes_->tagsPath_;
			int field = tm_->tags2field(tagsPath.data(), tagsPath.size());
			if (field > 0 && pt_->Field(field).Type().Is<KeyValueType::Int>()) {
				return {"int64"sv, KeyValueType::Int{}};
			}
		}
		return {"int64"sv, KeyValueType::Int64{}};
	} else if (props.type == "number") {
		return {"double"sv, KeyValueType::Double{}};
	} else if (props.type == "boolean") {
		return {"bool"sv, KeyValueType::Bool{}};
	} else if (props.type == "object") {
		return {props.xGoType, KeyValueType::Composite{}};
	} else if (props.type == "null") {
		return {{}, KeyValueType::Null{}};
	}
	return {{}, KeyValueType::Undefined{}};
}

void ProtobufSchemaBuilder::End() {
	if (type_ == ObjType::TypeObject) {
		if (fieldsTypes_->tagsPath_.size() > 0) {
			fieldsTypes_->tagsPath_.pop_back();
		}
		if (ser_) ser_->Write("}\n");
	}
	type_ = ObjType::TypePlain;
}

void ProtobufSchemaBuilder::Field(std::string_view name, int tagName, const FieldProps& props) {
	TagsPathScope<TagsPath> tagScope(fieldsTypes_->tagsPath_, tagName);
	const auto [typeName, type] = jsonSchemaTypeToProtobufType(props);
	if (type.Is<KeyValueType::Undefined>() || typeName.empty()) {
		throw Error(errLogic, "Can't get protobuf schema - field [%s] is of unsupported type [%s] (%s)", name, props.type, props.xGoType);
	}
	if (props.isArray) {
		assertrx(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		if (ser_) ser_->Write("repeated ");
		writeField(name, typeName, tagName);
		type.EvaluateOneOf(
			[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) {
				if (ser_) ser_->Write(" [packed=true]");
			},
			[](OneOf<KeyValueType::String, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
					 KeyValueType::Null>) noexcept {});
	} else {
		writeField(name, typeName, tagName);
	}
	fieldsTypes_->AddField(type, props.isArray);
	if (ser_) ser_->Write(";\n");
}

ProtobufSchemaBuilder ProtobufSchemaBuilder::Object(int tagName, std::string_view name, bool buildTypesOnly,
													const std::function<void(ProtobufSchemaBuilder& self)>& filler) {
	fieldsTypes_->tagsPath_.push_back(tagName);
	fieldsTypes_->AddObject(std::string{name});
	ProtobufSchemaBuilder obj(buildTypesOnly ? nullptr : ser_, fieldsTypes_, ObjType::TypeObject, name, pt_, tm_);
	if (filler) {
		filler(obj);
	}

	return obj;
}

void ProtobufSchemaBuilder::writeField(std::string_view name, std::string_view type, int number) {
	if (ser_) {
		ser_->Write(type);
		ser_->Write(" ");
		ser_->Write(name);
		ser_->Write(" = ");
		ser_->Write(std::to_string(number));
	}
}

}  // namespace reindexer
