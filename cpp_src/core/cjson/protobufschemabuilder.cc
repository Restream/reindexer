#include "protobufschemabuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "core/schema.h"

namespace reindexer::builders {

using namespace std::string_view_literals;

ProtobufSchemaBuilder::ProtobufSchemaBuilder()
	: ser_(nullptr), fieldsTypes_(nullptr), pt_(nullptr), tm_(nullptr), type_(ObjType::TypePlain) {}

ProtobufSchemaBuilder::ProtobufSchemaBuilder(WrSerializer* ser, SchemaFieldsTypes* fieldsTypes, ObjType type, std::string_view name,
											 PayloadType* pt, TagsMatcher* tm)
	: ser_(ser), fieldsTypes_(fieldsTypes), pt_(pt), tm_(tm), type_(type) {
	switch (type_) {
		case ObjType::TypePlain:
			if (ser_) {
				ser_->Write("syntax = \"proto3\";\n\n"sv);
			}
			break;
		case ObjType::TypeObject:
			if (ser_) {
				ser_->Write("message "sv);
			}
			if (ser_) {
				ser_->Write(name);
			}
			if (ser_) {
				ser_->Write(" {\n"sv);
			}
			break;
		case ObjType::TypeObjectArray:
		case ObjType::TypeArray:
			break;
	}
}

ProtobufSchemaBuilder::ProtobufSchemaBuilder(ProtobufSchemaBuilder&& obj) noexcept
	: ser_(obj.ser_), fieldsTypes_(obj.fieldsTypes_), pt_(obj.pt_), tm_(obj.tm_), type_(obj.type_) {
	obj.type_ = ObjType::TypePlain;
}
ProtobufSchemaBuilder::~ProtobufSchemaBuilder() { End(); }

std::pair<std::string_view, KeyValueType> ProtobufSchemaBuilder::jsonSchemaTypeToProtobufType(const FieldProps& props) const {
	using namespace std::string_view_literals;
	if (props.type == "string"sv) {
		return {"string"sv, KeyValueType::String{}};
	} else if (props.type == "integer"sv) {
		if (tm_ && pt_) {
			const TagsPath& tagsPath = fieldsTypes_->tagsPath_;
			const auto field = tm_->tags2field(tagsPath);
			if (field.IsRegularIndex() && field.ValueType().Is<KeyValueType::Int>()) {
				return {"sint64"sv, KeyValueType::Int{}};
			}
		}
		return {"sint64"sv, KeyValueType::Int64{}};
	} else if (props.type == "number"sv) {
		return {"double"sv, KeyValueType::Double{}};
	} else if (props.type == "boolean"sv) {
		return {"bool"sv, KeyValueType::Bool{}};
	} else if (props.type == "object"sv) {
		return {props.xGoType, KeyValueType::Composite{}};
	} else if (props.type == "null"sv) {
		return {{}, KeyValueType::Null{}};
	}
	return {{}, KeyValueType::Undefined{}};
}

void ProtobufSchemaBuilder::End() {
	if (type_ == ObjType::TypeObject) {
		if (fieldsTypes_->tagsPath_.size() > 0) {
			fieldsTypes_->tagsPath_.pop_back();
		}
		if (ser_) {
			ser_->Write("}\n"sv);
		}
	}
	type_ = ObjType::TypePlain;
}

void ProtobufSchemaBuilder::Field(std::string_view name, TagName tagName, const FieldProps& props) {
	TagsPathScope<TagsPath> tagScope(fieldsTypes_->tagsPath_, tagName);
	const auto [typeName, type] = jsonSchemaTypeToProtobufType(props);
	if (type.Is<KeyValueType::Undefined>() || typeName.empty()) {
		throw Error(errLogic, "Can't get protobuf schema - field [{}] is of unsupported type [{}] ({})", name, props.type, props.xGoType);
	}
	if (props.isArray) {
		assertrx(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		if (ser_) {
			ser_->Write("repeated "sv);
		}
		writeField(name, typeName, tagName);
		type.EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
								KeyValueType::Float> auto) {
				if (ser_) {
					ser_->Write(" [packed=true]");
				}
			},
			[](concepts::OneOf<KeyValueType::String, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
							   KeyValueType::Null, KeyValueType::Uuid, KeyValueType::FloatVector> auto) noexcept {});
	} else {
		writeField(name, typeName, tagName);
	}
	fieldsTypes_->AddField(type, props.isArray);
	if (ser_) {
		ser_->Write(";\n");
	}
}

ProtobufSchemaBuilder ProtobufSchemaBuilder::Object(TagName tagName, std::string_view name, bool buildTypesOnly,
													const std::function<void(ProtobufSchemaBuilder& self)>& filler) {
	fieldsTypes_->tagsPath_.emplace_back(tagName);
	fieldsTypes_->AddObject(std::string{name});
	ProtobufSchemaBuilder obj(buildTypesOnly ? nullptr : ser_, fieldsTypes_, ObjType::TypeObject, name, pt_, tm_);
	if (filler) {
		filler(obj);
	}

	return obj;
}

void ProtobufSchemaBuilder::writeField(std::string_view name, std::string_view type, TagName tagName) {
	if (ser_) {
		ser_->Write(type);
		ser_->Write(" ");
		ser_->Write(name);
		ser_->Write(" = ");
		ser_->Write(std::to_string(tagName.AsNumber()));
	}
}

}  // namespace reindexer::builders
