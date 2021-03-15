#include "protobufschemabuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "core/schema.h"

namespace reindexer {

ProtobufSchemaBuilder::ProtobufSchemaBuilder()
	: ser_(nullptr), fieldsTypes_(nullptr), pt_(nullptr), tm_(nullptr), type_(ObjType::TypePlain) {}

ProtobufSchemaBuilder::ProtobufSchemaBuilder(WrSerializer* ser, SchemaFieldsTypes* fieldsTypes, ObjType type, string_view name,
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

string_view ProtobufSchemaBuilder::jsonSchemaTypeToProtobufType(const FieldProps& props, KeyValueType& type) {
	if (props.type == "string") {
		type = KeyValueString;
		return "string"_sv;
	} else if (props.type == "integer") {
		type = KeyValueInt64;
		if (tm_ && pt_) {
			TagsPath& tagsPath = fieldsTypes_->tagsPath_;
			int field = tm_->tags2field(tagsPath.data(), tagsPath.size());
			if (field > 0 && pt_->Field(field).Type() == KeyValueInt) {
				type = KeyValueInt;
			}
		}
		return "int64"_sv;
	} else if (props.type == "number") {
		type = KeyValueDouble;
		return "double"_sv;
	} else if (props.type == "boolean") {
		type = KeyValueBool;
		return "bool"_sv;
	} else if (props.type == "object") {
		type = KeyValueComposite;
		return props.xGoType;
	} else if (props.type == "null") {
		type = KeyValueNull;
		return string_view();
	}
	type = KeyValueUndefined;
	return string_view();
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

void ProtobufSchemaBuilder::Field(string_view name, int tagName, const FieldProps& props) {
	TagsPathScope<TagsPath> tagScope(fieldsTypes_->tagsPath_, tagName);
	KeyValueType type;
	string_view typeName = jsonSchemaTypeToProtobufType(props, type);
	if (type == KeyValueUndefined || typeName.empty()) {
		throw Error(errLogic, "Can't get protobuf schema - field [%s] is of unsupported type [%s] (%s)", name, props.type, props.xGoType);
	}
	if (props.isArray) {
		assert(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		if (ser_) ser_->Write("repeated ");
		writeField(name, typeName, tagName);
		if (type == KeyValueBool || type == KeyValueInt || type == KeyValueInt64 || type == KeyValueDouble) {
			if (ser_) ser_->Write(" [packed=true]");
		}
	} else {
		writeField(name, typeName, tagName);
	}
	fieldsTypes_->AddField(type, props.isArray);
	if (ser_) ser_->Write(";\n");
}

ProtobufSchemaBuilder ProtobufSchemaBuilder::Object(int tagName, string_view name, bool buildTypesOnly,
													std::function<void(ProtobufSchemaBuilder& self)> filler) {
	fieldsTypes_->tagsPath_.push_back(tagName);
	fieldsTypes_->AddObject(name);
	ProtobufSchemaBuilder obj(buildTypesOnly ? nullptr : ser_, fieldsTypes_, ObjType::TypeObject, name, pt_, tm_);
	if (filler) {
		filler(obj);
	}

	return obj;
}

void ProtobufSchemaBuilder::writeField(string_view name, string_view type, int number) {
	if (ser_) {
		ser_->Write(type);
		ser_->Write(" ");
		ser_->Write(name);
		ser_->Write(" = ");
		ser_->Write(std::to_string(number));
	}
}

}  // namespace reindexer
