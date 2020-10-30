#pragma once

#include "core/cjson/tagsmatcherimpl.h"
#include "estl/string_view.h"
#include "objtype.h"

namespace reindexer {

class SchemaFieldsTypes;
class FieldProps;
class PayloadType;
class TagsMatcher;
class WrSerializer;

class ProtobufSchemaBuilder {
public:
	ProtobufSchemaBuilder();
	ProtobufSchemaBuilder(WrSerializer* ser, SchemaFieldsTypes* fieldsTypes, ObjType type, string_view name = string_view(),
						  PayloadType* pt = nullptr, TagsMatcher* tm = nullptr);
	ProtobufSchemaBuilder(ProtobufSchemaBuilder&&);
	ProtobufSchemaBuilder(const ProtobufSchemaBuilder&) = delete;
	ProtobufSchemaBuilder& operator=(ProtobufSchemaBuilder&&) = delete;
	ProtobufSchemaBuilder& operator=(const ProtobufSchemaBuilder&) = delete;
	~ProtobufSchemaBuilder();

	void Field(string_view name, int tagName, const FieldProps& props);
	ProtobufSchemaBuilder Object(int tagName, string_view name, bool buildTypesOnly = false,
								 std::function<void(ProtobufSchemaBuilder& self)> = nullptr);

	void End();

private:
	void writeField(string_view name, string_view type, int number);
	string_view jsonSchemaTypeToProtobufType(const FieldProps& props, KeyValueType& type);

	WrSerializer* ser_;
	SchemaFieldsTypes* fieldsTypes_;
	PayloadType* pt_;
	TagsMatcher* tm_;
	ObjType type_;
};

}  // namespace reindexer
