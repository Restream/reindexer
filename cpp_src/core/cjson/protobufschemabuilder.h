#pragma once

#include <string_view>
#include "core/key_value_type.h"
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
	ProtobufSchemaBuilder(WrSerializer* ser, SchemaFieldsTypes* fieldsTypes, ObjType type, std::string_view name = std::string_view(),
						  PayloadType* pt = nullptr, TagsMatcher* tm = nullptr);
	ProtobufSchemaBuilder(ProtobufSchemaBuilder&&) noexcept;
	ProtobufSchemaBuilder(const ProtobufSchemaBuilder&) = delete;
	ProtobufSchemaBuilder& operator=(ProtobufSchemaBuilder&&) = delete;
	ProtobufSchemaBuilder& operator=(const ProtobufSchemaBuilder&) = delete;
	~ProtobufSchemaBuilder();

	void Field(std::string_view name, int tagName, const FieldProps& props);
	ProtobufSchemaBuilder Object(int tagName, std::string_view name, bool buildTypesOnly = false,
								 const std::function<void(ProtobufSchemaBuilder& self)>& = nullptr);

	void End();

private:
	void writeField(std::string_view name, std::string_view type, int number);
	std::pair<std::string_view, KeyValueType> jsonSchemaTypeToProtobufType(const FieldProps& props) const;

	WrSerializer* ser_;
	SchemaFieldsTypes* fieldsTypes_;
	PayloadType* pt_;
	TagsMatcher* tm_;
	ObjType type_;
};

}  // namespace reindexer
