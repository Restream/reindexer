#pragma once

#include <functional>
#include <string_view>
#include "core/enums.h"
#include "core/key_value_type.h"
#include "core/tag_name_index.h"

namespace reindexer {

class SchemaFieldsTypes;
class FieldProps;
class PayloadType;
class TagsMatcher;
class WrSerializer;

namespace builders {

class [[nodiscard]] ProtobufSchemaBuilder {
public:
	ProtobufSchemaBuilder();
	ProtobufSchemaBuilder(WrSerializer* ser, SchemaFieldsTypes* fieldsTypes, ObjType type, std::string_view name = std::string_view(),
						  PayloadType* pt = nullptr, TagsMatcher* tm = nullptr);
	ProtobufSchemaBuilder(ProtobufSchemaBuilder&&) noexcept;
	ProtobufSchemaBuilder(const ProtobufSchemaBuilder&) = delete;
	ProtobufSchemaBuilder& operator=(ProtobufSchemaBuilder&&) = delete;
	ProtobufSchemaBuilder& operator=(const ProtobufSchemaBuilder&) = delete;
	~ProtobufSchemaBuilder();

	void Field(std::string_view name, TagName, const FieldProps& props);
	ProtobufSchemaBuilder Object(TagName, std::string_view name, bool buildTypesOnly = false,
								 const std::function<void(ProtobufSchemaBuilder& self)>& = nullptr);
	void End();

private:
	void writeField(std::string_view name, std::string_view type, TagName);
	[[nodiscard]] std::pair<std::string_view, KeyValueType> jsonSchemaTypeToProtobufType(const FieldProps& props) const;

	WrSerializer* ser_{nullptr};
	SchemaFieldsTypes* fieldsTypes_{nullptr};
	PayloadType* pt_{nullptr};
	TagsMatcher* tm_{nullptr};
	ObjType type_{ObjType::TypeObject};
};
}  // namespace builders
using builders::ProtobufSchemaBuilder;
}  // namespace reindexer
