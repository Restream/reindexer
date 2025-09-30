#pragma once

#include <unordered_map>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "estl/h_vector.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class TagsMatcher;
class PayloadType;
class WrSerializer;

namespace builders {
class ProtobufSchemaBuilder;
}  // namespace builders
using builders::ProtobufSchemaBuilder;

std::string_view kvTypeToJsonSchemaType(KeyValueType type);

class [[nodiscard]] FieldProps {
public:
	FieldProps() = default;
	FieldProps(KeyValueType _type, IsArray _isArray = IsArray_False, IsRequired _isRequired = IsRequired_False,
			   AllowAdditionalProps _allowAdditionalProps = AllowAdditionalProps_False, const std::string& _xGoType = {})
		: type(kvTypeToJsonSchemaType(_type)),
		  xGoType(_xGoType),
		  isArray(_isArray),
		  isRequired(_isRequired),
		  allowAdditionalProps(_allowAdditionalProps) {}
	FieldProps(std::string _type, IsArray _isArray = IsArray_False, IsRequired _isRequired = IsRequired_False,
			   AllowAdditionalProps _allowAdditionalProps = AllowAdditionalProps_False, const std::string& _xGoType = {})
		: type(std::move(_type)),
		  xGoType(_xGoType),
		  isArray(_isArray),
		  isRequired(_isRequired),
		  allowAdditionalProps(_allowAdditionalProps) {}
	FieldProps(FieldProps&&) = default;
	FieldProps& operator=(FieldProps&&) = default;

	bool operator==(const FieldProps& rh) const noexcept {
		return type == rh.type && xGoType == rh.xGoType && isArray == rh.isArray && isRequired == rh.isRequired &&
			   allowAdditionalProps == rh.allowAdditionalProps;
	}

	std::string type;
	std::string xGoType;
	bool isArray = false;
	bool isRequired = false;
	bool allowAdditionalProps = false;
};

class Schema;

struct [[nodiscard]] SchemaFieldType {
	KeyValueType type_{KeyValueType::Undefined{}};
	bool isArray_{false};
};

class [[nodiscard]] SchemaFieldsTypes {
public:
	void AddObject(std::string objectType);
	void AddField(KeyValueType type, bool isArray);
	KeyValueType GetField(const TagsPath& fieldPath, bool& isArray) const;
	std::string GenerateObjectName();

	bool NeedToEmbedType(const std::string& objectType) const noexcept;

private:
	friend class ProtobufSchemaBuilder;

	TagsPath tagsPath_;
	std::unordered_map<TagsPath, SchemaFieldType> types_;
	std::unordered_map<std::string, int> objectTypes_;
	int generatedObjectsNames = {0};
};

class [[nodiscard]] PrefixTree {
public:
	using PathT = h_vector<std::string, 10>;

	PrefixTree();

	void SetXGoType(std::string_view type);

	Error AddPath(FieldProps props, const PathT& splittedPath) noexcept;
	std::vector<std::string> GetSuggestions(std::string_view path) const;
	std::vector<std::string> GetPaths() const;
	bool HasPath(std::string_view path, bool allowAdditionalFields) const noexcept;
	Error BuildProtobufSchema(WrSerializer& schema, TagsMatcher& tm, PayloadType& pt) noexcept;

	class PrefixTreeNode;
	using map = fast_hash_map<std::string, std::unique_ptr<PrefixTreeNode>, hash_str, equal_str, less_str>;

	class [[nodiscard]] PrefixTreeNode {
	public:
		PrefixTreeNode() = default;
		PrefixTreeNode(FieldProps&& p) : props(std::move(p)) {}

		void GetPaths(std::string&& basePath, std::vector<std::string>& pathsList) const;

		FieldProps props;
		map children;
	};

private:
	friend Schema;
	static std::string pathToStr(const PathT&);
	PrefixTreeNode* findNode(std::string_view path, bool* maybeAdditionalField = nullptr) const noexcept;
	Error buildProtobufSchema(ProtobufSchemaBuilder& builder, const PrefixTreeNode& node, const std::string& basePath,
							  TagsMatcher& tm) noexcept;

	PrefixTreeNode root_;
	SchemaFieldsTypes fieldsTypes_;
};

class [[nodiscard]] Schema {
public:
	Schema() = default;
	explicit Schema(std::string_view json);

	std::vector<std::string> GetSuggestions(std::string_view path) const { return paths_.GetSuggestions(path); }
	std::vector<std::string> GetPaths() const noexcept { return paths_.GetPaths(); }
	KeyValueType GetFieldType(const TagsPath& fieldPath, bool& isArray) const;

	bool HasPath(std::string_view path, bool allowAdditionalFields = false) const noexcept {
		return paths_.HasPath(path, allowAdditionalFields);
	}

	Error FromJSON(std::string_view json);
	void GetJSON(WrSerializer&) const;
	std::string_view GetJSON() const noexcept { return originalJson_; }
	Error BuildProtobufSchema(TagsMatcher& tm, PayloadType& pt);
	Error GetProtobufSchema(WrSerializer& schema) const;
	int GetProtobufNsNumber() const noexcept { return protobufNsNumber_; }
	const PrefixTree::PrefixTreeNode* GetRoot() const noexcept { return &paths_.root_; }
	static std::string AppendProtobufNumber(std::string_view json, int protobufNsNumber);

	std::vector<TagName> MakeCsvTagOrdering(const TagsMatcher& tm) const;
	bool IsEmpty() const noexcept;

private:
	void parseJsonNode(const gason::JsonNode& node, PrefixTree::PathT& splittedPath, bool isRequired);

	PrefixTree paths_;
	std::string originalJson_ = "{}";
	std::string protobufSchema_;
	Error protobufSchemaStatus_;
	int protobufNsNumber_;
};

}  // namespace reindexer
