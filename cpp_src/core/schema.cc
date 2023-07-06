#include "schema.h"
#include <unordered_set>
#include "core/cjson/protobufschemabuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "gason/gason.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

using namespace std::string_view_literals;

std::string_view kvTypeToJsonSchemaType(KeyValueType type) {
	return type.EvaluateOneOf([](OneOf<KeyValueType::Int, KeyValueType::Int64>) noexcept { return "integer"sv; },
							  [](KeyValueType::Double) noexcept { return "number"sv; },
							  [](KeyValueType::Bool) noexcept { return "boolean"sv; },
							  [](OneOf<KeyValueType::String, KeyValueType::Uuid>) noexcept { return "string"sv; },
							  [](KeyValueType::Null) noexcept { return "null"sv; }, [](KeyValueType::Tuple) noexcept { return "object"sv; },
							  [&](OneOf<KeyValueType::Composite, KeyValueType::Undefined>) -> std::string_view {
								  throw Error(errParams, "Impossible to convert type [%s] to json schema type", type.Name());
							  });
}

void SchemaFieldsTypes::AddObject(std::string objectType) {
	types_[tagsPath_] = {KeyValueType::Composite{}, false};
	const auto it = objectTypes_.find(objectType);
	if (it == objectTypes_.end()) {
		objectTypes_.emplace(std::move(objectType), tagsPath_.size());
	} else {
		int depth = int(tagsPath_.size());
		if (depth < it->second) {
			it->second = depth;
		}
	}
}

void SchemaFieldsTypes::AddField(KeyValueType type, bool isArray) { types_[tagsPath_] = {type, isArray}; }

bool SchemaFieldsTypes::NeedToEmbedType(const std::string& objectType) const noexcept {
	auto it = objectTypes_.find(objectType);
	if (it == objectTypes_.end()) return false;
	return it->second < int(tagsPath_.size());
}

KeyValueType SchemaFieldsTypes::GetField(const TagsPath& fieldPath, bool& isArray) const {
	auto it = types_.find(fieldPath);
	if (it == types_.end()) return KeyValueType::Undefined{};
	isArray = it->second.isArray_;
	return it->second.type_;
}

std::string SchemaFieldsTypes::GenerateObjectName() { return "GeneratedType" + std::to_string(++generatedObjectsNames); }

PrefixTree::PrefixTree() : root_(FieldProps("object")) {}

void PrefixTree::SetXGoType(std::string_view type) { root_.props.xGoType.assign(type.data(), type.size()); }

Error PrefixTree::AddPath(FieldProps props, const PathT& splittedPath) noexcept {
	if (splittedPath.empty()) {
		return Error();
	}

	auto node = &root_;
	for (auto fieldIt = splittedPath.begin(); fieldIt != splittedPath.end(); ++fieldIt) {
		bool eos = (fieldIt + 1 == splittedPath.end());

		auto nextNodeIt = node->children.find(*fieldIt);
		if (nextNodeIt != node->children.end()) {
			if (eos) {
				auto& foundVal = nextNodeIt.value()->props;
				if (foundVal == props) {
					return Error();
				}
				if (foundVal.type.empty()) {
					foundVal = std::move(props);
					return Error();
				}

				return Error(errLogic, "Field with path '%s' already exists and has different type", pathToStr(splittedPath));
			}
		} else {
			try {
				if (eos) {
					node->children.emplace(*fieldIt, std::make_unique<PrefixTreeNode>(std::move(props)));
					return Error();
				}
				[[maybe_unused]] bool res;
				std::tie(nextNodeIt, res) = node->children.emplace(*fieldIt, std::make_unique<PrefixTreeNode>(FieldProps("object")));
			} catch (...) {
				return Error(errLogic, "PrefixTree.AddPath: Unexpected exception for path: '%s'", pathToStr(splittedPath));	 // For PVS
			}
		}
		node = nextNodeIt.value().get();
	}
	return Error();
}

std::vector<std::string> PrefixTree::GetSuggestions(std::string_view path) const {
	const PrefixTreeNode* node = nullptr;
	std::string_view field;
	std::vector<std::string> suggestions;

	auto lastDot = path.find_last_of("."sv);
	bool isNested = false;
	if (lastDot == std::string_view::npos) {
		field = path;
		node = &root_;
	} else {
		node = findNode(path.substr(0, lastDot));
		field = path.substr(lastDot + 1);
		isNested = true;
	}

	if (!node) {
		return suggestions;
	}

	if (field.empty()) {
		suggestions.reserve(node->children.size());
	}
	for (auto& it : node->children) {
		if (field.empty() || checkIfStartsWith(field, it.first)) {
			if (isNested) {
				suggestions.emplace_back("." + std::string(it.first));
			} else {
				suggestions.emplace_back(std::string(it.first));
			}
		}
	}
	return suggestions;
}

std::vector<std::string> PrefixTree::GetPaths() const {
	std::vector<std::string> paths;
	root_.GetPaths(std::string(), paths);
	return paths;
}

bool PrefixTree::HasPath(std::string_view path, bool allowAdditionalFields) const noexcept {
	bool maybeAdditionalField = false;
	return findNode(path, &maybeAdditionalField) || (allowAdditionalFields && maybeAdditionalField);
}

std::string PrefixTree::pathToStr(const PrefixTree::PathT& path) {
	std::string fullPath;
	for (auto& field : path) {
		if (!fullPath.empty()) {
			fullPath += ".";
		}
		fullPath += field;
	}
	return fullPath;
}

PrefixTree::PrefixTreeNode* PrefixTree::findNode(std::string_view path, bool* maybeAdditionalField) const noexcept {
	auto node = &root_;
	for (size_t pos = 0, lastPos = 0; pos != path.length(); lastPos = pos + 1) {
		pos = path.find('.', lastPos);
		bool eos = false;
		if (pos == std::string_view::npos) {
			eos = true;
			pos = path.length();
		}

		std::string_view field = path.substr(lastPos, pos - lastPos);
		auto found = node->children.find(field);
		if (found == node->children.end()) {
			if (maybeAdditionalField && node->props.allowAdditionalProps) {
				*maybeAdditionalField = true;
			}
			return nullptr;
		} else {
			if (eos) {
				return found.value().get();
			} else {
				node = found.value().get();
				if (!node) {
					return nullptr;
				}
			}
		}
	}
	if (maybeAdditionalField && node->props.allowAdditionalProps) {
		*maybeAdditionalField = true;
	}
	return nullptr;
}

void PrefixTree::PrefixTreeNode::GetPaths(std::string&& basePath, std::vector<std::string>& pathsList) const {
	if (children.empty()) {
		if (!basePath.empty()) {
			pathsList.emplace_back(std::move(basePath));
		}
		return;
	}
	for (const auto& child : children) {
		std::string path(basePath);
		if (!path.empty()) {
			path.append(".");
		}
		path.append(child.first);
		child.second->GetPaths(std::move(path), pathsList);
	}
}

Error PrefixTree::BuildProtobufSchema(WrSerializer& schema, TagsMatcher& tm, PayloadType& pt) noexcept {
	if (root_.children.empty()) {
		return Error(errLogic, "Schema is not initialized either just empty");
	}

	fieldsTypes_ = SchemaFieldsTypes();
	const std::string& objName = root_.props.xGoType.empty() ? pt.Name() : root_.props.xGoType;
	ProtobufSchemaBuilder builder(&schema, &fieldsTypes_, ObjType::TypeObject, objName, &pt, &tm);
	return buildProtobufSchema(builder, root_, "", tm);
}

Error PrefixTree::buildProtobufSchema(ProtobufSchemaBuilder& builder, const PrefixTreeNode& root, const std::string& basePath,
									  TagsMatcher& tm) noexcept {
	try {
		for (const auto& child : root.children) {
			const std::string& name = child.first;
			const std::unique_ptr<PrefixTreeNode>& node = child.second;

			std::string path = basePath;
			if (path.size() > 0) path += ".";
			path += name;

			int fieldNumber = tm.name2tag(name, true);
			if (node->props.type == "object") {
				if (node->props.xGoType.empty()) {
					node->props.xGoType = fieldsTypes_.GenerateObjectName();
				}
				if (node->props.xGoType == name) {
					node->props.xGoType = "type" + node->props.xGoType;
				}
				bool buildTypesOnly = fieldsTypes_.NeedToEmbedType(node->props.xGoType);
				ProtobufSchemaBuilder object = builder.Object(fieldNumber, node->props.xGoType, buildTypesOnly);
				buildProtobufSchema(object, *node, path, tm);
			}
			builder.Field(name, fieldNumber, node->props);
		}
	} catch (const Error& err) {
		return err;
	}
	return Error();
}

Schema::Schema(std::string_view json) : paths_(), originalJson_() {
	auto err = FromJSON(json);
	if (!err.ok()) {
		throw err;
	}
}

Error Schema::FromJSON(std::string_view json) {
	static std::atomic<int> counter;
	Error err;
	try {
		PrefixTree::PathT path;
		gason::JsonParser parser;
		auto node = parser.Parse(json);
		parseJsonNode(node, path, true);
		originalJson_.assign(json.data(), json.size());
		protobufNsNumber_ = node["x-protobuf-ns-number"].As<int>(-1);
		if (protobufNsNumber_ == -1 && originalJson_ != "{}") {
			protobufNsNumber_ = counter++;

			// TODO: fix it
			auto pos = originalJson_.find_last_of('}');
			if (pos != std::string::npos) {
				originalJson_ = originalJson_.erase(pos);
				originalJson_ += ",\"x-protobuf-ns-number\":" + std::to_string(protobufNsNumber_) + "}";
			}
		}

	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "Schema: %s\nJson: %s", ex.what(), originalJson_);
	} catch (const Error& err) {
		return err;
	}
	return err;
}

void Schema::GetJSON(WrSerializer& ser) const {
	if (!originalJson_.empty()) {
		ser << originalJson_;
	} else {
		ser << "{}";
	}
}

KeyValueType Schema::GetFieldType(const TagsPath& fieldPath, bool& isArray) const {
	return paths_.fieldsTypes_.GetField(fieldPath, isArray);
}

Error Schema::BuildProtobufSchema(TagsMatcher& tm, PayloadType& pt) {
	WrSerializer ser;
	protobufSchemaStatus_ = paths_.BuildProtobufSchema(ser, tm, pt);
	protobufSchema_ = std::string(ser.Slice());
	return protobufSchemaStatus_;
}

Error Schema::GetProtobufSchema(WrSerializer& schema) const {
	schema.Write(protobufSchema_);
	return protobufSchemaStatus_;
}

void Schema::parseJsonNode(const gason::JsonNode& node, PrefixTree::PathT& splittedPath, bool isRequired) {
	bool isArray = false;

	FieldProps field;
	field.type = node["type"].As<std::string>();
	if (std::string_view(field.type) == "array"sv) {
		field.type = node["items"]["type"].As<std::string>();
		field.allowAdditionalProps = node["items"]["additionalProperties"].As<bool>(false);
		field.xGoType = node["items"]["x-go-type"].As<std::string>();
		field.isArray = true;
		isArray = true;
	} else {
		field.allowAdditionalProps = node["additionalProperties"].As<bool>(false);
		field.xGoType = node["x-go-type"].As<std::string>();
	}
	field.isRequired = isRequired;
	if (!splittedPath.empty()) {
		paths_.AddPath(std::move(field), splittedPath);
	} else {
		paths_.root_.props = std::move(field);
		paths_.SetXGoType(node["x-go-type"].As<std::string_view>());
	}

	std::unordered_set<std::string_view> required;
	auto requiredList = isArray ? node["items"]["required"] : node["required"];
	for (auto& subnode : requiredList) {
		required.emplace(subnode.As<std::string_view>());
	}

	auto& properties = isArray ? node["items"]["properties"] : node["properties"];
	if (!properties.empty()) {
		for (auto& subnode : properties) {
			splittedPath.emplace_back(std::string(subnode.key));
			bool isSubnodeRequred = (required.find(std::string_view(subnode.key)) != required.end());
			parseJsonNode(subnode, splittedPath, isSubnodeRequred);
			splittedPath.pop_back();
		}
	}
}

std::vector<int> Schema::MakeCsvTagOrdering(const TagsMatcher& tm) const {
	if (paths_.root_.children.empty()) {
		return {};
	}

	gason::JsonParser parser;
	auto tags0lvl = parser.Parse(std::string_view(originalJson_))["required"];

	if (tags0lvl.value.getTag() != gason::JsonTag::JSON_ARRAY) {
		throw Error(errParams, "Incorrect type of \"required\" tag in namespace json-schema");
	}

	std::vector<int> result;
	for (const auto& tagNode : tags0lvl.value) {
		const auto& tagName = tagNode.As<std::string_view>();
		auto tag = tm.name2tag(tagName);
		if (tag == 0) {
			throw Error(errParams, "Tag %s not found in tagsmatcher", tagName);
		}
		result.emplace_back(tag);
	}
	return result;
}

bool Schema::IsEmpty() const noexcept { return paths_.root_.children.empty(); }

}  // namespace reindexer
