#include "schema.h"
#include <unordered_set>
#include "core/cjson/protobufschemabuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "gason/gason.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

string_view kvTypeToJsonSchemaType(KeyValueType type) {
	switch (type) {
		case KeyValueInt:
		case KeyValueInt64:
			return "integer";
		case KeyValueDouble:
			return "number";
		case KeyValueBool:
			return "boolean";
		case KeyValueString:
			return "string";
		case KeyValueNull:
			return "null";
		case KeyValueTuple:
			return "object";
		default:
			throw Error(errParams, "Impossible to convert type [%d] to json schema type", type);
	}
}

void SchemaFieldsTypes::AddObject(string_view objectType) {
	types_[tagsPath_] = {KeyValueComposite, false};
	auto it = objectTypes_.find(string(objectType));
	if (it == objectTypes_.end()) {
		objectTypes_.emplace(std::string(objectType), tagsPath_.size());
	} else {
		int depth = int(tagsPath_.size());
		if (depth < it->second) {
			it->second = depth;
		}
	}
}

void SchemaFieldsTypes::AddField(KeyValueType type, bool isArray) { types_[tagsPath_] = {type, isArray}; }

bool SchemaFieldsTypes::NeedToEmbedType(string objectType) const {
	auto it = objectTypes_.find(objectType);
	if (it == objectTypes_.end()) return false;
	return it->second < int(tagsPath_.size());
}

KeyValueType SchemaFieldsTypes::GetField(const TagsPath& fieldPath, bool& isArray) const {
	auto it = types_.find(fieldPath);
	if (it == types_.end()) return KeyValueUndefined;
	isArray = it->second.isArray_;
	return it->second.type_;
}

string SchemaFieldsTypes::GenerateObjectName() { return "GeneratedType" + std::to_string(++generatedObjectsNames); }

PrefixTree::PrefixTree() { root_.props_.type = "object"; }

void PrefixTree::SetXGoType(string_view type) { root_.props_.xGoType.assign(type.data(), type.size()); }

Error PrefixTree::AddPath(FieldProps props, const PathT& splittedPath) noexcept {
	if (splittedPath.empty()) {
		return errOK;
	}

	auto node = &root_;
	for (auto fieldIt = splittedPath.begin(); fieldIt != splittedPath.end(); ++fieldIt) {
		bool eos = (fieldIt + 1 == splittedPath.end());

		auto nextNodeIt = node->children_.find(*fieldIt);
		if (nextNodeIt != node->children_.end()) {
			if (eos) {
				auto& foundVal = nextNodeIt.value()->props_;
				if (foundVal == props) {
					return errOK;
				}
				if (foundVal.type.empty()) {
					foundVal = std::move(props);
					return errOK;
				}

				return Error(errLogic, "Field with path '%s' already exists and has different type", pathToStr(splittedPath));
			}
		} else {
			try {
				bool res = false;
				if (eos) {
					std::unique_ptr<PrefixTreeNode> newNode(new PrefixTreeNode{FieldProps(std::move(props)), {}});
					std::tie(nextNodeIt, res) = node->children_.emplace(*fieldIt, std::move(newNode));
					return errOK;
				}
				std::unique_ptr<PrefixTreeNode> newNode(new PrefixTreeNode{FieldProps(string("object"_sv)), {}});
				std::tie(nextNodeIt, res) = node->children_.emplace(*fieldIt, std::move(newNode));
			} catch (...) {
				return Error(errLogic, "PrefixTree.AddPath: Unexpected exception for path: '%s'", pathToStr(splittedPath));	 // For PVS
			}
		}
		node = nextNodeIt.value().get();
	}
	return errOK;
}

std::vector<std::string> PrefixTree::GetSuggestions(string_view path) const {
	const PrefixTreeNode* node = nullptr;
	string_view field;
	std::vector<std::string> suggestions;

	auto lastDot = path.find_last_of("."_sv);
	bool isNested = false;
	if (lastDot == string_view::npos) {
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
		suggestions.reserve(node->children_.size());
	}
	for (auto& it : node->children_) {
		if (field.empty() || checkIfStartsWith(field, it.first)) {
			if (isNested) {
				suggestions.emplace_back("." + string(it.first));
			} else {
				suggestions.emplace_back(string(it.first));
			}
		}
	}
	return suggestions;
}

vector<string> PrefixTree::GetPaths() const {
	vector<string> paths;
	root_.GetPaths(std::string(), paths);
	return paths;
}

bool PrefixTree::HasPath(string_view path, bool allowAdditionalFields) const noexcept {
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

PrefixTree::PrefixTreeNode* PrefixTree::findNode(string_view path, bool* maybeAdditionalField) const noexcept {
	auto node = &root_;
	for (size_t pos = 0, lastPos = 0; pos != path.length(); lastPos = pos + 1) {
		pos = path.find('.', lastPos);
		bool eos = false;
		if (pos == string_view::npos) {
			eos = true;
			pos = path.length();
		}

		string_view field = path.substr(lastPos, pos - lastPos);
		auto found = node->children_.find(field);
		if (found == node->children_.end()) {
			if (maybeAdditionalField && node->props_.allowAdditionalProps) {
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
	if (maybeAdditionalField && node->props_.allowAdditionalProps) {
		*maybeAdditionalField = true;
	}
	return nullptr;
}

void PrefixTree::PrefixTreeNode::GetPaths(std::string&& basePath, vector<string>& pathsList) const {
	if (children_.empty()) {
		if (!basePath.empty()) {
			pathsList.emplace_back(std::move(basePath));
		}
		return;
	}
	std::string path = std::move(basePath);
	for (auto& child : children_) {
		child.second->GetPaths(path + (path.empty() ? "" : ".") + string(child.first), pathsList);
	}
}

Error PrefixTree::BuildProtobufSchema(WrSerializer& schema, TagsMatcher& tm, PayloadType& pt) noexcept {
	if (root_.children_.empty()) {
		return Error(errLogic, "Schema is not initialized either just empty");
	}

	fieldsTypes_ = SchemaFieldsTypes();
	const string& objName = root_.props_.xGoType.empty() ? pt.Name() : root_.props_.xGoType;
	ProtobufSchemaBuilder builder(&schema, &fieldsTypes_, ObjType::TypeObject, objName, &pt, &tm);
	return buildProtobufSchema(builder, root_, "", tm);
}

Error PrefixTree::buildProtobufSchema(ProtobufSchemaBuilder& builder, const PrefixTreeNode& root, const std::string& basePath,
									  TagsMatcher& tm) noexcept {
	try {
		for (auto& child : root.children_) {
			const std::string& name = child.first;
			const std::unique_ptr<PrefixTreeNode>& node = child.second;

			string path = basePath;
			if (path.size() > 0) path += ".";
			path += name;

			int fieldNumber = tm.name2tag(name, true);
			if (node->props_.type == "object") {
				if (node->props_.xGoType.empty()) {
					node->props_.xGoType = fieldsTypes_.GenerateObjectName();
				}
				if (node->props_.xGoType == name) {
					node->props_.xGoType = "type" + node->props_.xGoType;
				}
				bool buildTypesOnly = fieldsTypes_.NeedToEmbedType(node->props_.xGoType);
				ProtobufSchemaBuilder object = builder.Object(fieldNumber, node->props_.xGoType, buildTypesOnly);
				buildProtobufSchema(object, *node, path, tm);
			}
			builder.Field(name, fieldNumber, node->props_);
		}
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Schema::Schema(string_view json) : paths_(), originalJson_() {
	auto err = FromJSON(json);
	if (!err.ok()) {
		throw err;
	}
}

Error Schema::FromJSON(string_view json) {
	static std::atomic<int> counter;
	Error err = errOK;
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
			auto pos = originalJson_.find_last_of("}");
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
	protobufSchema_ = string(ser.Slice());
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
	if (string_view(field.type) == "array"_sv) {
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
		paths_.root_.props_ = std::move(field);
		paths_.SetXGoType(node["x-go-type"].As<string_view>());
	}

	std::unordered_set<string_view> required;
	auto requiredList = isArray ? node["items"]["required"] : node["required"];
	for (auto& subnode : requiredList) {
		required.emplace(subnode.As<string_view>());
	}

	auto& properties = isArray ? node["items"]["properties"] : node["properties"];
	if (!properties.empty()) {
		for (auto& subnode : properties) {
			splittedPath.emplace_back(string(subnode.key));
			bool isSubnodeRequred = (required.find(string_view(subnode.key)) != required.end());
			parseJsonNode(subnode, splittedPath, isSubnodeRequred);
			splittedPath.pop_back();
		}
	}
}

}  // namespace reindexer
