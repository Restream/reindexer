#include "schema.h"
#include <unordered_set>
#include "gason/gason.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

PrefixTree::PrefixTree() { root_.props_.type = "object"; }

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

Schema::Schema(string_view json) {
	auto err = FromJSON(json);
	if (!err.ok()) {
		throw err;
	}
}

Error Schema::FromJSON(span<char> json) {
	Error err = errOK;
	try {
		PrefixTree::PathT path;
		std::string originalJson(json.data(), json.size());
		parseJsonNode(gason::JsonParser().Parse(json), path, true);
		originalJson_ = std::move(originalJson);
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "Schema: %s\n", ex.what(), originalJson_);
	} catch (const Error& err) {
		return err;
	}
	return err;
}

Error Schema::FromJSON(string_view json) {
	Error err = errOK;
	try {
		PrefixTree::PathT path;
		parseJsonNode(gason::JsonParser().Parse(json), path, true);
		originalJson_.assign(json.data(), json.size());
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

void Schema::parseJsonNode(const gason::JsonNode& node, PrefixTree::PathT& splittedPath, bool isRequired) {
	bool isArray = false;

	FieldProps field;
	field.type = node["type"].As<std::string>();
	if (string_view(field.type) == "array"_sv) {
		field.type = node["items"]["type"].As<std::string>();
		field.allowAdditionalProps = node["items"]["additionalProperties"].As<bool>(false);
		field.isArray = true;
		isArray = true;
	} else {
		field.allowAdditionalProps = node["additionalProperties"].As<bool>(false);
	}
	field.isRequired = isRequired;
	if (!splittedPath.empty()) {
		paths_.AddPath(std::move(field), splittedPath);
	} else
		paths_.root_.props_ = std::move(field);

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
