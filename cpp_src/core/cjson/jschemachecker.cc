#include "jschemachecker.h"
#include <map>
#include <string>
#include <vector>
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {


JsonSchemaChecker::JsonSchemaChecker(const std::string& json,std::string rootTypeName):rootTypeName_(rootTypeName) {
	Error err = createTypeTable(json);
	if (!err.ok()) throw err;
	isInit=true;
}

Error JsonSchemaChecker::Init(const std::string& json, std::string rootTypeName) {

	if(isInit)
		return Error(errLogic ,"JsonSchemaChecker already initialized.");
	rootTypeName_ = std::move(rootTypeName);
	return createTypeTable(json);
}

bool JsonSchemaChecker::isSimpleType(string_view tp) {
	return tp == "string"_sv || tp == "number"_sv || tp == "integer"_sv || tp == "boolean"_sv;
}

std::string JsonSchemaChecker::createType(const PrefixTree::PrefixTreeNode* node, const std::string& typeName) {
	JsonSchemaChecker::TypeDescr typeDescr;
	if (indexes_.find(typeName) != indexes_.end()) return typeName;
	if (node->children_.empty() && node->props_.allowAdditionalProps == true) {
		return std::string("any");
	}
	std::string tpName(typeName);
	if (tpName.empty()) {
		tpName = "type_" + std::to_string(typeIndex_);
		typeIndex_++;
	}
	for (auto it = node->children_.begin(); it != node->children_.end(); ++it) {
		SubElement subElement;
		subElement.required = it->second->props_.isRequired;
		subElement.array = it->second->props_.isArray;
		if (isSimpleType(it->second->props_.type)) {
			if (!it->second->children_.empty()) {
				throw Error(errLogic, "Simple type has childs");
			}
			subElement.typeName = it->second->props_.type;

		} else if (it->second->props_.type == "object") {
			subElement.typeName = createType(it->second.get());
		} else {
			throw Error(errLogic, "Incorrect schema type [%s]", it->second->props_.type);
		}
		auto typeIndexIt = indexes_.find(subElement.typeName);
		if (typeIndexIt == indexes_.end()) throw Error(errLogic, "Incorrect type %s", subElement.typeName);
		subElement.typeIndex = typeIndexIt->second;
		typeDescr.subElementsTable.emplace_back(it->first, subElement);
	}
	typeDescr.name = tpName;
	typeDescr.allowAdditionalProps = node->props_.allowAdditionalProps;
	typeDescr.init();
	typesTable_.push_back(std::move(typeDescr));
	indexes_.insert(std::make_pair(tpName, typesTable_.size() - 1));
	return tpName;
}

void JsonSchemaChecker::addSimpleType(std::string tpName) {
	typesTable_.emplace_back();
	typesTable_.back().name = tpName;
	indexes_.emplace(std::move(tpName), typesTable_.size() - 1);
}

Error JsonSchemaChecker::createTypeTable(const std::string& json) {
	auto err = schema_.FromJSON(string_view(json));
	if (!err.ok()) return err;
	auto root = schema_.GetRoot();

	addSimpleType("any");
	addSimpleType("string");
	addSimpleType("number");
	addSimpleType("integer");
	addSimpleType("boolean");

	try {
		createType(root, rootTypeName_);
	} catch (const Error& e) {
		return e;
	}

	valAppearance_.reserve(typesTable_.size());
	for (unsigned int i = 0; i < typesTable_.size(); ++i) {
		valAppearance_.emplace_back();
		auto& lastVal = valAppearance_.back();
		lastVal.reserve(typesTable_[i].subElementsTable.size());
		for (unsigned int k = 0; k < typesTable_[i].subElementsTable.size(); ++k) {
			lastVal.emplace_back(typesTable_[i].subElementsTable[k].second.required);
		}
	}
	return Error();
}

Error JsonSchemaChecker::Check(gason::JsonNode node) {
	if (node.value.getTag() != gason::JSON_OBJECT) {
		return Error(errParseJson, "Node [%s] should JSON_OBJECT.", node.key);
	}

	auto indxIt = indexes_.find(rootTypeName_);
	if (indxIt == indexes_.end()) return Error(errParseJson, "Type '%s' not found.", rootTypeName_);
	int nType = indxIt->second;
	std::string path;
	path.reserve(512);
	return checkScheme(node, nType, path, rootTypeName_);
}

struct PathPop {
	PathPop(std::string& path) : pathStack(path), len(path.size()) {}
	~PathPop() { pathStack.resize(len); }
	std::string& pathStack;
	size_t len;
};

Error JsonSchemaChecker::checkScheme(const gason::JsonNode& node, int typeIndex, std::string& path, const std::string& elementName) {
	PathPop popPath(path);
	if (!path.empty()) {
		path += ".";
	}
	path += elementName;
	const TypeDescr& descr = typesTable_[typeIndex];
	h_vector<ValAppearance, 16> mmVals(valAppearance_[typeIndex].begin(), valAppearance_[typeIndex].end());
	Error err;
	for (const auto& elem : node) {
		auto subElemIndex = descr.subElementsIndex.find(string_view(elem.key));
		if (subElemIndex == descr.subElementsIndex.end()) {
			if (!descr.allowAdditionalProps)
				return Error(errParseJson, "Key [%s] not allowed in [%s] object.", elem.key, path);
			else
				continue;
		}
		err = checkExists(elem.key, &mmVals[subElemIndex->second], path);
		if (!err.ok()) return err;
		if (elem.value.getTag() == gason::JSON_OBJECT) {
			if (descr.subElementsTable[subElemIndex->second].second.typeName != "any") {
				err = checkScheme(elem, descr.subElementsTable[subElemIndex->second].second.typeIndex, path,
								  descr.subElementsTable[subElemIndex->second].first);
				if (!err.ok()) return err;
			}
		} else if (elem.value.getTag() == gason::JSON_ARRAY) {
			if (descr.subElementsTable[subElemIndex->second].second.typeName != "any") {
				if (!descr.subElementsTable[subElemIndex->second].second.array) {
					return Error(errParseJson, "Element [%s] should array in [%s].", elem.key, path);
				}
				for (auto entry : elem.value) {
					if (entry->value.getTag() == gason::JSON_ARRAY || entry->value.getTag() == gason::JSON_OBJECT) {
						err = checkScheme(*entry, descr.subElementsTable[subElemIndex->second].second.typeIndex, path,
										  descr.subElementsTable[subElemIndex->second].first);
						if (!err.ok()) return err;
					}
				}
			}
		}
	}
	err = checkRequired(mmVals, typeIndex, path);
	return err;
}

Error JsonSchemaChecker::checkExists(string_view name, ValAppearance* element, const std::string& path) {
	if (!element->notExist) {
		return Error(errParseJson, "Key [%s] can occur only once in [%s] object.", std::string(name), path);
	}
	element->notExist = false;
	element->required = false;
	return Error();
}

Error JsonSchemaChecker::checkRequired(const h_vector<ValAppearance, 16>& elementAppearances, int typeNum, const std::string& path) {
	for (unsigned int k = 0; k < elementAppearances.size(); k++) {
		if (elementAppearances[k].required) {
			return Error(errParseJson, "Key [%s] must occur in [%s] object.", typesTable_[typeNum].subElementsTable[k].first, path);
		}
	}
	return Error();
}

}  // namespace reindexer
