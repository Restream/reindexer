#pragma once

#include <string>
#include <unordered_map>
#include "core/schema.h"
#include "estl/fast_hash_map.h"
#include "gason/gason.h"
#include "tools/errors.h"

namespace reindexer {

class JsonSchemaChecker {
public:
	explicit JsonSchemaChecker(const std::string& json, std::string rootTypeName);
	JsonSchemaChecker(){};
	Error Init(const std::string& json, std::string rootTypeName);
	Error Check(gason::JsonNode node);

private:
	struct SubElement {
		bool required = false;
		bool array = false;
		std::string typeName;
		int typeIndex = -1;
	};

	struct ValAppearance {
		ValAppearance() : required(false), notExist(true) {}
		ValAppearance(bool required) : required(required), notExist(true) {}
		bool required;	// false if not required or already exist in struct
		bool notExist;	// true if not exist on struct
	};

	struct TypeDescr {
		void init() {
			for (unsigned int i = 0; i < subElementsTable.size(); ++i) {
				subElementsIndex.insert(std::make_pair(subElementsTable[i].first, i));
			}
		}
		std::string name;
		fast_hash_map<string, int, nocase_hash_str, nocase_equal_str> subElementsIndex;
		bool allowAdditionalProps = false;
		std::vector<std::pair<std::string, SubElement>> subElementsTable;
	};

	Error checkScheme(const gason::JsonNode& node, int typeIndex, std::string& path, const std::string& elementName);
	std::string createType(const PrefixTree::PrefixTreeNode* node, const std::string& typeName = "");
	Error createTypeTable(const std::string& json);
	static bool isSimpleType(string_view tp);
	void addSimpleType(std::string tpName);
	Error checkExists(string_view name, ValAppearance* element, const std::string& path);
	Error checkRequired(const h_vector<ValAppearance, 16>& elementAppearances, int typeNum, const std::string& path);

	Schema schema_;
	std::vector<TypeDescr> typesTable_;
	std::unordered_map<std::string, unsigned int> indexes_;
	std::vector<std::vector<ValAppearance>> valAppearance_;
	int typeIndex_ = 0;
	std::string rootTypeName_;
	bool isInit=false;
};

}  // namespace reindexer
