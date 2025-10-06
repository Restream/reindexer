#pragma once

#include <string>
#include <unordered_map>
#include "core/schema.h"
#include "estl/fast_hash_map.h"
#include "gason/gason.h"
#include "tools/errors.h"

namespace reindexer {

class [[nodiscard]] JsonSchemaChecker {
public:
	explicit JsonSchemaChecker(std::string_view json, std::string rootTypeName);
	JsonSchemaChecker(){};
	Error Init(std::string_view json, std::string rootTypeName);
	Error Check(gason::JsonNode node);

private:
	struct [[nodiscard]] SubElement {
		std::string typeName;
		int typeIndex = -1;
		bool required = false;
		bool array = false;
	};

	struct [[nodiscard]] ValAppearance {
		ValAppearance() : required(false), notExist(true) {}
		ValAppearance(bool required) : required(required), notExist(true) {}
		bool required;	// false if not required or already exist in struct
		bool notExist;	// true if not exist on struct
	};

	struct [[nodiscard]] TypeDescr {
		void init() {
			for (unsigned int i = 0; i < subElementsTable.size(); ++i) {
				subElementsIndex.insert(std::make_pair(subElementsTable[i].first, i));
			}
		}
		std::string name;
		fast_hash_map<std::string, int, nocase_hash_str, nocase_equal_str, nocase_less_str> subElementsIndex;
		bool allowAdditionalProps = false;
		std::vector<std::pair<std::string, SubElement>> subElementsTable;
	};

	Error checkScheme(const gason::JsonNode& node, int typeIndex, std::string& path, const std::string& elementName);
	std::string createType(const PrefixTree::PrefixTreeNode* node, const std::string& typeName = "");
	Error createTypeTable(std::string_view json);
	static bool isSimpleType(std::string_view tp);
	void addSimpleType(std::string tpName);
	Error checkExists(std::string_view name, ValAppearance* element, const std::string& path);
	Error checkRequired(const h_vector<ValAppearance, 16>& elementAppearances, int typeNum, const std::string& path);

	Schema schema_;
	std::vector<TypeDescr> typesTable_;
	std::unordered_map<std::string, unsigned int> indexes_;
	std::vector<std::vector<ValAppearance>> valAppearance_;
	int typeIndex_ = 0;
	std::string rootTypeName_;
	bool isInit = false;
};

}  // namespace reindexer
