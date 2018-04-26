#include "indexdef.h"
#include <unordered_map>
#include "string.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"

namespace reindexer {

enum Caps { CapComposite = 0x1, CapSortable = 0x2, CapFullText = 0x4 };
struct IndexInfo {
	const string fieldType, indexType;
	const vector<string> conditions;
	int caps;
};

const vector<string> condsUsual = {"SET", "EQ", "ANY", "EMPTY", "LT", "LE", "GT", "GE", "RANGE"};
const vector<string> condsText = {"MATCH"};
const vector<string> condsBool = {"SET", "EQ", "ANY", "EMPTY"};

// clang-format off
std::unordered_map<IndexType, IndexInfo,std::hash<int>,std::equal_to<int> > availableIndexes = {
	{IndexIntHash,		    {"int",       "hash",    condsUsual,CapSortable}},
	{IndexInt64Hash,	    {"int64",     "hash",    condsUsual,CapSortable}},
	{IndexStrHash,		    {"string",    "hash",    condsUsual,CapSortable}},
	{IndexCompositeHash,    {"composite", "hash",    condsUsual,CapSortable|CapComposite}},
	{IndexIntBTree,		    {"int",       "tree",    condsUsual,CapSortable}},
	{IndexInt64BTree,	    {"int64",     "tree",    condsUsual,CapSortable}},
	{IndexDoubleBTree,	    {"double",    "tree",    condsUsual,CapSortable}},
	{IndexCompositeBTree,   {"composite", "tree",    condsUsual,CapComposite|CapSortable}},
	{IndexStrBTree,		    {"string",    "tree",    condsUsual,CapSortable}},
	{IndexIntStore,		    {"int",       "-",       condsUsual,CapSortable}},
	{IndexBool,			    {"bool",      "-",       condsBool, 0}},
	{IndexInt64Store,	    {"int64",     "-",       condsUsual,CapSortable}},
	{IndexStrStore,		    {"string",    "-",       condsUsual,CapSortable}},
	{IndexDoubleStore,	    {"double",    "-",       condsUsual,CapSortable}},
	{IndexStrStore,		    {"string",    "-",       condsUsual,CapSortable}},
	{IndexCompositeFastFT,  {"composite", "text",    condsText, CapComposite|CapFullText}},
	{IndexCompositeFuzzyFT, {"composite", "fuzzytext",condsText, CapComposite|CapFullText}},
	{IndexFastFT,           {"string",    "text",    condsText, CapFullText}},
	{IndexFuzzyFT,          {"string",    "fuzzytext", condsText, CapFullText}},
};

std::unordered_map<CollateMode, const string, std::hash<int>, std::equal_to<int> > availableCollates = {
	{CollateASCII,   "ascii"},
	{CollateUTF8,    "utf8"},
	{CollateNumeric, "numeric"},
	{CollateCustom,  "custom"},
	{CollateNone,    "none"},
};

// clang-format on

IndexType IndexDef::Type() const {
	string iType = indexType;
	if (iType == "") {
		if (fieldType == "double") iType = "tree";
		if (fieldType != "double") iType = "hash";
		if (fieldType == "bool") iType = "-";
	}
	for (auto &it : availableIndexes) {
		if (fieldType == it.second.fieldType && iType == it.second.indexType) return it.first;
	}

	throw Error(errParams, "Unsupported combination of field '%s' type '%s' and index type '%s'", name.c_str(), fieldType.c_str(),
				indexType.c_str());
}

void IndexDef::FromType(IndexType type) {
	auto &it = availableIndexes.at(type);
	fieldType = it.fieldType;
	indexType = it.indexType;
}

const vector<string> &IndexDef::Conditions() const { return availableIndexes.find(Type())->second.conditions; }
bool isComposite(IndexType type) { return availableIndexes.at(type).caps & CapComposite; }
bool isFullText(IndexType type) { return availableIndexes.at(type).caps & CapFullText; }
bool isSortable(IndexType type) { return availableIndexes.at(type).caps & CapSortable; }
string IndexDef::getCollateMode() const { return availableCollates.at(opts.GetCollateMode()); }

Error IndexDef::FromJSON(char *json) {
	JsonAllocator jalloc;
	JsonValue jvalue;
	char *endp;

	int status = jsonParse(json, &endp, &jvalue, jalloc);
	if (status != JSON_OK) {
		return Error(errParseJson, "Malformed JSON with namespace indexes");
	}

	return IndexDef::FromJSON(jvalue);
}

Error IndexDef::FromJSON(JsonValue &jvalue) {
	try {
		CollateMode collateValue = CollateNone;
		bool isPk = false, isArray = false, isDense = false, isAppendable = false;
		for (auto elem : jvalue) {
			parseJsonField("name", name, elem);
			parseJsonField("json_path", jsonPath, elem);
			parseJsonField("field_type", fieldType, elem);
			parseJsonField("index_type", indexType, elem);
			parseJsonField("is_pk", isPk, elem);
			parseJsonField("is_array", isArray, elem);
			parseJsonField("is_dense", isDense, elem);
			parseJsonField("is_appendable", isAppendable, elem);

			string collateStr;
			parseJsonField("collate_mode", collateStr, elem);
			if (collateStr != "") {
				bool found = false;
				for (auto it : availableCollates) {
					if (it.second == collateStr) {
						found = true;
						collateValue = it.first;
						opts.SetCollateMode(collateValue);
						if (collateValue != CollateCustom) break;
					}
				}
				if (!found) return Error(errParams, "Unknown collate mode %s", collateStr.c_str());
			}

			if (collateValue == CollateCustom) {
				string sortOrderLetters;
				parseJsonField("sort_order_letters", sortOrderLetters, elem);
				if (!sortOrderLetters.empty()) {
					opts.collateOpts_ = CollateOpts(sortOrderLetters);
					break;
				}
			}
		}
		opts.PK(isPk).Array(isArray).Dense(isDense).Appendable(isAppendable);
	} catch (const Error &err) {
		return err;
	}

	return 0;
}

void IndexDef::GetJSON(WrSerializer &ser) {
	ser.PutChar('{');
	ser.Printf("\"name\":\"%s\",", name.c_str());
	ser.Printf("\"json_path\":\"%s\",", jsonPath.c_str());
	ser.Printf("\"field_type\":\"%s\",", fieldType.c_str());
	ser.Printf("\"index_type\":\"%s\",", indexType.c_str());
	ser.Printf("\"is_pk\":%s,", opts.IsPK() ? "true" : "false");
	ser.Printf("\"is_array\":%s,", opts.IsArray() ? "true" : "false");
	ser.Printf("\"is_dense\":%s,", opts.IsDense() ? "true" : "false");
	ser.Printf("\"is_appendable\":%s,", opts.IsAppendable() ? "true" : "false");
	ser.Printf("\"collate_mode\":\"%s\",", getCollateMode().c_str());
	ser.Printf("\"sort_order_letters\":\"%s\"", opts.collateOpts_.sortOrderTable.GetSortOrderCharacters().c_str());
	ser.PutChars("}");
}

}  // namespace reindexer
