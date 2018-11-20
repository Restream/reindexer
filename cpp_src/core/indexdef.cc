#include "core/indexdef.h"
#include <unordered_map>
#include "cjson/jsonbuilder.h"
#include "string.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

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

IndexDef::IndexDef() {}
IndexDef::IndexDef(const string &name, const JsonPaths &jsonPaths, const string &indexType, const string &fieldType, const IndexOpts opts)
	: name_(name), jsonPaths_(jsonPaths), indexType_(indexType), fieldType_(fieldType), opts_(opts) {}

IndexDef::IndexDef(const string &name, const string &indexType, const string &fieldType, const IndexOpts opts)
	: name_(name), jsonPaths_({name}), indexType_(indexType), fieldType_(fieldType), opts_(opts) {}

IndexDef::IndexDef(const string &name, const JsonPaths &jsonPaths, const IndexType type, const IndexOpts opts)
	: name_(name), jsonPaths_(jsonPaths), opts_(opts) {
	this->FromType(type);
}

bool IndexDef::operator==(const IndexDef &other) const {
	return name_ == other.name_ && jsonPaths_ == other.jsonPaths_ && Type() == other.Type() && fieldType_ == other.fieldType_ &&
		   opts_ == other.opts_;
}

IndexType IndexDef::Type() const {
	string iType = indexType_;
	if (iType == "") {
		if (fieldType_ == "double") iType = "tree";
		if (fieldType_ != "double") iType = "hash";
		if (fieldType_ == "bool") iType = "-";
	}
	for (auto &it : availableIndexes) {
		if (fieldType_ == it.second.fieldType && iType == it.second.indexType) return it.first;
	}

	throw Error(errParams, "Unsupported combination of field '%s' type '%s' and index type '%s'", name_.c_str(), fieldType_.c_str(),
				indexType_.c_str());
}

void IndexDef::FromType(IndexType type) {
	auto &it = availableIndexes.at(type);
	fieldType_ = it.fieldType;
	indexType_ = it.indexType;
}

const vector<string> &IndexDef::Conditions() const { return availableIndexes.find(Type())->second.conditions; }
bool isComposite(IndexType type) {
	return type == IndexCompositeBTree || type == IndexCompositeFastFT || type == IndexCompositeFuzzyFT || type == IndexCompositeHash;
}
bool isFullText(IndexType type) {
	return type == IndexFastFT || type == IndexFuzzyFT || type == IndexCompositeFastFT || type == IndexCompositeFuzzyFT;
}
bool isSortable(IndexType type) { return availableIndexes.at(type).caps & CapSortable; }
string IndexDef::getCollateMode() const { return availableCollates.at(opts_.GetCollateMode()); }

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
		if (jvalue.getTag() != JSON_OBJECT) throw Error(errParseJson, "Expected json object in 'indexes' key");

		CollateMode collateValue = CollateNone;
		bool isPk = false, isArray = false, isDense = false, isSparse = false;
		string jsonPath;
		string collateStr;
		string sortOrderLetters;
		string config;

		for (auto elem : jvalue) {
			parseJsonField("name", name_, elem);
			parseJsonField("field_type", fieldType_, elem);
			parseJsonField("index_type", indexType_, elem);
			parseJsonField("is_pk", isPk, elem);
			parseJsonField("is_array", isArray, elem);
			parseJsonField("is_dense", isDense, elem);
			parseJsonField("is_sparse", isSparse, elem);
			parseJsonField("collate_mode", collateStr, elem);
			parseJsonField("sort_order_letters", sortOrderLetters, elem);
			parseJsonField("json_path", jsonPath, elem);

			if ("json_paths"_sv == elem->key) {
				if (elem->value.getTag() != JSON_ARRAY) throw Error(errParseJson, "Expected array in 'json_paths' key");
				for (auto subElem : elem->value) {
					if (subElem->value.getTag() != JSON_STRING) throw Error(errParseJson, "Expected string elements in 'json_paths' array");
					jsonPaths_.push_back(subElem->value.toString());
				}
			}

			if ("config"_sv == elem->key) {
				config = stringifyJson(elem);
			}
		}
		if (!jsonPath.empty()) {
			jsonPaths_.push_back(jsonPath);
			logPrintf(LogWarning,
					  "indexDef.json_path is used. It has been deprecated and will be removed in future releases. Use json_paths instead");
		}

		opts_.PK(isPk).Array(isArray).Dense(isDense).Sparse(isSparse);
		opts_.config = config;

		if (!collateStr.empty()) {
			auto collateIt = find_if(begin(availableCollates), end(availableCollates),
									 [&collateStr](const pair<CollateMode, string> &p) { return p.second == collateStr; });

			if (collateIt != end(availableCollates)) {
				collateValue = collateIt->first;
				opts_.SetCollateMode(collateValue);
				if (collateValue == CollateCustom && !sortOrderLetters.empty()) {
					opts_.collateOpts_ = CollateOpts(sortOrderLetters);
				}
			} else {
				return Error(errParams, "Unknown collate mode %s", collateStr.c_str());
			}
		}
	} catch (const Error &err) {
		return err;
	}

	return 0;
}

void IndexDef::GetJSON(WrSerializer &ser, bool describeCompat) const {
	JsonBuilder builder(ser);

	builder.Put("name", name_)
		.Put("field_type", fieldType_)
		.Put("index_type", indexType_)
		.Put("is_pk", opts_.IsPK())
		.Put("is_array", opts_.IsArray())
		.Put("is_dense", opts_.IsDense())
		.Put("is_sparse", opts_.IsSparse())
		.Put("collate_mode", getCollateMode())
		.Put("sort_order_letters", opts_.collateOpts_.sortOrderTable.GetSortOrderCharacters())
		.Raw("config", opts_.hasConfig() ? opts_.config.c_str() : "{}");

	if (describeCompat) {
		// extra data for support describe.
		// TODO: deprecate and remove it
		builder.Put("is_sortable", isSortable(Type()));
		builder.Put("is_fulltext", isFullText(Type()));
		auto arr = builder.Array("conditions");
		for (auto &cond : Conditions()) {
			arr.Put(nullptr, cond);
		}
	}
	auto arrNode = builder.Array("json_paths");
	for (auto &jsonPath : jsonPaths_) {
		arrNode.Put(nullptr, jsonPath);
	}
}

}  // namespace reindexer
