#include "core/indexdef.h"
#include <unordered_map>
#include "string.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
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

JsonPaths::JsonPaths() {}
JsonPaths::JsonPaths(const string_view &jsonPath) { split(jsonPath, ",", false, *this); }
void JsonPaths::Set(const string_view &other) { split(other, ",", false, *this); }
void JsonPaths::Set(const vector<string> &other) { vector::operator=(other); }

string JsonPaths::AsSerializedString() const {
	string output;
	for (size_t i = 0; i < size(); ++i) {
		const string &jsonPath(at(i));
		if (jsonPath.empty()) continue;
		output += jsonPath;
		if (i != size() - 1) output += ',';
	}
	return output;
}

IndexDef::IndexDef() {}
IndexDef::IndexDef(const string &name, const string_view &jsonPaths, const string &indexType, const string &fieldType, const IndexOpts opts)
	: name_(name), jsonPaths_(jsonPaths), indexType_(indexType), fieldType_(fieldType), opts_(opts) {}
IndexDef::IndexDef(const string &name, const string_view &jsonPaths, const IndexType type, const IndexOpts opts)
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
bool isComposite(IndexType type) { return availableIndexes.at(type).caps & CapComposite; }
bool isFullText(IndexType type) { return availableIndexes.at(type).caps & CapFullText; }
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
			parseJsonField("json_path", jsonPath, elem);
			parseJsonField("collate_mode", collateStr, elem);
			parseJsonField("sort_order_letters", sortOrderLetters, elem);

			if (!strcmp(elem->key, "config")) {
				config = stringifyJson(elem);
			}
		}

		opts_.PK(isPk).Array(isArray).Dense(isDense).Sparse(isSparse);
		opts_.config = config;

		if (!jsonPath.empty()) jsonPaths_.Set(jsonPath);
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
	ser.PutChar('{');
	ser.Printf("\"name\":\"%s\",", name_.c_str());
	ser.Printf("\"json_path\":\"%s\",", jsonPaths_.AsSerializedString().c_str());
	ser.Printf("\"field_type\":\"%s\",", fieldType_.c_str());
	ser.Printf("\"index_type\":\"%s\",", indexType_.c_str());
	ser.Printf("\"is_pk\":%s,", opts_.IsPK() ? "true" : "false");
	ser.Printf("\"is_array\":%s,", opts_.IsArray() ? "true" : "false");
	ser.Printf("\"is_dense\":%s,", opts_.IsDense() ? "true" : "false");
	ser.Printf("\"is_sparse\":%s,", opts_.IsSparse() ? "true" : "false");
	ser.Printf("\"collate_mode\":\"%s\",", getCollateMode().c_str());
	ser.Printf("\"sort_order_letters\":\"%s\",", opts_.collateOpts_.sortOrderTable.GetSortOrderCharacters().c_str());
	ser.Printf("\"config\":%s", opts_.hasConfig() ? opts_.config.c_str() : "{}");

	if (describeCompat) {
		// extra data for support describe.
		// TODO: deprecate and remove it
		ser.Printf(",\"is_sortable\":%s,", isSortable(Type()) ? "true" : "false");
		ser.Printf("\"is_fulltext\":%s,", isFullText(Type()) ? "true" : "false");
		ser.Printf("\"conditions\": [");
		auto conds = Conditions();
		for (unsigned j = 0; j < conds.size(); j++) {
			if (j != 0) ser.PutChar(',');
			ser.Printf("\"%s\"", conds.at(j).c_str());
		}
		ser.PutChar(']');
	}
	ser.PutChar('}');
}  // namespace reindexer

}  // namespace reindexer
