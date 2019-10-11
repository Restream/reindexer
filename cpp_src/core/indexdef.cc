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
std::unordered_map<IndexType, IndexInfo, std::hash<int>, std::equal_to<int> > availableIndexes = {
	{IndexIntHash,			{"int", "hash",				condsUsual,	CapSortable}},
	{IndexInt64Hash,		{"int64", "hash",			condsUsual,	CapSortable}},
	{IndexStrHash,			{"string", "hash",			condsUsual,	CapSortable}},
	{IndexCompositeHash,	{"composite", "hash",		condsUsual,	CapSortable | CapComposite}},
	{IndexIntBTree,			{"int", "tree",				condsUsual,	CapSortable}},
	{IndexInt64BTree,		{"int64", "tree",			condsUsual,	CapSortable}},
	{IndexDoubleBTree,		{"double", "tree",			condsUsual,	CapSortable}},
	{IndexCompositeBTree,	{"composite", "tree",		condsUsual,	CapComposite | CapSortable}},
	{IndexStrBTree,			{"string", "tree",			condsUsual,	CapSortable}},
	{IndexIntStore,			{"int", "-",				condsUsual,	CapSortable}},
	{IndexBool,				{"bool", "-",				condsBool,	0}},
	{IndexInt64Store,		{"int64", "-",				condsUsual,	CapSortable}},
	{IndexStrStore,			{"string", "-",				condsUsual,	CapSortable}},
	{IndexDoubleStore,		{"double", "-",				condsUsual,	CapSortable}},
	{IndexTtl,				{"int64", "ttl",			condsUsual,	CapSortable}},
	{IndexCompositeFastFT,	{"composite", "text",		condsText,	CapComposite | CapFullText}},
	{IndexCompositeFuzzyFT,	{"composite", "fuzzytext",	condsText,	CapComposite | CapFullText}},
	{IndexFastFT,			{"string", "text",			condsText,	CapFullText}},
	{IndexFuzzyFT,			{"string", "fuzzytext",		condsText,	CapFullText}},
};

std::unordered_map<CollateMode, const string, std::hash<int>, std::equal_to<int> > availableCollates = {
	{CollateASCII, "ascii"}, {CollateUTF8, "utf8"}, {CollateNumeric, "numeric"}, {CollateCustom, "custom"}, {CollateNone, "none"},
};

// clang-format on

IndexDef::IndexDef() {}
IndexDef::IndexDef(const string &name) : name_(name) {}

IndexDef::IndexDef(const string &name, const JsonPaths &jsonPaths, const string &indexType, const string &fieldType, const IndexOpts opts)
	: name_(name), jsonPaths_(jsonPaths), indexType_(indexType), fieldType_(fieldType), opts_(opts) {}

IndexDef::IndexDef(const string &name, const JsonPaths &jsonPaths, const string &indexType, const string &fieldType, const IndexOpts opts,
				   int64_t expireAfter)
	: IndexDef(name, jsonPaths, indexType, fieldType, opts) {
	expireAfter_ = expireAfter;
}

IndexDef::IndexDef(const string &name, const string &indexType, const string &fieldType, const IndexOpts opts)
	: name_(name), jsonPaths_({name}), indexType_(indexType), fieldType_(fieldType), opts_(opts) {}

IndexDef::IndexDef(const string &name, const JsonPaths &jsonPaths, const IndexType type, const IndexOpts opts)
	: name_(name), jsonPaths_(jsonPaths), opts_(opts) {
	this->FromType(type);
}

bool IndexDef::IsEqual(const IndexDef &other, bool skipConfig) const {
	return name_ == other.name_ && jsonPaths_ == other.jsonPaths_ && Type() == other.Type() && fieldType_ == other.fieldType_ &&
		   opts_.IsEqual(other.opts_, skipConfig) && expireAfter_ == other.expireAfter_;
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

	throw Error(errParams, "Unsupported combination of field '%s' type '%s' and index type '%s'", name_, fieldType_, indexType_);
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

Error IndexDef::FromJSON(span<char> json) {
	try {
		IndexDef::FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "IndexDef: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void IndexDef::FromJSON(const gason::JsonNode &root) {
	name_ = root["name"].As<string>();
	fieldType_ = root["field_type"].As<string>();
	indexType_ = root["index_type"].As<string>();
	expireAfter_ = root["expire_after"].As<int64_t>();
	opts_.PK(root["is_pk"].As<bool>());
	opts_.Array(root["is_array"].As<bool>());
	opts_.Dense(root["is_dense"].As<bool>());
	opts_.Sparse(root["is_sparse"].As<bool>());
	opts_.SetConfig(stringifyJson(root["config"]));
	jsonPaths_.clear();
	for (auto &subElem : root["json_paths"]) {
		jsonPaths_.push_back(subElem.As<string>());
	}

	auto collateStr = root["collate_mode"].As<string_view>();
	if (!collateStr.empty()) {
		auto collateIt = find_if(begin(availableCollates), end(availableCollates),
								 [&collateStr](const pair<CollateMode, string> &p) { return collateStr == p.second; });
		if (collateIt == end(availableCollates)) throw Error(errParams, "Unknown collate mode %s", collateStr);
		CollateMode collateValue = collateIt->first;
		opts_.SetCollateMode(collateValue);
		if (collateValue == CollateCustom) {
			opts_.collateOpts_ = CollateOpts(root["sort_order_letters"].As<string>());
		}
	}
}

void IndexDef::GetJSON(WrSerializer &ser, int formatFlags) const {
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
		.Put("expire_after", expireAfter_)
		.Raw("config", opts_.hasConfig() ? opts_.config : "{}");

	if (formatFlags & kIndexJSONWithDescribe) {
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
