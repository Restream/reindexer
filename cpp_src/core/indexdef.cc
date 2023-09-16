#include "core/indexdef.h"
#include <unordered_map>
#include "cjson/jsonbuilder.h"
#include "string.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "type_consts_helpers.h"

namespace {

static const std::vector<std::string> &condsUsual() {
	using namespace std::string_literals;
	static const std::vector data{"SET"s, "EQ"s, "ANY"s, "EMPTY"s, "LT"s, "LE"s, "GT"s, "GE"s, "RANGE"s};
	return data;
}

static const std::vector<std::string> &condsText() {
	using namespace std::string_literals;
	static const std::vector data{"MATCH"s};
	return data;
}

static const std::vector<std::string> &condsBool() {
	using namespace std::string_literals;
	static const std::vector data{"SET"s, "EQ"s, "ANY"s, "EMPTY"s};
	return data;
}

static const std::vector<std::string> &condsGeom() {
	using namespace std::string_literals;
	static const std::vector data{"DWITHIN"s};
	return data;
}

enum Caps { CapComposite = 0x1, CapSortable = 0x2, CapFullText = 0x4 };

struct IndexInfo {
	const std::string fieldType, indexType;
	const std::vector<std::string> &conditions;
	int caps;
};

static const std::unordered_map<IndexType, IndexInfo, std::hash<int>, std::equal_to<int>> &availableIndexes() {
	using namespace std::string_literals;
	// clang-format off
	static const std::unordered_map<IndexType, IndexInfo, std::hash<int>, std::equal_to<int>> data {
		{IndexIntHash,			{"int"s, "hash"s,				condsUsual(),	CapSortable}},
		{IndexInt64Hash,		{"int64"s, "hash"s,				condsUsual(),	CapSortable}},
		{IndexStrHash,			{"string"s, "hash",				condsUsual(),	CapSortable}},
		{IndexCompositeHash,	{"composite"s, "hash"s,			condsUsual(),	CapSortable | CapComposite}},
		{IndexIntBTree,			{"int"s, "tree"s,				condsUsual(),	CapSortable}},
		{IndexInt64BTree,		{"int64"s, "tree"s,				condsUsual(),	CapSortable}},
		{IndexDoubleBTree,		{"double"s, "tree"s,			condsUsual(),	CapSortable}},
		{IndexCompositeBTree,	{"composite"s, "tree"s,			condsUsual(),	CapComposite | CapSortable}},
		{IndexStrBTree,			{"string"s, "tree"s,			condsUsual(),	CapSortable}},
		{IndexIntStore,			{"int"s, "-"s,					condsUsual(),	CapSortable}},
		{IndexBool,				{"bool"s, "-"s,					condsBool(),	0}},
		{IndexInt64Store,		{"int64"s, "-"s,				condsUsual(),	CapSortable}},
		{IndexStrStore,			{"string"s, "-"s,				condsUsual(),	CapSortable}},
		{IndexDoubleStore,		{"double"s, "-"s,				condsUsual(),	CapSortable}},
		{IndexTtl,				{"int64"s, "ttl"s,				condsUsual(),	CapSortable}},
		{IndexCompositeFastFT,	{"composite"s, "text"s,			condsText(),	CapComposite | CapFullText}},
		{IndexCompositeFuzzyFT,	{"composite"s, "fuzzytext"s,	condsText(),	CapComposite | CapFullText}},
		{IndexFastFT,			{"string"s, "text"s,			condsText(),	CapFullText}},
		{IndexFuzzyFT,			{"string"s, "fuzzytext"s,		condsText(),	CapFullText}},
		{IndexRTree,			{"point"s, "rtree"s,			condsGeom(),	0}},
		{IndexUuidHash,			{"uuid"s, "hash"s,				condsUsual(),	CapSortable}},
		{IndexUuidStore,		{"uuid"s, "-"s,					condsUsual(),	CapSortable}},
	};
	// clang-format on
	return data;
}

static const std::unordered_map<CollateMode, const std::string, std::hash<int>, std::equal_to<int>> &availableCollates() {
	using namespace std::string_literals;
	static const std::unordered_map<CollateMode, const std::string, std::hash<int>, std::equal_to<int>> data{
		{CollateASCII, "ascii"s}, {CollateUTF8, "utf8"s}, {CollateNumeric, "numeric"s}, {CollateCustom, "custom"s}, {CollateNone, "none"s},
	};
	return data;
}

constexpr char const *kRTreeLinear = "linear";
constexpr char const *kRTreeQuadratic = "quadratic";
constexpr char const *kRTreeGreene = "greene";
constexpr char const *kRTreeRStar = "rstar";

}  // namespace

namespace reindexer {

IndexDef::IndexDef(std::string name) : name_(std::move(name)) {}

IndexDef::IndexDef(std::string name, JsonPaths jsonPaths, std::string indexType, std::string fieldType, IndexOpts opts)
	: name_(std::move(name)),
	  jsonPaths_(std::move(jsonPaths)),
	  indexType_(std::move(indexType)),
	  fieldType_(std::move(fieldType)),
	  opts_(std::move(opts)) {}

IndexDef::IndexDef(std::string name, JsonPaths jsonPaths, std::string indexType, std::string fieldType, IndexOpts opts, int64_t expireAfter)
	: IndexDef(std::move(name), std::move(jsonPaths), std::move(indexType), std::move(fieldType), std::move(opts)) {
	expireAfter_ = expireAfter;
}

IndexDef::IndexDef(std::string name, std::string indexType, std::string fieldType, IndexOpts opts)
	: name_(std::move(name)),
	  jsonPaths_({name_}),
	  indexType_(std::move(indexType)),
	  fieldType_(std::move(fieldType)),
	  opts_(std::move(opts)) {}

IndexDef::IndexDef(std::string name, JsonPaths jsonPaths, IndexType type, IndexOpts opts)
	: name_(std::move(name)), jsonPaths_(std::move(jsonPaths)), opts_(std::move(opts)) {
	this->FromType(type);
}

bool IndexDef::IsEqual(const IndexDef &other, bool skipConfig) const {
	return name_ == other.name_ && jsonPaths_ == other.jsonPaths_ && Type() == other.Type() && fieldType_ == other.fieldType_ &&
		   opts_.IsEqual(other.opts_, skipConfig) && expireAfter_ == other.expireAfter_;
}

IndexType IndexDef::Type() const {
	using namespace std::string_view_literals;
	std::string_view iType = indexType_;
	if (iType == "") {
		if (fieldType_ == "double"sv) {
			iType = "tree";
		} else if (fieldType_ == "bool"sv) {
			iType = "-";
		} else if (fieldType_ == "point"sv) {
			iType = "rtree";
		} else {
			iType = "hash";
		}
	}
	for (const auto &it : availableIndexes()) {
		if (fieldType_ == it.second.fieldType && iType == it.second.indexType) return it.first;
	}

	throw Error(errParams, "Unsupported combination of field '%s' type '%s' and index type '%s'", name_, fieldType_, indexType_);
}

void IndexDef::FromType(IndexType type) {
	const auto &it = availableIndexes().at(type);
	fieldType_ = it.fieldType;
	indexType_ = it.indexType;
}

const std::vector<std::string> &IndexDef::Conditions() const {
	const auto it{availableIndexes().find(Type())};
	assertrx(it != availableIndexes().cend());
	return it->second.conditions;
}

bool isSortable(IndexType type) { return availableIndexes().at(type).caps & CapSortable; }

bool isStore(IndexType type) noexcept {
	return type == IndexIntStore || type == IndexInt64Store || type == IndexStrStore || type == IndexDoubleStore || type == IndexBool ||
		   type == IndexUuidStore;
}

std::string IndexDef::getCollateMode() const { return availableCollates().at(opts_.GetCollateMode()); }

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
	name_ = root["name"].As<std::string>();
	jsonPaths_.clear();
	for (auto &subElem : root["json_paths"]) {
		jsonPaths_.push_back(subElem.As<std::string>());
	}
	fieldType_ = root["field_type"].As<std::string>();
	indexType_ = root["index_type"].As<std::string>();
	expireAfter_ = root["expire_after"].As<int64_t>();
	opts_.PK(root["is_pk"].As<bool>());
	opts_.Array(root["is_array"].As<bool>());
	opts_.Dense(root["is_dense"].As<bool>());
	opts_.Sparse(root["is_sparse"].As<bool>());
	if (fieldType_ == "uuid" && opts_.IsSparse()) {
		throw Error(errParams, "UUID index cannot be sparse");
	}
	opts_.SetConfig(stringifyJson(root["config"]));
	const std::string rtreeType = root["rtree_type"].As<std::string>();
	if (rtreeType.empty()) {
		if (indexType_ == "rtree" || fieldType_ == "point") {
			throw Error(errParams, "RTree type does not set");
		}
	} else {
		if (rtreeType == kRTreeLinear) {
			opts_.RTreeType(IndexOpts::Linear);
		} else if (rtreeType == kRTreeQuadratic) {
			opts_.RTreeType(IndexOpts::Quadratic);
		} else if (rtreeType == kRTreeGreene) {
			opts_.RTreeType(IndexOpts::Greene);
		} else if (rtreeType == kRTreeRStar) {
			opts_.RTreeType(IndexOpts::RStar);
		} else {
			throw Error(errParams, "Unknown RTree type %s", rtreeType);
		}
	}

	auto collateStr = root["collate_mode"].As<std::string_view>();
	if (!collateStr.empty()) {
		auto collateIt = find_if(begin(availableCollates()), end(availableCollates()),
								 [&collateStr](const std::pair<CollateMode, std::string> &p) { return collateStr == p.second; });
		if (collateIt == end(availableCollates())) throw Error(errParams, "Unknown collate mode %s", collateStr);
		CollateMode collateValue = collateIt->first;
		opts_.SetCollateMode(collateValue);
		if (collateValue == CollateCustom) {
			opts_.collateOpts_ = CollateOpts(root["sort_order_letters"].As<std::string>());
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
		.Put("is_sparse", opts_.IsSparse());
	if (indexType_ == "rtree" || fieldType_ == "point") {
		switch (opts_.RTreeType()) {
			case IndexOpts::Linear:
				builder.Put("rtree_type", kRTreeLinear);
				break;
			case IndexOpts::Quadratic:
				builder.Put("rtree_type", kRTreeQuadratic);
				break;
			case IndexOpts::Greene:
				builder.Put("rtree_type", kRTreeGreene);
				break;
			case IndexOpts::RStar:
				builder.Put("rtree_type", kRTreeRStar);
				break;
			default:
				assertrx(0);
				abort();
		}
	}
	builder.Put("collate_mode", getCollateMode())
		.Put("sort_order_letters", opts_.collateOpts_.sortOrderTable.GetSortOrderCharacters())
		.Put("expire_after", expireAfter_)
		.Raw("config", opts_.hasConfig() ? opts_.config : "{}");

	if (formatFlags & kIndexJSONWithDescribe) {
		// extra data for support describe.
		// TODO: deprecate and remove it
		builder.Put("is_sortable", isSortable(Type()));
		builder.Put("is_fulltext", IsFullText(Type()));
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

bool validateIndexName(std::string_view name, IndexType type) noexcept {
	if (!name.length()) {
		return false;
	}
	for (auto c : name) {
		if (!(std::isalpha(c) || std::isdigit(c) || c == '.' || c == '_' || c == '-')) {
			if (!(c == '+' && IsComposite(type))) {
				return false;
			}
		}
	}
	return true;
}

}  // namespace reindexer
