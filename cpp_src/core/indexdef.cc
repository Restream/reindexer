#include "core/indexdef.h"
#include <unordered_map>
#include "cjson/jsonbuilder.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "type_consts_helpers.h"
#include "vendor/frozen/unordered_map.h"

namespace {

using namespace std::string_view_literals;

static const std::vector<std::string_view>& condsUsual() {
	static const std::vector data{"SET"sv, "EQ"sv, "ANY"sv, "EMPTY"sv, "LT"sv, "LE"sv, "GT"sv, "GE"sv, "RANGE"sv};
	return data;
}

static const std::vector<std::string_view>& condsText() {
	static const std::vector data{"MATCH"sv};
	return data;
}

static const std::vector<std::string_view>& condsBool() {
	static const std::vector data{"SET"sv, "EQ"sv, "ANY"sv, "EMPTY"sv};
	return data;
}

static const std::vector<std::string_view>& condsGeom() {
	static const std::vector data{"DWITHIN"sv};
	return data;
}

enum Caps { CapComposite = 0x1, CapSortable = 0x2, CapFullText = 0x4 };

struct IndexInfo {
	const std::string_view fieldType, indexType;
	const std::vector<std::string_view>& conditions;
	int caps;
};

static const std::unordered_map<IndexType, IndexInfo, std::hash<int>, std::equal_to<int>>& availableIndexes() {
	// clang-format off
	static const std::unordered_map<IndexType, IndexInfo, std::hash<int>, std::equal_to<int>> data {
		{IndexIntHash,			{"int"sv, "hash"sv,				condsUsual(),	CapSortable}},
		{IndexInt64Hash,		{"int64"sv, "hash"sv,			condsUsual(),	CapSortable}},
		{IndexStrHash,			{"string"sv, "hash"sv,			condsUsual(),	CapSortable}},
		{IndexCompositeHash,	{"composite"sv, "hash"sv,		condsUsual(),	CapSortable | CapComposite}},
		{IndexIntBTree,			{"int"sv, "tree"sv,				condsUsual(),	CapSortable}},
		{IndexInt64BTree,		{"int64"sv, "tree"sv,			condsUsual(),	CapSortable}},
		{IndexDoubleBTree,		{"double"sv, "tree"sv,			condsUsual(),	CapSortable}},
		{IndexCompositeBTree,	{"composite"sv, "tree"sv,		condsUsual(),	CapComposite | CapSortable}},
		{IndexStrBTree,			{"string"sv, "tree"sv,			condsUsual(),	CapSortable}},
		{IndexIntStore,			{"int"sv, "-"sv,				condsUsual(),	CapSortable}},
		{IndexBool,				{"bool"sv, "-"sv,				condsBool(),	0}},
		{IndexInt64Store,		{"int64"sv, "-"sv,				condsUsual(),	CapSortable}},
		{IndexStrStore,			{"string"sv, "-"sv,				condsUsual(),	CapSortable}},
		{IndexDoubleStore,		{"double"sv, "-"sv,				condsUsual(),	CapSortable}},
		{IndexTtl,				{"int64"sv, "ttl"sv,			condsUsual(),	CapSortable}},
		{IndexCompositeFastFT,	{"composite"sv, "text"sv,		condsText(),	CapComposite | CapFullText}},
		{IndexCompositeFuzzyFT,	{"composite"sv, "fuzzytext"sv,	condsText(),	CapComposite | CapFullText}},
		{IndexFastFT,			{"string"sv, "text"sv,			condsText(),	CapFullText}},
		{IndexFuzzyFT,			{"string"sv, "fuzzytext"sv,		condsText(),	CapFullText}},
		{IndexRTree,			{"point"sv, "rtree"sv,			condsGeom(),	0}},
		{IndexUuidHash,			{"uuid"sv, "hash"sv,			condsUsual(),	CapSortable}},
		{IndexUuidStore,		{"uuid"sv, "-"sv,				condsUsual(),	CapSortable}},
	};
	// clang-format on
	return data;
}

constexpr static auto kAvailableCollates = frozen::make_unordered_map<CollateMode, std::string_view>({
	{CollateASCII, "ascii"sv},
	{CollateUTF8, "utf8"sv},
	{CollateNumeric, "numeric"sv},
	{CollateCustom, "custom"sv},
	{CollateNone, "none"sv},
});

constexpr auto kRTreeLinear = "linear"sv;
constexpr auto kRTreeQuadratic = "quadratic"sv;
constexpr auto kRTreeGreene = "greene"sv;
constexpr auto kRTreeRStar = "rstar"sv;

}  // namespace

namespace reindexer {

IndexDef::IndexDef(std::string name, JsonPaths jsonPaths, std::string indexType, std::string fieldType, IndexOpts opts,
				   int64_t expireAfter) noexcept
	: name_(std::move(name)),
	  jsonPaths_(std::move(jsonPaths)),
	  indexType_(std::move(indexType)),
	  fieldType_(std::move(fieldType)),
	  opts_(std::move(opts)),
	  expireAfter_{expireAfter} {}

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

bool IndexDef::IsEqual(const IndexDef& other, IndexComparison cmpType) const {
	return name_ == other.name_ && jsonPaths_ == other.jsonPaths_ && Type() == other.Type() && fieldType_ == other.fieldType_ &&
		   opts_.IsEqual(other.opts_, cmpType) && expireAfter_ == other.expireAfter_;
}

IndexType IndexDef::Type() const {
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
	for (const auto& it : availableIndexes()) {
		if (fieldType_ == it.second.fieldType && iType == it.second.indexType) {
			return it.first;
		}
	}

	throw Error(errParams, "Unsupported combination of field '%s' type '%s' and index type '%s'", name_, fieldType_, indexType_);
}

void IndexDef::FromType(IndexType type) {
	const auto& it = availableIndexes().at(type);
	fieldType_ = it.fieldType;
	indexType_ = it.indexType;
}

const std::vector<std::string_view>& IndexDef::Conditions() const noexcept {
	const auto it{availableIndexes().find(Type())};
	assertrx(it != availableIndexes().cend());
	return it->second.conditions;
}

bool isSortable(IndexType type) { return availableIndexes().at(type).caps & CapSortable; }

bool isStore(IndexType type) noexcept {
	return type == IndexIntStore || type == IndexInt64Store || type == IndexStrStore || type == IndexDoubleStore || type == IndexBool ||
		   type == IndexUuidStore;
}

Error IndexDef::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		IndexDef::FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "IndexDef: %s", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

void IndexDef::FromJSON(const gason::JsonNode& root) {
	name_ = root["name"].As<std::string>();
	jsonPaths_.clear();
	for (auto& subElem : root["json_paths"]) {
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
		auto collateIt = find_if(begin(kAvailableCollates), end(kAvailableCollates),
								 [&collateStr](const std::pair<CollateMode, std::string_view>& p) { return collateStr == p.second; });
		if (collateIt == end(kAvailableCollates)) {
			throw Error(errParams, "Unknown collate mode %s", collateStr);
		}
		CollateMode collateValue = collateIt->first;
		opts_.SetCollateMode(collateValue);
		if (collateValue == CollateCustom) {
			opts_.collateOpts_ = CollateOpts(root["sort_order_letters"].As<std::string>());
		}
	}
}

void IndexDef::GetJSON(WrSerializer& ser, int formatFlags) const {
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
	builder.Put("collate_mode", kAvailableCollates.at(opts_.GetCollateMode()))
		.Put("sort_order_letters", opts_.collateOpts_.sortOrderTable.GetSortOrderCharacters())
		.Put("expire_after", expireAfter_)
		.Raw("config", opts_.HasConfig() ? opts_.config : "{}");

	if (formatFlags & kIndexJSONWithDescribe) {
		// extra data for support describe.
		// TODO: deprecate and remove it
		builder.Put("is_sortable", isSortable(Type()));
		builder.Put("is_fulltext", IsFullText(Type()));
		auto arr = builder.Array("conditions");
		for (auto& cond : Conditions()) {
			arr.Put(nullptr, cond);
		}
	}

	auto arrNode = builder.Array("json_paths");
	for (auto& jsonPath : jsonPaths_) {
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
