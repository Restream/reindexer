#include "core/indexdef.h"
#include "cjson/jsonbuilder.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "type_consts_helpers.h"
#include "vendor/frozen/unordered_map.h"

namespace {

using namespace std::string_view_literals;

static constexpr auto kCondsGeneral = {"SET"sv, "ALLSET"sv, "EQ"sv, "LT"sv, "LE"sv, "GT"sv, "GE"sv, "RANGE"sv};
static constexpr auto kCondsGeneralArray = {"SET"sv, "ALLSET"sv, "EQ"sv, "ANY"sv, "EMPTY"sv};
static constexpr auto kCondsGeneralSparse = {"SET"sv, "ALLSET"sv, "EQ"sv, "ANY"sv, "EMPTY"sv, "LT"sv, "LE"sv, "GT"sv, "GE"sv, "RANGE"sv};
static constexpr auto kCondsText = {"EQ"sv, "SET"sv};
static constexpr auto kCondsBool = {"SET"sv, "ALLSET"sv, "EQ"sv, "ANY"sv, "EMPTY"sv};
static constexpr auto kCondsGeom = {"DWITHIN"sv};
static constexpr auto kCondsVector = {"KNN"sv, "ANY"sv, "EMPTY"sv};
static constexpr std::initializer_list<std::string_view> kCondsDummy = {};

class [[nodiscard]] IndexInfo {
public:
	constexpr IndexInfo(std::string_view fieldType, std::string_view indexType, std::initializer_list<std::string_view> conditions) noexcept
		: fieldType_{fieldType}, indexType_{indexType}, conditions_{conditions} {}

	std::string_view FieldType() const noexcept { return fieldType_; }
	std::string_view IndexType() const noexcept { return indexType_; }
	std::span<const std::string_view> Conditions(reindexer::IsArray isArray, reindexer::IsSparse isSparse) const noexcept {
		if (conditions_.data() == kCondsGeneral.begin()) {	// Compare pointers
			if (isArray) {
				return kCondsGeneralArray;
			}
			if (isSparse) {
				return kCondsGeneralSparse;
			}
		}
		return conditions_;
	}

private:
	const std::string_view fieldType_, indexType_;
	const std::span<const std::string_view> conditions_;
};

constexpr static auto kAvailableIndexes = frozen::make_unordered_map<IndexType, IndexInfo>({
	// clang-format off
	{IndexIntHash,          {"int"sv,          "hash"sv,      kCondsGeneral}},
	{IndexInt64Hash,        {"int64"sv,        "hash"sv,      kCondsGeneral}},
	{IndexStrHash,          {"string"sv,       "hash"sv,      kCondsGeneral}},
	{IndexCompositeHash,    {"composite"sv,    "hash"sv,      kCondsGeneral}},
	{IndexIntBTree,         {"int"sv,          "tree"sv,      kCondsGeneral}},
	{IndexInt64BTree,       {"int64"sv,        "tree"sv,      kCondsGeneral}},
	{IndexDoubleBTree,      {"double"sv,       "tree"sv,      kCondsGeneral}},
	{IndexCompositeBTree,   {"composite"sv,    "tree"sv,      kCondsGeneral}},
	{IndexStrBTree,         {"string"sv,       "tree"sv,      kCondsGeneral}},
	{IndexIntStore,         {"int"sv,          "-"sv,         kCondsGeneral}},
	{IndexBool,             {"bool"sv,         "-"sv,         kCondsBool}},
	{IndexInt64Store,       {"int64"sv,        "-"sv,         kCondsGeneral}},
	{IndexStrStore,         {"string"sv,       "-"sv,         kCondsGeneral}},
	{IndexDoubleStore,      {"double"sv,       "-"sv,         kCondsGeneral}},
	{IndexTtl,              {"int64"sv,        "ttl"sv,       kCondsGeneral}},
	{IndexCompositeFastFT,  {"composite"sv,    "text"sv,      kCondsText}},
	{IndexCompositeFuzzyFT, {"composite"sv,    "fuzzytext"sv, kCondsText}},
	{IndexFastFT,           {"string"sv,       "text"sv,      kCondsText}},
	{IndexFuzzyFT,          {"string"sv,       "fuzzytext"sv, kCondsText}},
	{IndexRTree,            {"point"sv,        "rtree"sv,     kCondsGeom}},
	{IndexUuidHash,         {"uuid"sv,         "hash"sv,      kCondsGeneral}},
	{IndexUuidStore,        {"uuid"sv,         "-"sv,         kCondsGeneral}},
	{IndexHnsw,             {"float_vector"sv, "hnsw"sv,      kCondsVector}},
	{IndexVectorBruteforce, {"float_vector"sv, "vec_bf"sv,    kCondsVector}},
	{IndexIvf,              {"float_vector"sv, "ivf"sv,       kCondsVector}},
	{IndexDummy,            {""sv,             "hash"sv,      kCondsDummy}},
	// clang-format on
});

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

void IndexDef::validate(::IndexType indexType, size_t jsonPathsCount, const IndexOpts& opts) {
	if (IsFloatVector(indexType)) {
		if (!opts.IsFloatVector()) {
			throw Error(errNotValid, "Float vector index without float vector options");
		}
	} else {
		if (opts.IsFloatVector()) {
			throw Error(errNotValid, "Not float vector index with float vector options");
		}
	}
	if (opts.IsFloatVector() && jsonPathsCount != 1) {
		throw Error(errNotValid, "For float vector index just single json path is allowed");
	}
}

bool IndexDef::isSortable() const noexcept {
	::IndexType type;
	try {
		type = IndexType();
	} catch (...) {
		return false;
	}
	switch (type) {
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
			return !bool(opts_.IsArray());
		case IndexFastFT:
		case IndexFuzzyFT:
		case IndexCompositeFastFT:
		case IndexCompositeFuzzyFT:
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
		case IndexDummy:
		default:
			return false;
	}
}

IndexDef::IndexDef(std::string name, reindexer::JsonPaths jsonPaths, std::string indexType, std::string fieldType, IndexOpts opts,
				   int64_t expireAfter)
	: name_(std::move(name)),
	  jsonPaths_(std::move(jsonPaths)),
	  indexType_(std::move(indexType)),
	  fieldType_(std::move(fieldType)),
	  opts_(std::move(opts)),
	  expireAfter_(expireAfter) {
	Validate();
}

IndexDef::IndexDef(std::string name, reindexer::JsonPaths jsonPaths, ::IndexType indexType, IndexOpts opts, int64_t expireAfter)
	: name_(std::move(name)), jsonPaths_(std::move(jsonPaths)), opts_(std::move(opts)), expireAfter_(expireAfter) {
	initFromIndexType(indexType);
	Validate();
}

IndexDef::IndexDef(std::string name, std::string indexType, std::string fieldType, IndexOpts opts)
	: name_(std::move(name)),
	  jsonPaths_({name_}),
	  indexType_(std::move(indexType)),
	  fieldType_(std::move(fieldType)),
	  opts_(std::move(opts)) {
	Validate();
}

IndexDef::IndexDef(std::string name, reindexer::JsonPaths jsonPaths, ::IndexType type, IndexOpts opts)
	: name_(std::move(name)), jsonPaths_(std::move(jsonPaths)), opts_(std::move(opts)) {
	this->initFromIndexType(type);
	Validate();
}

void IndexDef::SetJsonPaths(const reindexer::JsonPaths& jp) {
	validate(IndexType(), jp.size(), opts_);
	jsonPaths_ = jp;
}

void IndexDef::SetJsonPaths(reindexer::JsonPaths&& jp) {
	validate(IndexType(), jp.size(), opts_);
	jsonPaths_ = std::move(jp);
}

void IndexDef::SetOpts(const IndexOpts& opts) {
	validate(IndexType(), jsonPaths_.size(), opts);
	opts_ = opts;
}

void IndexDef::SetOpts(IndexOpts&& opts) {
	validate(IndexType(), jsonPaths_.size(), opts);
	opts_ = std::move(opts);
}

IndexDef::DiffResult IndexDef::Compare(const IndexDef& o) const noexcept {
	return DiffResult{}
		.Set<Diff::Name>(name_ == o.name_)
		.Set<Diff::JsonPaths>(jsonPaths_ == o.jsonPaths_)
		.Set<Diff::IndexType>(IndexType() == o.IndexType())
		.Set<Diff::FieldType>(fieldType_ == o.fieldType_)
		.Set<Diff::ExpireAfter>(expireAfter_ == o.expireAfter_)
		.Set(opts_.Compare(o.opts_));
}

::IndexType IndexDef::DetermineIndexType(std::string_view indexName, std::string_view indexType, std::string_view fieldType) {
	if (indexType == ""sv) {
		if (fieldType == "double"sv) {
			indexType = "tree"sv;
		} else if (fieldType == "bool"sv) {
			indexType = "-"sv;
		} else if (fieldType == "point"sv) {
			indexType = "rtree"sv;
		} else {
			indexType = "hash"sv;
		}
	}
	for (const auto& it : kAvailableIndexes) {
		if (fieldType == it.second.FieldType() && indexType == it.second.IndexType()) {
			return it.first;
		}
	}

	throw Error(errParams, "Unsupported combination of field '{}' type '{}' and index type '{}'", indexName, fieldType, indexType);
}

void IndexDef::Validate() const { validate(IndexType(), jsonPaths_.size(), opts_); }

void IndexDef::initFromIndexType(::IndexType type) {
	const auto& it = kAvailableIndexes.at(type);
	fieldType_ = it.FieldType();
	indexType_ = it.IndexType();
}

bool isStore(IndexType type) noexcept {
	return type == IndexIntStore || type == IndexInt64Store || type == IndexStrStore || type == IndexDoubleStore || type == IndexBool ||
		   type == IndexUuidStore;
}

Expected<IndexDef> IndexDef::FromJSON(std::string_view json) try {
	gason::JsonParser parser;
	return IndexDef::FromJSON(parser.Parse(json));
} catch (const gason::Exception& ex) {
	return Unexpected{Error{errParseJson, "IndexDef: {}", ex.what()}};
} catch (const Error& err) {
	return Unexpected{err};
}

Expected<IndexDef> IndexDef::FromJSON(std::span<char> json) try {
	gason::JsonParser parser;
	return IndexDef::FromJSON(parser.Parse(json));
} catch (const gason::Exception& ex) {
	return Unexpected{Error{errParseJson, "IndexDef: {}", ex.what()}};
} catch (const Error& err) {
	return Unexpected{err};
}

IndexDef IndexDef::FromJSON(const gason::JsonNode& root) {
	auto name = root["name"sv].As<std::string>();
	reindexer::JsonPaths jsonPaths;
	for (auto& subElem : root["json_paths"sv]) {
		jsonPaths.push_back(subElem.As<std::string>());
	}
	auto fieldType = root["field_type"sv].As<std::string>();
	auto indexType = root["index_type"sv].As<std::string>();
	auto expireAfter = root["expire_after"sv].As<int64_t>();
	const auto indexTypeEnum = DetermineIndexType(name, indexType, fieldType);
	IndexOpts opts;
	opts.PK(root["is_pk"sv].As<bool>());
	opts.Array(root["is_array"sv].As<bool>());
	opts.Dense(root["is_dense"sv].As<bool>());
	opts.Sparse(root["is_sparse"sv].As<bool>());
	auto isDisableColumnIndexNode = root["is_no_column"];
	if (isDisableColumnIndexNode.empty()) {
		opts.NoIndexColumn(*opts.IsDense() || *opts.IsSparse() || *opts.IsArray() || IsComposite(indexTypeEnum) ||
						   IsFullText(indexTypeEnum) || IsFloatVector(indexTypeEnum));
	} else {
		opts.NoIndexColumn(isDisableColumnIndexNode.As<bool>());
	}
	if (fieldType == "uuid"sv && opts.IsSparse()) {
		throw Error(errParams, "UUID index cannot be sparse");
	}
	opts.SetConfig(indexTypeEnum, stringifyJson(root["config"sv]));
	const std::string rtreeType = root["rtree_type"sv].As<std::string>();
	if (rtreeType.empty()) {
		if (indexType == "rtree"sv || fieldType == "point"sv) {
			throw Error(errParams, "RTree type does not set");
		}
	} else {
		if (rtreeType == kRTreeLinear) {
			opts.RTreeType(IndexOpts::Linear);
		} else if (rtreeType == kRTreeQuadratic) {
			opts.RTreeType(IndexOpts::Quadratic);
		} else if (rtreeType == kRTreeGreene) {
			opts.RTreeType(IndexOpts::Greene);
		} else if (rtreeType == kRTreeRStar) {
			opts.RTreeType(IndexOpts::RStar);
		} else {
			throw Error(errParams, "Unknown RTree type {}", rtreeType);
		}
	}

	auto collateStr = root["collate_mode"sv].As<std::string_view>();
	if (!collateStr.empty()) {
		auto collateIt = find_if(begin(kAvailableCollates), end(kAvailableCollates),
								 [&collateStr](const std::pair<CollateMode, std::string_view>& p) { return collateStr == p.second; });
		if (collateIt == end(kAvailableCollates)) {
			throw Error(errParams, "Unknown collate mode {}", collateStr);
		}
		CollateMode collateValue = collateIt->first;
		opts.SetCollateMode(collateValue);
		if (collateValue == CollateCustom) {
			opts.collateOpts_ = CollateOpts(root["sort_order_letters"sv].As<std::string>());
		}
	}
	return {std::move(name), std::move(jsonPaths), std::move(indexType), std::move(fieldType), std::move(opts), expireAfter};
}

void IndexDef::GetJSON(WrSerializer& ser, ExtraIndexDescription withExtras) const {
	Validate();
	JsonBuilder builder(ser);

	builder.Put("name"sv, name_);
	builder.Put("field_type"sv, fieldType_);
	builder.Put("index_type"sv, indexType_);
	builder.Put("is_pk"sv, *opts_.IsPK());
	builder.Put("is_array"sv, *opts_.IsArray());
	builder.Put("is_dense"sv, *opts_.IsDense());
	builder.Put("is_no_column", *opts_.IsNoIndexColumn());
	builder.Put("is_sparse"sv, *opts_.IsSparse());

	if (indexType_ == "rtree"sv || fieldType_ == "point"sv) {
		switch (opts_.RTreeType()) {
			case IndexOpts::Linear:
				builder.Put("rtree_type"sv, kRTreeLinear);
				break;
			case IndexOpts::Quadratic:
				builder.Put("rtree_type"sv, kRTreeQuadratic);
				break;
			case IndexOpts::Greene:
				builder.Put("rtree_type"sv, kRTreeGreene);
				break;
			case IndexOpts::RStar:
				builder.Put("rtree_type"sv, kRTreeRStar);
				break;
			default:
				assertrx(0);
				abort();
		}
	}

	builder.Put("collate_mode"sv, kAvailableCollates.at(opts_.GetCollateMode()));
	builder.Put("sort_order_letters"sv, opts_.collateOpts_.sortOrderTable.GetSortOrderCharacters());
	builder.Put("expire_after"sv, expireAfter_);

	if (opts_.IsFloatVector()) {
		auto config = builder.Object("config"sv);
		opts_.FloatVector().GetJson(config);
	} else {
		builder.Raw("config"sv, opts_.HasConfig() ? opts_.Config() : "{}"sv);
	}

	{
		auto arrNode = builder.Array("json_paths"sv);
		for (auto& jsonPath : jsonPaths_) {
			arrNode.Put(TagName::Empty(), jsonPath);
		}
	}

	if (withExtras) {
		builder.Put("is_sortable", isSortable());
		auto arr = builder.Array("conditions");
		if (const auto descIt = kAvailableIndexes.find(IndexType()); descIt != kAvailableIndexes.end()) {
			auto conds = descIt->second.Conditions(opts_.IsArray(), opts_.IsSparse());
			for (auto& cond : conds) {
				arr.Put(TagName::Empty(), cond);
			}
		}
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
