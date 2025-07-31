#include "indexopts.h"
#include <ostream>
#if defined(__GNUC__) && ((__GNUC__ == 12) || (__GNUC__ == 13)) && defined(REINDEX_WITH_ASAN)
// regex header is broken in GCC 12.0-13.3 with ASAN
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <regex>
#pragma GCC diagnostic pop
#else  // REINDEX_WITH_ASAN
#include <regex>
#endif	// REINDEX_WITH_ASAN
#include "cjson/jsonbuilder.h"
#include "core/enums.h"
#include "tools/errors.h"
#include "type_consts_helpers.h"
#include "vendor/gason/gason.h"

namespace {
constexpr size_t kFVDimensionMin = 1;
constexpr size_t kFVStartSizeMin = 1'000;
constexpr size_t kFVStartSizeDefault = kFVStartSizeMin;

constexpr size_t kHnswMMin = 2;
constexpr size_t kHnswMMax = 128;

constexpr size_t kHnswEfConstrMin = 4;
constexpr size_t kHnswEfConstrMax = 1'024;

constexpr size_t kIvfNCentroidsMin = 1;
constexpr size_t kIvfNCentroidsMax = 2 << 16;

constexpr std::string_view kEmbedding{"embedding"};
constexpr std::string_view kUpsertEmbedder{"upsert_embedder"};
constexpr std::string_view kQueryEmbedder{"query_embedder"};
constexpr std::string_view kEmbedderName{"name"};
constexpr std::string_view kEmbedderURL{"URL"};
constexpr std::string_view kEmbedderCacheTag{"cache_tag"};
constexpr std::string_view kEmbedderFields{"fields"};
constexpr std::string_view kEmbedderStrategy{"embedding_strategy"};
constexpr std::string_view kEmbedderStrategyAlways{"always"};
constexpr std::string_view kEmbedderStrategyEmpty{"empty_only"};
constexpr std::string_view kEmbedderStrategyStrict{"strict"};
constexpr std::string_view kConnectorPool{"pool"};
constexpr std::string_view kConnectorPoolConnections{"connections"};
constexpr std::string_view kConnectorPoolConnectTO{"connect_timeout_ms"};
constexpr std::string_view kConnectorPoolReadTO{"read_timeout_ms"};
constexpr std::string_view kConnectorPoolWriteTO{"write_timeout_ms"};

FloatVectorIndexOpts::EmbedderOpts::Strategy parseStrategy(std::string_view strategy, std::string_view name) {
	if (strategy == kEmbedderStrategyAlways) {
		return FloatVectorIndexOpts::EmbedderOpts::Strategy::Always;
	}
	if (strategy == kEmbedderStrategyEmpty) {
		return FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly;
	}
	if (strategy == kEmbedderStrategyStrict) {
		return FloatVectorIndexOpts::EmbedderOpts::Strategy::Strict;
	}
	throw reindexer::Error{errParams,
						   "Configuration '{}:{}' unexpected field value '{}'. Set '{}', but expected '{}', '{}' or '{}'",
						   kEmbedding,
						   name,
						   kEmbedderStrategy,
						   strategy,
						   kEmbedderStrategyAlways,
						   kEmbedderStrategyEmpty,
						   kEmbedderStrategyStrict};
}

std::string_view strategyToStr(FloatVectorIndexOpts::EmbedderOpts::Strategy strategy) {
	switch (strategy) {
		case FloatVectorIndexOpts::EmbedderOpts::Strategy::Always:
			return kEmbedderStrategyAlways;
		case FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly:
			return kEmbedderStrategyEmpty;
		case FloatVectorIndexOpts::EmbedderOpts::Strategy::Strict:
			return kEmbedderStrategyStrict;
		default:
			throw reindexer::Error{errParams, "Configuration '{}' unexpected field value '{}'. Value '{}'", kEmbedding, kEmbedderStrategy,
								   int(strategy)};
	}
}

FloatVectorIndexOpts::PoolOpts parsePoolConfig(const gason::JsonNode& node) {
	FloatVectorIndexOpts::PoolOpts opts;
	if (!node[kConnectorPoolConnections].empty()) {
		opts.connections = node[kConnectorPoolConnections].As<size_t>();
	}
	if (!node[kConnectorPoolConnectTO].empty()) {
		opts.connect_timeout_ms = node[kConnectorPoolConnectTO].As<size_t>();
	}
	if (!node[kConnectorPoolReadTO].empty()) {
		opts.read_timeout_ms = node[kConnectorPoolReadTO].As<size_t>();
	}
	if (!node[kConnectorPoolWriteTO].empty()) {
		opts.write_timeout_ms = node[kConnectorPoolWriteTO].As<size_t>();
	}
	return opts;
}

FloatVectorIndexOpts::EmbedderOpts parseEmbedderConfig(const gason::JsonNode& node, std::string_view name) {
	FloatVectorIndexOpts::EmbedderOpts opts;
	opts.endpointUrl = node[kEmbedderURL].As<std::string>();
	if (!node[kEmbedderName].empty()) {
		opts.name = reindexer::toLower(node[kEmbedderName].As<std::string>());
	}
	if (!node[kEmbedderCacheTag].empty()) {
		opts.cacheTag = reindexer::toLower(node[kEmbedderCacheTag].As<std::string>());
	}
	if (name == kUpsertEmbedder) {
		std::string field;
		for (auto& fld : node[kEmbedderFields]) {
			field = fld.As<std::string>();
			if (field.empty()) {
				throw reindexer::Error{errParams, "Configuration '{}:{}' does not support empty field names", kEmbedding, name};
			}
			if (std::find(opts.fields.cbegin(), opts.fields.cend(), field) != opts.fields.cend()) {
				throw reindexer::Error{errParams, "Configuration '{}:{}' does not support duplicate field names. Duplicate '{}'",
									   kEmbedding, name, field};
			}
			opts.fields.emplace_back(std::move(field));
		}

		if (!node[kEmbedderStrategy].empty()) {
			auto strategy = node[kEmbedderStrategy].As<std::string_view>();
			opts.strategy = parseStrategy(strategy, name);
		}
	}
	if (!node[kConnectorPool].empty()) {
		const auto& poolConf = node[kConnectorPool];
		opts.pool = parsePoolConfig(poolConf);
	}

	return opts;
}

FloatVectorIndexOpts::EmbeddingOpts parseEmbeddingConfig(const gason::JsonNode& node) {
	FloatVectorIndexOpts::EmbeddingOpts opts;

	if (!node[kUpsertEmbedder].empty()) {
		const auto& upsertConf = node[kUpsertEmbedder];
		opts.upsertEmbedder = parseEmbedderConfig(upsertConf, kUpsertEmbedder);
	}

	if (!node[kQueryEmbedder].empty()) {
		const auto& queryConf = node[kQueryEmbedder];
		opts.queryEmbedder = parseEmbedderConfig(queryConf, kQueryEmbedder);
	}

	if (!opts.upsertEmbedder.has_value() && !opts.queryEmbedder.has_value()) {
		throw reindexer::Error{errParams, "Configuration '{}' must contain object '{}' or '{}'", kEmbedding, kUpsertEmbedder,
							   kQueryEmbedder};
	}

	return opts;
}

void validateEmbedderPollTMOpt(size_t tm, size_t limit, std::string_view embedderName, std::string_view optName) {
	if (tm < limit) {
		throw reindexer::Error{errParams,	   "Configuration '{}:{}:{}:{}' should not be less than {} ms, in config '{}'",
							   kEmbedding,	   embedderName,
							   kConnectorPool, optName,
							   limit,		   tm};
	}
}

void validateEmbedderOpts(const FloatVectorIndexOpts::EmbedderOpts& opts, std::string_view name) {
	if (opts.endpointUrl.empty() || ((name == kUpsertEmbedder) && opts.fields.empty())) {
		throw reindexer::Error{errParams,	   "Configuration '{}:{}' must contain field '{}' and '{}'", kEmbedding, name, kEmbedderURL,
							   kEmbedderFields};
	}

	const static std::regex re(R"(^(http[s]?)://[0-9a-z\.-]+(:[1-9][0-9]*)?(/[^\s]*)*$)");
	if (!std::regex_match(opts.endpointUrl, re)) {
		throw reindexer::Error{errParams,	 "Configuration '{}:{}' contain field '{}' with unexpected value: '{}'",
							   kEmbedding,	 name,
							   kEmbedderURL, opts.endpointUrl};
	}

	if (opts.pool.connections < 1) {
		throw reindexer::Error{errParams,	   "Configuration '{}:{}:{}:{}' should not be less than 1",
							   kEmbedding,	   name,
							   kConnectorPool, kConnectorPoolConnections};
	}
	if (opts.pool.connections > 1024) {
		throw reindexer::Error{errParams,	   "Configuration '{}:{}:{}:{}' should not be more than 1024",
							   kEmbedding,	   name,
							   kConnectorPool, kConnectorPoolConnections};
	}
	validateEmbedderPollTMOpt(opts.pool.connect_timeout_ms, 100, name, kConnectorPoolConnectTO);
	validateEmbedderPollTMOpt(opts.pool.read_timeout_ms, 500, name, kConnectorPoolReadTO);
	validateEmbedderPollTMOpt(opts.pool.write_timeout_ms, 500, name, kConnectorPoolWriteTO);
}

void getJsonEmbedderConfig(const FloatVectorIndexOpts::EmbedderOpts& opts, reindexer::builders::JsonBuilder& json) {
	json.Put(kEmbedderURL, opts.endpointUrl);
	if (!opts.cacheTag.empty()) {
		json.Put(kEmbedderCacheTag, opts.cacheTag);
	}

	json.Put(kEmbedderStrategy, strategyToStr(opts.strategy));

	if (!opts.name.empty()) {
		json.Put(kEmbedderName, opts.name);
	}

	if (!opts.fields.empty()) {
		auto fieldsNode = json.Array(kEmbedderFields);
		for (const auto& fld : opts.fields) {
			fieldsNode.Put(reindexer::TagName::Empty(), fld);
		}
	}

	{
		const auto& pool = opts.pool;
		auto objNodePool = json.Object(kConnectorPool);
		objNodePool.Put(kConnectorPoolConnections, pool.connections);
		objNodePool.Put(kConnectorPoolConnectTO, pool.connect_timeout_ms);
		objNodePool.Put(kConnectorPoolReadTO, pool.read_timeout_ms);
		objNodePool.Put(kConnectorPoolWriteTO, pool.write_timeout_ms);
	}
}

}  // namespace

CollateOpts::CollateOpts(const std::string& sortOrderUTF8) : mode(CollateCustom), sortOrderTable(sortOrderUTF8) {}

template <typename T>
void CollateOpts::Dump(T& os) const {
	using namespace reindexer;
	os << mode;
	if (mode == CollateCustom) {
		os << ": [" << sortOrderTable.GetSortOrderCharacters() << ']';
	}
}
template void CollateOpts::Dump<std::ostream>(std::ostream&) const;

void FloatVectorIndexOpts::Validate(IndexType idxType) {
	if (dimension_ < kFVDimensionMin) {
		throw reindexer::Error{errParams, "Float vector index dimension should not be less then {}", kFVDimensionMin};
	}
	switch (idxType) {
		case IndexHnsw:
			if (startSize_ == 0) {
				startSize_ = kFVStartSizeDefault;
			} else if (startSize_ < kFVStartSizeMin) {
				startSize_ = kFVStartSizeMin;
			}
			if (M_ < kHnswMMin || M_ > kHnswMMax) {
				throw reindexer::Error{errParams, "Hnsw index parameter M = {} is out of bounds [{}, {}]", M_, kHnswMMin, kHnswMMax};
			}
			if (efConstruction_ < kHnswEfConstrMin || efConstruction_ > kHnswEfConstrMax) {
				throw reindexer::Error{errParams, "Hnsw index parameter efConstruction = {} is out of bounds [{}, {}]", efConstruction_,
									   kHnswEfConstrMin, kHnswEfConstrMax};
			}
			if (nCentroids_ != 0) {
				throw reindexer::Error{errParams, "Centroids count is not applicable for hnsw index"};
			}
			break;
		case IndexVectorBruteforce:
			if (startSize_ == 0) {
				startSize_ = kFVStartSizeDefault;
			} else if (startSize_ < kFVStartSizeMin) {
				startSize_ = kFVStartSizeMin;
			}
			if (M_ != 0) {
				throw reindexer::Error{errParams, "M parameter is not applicable for float vector bruteforce index"};
			}
			if (efConstruction_ != 0) {
				throw reindexer::Error{errParams, "EfConstruction parameter is not applicable for float vector bruteforce index"};
			}
			if (nCentroids_ != 0) {
				throw reindexer::Error{errParams, "Centroids count is not applicable for float vector bruteforce index"};
			}
			break;
		case IndexIvf:
			if (nCentroids_ < kIvfNCentroidsMin || nCentroids_ > kIvfNCentroidsMax) {
				throw reindexer::Error{errParams, "Ivf index centroids count = {} is out of bounds [{}, {}]", nCentroids_,
									   kIvfNCentroidsMin, kIvfNCentroidsMax};
			}
			if (M_ != 0) {
				throw reindexer::Error{errParams, "M parameter is not applicable for ivf index"};
			}
			if (efConstruction_ != 0) {
				throw reindexer::Error{errParams, "EfConstruction parameter is not applicable for ivf index"};
			}
			if (startSize_ != 0) {
				throw reindexer::Error{errParams, "Start size parameter is not applicable for ivf index"};
			}
			break;
		case IndexFastFT:
		case IndexCompositeFastFT:
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexFuzzyFT:
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexCompositeFuzzyFT:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
		case IndexDummy:
			throw reindexer::Error{errParams, "Float vector index options used for not float vector index"};
	}

	if (embedding_.has_value()) {
		const auto& opts = embedding_.value();
		if (opts.upsertEmbedder.has_value()) {
			validateEmbedderOpts(opts.upsertEmbedder.value(), kUpsertEmbedder);
		}
		if (opts.queryEmbedder.has_value()) {
			validateEmbedderOpts(opts.queryEmbedder.value(), kQueryEmbedder);
		}
	}
}

FloatVectorIndexOpts FloatVectorIndexOpts::ParseJson(IndexType idxType, std::string_view json) {
	using namespace std::string_view_literals;

	FloatVectorIndexOpts result(idxType);
	gason::JsonParser parser;
	auto root = parser.Parse(json);

	result.SetDimension(
		FloatVectorDimensionInt(root["dimension"sv].As<FloatVectorDimensionInt>(reindexer::CheckUnsigned_True, 0, kFVDimensionMin)));
	const auto metricStr = root["metric"sv].As<std::string_view>();
	if (reindexer::iequals(metricStr, "l2"sv)) {
		result.SetMetric(reindexer::VectorMetric::L2);
	} else if (reindexer::iequals(metricStr, "inner_product"sv)) {
		result.SetMetric(reindexer::VectorMetric::InnerProduct);
	} else if (reindexer::iequals(metricStr, "cosine"sv)) {
		result.SetMetric(reindexer::VectorMetric::Cosine);
	} else {
		throw reindexer::Error{errParams, "Unknown vector metric '{}'", metricStr};
	}
	if (const auto startSize = root["start_size"sv]; !startSize.empty()) {
		result.SetStartSize(startSize.As<size_t>(reindexer::CheckUnsigned_True, 0, 0, std::numeric_limits<IdType>::max()));
	}
	if (const auto m = root["m"sv]; !m.empty()) {
		result.SetM(m.As<size_t>(reindexer::CheckUnsigned_True, 0, kHnswMMin, kHnswMMax));
	}
	if (const auto efConstruction = root["ef_construction"sv]; !efConstruction.empty()) {
		result.SetEfConstruction(efConstruction.As<size_t>(reindexer::CheckUnsigned_True, 0, kHnswEfConstrMin, kHnswEfConstrMax));
	}
	if (const auto nCentroids = root["centroids_count"sv]; !nCentroids.empty()) {
		result.SetNCentroids(nCentroids.As<size_t>(reindexer::CheckUnsigned_True, 0, kIvfNCentroidsMin, kIvfNCentroidsMax));
	}
	if (const auto multithreading = root["multithreading"sv]; !multithreading.empty()) {
		result.SetMultithreading(static_cast<MultithreadingMode>(multithreading.As<int>()));
	}
	if (const auto embedding = root[kEmbedding]; !embedding.empty()) {
		result.embedding_ = parseEmbeddingConfig(embedding);
	}
	if (const auto radius = root["radius"sv]; !radius.empty()) {
		result.SetRadius(radius.As<float>());
	}

	result.Validate(idxType);
	return result;
}

std::string FloatVectorIndexOpts::GetJson() const {
	reindexer::WrSerializer ser;
	{
		reindexer::builders::JsonBuilder json{ser};
		GetJson(json);
	}
	return std::string{ser.Slice()};
}

void FloatVectorIndexOpts::GetJson(reindexer::builders::JsonBuilder& json) const {
	using namespace std::string_view_literals;

	json.Put("dimension"sv, uint32_t(dimension_));
	switch (metric_) {
		case reindexer::VectorMetric::L2:
			json.Put("metric"sv, "l2"sv);
			break;
		case reindexer::VectorMetric::InnerProduct:
			json.Put("metric"sv, "inner_product"sv);
			break;
		case reindexer::VectorMetric::Cosine:
			json.Put("metric"sv, "cosine"sv);
			break;
	}
	if (startSize_ != 0) {
		json.Put("start_size"sv, startSize_);
	}
	if (efConstruction_ != 0) {
		json.Put("ef_construction"sv, efConstruction_);
	}
	if (M_ != 0) {
		json.Put("m"sv, M_);
	}
	if (nCentroids_ != 0) {
		json.Put("centroids_count"sv, nCentroids_);
	}
	if (multithreadingMode_ != MultithreadingMode::SingleThread) {
		json.Put("multithreading"sv, static_cast<int>(multithreadingMode_));
	}
	if (embedding_.has_value()) {
		const auto& embedding = embedding_.value();
		auto obj = json.Object(kEmbedding);
		if (embedding.upsertEmbedder.has_value()) {
			auto objNode = obj.Object(kUpsertEmbedder);
			getJsonEmbedderConfig(embedding.upsertEmbedder.value(), objNode);
		}
		if (embedding.queryEmbedder.has_value()) {
			auto objNode = obj.Object(kQueryEmbedder);
			getJsonEmbedderConfig(embedding.queryEmbedder.value(), objNode);
		}
	}
	if (radius_) {
		json.Put("radius"sv, *radius_);
	}
}

IndexOpts::IndexOpts(uint8_t flags, CollateMode mode, RTreeIndexType rtreeType)
	: options(flags), collateOpts_(mode), rtreeType_(rtreeType) {}

IndexOpts::IndexOpts(const std::string& sortOrderUTF8, uint8_t flags, RTreeIndexType rtreeType)
	: options(flags), collateOpts_(sortOrderUTF8), rtreeType_(rtreeType) {}

IndexOpts::DiffResult IndexOpts::Compare(const IndexOpts& o) const noexcept {
	auto res = DiffResult{}
				   .Set<OptsDiff::kIndexOptPK>(IsPK() == o.IsPK())
				   .Set<OptsDiff::kIndexOptArray>(IsArray() == o.IsArray())
				   .Set<OptsDiff::kIndexOptDense>(IsDense() == o.IsDense())
				   .Set<OptsDiff::kIndexOptSparse>(IsSparse() == o.IsSparse())
				   .Set<OptsDiff::kIndexOptNoColumn>(IsNoIndexColumn() == o.IsNoIndexColumn())
				   .Set<ParamsDiff::CollateOpts>(collateOpts_.mode == o.collateOpts_.mode &&
												 collateOpts_.sortOrderTable.GetSortOrderCharacters() ==
													 collateOpts_.sortOrderTable.GetSortOrderCharacters())
				   .Set<ParamsDiff::RTreeIndexType>(rtreeType_ == o.rtreeType_)
				   .Set<ParamsDiff::Config>(config_ == o.config_);

	if (floatVector_ && o.floatVector_) {
		res.Set(floatVector_->Compare(*o.floatVector_));
	} else if (floatVector_ || o.floatVector_) {
		res.Set(FloatVectorIndexOpts::Diff::Full);
	}
	return res;
}

FloatVectorIndexOpts::DiffResult FloatVectorIndexOpts::Compare(const FloatVectorIndexOpts& o) const noexcept {
	return DiffResult{}
		.Set<Diff::Base>(dimension_ == o.dimension_ && startSize_ == o.startSize_ && M_ == o.M_ && efConstruction_ == o.efConstruction_ &&
						 nCentroids_ == o.nCentroids_ && multithreadingMode_ == o.multithreadingMode_ && metric_ == o.metric_)
		.Set<Diff::Embedding>(embedding_ == o.embedding_)
		.Set<Diff::Radius>(radius_ == o.radius_);
}

void IndexOpts::validateForFloatVector() const {
	if (IsArray()) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be array");
	}
	if (IsSparse()) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be sparse");
	}
	if (IsDense()) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be dense");
	}
	if (IsPK()) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be PK");
	}
}

IndexOpts& IndexOpts::SetFloatVector(IndexType idxType) & {
	return SetFloatVector(idxType, FloatVectorIndexOpts::ParseJson(idxType, config_));
}

IndexOpts& IndexOpts::SetFloatVector(IndexType idxType, FloatVectorIndexOpts fv) & {
	validateForFloatVector();
	fv.Validate(idxType);
	floatVector_ = fv;
	reindexer::WrSerializer ser;
	{
		reindexer::builders::JsonBuilder config{ser};
		floatVector_->GetJson(config);
	}
	config_ = ser.Slice();
	return *this;
}

IndexOpts&& IndexOpts::SetFloatVector(IndexType idxType, FloatVectorIndexOpts fv) && {
	return std::move(SetFloatVector(idxType, std::move(fv)));
}

IndexOpts& IndexOpts::PK(bool value) & {
	if (value && floatVector_) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be PK");
	}
	options = value ? options | kIndexOptPK : options & ~(kIndexOptPK);
	return *this;
}

IndexOpts& IndexOpts::Array(bool value) & {
	if (value && floatVector_) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be array");
	}
	options = value ? options | kIndexOptArray : options & ~(kIndexOptArray);
	return *this;
}

IndexOpts& IndexOpts::Dense(bool value) & noexcept {
	options = value ? options | kIndexOptDense : options & ~(kIndexOptDense);
	return *this;
}
IndexOpts& IndexOpts::NoIndexColumn(bool value) & noexcept {
	options = value ? options | kIndexOptNoColumn : options & ~(kIndexOptNoColumn);
	return *this;
}

IndexOpts& IndexOpts::Sparse(bool value) & {
	if (value && floatVector_) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be sparse");
	}
	options = value ? options | kIndexOptSparse : options & ~(kIndexOptSparse);
	return *this;
}

IndexOpts& IndexOpts::RTreeType(RTreeIndexType value) & noexcept {
	rtreeType_ = value;
	return *this;
}

IndexOpts& IndexOpts::SetCollateMode(CollateMode mode) & noexcept {
	collateOpts_.mode = mode;
	return *this;
}

IndexOpts& IndexOpts::SetCollateSortOrder(reindexer::SortingPrioritiesTable&& sortOrder) & noexcept {
	collateOpts_.sortOrderTable = std::move(sortOrder);
	return *this;
}

template <typename T>
void IndexOpts::Dump(T& os) const {
	os << '{';
	bool needComma = false;
	if (IsPK()) {
		os << "PK";
		needComma = true;
	}
	if (IsArray()) {
		if (needComma) {
			os << ", ";
		}
		os << "Array";
		needComma = true;
	}
	if (IsDense()) {
		if (needComma) {
			os << ", ";
		}
		os << "Dense";
		needComma = true;
	}
	if (IsNoIndexColumn()) {
		if (needComma) {
			os << ", ";
		}
		os << "ColumnIndexDisabled";
		needComma = true;
	}
	if (IsSparse()) {
		if (needComma) {
			os << ", ";
		}
		os << "Sparse";
		needComma = true;
	}
	if (needComma) {
		os << ", ";
	}
	os << RTreeType();
	if (HasConfig()) {
		os << ", config: " << config_;
	}
	os << ", collate: ";
	collateOpts_.Dump(os);
	os << '}';
}

template void IndexOpts::Dump<std::ostream>(std::ostream&) const;
