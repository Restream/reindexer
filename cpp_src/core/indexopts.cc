#include "indexopts.h"
#include <ostream>
#include "cjson/jsonbuilder.h"
#include "core/enums.h"
#include "tools/errors.h"
#include "type_consts_helpers.h"
#include "vendor/gason/gason.h"

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

static constexpr size_t kFVDimensionMin = 1;
static constexpr size_t kFVStartSizeMin = 1'000;
static constexpr size_t kFVStartSizeDefault = kFVStartSizeMin;

static constexpr size_t kHnswMMin = 2;
static constexpr size_t kHnswMMax = 128;

static constexpr size_t kHnswEfConstrMin = 4;
static constexpr size_t kHnswEfConstrMax = 1'024;

static constexpr size_t kIvfNCentroidsMin = 1;
static constexpr size_t kIvfNCentroidsMax = 2 << 16;

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

	result.Validate(idxType);
	return result;
}

std::string FloatVectorIndexOpts::GetJson() const {
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json{ser};
		GetJson(json);
	}
	return std::string{ser.Slice()};
}

void FloatVectorIndexOpts::GetJson(reindexer::JsonBuilder& json) const {
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
}

IndexOpts::IndexOpts(uint8_t flags, CollateMode mode, RTreeIndexType rtreeType)
	: options(flags), collateOpts_(mode), rtreeType_(rtreeType) {}

IndexOpts::IndexOpts(const std::string& sortOrderUTF8, uint8_t flags, RTreeIndexType rtreeType)
	: options(flags), collateOpts_(sortOrderUTF8), rtreeType_(rtreeType) {}

bool IndexOpts::IsEqual(const IndexOpts& other, IndexComparison cmpType) const noexcept {
	auto thisCopy = *this;
	thisCopy.Dense(other.IsDense());

	// Compare without config and 'IsDense' option
	const bool baseEqual =
		thisCopy.options == other.options && collateOpts_.mode == other.collateOpts_.mode &&
		collateOpts_.sortOrderTable.GetSortOrderCharacters() == other.collateOpts_.sortOrderTable.GetSortOrderCharacters() &&
		rtreeType_ == other.rtreeType_ && floatVector_ == other.floatVector_;
	if (!baseEqual) {
		return false;
	}
	switch (cmpType) {
		case IndexComparison::BasicCompatibilityOnly:
			return true;
		case IndexComparison::SkipConfig:
			return IsDense() == other.IsDense();
		case IndexComparison::Full:
		default:
			return IsDense() == other.IsDense() && config_ == other.config_;
	}
}

void IndexOpts::validateForFloatVector() const {
	if (IsArray()) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be array");
	}
	if (IsSparse()) {
		throw reindexer::Error(errNotValid, "FloatVector index cannot be sparse");
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
		reindexer::JsonBuilder config{ser};
		floatVector_->GetJson(config);
	}
	config_ = ser.Slice();
	return *this;
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
