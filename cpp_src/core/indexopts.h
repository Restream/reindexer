#pragma once

#include <optional>
#include "core/enums.h"
#include "core/type_consts.h"
#include "core/type_consts_helpers.h"
#include "sortingprioritiestable.h"

namespace reindexer {
class JsonBuilder;
}  // namespace reindexer

struct CollateOpts {
	explicit CollateOpts(CollateMode mode = CollateNone) noexcept : mode(mode) {}
	explicit CollateOpts(const std::string& sortOrderUTF8);

	CollateMode mode = CollateNone;
	reindexer::SortingPrioritiesTable sortOrderTable;
	template <typename T>
	void Dump(T& os) const;
};

enum class [[nodiscard]] IndexComparison { Full, SkipConfig, BasicCompatibilityOnly };
enum class [[nodiscard]] MultithreadingMode { SingleThread, MultithreadTransactions };

class [[nodiscard]] FloatVectorIndexOpts {
	using FloatVectorDimensionInt = reindexer::FloatVectorDimension::value_type;

public:
	FloatVectorIndexOpts() = default;
	FloatVectorIndexOpts(IndexType type) {
		// Set default for the index type
		if (type == IndexType::IndexHnsw) {
			M_ = 16;
			efConstruction_ = 200;
		}
	}

	FloatVectorDimensionInt Dimension() const noexcept { return dimension_; }
	size_t StartSize() const noexcept { return startSize_; }
	size_t M() const noexcept { return M_; }
	size_t EfConstruction() const noexcept { return efConstruction_; }
	size_t NCentroids() const noexcept { return nCentroids_; }
	MultithreadingMode Multithreading() const noexcept { return multithreadingMode_; }
	reindexer::VectorMetric Metric() const noexcept { return metric_; }
	FloatVectorIndexOpts& SetDimension(FloatVectorDimensionInt dim) & noexcept {
		dimension_ = dim;
		return *this;
	}
	FloatVectorIndexOpts&& SetDimension(FloatVectorDimensionInt dim) && noexcept { return std::move(SetDimension(dim)); }
	FloatVectorIndexOpts& SetStartSize(size_t startSize) & noexcept {
		startSize_ = startSize;
		return *this;
	}
	FloatVectorIndexOpts&& SetStartSize(size_t startSize) && noexcept { return std::move(SetStartSize(startSize)); }
	FloatVectorIndexOpts& SetM(size_t m) & noexcept {
		M_ = m;
		return *this;
	}
	FloatVectorIndexOpts&& SetM(size_t m) && noexcept { return std::move(SetM(m)); }
	FloatVectorIndexOpts& SetEfConstruction(size_t efConstr) & noexcept {
		efConstruction_ = efConstr;
		return *this;
	}
	FloatVectorIndexOpts&& SetEfConstruction(size_t efConstr) && noexcept { return std::move(SetEfConstruction(efConstr)); }
	FloatVectorIndexOpts& SetNCentroids(size_t nCentroids) & noexcept {
		nCentroids_ = nCentroids;
		return *this;
	}
	FloatVectorIndexOpts&& SetNCentroids(size_t nCentroids) && noexcept { return std::move(SetNCentroids(nCentroids)); }
	FloatVectorIndexOpts& SetMultithreading(MultithreadingMode mode) & noexcept {
		multithreadingMode_ = mode;
		return *this;
	}
	FloatVectorIndexOpts&& SetMultithreading(MultithreadingMode mode) && noexcept { return std::move(SetMultithreading(mode)); }
	FloatVectorIndexOpts& SetMetric(reindexer::VectorMetric metric) & noexcept {
		metric_ = metric;
		return *this;
	}
	FloatVectorIndexOpts&& SetMetric(reindexer::VectorMetric metric) && noexcept { return std::move(SetMetric(metric)); }
	bool operator==(const FloatVectorIndexOpts& o) const noexcept {
		return dimension_ == o.dimension_ && startSize_ == o.startSize_ && M_ == o.M_ && efConstruction_ == o.efConstruction_ &&
			   nCentroids_ == o.nCentroids_ && multithreadingMode_ == o.multithreadingMode_ && metric_ == o.metric_;
	}
	bool operator!=(const FloatVectorIndexOpts& o) const noexcept { return !(*this == o); }
	void Validate(IndexType);
	static FloatVectorIndexOpts ParseJson(IndexType, std::string_view json);
	std::string GetJson() const;
	void GetJson(reindexer::JsonBuilder&) const;

private:
	FloatVectorDimensionInt dimension_{0};
	size_t startSize_{0};
	size_t M_{0};
	size_t efConstruction_{0};
	size_t nCentroids_{0};
	MultithreadingMode multithreadingMode_{MultithreadingMode::SingleThread};
	reindexer::VectorMetric metric_{reindexer::VectorMetric::L2};
};

/// Cpp version of IndexOpts: includes
/// sort order table which is not possible
/// to link in C-GO version because of templates
/// in memory.h and unordered_map.h
struct IndexOpts {
	enum RTreeIndexType : uint8_t { Linear = 0, Quadratic = 1, Greene = 2, RStar = 3 };
	explicit IndexOpts(uint8_t flags = 0, CollateMode mode = CollateNone, RTreeIndexType = RStar);
	explicit IndexOpts(const std::string& sortOrderUTF8, uint8_t flags = 0, RTreeIndexType = RStar);

	bool IsPK() const noexcept { return options & kIndexOptPK; }
	bool IsArray() const noexcept { return options & kIndexOptArray; }
	bool IsDense() const noexcept { return options & kIndexOptDense; }
	bool IsSparse() const noexcept { return options & kIndexOptSparse; }
	RTreeIndexType RTreeType() const noexcept { return rtreeType_; }
	bool HasConfig() const noexcept { return !config_.empty(); }

	IndexOpts& PK(bool value = true) &;
	[[nodiscard]] IndexOpts&& PK(bool value = true) && { return std::move(PK(value)); }
	IndexOpts& Array(bool value = true) &;
	[[nodiscard]] IndexOpts&& Array(bool value = true) && { return std::move(Array(value)); }
	IndexOpts& Dense(bool value = true) & noexcept;
	[[nodiscard]] IndexOpts&& Dense(bool value = true) && noexcept { return std::move(Dense(value)); }
	IndexOpts& Sparse(bool value = true) &;
	[[nodiscard]] IndexOpts&& Sparse(bool value = true) && { return std::move(Sparse(value)); }
	IndexOpts& RTreeType(RTreeIndexType) & noexcept;
	[[nodiscard]] IndexOpts&& RTreeType(RTreeIndexType type) && noexcept { return std::move(RTreeType(type)); }
	IndexOpts& SetCollateMode(CollateMode mode) & noexcept;
	[[nodiscard]] IndexOpts&& SetCollateMode(CollateMode mode) && noexcept { return std::move(SetCollateMode(mode)); }
	IndexOpts& SetCollateSortOrder(reindexer::SortingPrioritiesTable&& sortOrder) & noexcept;
	[[nodiscard]] IndexOpts&& SetCollateSortOrder(reindexer::SortingPrioritiesTable&& sortOrder) && noexcept {
		return std::move(SetCollateSortOrder(std::move(sortOrder)));
	}
	template <typename Str, std::enable_if_t<std::is_assignable_v<std::string, Str>>* = nullptr>
	IndexOpts& SetConfig(IndexType indexType, Str&& conf) & {
		if (reindexer::IsFloatVector(indexType)) {
			SetFloatVector(indexType, FloatVectorIndexOpts::ParseJson(indexType, conf));
		} else {
			config_ = std::forward<Str>(conf);
		}
		return *this;
	}
	template <typename Str, std::enable_if_t<std::is_assignable_v<std::string, Str>>* = nullptr>
	[[nodiscard]] IndexOpts&& SetConfig(IndexType indexType, Str&& config) && {
		return std::move(SetConfig(indexType, std::forward<Str>(config)));
	}
	const std::string& Config() const& noexcept { return config_; }
	auto Config() const&& = delete;
	CollateMode GetCollateMode() const noexcept { return collateOpts_.mode; }
	reindexer::SortingPrioritiesTable GetCollateSortOrder() const noexcept { return collateOpts_.sortOrderTable; }

	bool IsEqual(const IndexOpts& other, IndexComparison cmpType) const noexcept;
	bool IsFloatVector() const noexcept { return floatVector_.has_value(); }

	size_t HeapSize() const noexcept { return config_.size(); }

	IndexOpts& SetFloatVector(IndexType) &;
	IndexOpts& SetFloatVector(IndexType, FloatVectorIndexOpts) &;
	IndexOpts&& SetFloatVector(IndexType idxType, FloatVectorIndexOpts fv) && { return std::move(SetFloatVector(idxType, fv)); }
	IndexOpts&& SetFloatVector(IndexType idxType) && { return std::move(SetFloatVector(idxType)); }
	const FloatVectorIndexOpts& FloatVector() const& {
		assertrx_throw(floatVector_);
		return *floatVector_;
	}
	auto FloatVector() const&& = delete;

	template <typename T>
	void Dump(T& os) const;

	uint8_t options;
	CollateOpts collateOpts_;
	RTreeIndexType rtreeType_ = RStar;

private:
	std::string config_ = "{}";
	void validateForFloatVector() const;
	std::optional<FloatVectorIndexOpts> floatVector_;
};

template <typename T>
T& operator<<(T& os, IndexOpts::RTreeIndexType t) {
	switch (t) {
		case IndexOpts::Linear:
			return os << "Linear";
		case IndexOpts::Quadratic:
			return os << "Quadratic";
		case IndexOpts::Greene:
			return os << "Greene";
		case IndexOpts::RStar:
			return os << "RStar";
	}
	std::abort();
}
