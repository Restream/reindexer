#pragma once

#include <optional>
#include "core/enums.h"
#include "core/type_consts.h"
#include "core/type_consts_helpers.h"
#include "estl/h_vector.h"
#include "sortingprioritiestable.h"
#include "tools/enum_compare.h"

namespace reindexer::builders {
class JsonBuilder;
}  // namespace reindexer::builders

struct [[nodiscard]] CollateOpts {
	explicit CollateOpts(CollateMode mode = CollateNone) noexcept : mode(mode) {}
	explicit CollateOpts(const std::string& sortOrderUTF8);

	CollateMode mode = CollateNone;
	reindexer::SortingPrioritiesTable sortOrderTable;
	template <typename T>
	void Dump(T& os) const;
};

enum [[nodiscard]] IndexOpt : uint8_t {
	kIndexOptPK = 1 << 7,
	kIndexOptArray = 1 << 6,
	kIndexOptDense = 1 << 5,
	kIndexOptSparse = 1 << 3,
	kIndexOptNoColumn = 1 << 2,
};

enum class [[nodiscard]] MultithreadingMode { SingleThread, MultithreadTransactions };

class [[nodiscard]] FloatVectorIndexOpts {
	using FloatVectorDimensionInt = reindexer::FloatVectorDimension::value_type;

public:
	struct [[nodiscard]] PoolOpts {
		size_t connections{10};
		size_t connect_timeout_ms{300};
		size_t read_timeout_ms{5'000};
		size_t write_timeout_ms{5'000};

		bool operator==(const PoolOpts& o) const noexcept = default;
	};
	struct [[nodiscard]] EmbedderOpts {
		std::string endpointUrl;
		std::string name;
		std::string cacheTag;
		reindexer::h_vector<std::string, 1> fields;
		enum class [[nodiscard]] Strategy : uint8_t { Always, EmptyOnly, Strict } strategy{Strategy::Always};
		PoolOpts pool{};

		bool operator==(const EmbedderOpts& o) const noexcept = default;
	};
	struct [[nodiscard]] EmbeddingOpts {
		std::optional<EmbedderOpts> upsertEmbedder;
		std::optional<EmbedderOpts> queryEmbedder;

		bool operator==(const EmbeddingOpts& o) const noexcept = default;
	};

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
	std::optional<float> Radius() const noexcept { return radius_; }
	MultithreadingMode Multithreading() const noexcept { return multithreadingMode_; }
	reindexer::VectorMetric Metric() const noexcept { return metric_; }
	std::optional<EmbeddingOpts> Embedding() const noexcept { return embedding_; }
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
	FloatVectorIndexOpts& SetEmbedding(EmbeddingOpts embedding) & noexcept {
		embedding_ = embedding;
		return *this;
	}
	FloatVectorIndexOpts&& SetRadius(float radius) && noexcept { return std::move(SetRadius(radius)); }
	FloatVectorIndexOpts& SetRadius(float radius) & noexcept {
		radius_ = radius;
		return *this;
	}
	FloatVectorIndexOpts&& SetEmbedding(EmbeddingOpts embedding) && noexcept { return std::move(SetEmbedding(embedding)); }
	void Validate(IndexType);
	static FloatVectorIndexOpts ParseJson(IndexType, std::string_view json);
	std::string GetJson() const;
	void GetJson(reindexer::builders::JsonBuilder&) const;

	enum class [[nodiscard]] Diff : uint8_t {
		Base = 1,
		Embedding = 1 << 1,
		Radius = 1 << 2,
		Full = (Radius << 1) - 1,
	};

	using DiffResult = compare_enum::Diff<FloatVectorIndexOpts::Diff>;
	DiffResult Compare(const FloatVectorIndexOpts& o) const noexcept;

private:
	FloatVectorDimensionInt dimension_{0};
	size_t startSize_{0};
	size_t M_{0};
	size_t efConstruction_{0};
	size_t nCentroids_{0};
	MultithreadingMode multithreadingMode_{MultithreadingMode::SingleThread};
	reindexer::VectorMetric metric_{reindexer::VectorMetric::L2};
	std::optional<EmbeddingOpts> embedding_;
	std::optional<float> radius_;
};

/// Cpp version of IndexOpts: includes
/// sort order table which is not possible
/// to link in C-GO version because of templates
/// in memory.h and unordered_map.h
struct [[nodiscard]] IndexOpts {
	enum [[nodiscard]] RTreeIndexType : uint8_t { Linear = 0, Quadratic = 1, Greene = 2, RStar = 3 };
	explicit IndexOpts(uint8_t flags = 0, CollateMode mode = CollateNone, RTreeIndexType = RStar);
	explicit IndexOpts(const std::string& sortOrderUTF8, uint8_t flags = 0, RTreeIndexType = RStar);

	reindexer::IsPk IsPK() const noexcept { return reindexer::IsPk(options & kIndexOptPK); }
	reindexer::IsArray IsArray() const noexcept { return reindexer::IsArray(options & kIndexOptArray); }
	reindexer::IsDense IsDense() const noexcept { return reindexer::IsDense(options & kIndexOptDense); }
	reindexer::IsSparse IsSparse() const noexcept { return reindexer::IsSparse(options & kIndexOptSparse); }
	reindexer::IsNoIndexColumn IsNoIndexColumn() const noexcept { return reindexer::IsNoIndexColumn(options & kIndexOptNoColumn); }
	RTreeIndexType RTreeType() const noexcept { return rtreeType_; }
	bool HasConfig() const noexcept { return !config_.empty(); }

	IndexOpts& PK(bool value = true) &;
	IndexOpts&& PK(bool value = true) && { return std::move(PK(value)); }
	IndexOpts& Array(bool value = true) &;
	IndexOpts&& Array(bool value = true) && { return std::move(Array(value)); }
	IndexOpts& Dense(bool value = true) & noexcept;
	IndexOpts&& Dense(bool value = true) && noexcept { return std::move(Dense(value)); }
	IndexOpts& NoIndexColumn(bool value = true) & noexcept;
	IndexOpts&& NoIndexColumn(bool value = true) && noexcept { return std::move(NoIndexColumn(value)); }
	IndexOpts& Sparse(bool value = true) &;
	IndexOpts&& Sparse(bool value = true) && { return std::move(Sparse(value)); }
	IndexOpts& RTreeType(RTreeIndexType) & noexcept;
	IndexOpts&& RTreeType(RTreeIndexType type) && noexcept { return std::move(RTreeType(type)); }
	IndexOpts& SetCollateMode(CollateMode mode) & noexcept;
	IndexOpts&& SetCollateMode(CollateMode mode) && noexcept { return std::move(SetCollateMode(mode)); }
	IndexOpts& SetCollateSortOrder(reindexer::SortingPrioritiesTable&& sortOrder) & noexcept;
	IndexOpts&& SetCollateSortOrder(reindexer::SortingPrioritiesTable&& sortOrder) && noexcept {
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
	IndexOpts&& SetConfig(IndexType indexType, Str&& config) && {
		return std::move(SetConfig(indexType, std::forward<Str>(config)));
	}
	const std::string& Config() const& noexcept { return config_; }
	auto Config() const&& = delete;
	CollateMode GetCollateMode() const noexcept { return collateOpts_.mode; }
	reindexer::SortingPrioritiesTable GetCollateSortOrder() const noexcept { return collateOpts_.sortOrderTable; }

	bool IsFloatVector() const noexcept { return floatVector_.has_value(); }

	size_t HeapSize() const noexcept { return config_.size(); }

	IndexOpts& SetFloatVector(IndexType) &;
	IndexOpts& SetFloatVector(IndexType, FloatVectorIndexOpts) &;
	IndexOpts&& SetFloatVector(IndexType idxType, FloatVectorIndexOpts fv) &&;
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

	using OptsDiff = IndexOpt;

	enum class [[nodiscard]] ParamsDiff : uint8_t {
		CollateOpts = 1,
		RTreeIndexType = 1 << 1,
		Config = 1 << 2,
	};

	using DiffResult = compare_enum::Diff<IndexOpts::ParamsDiff, IndexOpts::OptsDiff, FloatVectorIndexOpts::Diff>;
	DiffResult Compare(const IndexOpts& o) const noexcept;

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
