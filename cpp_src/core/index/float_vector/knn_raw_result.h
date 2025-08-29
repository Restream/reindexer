#pragma once

#if RX_WITH_BUILTIN_ANN_INDEXES

#include <queue>
#include <variant>
#include "estl/concepts.h"
#include "estl/h_vector.h"
#include "estl/overloaded.h"
#include "hnswlib/hnswlib.h"
#include "vendor_subdirs/faiss/MetricType.h"

namespace reindexer {

class [[nodiscard]] EmptyKnnRawResult final {
public:
	EmptyKnnRawResult() = default;
};

class [[nodiscard]] HnswKnnRawResult : public std::priority_queue<std::pair<float, hnswlib::labeltype>> {
	using Base = std::priority_queue<std::pair<float, hnswlib::labeltype>>;

public:
	HnswKnnRawResult(Base&& b) noexcept : Base{std::move(b)} {}
};

class [[nodiscard]] IvfKnnRawResult {
public:
	IvfKnnRawResult(size_t k) : dists_(k), ids_(k) {}
	const h_vector<float, 128>& Dists() const& noexcept { return dists_; }
	h_vector<float, 128>& Dists() & noexcept { return dists_; }
	h_vector<faiss::idx_t, 128>& Ids() & noexcept { return ids_; }
	const h_vector<faiss::idx_t, 128>& Ids() const& noexcept { return ids_; }

	auto Ids() const&& = delete;
	auto Dists() const&& = delete;

private:
	h_vector<float, 128> dists_;
	h_vector<faiss::idx_t, 128> ids_;
};

class [[nodiscard]] KnnRawResult : private std::variant<HnswKnnRawResult, IvfKnnRawResult, EmptyKnnRawResult> {
	using Base = std::variant<HnswKnnRawResult, IvfKnnRawResult, EmptyKnnRawResult>;

public:
	template <concepts::OneOf<Base, HnswKnnRawResult, IvfKnnRawResult, EmptyKnnRawResult> T>
	KnnRawResult(T&& base, VectorMetric metric) noexcept : Base{std::forward<T>(base)}, metric_{metric} {}

	size_t Size() const noexcept {
		return std::visit(overloaded{[](const HnswKnnRawResult& r) noexcept { return r.size(); },
									 [](const IvfKnnRawResult& r) noexcept -> size_t { return r.Ids().size(); },
									 [](const EmptyKnnRawResult&) noexcept -> size_t { return 0; }},
						  AsVariant());
	}

	const Base& AsVariant() const& noexcept { return *this; }
	Base& AsVariant() & noexcept { return *this; }
	auto AsVariant() const&& = delete;

	VectorMetric Metric() const noexcept { return metric_; }

private:
	VectorMetric metric_;
};

}  // namespace reindexer

#endif	// RX_WITH_BUILTIN_ANN_INDEXES
