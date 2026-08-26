#pragma once

#include <algorithm>
#include <limits>

namespace reindexer {

// Estimator of 'ef'/'batchSize' for streaming KNN search over an HNSW index.
//
// The more selective the non-KNN post-filters are, the more KNN candidates have to be streamed in order to
// collect 'offset + limit' survivors. Hence the estimated batch size grows when 'maxIterations' shrinks:
//
//   amplification = itemsCount / maxIterations   (>= 1; inverse of the post-filter selectivity)
//   value         = (offset + limit) * amplification
//   ef = batchSize = clamp(value, kMinEfBatch, kMaxEfBatch)
//
// 'maxIterations' must be estimated over all the query conditions except the KNN one itself.
// Example : maxIterations = 10'000, itemsCount = 100'000, offset = 50, limit = 20
//   => (50 + 20) * 100'000 / 10'000 = 700.
class [[nodiscard]] StreamingKnnEstimator {
public:
	static size_t EstimateEf(size_t maxIterations, size_t itemsCount, size_t offset, size_t limit) noexcept {
		return clamp(rawValue(maxIterations, itemsCount, offset, limit));
	}

	static size_t EstimateBatchSize(size_t accepted, size_t presented, size_t needed) {
		const size_t remaining = accepted >= needed ? 1 : needed - accepted;
		const double amplification = double(presented) / std::max(size_t(1), accepted);
		return clamp(amplification * remaining);
	}

private:
	friend class StreamingKnnIndexIterator;

	static constexpr size_t kMinEfBatch = 100;
	static constexpr size_t kMaxEfBatch = 800;

	static size_t clamp(size_t value) noexcept { return std::clamp(value, kMinEfBatch, kMaxEfBatch); }
	static size_t rawValue(size_t maxIterations, size_t itemsCount, size_t offset, size_t limit) noexcept {
		if (itemsCount == 0) {
			return 0;
		}
		// maxIterations is expected to be capped at itemsCount; guard against 0 to avoid division by zero.
		const size_t restricted = std::max<size_t>(1, std::min(maxIterations, itemsCount));
		const size_t needed = offset + limit;
		const size_t maxSize = std::numeric_limits<size_t>::max();
		if (needed > maxSize / itemsCount) {
			return maxSize;
		}
		return needed * itemsCount / restricted;
	}
};

}  // namespace reindexer
