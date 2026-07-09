#pragma once

#include "core/index/float_vector/float_vector_index.h"
#include "core/perfstatcounter.h"
#include "core/rdxcontext.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

struct [[nodiscard]] StreamingKnnParams {
	size_t ef = 0;
	size_t needed = 0;	// offset + limit; used for batch-size feedback
};

// Lazy HNSW streaming iterator for selectLoop: pulls candidates via ContinueKnnStreaming on demand.
class [[nodiscard]] StreamingKnnIndexIterator final : public IndexIterator {
public:
	StreamingKnnIndexIterator(const FloatVectorIndex& index, StreamingKnnParams params, ConstFloatVectorView queryVec,
							  std::span<const PayloadValue> items, PerfStatCounterMT& selectPerfCounter, bool enablePerfCounters,
							  const RdxContext& rdxCtx);

	void Start(bool reverse) override;
	IdType Value() const noexcept override { return lastVal_; }
	bool Next() noexcept override;
	void ExcludeLastSet() noexcept override {}
	size_t GetMaxIterations(size_t limitIters) noexcept override;
	void SetMaxIterations(size_t iters) noexcept override { maxIterationsHint_ = iters; }

	RankT CurrentRank() const noexcept { return currentRank_; }
	void NotifyFilterMatch() noexcept { ++accepted_; }

private:
	static constexpr size_t kMaxContinueCalls = 1000;

	bool advanceInCurrentBatch() noexcept;
	bool fetchNextBatch() noexcept;
	bool continueStreaming(size_t batchSize) noexcept;

	const FloatVectorIndex& index_;
	ConstFloatVectorView queryVec_;
	StreamingKnnParams params_;
	std::span<const PayloadValue> items_;
	PerfStatCounterMT& selectPerfCounter_;
	const bool enablePerfCounters_;
	const bool isArray_;
	const RdxContext& rdxCtx_;

	std::optional<KnnStreamingSession> session_;
	KnnStreamingBatch batch_;
	fast_hash_set<int> seen_;
	size_t batchPos_{0};
	size_t continueCalls_{0};
	size_t presented_{0};
	size_t accepted_{0};
	bool exhausted_{false};
	size_t maxIterationsHint_{0};

	IdType lastVal_{IdType::Min()};
	RankT currentRank_{};
};

}  // namespace reindexer
