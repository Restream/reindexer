#include "knn_streaming_index_iterator.h"
#include "knn_streaming_estimator.h"
#include "tools/logger.h"

namespace reindexer {
static constexpr size_t kSeenReserveFactor = 4;

StreamingKnnIndexIterator::StreamingKnnIndexIterator(const FloatVectorIndex& index, StreamingKnnParams params,
													 ConstFloatVectorView queryVec, std::span<const PayloadValue> items,
													 PerfStatCounterMT& selectPerfCounter, bool enablePerfCounters,
													 const RdxContext& rdxCtx)
	: index_{index},
	  queryVec_{queryVec},
	  params_{std::move(params)},
	  items_{items},
	  selectPerfCounter_{selectPerfCounter},
	  enablePerfCounters_{enablePerfCounters},
	  isArray_(index_.Opts().IsArray()),
	  rdxCtx_{rdxCtx} {}

void StreamingKnnIndexIterator::Start(bool reverse) {
	if (reverse) {
		throw Error(errLogic, "Reverse iteration is not supported for streaming KNN search");
	}
	if (session_) {
		throw Error(errLogic, "Streaming KNN iterator cannot be restarted");
	}
	PerfStatCalculatorMT calc(selectPerfCounter_, enablePerfCounters_);
	session_.emplace(index_.BeginKnnStreaming(queryVec_, params_.ef, rdxCtx_));
	if (isArray_) {
		seen_.reserve(
			std::min(std::min(items_.size(), kMaxContinueCalls * StreamingKnnEstimator::kMaxEfBatch), params_.needed * kSeenReserveFactor));
	}
}

bool StreamingKnnIndexIterator::Next() noexcept {
	assertrx_dbg(session_);
	if (continueCalls_ == 0) {
		if (!continueStreaming(params_.ef)) {
			lastVal_ = IdType::Min();
			return false;
		}
	}

	while (true) {
		if (advanceInCurrentBatch()) {
			return true;
		}
		if (exhausted_ || continueCalls_ >= kMaxContinueCalls) {
			lastVal_ = IdType::Min();
			return false;
		}
		if (!fetchNextBatch()) {
			lastVal_ = IdType::Min();
			return false;
		}
	}
}

bool StreamingKnnIndexIterator::advanceInCurrentBatch() noexcept {
	while (batchPos_ < batch_.ids.size()) {
		const size_t i = batchPos_++;
		const IdType rowId = batch_.ids[i];
		if (isArray_) {
			try {
				if (!seen_.emplace(rowId.ToNumber()).second) {
					continue;
				}
			} catch (const std::exception& e) {
				logFmt(LogError, "Streaming KNN search failed to track seen ids: {}", e.what());
				exhausted_ = true;
				return false;
			} catch (...) {
				logFmt(LogError, "Streaming KNN search failed to track seen ids: unexpected exception");
				exhausted_ = true;
				return false;
			}
		}
		lastVal_ = rowId;
		currentRank_ = batch_.ranks[i];
		++presented_;
		return true;
	}
	return false;
}

bool StreamingKnnIndexIterator::continueStreaming(size_t batchSize) noexcept {
	try {
		PerfStatCalculatorMT calc(selectPerfCounter_, enablePerfCounters_);
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access) session_ is set in Start() before any Continue call
		index_.ContinueKnnStreaming(*session_, batchSize, batch_, rdxCtx_);
	} catch (const std::exception& e) {
		logFmt(LogError, "Streaming KNN search failed to fetch next batch: {}", e.what());
		exhausted_ = true;
		return false;
	} catch (...) {
		logFmt(LogError, "Streaming KNN search failed to fetch next batch: unexpected exception");
		exhausted_ = true;
		return false;
	}
	++continueCalls_;
	batchPos_ = 0;
	exhausted_ = batch_.exhausted;
	return !batch_.ids.empty() || !exhausted_;
}

bool StreamingKnnIndexIterator::fetchNextBatch() noexcept {
	const auto batchSize = StreamingKnnEstimator::EstimateBatchSize(accepted_, presented_, params_.needed);
	return continueStreaming(batchSize);
}

size_t StreamingKnnIndexIterator::GetMaxIterations(size_t limitIters) noexcept {
	if (maxIterationsHint_ > 0) {
		return std::min(limitIters, maxIterationsHint_);
	}
	return std::min(limitIters, StreamingKnnEstimator::kMaxEfBatch);
}

}  // namespace reindexer
