
#include "index_optimizer.h"
#include "tools/clock.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"
#include "tools/thread_exception_wrapper.h"

namespace reindexer {

class [[nodiscard]] IndexOptimizer::UpdateSortedContext final : public IUpdateSortedContext {
public:
	UpdateSortedContext(IndexOptimizer& optimizer, IndexesSpan indexes, std::span<const PayloadValue> items, SortType curSortId)
		: optimizer_(optimizer), sortedIndexes_(optimizer_.getSortedIdxCount(indexes)), curSortId_(curSortId) {
		assertrx_dbg(curSortId_ > 0);
		ids2Sorts_.reserve(items.size());
		ids2SortsMemSize_ = ids2Sorts_.capacity() * sizeof(SortType);
		optimizer_.updateSortedContextMemory_.fetch_add(ids2SortsMemSize_, std::memory_order_relaxed);
		for (const auto& item : items) {
			ids2Sorts_.emplace_back(item.IsFree() ? SortIdNotExists : SortIdNotFilled);
		}
	}
	~UpdateSortedContext() override { optimizer_.updateSortedContextMemory_.fetch_sub(ids2SortsMemSize_, std::memory_order_relaxed); }
	int GetSortedIdxCount() const noexcept override { return sortedIndexes_; }
	SortType GetCurSortId() const noexcept override { return curSortId_; }
	const std::vector<SortType>& Ids2Sorts() const& noexcept override { return ids2Sorts_; }
	std::vector<SortType>& Ids2Sorts() & noexcept override { return ids2Sorts_; }

private:
	IndexOptimizer& optimizer_;
	const int sortedIndexes_{0};
	const IdType curSortId_{-1};
	std::vector<SortType> ids2Sorts_;
	int64_t ids2SortsMemSize_{0};
};

bool IndexOptimizer::IsOptimizationAvailable() const noexcept {
	const auto state = State();
	return state != OptimizationState::Completed && state != OptimizationState::Error;
}

void reindexer::IndexOptimizer::ScheduleOptimization(IndexOptimization requestedOptimization) noexcept {
	switch (requestedOptimization) {
		case IndexOptimization::Full:
			optimizationState_.store(OptimizationState::None);
			break;
		case IndexOptimization::Partial: {
			OptimizationState expected{OptimizationState::Completed};
			if (!optimizationState_.compare_exchange_strong(expected, OptimizationState::Partial)) {
				expected = OptimizationState::Error;
				optimizationState_.compare_exchange_strong(expected, OptimizationState::None);
			}
			break;
		}
	}
}

void IndexOptimizer::UpdateSortedIdxCount(IndexesSpan indexes) {
	const int sortedIdxCount = getSortedIdxCount(indexes);
	for (auto& idx : indexes) {
		idx->SetSortedIdxCount(sortedIdxCount);
	}
	ScheduleOptimization(IndexOptimization::Full);
}

void IndexOptimizer::TryOptimize(const Context& ctx, const std::function<bool()>& isCanceledF) noexcept {
	try {
		tryOptimize(ctx, isCanceledF);
	} catch (std::exception& e) {
		logFmt(LogError, "IndexOptimizer::TryOptimize[{}]: Unexpected error during indexes optimization process: {}", ctx.nsName, e.what());
		optimizationState_.store(OptimizationState::Error);
		assertrx_dbg(false);  // Do not expect this error in testing environment
	}
}

void IndexOptimizer::SetConfig(std::string_view nsName, IndexesSpan indexes, const Config& newCfg) {
	const bool needReoptimizeIndexes = (cfg_.optimizationSortWorkers == 0) != (newCfg.optimizationSortWorkers == 0);
	if (cfg_.optimizationSortWorkers != newCfg.optimizationSortWorkers || cfg_.optimizationTimeout != newCfg.optimizationTimeout) {
		logFmt(LogInfo, "IndexOptimizer[{}]: Setting new index optimization config. Workers: {}->{}, timeout: {}->{}", nsName,
			   cfg_.optimizationSortWorkers, newCfg.optimizationSortWorkers, cfg_.optimizationTimeout.count(),
			   newCfg.optimizationTimeout.count());
	}
	cfg_ = newCfg;
	if (needReoptimizeIndexes) {
		UpdateSortedIdxCount(indexes);
	}
}

int IndexOptimizer::getSortedIdxCount(IndexesSpan indexes) const noexcept {
	if (!cfg_.optimizationSortWorkers) {
		return 0;
	}
	int cnt = 0;
	for (auto& it : indexes) {
		if (it->IsOrdered()) {
			++cnt;
		}
	}
	return cnt;
}

void IndexOptimizer::tryOptimize(const Context& ctx, const std::function<bool()>& isCanceledF) {
	assertrx_dbg(isCanceledF);
	if (ctx.indexes.empty() || !ctx.lastUpdateTime.count() || !cfg_.optimizationTimeout.count() || !cfg_.optimizationSortWorkers) {
		return;
	}

	auto isCanceled = [&isCanceledF] { return isCanceledF && isCanceledF(); };
	const auto optState{optimizationState_.load(std::memory_order_acquire)};
	if (optState == OptimizationState::Completed || optState == OptimizationState::Error || isCanceled()) {
		return;
	}

	using namespace std::chrono;
	const auto now = duration_cast<milliseconds>(system_clock_w::now().time_since_epoch());
	if (!ctx.skipTimeCheck && (now - ctx.lastUpdateTime) < cfg_.optimizationTimeout) {
		return;
	}

	const bool forceBuildAllIndexes = (optState == OptimizationState::None);

	logFmt(LogTrace, "Namespace::optimizeIndexes({}) enter", ctx.nsName);
	for (auto& idx : ctx.indexes) {
		if (isCanceled()) {
			break;
		}
		PerfStatCalculatorMT calc(idx->GetCommitPerfCounter(), ctx.enablePerfCounters);
		calc.LockHit();
		idx->Commit();
	}

	// Update sort orders and sort_id for each index
	size_t currentSortId = 1;
	static const auto kHardwareConcurrency = hardware_concurrency();
	const size_t maxIndexWorkers = std::min<size_t>(kHardwareConcurrency, cfg_.optimizationSortWorkers);
	assertrx(maxIndexWorkers);
	for (auto& idx : ctx.indexes) {
		if (idx->IsOrdered()) {
			UpdateSortedContext sortCtx(*this, ctx.indexes, ctx.items, currentSortId++);
			const bool forceBuildAll = forceBuildAllIndexes || idx->IsBuilt() || idx->SortId() != currentSortId;
			idx->MakeSortOrders(sortCtx);
			ExceptionPtrWrapper exWrp;
			// Build in multiple threads
			std::vector<std::thread> thrs;
			thrs.reserve(maxIndexWorkers);
			for (size_t i = 0; i < maxIndexWorkers; i++) {
				thrs.emplace_back([&, i]() {
					try {
						for (size_t j = i; j < ctx.indexes.size() && !isCanceled(); j += maxIndexWorkers) {
							auto& idx = ctx.indexes[j];
							if (forceBuildAll || !idx->IsBuilt()) {
								idx->UpdateSortedIds(sortCtx);
							}
						}
					} catch (...) {
						exWrp.SetException(std::current_exception());
					}
				});
			}
			for (auto& th : thrs) {
				th.join();
			}
			exWrp.RethrowException();
		}
		if (isCanceled()) {
			break;
		}
	}

	if (isCanceled()) {
		logFmt(LogTrace, "IndexOptimizer::TryOptimize[{}] was cancelled by concurrent update", ctx.nsName);
	} else {
		optimizationState_.store(OptimizationState::Completed, std::memory_order_release);
		for (auto& idxIt : ctx.indexes) {
			if (idxIt->IsSupportSortedIdsBuild()) {
				idxIt->MarkBuilt();
			}
		}
		logFmt(LogTrace, "IndexOptimizer::TryOptimize[{}] done", ctx.nsName);
	}
}

}  // namespace reindexer
