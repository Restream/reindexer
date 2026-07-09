
#include "index_optimizer.h"
#include <thread>
#include "core/id_type.h"
#include "core/rdxcontext.h"
#include "estl/lock.h"
#include "tools/clock.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"
#include "tools/scope_guard.h"
#include "tools/thread_exception_wrapper.h"

namespace reindexer {

class [[nodiscard]] IndexOptimizer::UpdateSortedContext final : public index::IUpdateSortedContext {
public:
	UpdateSortedContext(IndexOptimizer& optimizer, IndexesSpan indexes, std::span<const PayloadValue> items, SortType curSortId,
						const index::ICancelable&)
		: optimizer_(optimizer), sortedIndexes_(optimizer_.getSortedIdxCount(indexes)), curSortId_(IdType::FromNumber(curSortId)) {
		assertrx_dbg(curSortId_ > IdType::Zero());
		ids2Sorts_.reserve(items.size());
		ids2SortsMemSize_ = ids2Sorts_.capacity() * sizeof(SortType);
		optimizer_.updateSortedContextMemory_.fetch_add(ids2SortsMemSize_, std::memory_order_relaxed);
		for (const auto& item : items) {
			ids2Sorts_.emplace_back(item.IsFree() ? SortIdNotExists : SortIdNotFilled);
		}
	}
	~UpdateSortedContext() override { optimizer_.updateSortedContextMemory_.fetch_sub(ids2SortsMemSize_, std::memory_order_relaxed); }
	unsigned GetSortedIdxCount() const noexcept override { return sortedIndexes_; }
	SortType GetCurSortId() const noexcept override { return curSortId_.ToNumber(); }
	const std::vector<SortType>& Ids2Sorts() const& noexcept override { return ids2Sorts_; }
	std::vector<SortType>& Ids2Sorts() & noexcept override { return ids2Sorts_; }

private:
	IndexOptimizer& optimizer_;
	const unsigned sortedIndexes_{0};
	const IdType curSortId_{IdType::NotSet()};
	std::vector<SortType> ids2Sorts_;
	int64_t ids2SortsMemSize_{0};
};

bool IndexOptimizer::IsOptimizationAvailable() const noexcept {
	const auto state = State();
	return state != OptimizationState::Completed && state != OptimizationState::Error;
}

void IndexOptimizer::AwaitIdle(const RdxContext& ctx) const {
	unique_lock lock(idleMtx_);
	idleCond_.wait(lock, [this]() noexcept { return running_.load(std::memory_order_acquire) == 0; }, ctx);
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
	const unsigned sortedIdxCount = getSortedIdxCount(indexes);
	for (auto& idx : indexes) {
		idx->SetSortedIdxCount(sortedIdxCount);
	}
	ScheduleOptimization(IndexOptimization::Full);
}

void IndexOptimizer::TryOptimize(const Context& ctx, const index::ICancelable& cancelable) noexcept {
	auto runningGuard = MakeScopeGuard([this]() noexcept { running_.fetch_add(1, std::memory_order_acq_rel); },
									   [this]() noexcept {
										   if (running_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
											   idleCond_.notify_all();
										   }
									   });
	try {
		tryOptimize(ctx, cancelable);
	} catch (std::exception& e) {
		logFmt(LogError, "IndexOptimizer::TryOptimize[{}]: Unexpected error during indexes optimization process: {}", ctx.nsName, e.what());
		optimizationState_.store(OptimizationState::Error);
		// Do not expect this error in testing environment
		assertrx_dbg(false);
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

unsigned IndexOptimizer::getSortedIdxCount(IndexesSpan indexes) const noexcept {
	if (!cfg_.optimizationSortWorkers) {
		return 0;
	}
	unsigned cnt = 0;
	for (auto& it : indexes) {
		if (it->IsOrdered()) {
			++cnt;
		}
	}
	return cnt;
}

void IndexOptimizer::tryOptimize(const Context& ctx, const index::ICancelable& cancelable) {
	if (ctx.indexes.empty() || !ctx.lastUpdateTime.count() || !cfg_.optimizationTimeout.count() || !cfg_.optimizationSortWorkers) {
		return;
	}

	const auto optState{optimizationState_.load(std::memory_order_acquire)};
	if (optState == OptimizationState::Completed || optState == OptimizationState::Error || cancelable.IsCanceled()) {
		return;
	}

	using namespace std::chrono;
	const auto now = duration_cast<milliseconds>(system_clock_w::now().time_since_epoch());
	if (!ctx.skipTimeCheck && (now - ctx.lastUpdateTime) < cfg_.optimizationTimeout) {
		return;
	}

	logFmt(LogTrace, "Namespace::optimizeIndexes({}) enter", ctx.nsName);
	static const auto kHardwareConcurrency = hardware_concurrency();
	const size_t maxIndexWorkers = std::min<size_t>(kHardwareConcurrency, cfg_.optimizationSortWorkers);
	assertrx(maxIndexWorkers);
	auto wasCanceled = commitIndexes(maxIndexWorkers, ctx, cancelable);
	if (!wasCanceled) {
		wasCanceled = updateSortedIDs(maxIndexWorkers, ctx, optState, cancelable);
	}

	if (wasCanceled) {
		logFmt(LogTrace, "IndexOptimizer::TryOptimize[{}] was cancelled by concurrent update", ctx.nsName);
	} else {
		for (auto& idx : ctx.indexes) {
			if (idx->IsSupportSortedIdsBuild()) {
				idx->MarkBuilt();
			}
		}

		optimizationState_.store(OptimizationState::Completed, std::memory_order_release);
		logFmt(LogTrace, "IndexOptimizer::TryOptimize[{}] done", ctx.nsName);
	}
}

WasCanceled IndexOptimizer::commitIndexes(size_t threadsCount, const Context& ctx, const index::ICancelable& cancelable) {
	assertrx_throw(threadsCount);
	std::vector<std::thread> thrs;
	thrs.reserve(threadsCount);
	std::atomic<size_t> nextIdx = 0;
	ExceptionPtrWrapper exWrp;
	std::atomic<WasCanceled> wasCanceled = WasCanceled_False;
	for (size_t i = 0; i < threadsCount; i++) {
		thrs.emplace_back([&]() noexcept {
			try {
				for (auto idxNum = nextIdx.fetch_add(1, std::memory_order_relaxed); idxNum < ctx.indexes.size();
					 idxNum = nextIdx.fetch_add(1, std::memory_order_relaxed)) {
					auto& idx = ctx.indexes[idxNum];

					PerfStatCalculatorMT calc(idx->GetCommitPerfCounter(), ctx.enablePerfCounters);
					calc.LockHit();
					const auto wasCanceledLocal = idx->Commit(cancelable);
					if (wasCanceledLocal) {
						calc.Disable();

						wasCanceled.store(WasCanceled_True, std::memory_order_relaxed);
						break;
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

	return wasCanceled.load(std::memory_order_relaxed);
}

WasCanceled IndexOptimizer::updateSortedIDs(size_t threadsCount, const Context& ctx, OptimizationState optState,
											const index::ICancelable& cancelable) {
	assertrx_throw(threadsCount);

	const bool forceBuildAllIndexes = (optState == OptimizationState::None);

	// Update sort orders and sort_id for each index
	size_t currentSortId = 0;

	for (auto& idx : ctx.indexes) {
		if (idx->IsOrdered()) {
			UpdateSortedContext sortCtx(*this, ctx.indexes, ctx.items, ++currentSortId, cancelable);

			// Rebuild index sort orders mapping in the next cases:
			// 1. Current ordered index is not built (i.e. it was modified) -> all mappings, related to it have to be rebuilt;
			// 2. Current sortID for the ordered index was change -> all mappings, related to it have to be rebuilt;
			// 3. Current ordered index is built, but one of the other indexes is not -> rebuild single mapping only;
			const bool forceBuildAllIndexMappings = forceBuildAllIndexes || !idx->IsBuilt() || (idx->SortId() != currentSortId);

			const auto wasCanceledL = idx->MakeSortOrders(sortCtx, cancelable);
			ExceptionPtrWrapper exWrp;

			if (wasCanceledL) {
				return wasCanceledL;
			}

			// Build in multiple threads
			std::vector<std::thread> thrs;
			thrs.reserve(threadsCount);
			std::atomic<size_t> nextIdx = 0;
			std::atomic<WasCanceled> wasCanceled = WasCanceled_False;
			for (size_t i = 0; i < threadsCount; i++) {
				thrs.emplace_back([&]() {
					try {
						for (auto idxNum = nextIdx.fetch_add(1, std::memory_order_relaxed); idxNum < ctx.indexes.size();
							 idxNum = nextIdx.fetch_add(1, std::memory_order_relaxed)) {
							auto& idx = ctx.indexes[idxNum];
							if (forceBuildAllIndexMappings || !idx->IsBuilt()) {
								const auto wasCanceledL = idx->UpdateSortedIds(sortCtx, cancelable);
								if (wasCanceledL || exWrp.HasException()) {
									wasCanceled.store(WasCanceled_True, std::memory_order_relaxed);
									return;
								}
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
	}
	return WasCanceled_False;
}

}  // namespace reindexer
