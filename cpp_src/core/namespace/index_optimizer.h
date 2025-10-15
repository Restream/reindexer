#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include "core/index/index.h"

namespace reindexer {

struct NamespaceConfigData;

enum class [[nodiscard]] IndexOptimization : int8_t { Partial, Full };

class [[nodiscard]] IndexOptimizer {
public:
	using IndexesSpan = std::span<const std::unique_ptr<Index>>;

	struct [[nodiscard]] Context {
		std::string_view nsName;
		bool enablePerfCounters{false};
		bool skipTimeCheck{false};
		std::chrono::milliseconds lastUpdateTime{0};
		IndexesSpan indexes;
		std::span<const PayloadValue> items;
	};

	struct [[nodiscard]] Config {
		std::chrono::milliseconds optimizationTimeout{800};
		int optimizationSortWorkers{4};
	};

	IndexOptimizer() = default;
	IndexOptimizer(const IndexOptimizer& other)
		: optimizationState_{OptimizationState::None}, updateSortedContextMemory_{0}, cfg_{other.cfg_} {}

	bool IsOptimizationAvailable() const noexcept;
	bool IsOptimizationCompleted() const noexcept { return State() == OptimizationState::Completed; }
	int64_t UpdateSortedContextMemory() const noexcept { return updateSortedContextMemory_.load(std::memory_order_relaxed); }
	void ScheduleOptimization(IndexOptimization requestedOptimization) noexcept;
	void UpdateSortedIdxCount(IndexesSpan indexes);
	void TryOptimize(const Context& optCtx, const std::function<bool()>& isCanceledF) noexcept;
	void SetConfig(std::string_view nsName, IndexesSpan indexes, const Config& newCfg);
	OptimizationState State() const noexcept { return optimizationState_.load(std::memory_order_acquire); }
	const std::atomic<OptimizationState>& StateRef() const noexcept { return optimizationState_; }

private:
	class UpdateSortedContext;

	int getSortedIdxCount(IndexesSpan indexes) const noexcept;
	void tryOptimize(const Context& ctx, const std::function<bool()>& isCanceledF);

	std::atomic<OptimizationState> optimizationState_{OptimizationState::None};
	std::atomic_int64_t updateSortedContextMemory_{0};
	Config cfg_;
};

}  // namespace reindexer
