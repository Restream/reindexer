#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include "core/id_type.h"
#include "estl/intrusive_ptr.h"

namespace reindexer {

enum class [[nodiscard]] MaxIterationsEstimateKind : uint8_t { Exact, AtLeast, UpperBound, Heuristic };

struct [[nodiscard]] MaxIterationsEstimate {
	static constexpr MaxIterationsEstimate Exact(size_t value) noexcept {
		return MaxIterationsEstimate{value, MaxIterationsEstimateKind::Exact};
	}
	static constexpr MaxIterationsEstimate AtLeast(size_t value) noexcept {
		return MaxIterationsEstimate{value, MaxIterationsEstimateKind::AtLeast};
	}
	static constexpr MaxIterationsEstimate UpperBound(size_t value) noexcept {
		return MaxIterationsEstimate{value, MaxIterationsEstimateKind::UpperBound};
	}
	static constexpr MaxIterationsEstimate Heuristic(size_t value) noexcept {
		return MaxIterationsEstimate{value, MaxIterationsEstimateKind::Heuristic};
	}

	constexpr bool IsExact() const noexcept { return kind == MaxIterationsEstimateKind::Exact; }
	constexpr MaxIterationsEstimate Complement(size_t universe) const noexcept {
		return IsExact() ? Exact(universe - std::min(value, universe)) : UpperBound(universe);
	}

	size_t value;
	MaxIterationsEstimateKind kind;
};

class [[nodiscard]] IndexIteratorBase {
public:
	virtual ~IndexIteratorBase() = default;
	virtual void Start(bool reverse) = 0;
	virtual std::pair<bool, IdType> Next() noexcept = 0;
	virtual void ExcludeLastSet() noexcept = 0;
	// May scan up to limitIters when cardinality is unknown.
	// Exact: full cardinality (value may exceed limitIters if already known without a scan, e.g. idset or fully cached probe).
	// AtLeast / Heuristic: partial probe; value is a lower bound from the work done (typically <= limitIters for that call).
	virtual MaxIterationsEstimate ProbeMaxIterations(size_t limitIters) noexcept = 0;
	// Must not start an expensive scan. The result may be an upper bound or a heuristic planning budget.
	virtual MaxIterationsEstimate GetPlanningEstimate() const noexcept = 0;
	virtual void SetMaxIterations(size_t iters) noexcept = 0;
};

class [[nodiscard]] IndexIterator : public intrusive_atomic_rc_wrapper<IndexIteratorBase> {
public:
	using Ptr = intrusive_ptr<IndexIterator>;
};

}  // namespace reindexer
