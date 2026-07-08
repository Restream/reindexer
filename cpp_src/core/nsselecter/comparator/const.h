#pragma once

namespace reindexer::comparators {
/// Those values have to be tuned. Current values are based on our CXX benchmarks. Issue #2237
// Non-indexed values extraction is very slow
constexpr double kNonIdxFieldComparatorCostMultiplier = 64.0;
// Indexed values extraction by jsonpath is equivalent to non-indexed fields
constexpr double kIdxJsonPathComparatorCostMultiplier = kNonIdxFieldComparatorCostMultiplier;
// Indexed columns are the best possible scenario for comparator
constexpr double kIdxColumnComparatorCostMultiplier = 1.125;
// Indexed values extraction by payload offset is also fast, but slower than extraction from column
constexpr double kIdxOffsetComparatorCostMultiplier = 1.25;
}  // namespace reindexer::comparators
