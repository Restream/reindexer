#pragma once

#include <span>
#include "core/index/index.h"

namespace reindexer {

class QueryEntries;
class RdxContext;
class QueryEntry;
struct SelectCtx;

namespace sorting_heuristics {

struct [[nodiscard]] NamespaceData {
	std::span<const std::unique_ptr<Index>> indexes;
	size_t itemsCount;
};

/** @brief Tries to evaluate, what would be more optimal:
 * 1. Use ordered index as a main IdSet and the filter everething else with comparators
 * 2. Perform full selection with unordered IdSets and apply general sort
 * @return true if ordered index usage is effective
 */
bool IsSortOptimizationEffective(const QueryEntries& qentries, const SelectCtx& ctx, bool needCalcTotal, const NamespaceData& nsData,
								 const RdxContext& rdxCtx);

/** @brief Performs search for the isolated ordered index with highest selectivity and compatible conditions
 *  @returns Pointer to this index or nullprt
 */
const Index* AdviceSortingIndex(const QueryEntries& qentries, const NamespaceData& nsData);

/** @brief Checks if QueryEntry will return results, ordered by key
 */
bool IsExpectingOrderedResults(const QueryEntry& qe) noexcept;

}  // namespace sorting_heuristics
}  // namespace reindexer