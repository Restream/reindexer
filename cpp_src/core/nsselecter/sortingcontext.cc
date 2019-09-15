#include "sortingcontext.h"
#include "core/index/index.h"
#include "core/query/query.h"

namespace reindexer {

Index *SortingContext::sortIndex() const { return entries.empty() ? nullptr : entries[0].index; }

int SortingContext::sortId() const {
	if (!enableSortOrders) return 0;
	Index *sortIdx = sortIndex();
	return sortIdx ? sortIdx->SortId() : 0;
}

bool SortingContext::isIndexOrdered() const {
	if (entries.empty()) return false;
	return (!entries.empty() && entries[0].index && entries[0].index->IsOrdered());
}

bool SortingContext::isOptimizationEnabled() const { return (uncommitedIndex >= 0) && sortIndex(); }

const SortingContext::Entry *SortingContext::getFirstColumnEntry() const {
	if (entries.empty()) return nullptr;
	return &entries[0];
}

void SortingContext::resetOptimization() {
	uncommitedIndex = -1;
	if (!entries.empty()) entries[0].index = nullptr;
}

SortingOptions::SortingOptions(const Query &q, const SortingContext &sortingContext) {
	forcedMode = !q.forcedSortOrder.empty();
	multiColumn = (sortingContext.entries.size() > 1);
	if (sortingContext.entries.empty()) {
		usingGeneralAlgorithm = false;
		byBtreeIndex = false;
	} else {
		const SortingContext::Entry &sortEntry = sortingContext.entries[0];
		if (sortEntry.index && sortEntry.index->IsOrdered()) {
			byBtreeIndex = (sortingContext.isOptimizationEnabled() || sortingContext.enableSortOrders);
			multiColumnByBtreeIndex = (byBtreeIndex && multiColumn);
		}
		usingGeneralAlgorithm = !byBtreeIndex;
	}
}

bool SortingOptions::postLoopSortingRequired() const { return multiColumn || usingGeneralAlgorithm || forcedMode; }

}  // namespace reindexer
