#include "sortingcontext.h"
#include "core/index/index.h"
#include "core/query/query.h"

namespace reindexer {

Index *SortingContext::sortIndex() const noexcept {
	if (entries.empty()) return nullptr;
	return std::visit(overloaded{[](const OneOf<ExpressionEntry, JoinedFieldEntry> &) noexcept -> Index * { return nullptr; },
								 [](const FieldEntry &e) noexcept { return e.index; }},
					  entries[0]);
}

const Index *SortingContext::sortIndexIfOrdered() const noexcept {
	if (entries.empty() || !isIndexOrdered() || !enableSortOrders) return nullptr;
	return std::visit(overloaded{[](const OneOf<ExpressionEntry, JoinedFieldEntry> &) noexcept -> Index * { return nullptr; },
								 [](const FieldEntry &e) noexcept { return e.index; }},
					  entries[0]);
}

int SortingContext::sortId() const noexcept {
	if (!enableSortOrders) return 0;
	Index *sortIdx = sortIndex();
	return sortIdx ? sortIdx->SortId() : 0;
}

bool SortingContext::isIndexOrdered() const noexcept {
	if (entries.empty()) return false;
	return std::visit(overloaded{[](const OneOf<ExpressionEntry, JoinedFieldEntry> &) noexcept { return false; },
								 [](const FieldEntry &e) noexcept { return e.index && e.index->IsOrdered(); }},
					  entries[0]);
}

bool SortingContext::isOptimizationEnabled() const noexcept { return (uncommitedIndex >= 0) && sortIndex(); }

const SortingContext::Entry &SortingContext::getFirstColumnEntry() const noexcept {
	assertrx(!entries.empty());
	return entries[0];
}

void SortingContext::resetOptimization() noexcept {
	uncommitedIndex = -1;
	if (!entries.empty()) {
		std::visit(
			overloaded{[](const OneOf<ExpressionEntry, JoinedFieldEntry> &) noexcept {}, [](FieldEntry &e) noexcept { e.index = nullptr; }},
			entries[0]);
	}
}

SortingOptions::SortingOptions(const SortingContext &sortingContext) noexcept
	: forcedMode{sortingContext.forcedMode},
	  multiColumn{sortingContext.entries.size() > 1},
	  haveExpression{!sortingContext.expressions.empty()} {
	if (sortingContext.entries.empty()) {
		usingGeneralAlgorithm = false;
		byBtreeIndex = false;
	} else {
		std::visit(overloaded{[](const OneOf<SortingContext::ExpressionEntry, SortingContext::JoinedFieldEntry> &) noexcept {},
							  [&](const SortingContext::FieldEntry &sortEntry) noexcept {
								  if (sortEntry.index && sortEntry.index->IsOrdered()) {
									  byBtreeIndex = (sortingContext.isOptimizationEnabled() || sortingContext.enableSortOrders);
									  multiColumnByBtreeIndex = (byBtreeIndex && multiColumn);
								  }
								  usingGeneralAlgorithm = !byBtreeIndex;
							  }},
				   sortingContext.entries[0]);
	}
}

bool SortingOptions::postLoopSortingRequired() const noexcept {
	return multiColumn || usingGeneralAlgorithm || forcedMode || haveExpression;
}

}  // namespace reindexer
