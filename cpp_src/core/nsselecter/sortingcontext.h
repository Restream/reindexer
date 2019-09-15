#pragma once

#include "core/indexopts.h"
#include "estl/h_vector.h"

namespace reindexer {

class Index;
class Query;
struct SortingEntry;

struct SortingContext {
	struct Entry {
		Index *index = nullptr;
		const SortingEntry *data = nullptr;
		const CollateOpts *opts = nullptr;
	};

	int sortId() const;
	Index *sortIndex() const;
	bool isOptimizationEnabled() const;
	bool isIndexOrdered() const;
	const Entry *getFirstColumnEntry() const;
	void resetOptimization();

	bool enableSortOrders = false;
	h_vector<Entry, 1> entries;
	int uncommitedIndex = -1;
};

struct SortingOptions {
	SortingOptions(const Query &q, const SortingContext &sortingContext);
	bool postLoopSortingRequired() const;

	bool byBtreeIndex = false;
	bool usingGeneralAlgorithm = true;
	bool forcedMode = false;
	bool multiColumn = false;
	bool multiColumnByBtreeIndex = false;
};

}  // namespace reindexer
