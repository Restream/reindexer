#pragma once

#include "core/indexopts.h"
#include "estl/h_vector.h"
#include "sortexpression.h"

namespace reindexer {

class Index;
struct SortingEntry;

struct SortingContext {
	struct Entry {
		enum { NoExpression = -1 };
		Index *index = nullptr;
		const SortingEntry *data = nullptr;
		const CollateOpts *opts = nullptr;
		int expression = NoExpression;
	};

	int sortId() const;
	Index *sortIndex() const;
	const Index *sortIndexIfOrdered() const;
	bool isOptimizationEnabled() const;
	bool isIndexOrdered() const;
	const Entry *getFirstColumnEntry() const;
	void resetOptimization();

	bool enableSortOrders = false;
	h_vector<Entry, 1> entries;
	int uncommitedIndex = -1;
	bool forcedMode = false;
	vector<SortExpression> expressions;
	vector<h_vector<double, 32>> exprResults;
};

struct SortingOptions {
	SortingOptions(const SortingContext &sortingContext);
	bool postLoopSortingRequired() const;

	bool byBtreeIndex = false;
	bool usingGeneralAlgorithm = true;
	bool forcedMode = false;
	bool multiColumn = false;
	bool multiColumnByBtreeIndex = false;
	bool haveExpression = false;
};

}  // namespace reindexer
