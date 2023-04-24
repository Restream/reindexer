#pragma once

#include "core/indexopts.h"
#include "estl/h_vector.h"
#include "sortexpression.h"

namespace reindexer {

class Index;
struct SortingEntry;

struct SortingContext {
	struct FieldEntry {
		Index *index = nullptr;
		const SortingEntry &data;
		const CollateOpts *opts = nullptr;
	};
	struct JoinedFieldEntry {
		const SortingEntry &data;
		size_t nsIdx;
		std::string_view field;
		int index = IndexValueType::NotSet;
	};
	struct ExpressionEntry {
		const SortingEntry &data;
		size_t expression;
	};
	using Entry = std::variant<FieldEntry, JoinedFieldEntry, ExpressionEntry>;

	[[nodiscard]] int sortId() const noexcept;
	[[nodiscard]] Index *sortIndex() const noexcept;
	[[nodiscard]] const Index *sortIndexIfOrdered() const noexcept;
	[[nodiscard]] bool isOptimizationEnabled() const noexcept;
	[[nodiscard]] bool isIndexOrdered() const noexcept;
	[[nodiscard]] const Entry &getFirstColumnEntry() const noexcept;
	void resetOptimization() noexcept;

	bool enableSortOrders = false;
	h_vector<Entry, 1> entries;
	int uncommitedIndex = -1;
	bool forcedMode = false;
	std::vector<SortExpression> expressions;
	std::vector<h_vector<double, 32>> exprResults;
};

struct SortingOptions {
	SortingOptions(const SortingContext &sortingContext) noexcept;
	[[nodiscard]] bool postLoopSortingRequired() const noexcept;

	bool byBtreeIndex = false;
	bool usingGeneralAlgorithm = true;
	bool forcedMode = false;
	bool multiColumn = false;
	bool multiColumnByBtreeIndex = false;
	bool haveExpression = false;
};

}  // namespace reindexer
