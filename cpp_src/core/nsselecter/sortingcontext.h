#pragma once

#include "core/index/index.h"
#include "core/indexopts.h"
#include "core/sorting/sortexpression.h"
#include "estl/h_vector.h"

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
		JoinedFieldEntry(const SortingEntry &d, unsigned nsI, std::string &&f, int i)
			: data(d), nsIdx(nsI), index(i), field(std::move(f)) {}
		JoinedFieldEntry(const JoinedFieldEntry &) = delete;
		JoinedFieldEntry(JoinedFieldEntry &&) = default;
		JoinedFieldEntry &operator=(const JoinedFieldEntry &) = delete;

		const SortingEntry &data;
		unsigned nsIdx;
		int index;	// = IndexValueType::NotSet;
		std::string field;
	};
	struct ExpressionEntry {
		const SortingEntry &data;
		size_t expression;
	};
	using Entry = std::variant<FieldEntry, JoinedFieldEntry, ExpressionEntry>;

	[[nodiscard]] int sortId() const noexcept {
		if (!enableSortOrders) return 0;
		const Index *sortIdx = sortIndex();
		return sortIdx ? int(sortIdx->SortId()) : 0;
	}
	[[nodiscard]] Index *sortIndex() const noexcept {
		if (entries.empty()) return nullptr;
		// get_if is truly noexcept, so using it instead of std::visit
		if (const auto *fe = std::get_if<FieldEntry>(&entries[0]); fe) {
			return fe->index;
		}
		return nullptr;
	}
	[[nodiscard]] const Index *sortIndexIfOrdered() const noexcept {
		if (entries.empty() || !isIndexOrdered() || !enableSortOrders) return nullptr;
		// get_if is truly noexcept, so using it instead of std::visit
		if (const auto *fe = std::get_if<FieldEntry>(&entries[0]); fe) {
			return fe->index;
		}
		return nullptr;
	}
	[[nodiscard]] bool isOptimizationEnabled() const noexcept { return (uncommitedIndex >= 0) && sortIndex(); }
	[[nodiscard]] bool isIndexOrdered() const noexcept {
		if (entries.empty()) return false;
		// get_if is truly noexcept, so using it instead of std::visit
		if (const auto *fe = std::get_if<FieldEntry>(&entries[0]); fe) {
			return fe->index && fe->index->IsOrdered();
		}
		return false;
	}
	[[nodiscard]] const Entry &getFirstColumnEntry() const noexcept {
		assertrx(!entries.empty());
		return entries[0];
	}
	void resetOptimization() noexcept {
		uncommitedIndex = -1;
		if (!entries.empty()) {
			// get_if is truly noexcept, so using it instead of std::visit
			if (auto *fe = std::get_if<FieldEntry>(&entries[0]); fe) {
				fe->index = nullptr;
			}
		}
	}

	bool enableSortOrders = false;
	h_vector<Entry, 1> entries;
	int uncommitedIndex = -1;
	bool forcedMode = false;
	std::vector<SortExpression> expressions;
	std::vector<h_vector<double, 32>> exprResults;
};

struct SortingOptions {
	SortingOptions(const SortingContext &sortingContext) noexcept
		: forcedMode{sortingContext.forcedMode},
		  multiColumn{sortingContext.entries.size() > 1},
		  haveExpression{!sortingContext.expressions.empty()} {
		if (sortingContext.entries.empty()) {
			usingGeneralAlgorithm = false;
			byBtreeIndex = false;
		} else {
			// get_if is truly noexcept, so using it instead of std::visit
			if (auto *sortEntry = std::get_if<SortingContext::FieldEntry>(&sortingContext.entries[0]); sortEntry) {
				if (sortEntry->index && sortEntry->index->IsOrdered()) {
					byBtreeIndex = (sortingContext.isOptimizationEnabled() || sortingContext.enableSortOrders);
					multiColumnByBtreeIndex = (byBtreeIndex && multiColumn);
				}
				usingGeneralAlgorithm = !byBtreeIndex;
			}
		}
	}
	[[nodiscard]] bool postLoopSortingRequired() const noexcept {
		return multiColumn || usingGeneralAlgorithm || forcedMode || haveExpression;
	}

	bool byBtreeIndex = false;
	bool usingGeneralAlgorithm = true;
	bool forcedMode = false;
	bool multiColumn = false;
	bool multiColumnByBtreeIndex = false;
	bool haveExpression = false;
};

}  // namespace reindexer
