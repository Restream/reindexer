#pragma once

#include "core/index/index.h"
#include "core/indexopts.h"
#include "estl/h_vector.h"
#include "sortexpression.h"

namespace reindexer {

class Index;
struct SortingEntry;

struct SortingContext {
	struct RawDataParams {
		RawDataParams() = default;
		RawDataParams(const void *p, const PayloadType &pt, int field) noexcept
			: ptr(p), type(ptr ? pt.Field(field).Type() : KeyValueType::Undefined{}) {}

		const void *ptr = nullptr;
		KeyValueType type = KeyValueType::Undefined{};
	};

	struct FieldEntry {
		const SortingEntry &data;
		Index *index = nullptr;
		RawDataParams rawData = {};
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
		assertrx_throw(!entries.empty());
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
