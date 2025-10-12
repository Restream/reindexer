#pragma once

#include "core/index/index.h"
#include "core/indexopts.h"
#include "core/sorting/sortexpression.h"
#include "estl/h_vector.h"

namespace reindexer {

class Index;
struct SortingEntry;

struct [[nodiscard]] SortingContext {
	struct [[nodiscard]] RawDataParams {
		RawDataParams() = default;
		RawDataParams(const void* p, const PayloadType& pt, int field) noexcept
			: ptr(p), type(ptr ? pt.Field(field).Type() : KeyValueType::Undefined{}) {}

		const void* ptr = nullptr;
		KeyValueType type = KeyValueType::Undefined{};
	};

	struct [[nodiscard]] FieldEntry {
		const SortingEntry& data;
		Index* index = nullptr;
		RawDataParams rawData = {};
		const CollateOpts* opts = nullptr;
	};
	struct [[nodiscard]] JoinedFieldEntry {
		JoinedFieldEntry(const SortingEntry& d, unsigned nsI, std::string&& f, int i)
			: data(d), nsIdx(nsI), index(i), field(std::move(f)) {}
		JoinedFieldEntry(const JoinedFieldEntry&) = delete;
		JoinedFieldEntry(JoinedFieldEntry&&) = default;
		JoinedFieldEntry& operator=(const JoinedFieldEntry&) = delete;

		const SortingEntry& data;
		unsigned nsIdx;
		int index;	// = IndexValueType::NotSet;
		std::string field;
	};
	struct [[nodiscard]] ExpressionEntry {
		const SortingEntry& data;
		size_t expression;
	};
	struct [[nodiscard]] Entry : public std::variant<FieldEntry, JoinedFieldEntry, ExpressionEntry> {
		using Base = std::variant<FieldEntry, JoinedFieldEntry, ExpressionEntry>;
		using Base::Base;
		reindexer::Desc Desc() const;
		const Base& AsVariant() const& noexcept { return *this; }
		auto AsVariant() const&& = delete;
	};

	int sortId() const noexcept {
		if (!enableSortOrders) {
			return 0;
		}
		const Index* sortIdx = sortIndex();
		return sortIdx ? int(sortIdx->SortId()) : 0;
	}
	Index* sortIndex() const noexcept {
		if (entries.empty()) {
			return nullptr;
		}
		// get_if is truly noexcept, so using it instead of std::visit
		if (const auto* fe = std::get_if<FieldEntry>(&entries[0]); fe) {
			return fe->index;
		}
		return nullptr;
	}
	const Index* sortIndexIfOrdered() const noexcept {
		if (entries.empty() || !isIndexOrdered() || !enableSortOrders) {
			return nullptr;
		}
		// get_if is truly noexcept, so using it instead of std::visit
		if (const auto* fe = std::get_if<FieldEntry>(&entries[0]); fe) {
			return fe->index;
		}
		return nullptr;
	}
	const FieldEntry* sortFieldEntryIfOrdered() const noexcept {
		if (entries.empty() || !isIndexOrdered() || !enableSortOrders) {
			return nullptr;
		}
		return std::get_if<FieldEntry>(&entries[0]);
	}
	bool isOptimizationEnabled() const noexcept { return (uncommitedIndex >= 0) && sortIndex(); }
	bool isIndexOrdered() const noexcept {
		if (entries.empty()) {
			return false;
		}
		// get_if is truly noexcept, so using it instead of std::visit
		if (const auto* fe = std::get_if<FieldEntry>(&entries[0]); fe) {
			return fe->index && fe->index->IsOrdered();
		}
		return false;
	}
	const Entry& getFirstColumnEntry() const noexcept {
		assertrx_throw(!entries.empty());
		return entries[0];
	}
	void resetOptimization() noexcept {
		uncommitedIndex = -1;
		if (!entries.empty()) {
			// get_if is truly noexcept, so using it instead of std::visit
			if (auto* fe = std::get_if<FieldEntry>(&entries[0]); fe) {
				fe->index = nullptr;
			}
		}
	}

	Reranker ToReranker(const NamespaceImpl&) const;

	bool enableSortOrders = false;
	h_vector<Entry, 1> entries;
	int uncommitedIndex = -1;
	bool forcedMode = false;
	std::vector<SortExpression> expressions;
	std::vector<h_vector<double, 32>> exprResults;
};

struct [[nodiscard]] SortingOptions {
	SortingOptions(const SortingContext& sortingContext) noexcept
		: forcedMode{sortingContext.forcedMode},
		  multiColumn{sortingContext.entries.size() > 1},
		  haveExpression{!sortingContext.expressions.empty()} {
		if (sortingContext.entries.empty()) {
			usingGeneralAlgorithm = false;
			byBtreeIndex = false;
		} else {
			// get_if is truly noexcept, so using it instead of std::visit
			if (auto* sortEntry = std::get_if<SortingContext::FieldEntry>(&sortingContext.entries[0]); sortEntry) {
				if (sortEntry->index && sortEntry->index->IsOrdered()) {
					byBtreeIndex = (sortingContext.isOptimizationEnabled() || sortingContext.enableSortOrders);
					multiColumnByBtreeIndex = (byBtreeIndex && multiColumn);
				}
				usingGeneralAlgorithm = !byBtreeIndex;
			}
		}
	}
	bool postLoopSortingRequired() const noexcept { return multiColumn || usingGeneralAlgorithm || forcedMode || haveExpression; }

	bool byBtreeIndex = false;
	bool usingGeneralAlgorithm = true;
	bool forcedMode = false;
	bool multiColumn = false;
	bool multiColumnByBtreeIndex = false;
	bool haveExpression = false;
};

}  // namespace reindexer
