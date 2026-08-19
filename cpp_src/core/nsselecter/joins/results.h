#pragma once

#include "core/nsselecter/joins/query_joins_table.h"
#include "core/queryresults/itemref.h"
#include "estl/fast_hash_map.h"

#include <optional>
#include <vector>

namespace reindexer {

class LocalQueryResults;
class Query;

namespace joins {

/// Offset in 'items_' for left Ns item
struct [[nodiscard]] ItemOffset {
	ItemOffset() = default;
	ItemOffset(uint32_t f, uint32_t o, uint32_t c) noexcept : field(f), offset(o), count(c) {}
	auto operator<=>(const ItemOffset& other) const noexcept = default;
	/// index of joined field
	/// (equals to position in joinItemsProcessors_)
	uint16_t field{0};
	/// Offset of items in 'items_' container
	uint32_t offset{0};
	/// Amount of joined items for this field
	uint32_t count{0};
};
using ItemsOffsets = h_vector<ItemOffset, 1>;

/// Result of joining entire NamespaceImpl
class [[nodiscard]] NamespaceResults {
public:
	using Offsets = fast_hash_map<IdType, ItemsOffsets>;

	/// Move-insertion of LocalQueryResults (for n-th joined field)
	/// ItemRefs into our results container
	/// @param rowid - rowid of item
	/// @param joinedNsId - nsid of a joined NS.
	/// @param fieldIdx - index of joined field
	/// @param qr - QueryResults reference
	void Insert(IdType rowid, int joinedNsId, uint16_t fieldIdx, LocalQueryResults&& qr);

	/// Gets/sets amount of joined joined fields
	/// @param joinedFieldsCount - joinItemsProcessors.size()
	void SetJoinedFieldsCount(uint32_t joinedFieldsCount) noexcept { joinedFieldsCount_ = joinedFieldsCount; }
	uint32_t GetFieldsCount() const noexcept { return joinedFieldsCount_; }

	/// @returns total amount of joined items for
	/// all the joined fields
	size_t TotalItems() const noexcept { return items_.Size(); }

	/// Clear all internal data
	void Clear() {
		offsets_.clear();
		items_.Clear();
		tmpBuf_.Clear();
		joinedFieldsCount_ = 0;
	}

	/// Clears all joined items, except the chosen row
	void ClearJoinedItemsExceptFor(IdType rowId) {
		constexpr bool deallocateMemory = false;
		ItemsOffsets offsets;
		if (auto found = offsets_.find(rowId); found != offsets_.end()) {
			offsets = std::move(found->second);
			uint32_t pos = 0;
			tmpBuf_.Clear<deallocateMemory>();
			tmpBuf_.Reserve(offsets.size());
			for (auto& offset : offsets) {
				auto mbegin = std::move(items_).mbegin() + offset.offset;
				auto mend = mbegin + offset.count;
				tmpBuf_.Insert(tmpBuf_.cend(), mbegin, mend);
				offset.offset = pos;
				pos += offset.count;
			}
		}
		offsets_.clear();
		items_.Clear<deallocateMemory>();
		if (offsets.size()) {
			items_.Insert(items_.cend(), std::move(tmpBuf_).mbegin(), std::move(tmpBuf_).mend());
			offsets_[rowId] = std::move(offsets);
		}
	}

private:
	friend class ItemIterator;
	friend class FieldIterator;
	/// Offsets in 'result' for every item
	Offsets offsets_;
	/// Items for all the joined fields
	ItemRefVector items_;
	/// Temporary buffer for internal usage
	ItemRefVector tmpBuf_;
	/// Amount of joined selectors for this NS
	uint32_t joinedFieldsCount_ = 0;
};

/// Results of joining all the namespaces (in case of merge queries)
class [[nodiscard]] Results : public std::vector<NamespaceResults> {
public:
	using Base = std::vector<NamespaceResults>;
	using Base::Base;
	using JoinsTable = std::optional<QueryJoinsTable>;

	/// Set Query joins table.
	/// @param q - query with join queries.
	void SetJoinsTable(const Query& q) { joinsTable_.emplace(q); }

	/// Set Query joins table.
	/// @param joinsTable - query join context.
	void SetJoinsTable(const QueryJoinsTable& joinsTable) { joinsTable_ = joinsTable; }

	/// @return Query Join table.
	const JoinsTable& GetJoinsTable() const noexcept { return joinsTable_; }

private:
	JoinsTable joinsTable_;
};

}  // namespace joins
}  // namespace reindexer
