#pragma once

#include <vector>
#include "core/itemimpl.h"
#include "estl/fast_hash_map.h"
#include "itemref.h"
#include "localqueryresults.h"

namespace reindexer {

class PayloadType;
class TagsMatcher;

namespace joins {

class NamespaceResults;

/// Offset in 'items_' for left Ns item
struct [[nodiscard]] ItemOffset {
	ItemOffset() noexcept : field(0), offset(0), size(0) {}
	ItemOffset(uint32_t f, uint32_t o, uint32_t s) noexcept : field(f), offset(o), size(s) {}
	bool operator==(const ItemOffset& other) const noexcept { return field == other.field && offset == other.offset && size == other.size; }
	bool operator!=(const ItemOffset& other) const noexcept { return !operator==(other); }
	/// index of joined field
	/// (equals to position in joinedSelectors_)
	uint32_t field;
	/// Offset of items in 'items_' container
	uint32_t offset;
	/// Amount of joined items for this field
	uint32_t size;
};
using ItemOffsets = h_vector<ItemOffset, 1>;

/// Result of joining entire NamespaceImpl
class [[nodiscard]] NamespaceResults {
public:
	/// Move-insertion of LocalQueryResults (for n-th joined field)
	/// ItemRefs into our results container
	/// @param rowid - rowid of item
	/// @param fieldIdx - index of joined field
	/// @param qr - QueryResults reference
	void Insert(IdType rowid, uint32_t fieldIdx, LocalQueryResults&& qr);

	/// Gets/sets amount of joined selectors
	/// @param joinedSelectorsCount - joinedSelectors.size()
	void SetJoinedSelectorsCount(uint32_t joinedSelectorsCount) noexcept { joinedSelectorsCount_ = joinedSelectorsCount; }
	uint32_t GetJoinedSelectorsCount() const noexcept { return joinedSelectorsCount_; }

	/// @returns total amount of joined items for
	/// all the joined fields
	size_t TotalItems() const noexcept { return items_.Size(); }

	/// Clear all internal data
	void Clear() {
		offsets_.clear();
		items_.Clear();
		tmpBuf_.Clear();
		joinedSelectorsCount_ = 0;
	}

	/// Clears all joined items, except the chosen row
	void ClearJoinedItemsExcept(IdType rowId) {
		constexpr bool deallocateMemory = false;
		ItemOffsets offsets;
		if (auto found = offsets_.find(rowId); found != offsets_.end()) {
			offsets = std::move(found->second);
			uint32_t pos = 0;
			tmpBuf_.Clear<deallocateMemory>();
			tmpBuf_.Reserve(offsets.size());
			for (auto& offset : offsets) {
				auto mbegin = std::move(items_).mbegin() + offset.offset;
				auto mend = mbegin + offset.size;
				tmpBuf_.Insert(tmpBuf_.cend(), mbegin, mend);
				offset.offset = pos;
				pos += offset.size;
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
	friend class JoinedFieldIterator;
	/// Offsets in 'result' for every item
	fast_hash_map<IdType, ItemOffsets> offsets_;
	/// Items for all the joined fields
	ItemRefVector items_;
	/// Temporary buffer for internal usage
	ItemRefVector tmpBuf_;
	/// Amount of joined selectors for this NS
	uint32_t joinedSelectorsCount_ = 0;
};

/// Results of joining all the namespaces (in case of merge queries)
class [[nodiscard]] Results : public std::vector<NamespaceResults> {};

/// Joined field iterator for Item
/// of left NamespaceImpl (main ns).
class [[nodiscard]] JoinedFieldIterator {
public:
	using reference = ItemRef&;
	using const_reference = const ItemRef&;

	JoinedFieldIterator(const NamespaceResults* parent, const ItemOffsets& offsets, uint8_t joinedFieldOrder) noexcept
		: joinRes_(parent), offsets_(&offsets), order_(joinedFieldOrder) {
		if (offsets_->size() > 0) {
			updateOffset();
		}
	}

	bool operator==(const JoinedFieldIterator& other) const;
	bool operator!=(const JoinedFieldIterator& other) const { return !operator==(other); }

	const_reference operator[](size_t idx) const noexcept {
		assertrx(currOffset_ + idx < joinRes_->items_.Size());
		return joinRes_->items_.GetItemRef(currOffset_ + idx);
	}
	reference operator[](size_t idx) noexcept {
		assertrx(currOffset_ + idx < joinRes_->items_.Size());
		return const_cast<reference>(joinRes_->items_.GetItemRef(currOffset_ + idx));
	}
	JoinedFieldIterator& operator++() noexcept {
		++order_;
		updateOffset();
		return *this;
	}

	ItemImpl GetItem(int itemIdx, const PayloadType& pt, const TagsMatcher& tm) const;
	// TODO: This function returns LocalQueryResults without namespace context. So, user have to call qr.addNSContext() to be able to get
	// any items/jsons. This API should be improved in #2273
	LocalQueryResults ToQueryResults() const;

	int ItemsCount() const noexcept;

private:
	void updateOffset() noexcept;
	const NamespaceResults* joinRes_ = nullptr;
	const ItemOffsets* offsets_ = nullptr;
	uint8_t order_ = 0;
	int currField_ = 0;
	uint32_t currOffset_ = 0;
};

/// Left namespace (main ns) iterator.
/// Iterates over joined fields (if there are some) of item.
class [[nodiscard]] ItemIterator {
public:
	ItemIterator(const NamespaceResults* parent, IdType rowid) noexcept : joinRes_(parent), rowid_(rowid) {}

	JoinedFieldIterator at(uint8_t joinedField) const;
	JoinedFieldIterator begin() const noexcept;
	JoinedFieldIterator end() const noexcept;

	int getJoinedFieldsCount() const noexcept { return joinRes_->GetJoinedSelectorsCount(); }
	int getJoinedItemsCount() const noexcept;

	static ItemIterator CreateFrom(const LocalQueryResults::ConstIterator& it) noexcept;
	static ItemIterator CreateEmpty() noexcept;

private:
	const NamespaceResults* joinRes_;
	const IdType rowid_;
	mutable int joinedItemsCount_ = -1;
};

}  // namespace joins
}  // namespace reindexer
