#pragma once

#include <vector>
#include "core/indexopts.h"
#include "core/itemimpl.h"
#include "estl/fast_hash_map.h"
#include "itemref.h"
#include "localqueryresults.h"

namespace reindexer {

class QueryResults;
class PayloadType;
class TagsMatcher;
class FieldsSet;

namespace joins {

class NamespaceResults;

/// Offset in 'items_' for left Ns item
struct ItemOffset {
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
class NamespaceResults {
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
	size_t TotalItems() const noexcept { return items_.size(); }

	/// Clear all internal data
	void Clear() {
		offsets_.clear();
		items_.clear();
		joinedSelectorsCount_ = 0;
	}

private:
	friend class ItemIterator;
	friend class JoinedFieldIterator;
	/// Offsets in 'result' for every item
	fast_hash_map<IdType, ItemOffsets> offsets_;
	/// Items for all the joined fields
	ItemRefVector items_;
	/// Amount of joined selectors for this NS
	uint32_t joinedSelectorsCount_ = 0;
};

/// Results of joining all the namespaces (in case of merge queries)
class Results : public std::vector<NamespaceResults> {};

/// Joined field iterator for Item
/// of left NamespaceImpl (main ns).
class JoinedFieldIterator {
public:
	using reference = ItemRef&;
	using const_reference = const ItemRef&;

	JoinedFieldIterator(const NamespaceResults* parent, const ItemOffsets& offsets, uint8_t joinedFieldOrder) noexcept
		: joinRes_(parent), offsets_(&offsets), order_(joinedFieldOrder) {
		if (offsets_->size() > 0) updateOffset();
	}

	bool operator==(const JoinedFieldIterator& other) const;
	bool operator!=(const JoinedFieldIterator& other) const { return !operator==(other); }

	const_reference operator[](size_t idx) const noexcept {
		assertrx(currOffset_ + idx < joinRes_->items_.size());
		return joinRes_->items_[currOffset_ + idx];
	}
	reference operator[](size_t idx) noexcept {
		assertrx(currOffset_ + idx < joinRes_->items_.size());
		return const_cast<reference>(joinRes_->items_[currOffset_ + idx]);
	}
	JoinedFieldIterator& operator++() noexcept {
		++order_;
		updateOffset();
		return *this;
	}

	ItemImpl GetItem(int itemIdx, const PayloadType& pt, const TagsMatcher& tm) const;
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
class ItemIterator {
public:
	ItemIterator(const NamespaceResults* parent, IdType rowid) noexcept : joinRes_(parent), rowid_(rowid) {}

	JoinedFieldIterator at(uint8_t joinedField) const;
	JoinedFieldIterator begin() const noexcept;
	JoinedFieldIterator end() const noexcept;

	int getJoinedFieldsCount() const noexcept { return joinRes_->GetJoinedSelectorsCount(); }
	int getJoinedItemsCount() const noexcept;

	static ItemIterator CreateFrom(const LocalQueryResults::Iterator& it) noexcept;
	static ItemIterator CreateEmpty() noexcept;

private:
	const NamespaceResults* joinRes_;
	const IdType rowid_;
	mutable int joinedItemsCount_ = -1;
};

}  // namespace joins
}  // namespace reindexer
