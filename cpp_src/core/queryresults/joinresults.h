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
	ItemOffset(size_t field_, int offset_, int size_) noexcept : field(uint8_t(field_)), offset(offset_), size(size_) {}
	bool operator==(const ItemOffset& other) const noexcept { return field == other.field && offset == other.offset && size == other.size; }
	bool operator!=(const ItemOffset& other) const noexcept { return !operator==(other); }
	/// index of joined field
	/// (equals to position in joinedSelectors_)
	unsigned field : 8;
	/// Offset of items in 'items_' container
	unsigned int offset : 24;
	/// Amount of joined items for this field
	uint32_t size = 0;
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
	void Insert(IdType rowid, size_t fieldIdx, LocalQueryResults&& qr);

	/// Gets/sets amount of joined selectors
	/// @param joinedSelectorsCount - joinedSelectors.size()
	void SetJoinedSelectorsCount(int joinedSelectorsCount) noexcept { joinedSelectorsCount_ = joinedSelectorsCount; }
	int GetJoinedSelectorsCount() const noexcept { return joinedSelectorsCount_; }

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
	uint8_t joinedSelectorsCount_ = 0;
};

/// Results of joining all the namespaces (in case of merge queries)
class Results : public h_vector<NamespaceResults, 0> {};

/// Joined field iterator for Item
/// of left NamespaceImpl (main ns).
class JoinedFieldIterator {
public:
	using reference = ItemRef&;
	using const_reference = const ItemRef&;

	JoinedFieldIterator(const NamespaceResults* parent, const ItemOffsets& offsets, uint8_t joinedFieldOrder);

	bool operator==(const JoinedFieldIterator& other) const;
	bool operator!=(const JoinedFieldIterator& other) const;

	const_reference operator[](size_t idx) const;
	reference operator[](size_t idx);
	JoinedFieldIterator& operator++();

	ItemImpl GetItem(int itemIdx, const PayloadType& pt, const TagsMatcher& tm) const;
	LocalQueryResults ToQueryResults() const;

	int ItemsCount() const;

private:
	void updateOffset();
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
	JoinedFieldIterator begin() const;
	JoinedFieldIterator end() const;

	int getJoinedFieldsCount() const;
	int getJoinedItemsCount() const;

	static ItemIterator CreateFrom(LocalQueryResults::Iterator it);
	static ItemIterator CreateEmpty();

private:
	const NamespaceResults* joinRes_;
	const IdType rowid_;
	mutable int joinedItemsCount_ = -1;
};

}  // namespace joins
}  // namespace reindexer
