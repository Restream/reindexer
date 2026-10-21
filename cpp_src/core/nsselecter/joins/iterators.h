#pragma once

#include "core/itemimpl.h"
#include "core/queryresults/localqueryresults.h"
#include "results.h"

namespace reindexer::joins {

class ItemIterator;
struct JoinedItemContext;

/// Joined field iterator for Item of left Namespace.
class [[nodiscard]] FieldIterator {
public:
	using reference = ItemRef&;
	using const_reference = const ItemRef&;

	FieldIterator(const NamespaceResults* parent, const ItemsOffsets& offsets, uint8_t joinedField) noexcept
		: nsRes_(parent), offsets_(&offsets), field_(joinedField) {
		updateOffsets();
	}

	FieldIterator(const FieldIterator&) = default;
	FieldIterator(FieldIterator&&) = default;
	FieldIterator& operator=(const FieldIterator&) = default;
	FieldIterator& operator=(FieldIterator&&) = default;

	bool operator==(const FieldIterator& other) const;
	bool operator!=(const FieldIterator& other) const { return !operator==(other); }

	const_reference operator[](size_t itemIndex) const noexcept {
		assertrx(nsRes_);
		assertrx(offset_ + itemIndex < nsRes_->items_.Size());
		return nsRes_->items_.GetItemRef(offset_ + itemIndex);
	}

	reference operator[](size_t itemIndex) noexcept {
		return const_cast<reference>(static_cast<const FieldIterator*>(this)->operator[](itemIndex));
	}

	FieldIterator& operator++() noexcept {
		assertrx(nsRes_);
		++field_;
		updateOffsets();
		return *this;
	}

	ItemImpl GetItem(int itemIndex, const PayloadType& pt, const TagsMatcher& tm) const;
	const ItemRefRanked& GetItemRefRanked(int itemIndex) const;

	/// This function returns LocalQueryResults without namespace context.
	/// A user should call qr.addNSContext() to be able to get items/jsons.
	LocalQueryResults ToQueryResults() const;

	///	Creates and returns LocalQueryResults for joined
	/// items with appropriate Context from JoinedItemContext.
	/// @param leftItemCtx - JoinedItemContext of left NS item.
	/// @returns LocalQueryResults object with all nested items.
	LocalQueryResults ToQueryResults(JoinedItemContext& leftItemCtx) const;

	int ItemsCount() const;

private:
	void updateOffsets() noexcept;

	const NamespaceResults* nsRes_{nullptr};
	const ItemsOffsets* offsets_{nullptr};
	uint8_t field_{0};
	uint32_t offset_{0};
};

/// Left namespace Item iterator.
/// Iterates over joined fields of item.
class [[nodiscard]] ItemIterator {
public:
	ItemIterator(const NamespaceResults* parent, IdType rowid) noexcept : nsRes_(parent), rowid_(rowid) {
		if (nsRes_) {
			auto it{nsRes_->offsets_.find(rowid_)};
			if (it != nsRes_->offsets_.end()) {
				offsetsPtr_ = &it->second;
			}
		}
	}

	ItemIterator(const ItemIterator&) = default;
	ItemIterator(ItemIterator&&) = default;
	ItemIterator& operator=(const ItemIterator&) = delete;
	ItemIterator& operator=(ItemIterator&&) = delete;

	FieldIterator At(uint8_t field) const;
	FieldIterator Begin() const;
	FieldIterator End() const;

	int GetFieldsCount() const noexcept { return nsRes_->GetFieldsCount(); }
	int GetItemsCount() const noexcept;

	static ItemIterator CreateFrom(const LocalQueryResults::ConstIterator& it) noexcept;
	static ItemIterator CreateEmpty() noexcept;

private:
	FieldIterator createFieldIterator(uint8_t joinedField) const;

	const NamespaceResults* nsRes_;
	const IdType rowid_;
	const ItemsOffsets* offsetsPtr_{nullptr};
	mutable std::optional<int> joinedItemsCount_;
};

}  // namespace reindexer::joins
