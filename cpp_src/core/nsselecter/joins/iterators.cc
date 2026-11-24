#include "iterators.h"
#include "core/cjson/tagsmatcher.h"
#include "core/nsselecter/joins/results.h"
#include "item_context.h"

namespace reindexer::joins {
namespace {
const ItemsOffsets kEmptyOffsets;
const FieldIterator kNoJoinedDataIt{nullptr, kEmptyOffsets, 0};
const NamespaceResults kEmptyNamespaceResults;
}  // namespace

bool FieldIterator::operator==(const FieldIterator& other) const {
	if (nsRes_ != other.nsRes_) {
		throw Error(errLogic, "Comparising joined fields of different namespaces!");
	}
	if (offsets_ != other.offsets_) {
		throw Error(errLogic, "Comparising joined fields of different items!");
	}
	return (field_ == other.field_);
}

ItemImpl FieldIterator::GetItem(int itemIndex, const PayloadType& pt, const TagsMatcher& tm) const {
	const_reference constItemRef{operator[](itemIndex)};
	return ItemImpl(pt, constItemRef.Value(), tm);
}

const ItemRefRanked& FieldIterator::GetItemRefRanked(int itemIndex) const {
	assertrx_throw(nsRes_);
	assertrx_throw(offset_ + itemIndex < nsRes_->items_.Size());
	return nsRes_->items_.GetItemRefRanked(offset_ + itemIndex);
}

LocalQueryResults FieldIterator::ToQueryResults() const {
	if (ItemsCount() == 0) {
		return {};
	}
	const auto begin{nsRes_->items_.begin() + offset_};
	const auto end{begin + ItemsCount()};
	return LocalQueryResults{begin, end};
}

LocalQueryResults FieldIterator::ToQueryResults(JoinedItemContext& leftItemCtx) const {
	if (ItemsCount() == 0) {
		return {};
	}
	const auto begin{nsRes_->items_.begin() + offset_};
	const auto end{begin + ItemsCount()};
	LocalQueryResults qr{begin, end};
	for (const auto& ctx : leftItemCtx.contexts) {
		qr.addNSContext(ctx.pt, ctx.tm, ctx.filter, ctx.schema, lsn_t{});
	}
	qr.SetJoined(*leftItemCtx.results);
	return qr;
}

int FieldIterator::ItemsCount() const {
	if (!nsRes_ || !offsets_) {
		return 0;
	}
	if (field_ < nsRes_->GetFieldsCount()) {
		assertrx_throw(size_t(field_) < offsets_->size());
		return (*offsets_)[field_].count;
	}
	return 0;
}

void FieldIterator::updateOffsets() noexcept {
	if (nsRes_ && field_ < nsRes_->GetFieldsCount()) {
		offset_ = (*offsets_)[field_].offset;
	}
}

FieldIterator ItemIterator::createFieldIterator(uint8_t joinedField) const {
	assertrx_throw(nsRes_);
	if (!offsetsPtr_ || offsetsPtr_->empty()) {
		return kNoJoinedDataIt;
	}
	return FieldIterator(nsRes_, *offsetsPtr_, joinedField);
}

FieldIterator ItemIterator::Begin() const { return createFieldIterator(0); }

FieldIterator ItemIterator::At(uint8_t field) const {
	assertrx_throw(nsRes_);
	assertrx_throw(field < nsRes_->GetFieldsCount());
	return createFieldIterator(field);
}

FieldIterator ItemIterator::End() const {
	assertrx_throw(nsRes_);
	return createFieldIterator(nsRes_->GetFieldsCount());
}

int ItemIterator::GetItemsCount() const noexcept {
	if (!joinedItemsCount_.has_value()) {
		joinedItemsCount_ = 0;
		if (offsetsPtr_) {
			for (const auto& offset : *offsetsPtr_) {
				*joinedItemsCount_ += offset.count;
			}
		}
	}
	return *joinedItemsCount_;
}

ItemIterator ItemIterator::CreateFrom(const LocalQueryResults::ConstIterator& it) noexcept {
	auto& itemRef{it.GetItemRef()};
	if ((itemRef.Nsid() >= it.Owner()->Joined().size())) {
		return ItemIterator::CreateEmpty();
	}
	return ItemIterator{&(it.Owner()->Joined()[itemRef.Nsid()]), itemRef.Id()};
}

ItemIterator ItemIterator::CreateEmpty() noexcept { return ItemIterator{&kEmptyNamespaceResults, IdType::Zero()}; }

}  // namespace reindexer::joins
