#include "joinresults.h"
#include "core/cjson/tagsmatcher.h"

namespace reindexer::joins {

bool JoinedFieldIterator::operator==(const JoinedFieldIterator& other) const {
	if (joinRes_ != other.joinRes_) {
		throw Error(errLogic, "Comparising joined fields of different namespaces!");
	}
	if (offsets_ != other.offsets_) {
		throw Error(errLogic, "Comparising joined fields of different items!");
	}
	return (order_ == other.order_);
}

void JoinedFieldIterator::updateOffset() noexcept {
	currField_ = -1;
	if (order_ == joinRes_->GetJoinedSelectorsCount()) {
		return;
	}

	size_t i = 0;
	for (; i < offsets_->size(); ++i) {
		if (order_ == (*offsets_)[i].field) {
			currOffset_ = (*offsets_)[i].offset;
			break;
		}
	}
	if (i < offsets_->size()) {
		currField_ = i;
	}
}

ItemImpl JoinedFieldIterator::GetItem(int itemIdx, const PayloadType& pt, const TagsMatcher& tm) const {
	const_reference constItemRef = operator[](itemIdx);
	return ItemImpl(pt, constItemRef.Value(), tm);
}

LocalQueryResults JoinedFieldIterator::ToQueryResults() const {
	if (ItemsCount() == 0) {
		return LocalQueryResults();
	}
	const auto begin = joinRes_->items_.begin() + currOffset_;
	const auto end = begin + ItemsCount();
	return LocalQueryResults(begin, end);
}

int JoinedFieldIterator::ItemsCount() const noexcept {
	if (!joinRes_ || !offsets_) {
		return 0;
	}
	assertrx(order_ < joinRes_->GetJoinedSelectorsCount());

	if ((currField_ != -1) && (currField_ < uint8_t(offsets_->size()))) {
		return (*offsets_)[currField_].size;
	}

	return 0;
}

static const ItemOffsets kEmptyOffsets;
static const JoinedFieldIterator kNoJoinedDataIt(nullptr, kEmptyOffsets, 0);

JoinedFieldIterator ItemIterator::begin() const noexcept {
	auto it = joinRes_->offsets_.find(rowid_);
	if (it == joinRes_->offsets_.end() || it->second.empty()) {
		return kNoJoinedDataIt;
	}
	return JoinedFieldIterator(joinRes_, it->second, 0);
}

JoinedFieldIterator ItemIterator::at(uint8_t joinedField) const {
	auto it = joinRes_->offsets_.find(rowid_);
	if (it == joinRes_->offsets_.end() || it->second.empty()) {
		return kNoJoinedDataIt;
	}
	assertrx_throw(joinedField < joinRes_->GetJoinedSelectorsCount());
	return JoinedFieldIterator(joinRes_, it->second, joinedField);
}

JoinedFieldIterator ItemIterator::end() const noexcept {
	auto it = joinRes_->offsets_.find(rowid_);
	if (it == joinRes_->offsets_.end() || it->second.empty()) {
		return kNoJoinedDataIt;
	}
	return JoinedFieldIterator(joinRes_, it->second, joinRes_->GetJoinedSelectorsCount());
}

int ItemIterator::getJoinedItemsCount() const noexcept {
	if (joinedItemsCount_ == -1) {
		joinedItemsCount_ = 0;
		auto it = joinRes_->offsets_.find(rowid_);
		if (it != joinRes_->offsets_.end()) {
			const ItemOffsets& offsets = it->second;
			for (auto& offset : offsets) {
				joinedItemsCount_ += offset.size;
			}
		}
	}
	return joinedItemsCount_;
}

ItemIterator ItemIterator::CreateFrom(const LocalQueryResults::ConstIterator& it) noexcept {
	auto ret = ItemIterator::CreateEmpty();
	auto& itemRef = it.GetItemRef();
	if ((itemRef.Nsid() >= it.Owner()->joined_.size())) {
		return ret;
	}
	return ItemIterator(&(it.Owner()->joined_[itemRef.Nsid()]), itemRef.Id());
}

const static NamespaceResults kEmptyNamespaceResults;

ItemIterator ItemIterator::CreateEmpty() noexcept { return ItemIterator{&kEmptyNamespaceResults, 0}; }

void NamespaceResults::Insert(IdType rowid, uint32_t fieldIdx, LocalQueryResults&& qr) {
	assertrx_throw(fieldIdx < joinedSelectorsCount_);
	ItemOffsets& offsets = offsets_[rowid];
	if (offsets.empty()) {
		offsets.reserve(joinedSelectorsCount_);
	}
	offsets.emplace_back(fieldIdx, items_.Size(), qr.Count());
	items_.Insert(items_.end(), std::move(qr.Items()).mbegin(), std::move(qr.Items()).mend());
}

}  // namespace reindexer::joins
