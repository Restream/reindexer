#include "joinresults.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadiface.h"
#include "queryresults.h"

#include <numeric>

namespace reindexer {
namespace joins {

JoinedFieldIterator::JoinedFieldIterator(const NamespaceResults* parent, const ItemOffsets& offsets, uint8_t joinedFieldOrder)
	: joinRes_(parent), offsets_(&offsets), order_(joinedFieldOrder) {
	if (offsets_->size() > 0) updateOffset();
}

bool JoinedFieldIterator::operator==(const JoinedFieldIterator& other) const {
	if (joinRes_ != other.joinRes_) throw Error(errLogic, "Comparising joined fields of different namespaces!");
	if (offsets_ != other.offsets_) throw Error(errLogic, "Comparising joined fields of different items!");
	if (order_ != other.order_) return false;
	return true;
}

bool JoinedFieldIterator::operator!=(const JoinedFieldIterator& other) const { return !operator==(other); }

JoinedFieldIterator::const_reference JoinedFieldIterator::operator[](size_t idx) const {
	assert(currOffset_ + idx < joinRes_->items_.size());
	return joinRes_->items_[currOffset_ + idx];
}

JoinedFieldIterator::reference JoinedFieldIterator::operator[](size_t idx) {
	assert(currOffset_ + idx < joinRes_->items_.size());
	return const_cast<reference>(joinRes_->items_[currOffset_ + idx]);
}

JoinedFieldIterator& JoinedFieldIterator::operator++() {
	++order_;
	updateOffset();
	return *this;
}

void JoinedFieldIterator::updateOffset() {
	currField_ = -1;
	if (order_ == joinRes_->GetJoinedSelectorsCount()) return;

	size_t i = 0;
	for (; i < offsets_->size(); ++i) {
		if (int(order_) == (*offsets_)[i].field) {
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

QueryResults JoinedFieldIterator::ToQueryResults() const {
	if (ItemsCount() == 0) return QueryResults();
	ItemRefVector::const_iterator begin = joinRes_->items_.begin() + currOffset_;
	ItemRefVector::const_iterator end = begin + ItemsCount();
	return QueryResults(begin, end);
}

int JoinedFieldIterator::ItemsCount() const {
	assert(order_ < joinRes_->GetJoinedSelectorsCount());

	if ((currField_ != -1) && (currField_ < uint8_t(offsets_->size()))) {
		return (*offsets_)[currField_].size;
	}

	return 0;
}

const JoinedFieldIterator noJoinedDataIt(nullptr, 0, 0);

ItemIterator::ItemIterator(const NamespaceResults* parent, IdType rowid) : joinRes_(parent), rowid_(rowid) {}

JoinedFieldIterator ItemIterator::begin() const {
	auto it = joinRes_->offsets_.find(rowid_);
	if (it == joinRes_->offsets_.end()) return noJoinedDataIt;
	if (it->second.empty()) return noJoinedDataIt;
	return JoinedFieldIterator(joinRes_, it->second, 0);
}

JoinedFieldIterator ItemIterator::at(uint8_t joinedField) const {
	auto it = joinRes_->offsets_.find(rowid_);
	if (it == joinRes_->offsets_.end()) return noJoinedDataIt;
	if (it->second.empty()) return noJoinedDataIt;
	assert(joinedField < joinRes_->GetJoinedSelectorsCount());
	return JoinedFieldIterator(joinRes_, it->second, joinedField);
}

JoinedFieldIterator ItemIterator::end() const {
	auto it = joinRes_->offsets_.find(rowid_);
	if (it == joinRes_->offsets_.end()) return noJoinedDataIt;
	if (it->second.empty()) return noJoinedDataIt;
	return JoinedFieldIterator(joinRes_, it->second, joinRes_->GetJoinedSelectorsCount());
}

int ItemIterator::getJoinedFieldsCount() const { return joinRes_->GetJoinedSelectorsCount(); }

int ItemIterator::getJoinedItemsCount() const {
	if (joinedItemsCount_ == -1) {
		joinedItemsCount_ = 0;
		auto it = joinRes_->offsets_.find(rowid_);
		if (it != joinRes_->offsets_.end()) {
			const ItemOffsets& offsets = it->second;
			for (size_t i = 0; i < offsets.size(); ++i) joinedItemsCount_ += offsets[i].size;
		}
	}
	return joinedItemsCount_;
}

ItemIterator ItemIterator::FromQRIterator(QueryResults::Iterator it) {
	static NamespaceResults empty;
	static ItemIterator ret(&empty, 0);
	auto& itemRef = it.qr_->Items()[it.idx_];
	if ((itemRef.Nsid() >= it.qr_->joined_.size())) return ret;
	return ItemIterator(&(it.qr_->joined_[itemRef.Nsid()]), itemRef.Id());
}

ItemOffset::ItemOffset() : field(0), offset(0), size(0) {}
ItemOffset::ItemOffset(size_t f, int o, int s) : field(uint8_t(f)), offset(o), size(s) {}
bool ItemOffset::operator==(const ItemOffset& other) const {
	if (field != other.field) return false;
	if (offset != other.offset) return false;
	if (size != other.size) return false;
	return true;
}
bool ItemOffset::operator!=(const ItemOffset& other) const { return !operator==(other); }

void NamespaceResults::Insert(IdType rowid, size_t fieldIdx, QueryResults&& qr) {
	assert(int(fieldIdx) < joinedSelectorsCount_);
	ItemOffsets& offsets = offsets_[rowid];
	if (offsets.empty()) {
		offsets.reserve(joinedSelectorsCount_);
	}
	offsets.emplace_back(ItemOffset(fieldIdx, items_.size(), qr.Count()));
	items_.insert(items_.end(), std::make_move_iterator(qr.Items().begin()), std::make_move_iterator(qr.Items().end()));
}

void NamespaceResults::SetJoinedSelectorsCount(int joinedSelectorsCount) { joinedSelectorsCount_ = joinedSelectorsCount; }
int NamespaceResults::GetJoinedSelectorsCount() const { return joinedSelectorsCount_; }

size_t NamespaceResults::TotalItems() const { return items_.size(); }

}  // namespace joins
}  // namespace reindexer
