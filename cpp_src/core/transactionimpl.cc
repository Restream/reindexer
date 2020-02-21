#include "transactionimpl.h"
#include "item.h"
#include "itemimpl.h"

namespace reindexer {

using std::string;

void TransactionImpl::checkTagsMatcher(Item &item) {
	if (item.IsTagsUpdated()) {
		ItemImpl *ritem = item.impl_;
		UpdateTagsMatcherFromItem(ritem);
		tagsUpdated_ = true;
	}
}

Item TransactionImpl::NewItem() {
	std::unique_lock<std::mutex> lock(mtx_);
	return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_));
}
Item TransactionImpl::GetItem(TransactionStep &&st) {
	std::unique_lock<std::mutex> lock(mtx_);
	return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_, std::move(st.itemData_)));
}

TransactionImpl::TransactionImpl(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf)
	: payloadType_(pt),
	  tagsMatcher_(tm),
	  pkFields_(pf),
	  nsName_(nsName),
	  tagsUpdated_(false),
	  startTime_(std::chrono::high_resolution_clock::now()) {}

void TransactionImpl::UpdateTagsMatcherFromItem(ItemImpl *ritem) {
	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		string jsonSliceBuf(ritem->GetJSON());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Value().SetLSN(ritem->Value().GetLSN());
		*ritem = std::move(tmpItem);

		auto err = ritem->FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) throw err;

		if (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))
			throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
	if (ritem->tagsMatcher().isUpdated()) {
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
}

void TransactionImpl::Insert(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeInsert});
}
void TransactionImpl::Update(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpdate});
}
void TransactionImpl::Upsert(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpsert});
}
void TransactionImpl::Delete(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeDelete});
}
void TransactionImpl::Modify(Item &&item, ItemModifyMode mode) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), mode});
}

void TransactionImpl::Modify(Query &&query) {
	std::unique_lock<std::mutex> lock(mtx_);
	steps_.emplace_back(TransactionStep(std::move(query)));
}

}  // namespace reindexer
