#include "transactionimpl.h"
#include "item.h"
#include "itemimpl.h"

namespace reindexer {

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
	return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_, schema_, std::move(st.itemData_)));
}

void TransactionImpl::ValidatePK(const FieldsSet &pkFields) {
	std::lock_guard lck(mtx_);
	if (hasDeleteItemSteps_ && rx_unlikely(pkFields != pkFields_)) {
		throw Error(
			errNotValid,
			"Transaction has Delete-calls and it's PK metadata is outdated (probably PK has been change during the transaction creation)");
	}
}

void TransactionImpl::UpdateTagsMatcherFromItem(ItemImpl *ritem) {
	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		std::string jsonSliceBuf(ritem->GetJSON());

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
	steps_.emplace_back(TransactionStep{std::move(item), ModeInsert});
}
void TransactionImpl::Update(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{std::move(item), ModeUpdate});
}
void TransactionImpl::Upsert(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{std::move(item), ModeUpsert});
}
void TransactionImpl::Delete(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{std::move(item), ModeDelete});
}
void TransactionImpl::Modify(Item &&item, ItemModifyMode mode) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	hasDeleteItemSteps_ = hasDeleteItemSteps_ || (mode == ModeDelete);
	steps_.emplace_back(TransactionStep{std::move(item), mode});
}

void TransactionImpl::Modify(Query &&query) {
	std::unique_lock<std::mutex> lock(mtx_);
	steps_.emplace_back(TransactionStep(std::move(query)));
}

}  // namespace reindexer
