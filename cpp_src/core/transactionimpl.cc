#include "transactionimpl.h"
#include "item.h"
#include "itemimpl.h"

namespace reindexer {

void TransactionImpl::checkTagsMatcher(Item &item) {
	if (item.IsTagsUpdated()) {
		ItemImpl *ritem = item.impl_;
		if (ritem->Type().get() != payloadType_.get() || !tagsMatcher_.try_merge(ritem->tagsMatcher())) {
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
		} else {
			ritem->tagsMatcher() = tagsMatcher_;
			ritem->tagsMatcher().setUpdated();
		}
		tagsUpdated_ = true;
	}
}

Item TransactionImpl::NewItem() {
	std::unique_lock<std::mutex> lock(mtx_);
	Item item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_));
	item.impl_->tagsMatcher().clearUpdated();
	return item;
}

Item TransactionImpl::GetItem(TransactionStep &&st) {
	std::unique_lock<std::mutex> lock(mtx_);
	auto &data = std::get<TransactionItemStep>(st.data_);
	auto item = Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_, schema_, std::move(data.data)));
	data.hadTmUpdate ? item.impl_->tagsMatcher().setUpdated() : item.impl_->tagsMatcher().clearUpdated();
	return item;
}

void TransactionImpl::ValidatePK(const FieldsSet &pkFields) {
	std::lock_guard lck(mtx_);
	if (hasDeleteItemSteps_ && rx_unlikely(pkFields != pkFields_)) {
		throw Error(
			errNotValid,
			"Transaction has Delete-calls and it's PK metadata is outdated (probably PK has been change during the transaction creation)");
	}
}

void TransactionImpl::Insert(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(std::move(item), ModeInsert);
}
void TransactionImpl::Update(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(std::move(item), ModeUpdate);
}
void TransactionImpl::Upsert(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(std::move(item), ModeUpsert);
}
void TransactionImpl::Delete(Item &&item) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	steps_.emplace_back(std::move(item), ModeDelete);
}
void TransactionImpl::Modify(Item &&item, ItemModifyMode mode) {
	std::unique_lock<std::mutex> lock(mtx_);
	checkTagsMatcher(item);
	hasDeleteItemSteps_ = hasDeleteItemSteps_ || (mode == ModeDelete);
	steps_.emplace_back(std::move(item), mode);
}

void TransactionImpl::Modify(Query &&query) {
	std::unique_lock<std::mutex> lock(mtx_);
	steps_.emplace_back(std::move(query));
}

void TransactionImpl::PutMeta(std::string_view key, std::string_view value) {
	if (key.empty()) {
		throw Error(errLogic, "Empty meta key is not allowed in tx");
	}

	std::lock_guard lock(mtx_);
	steps_.emplace_back(key, value);
}

void TransactionImpl::SetTagsMatcher(TagsMatcher &&tm) {
	std::lock_guard lock(mtx_);
	// NOTE: In v4 tm tokens here are always the same, but in v3 those tokens are not synchronized. Probably it should workd anyway
	//	if (tm.stateToken() != tagsMatcher_.stateToken()) {
	//		throw Error(errParams, "Tx tm statetoken missmatch: %08X vs %08X", tagsMatcher_.stateToken(), tm.stateToken());
	//	}
	tagsMatcher_ = tm;
	tagsMatcher_.UpdatePayloadType(payloadType_, false);
	tagsUpdated_ = true;
	steps_.emplace_back(std::move(tm));
}

}  // namespace reindexer
