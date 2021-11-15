#include "transactionimpl.h"
#include "item.h"
#include "itemimpl.h"

namespace reindexer {

using std::string;

void TransactionImpl::updateTagsMatcherIfNecessary(Item &item) {
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
	auto &data = std::get<TransactionItemStep>(st.data_);
	return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_, schema_, std::move(data.data)));
}

TransactionImpl::TransactionImpl(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
								 std::shared_ptr<const Schema> schema, lsn_t lsn)
	: payloadType_(pt),
	  tagsMatcher_(tm),
	  pkFields_(pf),
	  schema_(std::move(schema)),
	  nsName_(nsName),
	  tagsUpdated_(false),
	  startTime_(std::chrono::high_resolution_clock::now()),
	  lsn_(lsn) {}

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

void TransactionImpl::Insert(Item &&item, lsn_t lsn) {
	std::unique_lock<std::mutex> lock(mtx_);
	updateTagsMatcherIfNecessary(item);
	steps_.emplace_back(TransactionStep{move(item), ModeInsert, lsn});
}
void TransactionImpl::Update(Item &&item, lsn_t lsn) {
	std::unique_lock<std::mutex> lock(mtx_);
	updateTagsMatcherIfNecessary(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpdate, lsn});
}
void TransactionImpl::Upsert(Item &&item, lsn_t lsn) {
	std::unique_lock<std::mutex> lock(mtx_);
	updateTagsMatcherIfNecessary(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpsert, lsn});
}
void TransactionImpl::Delete(Item &&item, lsn_t lsn) {
	std::unique_lock<std::mutex> lock(mtx_);
	updateTagsMatcherIfNecessary(item);
	steps_.emplace_back(TransactionStep{move(item), ModeDelete, lsn});
}
void TransactionImpl::Modify(Item &&item, ItemModifyMode mode, lsn_t lsn) {
	std::unique_lock<std::mutex> lock(mtx_);
	updateTagsMatcherIfNecessary(item);
	steps_.emplace_back(TransactionStep{move(item), mode, lsn});
}

void TransactionImpl::Modify(Query &&query, lsn_t lsn) {
	std::unique_lock<std::mutex> lock(mtx_);
	steps_.emplace_back(TransactionStep(std::move(query), lsn));
}

void TransactionImpl::Nop(lsn_t lsn) {
	std::unique_lock<std::mutex> lock(mtx_);
	steps_.emplace_back(TransactionStep{lsn});
}

void TransactionImpl::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (key.empty()) {
		throw Error(errLogic, "Empty meta key is not allowed in tx");
	}
	std::unique_lock<std::mutex> lock(mtx_);
	steps_.emplace_back(TransactionStep{key, value, lsn});
}

}  // namespace reindexer
