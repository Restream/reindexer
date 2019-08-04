#include "transactionimpl.h"
#include "reindexerimpl.h"
namespace reindexer {

using std::string;

TransactionAccessor::TransactionAccessor(TransactionAccessor &&rhs) noexcept : Transaction(std::move(rhs)) {}
TransactionAccessor &TransactionAccessor::operator=(TransactionAccessor &&rhs) noexcept {
	Transaction::operator=(std::move(rhs));
	return *this;
}
const string &TransactionAccessor::GetName() { return impl_->nsName_; }

Completion TransactionAccessor::GetCmpl() { return impl_->cmpl_; }

vector<TransactionStep> &TransactionAccessor::GetSteps() { return impl_->steps_; }

void TransactionImpl::checkTagsMatcher(Item &item) {
	if (item.IsTagsUpdated()) {
		ItemImpl *ritem = item.impl_;
		UpdateTagsMatcherFromItem(ritem);
	}
}

Item TransactionImpl::NewItem() { return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_)); }

TransactionImpl::TransactionImpl(const string &nsName, ReindexerImpl *rx, Completion cmpl) : nsName_(nsName), cmpl_(cmpl), rx_(rx) {
	static const RdxContext dummyCtx;
	auto nsPtr_ = rx_->getClonableNamespace(nsName_, dummyCtx);
	Namespace::RLock lck(nsPtr_->mtx_, &dummyCtx);
	payloadType_ = nsPtr_->payloadType_;
	tagsMatcher_ = nsPtr_->tagsMatcher_;
	pkFields_ = nsPtr_->pkFields();
}

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
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeInsert});
}
void TransactionImpl::Update(Item &&item) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpdate});
}
void TransactionImpl::Upsert(Item &&item) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpsert});
}
void TransactionImpl::Delete(Item &&item) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeDelete});
}
void TransactionImpl::Modify(Item &&item, ItemModifyMode mode) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), mode});
}

}  // namespace reindexer
