#include "client/synccorotransaction.h"
#include "client/synccororeindexerimpl.h"
#include "core/cjson/tagsmatcher.h"

namespace reindexer {
namespace client {

static const auto kBadTxStatus = Error(errBadTransaction, "Transaction is free");

Item SyncCoroTransaction::NewItem() {
	if (!Status().ok()) {
		return Item(Status());
	}
	if (!IsFree()) {
		return rx_->newItemTx(tr_);
	}
	return Item(kBadTxStatus);
}

SyncCoroTransaction::~SyncCoroTransaction() {
	if (!IsFree()) {
		rx_->RollBackTransaction(*this, InternalRdxContext());
	}
	tr_.clear();
}

PayloadType SyncCoroTransaction::GetPayloadType() const { return tr_.GetPayloadType(); }
TagsMatcher SyncCoroTransaction::GetTagsMatcher() const { return tr_.GetTagsMatcher(); }

int64_t SyncCoroTransaction::GetTransactionId() const noexcept { return tr_.i_.txId_; }

Error SyncCoroTransaction::modify(Item&& item, ItemModifyMode mode, lsn_t lsn, SyncCoroTransaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->addTxItem(*this, std::move(item), mode, lsn, std::move(asyncCmpl));
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error SyncCoroTransaction::modify(Query&& query, lsn_t lsn, SyncCoroTransaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->modifyTx(*this, std::move(query), lsn, std::move(asyncCmpl));
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error SyncCoroTransaction::putMeta(std::string_view key, std::string_view value, lsn_t lsn, SyncCoroTransaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->putTxMeta(*this, key, value, lsn, std::move(asyncCmpl));
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error SyncCoroTransaction::setTagsMatcher(TagsMatcher&& tm, lsn_t lsn, SyncCoroTransaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->setTxTm(*this, std::move(tm), lsn, std::move(asyncCmpl));
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

}  // namespace client
}  // namespace reindexer
