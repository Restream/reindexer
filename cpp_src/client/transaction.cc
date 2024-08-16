#include "client/transaction.h"
#include "client/reindexerimpl.h"
#include "core/cjson/tagsmatcher.h"

namespace reindexer {
namespace client {

static const auto kBadTxStatus = Error(errBadTransaction, "Transaction is free");

Item Transaction::NewItem() {
	if (!Status().ok()) {
		return Item(Status());
	}
	if (!IsFree()) {
		return rx_->newItemTx(tr_);
	}
	return Item(kBadTxStatus);
}

Transaction::~Transaction() {
	if (!IsFree()) {
		auto err = rx_->RollBackTransaction(*this, InternalRdxContext());
		(void)err;	// ignore
	}
	tr_.clear();
}

PayloadType Transaction::GetPayloadType() const { return tr_.GetPayloadType(); }
TagsMatcher Transaction::GetTagsMatcher() const { return tr_.GetTagsMatcher(); }

int64_t Transaction::GetTransactionId() const noexcept { return tr_.i_.txId_; }

Error Transaction::modify(Item&& item, ItemModifyMode mode, lsn_t lsn, Transaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->addTxItem(*this, std::move(item), mode, lsn, std::move(asyncCmpl));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error Transaction::modify(Query&& query, lsn_t lsn, Transaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->modifyTx(*this, std::move(query), lsn, std::move(asyncCmpl));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error Transaction::putMeta(std::string_view key, std::string_view value, lsn_t lsn, Transaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->putTxMeta(*this, key, value, lsn, std::move(asyncCmpl));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error Transaction::setTagsMatcher(TagsMatcher&& tm, lsn_t lsn, Transaction::Completion asyncCmpl) {
	if (!IsFree()) {
		auto err = rx_->setTxTm(*this, std::move(tm), lsn, std::move(asyncCmpl));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

}  // namespace client
}  // namespace reindexer
