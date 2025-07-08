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
		auto err = rx_->RollBackTransaction(*this, InternalRdxContext(lsn_t{0}, nullptr, 0));
		(void)err;	// ignore
	}
	tr_.clear();
}

PayloadType Transaction::GetPayloadType() const { return tr_.GetPayloadType(); }
TagsMatcher Transaction::GetTagsMatcher() const { return tr_.GetTagsMatcher(); }

int64_t Transaction::GetTransactionId() const noexcept { return tr_.i_.txId_; }

Error Transaction::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	return modify(std::move(item), mode, InternalRdxContext(std::move(lsn)));
}
Error Transaction::Modify(Item&& item, ItemModifyMode mode, Completion cmpl, lsn_t lsn) {
	return modify(std::move(item), mode, InternalRdxContext(std::move(lsn)).WithCompletion(std::move(cmpl)));
}
Error Transaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	return putMeta(key, value, InternalRdxContext(std::move(lsn)));
}
Error Transaction::SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) { return setTagsMatcher(std::move(tm), InternalRdxContext(std::move(lsn))); }
Error Transaction::Modify(Query&& query, lsn_t lsn) { return modify(std::move(query), InternalRdxContext(std::move(lsn))); }

Error Transaction::modify(Item&& item, ItemModifyMode mode, InternalRdxContext&& ctx) {
	if (!IsFree()) {
		auto err = rx_->addTxItem(*this, std::move(item), mode, ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error Transaction::modify(Query&& query, InternalRdxContext&& ctx) {
	if (!IsFree()) {
		auto err = rx_->modifyTx(*this, std::move(query), ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error Transaction::putMeta(std::string_view key, std::string_view value, InternalRdxContext&& ctx) {
	if (!IsFree()) {
		auto err = rx_->putTxMeta(*this, key, value, ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error Transaction::setTagsMatcher(TagsMatcher&& tm, InternalRdxContext&& ctx) {
	if (!IsFree()) {
		auto err = rx_->setTxTm(*this, std::move(tm), ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
		if (!err.ok()) {
			setStatus(std::move(err));
		}
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

}  // namespace client
}  // namespace reindexer
