#include "client/synccorotransaction.h"
#include "client/synccororeindexerimpl.h"
#include "core/cjson/tagsmatcher.h"

namespace reindexer {
namespace client {

static const auto kBadTxStatus = Error(errBadTransaction, "Transaction is free");

Error SyncCoroTransaction::Insert(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeInsert, lsn); }
Error SyncCoroTransaction::Update(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeUpdate, lsn); }
Error SyncCoroTransaction::Upsert(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeUpsert, lsn); }
Error SyncCoroTransaction::Delete(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeDelete, lsn); }
Error SyncCoroTransaction::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	if (!IsFree()) {
		auto err = rx_->addTxItem(*this, std::move(item), mode, lsn);
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}
Error SyncCoroTransaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (!IsFree()) {
		auto err = rx_->putTxMeta(*this, key, value, lsn);
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error SyncCoroTransaction::SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) {
	if (!IsFree()) {
		auto err = rx_->setTxTm(*this, std::move(tm), lsn);
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}

Error SyncCoroTransaction::Modify(Query&& query, lsn_t lsn) {
	if (!IsFree()) {
		auto err = rx_->modifyTx(*this, std::move(query), lsn);
		if (!err.ok()) setStatus(std::move(err));
		return Status();
	}
	return Status().ok() ? kBadTxStatus : Status();
}
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
}

PayloadType SyncCoroTransaction::GetPayloadType() const { return tr_.GetPayloadType(); }
TagsMatcher SyncCoroTransaction::GetTagsMatcher() const { return tr_.GetTagsMatcher(); }

}  // namespace client
}  // namespace reindexer
