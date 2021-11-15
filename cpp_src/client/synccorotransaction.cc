#include "client/synccorotransaction.h"
#include "client/synccororeindexerimpl.h"
#include "core/cjson/tagsmatcher.h"

namespace reindexer {
namespace client {

Error SyncCoroTransaction::Insert(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeInsert, lsn); }
Error SyncCoroTransaction::Update(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeUpdate, lsn); }
Error SyncCoroTransaction::Upsert(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeUpsert, lsn); }
Error SyncCoroTransaction::Delete(Item&& item, lsn_t lsn) { return Modify(std::move(item), ModeDelete, lsn); }
Error SyncCoroTransaction::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	if (!IsFree()) {
		return rx_->addTxItem(*this, std::move(item), mode, lsn);
	}
	if (!status_.ok()) {
		return status_;
	}
	return Error(errBadTransaction, "Transaction is free");
}
Error SyncCoroTransaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (!IsFree()) {
		return rx_->putTxMeta(*this, key, value, lsn);
	}
	if (!status_.ok()) {
		return status_;
	}
	return Error(errBadTransaction, "Transaction is free");
}

Error SyncCoroTransaction::Modify(Query&& query, lsn_t lsn) {
	if (!IsFree()) {
		return rx_->modifyTx(*this, std::move(query), lsn);
	}
	if (!status_.ok()) {
		return status_;
	}
	return Error(errBadTransaction, "Transaction is free");
}
Item SyncCoroTransaction::NewItem() {
	if (!IsFree()) {
		return rx_->newItemTx(tr_);
	}
	if (!status_.ok()) {
		return Item(status_);
	}
	return Item(Error(errBadTransaction, "Transaction is free"));
}

PayloadType SyncCoroTransaction::GetPayloadType() const { return tr_.GetPayloadType(); }
TagsMatcher SyncCoroTransaction::GetTagsMatcher() const { return tr_.GetTagsMatcher(); }

}  // namespace client
}  // namespace reindexer
