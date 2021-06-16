#include "client/synccorotransaction.h"
#include "client/synccororeindexerimpl.h"

namespace reindexer {
namespace client {

Error SyncCoroTransaction::Insert(Item&& item) { return rx_->addTxItem(*this, std::move(item), ModeInsert); }
Error SyncCoroTransaction::Update(Item&& item) { return rx_->addTxItem(*this, std::move(item), ModeUpdate); }
Error SyncCoroTransaction::Upsert(Item&& item) { return rx_->addTxItem(*this, std::move(item), ModeUpsert); }
Error SyncCoroTransaction::Delete(Item&& item) { return rx_->addTxItem(*this, std::move(item), ModeDelete); }
Error SyncCoroTransaction::Modify(Item&& item, ItemModifyMode mode) { return rx_->addTxItem(*this, std::move(item), mode); }
Error SyncCoroTransaction::Modify(Query&& query) { return rx_->modifyTx(*this, std::move(query)); }
Item SyncCoroTransaction::NewItem() { return rx_->newItemTx(tr_); }

}  // namespace client
}  // namespace reindexer
