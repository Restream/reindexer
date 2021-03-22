#pragma once
#include <chrono>
#include "client/corotransaction.h"
#include "client/item.h"
#include "client/synccororeindexerimpl.h"
#include "core/query/query.h"
namespace reindexer {

namespace client {

class SyncCoroReindexerImpl;

class SyncCoroTransaction {
public:
	Error Insert(Item&& item, lsn_t lsn = lsn_t());
	Error Update(Item&& item, lsn_t lsn = lsn_t());
	Error Upsert(Item&& item, lsn_t lsn = lsn_t());
	Error Delete(Item&& item, lsn_t lsn = lsn_t());
	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t());

	Error Modify(Query&& query, lsn_t lsn = lsn_t());
	bool IsFree() const { return isFree || !status_.ok(); }
	Item NewItem();
	Error Status() const { return status_; }

private:
	friend class SyncCoroReindexerImpl;
	SyncCoroTransaction(Error status, SyncCoroReindexerImpl& rx) : tr_(status), rx_(rx), status_(status), isFree(true) {}
	SyncCoroTransaction(SyncCoroReindexerImpl& rx, CoroTransaction&& tr) : tr_(std::move(tr)), rx_(rx) {}

	CoroTransaction tr_;
	SyncCoroReindexerImpl& rx_;
	Error status_ = errOK;
	bool isFree = false;
};

}  // namespace client
}  // namespace reindexer
