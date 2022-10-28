#pragma once

#include <chrono>
#include "client/corotransaction.h"
#include "client/item.h"

namespace reindexer {

namespace client {

class SyncCoroReindexerImpl;

class SyncCoroTransaction {
public:
	Error Insert(Item&& item);
	Error Update(Item&& item);
	Error Upsert(Item&& item);
	Error Delete(Item&& item);
	Error Modify(Item&& item, ItemModifyMode mode);

	Error Modify(Query&& query);
	bool IsFree() const { return isFree_ || !status_.ok(); }
	Item NewItem();
	Error Status() const { return status_; }

private:
	friend class SyncCoroReindexerImpl;
	SyncCoroTransaction(Error status, SyncCoroReindexerImpl* rx) : tr_(status), rx_(rx), status_(std::move(status)), isFree_(true) {}
	SyncCoroTransaction(CoroTransaction&& tr, SyncCoroReindexerImpl* rx) : tr_(std::move(tr)), rx_(rx) {}

	CoroTransaction tr_;
	SyncCoroReindexerImpl* rx_;
	Error status_ = errOK;
	bool isFree_ = false;
};

}  // namespace client
}  // namespace reindexer
