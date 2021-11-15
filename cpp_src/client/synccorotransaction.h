#pragma once

#include <chrono>
#include "client/corotransaction.h"
#include "client/item.h"

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
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());

	Error Modify(Query&& query, lsn_t lsn = lsn_t());
	bool IsFree() const { return !rx_ || !status_.ok() || tr_.txId_ == -1; }
	Item NewItem();
	Error Status() const { return status_; }

	SyncCoroTransaction(SyncCoroTransaction&) = delete;
	SyncCoroTransaction& operator=(const SyncCoroTransaction&) = delete;
	SyncCoroTransaction(SyncCoroTransaction&&) noexcept = default;
	SyncCoroTransaction& operator=(SyncCoroTransaction&&) = default;

	PayloadType GetPayloadType() const;
	TagsMatcher GetTagsMatcher() const;

private:
	friend class SyncCoroReindexer;
	friend class SyncCoroReindexerImpl;
	friend class reindexer::ClusterProxy;
	SyncCoroTransaction(std::shared_ptr<SyncCoroReindexerImpl> rx, CoroTransaction&& tr) : tr_(std::move(tr)), rx_(rx) {}
	SyncCoroTransaction(Error status) : tr_(status), status_(status) {}

	CoroTransaction tr_;
	std::shared_ptr<SyncCoroReindexerImpl> rx_;
	Error status_ = errOK;
};

}  // namespace client
}  // namespace reindexer
