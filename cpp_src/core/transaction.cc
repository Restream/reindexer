#include "transaction.h"
#include "client/synccororeindexerimpl.h"
#include "client/synccorotransaction.h"
#include "transactionimpl.h"

#ifdef WITH_SHARDING
#include "cluster/sharding/sharding.h"
#endif	// WITH_SHARDIN

namespace reindexer {

Transaction::Transaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
						 std::shared_ptr<const Schema> schema, lsn_t lsn)
	: impl_(new TransactionImpl(nsName, pt, tm, pf, schema, lsn)) {}

Transaction::Transaction(Error err) : status_(std::move(err)), shardId_(IndexValueType::NotSet) {}

Transaction::Transaction() = default;
Transaction::~Transaction() = default;
Transaction::Transaction(Transaction &&) noexcept = default;
Transaction &Transaction::operator=(Transaction &&) noexcept = default;

#ifdef WITH_SHARDING

void Transaction::updateShardIdIfNecessary(int shardId) {
	if (shardId == IndexValueType::NotSet) {
		// no matches with shards means going to proxy
		shardId = 0;
	}
	if ((shardId_ != IndexValueType::NotSet) && (shardId != shardId_)) {
		throw Error(errLogic, "Transaction query to a different shard: %d (%d is expected)", shardId, shardId_);
	}
	if ((shardId_ == IndexValueType::NotSet) && !clientTransaction_) {
		Error status;
		if (auto connection = shardingRouter_->GetShardConnection(shardId, true, status, impl_->nsName_)) {
			if (status.ok()) {
				clientTransaction_ = std::make_unique<client::SyncCoroTransaction>(connection->NewTransaction(impl_->nsName_));
			}
		} else if (status.ok() && leaderRx_.has_value()) {
			clientTransaction_ = std::make_unique<client::SyncCoroTransaction>(leaderRx_->NewTransaction(impl_->nsName_));
			status_ = clientTransaction_->Status();	 // TODO: Rewrite status() logic in those txs
		}
	}
	shardId_ = shardId;
}

void Transaction::ensureShardIdIsCorrect(const Query &q) {
	if (impl_ && shardingRouter_) {
		auto ids = shardingRouter_->GetShardId(q);
		bool exactMatch = (ids.size() == 1);
		updateShardIdIfNecessary(exactMatch ? ids.front() : IndexValueType::NotSet);
	}
}

void Transaction::ensureShardIdIsCorrect(const Item &item) {
	if (impl_ && shardingRouter_) {
		updateShardIdIfNecessary(shardingRouter_->GetShardId(impl_->nsName_, item));
	}
}

#endif	// WITH_SHARDING

const string &Transaction::GetNsName() {
	static std::string empty;
	if (impl_) {
		return impl_->nsName_;
	}
	return empty;
}

Error Transaction::Insert(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeInsert, lsn); }
Error Transaction::Update(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeUpdate, lsn); }
Error Transaction::Upsert(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeUpsert, lsn); }
Error Transaction::Delete(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeDelete, lsn); }

Error Transaction::Modify(Item &&item, ItemModifyMode mode, lsn_t lsn) {
#ifdef WITH_SHARDING
	try {
		ensureShardIdIsCorrect(item);
	} catch (Error &err) {
		return err;
	}
#endif	// WITH_SHARDING

	if (clientTransaction_ && impl_) {
		if (!status_.ok()) {
			return status_;
		}
		auto clientItem = clientTransaction_->NewItem();
		if (!clientItem.Status().ok()) {
			return clientItem.Status();
		}
		std::string_view serverCJson = item.impl_->GetCJSON(true);
		Error err = clientItem.Unsafe().FromCJSON(serverCJson);
		if (!err.ok()) {
			return err;
		}
		// Update tx tm to allow new items creation
		{
			std::unique_lock<std::mutex> lock(impl_->mtx_);
			impl_->updateTagsMatcherIfNecessary(item);
		}
		clientItem.SetPrecepts(item.impl_->GetPrecepts());
		status_ = clientTransaction_->Modify(std::move(clientItem), mode, lsn);
		if (!status_.ok()) {
			return status_;
		}
	} else if (impl_) {
		impl_->Modify(move(item), mode, lsn);
	}
	return Error();
}

Error Transaction::Modify(Query &&query, lsn_t lsn) {
#ifdef WITH_SHARDING
	try {
		ensureShardIdIsCorrect(query);
	} catch (Error &err) {
		return err;
	}
#endif	// WITH_SHARDING

	if (clientTransaction_ && impl_) {
		if (!status_.ok()) {
			return status_;
		}
		status_ = clientTransaction_->Modify(std::move(query), lsn);
		if (!status_.ok()) {
			return status_;
		}
	} else if (impl_) {
		impl_->Modify(move(query), lsn);
	}
	return Error();
}

void Transaction::Nop(lsn_t lsn) {
	assert(!clientTransaction_);
	if (impl_) impl_->Nop(lsn);
}

Error Transaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (clientTransaction_ && impl_) {
		if (!status_.ok()) {
			return status_;
		}
		status_ = clientTransaction_->PutMeta(key, value, lsn);
		if (!status_.ok()) {
			return status_;
		}
	} else if (impl_) {
		impl_->PutMeta(key, value, lsn);
	}
	return Error();
}

Error Transaction::SetTagsMatcher(TagsMatcher &&tm, lsn_t lsn) {
	if (lsn.isEmpty()) {
		return Error(errLogic, "Unable to set tx tagsmatcher without lsn");
	}
	if (clientTransaction_ && impl_) {
		if (!status_.ok()) {
			return status_;
		}
		auto tmCopy = tm;
		status_ = clientTransaction_->SetTagsMatcher(std::move(tmCopy), lsn);
		if (!status_.ok()) {
			return status_;
		}
		impl_->SetTagsMatcher(std::move(tm), lsn);
	} else if (impl_) {
		impl_->SetTagsMatcher(std::move(tm), lsn);
	}
	return Error();
}

Item Transaction::NewItem() {
	assert(impl_);
	return impl_->NewItem();
}

vector<TransactionStep> &Transaction::GetSteps() noexcept {
	assert(impl_ && !clientTransaction_);
	return impl_->steps_;
}

const vector<TransactionStep> &Transaction::GetSteps() const noexcept {
	assert(impl_ && !clientTransaction_);
	return impl_->steps_;
}

Item Transaction::GetItem(TransactionStep &&st) {
	assert(impl_ && !clientTransaction_);
	return impl_->GetItem(std::move(st));
}

lsn_t Transaction::GetLSN() const noexcept {
	assert(impl_ && !clientTransaction_);
	return impl_->GetLSN();
}

bool Transaction::IsTagsUpdated() const noexcept {
	assert(impl_);
	return impl_->tagsUpdated_;
}

Transaction::time_point Transaction::GetStartTime() const noexcept {
	assert(impl_ && !clientTransaction_);
	return impl_->startTime_;
}

void Transaction::SetClient(client::SyncCoroTransaction &&tx) { clientTransaction_.reset(new client::SyncCoroTransaction(std::move(tx))); }
void Transaction::SetLeader(client::SyncCoroReindexer &&l) { leaderRx_ = std::move(l); }
void Transaction::SetShardingRouter(std::shared_ptr<sharding::LocatorService> shardingRouter) { shardingRouter_ = shardingRouter; }

}  // namespace reindexer
