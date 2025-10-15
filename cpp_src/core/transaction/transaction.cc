#include "transaction.h"
#include "core/item.h"
#include "transactionimpl.h"

namespace reindexer {

Transaction::Transaction(LocalTransaction&& ltx) : impl_(std::make_unique<TransactionImpl>(std::move(ltx))) {}

Transaction::Transaction(LocalTransaction&& ltx, client::Reindexer&& clusterLeader)
	: impl_(std::make_unique<TransactionImpl>(std::move(ltx), std::move(clusterLeader))) {}

Transaction::~Transaction() = default;
Transaction::Transaction(Transaction&&) noexcept = default;
Transaction& Transaction::operator=(Transaction&&) noexcept = default;

std::string_view Transaction::GetNsName() const noexcept {
	static const std::string empty;
	if (impl_) {
		return impl_->GetNsName();
	}
	return empty;
}

Error Transaction::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	if (impl_) {
		return impl_->Modify(std::move(item), mode, lsn);
	}
	return status_;
}

Error Transaction::Modify(Query&& query, lsn_t lsn) {
	try {
		query.VerifyForUpdateTransaction();
	} catch (std::exception& err) {
		return err;
	}
	if (impl_) {
		return impl_->Modify(std::move(query), lsn);
	}
	return status_;
}

Error Transaction::Nop(lsn_t lsn) {
	if (impl_) {
		return impl_->Nop(lsn);
	}
	return status_;
}

Error Transaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (impl_) {
		return impl_->PutMeta(key, value, lsn);
	}
	return status_;
}

Error Transaction::SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) {
	if (impl_) {
		return impl_->SetTagsMatcher(std::move(tm), lsn);
	}
	return status_;
}

Item Transaction::NewItem() {
	if (impl_) {
		return impl_->NewItem();
	}
	return Item(status_);
}

Error Transaction::Status() const noexcept { return impl_ ? impl_->Status() : status_; }

int Transaction::GetShardID() const noexcept { return impl_ ? impl_->GetShardID() : ShardingKeyType::NotSetShard; }

bool Transaction::IsTagsUpdated() const noexcept {
	if (impl_) {
		return impl_->IsTagsUpdated();
	}
	return false;
}

Transaction::TimepointT Transaction::GetStartTime() const noexcept {
	if (impl_) {
		return impl_->GetStartTime();
	}
	return Transaction::TimepointT();
}

LocalTransaction Transaction::Transform(Transaction&& tx) {
	if (tx.impl_) {
		return TransactionImpl::Transform(*tx.impl_);
	}
	return LocalTransaction(Error(errNotValid, "Empty local transaction"));
}

// NOLINTNEXTLINE (bugprone-throw-keyword-missing)
Transaction::Transaction(Error err) : status_(std::move(err)) {}

Transaction::Transaction(Transaction&& tr, sharding::LocatorServiceAdapter shardingRouter) : Transaction(std::move(tr)) {
	assertrx(impl_);
	impl_->SetShardingRouter(std::move(shardingRouter));
}

Transaction::Transaction() = default;

Error Transaction::rollback(int serverId, const RdxContext& ctx) { return impl_ ? impl_->Rollback(serverId, ctx) : status_; }

Error Transaction::commit(int serverId, bool expectSharding, ReindexerImpl& rx, QueryResults& result, const RdxContext& ctx) {
	return impl_ ? impl_->Commit(serverId, expectSharding, rx, result, ctx) : status_;
}

}  // namespace reindexer
