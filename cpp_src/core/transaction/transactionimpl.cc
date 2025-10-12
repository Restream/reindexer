#include "transactionimpl.h"
#include "core/queryresults/queryresults.h"
#include "core/reindexer_impl/reindexerimpl.h"

namespace reindexer {

const static Error kTxImplIsNotValid = Error(errNotValid, "Transaction is not initialized");

Error TransactionImpl::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	lock_guard lck(mtx_);

	if (!status_.ok()) {
		return status_;
	}
	if (!item.impl_) {
		status_ = Error(errLogic, "Broken item in transaction");
		return status_;
	}

	try {
		lazyInit(item);

		data_->UpdateTagsMatcherIfNecessary(*item.impl_);
		if (auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_); proxiedTx && *proxiedTx) {
			status_ = (*proxiedTx)->Modify(std::move(item), mode, lsn);
			return status_;
		} else if (auto* localTx = std::get_if<TxStepsPtr>(&tx_); localTx && *localTx) {
			(*localTx)->Modify(std::move(item), mode, lsn);
			return {};
		}
	} catch (Error& err) {
		status_ = err;
		return err;
	}

	return kTxImplIsNotValid;
}

Error TransactionImpl::Modify(Query&& query, lsn_t lsn) {
	lock_guard lck(mtx_);

	if (!status_.ok()) {
		return status_;
	}

	try {
		lazyInit(query);
	} catch (Error& err) {
		return err;
	}

	if (auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_); proxiedTx && *proxiedTx) {
		status_ = (*proxiedTx)->Modify(std::move(query), lsn);
		return status_;
	} else if (auto* localTx = std::get_if<TxStepsPtr>(&tx_); localTx && *localTx) {
		(*localTx)->Modify(std::move(query), lsn);
		return {};
	}
	return kTxImplIsNotValid;
}

Error TransactionImpl::Nop(lsn_t lsn) {
	lock_guard lck(mtx_);
	if (!status_.ok()) {
		return status_;
	}
	try {
		lazyInit();
	} catch (Error& err) {
		return err;
	}
	if (auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_); proxiedTx && *proxiedTx) {
		status_ = Error(errLogic, "Nop() is not available for proxied transactions");
		return status_;
	} else if (auto* localTx = std::get_if<TxStepsPtr>(&tx_); localTx && *localTx) {
		(*localTx)->Nop(lsn);
		return {};
	}
	return kTxImplIsNotValid;
}

Error TransactionImpl::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (key.empty()) {
		throw Error(errLogic, "Empty meta key is not allowed in tx");
	}

	lock_guard lck(mtx_);
	if (!status_.ok()) {
		return status_;
	}
	try {
		lazyInit();
	} catch (Error& err) {
		return err;
	}
	if (auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_); proxiedTx && *proxiedTx) {
		status_ = (*proxiedTx)->PutMeta(key, value, lsn);
		return status_;
	} else if (auto* localTx = std::get_if<TxStepsPtr>(&tx_); localTx && *localTx) {
		(*localTx)->PutMeta(key, value, lsn);
		return {};
	}
	return kTxImplIsNotValid;
}

Error TransactionImpl::SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) {
	if (lsn.isEmpty()) {
		return Error(errLogic, "Unable to set tx tagsmatcher without lsn");
	}

	lock_guard lck(mtx_);
	if (!status_.ok()) {
		return status_;
	}
	try {
		lazyInit();
	} catch (Error& err) {
		return err;
	}

	try {
		auto tmCopy = tm;
		data_->SetTagsMatcher(std::move(tmCopy));
	} catch (Error& err) {
		status_ = std::move(err);
		return status_;
	}

	if (auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_); proxiedTx && *proxiedTx) {
		status_ = (*proxiedTx)->SetTagsMatcher(std::move(tm), lsn);
		return status_;
	} else if (auto* localTx = std::get_if<TxStepsPtr>(&tx_); localTx && *localTx) {
		(*localTx)->SetTagsMatcher(std::move(tm), lsn);
		return {};
	}
	return kTxImplIsNotValid;
}

Item TransactionImpl::NewItem() {
	lock_guard lck(mtx_);
	assertrx(data_);
	Item item(new ItemImpl(data_->GetPayloadType(), data_->GetTagsMatcher(), data_->GetPKFileds(), data_->GetSchema()));
	item.impl_->tagsMatcher().clearUpdated();
	return item;
}

Error TransactionImpl::Status() const noexcept {
	lock_guard lck(mtx_);
	return status_;
}

int TransactionImpl::GetShardID() const noexcept {
	lock_guard lck(mtx_);
	return shardId_;
}

bool TransactionImpl::IsTagsUpdated() const noexcept {
	lock_guard lck(mtx_);
	assertrx(data_);
	return data_->IsTagsUpdated();
}

void TransactionImpl::SetShardingRouter(sharding::LocatorServiceAdapter shardingRouter) {
	lock_guard lck(mtx_);
	assertrx(!shardingRouter_);
	shardingRouter_ = std::move(shardingRouter);
}

Error TransactionImpl::Rollback(int serverId, const RdxContext& ctx) {
	lock_guard lck(mtx_);
	if (auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_); proxiedTx && *proxiedTx) {
		const auto ward = ctx.BeforeClusterProxy();
		(*proxiedTx)->Rollback(serverId, ctx);
	}

	data_.reset();
	shardingRouter_.reset();
	tx_ = Empty{};
	shardId_ = ShardingKeyType::NotSetShard;
	status_ = Error(errNotValid, "Transaction was rolled back");
	return {};
}

Error TransactionImpl::Commit(int serverId, bool expectSharding, ReindexerImpl& rx, QueryResults& result, const RdxContext& ctx) {
	const static Error kErrCommitted(errNotValid, "Tx is already committed");

	lock_guard lck(mtx_);
	if (!status_.ok()) {
		return status_;
	}
	if (expectSharding && shardId_ == ShardingKeyType::NotSetShard) {
		return Error(errLogic, "Error committing transaction with sharding: shard ID is not set");
	}

	if (auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_); proxiedTx && *proxiedTx) {
		const auto ward = ctx.BeforeClusterProxy();
		auto res = (*proxiedTx)->Commit(serverId, result, ctx);
		if (res.ok() && shardingRouter_) {
			result.SetShardingConfigVersion(shardingRouter_.SourceId());
		}
		status_ = kErrCommitted;
		return res;
	} else if (auto* localTx = std::get_if<TxStepsPtr>(&tx_); localTx && *localTx) {
		try {
			if (shardingRouter_) {
				result.AddQr(LocalQueryResults(), shardingRouter_.ActualShardId());
				result.SetShardingConfigVersion(shardingRouter_.SourceId());
			} else {
				result.AddQr(LocalQueryResults(), shardingRouter_ ? shardingRouter_.ActualShardId() : ShardingKeyType::ProxyOff);
			}
			status_ = kErrCommitted;
			LocalTransaction ltx(std::move(data_), std::move(*localTx), Error());
			tx_ = Empty{};
			auto res = rx.CommitTransaction(ltx, result.ToLocalQr(false), ctx);
			data_ = std::move(ltx.data_);
			return res;
		} catch (Error& e) {
			return e;
		}
	} else if (std::holds_alternative<RxClientT>(tx_)) {
		// Empty proxied transaction. Just skipping commit
		return {};
	}
	return kTxImplIsNotValid;
}

LocalTransaction TransactionImpl::Transform(TransactionImpl& tx) {
	lock_guard lck(tx.mtx_);
	if (auto* localTx = std::get_if<TxStepsPtr>(&tx.tx_); localTx && *localTx) {
		LocalTransaction l(std::move(tx.data_), std::move(*localTx), std::move(tx.status_));
		tx.status_ = Error(errNotValid, "Transformed into local tx");
		return l;
	} else if (auto* empty = std::get_if<Empty>(&tx.tx_); empty && !tx.status_.ok()) {
		LocalTransaction l(std::move(tx.data_), std::make_unique<TransactionSteps>(), std::move(tx.status_));
		tx.status_ = Error(errNotValid, "Transformed into local tx");
		return l;
	}
	return LocalTransaction(Error(errNotValid, "Non-local transaction"));
}

void TransactionImpl::updateShardIdIfNecessary(int shardId, const Variant& curShardKey) {
	if ((shardId_ != ShardingKeyType::NotSetShard) && (shardId != shardId_)) {
		throw Error(errLogic,
					"Transaction query to a different shard: {} ({} is expected); First tx shard key - {}, current tx shard key - {}",
					shardId, shardId_, firstShardKey_.Dump(), curShardKey.Dump());
	}
	if (shardId_ == ShardingKeyType::NotSetShard) {
		shardId_ = shardId;
		Error status;
		assertrx(data_);
		if (auto connection = shardingRouter_.GetShardConnection(data_->nsName, shardId, status)) {
			if (status.ok()) {
				tx_ = std::make_unique<ProxiedTransaction>(connection->NewTransaction(data_->nsName), shardId_);
			}
		} else if (status.ok()) {
			if (auto* leader = std::get_if<RxClientT>(&tx_)) {
				initProxiedTx(leader);
			} else {
				auto* proxiedTx = std::get_if<ProxiedTxPtr>(&tx_);
				if (proxiedTx) {
					throw Error(errLogic, "Transaction was proxied before target shard ID was set");
				}
			}
		}
		if (!status.ok()) {
			throw status;
		}
	}
}

void TransactionImpl::lazyInit(const Query& q) {
	if (shardingRouter_) {
		const auto [ids, shardKey] = shardingRouter_.GetShardIdKeyPair(q);
		if (firstShardKey_.IsNullValue()) {
			firstShardKey_ = shardKey;
		}
		if (ids.size() != 1) {
			Error status(errLogic, "Transaction query must correspond to exactly one shard ({} corresponding shards found)", ids.size());
			status_ = status;
			throw status;
		}
		updateShardIdIfNecessary(ids.front(), shardKey);
	} else {
		initProxiedTxIfRequired();
	}
}

void TransactionImpl::lazyInit() {
	if (shardingRouter_ && std::holds_alternative<RxClientT>(tx_)) {
		Error status(errLogic, "Transaction, proxied by Sharding Proxy, can not start with Nop() or Meta() steps");
		status_ = status;
		throw status;
	} else {
		initProxiedTxIfRequired();
	}
}

void TransactionImpl::initProxiedTx(RxClientT* leader) {
	tx_ = std::make_unique<ProxiedTransaction>(leader->NewTransaction(data_->nsName), ShardingKeyType::NotSetShard);
}

void TransactionImpl::initProxiedTxIfRequired() {
	if (auto* leader = std::get_if<RxClientT>(&tx_)) {
		initProxiedTx(leader);
	}
}

void TransactionImpl::lazyInit(const Item& item) {
	if (shardingRouter_) {
		const auto [id, shardKey] = shardingRouter_.GetShardIdKeyPair(data_->nsName, item);
		if (firstShardKey_.IsNullValue()) {
			firstShardKey_ = shardKey;
		}
		updateShardIdIfNecessary(id, shardKey);
	} else {
		initProxiedTxIfRequired();
	}
}

}  // namespace reindexer
