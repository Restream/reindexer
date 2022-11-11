#pragma once

#include <optional>
#include "client/reindexer.h"
#include "localtransaction.h"
#include "proxiedtransaction.h"

namespace reindexer {

namespace client {
class Transaction;
}  // namespace client

namespace sharding {
class LocatorService;
}

class TransactionImpl {
public:
	TransactionImpl(LocalTransaction &&ltx) : data_(std::move(ltx.data_)), tx_{std::move(ltx.tx_)}, status_(std::move(ltx.err_)) {}
	TransactionImpl(LocalTransaction &&ltx, client::Transaction &&tx) : data_(std::move(ltx.data_)), status_(std::move(ltx.err_)) {
		tx_ = std::make_unique<ProxiedTransaction>(std::move(tx), false);
	}
	TransactionImpl(LocalTransaction &&ltx, client::Reindexer &&clusterLeader)
		: data_(std::move(ltx.data_)), tx_{std::move(clusterLeader)}, status_(std::move(ltx.err_)) {}

	Error Insert(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeInsert, lsn); }
	Error Update(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeUpdate, lsn); }
	Error Upsert(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeUpsert, lsn); }
	Error Delete(Item &&item, lsn_t lsn) { return Modify(std::move(item), ModeDelete, lsn); }
	Error Modify(Item &&item, ItemModifyMode mode, lsn_t lsn = lsn_t());
	Error Modify(Query &&query, lsn_t lsn = lsn_t());
	Error Nop(lsn_t lsn);
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());
	Error SetTagsMatcher(TagsMatcher &&tm, lsn_t lsn);

	Item NewItem();
	Error Status() const noexcept;
	int GetShardID() const noexcept;

	const std::string &GetNsName() const noexcept { return data_->nsName; }
	bool IsTagsUpdated() const noexcept;
	Transaction::TimepointT GetStartTime() const noexcept { return data_->startTime; }
	void SetShardingRouter(std::shared_ptr<sharding::LocatorService> shardingRouter);
	Error Rollback(int serverId, const RdxContext &ctx);
	Error Commit(int serverId, bool expectSharding, ReindexerImpl &rx, QueryResults &result, const RdxContext &ctx);

	static LocalTransaction Transform(TransactionImpl &tx);

private:
	struct Empty {};
	using ProxiedTxPtr = std::unique_ptr<ProxiedTransaction>;
	using TxStepsPtr = std::unique_ptr<TransactionSteps>;
	using RxClientT = client::Reindexer;

	void updateShardIdIfNecessary(int shardId);
	void ensureShardIdIsCorrect(const Item &item);
	void ensureShardIdIsCorrect(const Query &q);
	void updateTagsMatcherIfNecessary(Item &item);

	mutable std::mutex mtx_;
	std::unique_ptr<SharedTransactionData> data_;
	std::shared_ptr<sharding::LocatorService> shardingRouter_;
	std::variant<Empty, TxStepsPtr, ProxiedTxPtr, RxClientT> tx_;
	int shardId_ = ShardingKeyType::NotSetShard;
	Error status_;
};

}  // namespace reindexer
