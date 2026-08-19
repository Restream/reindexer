#pragma once

#include "client/reindexer.h"
#include "cluster/sharding/locatorserviceadapter.h"
#include "localtransaction.h"
#include "proxiedtransaction.h"

namespace reindexer {

namespace client {
class Transaction;
}  // namespace client

class [[nodiscard]] TransactionImpl {
public:
	TransactionImpl(LocalTransaction&& ltx) : data_(std::move(ltx.data_)), tx_{std::move(ltx.tx_)}, status_(std::move(ltx.err_)) {}
	TransactionImpl(LocalTransaction&& ltx, client::Reindexer&& clusterLeader)
		: data_(std::move(ltx.data_)), tx_{std::move(clusterLeader)}, status_(std::move(ltx.err_)) {}

	void Insert(Item&& item, lsn_t lsn) { Modify(std::move(item), ModeInsert, lsn); }
	void Update(Item&& item, lsn_t lsn) { Modify(std::move(item), ModeUpdate, lsn); }
	void Upsert(Item&& item, lsn_t lsn) { Modify(std::move(item), ModeUpsert, lsn); }
	void Delete(Item&& item, lsn_t lsn) { Modify(std::move(item), ModeDelete, lsn); }
	void Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t());
	void Modify(Query&& query, lsn_t lsn = lsn_t());
	void Nop(lsn_t lsn);
	void PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());
	void SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn);

	Item NewItem() noexcept;
	Error Status() const noexcept;
	int GetShardID() const noexcept;

	std::string_view GetNsName() const noexcept { return data_->nsName; }
	bool IsTagsUpdated() const noexcept;
	Transaction::TimepointT GetStartTime() const noexcept { return data_->startTime; }
	void SetShardingRouter(sharding::LocatorServiceAdapter shardingRouter);
	void Rollback(int serverId, const RdxContext& ctx) noexcept;
	void Commit(int serverId, bool expectSharding, ReindexerImpl& rx, QueryResults& result, const RdxContext& ctx);

	static LocalTransaction Transform(TransactionImpl& tx) noexcept;

private:
	struct [[nodiscard]] Empty {};
	using ProxiedTxPtr = std::unique_ptr<ProxiedTransaction>;
	using TxStepsPtr = std::unique_ptr<TransactionSteps>;
	using RxClientT = client::Reindexer;

	void updateShardIdIfNecessary(int shardId, const Variant& curShardKey);
	void lazyInit(const Item& item);
	void lazyInit(const Query& q);
	void lazyInit();
	void initProxiedTx(RxClientT* leader);
	void initProxiedTxIfRequired();
	void updateTagsMatcherIfNecessary(Item& item);

	mutable mutex mtx_;
	std::unique_ptr<SharedTransactionData> data_;
	sharding::LocatorServiceAdapter shardingRouter_;
	std::variant<Empty, TxStepsPtr, ProxiedTxPtr, RxClientT> tx_;
	int shardId_ = ShardingKeyType::NotSetShard;
	Error status_;
	Variant firstShardKey_;
};

}  // namespace reindexer
