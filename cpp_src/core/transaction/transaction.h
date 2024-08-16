#pragma once

#include <chrono>
#include "tools/clock.h"
#include "tools/errors.h"
#include "tools/lsn.h"

namespace reindexer_server {
class RPCServer;
}

namespace reindexer {

namespace client {
class Reindexer;
}  // namespace client

namespace sharding {
class LocatorServiceAdapter;
}

class TransactionImpl;
class PayloadType;
class TagsMatcher;
class FieldsSet;
class LocalTransaction;
class ReindexerImpl;
class QueryResults;
class Item;
class Query;
class RdxContext;

namespace client {
class Transaction;
}

class Transaction {
public:
	using ClockT = system_clock_w;
	using TimepointT = ClockT::time_point;

	explicit Transaction(LocalTransaction&& ltx);
	Transaction(LocalTransaction&& ltx, client::Reindexer&& clusterLeader);

	~Transaction();
	Transaction(Transaction&&) noexcept;
	Transaction& operator=(Transaction&&) noexcept;

	Error Insert(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeInsert, lsn); }
	Error Update(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeUpdate, lsn); }
	Error Upsert(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeUpsert, lsn); }
	Error Delete(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeDelete, lsn); }
	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t());
	Error Modify(Query&& query, lsn_t lsn = lsn_t());
	Error Nop(lsn_t lsn);
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());
	Error SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn);
	bool IsFree() const noexcept { return impl_ == nullptr && status_.ok(); }
	Item NewItem();
	Error Status() const noexcept;
	int GetShardID() const noexcept;

	std::string_view GetNsName() const noexcept;
	bool IsTagsUpdated() const noexcept;
	TimepointT GetStartTime() const noexcept;

	static LocalTransaction Transform(Transaction&& tx);

protected:
	Transaction(Error err);
	Transaction();
	Transaction(Transaction&& tr, sharding::LocatorServiceAdapter shardingRouter);

	Error rollback(int serverId, const RdxContext&);
	Error commit(int serverId, bool expectSharding, ReindexerImpl& rx, QueryResults& result, const RdxContext& ctx);

	std::unique_ptr<TransactionImpl> impl_;
	Error status_;

	friend class ClusterProxy;
	friend class ShardingProxy;
	friend class reindexer_server::RPCServer;
};

}  // namespace reindexer
