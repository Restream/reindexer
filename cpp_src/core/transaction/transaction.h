#pragma once

#include <memory>
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

class [[nodiscard]] Transaction {
public:
	using ClockT = system_clock_w;
	using TimepointT = ClockT::time_point;
	using Completion = std::function<void(const Error& err)>;

	explicit Transaction(LocalTransaction&& ltx);
	Transaction(LocalTransaction&& ltx, client::Reindexer&& clusterLeader);

	~Transaction();
	Transaction(Transaction&&) noexcept;
	Transaction& operator=(Transaction&&) noexcept;

	Error Insert(Item&& item, lsn_t lsn = lsn_t()) noexcept { return Modify(std::move(item), ModeInsert, lsn); }
	Error Update(Item&& item, lsn_t lsn = lsn_t()) noexcept { return Modify(std::move(item), ModeUpdate, lsn); }
	Error Upsert(Item&& item, lsn_t lsn = lsn_t()) noexcept { return Modify(std::move(item), ModeUpsert, lsn); }
	Error Upsert(Item&& item, const Completion& cmpl, lsn_t lsn = lsn_t()) noexcept;
	Error Delete(Item&& item, lsn_t lsn = lsn_t()) noexcept { return Modify(std::move(item), ModeDelete, lsn); }
	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t()) noexcept;
	Error Modify(Query&& query, lsn_t lsn = lsn_t()) noexcept;
	Error Nop(lsn_t lsn) noexcept;
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t()) noexcept;
	Error SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) noexcept;
	bool IsFree() const noexcept { return impl_ == nullptr && status_.ok(); }
	Item NewItem() noexcept;
	Error Status() const noexcept;
	int GetShardID() const noexcept;

	std::string_view GetNsName() const noexcept;
	bool IsTagsUpdated() const noexcept;
	TimepointT GetStartTime() const noexcept;

	static LocalTransaction Transform(Transaction&& tx) noexcept;

private:
	Transaction(Error err);
	Transaction();
	Transaction(Transaction&& tr, sharding::LocatorServiceAdapter shardingRouter);

	Error rollback(int serverId, const RdxContext&) noexcept;
	Error commit(int serverId, bool expectSharding, ReindexerImpl& rx, QueryResults& result, const RdxContext& ctx) noexcept;

	std::unique_ptr<TransactionImpl> impl_;
	Error status_;

	friend class ClusterProxy;
	friend class ShardingProxy;
	friend class Reindexer;
	friend class reindexer_server::RPCServer;
};

}  // namespace reindexer
