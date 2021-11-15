#pragma once
#include <chrono>
#include "client/item.h"
#include "core/query/query.h"
#include "tools/lsn.h"

namespace reindexer {

namespace net {
namespace cproto {
class CoroClientConnection;
}  // namespace cproto
}  // namespace net

namespace client {

class Namespace;
class CoroRPCClient;

class CoroTransaction {
public:
	CoroTransaction() noexcept : CoroTransaction(Error()) {}
	~CoroTransaction();
	CoroTransaction(const CoroTransaction&) = delete;
	CoroTransaction(CoroTransaction&&) noexcept;
	CoroTransaction& operator=(CoroTransaction&&) noexcept;
	CoroTransaction& operator=(const CoroTransaction&) = delete;

	Error Insert(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeInsert, lsn); }
	Error Update(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeUpdate, lsn); }
	Error Upsert(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeUpsert, lsn); }
	Error Delete(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeDelete, lsn); }
	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), mode, lsn); }
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());

	Error Modify(Query&& query, lsn_t lsn = lsn_t());
	bool IsFree() const noexcept { return (rpcClient_ == nullptr) || !status_.ok(); }
	Item NewItem();
	template <typename ClientT>
	Item NewItem(ClientT*);
	Error Status() const noexcept { return status_; }
	TagsMatcher GetTagsMatcher() const noexcept;
	PayloadType GetPayloadType() const noexcept;

private:
	friend class CoroRPCClient;
	friend class SyncCoroReindexerImpl;
	friend class SyncCoroTransaction;
	CoroTransaction(Error status) noexcept;
	CoroTransaction(CoroRPCClient* rpcClient, int64_t txId, std::chrono::milliseconds RequestTimeout, std::chrono::milliseconds execTimeout,
					Namespace* ns);

	Error addTxItem(Item&& item, ItemModifyMode mode, lsn_t lsn);
	void clear() noexcept {
		txId_ = -1;
		rpcClient_ = nullptr;
		status_ = errOK;
	}
	Error mergeTmFromItem(Item& item, Item& rItem);
	net::cproto::CoroClientConnection* getConn() const noexcept;

	int64_t txId_ = -1;
	CoroRPCClient* rpcClient_ = nullptr;
	std::chrono::milliseconds requestTimeout_ = std::chrono::milliseconds{0};
	std::chrono::milliseconds execTimeout_ = std::chrono::milliseconds{0};
	Error status_;
	std::unique_ptr<TagsMatcher> localTm_;
	Namespace* ns_ = nullptr;
};

}  // namespace client
}  // namespace reindexer
