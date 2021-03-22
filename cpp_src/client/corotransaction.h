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

class CoroRPCClient;

class CoroTransaction {
public:
	CoroTransaction() noexcept : CoroTransaction(errOK) {}
	CoroTransaction(const CoroTransaction&) = delete;
	CoroTransaction(CoroTransaction&&) = default;
	CoroTransaction& operator=(CoroTransaction&&) = default;
	CoroTransaction& operator=(const CoroTransaction&) = delete;

	Error Insert(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeInsert, lsn); }
	Error Update(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeUpdate, lsn); }
	Error Upsert(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeUpsert, lsn); }
	Error Delete(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeDelete, lsn); }
	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), mode, lsn); }

	Error Modify(Query&& query, lsn_t lsn = lsn_t());
	bool IsFree() const noexcept { return (conn_ == nullptr) || !status_.ok(); }
	Item NewItem();
	Error Status() const noexcept { return status_; }

private:
	friend class CoroRPCClient;
	friend class SyncCoroReindexerImpl;
	friend class SyncCoroTransaction;
	CoroTransaction(Error status) noexcept : status_(std::move(status)) {}
	CoroTransaction(CoroRPCClient* rpcClient, net::cproto::CoroClientConnection* conn, int64_t txId,
					std::chrono::milliseconds RequestTimeout, std::chrono::milliseconds execTimeout, std::string nsName)
		: txId_(txId),
		  rpcClient_(rpcClient),
		  conn_(conn),
		  requestTimeout_(RequestTimeout),
		  execTimeout_(execTimeout),
		  nsName_(std::move(nsName)) {}

	Error addTxItem(Item&& item, ItemModifyMode mode, lsn_t lsn);
	void clear() noexcept {
		txId_ = -1;
		rpcClient_ = nullptr;
		conn_ = nullptr;
		status_ = errOK;
	}

	int64_t txId_ = -1;
	CoroRPCClient* rpcClient_ = nullptr;
	reindexer::net::cproto::CoroClientConnection* conn_ = nullptr;
	std::chrono::milliseconds requestTimeout_ = std::chrono::milliseconds{0};
	std::chrono::milliseconds execTimeout_ = std::chrono::milliseconds{0};
	std::string nsName_;
	Error status_;
};

}  // namespace client
}  // namespace reindexer
