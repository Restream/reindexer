#pragma once
#include <chrono>
#include "client/item.h"
#include "core/query/query.h"

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
	Error Insert(Item&& item) { return addTxItem(std::move(item), ModeInsert); }
	Error Update(Item&& item) { return addTxItem(std::move(item), ModeUpdate); }
	Error Upsert(Item&& item) { return addTxItem(std::move(item), ModeUpsert); }
	Error Delete(Item&& item) { return addTxItem(std::move(item), ModeDelete); }
	Error Modify(Item&& item, ItemModifyMode mode) { return addTxItem(std::move(item), mode); }

	Error Modify(Query&& query);
	bool IsFree() const { return (conn_ == nullptr) || !status_.ok(); }
	Item NewItem();
	Error Status() const { return status_; }

private:
	friend class RPCClient;
	friend class CoroRPCClient;
	CoroTransaction(Error status) : status_(std::move(status)) {}
	CoroTransaction(CoroRPCClient* rpcClient, net::cproto::CoroClientConnection* conn, int64_t txId, std::chrono::seconds RequestTimeout,
					std::chrono::milliseconds execTimeout, std::string nsName)
		: txId_(txId),
		  rpcClient_(rpcClient),
		  conn_(conn),
		  RequestTimeout_(RequestTimeout),
		  execTimeout_(execTimeout),
		  nsName_(std::move(nsName)) {}

	Error addTxItem(Item&& item, ItemModifyMode mode);
	void clear() {
		txId_ = -1;
		rpcClient_ = nullptr;
		conn_ = nullptr;
		status_ = errOK;
	}

	int64_t txId_ = -1;
	CoroRPCClient* rpcClient_ = nullptr;
	reindexer::net::cproto::CoroClientConnection* conn_ = nullptr;
	std::chrono::seconds RequestTimeout_;
	std::chrono::milliseconds execTimeout_;
	std::string nsName_;
	Error status_;
};

}  // namespace client
}  // namespace reindexer
