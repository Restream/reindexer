#pragma once
#include <chrono>
#include "client/item.h"
#include "core/query/query.h"

namespace reindexer {

namespace net {
namespace cproto {

class ClientConnection;
}  // namespace cproto
}  // namespace net

namespace client {

class RPCClient;

class Transaction {
public:
	void Insert(Item&& item) { addTxItem(std::move(item), ModeInsert); }
	void Update(Item&& item) { addTxItem(std::move(item), ModeUpdate); }
	void Upsert(Item&& item) { addTxItem(std::move(item), ModeUpsert); }
	void Delete(Item&& item) { addTxItem(std::move(item), ModeDelete); }
	void Modify(Item&& item, ItemModifyMode mode) { addTxItem(std::move(item), mode); }

	void Modify(Query&& query);
	bool IsFree() const { return (conn_ == nullptr) || !status_.ok(); }
	Item NewItem();
	Error Status() const { return status_; }

private:
	friend class RPCClient;
	Transaction(Error status) : status_(std::move(status)) {}
	Transaction(RPCClient* rpcClient, net::cproto::ClientConnection* conn, int64_t txId, std::chrono::seconds RequestTimeout,
				std::chrono::milliseconds execTimeout, std::string nsName)
		: txId_(txId),
		  rpcClient_(rpcClient),
		  conn_(conn),
		  RequestTimeout_(RequestTimeout),
		  execTimeout_(execTimeout),
		  nsName_(std::move(nsName)) {}

	void addTxItem(Item&& item, ItemModifyMode mode);
	void clear() {
		txId_ = -1;
		rpcClient_ = nullptr;
		conn_ = nullptr;
		status_ = errOK;
	}

	int64_t txId_ = -1;
	RPCClient* rpcClient_ = nullptr;
	reindexer::net::cproto::ClientConnection* conn_ = nullptr;
	std::chrono::seconds RequestTimeout_;
	std::chrono::milliseconds execTimeout_;
	std::string nsName_;
	Error status_;
};

}  // namespace client
}  // namespace reindexer
