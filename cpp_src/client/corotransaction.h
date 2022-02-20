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
	CoroTransaction(CoroTransaction&& o) noexcept : i_(std::move(o.i_)) { o.clear(); }
	CoroTransaction& operator=(CoroTransaction&& o) noexcept {
		if (this != &o) {
			i_ = std::move(o.i_);
			o.clear();
		}
		return *this;
	}
	CoroTransaction& operator=(const CoroTransaction&) = delete;

	Error Insert(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeInsert, lsn); }
	Error Update(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeUpdate, lsn); }
	Error Upsert(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeUpsert, lsn); }
	Error Delete(Item&& item, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), ModeDelete, lsn); }
	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t()) { return addTxItem(std::move(item), mode, lsn); }
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());
	Error SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn);

	Error Modify(Query&& query, lsn_t lsn = lsn_t());
	bool IsFree() const noexcept { return (i_.rpcClient_ == nullptr) || i_.txId_ < 0; }
	Item NewItem();
	template <typename ClientT>
	Item NewItem(ClientT*);
	const Error& Status() const noexcept { return i_.status_; }
	TagsMatcher GetTagsMatcher() const noexcept;
	PayloadType GetPayloadType() const noexcept;

private:
	friend class CoroRPCClient;
	friend class SyncCoroReindexerImpl;
	friend class SyncCoroTransaction;
	CoroTransaction(Error status) noexcept : i_(std::move(status)) {}
	CoroTransaction(CoroRPCClient* rpcClient, int64_t txId, std::chrono::milliseconds requestTimeout, std::chrono::milliseconds execTimeout,
					Namespace* ns) noexcept
		: i_(rpcClient, txId, requestTimeout, execTimeout, ns) {}

	Error addTxItem(Item&& item, ItemModifyMode mode, lsn_t lsn);
	void clear() noexcept {
		i_.txId_ = -1;
		i_.rpcClient_ = nullptr;
		i_.status_ = errOK;
	}
	Error mergeTmFromItem(Item& item, Item& rItem);
	net::cproto::CoroClientConnection* getConn() const noexcept;
	void setStatus(Error&& status) noexcept { i_.status_ = std::move(status); }

	struct Impl {
		Impl(CoroRPCClient* rpcClient, int64_t txId, std::chrono::milliseconds requestTimeout, std::chrono::milliseconds execTimeout,
			 Namespace* ns) noexcept;
		Impl(Error&& status) noexcept;
		Impl(Impl&&);
		Impl& operator=(Impl&&);
		~Impl();

		int64_t txId_ = -1;
		CoroRPCClient* rpcClient_ = nullptr;
		std::chrono::milliseconds requestTimeout_ = std::chrono::milliseconds{0};
		std::chrono::milliseconds execTimeout_ = std::chrono::milliseconds{0};
		Error status_;
		std::unique_ptr<TagsMatcher> localTm_;
		Namespace* ns_ = nullptr;
		std::chrono::steady_clock::time_point sessionTs_;
	};

	Impl i_;
};

}  // namespace client
}  // namespace reindexer
