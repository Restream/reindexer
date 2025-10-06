#pragma once

#include "client/item.h"
#include "core/query/query.h"
#include "tools/clock.h"
#include "tools/lsn.h"

namespace reindexer {

namespace net::cproto {
class CoroClientConnection;
}  // namespace net::cproto

namespace client {

class Namespace;
class RPCClient;

class [[nodiscard]] CoroTransaction {
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
	Error Insert(std::string_view cjson, lsn_t lsn) { return addTxItemRaw(cjson, ModeInsert, lsn); }
	Error Update(std::string_view cjson, lsn_t lsn) { return addTxItemRaw(cjson, ModeUpdate, lsn); }
	Error Upsert(std::string_view cjson, lsn_t lsn) { return addTxItemRaw(cjson, ModeUpsert, lsn); }
	Error Delete(std::string_view cjson, lsn_t lsn) { return addTxItemRaw(cjson, ModeDelete, lsn); }
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
	friend class RPCClient;
	friend class ReindexerImpl;
	friend class Transaction;
	explicit CoroTransaction(Error status) noexcept : i_(std::move(status)) {}
	CoroTransaction(RPCClient* rpcClient, int64_t txId, std::chrono::milliseconds requestTimeout, std::chrono::milliseconds execTimeout,
					std::shared_ptr<Namespace>&& ns, int emitterServerId) noexcept
		: i_(rpcClient, txId, requestTimeout, execTimeout, std::move(ns), emitterServerId) {}

	Error addTxItem(Item&& item, ItemModifyMode mode, lsn_t lsn);
	Error addTxItemRaw(std::string_view cjson, ItemModifyMode mode, lsn_t lsn);
	void clear() noexcept {
		i_.txId_ = -1;
		i_.rpcClient_ = nullptr;
		i_.status_ = errOK;
	}
	Error mergeTmFromItem(Item& item, Item& rItem);
	net::cproto::CoroClientConnection* getConn() const noexcept;
	void setStatus(Error&& status) noexcept { i_.status_ = std::move(status); }

	struct [[nodiscard]] Impl {
		Impl(RPCClient* rpcClient, int64_t txId, std::chrono::milliseconds requestTimeout, std::chrono::milliseconds execTimeout,
			 std::shared_ptr<Namespace>&& ns, int emitterServerId) noexcept;
		Impl(Error&& status) noexcept;
		Impl(Impl&&) noexcept;
		Impl& operator=(Impl&&) noexcept;
		~Impl();

		int64_t txId_{-1};
		RPCClient* rpcClient_{nullptr};
		std::chrono::milliseconds requestTimeout_{0};
		std::chrono::milliseconds execTimeout_{0};
		Error status_;
		std::unique_ptr<TagsMatcher> localTm_;
		std::shared_ptr<Namespace> ns_;
		steady_clock_w::time_point sessionTs_;
		int emitterServerId_ = -1;
	};

	Impl i_;
};

}  // namespace client

}  // namespace reindexer
