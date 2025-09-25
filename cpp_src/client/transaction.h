#pragma once

#include "client/corotransaction.h"
#include "client/item.h"

namespace reindexer {

class ProxiedTransaction;

namespace client {

class InternalRdxContext;

class [[nodiscard]] Transaction {
public:
	using Completion = std::function<void(const Error& err)>;
	Error Insert(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeInsert, lsn); }
	Error Update(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeUpdate, lsn); }
	Error Upsert(Item&& item, Completion cmpl, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeUpsert, std::move(cmpl), lsn); }
	Error Upsert(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeUpsert, lsn); }
	Error Delete(Item&& item, lsn_t lsn = lsn_t()) { return Modify(std::move(item), ModeDelete, lsn); }
	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn = lsn_t());
	Error Modify(Item&& item, ItemModifyMode mode, Completion cmpl, lsn_t lsn = lsn_t());
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());
	Error SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn);
	Error Modify(Query&& query, lsn_t lsn = lsn_t());
	bool IsFree() const noexcept { return !rx_ || tr_.IsFree(); }
	Item NewItem();
	const Error& Status() const noexcept { return tr_.Status(); }

	Transaction(Transaction&) = delete;
	Transaction& operator=(const Transaction&) = delete;
	Transaction(Transaction&&) noexcept = default;
	Transaction& operator=(Transaction&&) = default;
	~Transaction();

	PayloadType GetPayloadType() const;
	TagsMatcher GetTagsMatcher() const;

	int64_t GetTransactionId() const noexcept;

private:
	Error modify(Item&& item, ItemModifyMode mode, InternalRdxContext&& ctx);
	Error modify(Query&& query, InternalRdxContext&& ctx);
	Error putMeta(std::string_view key, std::string_view value, InternalRdxContext&& ctx);
	Error setTagsMatcher(TagsMatcher&& tm, InternalRdxContext&& ctx);

	friend class Reindexer;
	friend class ReindexerImpl;
	friend class reindexer::ClusterProxy;
	friend class reindexer::ProxiedTransaction;
	Transaction(std::shared_ptr<ReindexerImpl> rx, CoroTransaction&& tr) noexcept : tr_(std::move(tr)), rx_(std::move(rx)) {}
	Transaction(Error status) noexcept : tr_(std::move(status)) {}
	void setStatus(Error&& status) noexcept { tr_.setStatus(std::move(status)); }
	const net::cproto::CoroClientConnection* coroConnection() const noexcept { return tr_.getConn(); }

	CoroTransaction tr_;
	std::shared_ptr<ReindexerImpl> rx_;
};

}  // namespace client
}  // namespace reindexer
