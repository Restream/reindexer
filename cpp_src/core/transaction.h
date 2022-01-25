#pragma once
#include "client/synccororeindexer.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

namespace client {
class SyncCoroReindexer;
class SyncCoroReindexerImpl;
}  // namespace client

namespace sharding {
class LocatorService;
}

class TransactionImpl;
class TransactionStep;
class PayloadType;
class TagsMatcher;
class FieldsSet;

namespace client {
class SyncCoroTransaction;
}

class Transaction {
public:
	using time_point = std::chrono::time_point<std::chrono::high_resolution_clock>;

	Transaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
				std::shared_ptr<const Schema> schema, lsn_t lsn);
	Transaction(Error err);

	~Transaction();
	Transaction();
	Transaction(Transaction &&) noexcept;
	Transaction &operator=(Transaction &&) noexcept;

	Error Insert(Item &&item, lsn_t lsn = lsn_t());
	Error Update(Item &&item, lsn_t lsn = lsn_t());
	Error Upsert(Item &&item, lsn_t lsn = lsn_t());
	Error Delete(Item &&item, lsn_t lsn = lsn_t());
	Error Modify(Item &&item, ItemModifyMode mode, lsn_t lsn = lsn_t());
	Error Modify(Query &&query, lsn_t lsn = lsn_t());
	void Nop(lsn_t lsn);
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn = lsn_t());
	Error SetTagsMatcher(TagsMatcher &&tm, lsn_t lsn);
	bool IsFree() const noexcept { return impl_ == nullptr; }
	Item NewItem();
	Item GetItem(TransactionStep &&st);
	Error Status() const noexcept { return status_; }
	lsn_t GetLSN() const noexcept;

	const std::string &GetNsName();

	friend class ClusterProxy;

	vector<TransactionStep> &GetSteps() noexcept;
	const vector<TransactionStep> &GetSteps() const noexcept;
	bool IsTagsUpdated() const noexcept;
	time_point GetStartTime() const noexcept;

	void SetClient(client::SyncCoroTransaction &&tx);
	void SetLeader(client::SyncCoroReindexer &&);
	void SetShardingRouter(std::shared_ptr<sharding::LocatorService>);

protected:
#ifdef WITH_SHARDING
	void updateShardIdIfNecessary(int shardId);
	void ensureShardIdIsCorrect(const Item &item);
	void ensureShardIdIsCorrect(const Query &q);
#endif	// WITH_SHARDING

	std::unique_ptr<TransactionImpl> impl_;
	std::unique_ptr<client::SyncCoroTransaction> clientTransaction_;
	Error status_;
	std::shared_ptr<sharding::LocatorService> shardingRouter_;
	std::optional<client::SyncCoroReindexer> leaderRx_;
	int shardId_ = IndexValueType::NotSet;
};

}  // namespace reindexer
