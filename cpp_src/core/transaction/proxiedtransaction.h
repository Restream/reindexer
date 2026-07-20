#pragma once

#include "client/transaction.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "estl/condition_variable.h"
#include "estl/mutex.h"

namespace reindexer {

class Item;
class SharedTransactionData;
class RdxContext;
class ReindexerImpl;
class QueryResults;

class [[nodiscard]] ProxiedTransaction {
public:
	ProxiedTransaction(client::Transaction&& _tx, int shardId) : tx_(std::move(_tx)), shardId_(shardId), asyncData_(mtx_) {}

	Error Modify(Item&& item, ItemModifyMode mode, lsn_t lsn);
	Error Modify(Query&& query, lsn_t lsn);
	Error PutMeta(std::string_view key, std::string_view value, lsn_t lsn);
	Error SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn);
	void Rollback(int serverId, const RdxContext& ctx);
	Error Commit(int serverId, QueryResults& result, const RdxContext& ctx);

private:
	class [[nodiscard]] AsyncData {
	public:
		AsyncData(mutex& mtx) noexcept : mtx_(mtx) {}
		void AddNewAsyncRequest();
		void OnAsyncRequestDone(const Error& e) noexcept;
		Error AwaitAsyncRequests() noexcept;
		~AsyncData() {
			auto err = AwaitAsyncRequests();
			(void)err;	// ignore
		}

	private:
		mutex& mtx_;
		condition_variable cv_;
		Error err_;
		unsigned asyncRequests_ = 0;
	};
	struct [[nodiscard]] ItemCache {
		PayloadType pt;
		TagsMatcher tm;
		bool isValid = false;
	};

	client::Transaction tx_;
	int shardId_ = ShardingKeyType::NotSetShard;
	mutex mtx_;
	AsyncData asyncData_;
	ItemCache itemCache_;
};

}  // namespace reindexer
