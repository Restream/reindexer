#include "proxiedtransaction.h"
#include <exception>
#include "client/itemimpl.h"
#include "client/reindexerimpl.h"
#include "core/item.h"
#include "core/itemimpl.h"
#include "core/queryresults/queryresults.h"
#include "estl/lock.h"
#include "tools/clusterproxyloghelper.h"
#include "transactionimpl.h"

namespace reindexer {

void ProxiedTransaction::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	client::Item clientItem;
	bool itemFromCache = false;
	try {
		{
			unique_lock lck(mtx_);
			if (itemCache_.isValid) {
				itemFromCache = true;
				clientItem = client::Item(
					new client::ItemImpl<client::ReindexerImpl>(itemCache_.pt, itemCache_.tm, nullptr, std::chrono::milliseconds()));
			} else {
				lck.unlock();
				clientItem = tx_.NewItem();
			}
		}
		if (!clientItem.Status().ok()) [[unlikely]] {
			throw clientItem.Status();
		}
		std::string_view serverCJson = item.impl_->GetCJSON(WithTagsMatcher_True);
		auto err = clientItem.Unsafe().FromCJSON(serverCJson);
		if (!err.ok()) [[unlikely]] {
			throw err;
		}
	} catch (std::exception& e) {
		if (!itemFromCache) [[unlikely]] {
			throw;
		}
		// Update cache, if got error on item conversion
		itemFromCache = false;
		clientItem = tx_.NewItem();
		if (!clientItem.Status().ok()) [[unlikely]] {
			throw clientItem.Status();
		}
		std::string_view serverCJson = item.impl_->GetCJSON(WithTagsMatcher_True);
		auto err = clientItem.Unsafe().FromCJSON(serverCJson);
		if (!err.ok()) [[unlikely]] {
			throw err;
		}
	}

	clientItem.SetPrecepts(item.impl_->GetPrecepts());

	if (clientItem.impl_->tagsMatcher().isUpdated()) {
		// Disable async logic for tm updates - next items may depend on this
		auto err = asyncData_.AwaitAsyncRequests();
		if (!err.ok()) [[unlikely]] {
			throw err;
		}

		unique_lock lck(mtx_);
		itemCache_.isValid = false;
		lck.unlock();

		tx_.modify(std::move(clientItem), mode, client::InternalRdxContext(lsn, nullptr, shardId_));
		return;
	}
	if (!itemFromCache) {
		lock_guard lck(mtx_);
		itemCache_.pt = clientItem.impl_->Type();
		itemCache_.tm = clientItem.impl_->tagsMatcher();
		itemCache_.isValid = true;
	}

	asyncData_.AddNewAsyncRequest();

	tx_.modify(std::move(clientItem), mode,
			   client::InternalRdxContext(lsn, [this](const Error& e) { asyncData_.OnAsyncRequestDone(e); }, shardId_));
}

void ProxiedTransaction::Modify(Query&& query, lsn_t lsn) {
	asyncData_.AddNewAsyncRequest();

	tx_.modify(std::move(query), client::InternalRdxContext(lsn, [this](const Error& e) { asyncData_.OnAsyncRequestDone(e); }, shardId_));
}

void ProxiedTransaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	asyncData_.AddNewAsyncRequest();

	tx_.putMeta(key, value, client::InternalRdxContext(lsn, [this](const Error& e) { asyncData_.OnAsyncRequestDone(e); }, shardId_));
}

void ProxiedTransaction::SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) {
	asyncData_.AddNewAsyncRequest();

	{
		lock_guard lck(mtx_);
		itemCache_.isValid = false;
	}
	tx_.setTagsMatcher(std::move(tm),
					   client::InternalRdxContext(lsn, [this](const Error& e) { asyncData_.OnAsyncRequestDone(e); }, shardId_));
}

void ProxiedTransaction::Rollback(int serverId, const RdxContext& ctx) noexcept {
	auto err = asyncData_.AwaitAsyncRequests();
	(void)err;	// ignore; Error does not matter here
	if (tx_.rx_) {
		const auto _ctx = client::InternalRdxContext(ctx.GetOriginLSN(), nullptr, shardId_).WithEmitterServerId(serverId);
		try {
			err = tx_.rx_->RollBackTransaction(tx_, _ctx);
			(void)err;	// ignore; Error does not matter here
		} catch (std::exception& e) {
			(void)e;  // ignore; Error does not matter here
		}
	}
}

void ProxiedTransaction::Commit(int serverId, QueryResults& result, const RdxContext& ctx) {
	auto err = asyncData_.AwaitAsyncRequests();
	if (!err.ok()) [[unlikely]] {
		throw err;
	}

	if (!tx_.Status().ok()) [[unlikely]] {
		throw tx_.Status();
	}

	assertrx(tx_.rx_);
	client::InternalRdxContext c;

	if (shardId_ < 0) {
		c = client::InternalRdxContext(ctx.GetOriginLSN()).WithEmitterServerId(serverId);
		clusterProxyLog(LogTrace, "[proxy] Proxying commit to leader. SID: {}", serverId);
	} else {
		c = client::InternalRdxContext{}.WithShardId(shardId_, false);
		clusterProxyLog(LogTrace, "[proxy] Proxying commit to shard {}. SID: {}", shardId_, serverId);
	}

	client::QueryResults clientResults;
	err = tx_.rx_->CommitTransaction(tx_, clientResults, c);
	if (!err.ok()) [[unlikely]] {
		throw err;
	}
	result.AddQr(std::move(clientResults), shardId_);
}

void ProxiedTransaction::AsyncData::AddNewAsyncRequest() {
	lock_guard lck(mtx_);
	if (!err_.ok()) [[unlikely]] {
		throw err_;
	}
	++asyncRequests_;
}

void ProxiedTransaction::AsyncData::OnAsyncRequestDone(const Error& e) noexcept {
	lock_guard lck(mtx_);
	if (!e.ok()) {
		err_ = e;
	}
	assertrx(asyncRequests_ > 0);
	if (--asyncRequests_ == 0) {
		cv_.notify_all();
	}
}

Error ProxiedTransaction::AsyncData::AwaitAsyncRequests() noexcept {
	unique_lock lck(mtx_);
	cv_.wait(lck, [this] { return asyncRequests_ == 0; });
	return err_;
}

}  // namespace reindexer
