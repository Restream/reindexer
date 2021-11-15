#include "corotransaction.h"
#include "client/cororpcclient.h"
#include "client/itemimpl.h"
#include "client/namespace.h"
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

CoroTransaction::~CoroTransaction() = default;
CoroTransaction::CoroTransaction(CoroTransaction&&) noexcept = default;
CoroTransaction& CoroTransaction::operator=(CoroTransaction&&) noexcept = default;

Error CoroTransaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (!rpcClient_) {
		return Error(errLogic, "Connection pointer in transaction is nullptr");
	}
	return rpcClient_->conn_
		.Call({net::cproto::kCmdPutTxMeta, requestTimeout_, execTimeout_, lsn, -1, IndexValueType::NotSet, nullptr}, key, value, txId_)
		.Status();
}

Error CoroTransaction::Modify(Query&& query, lsn_t lsn) {
	if (!rpcClient_) {
		return Error(errLogic, "Connection pointer in transaction is nullptr.");
	}
	WrSerializer ser;
	query.Serialize(ser);
	switch (query.type_) {
		case QueryUpdate: {
			return rpcClient_->conn_
				.Call({cproto::kCmdUpdateQueryTx, requestTimeout_, execTimeout_, lsn, -1, IndexValueType::NotSet, nullptr}, ser.Slice(),
					  txId_)
				.Status();
		}
		case QueryDelete: {
			return rpcClient_->conn_
				.Call({cproto::kCmdDeleteQueryTx, requestTimeout_, execTimeout_, lsn, -1, IndexValueType::NotSet, nullptr}, ser.Slice(),
					  txId_)
				.Status();
		}
		default:
			return Error(errParams, "Incorrect query type in transaction modify %d", query.type_);
	}
}

Error CoroTransaction::addTxItem(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	if (!rpcClient_ || !ns_) {
		return Error(errLogic, "Connection pointer in transaction is nullptr");
	}

	WrSerializer ser;
	// Precepts have to be serialized before new item creation
	item.impl_->GetPrecepts(ser);

	std::string_view itData;
	p_string itemData;
	Item newItem;
	Error err;
	if (item.IsTagsUpdated()) {
		newItem = NewItem();
		err = mergeTmFromItem(item, newItem);
		if (!err.ok()) return err;
		item = std::move(newItem);
	}

	for (int tryCount = 0;; ++tryCount) {
		itData = item.GetCJSON();
		int stateToken = item.GetStateToken();
		itemData = p_string(&itData);

		err = rpcClient_->conn_
				  .Call({net::cproto::kCmdAddTxItem, requestTimeout_, execTimeout_, lsn, -1, IndexValueType::NotSet, nullptr}, FormatCJson,
						itemData, mode, ser.Slice(), stateToken, txId_)
				  .Status();
		if (err.ok()) {
			break;
		}
		if (err.code() != errStateInvalidated || tryCount > 2) {
			return err;
		}

		CoroQueryResults qr;
		InternalRdxContext ctx;
		ctx = ctx.WithTimeout(execTimeout_).WithShardId(ShardingKeyType::ShardingProxyOff);
		err = rpcClient_->Select(Query(ns_->name).Limit(0), qr, ctx);
		if (!err.ok()) return Error(errLogic, "Can't update TagsMatcher");

		auto nsTm = ns_->GetTagsMatcher();
		if (nsTm.stateToken() != localTm_->stateToken()) {
			WrSerializer wrser;
			nsTm.serialize(wrser);
			std::string_view buf = wrser.Slice();
			Serializer ser(buf.data(), buf.length());
			localTm_->deserialize(ser, nsTm.version(), nsTm.stateToken());
		}

		newItem = NewItem();
		err = mergeTmFromItem(item, newItem);
		if (!err.ok()) return err;

		item = std::move(newItem);
	}
	return Error();
}

Error CoroTransaction::mergeTmFromItem(Item& item, Item& rItem) {
	bool itemTmMergeSucceed = true;
	if (item.IsTagsUpdated()) {
		if (localTm_->try_merge(item.impl_->tagsMatcher())) {
			rItem = NewItem();
		} else {
			itemTmMergeSucceed = false;
		}
	}
	if (itemTmMergeSucceed) {
		auto err = rItem.Unsafe().FromCJSON(item.impl_->GetCJSON());
		if (!err.ok()) return err;
	} else {
		auto err = rItem.Unsafe().FromJSON(item.impl_->GetJSON());
		if (!err.ok()) return err;
		if (rItem.IsTagsUpdated() && !localTm_->try_merge(rItem.impl_->tagsMatcher()))
			return Error(errLogic, "Unable to merge item's TagsMatcher.");
		rItem.impl_->tagsMatcher() = *localTm_;
		rItem.impl_->tagsMatcher().setUpdated();
	}
	return Error();
}

net::cproto::CoroClientConnection* CoroTransaction::getConn() const noexcept { return rpcClient_ ? &rpcClient_->conn_ : nullptr; }

Item CoroTransaction::NewItem() { return NewItem(rpcClient_); }

TagsMatcher CoroTransaction::GetTagsMatcher() const noexcept { return *localTm_; }

PayloadType CoroTransaction::GetPayloadType() const noexcept {
	assert(ns_);
	return ns_->payloadType;
}

CoroTransaction::CoroTransaction(Error status) noexcept : status_(std::move(status)), localTm_(std::make_unique<TagsMatcher>()) {}

CoroTransaction::CoroTransaction(CoroRPCClient* rpcClient, int64_t txId, std::chrono::milliseconds RequestTimeout,
								 std::chrono::milliseconds execTimeout, Namespace* ns)
	: txId_(txId),
	  rpcClient_(rpcClient),
	  requestTimeout_(RequestTimeout),
	  execTimeout_(execTimeout),
	  localTm_(std::make_unique<TagsMatcher>(ns->GetTagsMatcher())),
	  ns_(ns) {
	assert(ns_);
}

template <typename ClientT>
Item CoroTransaction::NewItem(ClientT* client) {
	if (IsFree()) return Item(Error(errLogic, "Transaction was not initialized"));
	Item item(new ItemImpl<ClientT>(ns_->payloadType, *localTm_, client, requestTimeout_));
	item.impl_->tagsMatcher().clearUpdated();
	return item;
}

template Item CoroTransaction::NewItem<SyncCoroReindexerImpl>(SyncCoroReindexerImpl* client);

}  // namespace client
}  // namespace reindexer
