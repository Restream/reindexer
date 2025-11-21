#include "corotransaction.h"
#include "client/corotransaction.h"
#include "client/itemimpl.h"
#include "client/namespace.h"
#include "client/rpcclient.h"
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer::client {

CoroTransaction::~CoroTransaction() {
	if (!IsFree()) {
		try {
			std::ignore = getConn()->Call({net::cproto::kCmdRollbackTx, i_.requestTimeout_, i_.execTimeout_, lsn_t(), -1,
										   ShardingKeyType::NotSetShard, nullptr, false, i_.sessionTs_},
										  i_.txId_);
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: unexpected exception in ~CoroTransaction: %s\n", e.what());
			assertrx_dbg(false);
		}
	}
}

Error CoroTransaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) {
	if (!i_.rpcClient_) {
		return Error(errLogic, "Connection pointer in transaction is nullptr");
	}
	return i_.rpcClient_->conn_
		.Call({net::cproto::kCmdPutTxMeta, i_.requestTimeout_, i_.execTimeout_, lsn, -1, ShardingKeyType::NotSetShard, nullptr, false,
			   i_.sessionTs_},
			  key, value, i_.txId_)
		.Status();
}

Error CoroTransaction::SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) {
	if (!i_.rpcClient_) {
		return Error(errLogic, "Connection pointer in transaction is nullptr");
	}
	*i_.localTm_ = std::move(tm);
	WrSerializer ser;
	i_.localTm_->serialize(ser);
	return i_.rpcClient_->conn_
		.Call({net::cproto::kCmdSetTagsMatcherTx, i_.requestTimeout_, i_.execTimeout_, lsn, -1, ShardingKeyType::NotSetShard, nullptr,
			   false, i_.sessionTs_},
			  int64_t(i_.localTm_->stateToken()), int64_t(i_.localTm_->version()), ser.Slice(), i_.txId_)
		.Status();
}

Error CoroTransaction::Modify(Query&& query, lsn_t lsn) {
	if (!i_.rpcClient_) {
		return Error(errLogic, "Connection pointer in transaction is nullptr.");
	}
	WrSerializer ser;
	query.Serialize(ser);
	switch (query.type_) {
		case QueryUpdate: {
			return i_.rpcClient_->conn_
				.Call({cproto::kCmdUpdateQueryTx, i_.requestTimeout_, i_.execTimeout_, lsn, -1, ShardingKeyType::NotSetShard, nullptr,
					   false, i_.sessionTs_},
					  ser.Slice(), i_.txId_)
				.Status();
		}
		case QueryDelete: {
			return i_.rpcClient_->conn_
				.Call({cproto::kCmdDeleteQueryTx, i_.requestTimeout_, i_.execTimeout_, lsn, -1, ShardingKeyType::NotSetShard, nullptr,
					   false, i_.sessionTs_},
					  ser.Slice(), i_.txId_)
				.Status();
		}
		case QuerySelect:
		case QueryTruncate:
		default:
			return Error(errParams, "Incorrect query type in transaction modify {}", int(query.type_));
	}
}

Error CoroTransaction::addTxItem(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	if (!i_.rpcClient_ || !i_.ns_) {
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
		if (!err.ok()) {
			return err;
		}
		item = std::move(newItem);
	}

	for (int tryCount = 0;; ++tryCount) {
		itData = item.GetCJSON();
		int stateToken = item.GetStateToken();
		itemData = p_string(&itData);

		err = i_.rpcClient_->conn_
				  .Call({net::cproto::kCmdAddTxItem, i_.requestTimeout_, i_.execTimeout_, lsn, -1, ShardingKeyType::NotSetShard, nullptr,
						 false, i_.sessionTs_},
						FormatCJson, itemData, mode, ser.Slice(), stateToken, i_.txId_)
				  .Status();
		if (err.ok()) {
			break;
		}
		if (err.code() != errStateInvalidated || tryCount > 2) {
			return err;
		}

		CoroQueryResults qr;
		InternalRdxContext ctx = InternalRdxContext{}.WithTimeout(i_.execTimeout_).WithShardId(ShardingKeyType::ProxyOff, false);
		err = i_.rpcClient_->Select(Query(i_.ns_->name).Limit(0), qr, ctx);
		if (!err.ok()) {
			return Error(errLogic, "Can't update TagsMatcher");
		}

		auto nsTm = i_.ns_->GetTagsMatcher();
		if (nsTm.stateToken() != i_.localTm_->stateToken()) {
			WrSerializer wrser;
			nsTm.serialize(wrser);
			std::string_view buf = wrser.Slice();
			Serializer ser(buf.data(), buf.length());
			i_.localTm_->deserialize(ser, nsTm.version(), nsTm.stateToken());
		}

		newItem = NewItem();
		err = mergeTmFromItem(item, newItem);
		if (!err.ok()) {
			return err;
		}

		item = std::move(newItem);
	}
	return Error();
}

Error CoroTransaction::addTxItemRaw(std::string_view cjson, ItemModifyMode mode, lsn_t lsn) {
	if (!i_.rpcClient_ || !i_.ns_) {
		return Error(errLogic, "Connection pointer in transaction is nullptr");
	}
	if (ItemImplBase::HasBundledTm(cjson)) {
		return Error(errParams, "Raw CJSON interface does not support CJSON with bundled tags matcher");
	}

	return i_.rpcClient_->conn_
		.Call({net::cproto::kCmdAddTxItem, i_.requestTimeout_, i_.execTimeout_, lsn, -1, ShardingKeyType::NotSetShard, nullptr, false,
			   i_.sessionTs_},
			  FormatCJson, cjson, mode, std::string_view(), int(i_.localTm_->stateToken()), i_.txId_)
		.Status();
}

Error CoroTransaction::mergeTmFromItem(Item& item, Item& rItem) {
	bool itemTmMergeSucceed = true;
	if (item.IsTagsUpdated() || item.GetStateToken() != rItem.GetStateToken()) {
		if (i_.localTm_->try_merge(item.impl_->tagsMatcher())) {
			rItem = NewItem();
			item.impl_->tagsMatcher().setUpdated();
		} else {
			itemTmMergeSucceed = false;
		}
	}
	if (itemTmMergeSucceed) {
		auto err = rItem.Unsafe().FromCJSON(item.impl_->GetCJSON());
		if (!err.ok()) {
			return err;
		}
	} else {
		auto err = rItem.Unsafe().FromJSON(item.impl_->GetJSON());
		if (!err.ok()) {
			return err;
		}
		if (rItem.IsTagsUpdated() && !i_.localTm_->try_merge(rItem.impl_->tagsMatcher())) {
			return Error(errLogic, "Unable to merge item's TagsMatcher.");
		}
		rItem.impl_->tagsMatcher() = *i_.localTm_;
		rItem.impl_->tagsMatcher().setUpdated();
	}
	return Error();
}

net::cproto::CoroClientConnection* CoroTransaction::getConn() const noexcept { return i_.rpcClient_ ? &i_.rpcClient_->conn_ : nullptr; }

Item CoroTransaction::NewItem() { return NewItem(i_.rpcClient_); }

TagsMatcher CoroTransaction::GetTagsMatcher() const noexcept { return *i_.localTm_; }

PayloadType CoroTransaction::GetPayloadType() const noexcept {
	assert(i_.ns_);
	return i_.ns_->payloadType;
}

template <typename ClientT>
Item CoroTransaction::NewItem(ClientT* client) {
	if (IsFree()) {
		return Item(Error(errLogic, "Transaction was not initialized"));
	}
	Item item(new ItemImpl<ClientT>(i_.ns_->payloadType, *i_.localTm_, client, i_.requestTimeout_));
	item.impl_->tagsMatcher().clearUpdated();
	return item;
}

template Item CoroTransaction::NewItem<ReindexerImpl>(ReindexerImpl* client);

CoroTransaction::Impl::Impl(RPCClient* rpcClient, int64_t txId, std::chrono::milliseconds requestTimeout,
							std::chrono::milliseconds execTimeout, std::shared_ptr<Namespace>&& ns, int emitterServerId) noexcept
	: txId_(txId),
	  rpcClient_(rpcClient),
	  requestTimeout_(requestTimeout),
	  execTimeout_(execTimeout),
	  localTm_(std::make_unique<TagsMatcher>(ns->GetTagsMatcher())),
	  ns_(std::move(ns)),
	  emitterServerId_(emitterServerId) {
	assert(rpcClient_);
	assert(ns_);
	const auto sessinTsOpt = rpcClient_->conn_.LoginTs();
	if (sessinTsOpt.has_value()) {
		sessionTs_ = sessinTsOpt.value();
	}
}

// NOLINTNEXTLINE (bugprone-throw-keyword-missing)
CoroTransaction::Impl::Impl(Error&& status) noexcept : status_(std::move(status)), localTm_(std::make_unique<TagsMatcher>()) {}

CoroTransaction::Impl::Impl(CoroTransaction::Impl&&) noexcept = default;
CoroTransaction::Impl& CoroTransaction::Impl::operator=(CoroTransaction::Impl&&) noexcept = default;
CoroTransaction::Impl::~Impl() = default;

}  // namespace reindexer::client
