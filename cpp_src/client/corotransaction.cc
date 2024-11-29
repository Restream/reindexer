#include "corotransaction.h"
#include "client/cororpcclient.h"
#include "client/itemimpl.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer::client {

Error CoroTransaction::Modify(Query&& query) {
	if (!conn_) {
		return {errLogic, "Connection pointer in transaction is nullptr."};
	}

	WrSerializer ser;
	query.Serialize(ser);
	return conn_->Call({cproto::kCmdUpdateQueryTx, RequestTimeout_, execTimeout_, nullptr}, ser.Slice(), txId_).Status();
}

Error CoroTransaction::addTxItem(Item&& item, ItemModifyMode mode) {
	if (!conn_) {
		return {errLogic, "Connection pointer in transaction is nullptr."};
	}

	WrSerializer ser;
	if (item.impl_->GetPrecepts().size()) {
		ser.PutVarUint(item.impl_->GetPrecepts().size());
		for (auto& p : item.impl_->GetPrecepts()) {
			ser.PutVString(p);
		}
	}

	auto itData = item.GetJSON();
	p_string itemData(&itData);

	for (int tryCount = 0;; ++tryCount) {
		auto ret = conn_->Call({net::cproto::kCmdAddTxItem, RequestTimeout_, execTimeout_, nullptr}, FormatJson, itemData, mode,
							   ser.Slice(), 0, txId_);
		if (ret.Status().ok()) {
			break;
		}

		if (ret.Status().code() != errStateInvalidated || tryCount > 2) {
			return ret.Status();
		}

		CoroQueryResults qr;
		InternalRdxContext ctx;
		ctx = ctx.WithTimeout(execTimeout_);
		auto err = rpcClient_->Select(Query(nsName_).Limit(0), qr, ctx);
		if (!err.ok()) {
			return {errLogic, "Can't update TagsMatcher"};
		}

		auto newItem = NewItem();

		char* endp = nullptr;
		err = newItem.FromJSON(item.impl_->GetJSON(), &endp);
		if (!err.ok()) {
			return err;
		}

		item = std::move(newItem);
	}
	return errOK;
}

Item CoroTransaction::NewItem() {
	if (!rpcClient_) {
		return Item(Error(errLogic, "rpcClient not set for client transaction"));
	}
	return rpcClient_->NewItem(nsName_);
}

}  // namespace reindexer::client
