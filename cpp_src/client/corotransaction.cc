#include "corotransaction.h"
#include "client/cororpcclient.h"
#include "client/itemimpl.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer {
namespace client {

Error CoroTransaction::Modify(Query&& query) {
	if (conn_) {
		WrSerializer ser;
		query.Serialize(ser);
		return conn_->Call({cproto::kCmdUpdateQueryTx, RequestTimeout_, execTimeout_, nullptr}, ser.Slice(), txId_).Status();
	}
	return Error(errLogic, "Connection pointer in transaction is nullptr.");
}

Error CoroTransaction::addTxItem(Item&& item, ItemModifyMode mode) {
	auto itData = item.GetJSON();
	p_string itemData(&itData);
	if (conn_) {
		for (int tryCount = 0;; tryCount++) {
			auto ret =
				conn_->Call({net::cproto::kCmdAddTxItem, RequestTimeout_, execTimeout_, nullptr}, FormatJson, itemData, mode, "", 0, txId_);

			if (!ret.Status().ok()) {
				if (ret.Status().code() != errStateInvalidated || tryCount > 2) {
					return ret.Status();
				}

				CoroQueryResults qr;
				InternalRdxContext ctx;
				ctx = ctx.WithTimeout(execTimeout_);
				auto err = rpcClient_->Select(Query(nsName_).Limit(0), qr, ctx);
				if (!err.ok()) {
					return Error(errLogic, "Can't update TagsMatcher");
				}

				auto newItem = NewItem();
				char* endp = nullptr;
				err = newItem.FromJSON(item.impl_->GetJSON(), &endp);
				if (!err.ok()) {
					return err;
				}
				item = std::move(newItem);
			} else {
				break;
			}
		}
		return errOK;
	}
	return Error(errLogic, "Connection pointer in transaction is nullptr.");
}

Item CoroTransaction::NewItem() {
	if (!rpcClient_) {
		return Item(Error(errLogic, "rpcClient not set for client transaction"));
	}
	return rpcClient_->NewItem(nsName_);
}
}  // namespace client
}  // namespace reindexer
