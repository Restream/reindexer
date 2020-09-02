#include "transaction.h"
#include "client/itemimpl.h"
#include "client/rpcclient.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/clientconnection.h"
#include "net/cproto/cproto.cc"

namespace reindexer {
namespace client {

void Transaction::Modify(Query&& query) {
	if (conn_) {
		WrSerializer ser;
		query.Serialize(ser);
		auto ret = conn_->Call({cproto::kCmdUpdateQueryTx, RequestTimeout_, execTimeout_}, ser.Slice(), txId_).Status();
		if (!ret.ok()) throw ret;
	}
	throw Error(errLogic, "Connection pointer in transaction is nullptr.");
}

void Transaction::addTxItem(Item&& item, ItemModifyMode mode) {
	auto itData = item.GetJSON();
	p_string itemData(&itData);
	if (conn_) {
		for (int tryCount = 0;; tryCount++) {
			auto ret = conn_->Call({net::cproto::kCmdAddTxItem, RequestTimeout_, execTimeout_}, FormatJson, itemData, mode, "", 0, txId_);

			if (!ret.Status().ok()) {
				if (ret.Status().code() != errStateInvalidated || tryCount > 2) throw ret.Status();

				QueryResults qr;
				InternalRdxContext ctx;
				ctx = ctx.WithTimeout(execTimeout_);
				auto err = rpcClient_->Select(Query(nsName_).Limit(0), qr, ctx, conn_);
				if (!err.ok()) throw Error(errLogic, "Can't update TagsMatcher");

				auto newItem = NewItem();
				char* endp = nullptr;
				err = newItem.FromJSON(item.impl_->GetJSON(), &endp);
				if (!err.ok()) throw err;
				item = std::move(newItem);
			} else {
				break;
			}
		}
	} else
		throw Error(errLogic, "Connection pointer in transaction is nullptr.");
}

Item Transaction::NewItem() {
	if (!rpcClient_) throw Error(errLogic, "rpcClient not set for client transaction");
	return rpcClient_->NewItem(nsName_);
}
}  // namespace client
}  // namespace reindexer
