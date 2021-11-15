#include "transaction.h"
#include "client/itemimpl.h"
#include "client/rpcclient.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/clientconnection.h"

namespace reindexer {
namespace client {

void Transaction::Modify(Query&& query) {
	if (conn_) {
		WrSerializer ser;
		query.Serialize(ser);
		Error ret;
		switch (query.type_) {
			case QueryUpdate: {
				ret = conn_->Call({cproto::kCmdUpdateQueryTx, RequestTimeout_, execTimeout_, nullptr}, ser.Slice(), txId_).Status();
				break;
			}
			case QueryDelete: {
				ret = conn_->Call({cproto::kCmdDeleteQueryTx, RequestTimeout_, execTimeout_, nullptr}, ser.Slice(), txId_).Status();
				break;
			}
			default:
				throw(Error(errParams, "Incorrect query type in transaction modify %d", query.type_));
		}
		if (!ret.ok()) throw ret;
		return;
	}
	throw Error(errLogic, "Connection pointer in transaction is nullptr.");
}

void Transaction::addTxItem(Item&& item, ItemModifyMode mode) {
	auto itData = item.GetJSON();
	p_string itemData(&itData);
	if (conn_) {
		for (int tryCount = 0;; tryCount++) {
			auto ret =
				conn_->Call({net::cproto::kCmdAddTxItem, RequestTimeout_, execTimeout_, nullptr}, FormatJson, itemData, mode, "", 0, txId_);

			if (!ret.Status().ok()) {
				if (ret.Status().code() != errStateInvalidated || tryCount > 2) throw ret.Status();

				QueryResults qr;
				InternalRdxContext ctx;
				ctx = ctx.WithTimeout(execTimeout_);
				auto err = rpcClient_->Select(Query(nsName_).Limit(0), qr, ctx, conn_);
				if (!err.ok()) {
					throw Error(errLogic, "Can't update TagsMatcher");
				}

				auto newItem = NewItem();
				char* endp = nullptr;
				err = newItem.FromJSON(item.GetJSON(), &endp);
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
