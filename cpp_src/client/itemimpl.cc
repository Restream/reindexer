#include "itemimpl.h"
#include "client/queryresults.h"
#include "client/reindexerimpl.h"
#include "client/rpcclient.h"

namespace reindexer {
namespace client {

template <typename C>
Error ItemImpl<C>::tryToUpdateTagsMatcher() {
	if (!client_) {
		return Error(errLogic, "Client pointer is null");
	}
	typename C::QueryResultsT qr;
	Query q = Query(payloadType_.Name()).Limit(0);
	Error err = client_->Select(q, qr, InternalRdxContext().WithTimeout(requestTimeout_).WithShardId(ShardingKeyType::ProxyOff, false));
	if (err.ok() && qr.GetNamespacesCount() > 0) {
		TagsMatcher newTm = qr.GetTagsMatcher(0);
		if (newTm.version() == tagsMatcher_.version() && newTm.stateToken() == tagsMatcher_.stateToken()) {
			return Error(errLogic, "TM version is up-to-date ({}) and state tokens are same: ({:#08x})", newTm.version(),
						 newTm.stateToken());
		}
		WrSerializer wrser;
		newTm.serialize(wrser);
		std::string_view buf(wrser.Slice());
		Serializer ser(buf);
		tagsMatcher_.deserialize(ser, newTm.version(), newTm.stateToken());
		return {};
	}
	return err.ok() ? Error(errLogic, "Unable to update tagsmatcher: QR namespaces array is empty") : err;
}

template class ItemImpl<RPCClient>;
template class ItemImpl<ReindexerImpl>;

}  // namespace client
}  // namespace reindexer
