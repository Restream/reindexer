#include "itemimpl.h"
#include "cororpcclient.h"
#include "synccoroqueryresults.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

template <typename C>
Error ItemImpl<C>::tryToUpdateTagsMatcher() {
	if (!client_) {
		return Error(errLogic, "Client pointer is null");
	}
	typename C::QueryResultsT qr;
	Query q = Query(string(payloadType_.Name())).Limit(0);
	Error err = client_->Select(q, qr, InternalRdxContext().WithTimeout(requestTimeout_).WithShardId(ShardingKeyType::ShardingProxyOff));
	if (err.ok() && qr.GetNamespacesCount() > 0) {
		TagsMatcher newTm = qr.GetTagsMatcher(0);
		if (newTm.version() == tagsMatcher_.version()) {
			return Error(errLogic, "TM version is up-to-date (%d)", newTm.version());
		}
		WrSerializer wrser;
		newTm.serialize(wrser);
		std::string_view buf(wrser.Slice());
		Serializer ser(buf);
		tagsMatcher_.deserialize(ser, newTm.version(), newTm.stateToken());
		return errOK;
	}
	return err.ok() ? Error(errLogic, "Unable to update tagsmatcher: QR namespaces array is empty") : err;
}

template class ItemImpl<CoroRPCClient>;
template class ItemImpl<SyncCoroReindexerImpl>;

}  // namespace client
}  // namespace reindexer
