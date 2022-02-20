#include "synccoroqueryresults.h"
#include "client/namespace.h"
#include "synccororeindexer.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

void SyncCoroQueryResults::fetchNextResults() {
	if (rx_) {
		Error err =
			rx_->fetchResults(results_.i_.fetchFlags_ ? (results_.i_.fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson, *this);
		if (!err.ok()) {
			throw err;
		}
	} else {
		throw Error(errLogic, "Reindexer client not set for SyncCoroQueryResults");
	}
}

Error SyncCoroQueryResults::setClient(SyncCoroReindexerImpl* rx) {
	if (rx_) {
		return Error(errLogic, "Client is already set for SyncCoroQuery Results");
	}
	rx_ = rx;
	return Error();
}

SyncCoroQueryResults::~SyncCoroQueryResults() {
	if (rx_ && results_.holdsRemoteData()) {
		rx_->closeResults(*this);
	}
}

TagsMatcher SyncCoroQueryResults::GetTagsMatcher(int nsid) const { return results_.GetTagsMatcher(nsid); }
TagsMatcher SyncCoroQueryResults::GetTagsMatcher(std::string_view ns) const { return results_.GetTagsMatcher(ns); }

}  // namespace client
}  // namespace reindexer
