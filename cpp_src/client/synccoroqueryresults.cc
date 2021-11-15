#include "synccoroqueryresults.h"
#include "client/namespace.h"
#include "synccororeindexer.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

SyncCoroQueryResults::SyncCoroQueryResults(int fetchFlags) : results_(fetchFlags) {}

void SyncCoroQueryResults::fetchNextResults() {
	int flags = results_.fetchFlags_ ? (results_.fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson;
	if (rx_) {
		Error err = rx_->fetchResults(flags, *this);
		if (!err.ok()) {
			throw err;
		}
	} else
		throw Error(errLogic, "Reindexer client not set for SyncCoroQueryResults");
}

Error SyncCoroQueryResults::setClient(SyncCoroReindexerImpl* rx) {
	if (rx_) {
		return Error(errLogic, "Client is already set for SyncCoroQuery Results");
	}
	rx_ = rx;
	return errOK;
}

h_vector<std::string_view, 1> SyncCoroQueryResults::GetNamespaces() const { return results_.GetNamespaces(); }
int SyncCoroQueryResults::GetMergedNSCount() const { return results_.nsArray_.size(); }
TagsMatcher SyncCoroQueryResults::GetTagsMatcher(int nsid) const { return results_.GetTagsMatcher(nsid); }
TagsMatcher SyncCoroQueryResults::GetTagsMatcher(std::string_view ns) const { return results_.GetTagsMatcher(ns); }
PayloadType SyncCoroQueryResults::GetPayloadType(int nsid) const { return results_.GetPayloadType(nsid); }
PayloadType SyncCoroQueryResults::GetPayloadType(std::string_view ns) const { return results_.GetPayloadType(ns); }

}  // namespace client
}  // namespace reindexer
