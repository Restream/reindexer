#include "synccoroqueryresults.h"
#include "client/namespace.h"
#include "synccororeindexer.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

SyncCoroQueryResults::SyncCoroQueryResults(SyncCoroReindexer *rx, int fetchFlags) : results_(fetchFlags), rx_(rx) {}

void SyncCoroQueryResults::Bind(std::string_view rawResult, int queryID) { results_.Bind(rawResult, queryID); }

void SyncCoroQueryResults::fetchNextResults() {
	int flags = results_.fetchFlags_ ? (results_.fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson;
	rx_->impl_->fetchResults(flags, *this);
}

h_vector<std::string_view, 1> SyncCoroQueryResults::GetNamespaces() const { return results_.GetNamespaces(); }

TagsMatcher SyncCoroQueryResults::getTagsMatcher(int nsid) const { return results_.nsArray_[nsid]->tagsMatcher_; }

}  // namespace client
}  // namespace reindexer
