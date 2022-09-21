#include "synccoroqueryresults.h"
#include "client/namespace.h"
#include "synccororeindexer.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

void SyncCoroQueryResults::FetchNextResults(int flags, int offset, int limit) {
	if (rx_) {
		const auto reqFlags = flags ? (flags & ~kResultsWithPayloadTypes) : kResultsCJson;

		int curOffset = results_.i_.fetchOffset_;
		if (unsigned(curOffset) > results_.Count()) {
			curOffset = results_.Count();
		}

		int curLimit = results_.i_.fetchAmount_;
		if (unsigned(curOffset + curLimit) > results_.Count()) {
			curLimit = results_.Count() - curOffset;
		}

		if (curOffset == offset && curLimit == limit) {
			return;
		}
		Error err = rx_->fetchResults(reqFlags, offset, limit, *this);
		if (!err.ok()) {
			throw err;
		}
	} else {
		throw Error(errLogic, "Reindexer client not set for SyncCoroQueryResults");
	}
}

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
	results_.setClosed();
}

TagsMatcher SyncCoroQueryResults::GetTagsMatcher(int nsid) const { return results_.GetTagsMatcher(nsid); }
TagsMatcher SyncCoroQueryResults::GetTagsMatcher(std::string_view ns) const { return results_.GetTagsMatcher(ns); }

}  // namespace client
}  // namespace reindexer
