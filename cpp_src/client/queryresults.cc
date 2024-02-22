#include "client/queryresults.h"
#include "client/namespace.h"
#include "client/reindexer.h"
#include "client/reindexerimpl.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

void QueryResults::FetchNextResults(int flags, int offset, int limit) {
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
		throw Error(errLogic, "Reindexer client not set for QueryResults");
	}
}

void QueryResults::fetchNextResults() {
	if (rx_) {
		Error err =
			rx_->fetchResults(results_.i_.fetchFlags_ ? (results_.i_.fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson, *this);
		if (!err.ok()) {
			throw err;
		}
	} else {
		throw Error(errLogic, "Reindexer client not set for QueryResults");
	}
}

Error QueryResults::setClient(ReindexerImpl* rx) {
	if (rx_) {
		return Error(errLogic, "Client is already set for SyncCoroQuery Results");
	}
	rx_ = rx;
	return Error();
}

QueryResults::~QueryResults() {
	if (rx_ && results_.holdsRemoteData()) {
		rx_->closeResults(*this);
	}
	results_.setClosed();
}

TagsMatcher QueryResults::GetTagsMatcher(int nsid) const noexcept { return results_.GetTagsMatcher(nsid); }
TagsMatcher QueryResults::GetTagsMatcher(std::string_view ns) const noexcept { return results_.GetTagsMatcher(ns); }

}  // namespace client
}  // namespace reindexer
