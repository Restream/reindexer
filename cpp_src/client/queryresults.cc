#include "client/queryresults.h"
#include "client/namespace.h"
#include "client/reindexerimpl.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

void QueryResults::FetchNextResults(int flags, int offset, int limit) {
	if (auto rx = rx_.lock(); rx) {
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
		Error err = rx->fetchResults(reqFlags, offset, limit, *this);
		if (!err.ok()) {
			throw err;
		}
	} else {
		throw Error(errLogic, "Reindexer client not set for QueryResults");
	}
}

void QueryResults::fetchNextResults() {
	if (auto rx = rx_.lock(); rx) {
		Error err =
			rx->fetchResults(results_.i_.fetchFlags_ ? (results_.i_.fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson, *this);
		if (!err.ok()) {
			throw err;
		}
	} else {
		throw Error(errLogic, "Reindexer client not set for QueryResults");
	}
}

Error QueryResults::setClient(const std::shared_ptr<ReindexerImpl>& rx) noexcept {
	if (!rx) [[unlikely]] {
		return Error(errLogic, "Client is for client::QueryResults is null");
	}
	if (rx_.owner_before(std::weak_ptr<ReindexerImpl>{}) || std::weak_ptr<ReindexerImpl>{}.owner_before(rx_)) {
		return Error(errLogic, "Client is already set for client::QueryResults");
	}
	rx_ = rx->weak_from_this();
	return Error();
}

QueryResults::~QueryResults() {
	if (auto rx = rx_.lock(); rx && results_.holdsRemoteData()) {
		auto err = rx->closeResults(*this);
		(void)err;	// ignore
	}
	results_.setClosed();
}

TagsMatcher QueryResults::GetTagsMatcher(int nsid) const noexcept { return results_.GetTagsMatcher(nsid); }
TagsMatcher QueryResults::GetTagsMatcher(std::string_view ns) const noexcept { return results_.GetTagsMatcher(ns); }

}  // namespace client
}  // namespace reindexer
