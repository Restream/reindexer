#pragma once

#include "client/item.h"
#include "client/resultserializer.h"

namespace reindexer {
namespace net {
namespace cproto {
class ClientConnection;
};
};  // namespace net

namespace client {

class Namespace;
using NSArray = h_vector<std::shared_ptr<Namespace>, 1>;

class QueryResults {
public:
	QueryResults();
	QueryResults(const QueryResults &) = delete;
	QueryResults(QueryResults &&);
	~QueryResults();
	QueryResults &operator=(const QueryResults &) = delete;
	QueryResults &operator=(QueryResults &&obj) noexcept;

	class Iterator {
	public:
		Error GetJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer &wrser, bool withHdrLen = true);
		Item GetItem();
		Iterator &operator++();
		Error Status() { return qr_->status_; }
		bool operator!=(const Iterator &) const;
		bool operator==(const Iterator &) const;
		Iterator &operator*() { return *this; }

		const QueryResults *qr_;
		int idx_, pos_, nextPos_;
	};

	Iterator begin() const { return Iterator{this, 0, 0, 0}; }
	Iterator end() const { return Iterator{this, queryParams_.qcount, 0, 0}; }

	size_t Count() const { return queryParams_.qcount; }
	int TotalCount() const { return queryParams_.totalcount; }
	bool HaveProcent() const { return queryParams_.flags & kResultsWithPercents; };
	const string &GetExplainResults() const { return queryParams_.explainResults; }
	const vector<AggregationResult> &GetAggregationResults() const { return queryParams_.aggResults; }
	Error Status () {return status_;}

private:
	friend class RPCClient;
	QueryResults(net::cproto::ClientConnection *conn, const NSArray &nsArray, string_view rawResult, int queryID);
	void fetchNextResults();

	net::cproto::ClientConnection *conn_;

	NSArray nsArray_;
	string rawResult_;
	int queryID_; 
	int fetchOffset_;

	ResultSerializer::QueryParams queryParams_;
	Error status_;
};
}  // namespace client
}  // namespace reindexer
