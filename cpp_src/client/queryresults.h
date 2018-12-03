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
using NSArray = h_vector<Namespace *, 1>;

class QueryResults {
public:
	typedef std::function<void(const Error &err)> Completion;
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
	QueryResults(net::cproto::ClientConnection *conn, NSArray &&nsArray,Completion cmpl);
	QueryResults(net::cproto::ClientConnection *conn, NSArray &&nsArray,Completion cmpl,string_view rawResult, int queryID);
	void Bind (string_view rawResult, int queryID);
	void fetchNextResults();
	void completion (const Error &err) {
		if (cmpl_) {
			auto cmpl = std::move (cmpl_);
			cmpl (err);
		}
	}

	net::cproto::ClientConnection *conn_;

	NSArray nsArray_;
	h_vector<char,0x100> rawResult_;
	int queryID_; 
	int fetchOffset_;

	ResultSerializer::QueryParams queryParams_;
	Error status_;
	Completion cmpl_;

};
}  // namespace client
}  // namespace reindexer
