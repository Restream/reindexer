#pragma once

#include "client/item.h"
#include "client/namespace.h"
#include "client/resultserializer.h"

namespace reindexer {
namespace net {
namespace cproto {
class ClientConnection;
};
};  // namespace net

namespace client {

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
		void GetJSON(WrSerializer &wrser, bool withHdrLen = true);
		void GetCJSON(WrSerializer &wrser, bool withHdrLen = true);
		Item GetItem();
		Iterator &operator++();
		Error Status() { return err_; }
		bool operator!=(const Iterator &) const;
		bool operator==(const Iterator &) const;
		Iterator &operator*() { return *this; }

		const QueryResults *qr_;
		int idx_, pos_, nextPos_;
		Error err_;
	};

	Iterator begin() const { return Iterator{this, 0, 0, 0, errOK}; }
	Iterator end() const { return Iterator{this, queryParams_.qcount, 0, 0, errOK}; }

	size_t Count() const { return queryParams_.qcount; }
	int TotalCount() const { return queryParams_.totalcount; }
	bool HaveProcent() const { return queryParams_.haveProcent; };

private:
	friend class RPCClient;
	QueryResults(net::cproto::ClientConnection *conn, const NSArray &nsArray, string_view rawResult, int queryID);
	Error fetchNextResults();

	net::cproto::ClientConnection *conn_;

	NSArray nsArray_;
	string rawResult_;
	int queryID_;
	int fetchOffset_;

	ResultSerializer::QueryParams queryParams_;
};
}  // namespace client
}  // namespace reindexer