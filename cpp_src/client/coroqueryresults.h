#pragma once

#include <chrono>
#include "client/item.h"
#include "client/resultserializer.h"

namespace reindexer {

class TagsMatcher;

namespace net {
namespace cproto {
class CoroClientConnection;
}
}  // namespace net

namespace client {

using std::chrono::seconds;
using std::chrono::milliseconds;

class Namespace;

class CoroQueryResults {
public:
	using NsArray = h_vector<Namespace*, 1>;

	CoroQueryResults(int fetchFlags = 0) noexcept;
	CoroQueryResults(const CoroQueryResults&) = delete;
	CoroQueryResults(CoroQueryResults&&) = default;
	CoroQueryResults& operator=(const CoroQueryResults&) = delete;
	CoroQueryResults& operator=(CoroQueryResults&& obj) = default;

	class Iterator {
	public:
		using JoinedData = h_vector<h_vector<ResultSerializer::ItemParams, 1>, 1>;
		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true);
		Item GetItem();
		int64_t GetLSN();
		bool IsRaw();
		std::string_view GetRaw();
		const JoinedData& GetJoined() const { return joinedData_; }
		Iterator& operator++();
		Error Status() const noexcept { return qr_->status_; }
		bool operator!=(const Iterator& other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator& other) const noexcept { return idx_ == other.idx_; }
		Iterator& operator*() { return *this; }
		void readNext();
		void getJSONFromCJSON(std::string_view cjson, WrSerializer& wrser, bool withHdrLen = true) const;

		const CoroQueryResults* qr_;
		int idx_, pos_, nextPos_;
		ResultSerializer::ItemParams itemParams_;
		JoinedData joinedData_;
	};

	Iterator begin() const noexcept { return Iterator{this, 0, 0, 0, {}, {}}; }
	Iterator end() const noexcept { return Iterator{this, queryParams_.qcount, 0, 0, {}, {}}; }

	size_t Count() const noexcept { return queryParams_.qcount; }
	int TotalCount() const noexcept { return queryParams_.totalcount; }
	bool HaveRank() const noexcept { return queryParams_.flags & kResultsWithRank; }
	bool NeedOutputRank() const noexcept { return queryParams_.flags & kResultsNeedOutputRank; }
	const string& GetExplainResults() const noexcept { return queryParams_.explainResults; }
	const vector<AggregationResult>& GetAggregationResults() const noexcept { return queryParams_.aggResults; }
	Error Status() const noexcept { return status_; }
	h_vector<std::string_view, 1> GetNamespaces() const;
	size_t GetNamespacesCount() const noexcept { return nsArray_.size(); }
	bool IsCacheEnabled() const noexcept { return queryParams_.flags & kResultsWithItemID; }

	TagsMatcher GetTagsMatcher(int nsid) const noexcept;
	TagsMatcher GetTagsMatcher(std::string_view ns) const noexcept;
	PayloadType GetPayloadType(int nsid) const noexcept;
	PayloadType GetPayloadType(std::string_view ns) const noexcept;

private:
	friend class SyncCoroQueryResults;
	friend class SyncCoroReindexerImpl;
	friend class CoroRPCClient;
	friend class RPCClientMock;
	CoroQueryResults(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, int fetchFlags, int fetchAmount, milliseconds timeout);
	CoroQueryResults(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, std::string_view rawResult, int queryID, int fetchFlags,
					 int fetchAmount, milliseconds timeout);
	CoroQueryResults(NsArray&& nsArray, Item& item);

	void Bind(std::string_view rawResult, int queryID);
	void fetchNextResults();

	net::cproto::CoroClientConnection* conn_;

	NsArray nsArray_;
	h_vector<char, 0x100> rawResult_;
	int queryID_;
	int fetchOffset_;
	int fetchFlags_;
	int fetchAmount_;
	milliseconds requestTimeout_;

	ResultSerializer::QueryParams queryParams_;
	Error status_;
};
}  // namespace client
}  // namespace reindexer
