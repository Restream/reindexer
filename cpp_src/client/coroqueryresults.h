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
using NsArray = h_vector<Namespace*, 1>;

class CoroQueryResults {
public:
	CoroQueryResults(int fetchFlags = 0);
	CoroQueryResults(const CoroQueryResults&) = delete;
	CoroQueryResults(CoroQueryResults&&) = default;
	CoroQueryResults& operator=(const CoroQueryResults&) = delete;
	CoroQueryResults& operator=(CoroQueryResults&& obj) = default;

	class Iterator {
	public:
		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true);
		Item GetItem();
		int64_t GetLSN();
		bool IsRaw();
		std::string_view GetRaw();
		Iterator& operator++();
		Error Status() const noexcept { return qr_->status_; }
		bool operator!=(const Iterator& other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator& other) const noexcept { return idx_ == other.idx_; }
		Iterator& operator*() { return *this; }
		void readNext();
		void getJSONFromCJSON(std::string_view cjson, WrSerializer& wrser, bool withHdrLen = true);

		const CoroQueryResults* qr_;
		int idx_, pos_, nextPos_;
		ResultSerializer::ItemParams itemParams_;
	};

	Iterator begin() const { return Iterator{this, 0, 0, 0, {}}; }
	Iterator end() const { return Iterator{this, queryParams_.qcount, 0, 0, {}}; }

	size_t Count() const { return queryParams_.qcount; }
	int TotalCount() const { return queryParams_.totalcount; }
	bool HaveRank() const { return queryParams_.flags & kResultsWithRank; }
	bool NeedOutputRank() const { return queryParams_.flags & kResultsNeedOutputRank; }
	int Flags() const noexcept { return queryParams_.flags; }
	const std::string& GetExplainResults() const { return queryParams_.explainResults; }
	const std::vector<AggregationResult>& GetAggregationResults() const { return queryParams_.aggResults; }
	Error Status() { return status_; }
	h_vector<std::string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const { return queryParams_.flags & kResultsWithItemID; }

	TagsMatcher getTagsMatcher(int nsid) const;

private:
	friend class SyncCoroQueryResults;
	friend class SyncCoroReindexerImpl;
	friend class RPCClient;
	friend class CoroRPCClient;
	friend class RPCClientMock;
	using RawResBufT = h_vector<char, 0x100>;
	CoroQueryResults(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, int fetchFlags, int fetchAmount, seconds timeout);
	CoroQueryResults(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, std::string_view rawResult, RPCQrId id, int fetchFlags,
					 int fetchAmount, seconds timeout);
	void Bind(std::string_view rawResult, RPCQrId id);
	void fetchNextResults();

	net::cproto::CoroClientConnection* conn_;

	NsArray nsArray_;
	RawResBufT rawResult_;
	RPCQrId queryID_;
	int fetchOffset_;
	int fetchFlags_;
	int fetchAmount_;
	seconds requestTimeout_;

	ResultSerializer::QueryParams queryParams_;
	Error status_;
};
}  // namespace client
}  // namespace reindexer
