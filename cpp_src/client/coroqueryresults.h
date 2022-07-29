#pragma once

#include <chrono>
#include <optional>
#include "client/item.h"
#include "client/resultserializer.h"
#include "tools/lsn.h"

namespace reindexer {

class TagsMatcher;
class Query;

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

	CoroQueryResults(int fetchFlags = 0) noexcept : i_(fetchFlags) {}
	CoroQueryResults(const CoroQueryResults&) = delete;
	CoroQueryResults(CoroQueryResults&& o) noexcept : i_(std::move(o.i_)) { o.setClosed(); }
	CoroQueryResults& operator=(const CoroQueryResults&) = delete;
	CoroQueryResults& operator=(CoroQueryResults&& o) {
		if (this != &o) {
			i_ = std::move(o.i_);
			o.setClosed();
		}
		return *this;
	}
	~CoroQueryResults();

	class Iterator {
	public:
		using JoinedData = h_vector<h_vector<ResultSerializer::ItemParams, 1>, 1>;
		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true);
		Item GetItem();
		lsn_t GetLSN();
		int GetNSID();
		int GetID();
		int GetShardID();
		int16_t GetRank();
		bool IsRaw();
		std::string_view GetRaw();
		const JoinedData& GetJoined();
		Iterator& operator++();
		Error Status() const noexcept { return qr_->i_.status_; }
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
	struct QueryData {
		uint16_t joinedSize = 0;
		h_vector<uint16_t, 8> mergedJoinedSizes;
	};

	Iterator begin() const noexcept { return Iterator{this, 0, 0, 0, {}, {}}; }
	Iterator end() const noexcept { return Iterator{this, i_.queryParams_.qcount, 0, 0, {}, {}}; }

	size_t Count() const noexcept { return i_.queryParams_.qcount; }
	int TotalCount() const noexcept { return i_.queryParams_.totalcount; }
	bool HaveRank() const noexcept { return i_.queryParams_.flags & kResultsWithRank; }
	bool NeedOutputRank() const noexcept { return i_.queryParams_.flags & kResultsNeedOutputRank; }
	bool NeedOutputShardId() const noexcept { return i_.fetchFlags_ & kResultsNeedOutputShardId; }
	const string& GetExplainResults() const noexcept { return i_.queryParams_.explainResults; }
	const vector<AggregationResult>& GetAggregationResults() const noexcept { return i_.queryParams_.aggResults; }
	Error Status() const noexcept { return i_.status_; }
	h_vector<std::string_view, 1> GetNamespaces() const;
	size_t GetNamespacesCount() const noexcept { return i_.nsArray_.size(); }
	bool IsCacheEnabled() const noexcept { return i_.queryParams_.flags & kResultsWithItemID; }
	int GetMergedNSCount() const noexcept { return i_.nsArray_.size(); }

	TagsMatcher GetTagsMatcher(int nsid) const noexcept;
	TagsMatcher GetTagsMatcher(std::string_view ns) const noexcept;
	PayloadType GetPayloadType(int nsid) const noexcept;
	PayloadType GetPayloadType(std::string_view ns) const noexcept;
	const std::string& GetNsName(int nsid) const noexcept;

	bool IsJSON() const noexcept { return (i_.queryParams_.flags & kResultsFormatMask) == kResultsJson; }
	bool IsCJSON() const noexcept { return (i_.queryParams_.flags & kResultsFormatMask) == kResultsCJson; }
	bool HaveJoined() const noexcept { return i_.queryParams_.flags & kResultsWithJoined; }
	const std::optional<QueryData> GetQueryData() const noexcept { return i_.qData_; }

private:
	friend class SyncCoroQueryResults;
	friend class SyncCoroReindexerImpl;
	friend class CoroRPCClient;
	friend class RPCClientMock;
	CoroQueryResults(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, int fetchFlags, int fetchAmount,
					 milliseconds timeout) noexcept
		: i_(conn, std::move(nsArray), fetchFlags, fetchAmount, timeout) {}
	CoroQueryResults(const Query* q, net::cproto::CoroClientConnection* conn, NsArray&& nsArray, std::string_view rawResult, RPCQrId id,
					 int fetchFlags, int fetchAmount, milliseconds timeout)
		: i_(conn, std::move(nsArray), fetchFlags, fetchAmount, timeout) {
		Bind(rawResult, id, q);
	}
	CoroQueryResults(NsArray&& nsArray, Item& item);

	void Bind(std::string_view rawResult, RPCQrId id, const Query* q);
	void fetchNextResults();
	bool holdsRemoteData() const noexcept {
		return i_.conn_ && i_.queryID_.main >= 0 && i_.fetchOffset_ + i_.queryParams_.count < i_.queryParams_.qcount;
	}
	void setClosed() noexcept {
		i_.conn_ = nullptr;
		i_.queryID_ = RPCQrId{};
	}
	const net::cproto::CoroClientConnection* getConn() const noexcept { return i_.conn_; }

	struct Impl {
		Impl(int fetchFlags) noexcept : fetchFlags_(fetchFlags) {}
		Impl(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, int fetchFlags, int fetchAmount, milliseconds timeout);
		Impl(NsArray&& nsArray) noexcept : nsArray_(std::move(nsArray)) {}

		net::cproto::CoroClientConnection* conn_ = nullptr;
		NsArray nsArray_;
		h_vector<char, 0x100> rawResult_;
		RPCQrId queryID_;
		int fetchOffset_ = 0;
		int fetchFlags_ = 0;
		int fetchAmount_ = 0;
		milliseconds requestTimeout_;
		ResultSerializer::QueryParams queryParams_;
		Error status_;
		int64_t shardingConfigVersion_ = -1;
		std::chrono::steady_clock::time_point sessionTs_;
		std::optional<QueryData> qData_;
	};

	Impl i_;
};
}  // namespace client
}  // namespace reindexer
