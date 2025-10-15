#pragma once

#include <chrono>
#include <optional>
#include "client/item.h"
#include "client/resultserializer.h"
#include "core/namespace/incarnationtags.h"
#include "core/rank_t.h"
#include "tools/clock.h"
#include "tools/lsn.h"

namespace reindexer {

class Query;

namespace builders {
struct CsvOrdering;
}  // namespace builders
using builders::CsvOrdering;

namespace net {
namespace cproto {
class CoroClientConnection;
class CoroRPCAnswer;
}  // namespace cproto
}  // namespace net

namespace client {

using QrRawBuffer = h_vector<char, 0x100>;

struct [[nodiscard]] ParsedQrRawBuffer {
	QrRawBuffer* buf = nullptr;
	ResultSerializer::ParsingData parsingData;
};

struct [[nodiscard]] LazyQueryResultsMode {};

using std::chrono::seconds;
using std::chrono::milliseconds;

class QueryResults;

class [[nodiscard]] CoroQueryResults {
public:
	using NsArray = h_vector<std::shared_ptr<Namespace>, 4>;

	CoroQueryResults(int fetchFlags = 0, int fetchAmount = 0) noexcept : i_(fetchFlags, fetchAmount, false) {}
	// This constructor enables lazy parsing for aggregations and explain results. This mode is usefull for raw buffer qr's proxying on
	// sharded server
	CoroQueryResults(int fetchFlags, int fetchAmount, LazyQueryResultsMode) noexcept : i_(fetchFlags, fetchAmount, true) {}
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

	class [[nodiscard]] Iterator {
	public:
		Iterator(const CoroQueryResults& qr, int idx) noexcept : qr_{&qr}, idx_{idx} {}

		using JoinedData = h_vector<h_vector<ResultSerializer::ItemParams, 1>, 1>;
		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCSV(WrSerializer& wrser, CsvOrdering& ordering) noexcept;
		Item GetItem();
		lsn_t GetLSN();
		int GetNSID();
		int GetID();
		int GetShardID();
		RankT GetRank();
		bool IsRanked() noexcept;
		bool IsRaw();
		std::string_view GetRaw();
		const JoinedData& GetJoined();
		Iterator& operator++();
		Error Status() const noexcept {
			if (!qr_->i_.status_.ok()) {
				return qr_->i_.status_;
			}
			if (!isAvailable()) {
				return unavailableIdxError();
			}
			return Error();
		}
		bool operator!=(const Iterator& other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator& other) const noexcept { return idx_ == other.idx_; }
		Iterator& operator*() { return *this; }
		const ResultSerializer::ItemParams& GetItemParams() const& { return itemParams_; }

	protected:
		Iterator(const CoroQueryResults& qr, int idx, int pos, int nextPos, ResultSerializer::ItemParams&& params) noexcept
			: qr_{&qr}, idx_{idx}, pos_{pos}, nextPos_{nextPos}, itemParams_{std::move(params)} {}

		void readNext();
		void getJSONFromCJSON(std::string_view cjson, WrSerializer& wrser, bool withHdrLen = true) const;
		void getCSVFromCJSON(std::string_view cjson, WrSerializer& wrser, CsvOrdering& ordering) const;
		void checkIdx() const;
		bool isAvailable() const noexcept { return idx_ >= qr_->i_.fetchOffset_ && idx_ < qr_->i_.queryParams_.qcount; }
		Error unavailableIdxError() const;

		const CoroQueryResults* qr_{nullptr};
		int idx_{0}, pos_{0}, nextPos_{0};
		ResultSerializer::ItemParams itemParams_;
		JoinedData joinedData_;
	};
	struct [[nodiscard]] QueryData {
		uint16_t joinedSize = 0;
		h_vector<uint16_t, 8> mergedJoinedSizes;
	};

	Iterator begin() const noexcept { return Iterator{*this, 0}; }
	Iterator end() const noexcept { return Iterator{*this, i_.queryParams_.qcount}; }

	size_t Count() const noexcept { return i_.queryParams_.qcount; }
	int TotalCount() const noexcept { return i_.queryParams_.totalcount; }
	bool HaveRank() const noexcept { return i_.queryParams_.flags & kResultsWithRank; }
	bool NeedOutputRank() const noexcept { return i_.queryParams_.flags & kResultsNeedOutputRank; }
	bool NeedOutputShardId() const noexcept { return i_.fetchFlags_ & kResultsNeedOutputShardId; }
	int64_t GetShardingConfigVersion() const noexcept { return i_.queryParams_.shardingConfigVersion; }
	const std::string& GetExplainResults();
	const std::vector<AggregationResult>& GetAggregationResults();
	Error Status() const noexcept { return i_.status_; }
	h_vector<std::string_view, 1> GetNamespaces() const;
	const NsShardsIncarnationTags& GetIncarnationTags() const& noexcept { return i_.queryParams_.nsIncarnationTags; }
	const NsShardsIncarnationTags& GetIncarnationTags() && = delete;
	size_t GetNamespacesCount() const noexcept { return i_.nsArray_.size(); }
	bool IsCacheEnabled() const noexcept { return i_.queryParams_.flags & kResultsWithItemID; }
	int GetMergedNSCount() const noexcept { return i_.nsArray_.size(); }

	TagsMatcher GetTagsMatcher(int nsid) const noexcept;
	TagsMatcher GetTagsMatcher(std::string_view ns) const noexcept;
	PayloadType GetPayloadType(int nsid) const noexcept;
	PayloadType GetPayloadType(std::string_view ns) const noexcept;
	const std::string& GetNsName(int nsid) const noexcept;

	int GetFormat() const noexcept { return i_.queryParams_.flags & kResultsFormatMask; }
	int GetFlags() const noexcept { return i_.queryParams_.flags; }
	bool IsJSON() const noexcept { return GetFormat() == kResultsJson; }
	bool IsCJSON() const noexcept { return GetFormat() == kResultsCJson; }
	bool HaveJoined() const noexcept { return i_.queryParams_.flags & kResultsWithJoined; }
	const std::optional<QueryData>& GetQueryData() const noexcept { return i_.qData_; }
	bool GetRawBuffer(ParsedQrRawBuffer& out) {
		if (!Status().ok()) {
			throw Status();
		}
		if (!IsInLazyMode()) {
			throw Error(errLogic, "Unable to get raw buffer: client QueryResults is not in lazy parsing mode");
		}
		out.buf = &i_.rawResult_;
		out.parsingData = i_.parsingData_;
		i_.status_ = Error(errNotValid, "QueryResults buffer was moved");
		return holdsRemoteData();
	}
	int FetchAmount() const noexcept { return i_.fetchAmount_; }
	bool IsInLazyMode() const noexcept { return i_.lazyMode_; }
	bool IsBound() const noexcept { return i_.isBound_; }

private:
	friend class client::QueryResults;
	friend class client::ReindexerImpl;
	friend class client::RPCClient;
	CoroQueryResults(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, int fetchFlags, int fetchAmount, milliseconds timeout,
					 bool lazyMode) noexcept
		: i_(conn, std::move(nsArray), fetchFlags, fetchAmount, timeout, lazyMode) {}
	CoroQueryResults(const Query* q, net::cproto::CoroClientConnection* conn, NsArray&& nsArray, std::string_view rawResult, RPCQrId id,
					 int fetchFlags, int fetchAmount, milliseconds timeout, bool lazyMode)
		: i_(conn, std::move(nsArray), fetchFlags, fetchAmount, timeout, lazyMode) {
		Bind(rawResult, id, q);
	}
	CoroQueryResults(NsArray&& nsArray, Item& item);

	void Bind(std::string_view rawResult, RPCQrId id, const Query* q);
	void fetchNextResults();
	void handleFetchedBuf(net::cproto::CoroRPCAnswer& ans);
	void parseExtraData();
	bool holdsRemoteData() const noexcept {
		return i_.conn_ && i_.queryID_.main >= 0 && i_.fetchOffset_ + i_.queryParams_.count < i_.queryParams_.qcount;
	}
	bool hadFetchedRemoteData() const noexcept { return i_.fetchOffset_ > 0; }
	void setClosed() noexcept {
		i_.conn_ = nullptr;
		i_.queryID_ = RPCQrId{};
	}
	const net::cproto::CoroClientConnection* getConn() const noexcept { return i_.conn_; }

	struct [[nodiscard]] Impl {
		Impl(int fetchFlags, int fetchAmount, bool lazyMode) noexcept
			: fetchFlags_(fetchFlags), fetchAmount_(fetchAmount), lazyMode_(lazyMode) {
			InitLazyData();
		}
		Impl(net::cproto::CoroClientConnection* conn, NsArray&& nsArray, int fetchFlags, int fetchAmount, milliseconds timeout,
			 bool lazyMode);
		Impl(NsArray&& nsArray) noexcept : nsArray_(std::move(nsArray)) { InitLazyData(); }
		void InitLazyData() {
			if (!lazyMode_) {
				queryParams_.aggResults.emplace();
				queryParams_.explainResults.emplace();
			}
		}

		net::cproto::CoroClientConnection* conn_ = nullptr;
		NsArray nsArray_;
		QrRawBuffer rawResult_;
		RPCQrId queryID_;
		int fetchOffset_ = 0;
		int fetchFlags_ = 0;
		int fetchAmount_ = 0;
		milliseconds requestTimeout_;
		ResultSerializer::ParsingData parsingData_;
		ResultSerializer::QueryParams queryParams_;
		bool lazyMode_ = false;
		bool isBound_ = false;
		Error status_;
		steady_clock_w::time_point sessionTs_;
		std::optional<QueryData> qData_;
	};

	Impl i_;
};
}  // namespace client
}  // namespace reindexer
