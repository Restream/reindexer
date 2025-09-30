#pragma once

#include <chrono>
#include "client/coroqueryresults.h"
#include "client/item.h"
#include "client/resultserializer.h"

namespace reindexer {
class TagsMatcher;
namespace net {
namespace cproto {
class CoroClientConnection;
}  // namespace cproto
}  // namespace net

namespace client {

using std::chrono::seconds;
using std::chrono::milliseconds;

class Namespace;
using NsArray = h_vector<Namespace*, 1>;
class ReindexerImpl;
class Reindexer;

class [[nodiscard]] QueryResults {
public:
	QueryResults(int fetchFlags = 0, int fetchAmount = 0) noexcept : results_(fetchFlags, fetchAmount) {}
	// This constructor enables lazy parsing for aggregations and explain results. This mode is usefull for raw buffer qr's proxying on
	// sharded server
	QueryResults(int fetchFlags, int fetchAmount, LazyQueryResultsMode m) noexcept : results_(fetchFlags, fetchAmount, m) {}
	QueryResults(const QueryResults&) = delete;
	QueryResults(QueryResults&&) noexcept = default;
	QueryResults& operator=(const QueryResults&) = delete;
	QueryResults& operator=(QueryResults&& obj) = default;
	~QueryResults();

	class [[nodiscard]] Iterator : public CoroQueryResults::Iterator {
	public:
		Iterator(const QueryResults& r, int idx, int pos, int nextPos, ResultSerializer::ItemParams itemParams) noexcept
			: CoroQueryResults::Iterator{r.results_, idx, pos, nextPos, std::move(itemParams)}, r_(&r) {}
		Iterator& operator*() { return *this; }
		Iterator& operator++() noexcept {
			try {
				readNext();
				idx_++;
				pos_ = nextPos_;
				nextPos_ = 0;

				if (idx_ != qr_->i_.queryParams_.qcount && idx_ == qr_->i_.queryParams_.count + qr_->i_.fetchOffset_) {
					const_cast<QueryResults*>(r_)->fetchNextResults();
					pos_ = 0;
				}
			} catch (std::exception& err) {
				const_cast<CoroQueryResults*>(qr_)->i_.status_ = std::move(err);
			}
			return *this;
		}

	private:
		const QueryResults* r_;
	};

	Iterator begin() const noexcept { return Iterator{*this, 0, 0, 0, {}}; }
	Iterator end() const noexcept { return Iterator{*this, int(results_.Count()), 0, 0, {}}; }

	size_t Count() const noexcept { return results_.Count(); }
	int TotalCount() const noexcept { return results_.TotalCount(); }
	bool HaveRank() const noexcept { return results_.HaveRank(); }
	bool NeedOutputRank() const noexcept { return results_.NeedOutputRank(); }
	int64_t GetShardingConfigVersion() const noexcept { return results_.GetShardingConfigVersion(); }
	const std::string& GetExplainResults() & { return results_.GetExplainResults(); }
	const std::string& GetExplainResults() && = delete;
	const std::vector<AggregationResult>& GetAggregationResults() & { return results_.GetAggregationResults(); }
	const std::vector<AggregationResult>& GetAggregationResults() && = delete;

	Error Status() const noexcept { return results_.Status(); }
	h_vector<std::string_view, 1> GetNamespaces() const { return results_.GetNamespaces(); }
	const NsShardsIncarnationTags& GetIncarnationTags() const& noexcept { return results_.GetIncarnationTags(); }
	const NsShardsIncarnationTags& GetIncarnationTags() && = delete;
	size_t GetNamespacesCount() const noexcept { return results_.GetNamespacesCount(); }
	bool IsCacheEnabled() const noexcept { return results_.IsCacheEnabled(); }

	int GetMergedNSCount() const noexcept { return results_.GetMergedNSCount(); }
	TagsMatcher GetTagsMatcher(int nsid) const noexcept;
	TagsMatcher GetTagsMatcher(std::string_view ns) const noexcept;
	PayloadType GetPayloadType(int nsid) const noexcept { return results_.GetPayloadType(nsid); }
	PayloadType GetPayloadType(std::string_view ns) const noexcept { return results_.GetPayloadType(ns); }

	int GetFormat() const noexcept { return results_.GetFormat(); }
	int GetFlags() const noexcept { return results_.GetFlags(); }
	bool IsInLazyMode() const noexcept { return results_.IsInLazyMode(); }
	bool IsJSON() const noexcept { return results_.IsJSON(); }
	bool IsCJSON() const noexcept { return results_.IsCJSON(); }
	bool HaveJoined() const noexcept { return results_.HaveJoined(); }
	bool GetRawBuffer(ParsedQrRawBuffer& out) { return results_.GetRawBuffer(out); }
	void FetchNextResults(int flags, int offset, int limit);

private:
	friend class Reindexer;
	friend class ReindexerImpl;
	void fetchNextResults();
	Error setClient(const std::shared_ptr<ReindexerImpl> &) noexcept;
	const net::cproto::CoroClientConnection* coroConnection() const noexcept { return results_.getConn(); }

	CoroQueryResults results_;
	std::weak_ptr<ReindexerImpl> rx_;
};
}  // namespace client
}  // namespace reindexer
