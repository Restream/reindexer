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

class QueryResults {
public:
	QueryResults(int fetchFlags = 0, int fetchAmount = 0, bool lazyMode = false) noexcept : results_(fetchFlags, fetchAmount, lazyMode) {}
	QueryResults(const QueryResults&) = delete;
	QueryResults(QueryResults&&) = default;
	QueryResults& operator=(const QueryResults&) = delete;
	QueryResults& operator=(QueryResults&& obj) = default;
	~QueryResults();

	class Iterator : public CoroQueryResults::Iterator {
	public:
		Iterator(const QueryResults* r, const CoroQueryResults* qr, int idx, int pos, int nextPos,
				 ResultSerializer::ItemParams itemParams) noexcept
			: CoroQueryResults::Iterator{qr, idx, pos, nextPos, itemParams, {}}, r_(r) {}
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
			} catch (const Error& err) {
				const_cast<CoroQueryResults*>(qr_)->i_.status_ = err;
			}
			return *this;
		}
		const QueryResults* r_;
	};

	Iterator begin() const noexcept { return Iterator{this, &results_, 0, 0, 0, {}}; }
	Iterator end() const noexcept { return Iterator{this, &results_, int(results_.Count()), 0, 0, {}}; }

	size_t Count() const noexcept { return results_.Count(); }
	int TotalCount() const noexcept { return results_.TotalCount(); }
	bool HaveRank() const noexcept { return results_.HaveRank(); }
	bool NeedOutputRank() const noexcept { return results_.NeedOutputRank(); }
	const std::string& GetExplainResults() { return results_.GetExplainResults(); }
	const std::vector<AggregationResult>& GetAggregationResults() { return results_.GetAggregationResults(); }
	Error Status() const noexcept { return results_.Status(); }
	h_vector<std::string_view, 1> GetNamespaces() const { return results_.GetNamespaces(); }
	size_t GetNamespacesCount() const noexcept { return results_.GetNamespacesCount(); }
	bool IsCacheEnabled() const noexcept { return results_.IsCacheEnabled(); }

	int GetMergedNSCount() const noexcept { return results_.GetMergedNSCount(); }
	TagsMatcher GetTagsMatcher(int nsid) const;
	TagsMatcher GetTagsMatcher(std::string_view ns) const;
	PayloadType GetPayloadType(int nsid) const { return results_.GetPayloadType(nsid); }
	PayloadType GetPayloadType(std::string_view ns) const { return results_.GetPayloadType(ns); }

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
	Error setClient(ReindexerImpl*);
	const net::cproto::CoroClientConnection* coroConnection() const noexcept { return results_.getConn(); }

	CoroQueryResults results_;
	ReindexerImpl* rx_ = nullptr;
};
}  // namespace client
}  // namespace reindexer
