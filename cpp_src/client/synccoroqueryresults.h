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
class SyncCoroReindexer;

class SyncCoroQueryResults {
public:
	SyncCoroQueryResults(SyncCoroReindexer* rx, int fetchFlags = 0);
	SyncCoroQueryResults(const SyncCoroQueryResults&) = delete;
	SyncCoroQueryResults(SyncCoroQueryResults&&) = default;
	SyncCoroQueryResults& operator=(const SyncCoroQueryResults&) = delete;
	SyncCoroQueryResults& operator=(SyncCoroQueryResults&& obj) = default;

	class Iterator : public CoroQueryResults::Iterator {
	public:
		Iterator(const SyncCoroQueryResults* r, const CoroQueryResults* qr, int idx, int pos, int nextPos,
				 ResultSerializer::ItemParams itemParams)
			: CoroQueryResults::Iterator{qr, idx, pos, nextPos, itemParams}, r_(r) {}
		Iterator& operator++() {
			try {
				readNext();
				idx_++;
				pos_ = nextPos_;
				nextPos_ = 0;

				if (idx_ != qr_->queryParams_.qcount && idx_ == qr_->queryParams_.count + qr_->fetchOffset_) {
					const_cast<SyncCoroQueryResults*>(r_)->fetchNextResults();
					pos_ = 0;
				}
			} catch (const Error& err) {
				const_cast<CoroQueryResults*>(qr_)->status_ = err;
			}
			return *this;
		}
		const SyncCoroQueryResults* r_;
	};

	Iterator begin() const { return Iterator{this, &results_, 0, 0, 0, {}}; }
	Iterator end() const { return Iterator{this, &results_, results_.queryParams_.qcount, 0, 0, {}}; }

	size_t Count() const { return results_.queryParams_.qcount; }
	int TotalCount() const { return results_.queryParams_.totalcount; }
	bool HaveRank() const { return results_.queryParams_.flags & kResultsWithRank; }
	bool NeedOutputRank() const { return results_.queryParams_.flags & kResultsNeedOutputRank; }
	const std::string& GetExplainResults() const { return results_.queryParams_.explainResults; }
	const std::vector<AggregationResult>& GetAggregationResults() const { return results_.queryParams_.aggResults; }
	Error Status() { return results_.status_; }
	h_vector<std::string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const { return results_.queryParams_.flags & kResultsWithItemID; }

	TagsMatcher getTagsMatcher(int nsid) const;

private:
	friend class SyncCoroReindexer;
	friend class SyncCoroReindexerImpl;
	void Bind(std::string_view rawResult, RPCQrId id);
	void fetchNextResults();

	CoroQueryResults results_;
	SyncCoroReindexer* rx_;
};
}  // namespace client
}  // namespace reindexer
