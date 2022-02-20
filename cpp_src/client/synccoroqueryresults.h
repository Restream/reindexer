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
class SyncCoroReindexerImpl;
class SyncCoroReindexer;

class SyncCoroQueryResults {
public:
	SyncCoroQueryResults(int fetchFlags = 0) noexcept : results_(fetchFlags) {}
	SyncCoroQueryResults(const SyncCoroQueryResults&) = delete;
	SyncCoroQueryResults(SyncCoroQueryResults&&) = default;
	SyncCoroQueryResults& operator=(const SyncCoroQueryResults&) = delete;
	SyncCoroQueryResults& operator=(SyncCoroQueryResults&& obj) = default;
	~SyncCoroQueryResults();

	class Iterator : public CoroQueryResults::Iterator {
	public:
		Iterator(const SyncCoroQueryResults* r, const CoroQueryResults* qr, int idx, int pos, int nextPos,
				 ResultSerializer::ItemParams itemParams) noexcept
			: CoroQueryResults::Iterator{qr, idx, pos, nextPos, itemParams, {}}, r_(r) {}
		Iterator& operator++() noexcept {
			try {
				readNext();
				idx_++;
				pos_ = nextPos_;
				nextPos_ = 0;

				if (idx_ != qr_->i_.queryParams_.qcount && idx_ == qr_->i_.queryParams_.count + qr_->i_.fetchOffset_) {
					const_cast<SyncCoroQueryResults*>(r_)->fetchNextResults();
					pos_ = 0;
				}
			} catch (const Error& err) {
				const_cast<CoroQueryResults*>(qr_)->i_.status_ = err;
			}
			return *this;
		}
		const SyncCoroQueryResults* r_;
	};

	Iterator begin() const noexcept { return Iterator{this, &results_, 0, 0, 0, {}}; }
	Iterator end() const noexcept { return Iterator{this, &results_, int(results_.Count()), 0, 0, {}}; }

	size_t Count() const noexcept { return results_.Count(); }
	int TotalCount() const noexcept { return results_.TotalCount(); }
	bool HaveRank() const noexcept { return results_.HaveRank(); }
	bool NeedOutputRank() const noexcept { return results_.NeedOutputRank(); }
	const string& GetExplainResults() const noexcept { return results_.GetExplainResults(); }
	const vector<AggregationResult>& GetAggregationResults() const noexcept { return results_.GetAggregationResults(); }
	Error Status() const noexcept { return results_.Status(); }
	h_vector<std::string_view, 1> GetNamespaces() const { return results_.GetNamespaces(); }
	size_t GetNamespacesCount() const noexcept { return results_.GetNamespacesCount(); }
	bool IsCacheEnabled() const noexcept { return results_.IsCacheEnabled(); }

	int GetMergedNSCount() const noexcept { return results_.GetMergedNSCount(); }
	TagsMatcher GetTagsMatcher(int nsid) const;
	TagsMatcher GetTagsMatcher(std::string_view ns) const;
	PayloadType GetPayloadType(int nsid) const { return results_.GetPayloadType(nsid); }
	PayloadType GetPayloadType(std::string_view ns) const { return results_.GetPayloadType(ns); }

	bool IsJSON() const noexcept { return results_.IsJSON(); }
	bool IsCJSON() const noexcept { return results_.IsCJSON(); }
	bool HaveJoined() const noexcept { return results_.HaveJoined(); }

private:
	friend class SyncCoroReindexer;
	friend class SyncCoroReindexerImpl;
	void fetchNextResults();
	Error setClient(SyncCoroReindexerImpl*);

	CoroQueryResults results_;
	SyncCoroReindexerImpl* rx_ = nullptr;
};
}  // namespace client
}  // namespace reindexer
