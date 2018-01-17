#pragma once
#include <functional>
#include "core/aggregator.h"
#include "core/ft/fulltextctx.h"
#include "core/index/index.h"
#include "core/nsselecter/selectiterator.h"
#include "core/query/query.h"
#include "core/query/queryresults.h"

namespace reindexer {

using std::string;
using std::vector;

struct JoinedSelector {
	JoinType type;
	std::function<bool(IdType, ConstPayload, bool)> func;
};
typedef vector<JoinedSelector> JoinedSelectors;

struct SelectCtx {
	typedef shared_ptr<SelectCtx> Ptr;

	explicit SelectCtx(const Query &query_, std::function<void()> lockUpgrader) : query(query_), lockUpgrader(lockUpgrader) {}
	const Query &query;
	JoinedSelectors *joinedSelectors = nullptr;
	const IdSet *preResult = nullptr;
	bool rsltAsSrtOrdrs = false;
	uint8_t nsid = 0;
	bool isForceAll = false;
	std::function<void()> lockUpgrader;
};

class NsSelecter {
public:
	typedef vector<JoinedSelector> JoinedSelectors;
	NsSelecter(Namespace *parent) : ns_(parent) {}
	struct RawQueryResult : public h_vector<SelectIterator> {
		h_vector<FullTextCtx::Ptr> ctxs;
	};

	void operator()(QueryResults &result, const SelectCtx &ctx);

private:
	struct LoopCtx : public SelectCtx {
		LoopCtx(const SelectCtx &ctx) : SelectCtx(ctx) {}
		RawQueryResult *qres = nullptr;
		Index *sortIndex = nullptr;
		bool ftIndex = false;
		bool calcTotal = false;
	};

	template <bool reverse, bool haveComparators, bool haveDistinct>
	void selectLoop(const LoopCtx &ctx, QueryResults &result);
	void applyCustomSort(QueryResults &result, const SelectCtx &ctx);
	void applyGeneralSort(QueryResults &result, const SelectCtx &ctx, int collateMode);

	bool isContainsFullText(const QueryEntries &entries);
	void selectWhere(const QueryEntries &entries, RawQueryResult &result, SortType sortId, bool is_ft);
	QueryEntries lookupQueryIndexes(const QueryEntries &entries);
	void substituteCompositeIndexes(QueryEntries &entries);
	const string &getOptimalSortOrder(const QueryEntries &qe);
	h_vector<Aggregator, 4> getAggregators(const Query &q);
	int getCompositeIndex(const FieldsSet &fieldsmask);
	bool mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs);
	void setLimitsAfterSortByUnorderedIndex(QueryResults &result, const SelectCtx &ctx);

	Namespace *ns_;
};
}  // namespace reindexer
