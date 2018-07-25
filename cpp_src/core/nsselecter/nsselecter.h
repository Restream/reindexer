#pragma once
#include <functional>
#include "core/aggregator.h"
#include "core/nsselecter/selectiterator.h"
#include "core/query/query.h"
#include "core/query/queryresults.h"
#include "core/selectfunc/ctx/basefunctionctx.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfunc.h"

namespace reindexer {

using std::string;
using std::vector;

struct JoinedSelector {
	typedef std::function<bool(IdType, ConstPayload, bool)> FuncType;
	JoinType type;
	bool nodata;
	FuncType func;
	int called, matched;
	string ns;
};
typedef vector<JoinedSelector> JoinedSelectors;

class SelectLockUpgrader {
public:
	virtual ~SelectLockUpgrader() = default;
	virtual void Upgrade() = 0;
};

struct SelectCtx {
	explicit SelectCtx(const Query &query_, SelectLockUpgrader *lockUpgrader) : query(query_), lockUpgrader(lockUpgrader) {}
	const Query &query;
	JoinedSelectors *joinedSelectors = nullptr;

	uint8_t nsid = 0;
	bool isForceAll = false;
	bool skipIndexesLookup = false;
	SelectLockUpgrader *lockUpgrader;
	SelectFunctionsHolder *functions = nullptr;
	struct PreResult {
		enum Mode { ModeBuild, ModeIterators, ModeIdSet };

		typedef shared_ptr<PreResult> Ptr;
		IdSet ids;
		h_vector<SelectIterator> iterators;
		Mode mode;
		string sortBy;
	};
	bool matchedAtLeastOnce = false;
	bool reqMatchedOnceFlag = false;
	PreResult::Ptr preResult;
};

class NsSelecter {
public:
	typedef vector<JoinedSelector> JoinedSelectors;
	NsSelecter(Namespace *parent) : ns_(parent) {}
	struct RawQueryResult : public h_vector<SelectIterator> {};

	void operator()(QueryResults &result, SelectCtx &ctx);

private:
	struct LoopCtx {
		LoopCtx(SelectCtx &ctx) : sctx(ctx) {}
		RawQueryResult *qres = nullptr;
		Index *sortIndex = nullptr;
		bool ftIndex = false;
		bool calcTotal = false;
		SelectCtx &sctx;
	};

	template <bool reverse, bool haveComparators, bool haveDistinct>
	void selectLoop(LoopCtx &ctx, QueryResults &result);
	void applyCustomSort(ItemRefVector &result, const SelectCtx &ctx);
	void applyGeneralSort(ItemRefVector &result, const SelectCtx &ctx, const string &fieldName, const CollateOpts &collateOpts);

	bool containsFullTextIndexes(const QueryEntries &entries);
	void selectWhere(const QueryEntries &entries, RawQueryResult &result, SortType sortId, bool is_ft);
	QueryEntries lookupQueryIndexes(const QueryEntries &entries);
	void substituteCompositeIndexes(QueryEntries &entries);
	const string &getOptimalSortOrder(const QueryEntries &entries);
	h_vector<Aggregator, 4> getAggregators(const Query &q);
	int getCompositeIndex(const FieldsSet &fieldsmask);
	bool mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs);
	void setLimitsAndOffset(ItemRefVector &result, const SelectCtx &ctx);
	void updateCompositeIndexesValues(QueryEntries &qe);
	KeyValueType getQueryEntryIndexType(const QueryEntry &qentry) const;

	Namespace *ns_;
	SelectFunction::Ptr fnc_;
	FtCtx::Ptr ft_ctx_;
};
}  // namespace reindexer
