#pragma once
#include "core/aggregator.h"
#include "core/index/index.h"
#include "core/joincache.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "sortingcontext.h"

namespace reindexer {

struct JoinedSelector {
	typedef std::function<bool(JoinedSelector *, IdType, int nsId, ConstPayload, bool)> FuncType;
	JoinType type;
	bool nodata;
	FuncType func;
	int called, matched;
	string ns;
	JoinCacheRes joinRes;
	Query query;
};

typedef vector<JoinedSelector> JoinedSelectors;

struct JoinPreResult {
	enum Mode { ModeBuild, ModeIterators, ModeIdSet, ModeEmpty };

	typedef shared_ptr<JoinPreResult> Ptr;
	IdSet ids;
	SelectIteratorContainer iterators;
	Mode mode = ModeEmpty;
	bool enableSortOrders = false;
	bool btreeIndexOptimizationEnabled = true;
};

struct SelectCtx {
	explicit SelectCtx(const Query &query_) : query(query_) {}
	const Query &query;
	JoinedSelectors *joinedSelectors = nullptr;
	SelectFunctionsHolder *functions = nullptr;

	JoinPreResult::Ptr preResult;
	SortingContext sortingContext;
	uint8_t nsid = 0;
	bool isForceAll = false;
	bool skipIndexesLookup = false;
	bool matchedAtLeastOnce = false;
	bool reqMatchedOnceFlag = false;
	bool contextCollectingMode = false;
};

class NsSelecter {
public:
	NsSelecter(Namespace *parent) : ns_(parent) {}

	void operator()(QueryResults &result, SelectCtx &ctx, const RdxContext &);

private:
	struct LoopCtx {
		LoopCtx(SelectCtx &ctx) : sctx(ctx) {}
		SelectIteratorContainer *qres = nullptr;
		bool calcTotal = false;
		SelectCtx &sctx;
	};

	template <bool reverse, bool haveComparators>
	void selectLoop(LoopCtx &ctx, QueryResults &result, const RdxContext &);
	void applyForcedSort(ItemRefVector &result, const SelectCtx &ctx);
	void applyForcedSortDesc(ItemRefVector &result, const SelectCtx &ctx);

	using ItemIterator = ItemRefVector::iterator;
	using ConstItemIterator = const ItemIterator &;
	void applyGeneralSort(ConstItemIterator itFirst, ConstItemIterator itLast, ConstItemIterator itEnd, const SelectCtx &ctx);

	void addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, const SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
						 QueryResults &result);

	h_vector<Aggregator, 4> getAggregators(const Query &q);
	int getCompositeIndex(const FieldsSet &fieldsmask);
	void setLimitAndOffset(ItemRefVector &result, size_t offset, size_t limit);
	void prepareSortingContext(const SortingEntries &sortBy, SelectCtx &ctx, bool isFt);
	void prepareSortingIndexes(SortingEntries &sortBy);
	void getSortIndexValue(const SortingContext::Entry *sortCtx, IdType rowId, VariantArray &value);
	void processLeftJoins(QueryResults &qr, SelectCtx &sctx);
	bool checkIfThereAreLeftJoins(SelectCtx &sctx) const;
	void sortResults(reindexer::SelectCtx &sctx, QueryResults &result, const SortingOptions &sortingOptions, size_t multisortLimitLeft);

	bool isSortOptimizatonEffective(const QueryEntries &qe, SelectCtx &ctx, const RdxContext &rdxCtx);

	Namespace *ns_;
	SelectFunction::Ptr fnc_;
	FtCtx::Ptr ft_ctx_;
};
}  // namespace reindexer
