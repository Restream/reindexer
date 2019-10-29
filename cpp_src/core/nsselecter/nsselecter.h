#pragma once
#include "core/aggregator.h"
#include "core/index/index.h"
#include "core/joincache.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "sortingcontext.h"

namespace reindexer {

struct JoinPreResult {
	enum Mode { ModeBuild, ModeIterators, ModeIdSet, ModeEmpty };

	typedef shared_ptr<JoinPreResult> Ptr;
	IdSet ids;
	SelectIteratorContainer iterators;
	Mode mode = ModeEmpty;
	bool enableSortOrders = false;
	bool btreeIndexOptimizationEnabled = true;
};

class JoinedSelector {
public:
	JoinedSelector(JoinType joinType, std::shared_ptr<Namespace> leftNs, std::shared_ptr<Namespace> rightNs, JoinCacheRes &&joinRes,
				   Query &&itemQuery, QueryResults &result, const JoinedQuery &joinQuery, JoinPreResult::Ptr preResult,
				   size_t joinedFieldIdx, SelectFunctionsHolder &selectFunctions, int joinedSelectorsCount, const RdxContext &rdxCtx)
		: joinType_(joinType),
		  called_(0),
		  matched_(0),
		  leftNs_(std::move(leftNs)),
		  rightNs_(std::move(rightNs)),
		  joinRes_(std::move(joinRes)),
		  itemQuery_(std::move(itemQuery)),
		  result_(result),
		  joinQuery_(joinQuery),
		  preResult_(preResult),
		  joinedFieldIdx_(joinedFieldIdx),
		  selectFunctions_(selectFunctions),
		  joinedSelectorsCount_(joinedSelectorsCount),
		  rdxCtx_(rdxCtx),
		  optimized_(false) {}

	JoinedSelector(JoinedSelector &&) = default;

	JoinedSelector(const JoinedSelector &) = delete;
	JoinedSelector &operator=(const JoinedSelector &) = delete;
	JoinedSelector &operator=(JoinedSelector &&) = delete;

	bool Process(IdType, int nsId, ConstPayload, bool match);
	JoinType Type() const { return joinType_; }
	const Namespace &RightNs() const { return *rightNs_; }
	int Called() const { return called_; }
	int Matched() const { return matched_; }
	void AppendSelectIteratorOfJoinIndexData(SelectIteratorContainer &, int *maxIterations, unsigned sortId, SelectFunction::Ptr,
											 const RdxContext &);

private:
	template <bool byJsonPath>
	void readValues(VariantArray &values, const Index &leftIndex, int rightIdxNo, const std::string &rightIndex) const;

	JoinType joinType_;
	int called_, matched_;
	std::shared_ptr<Namespace> leftNs_;
	std::shared_ptr<Namespace> rightNs_;
	JoinCacheRes joinRes_;
	Query itemQuery_;
	QueryResults &result_;
	const JoinedQuery &joinQuery_;
	JoinPreResult::Ptr preResult_;
	size_t joinedFieldIdx_;
	SelectFunctionsHolder &selectFunctions_;
	int joinedSelectorsCount_;
	const RdxContext &rdxCtx_;
	bool optimized_;
};

typedef vector<JoinedSelector> JoinedSelectors;

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
