#pragma once
#include "aggregator.h"
#include "core/index/index.h"
#include "joinedselector.h"
#include "sortingcontext.h"

namespace reindexer {

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

class ItemComparator;
class ExplainCalc;
class QueryPreprocessor;

class NsSelecter {
public:
	NsSelecter(NamespaceImpl *parent) : ns_(parent) {}

	void operator()(QueryResults &result, SelectCtx &ctx, const RdxContext &);

private:
	struct LoopCtx {
		LoopCtx(SelectIteratorContainer &sIt, SelectCtx &ctx, const QueryPreprocessor &qpp, h_vector<Aggregator, 4> &agg, ExplainCalc &expl)
			: qres(sIt), sctx(ctx), qPreproc(qpp), aggregators(agg), explain(expl) {}
		SelectIteratorContainer &qres;
		bool calcTotal = false;
		SelectCtx &sctx;
		const QueryPreprocessor &qPreproc;
		h_vector<Aggregator, 4> &aggregators;
		ExplainCalc &explain;
		unsigned start = 0;
		unsigned count = UINT_MAX;
	};

	template <bool reverse, bool haveComparators, bool aggregationsOnly>
	void selectLoop(LoopCtx &ctx, QueryResults &result, const RdxContext &);
	template <bool desc, bool multiColumnSort, typename It>
	It applyForcedSort(It begin, It end, const ItemComparator &, const SelectCtx &ctx);
	template <typename It>
	void applyGeneralSort(It itFirst, It itLast, It itEnd, const ItemComparator &, const SelectCtx &ctx);

	template <bool aggregationsOnly>
	void addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
						 QueryResults &result);

	h_vector<Aggregator, 4> getAggregators(const Query &) const;
	void setLimitAndOffset(ItemRefVector &result, size_t offset, size_t limit);
	void prepareSortingContext(SortingEntries &sortBy, SelectCtx &ctx, bool isFt, bool availableSelectBySortIndex);
	void getSortIndexValue(const SortingContext &sortCtx, IdType rowId, VariantArray &value, uint8_t proc, joins::NamespaceResults &,
						   const JoinedSelectors &);
	void processLeftJoins(QueryResults &qr, SelectCtx &sctx, size_t startPos);
	bool checkIfThereAreLeftJoins(SelectCtx &sctx) const;
	template <typename It>
	void sortResults(LoopCtx &sctx, It begin, It end, const SortingOptions &sortingOptions);

	bool isSortOptimizatonEffective(const QueryEntries &qe, SelectCtx &ctx, const RdxContext &rdxCtx);

	NamespaceImpl *ns_;
	SelectFunction::Ptr fnc_;
	FtCtx::Ptr ft_ctx_;
};
}  // namespace reindexer
