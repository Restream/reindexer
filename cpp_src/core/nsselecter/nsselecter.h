#pragma once
#include "aggregator.h"
#include "core/index/index.h"
#include "joinedselector.h"
#include "sortingcontext.h"

namespace reindexer {

struct SelectCtx {
	explicit SelectCtx(const Query &query_, const Query *parentQuery_) : query(query_), parentQuery(parentQuery_) {}
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

	const Query *parentQuery = nullptr;
	bool requiresCrashTracking = false;
};

class ItemComparator;
class ExplainCalc;
class QueryPreprocessor;

class NsSelecter {
public:
	NsSelecter(NamespaceImpl *parent) : ns_(parent) {}

	void operator()(LocalQueryResults &result, SelectCtx &ctx, const RdxContext &);

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
	void selectLoop(LoopCtx &ctx, LocalQueryResults &result, const RdxContext &);
	template <bool desc, bool multiColumnSort, typename It>
	It applyForcedSort(It begin, It end, const ItemComparator &, const SelectCtx &ctx);
	template <typename It>
	void applyGeneralSort(It itFirst, It itLast, It itEnd, const ItemComparator &, const SelectCtx &ctx);

	void calculateSortExpressions(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &, const LocalQueryResults &);
	template <bool aggregationsOnly>
	void addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
						 LocalQueryResults &result);

	h_vector<Aggregator, 4> getAggregators(const Query &) const;
	void setLimitAndOffset(ItemRefVector &result, size_t offset, size_t limit);
	void prepareSortingContext(SortingEntries &sortBy, SelectCtx &ctx, bool isFt, bool availableSelectBySortIndex);
	void prepareSortIndex(std::string_view column, int &index, bool &skipSortingEntry, StrictMode);
	static void prepareSortJoinedIndex(size_t nsIdx, std::string_view column, int &index, const std::vector<JoinedSelector> &,
									   bool &skipSortingEntry, StrictMode);
	void getSortIndexValue(const SortingContext &sortCtx, IdType rowId, VariantArray &value, uint8_t proc, const joins::NamespaceResults &,
						   const JoinedSelectors &);
	void processLeftJoins(LocalQueryResults &qr, SelectCtx &sctx, size_t startPos, const RdxContext &);
	bool checkIfThereAreLeftJoins(SelectCtx &sctx) const;
	template <typename It>
	void sortResults(LoopCtx &sctx, It begin, It end, const SortingOptions &sortingOptions);

	bool isSortOptimizatonEffective(const QueryEntries &qe, SelectCtx &ctx, const RdxContext &rdxCtx);
	static bool validateField(StrictMode strictMode, std::string_view name, const std::string &nsName, const TagsMatcher &tagsMatcher);

	NamespaceImpl *ns_;
	SelectFunction::Ptr fnc_;
	FtCtx::Ptr ft_ctx_;
};
}  // namespace reindexer
