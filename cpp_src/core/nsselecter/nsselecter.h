#pragma once
#include "aggregator.h"
#include "core/index/index.h"
#include "explaincalc.h"
#include "joinedselector.h"
#include "sortingcontext.h"

namespace reindexer {

enum class IsMergeQuery : bool { Yes = true, No = false };
enum class IsFTQuery { Yes, No, NotSet };

struct SelectCtx {
	explicit SelectCtx(const Query &query_, const Query *parentQuery_) noexcept : query(query_), parentQuery(parentQuery_) {}
	const Query &query;
	JoinedSelectors *joinedSelectors = nullptr;
	SelectFunctionsHolder *functions = nullptr;

	JoinPreResult::Ptr preResult;
	ExplainCalc::Duration preResultTimeTotal = ExplainCalc::Duration::zero();
	SortingContext sortingContext;
	uint8_t nsid = 0;
	bool isForceAll = false;
	bool skipIndexesLookup = false;
	bool matchedAtLeastOnce = false;
	bool reqMatchedOnceFlag = false;
	bool contextCollectingMode = false;
	bool inTransaction = false;
	IsMergeQuery isMergeQuery = IsMergeQuery::No;
	IsFTQuery isFtQuery = IsFTQuery::NotSet;
	QueryType crashReporterQueryType = QuerySelect;

	const Query *parentQuery = nullptr;
	ExplainCalc explain;
	bool requiresCrashTracking = false;
	std::vector<SubQueryExplain> subQueriesExplains;

	RX_ALWAYS_INLINE bool isMergeQuerySubQuery() const noexcept { return isMergeQuery == IsMergeQuery::Yes && parentQuery; }
};

class ItemComparator;
class ExplainCalc;
class QueryPreprocessor;

class NsSelecter {
	template <typename It>
	class MainNsValueGetter;
	class JoinedNsValueGetter;

public:
	NsSelecter(NamespaceImpl *parent) noexcept : ns_(parent) {}

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
		unsigned start = QueryEntry::kDefaultOffset;
		unsigned count = QueryEntry::kDefaultLimit;
		bool preselectForFt = false;
	};

	template <bool reverse, bool haveComparators, bool aggregationsOnly, typename ResultsT>
	void selectLoop(LoopCtx &ctx, ResultsT &result, const RdxContext &);
	template <bool desc, bool multiColumnSort, typename It>
	It applyForcedSort(It begin, It end, const ItemComparator &, const SelectCtx &ctx, const joins::NamespaceResults *);
	template <bool desc, bool multiColumnSort, typename It, typename ValueGetter>
	static It applyForcedSortImpl(NamespaceImpl &, It begin, It end, const ItemComparator &, const std::vector<Variant> &forcedSortOrder,
								  const std::string &fieldName, const ValueGetter &);
	template <typename It>
	void applyGeneralSort(It itFirst, It itLast, It itEnd, const ItemComparator &, const SelectCtx &ctx);

	void calculateSortExpressions(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &, const LocalQueryResults &);
	template <bool aggregationsOnly>
	void addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
						 LocalQueryResults &result, bool preselectForFt);

	h_vector<Aggregator, 4> getAggregators(const std::vector<AggregateEntry> &aggEntrys, StrictMode strictMode) const;
	void setLimitAndOffset(ItemRefVector &result, size_t offset, size_t limit);
	void prepareSortingContext(SortingEntries &sortBy, SelectCtx &ctx, bool isFt, bool availableSelectBySortIndex);
	static void prepareSortIndex(const NamespaceImpl &, std::string &column, int &index, bool &skipSortingEntry, StrictMode);
	static void prepareSortJoinedIndex(size_t nsIdx, std::string_view column, int &index, const std::vector<JoinedSelector> &,
									   bool &skipSortingEntry, StrictMode);
	void getSortIndexValue(const SortingContext &sortCtx, IdType rowId, VariantArray &value, uint8_t proc, const joins::NamespaceResults *,
						   const JoinedSelectors &);
	void processLeftJoins(LocalQueryResults &qr, SelectCtx &sctx, size_t startPos, const RdxContext &);
	bool checkIfThereAreLeftJoins(SelectCtx &sctx) const;
	template <typename It>
	void sortResults(LoopCtx &sctx, It begin, It end, const SortingOptions &sortingOptions, const joins::NamespaceResults *);

	size_t calculateNormalCost(const QueryEntries &qe, SelectCtx &ctx, const RdxContext &rdxCtx);
	size_t calculateOptimizedCost(size_t costNormal, const QueryEntries &qe, SelectCtx &ctx, const RdxContext &rdxCtx);
	bool isSortOptimizatonEffective(const QueryEntries &qe, SelectCtx &ctx, const RdxContext &rdxCtx);
	static bool validateField(StrictMode strictMode, std::string_view name, std::string_view nsName, const TagsMatcher &tagsMatcher);
	void checkStrictModeAgg(StrictMode strictMode, const std::string &name, const std::string &nsName,
							const TagsMatcher &tagsMatcher) const;

	void writeAggregationResultMergeSubQuery(LocalQueryResults &result, h_vector<Aggregator, 4> &aggregators, SelectCtx &ctx);
	NamespaceImpl *ns_;
	SelectFunction::Ptr fnc_;
	FtCtx::Ptr ft_ctx_;
};
}  // namespace reindexer
