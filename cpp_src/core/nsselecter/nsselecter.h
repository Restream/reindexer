#pragma once
#include "aggregator.h"
#include "core/index/index.h"
#include "core/nsselecter/joins/items_processor.h"
#include "ranks_holder.h"
#include "selectctx.h"
#include "sortingcontext.h"

namespace reindexer {

class ItemComparator;
class SingleQueryExplainCalc;
class QueryPreprocessor;

template <typename Result, typename PreSelectType>
class ResultHandler;

class [[nodiscard]] NsSelecter {
	template <typename It>
	class MainNsValueGetter;
	class JoinedNsValueGetter;

public:
	NsSelecter(NamespaceImpl* parent) noexcept : ns_(parent) {}

	template <typename JoinPreSelectCtx>
	void operator()(LocalQueryResults& result, SelectAndPreSelectCtx<JoinPreSelectCtx>& ctx, const RdxContext&);

	static size_t GetMaxScanIterations(const NamespaceImpl& ns, const SortingContext& sortingCtx);

protected:
	template <typename SelectCtxT>
	struct [[nodiscard]] LoopCtx {
		LoopCtx(SelectIteratorContainer& sIt, SelectCtxT& ctx, const QueryPreprocessor& qpp, h_vector<Aggregator, 4>& agg,
				SingleQueryExplainCalc& expl)
			: qres(sIt), sctx(ctx), qPreproc(qpp), aggregators(agg), explain(expl) {}

		SelectIteratorContainer& qres;
		bool calcTotal = false;
		SelectCtxT& sctx;
		const QueryPreprocessor& qPreproc;
		h_vector<Aggregator, 4>& aggregators;
		SingleQueryExplainCalc& explain;
		unsigned start = QueryEntry::kDefaultOffset;
		unsigned count = QueryEntry::kDefaultLimit;
		bool preselectForFt = false;
		bool calcAggsImmediately = true;
	};

	template <bool reverse, bool aggregationsOnly, typename ResultsT, typename SelectCtxT>
	void selectLoop(LoopCtx<SelectCtxT>& ctx, ResultsT& result, const RdxContext&);
	template <bool desc, bool multiColumnSort, typename It>
	It applyForcedSort(It begin, It end, const ItemComparator&, const SelectCtx& ctx, const joins::NamespaceResults*);
	template <bool desc, bool multiColumnSort, typename It, typename ValueGetter>
	static It applyForcedSortImpl(NamespaceImpl&, It begin, It end, const ItemComparator&, const VariantArray& forcedSortOrder,
								  const std::string& fieldName, const ValueGetter&);
	template <typename It>
	void applyGeneralSort(It itFirst, It itLast, It itEnd, const ItemComparator&, const SelectCtx& ctx);

	template <bool aggregationsOnly, typename ResultType, typename SelectCtxT>
	void addSelectResult(SelectCtxT& ctx, ResultHandler<ResultType, SelectCtxT>& resultHandler, RankT, IdType rowId, IdType properRowId,
						 h_vector<Aggregator, 4>& aggregators, bool needAggsCalc, bool preselectForFt);

	template <typename SelectCtxT, typename Results>
	bool sortPreSelectBuildValues(LoopCtx<SelectCtxT>& ctx, size_t initSize, const SortingOptions& sortingOptions, Results& results);

	h_vector<Aggregator, 4> getAggregators(const std::vector<AggregateEntry>& aggEntrys, StrictMode strictMode) const;
	void setLimitAndOffset(ItemRefVector& result, size_t offset, size_t limit);
	void prepareSortingContext(SortingEntries& sortBy, SelectCtx& ctx, QueryRankType, IndexValueType rankedIndexNo,
							   bool availableSelectBySortIndex) const;
	static void prepareSortIndex(const NamespaceImpl&, std::string& column, int& index, StrictMode, IsRanked);
	static void prepareSortJoinedIndex(size_t nsIdx, std::string_view column, int& index, const std::vector<joins::ItemsProcessor>&,
									   StrictMode);
	void getSortIndexValue(const SortingContext& sortCtx, IdType rowId, VariantArray& value, RankT, const joins::NamespaceResults*,
						   const joins::ItemsProcessors&, int shardId);
	const CollateOpts& getSortIndexCollateOpts(const SortingContext& sortCtx, const joins::ItemsProcessors&);
	void processLeftJoins(LocalQueryResults& qr, SelectCtx& sctx, size_t startPos, const RdxContext&);
	bool checkIfThereAreLeftJoins(SelectCtx& sctx) const;
	template <typename It, typename SelectCtxT>
	void sortResults(LoopCtx<SelectCtxT>& sctx, It begin, It end, const SortingOptions& sortingOptions, const joins::NamespaceResults*);

	size_t calculateNormalCost(const QueryEntries& qe, SelectCtx& ctx, const RdxContext& rdxCtx);
	size_t calculateOptimizedCost(size_t costNormal, const QueryEntries& qe, SelectCtx& ctx, const RdxContext& rdxCtx);
	bool isSortOptimizationEffective(const QueryEntries& qe, SelectCtx& ctx, const RdxContext& rdxCtx);
	static void validateField(StrictMode strictMode, std::string_view name, const NamespaceName& nsName, const TagsMatcher& tagsMatcher);
	void checkStrictModeAgg(StrictMode strictMode, std::string_view name, const NamespaceName& nsName,
							const TagsMatcher& tagsMatcher) const;

	void writeAggregationResultMergeSubQuery(LocalQueryResults& result, h_vector<Aggregator, 4>&& aggregators, SelectCtx& ctx);
	[[noreturn]] RX_NO_INLINE void throwIncorrectRowIdInSortOrders(int rowId, const Index& firstSortIndex,
																   const SelectIterator& firstIterator);
	[[noreturn]] RX_NO_INLINE void throwUnexpectedItemID(IdType rowId, IdType properRowId);
	template <typename SelectCtxT>
	void holdFloatVectors(LocalQueryResults&, SelectCtxT&, size_t offset, const FieldsFilter&) const;
	NamespaceImpl* ns_;
	FtFunction::Ptr ftFunc_;
	RanksHolder::Ptr ranks_;
};

}  // namespace reindexer
