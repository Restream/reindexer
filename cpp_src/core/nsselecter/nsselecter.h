#pragma once
#include "aggregator.h"
#include "core/enums.h"
#include "core/index/index.h"
#include "joinedselector.h"
#include "selectctx.h"
#include "sortingcontext.h"

namespace reindexer {

template <typename JoinPreSelCtx>
struct SelectCtxWithJoinPreSelect : public SelectCtx {
	explicit SelectCtxWithJoinPreSelect(const Query& query, const Query* parentQuery, JoinPreSelCtx preSel,
										FloatVectorsHolderMap* fvHolder) noexcept
		: SelectCtx(query, parentQuery, fvHolder), preSelect{std::move(preSel)} {}
	JoinPreSelCtx preSelect;
};

template <>
struct SelectCtxWithJoinPreSelect<void> : public SelectCtx {
	explicit SelectCtxWithJoinPreSelect(const Query& query, const Query* parentQuery, FloatVectorsHolderMap* fvHolder) noexcept
		: SelectCtx(query, parentQuery, fvHolder) {}
};
SelectCtxWithJoinPreSelect(const Query&, const Query*, FloatVectorsHolderMap*) -> SelectCtxWithJoinPreSelect<void>;

class ItemComparator;
class ExplainCalc;
class QueryPreprocessor;

class NsSelecter {
	template <typename It>
	class MainNsValueGetter;
	class JoinedNsValueGetter;

public:
	NsSelecter(NamespaceImpl* parent) noexcept : ns_(parent) {}

	template <typename JoinPreResultCtx>
	void operator()(LocalQueryResults& result, SelectCtxWithJoinPreSelect<JoinPreResultCtx>& ctx, const RdxContext&);

private:
	template <typename JoinPreResultCtx>
	struct LoopCtx {
		LoopCtx(SelectIteratorContainer& sIt, SelectCtxWithJoinPreSelect<JoinPreResultCtx>& ctx, const QueryPreprocessor& qpp,
				h_vector<Aggregator, 4>& agg, ExplainCalc& expl)
			: qres(sIt), sctx(ctx), qPreproc(qpp), aggregators(agg), explain(expl) {}
		SelectIteratorContainer& qres;
		bool calcTotal = false;
		SelectCtxWithJoinPreSelect<JoinPreResultCtx>& sctx;
		const QueryPreprocessor& qPreproc;
		h_vector<Aggregator, 4>& aggregators;
		ExplainCalc& explain;
		unsigned start = QueryEntry::kDefaultOffset;
		unsigned count = QueryEntry::kDefaultLimit;
		bool preselectForFt = false;
	};

	template <bool reverse, bool haveComparators, bool aggregationsOnly, typename ResultsT, typename JoinPreResultCtx>
	void selectLoop(LoopCtx<JoinPreResultCtx>& ctx, ResultsT& result, const RdxContext&);
	template <bool desc, bool multiColumnSort, typename It>
	It applyForcedSort(It begin, It end, const ItemComparator&, const SelectCtx& ctx, const joins::NamespaceResults*);
	template <bool desc, bool multiColumnSort, typename It, typename ValueGetter>
	static It applyForcedSortImpl(NamespaceImpl&, It begin, It end, const ItemComparator&, const std::vector<Variant>& forcedSortOrder,
								  const std::string& fieldName, const ValueGetter&);
	template <typename It>
	void applyGeneralSort(It itFirst, It itLast, It itEnd, const ItemComparator&, const SelectCtx& ctx);

	void calculateSortExpressions(RankT, IdType rowId, IdType properRowId, SelectCtx&, const LocalQueryResults&);
	template <bool aggregationsOnly, typename JoinPreResultCtx>
	void addSelectResult(RankT, IdType rowId, IdType properRowId, SelectCtxWithJoinPreSelect<JoinPreResultCtx>& sctx,
						 h_vector<Aggregator, 4>& aggregators, LocalQueryResults& result, bool preselectForFt);

	h_vector<Aggregator, 4> getAggregators(const std::vector<AggregateEntry>& aggEntrys, StrictMode strictMode) const;
	void setLimitAndOffset(ItemRefVector& result, size_t offset, size_t limit);
	void prepareSortingContext(SortingEntries& sortBy, SelectCtx& ctx, IsRanked, bool availableSelectBySortIndex);
	static void prepareSortIndex(const NamespaceImpl&, std::string& column, int& index, bool& skipSortingEntry, StrictMode);
	static void prepareSortJoinedIndex(size_t nsIdx, std::string_view column, int& index, const std::vector<JoinedSelector>&,
									   bool& skipSortingEntry, StrictMode);
	void getSortIndexValue(const SortingContext& sortCtx, IdType rowId, VariantArray& value, RankT, const joins::NamespaceResults*,
						   const JoinedSelectors&);
	void processLeftJoins(LocalQueryResults& qr, SelectCtx& sctx, size_t startPos, const RdxContext&);
	bool checkIfThereAreLeftJoins(SelectCtx& sctx) const;
	template <typename It, typename JoinPreResultCtx>
	void sortResults(LoopCtx<JoinPreResultCtx>& sctx, It begin, It end, const SortingOptions& sortingOptions,
					 const joins::NamespaceResults*);

	size_t calculateNormalCost(const QueryEntries& qe, SelectCtx& ctx, const RdxContext& rdxCtx);
	size_t calculateOptimizedCost(size_t costNormal, const QueryEntries& qe, SelectCtx& ctx, const RdxContext& rdxCtx);
	bool isSortOptimizationEffective(const QueryEntries& qe, SelectCtx& ctx, const RdxContext& rdxCtx);
	static bool validateField(StrictMode strictMode, std::string_view name, const NamespaceName& nsName, const TagsMatcher& tagsMatcher);
	void checkStrictModeAgg(StrictMode strictMode, std::string_view name, const NamespaceName& nsName,
							const TagsMatcher& tagsMatcher) const;

	void writeAggregationResultMergeSubQuery(LocalQueryResults& result, h_vector<Aggregator, 4>& aggregators, SelectCtx& ctx);
	[[noreturn]] RX_NO_INLINE void throwIncorrectRowIdInSortOrders(int rowId, const Index& firstSortIndex,
																   const SelectIterator& firstIterator);
	template <typename JoinPreResultCtx>
	void holdFloatVectors(LocalQueryResults&, SelectCtxWithJoinPreSelect<JoinPreResultCtx>&, size_t offset, const FieldsFilter&) const;
	NamespaceImpl* ns_;
	SelectFunction::Ptr fnc_;
	BaseFunctionCtx::Ptr rankedCtx_;
};
}  // namespace reindexer
