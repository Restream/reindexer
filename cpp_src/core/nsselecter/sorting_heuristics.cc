#include "sorting_heuristics.h"
#include "core/query/queryentry.h"
#include "selectctx.h"
#include "selectiteratorcontainer.h"
namespace reindexer::sorting_heuristics {

namespace {

struct [[nodiscard]] CostCalcResults {
	size_t expectedMaxIters;
	std::optional<size_t> bestDistinctUniques;
};

struct [[nodiscard]] FoundIndexInfo {
	enum class [[nodiscard]] ConditionType { Incompatible = 0, Compatible = 1 };

	FoundIndexInfo() noexcept : index(nullptr), size(0), isFitForSortOptimization(0) {}
	FoundIndexInfo(const Index* i, ConditionType ct) noexcept : index(i), size(i->Size()), isFitForSortOptimization(unsigned(ct)) {}

	const Index* index;
	uint64_t size : 63;
	uint64_t isFitForSortOptimization : 1;
};

class [[nodiscard]] CostCalculator {
public:
	explicit CostCalculator(size_t _expectedMaxIters) noexcept : expectedMaxIters_(_expectedMaxIters) {}
	void BeginSequence() noexcept {
		isInSequence_ = true;
		hasInappositeEntries_ = false;
		onlyTargetIdxInSequence_ = true;
		curMaxIters_ = 0;
	}
	void EndSequence() noexcept {
		if (isInSequence_ && !hasInappositeEntries_) {
			expectedMaxIters_ = std::min(curMaxIters_, expectedMaxIters_);
		}
		isInSequence_ = false;
		onlyTargetIdxInSequence_ = true;
		curMaxIters_ = 0;
	}
	bool IsInOrSequence() const noexcept { return isInSequence_; }
	void Add(const SelectKeyResults& results, bool isTargetSortIndex, IsDistinct distinct, const Index& idx) {
		onlyTargetIdxInSequence_ = onlyTargetIdxInSequence_ && isTargetSortIndex;
		Add(results, distinct, idx);
	}
	void Add(const SelectKeyResults& results, IsDistinct distinct, const Index& idx) {
		std::visit(
			overloaded{
				[&](const SelectKeyResultsVector& selRes) {
					size_t cost = 0;
					for (const SelectKeyResult& res : selRes) {
						cost += res.GetMaxIterations(expectedMaxIters_);
					}
					if (isInSequence_) {
						curMaxIters_ += cost;
					} else {
						expectedMaxIters_ = std::min(expectedMaxIters_, cost);
						if (distinct) {
							bestDistinctUniqueKeys_ = std::min(bestDistinctUniqueKeys_, idx.Size());
						}
					}
				},
				[this](const concepts::OneOf<ComparatorNotIndexed, Template<ComparatorIndexed, bool, int, int64_t, double, key_string,
																			PayloadValue, Point, Uuid, FloatVector>> auto&) {
					hasInappositeEntries_ = true;
				}},
			results.AsVariant());
	}
	CostCalcResults GetResults() const noexcept {
		return (bestDistinctUniqueKeys_ == std::numeric_limits<size_t>::max())
				   ? CostCalcResults{.expectedMaxIters = expectedMaxIters_, .bestDistinctUniques = std::nullopt}
				   : CostCalcResults{.expectedMaxIters = expectedMaxIters_, .bestDistinctUniques = bestDistinctUniqueKeys_};
	}
	void MarkInapposite() noexcept { hasInappositeEntries_ = true; }
	bool OnNewEntry(const QueryEntries& qentries, size_t i, size_t next) {
		const OpType op = qentries.GetOperation(i);
		switch (op) {
			case OpAnd: {
				EndSequence();
				if (next != qentries.Size() && qentries.GetOperation(next) == OpOr) {
					BeginSequence();
				}
				return true;
			}
			case OpOr: {
				if (hasInappositeEntries_) {
					return false;
				}
				if (next != qentries.Size() && qentries.GetOperation(next) == OpOr) {
					BeginSequence();
				}
				return true;
			}
			case OpNot: {
				if (next != qentries.Size() && qentries.GetOperation(next) == OpOr) {
					BeginSequence();
				}
				hasInappositeEntries_ = true;
				return false;
			}
		}
		throw Error(errLogic, "Unexpected op value: {}", int(op));
	}

private:
	bool isInSequence_ = false;
	bool onlyTargetIdxInSequence_ = true;
	bool hasInappositeEntries_ = false;
	size_t curMaxIters_ = 0;
	size_t expectedMaxIters_ = std::numeric_limits<size_t>::max();
	size_t bestDistinctUniqueKeys_ = std::numeric_limits<size_t>::max();
};

CostCalcResults calculateNormalCost(const QueryEntries& qentries, const SelectCtx& ctx, const NamespaceData& nsData,
									const RdxContext& rdxCtx) {
	const size_t totalItemsCount = nsData.itemsCount;
	CostCalculator CostCalculator(totalItemsCount);
	enum { SortIndexNotFound = 0, SortIndexFound, SortIndexHasUnorderedConditions } sortIndexSearchState = SortIndexNotFound;
	for (size_t next, i = 0, sz = qentries.Size(); i != sz; i = next) {
		next = qentries.Next(i);
		const bool calculateEntry = CostCalculator.OnNewEntry(qentries, i, next);
		qentries.Visit(
			i,
			[] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, SubQueryFunctionEntry> auto&)
				RX_POST_LMBD_ALWAYS_INLINE { throw_as_assert; },
			Skip<AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry, QueryFunctionEntry>{},
			[&CostCalculator] RX_PRE_LMBD_ALWAYS_INLINE(
				const concepts::OneOf<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, KnnQueryEntry> auto&)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { CostCalculator.MarkInapposite(); },
			[&](const QueryEntry& qe) {
				if (!qe.IsFieldIndexed()) {
					CostCalculator.MarkInapposite();
					return;
				}
				if (qe.IndexNo() == ctx.sortingContext.uncommitedIndex) {
					if (sortIndexSearchState == SortIndexNotFound) {
						const bool isExpectingIdSet =
							qentries.GetOperation(i) == OpAnd && (next == sz || qentries.GetOperation(next) != OpOr);
						if (isExpectingIdSet && !IsExpectingOrderedResults(qe)) {
							sortIndexSearchState = SortIndexHasUnorderedConditions;
							return;
						} else {
							sortIndexSearchState = SortIndexFound;
						}
					}
				}

				if (!calculateEntry || CostCalculator.GetResults().expectedMaxIters == 0 ||
					sortIndexSearchState == SortIndexHasUnorderedConditions) {
					return;
				}

				auto& index = *nsData.indexes[qe.IndexNo()];
				if (IsFullText(index.Type())) [[unlikely]] {
					CostCalculator.MarkInapposite();
					return;
				}

				Index::SelectContext indexSelectContext;
				indexSelectContext.opts.disableIdSetCache = 1;
				indexSelectContext.opts.itemsCountInNamespace = totalItemsCount;
				indexSelectContext.opts.indexesNotOptimized = !ctx.sortingContext.enableSortOrders;
				indexSelectContext.opts.inTransaction = ctx.inTransaction;
				indexSelectContext.opts.distinct = qe.Distinct() ? 1 : 0;

				try {
					SelectKeyResults results = index.SelectKey(qe.Values(), qe.Condition(), 0, indexSelectContext, rdxCtx);
					CostCalculator.Add(results, qe.IndexNo() == ctx.sortingContext.uncommitedIndex, qe.Distinct(), index);
				} catch (const Error&) {
					CostCalculator.MarkInapposite();
				}
			});
	}
	CostCalculator.EndSequence();

	if (sortIndexSearchState == SortIndexHasUnorderedConditions) {
		return CostCalcResults{.expectedMaxIters = 0, .bestDistinctUniques = std::nullopt};
	}
	return CostCalculator.GetResults();
}

size_t calculateOptimizedCost(size_t costNormal, const QueryEntries& qentries, const SelectCtx& ctx, const NamespaceData& nsData,
							  const RdxContext& rdxCtx) {
	// 'costOptimized == costNormal + 1' reduces internal iterations count for the tree in the res.GetMaxIterations() call
	CostCalculator CostCalculator(costNormal + 1);
	for (size_t next, i = 0, sz = qentries.Size(); i != sz; i = next) {
		next = qentries.Next(i);
		if (!CostCalculator.OnNewEntry(qentries, i, next)) {
			continue;
		}
		qentries.Visit(
			i, Skip<AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry>{},
			[] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, SubQueryFunctionEntry> auto&)
				RX_POST_LMBD_ALWAYS_INLINE { throw_as_assert; },
			[&CostCalculator] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry,
																			  KnnQueryEntry, QueryFunctionEntry> auto&)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { CostCalculator.MarkInapposite(); },
			[&](const QueryEntry& qe) {
				if (!qe.IsFieldIndexed() || qe.IndexNo() != ctx.sortingContext.uncommitedIndex) {
					CostCalculator.MarkInapposite();
					return;
				}

				const bool isIsolated = qentries.GetOperation(i) == OpAnd && (next == sz || qentries.GetOperation(next) != OpOr);
				auto& index = *nsData.indexes[qe.IndexNo()];

				if (isIsolated && !qe.Distinct()) {
					Index::SelectContext indexSelectContext;
					indexSelectContext.opts.itemsCountInNamespace = nsData.itemsCount;
					indexSelectContext.opts.disableIdSetCache = 1;
					indexSelectContext.opts.unbuiltSortOrders = 1;
					indexSelectContext.opts.indexesNotOptimized = !ctx.sortingContext.enableSortOrders;
					indexSelectContext.opts.inTransaction = ctx.inTransaction;

					try {
						auto results = index.SelectKey(qe.Values(), qe.Condition(), 0, indexSelectContext, rdxCtx);
						CostCalculator.Add(results, IsDistinct_False, index);
					} catch (std::exception&) {
						CostCalculator.MarkInapposite();
					}
				} else {
					// Non-isolated and distinct sorting filters will create scan select results in
					// SelectIteratorContainer::prepareIteratorsForSelectLoop
					CostCalculator.MarkInapposite();
				}
			});
	}
	CostCalculator.EndSequence();
	return CostCalculator.GetResults().expectedMaxIters;
}

}  // namespace

bool IsSortOptimizationEffective(const QueryEntries& qentries, const SelectCtx& ctx, bool needCalcTotal, const NamespaceData& nsData,
								 const RdxContext& rdxCtx) {
	assertrx_dbg(!ctx.sortingContext.sortIndex()->Opts().IsArray());
	if (qentries.Size() == 0) {
		return true;
	}
	if (qentries.Size() == 1 && qentries.Is<QueryEntry>(0)) {
		const auto& qe = qentries.Get<QueryEntry>(0);
		if (qe.IndexNo() == ctx.sortingContext.uncommitedIndex) {
			return IsExpectingOrderedResults(qe);
		}
	}

	const auto expectedNormal = calculateNormalCost(qentries, ctx, nsData, rdxCtx);
	const auto expectedMaxIterationsNormal = expectedNormal.expectedMaxIters;
	if (expectedMaxIterationsNormal == 0) {
		return false;
	}
	const size_t totalItemsCount = nsData.itemsCount;
	// '3' is empirical constant here
	const auto costNormalNoDistincts = size_t(double(expectedMaxIterationsNormal) * log2(expectedMaxIterationsNormal)) / 3;
	// If we have distinct filters, than keys count for sorting is limited by min unique keys count
	const auto costNormal = (expectedNormal.bestDistinctUniques
								 ? std::min(size_t(double(*expectedNormal.bestDistinctUniques) * log2(*expectedNormal.bestDistinctUniques)),
											costNormalNoDistincts)
								 : costNormalNoDistincts);
	if (costNormal >= totalItemsCount) {
		// Check if it's more effective to iterate over all the items via btree, than select and sort ids via the most effective index
		return true;
	}

	const bool expectingLimitedIterations = !ctx.isForceAll && !needCalcTotal && ctx.HasLimit();
	// If query has limit, 'costOptimized' must be calculated as accurate as possible, because it will be used in further calculations.
	// If query must perform full iterations loop, than we may use 'costNormal' as upper limit for 'costOptimized'.
	const size_t maxOptimizedCost = expectingLimitedIterations ? totalItemsCount : costNormal;
	size_t costOptimized = calculateOptimizedCost(maxOptimizedCost, qentries, ctx, nsData, rdxCtx);
	if (costNormal >= costOptimized) {
		return true;  // If max iterations count with btree indexes is better than with any other condition (including sort overhead)
	}
	if (expectedMaxIterationsNormal <= 150) {
		return false;  // If there is very good filtering condition (case for the issues #1489)
	}
	if (ctx.isForceAll || ctx.HasLimit() || needCalcTotal) {
		if (expectedMaxIterationsNormal < 2000) {
			return false;  // Skip attempt to check limit if there is good enough unordered filtering condition
		}
	}
	if (expectingLimitedIterations) {
		// If optimization will be disabled, selector will must iterate over all the results, ignoring limit
		// Experimental value. It was chosen during debugging request from issue #1402.
		// TODO: It's possible to evaluate this multiplier, based on the query conditions, but the only way to avoid corner cases is to
		// allow user to hint this optimization.
		const size_t limitMultiplier = std::max(size_t(20), size_t(totalItemsCount / expectedMaxIterationsNormal) * 4);
		const auto offset = ctx.HasOffset() ? ctx.offset : 1;
		costOptimized = limitMultiplier * (ctx.limit + offset);
	}
	return costOptimized <= costNormal;
}

static void findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end, h_vector<FoundIndexInfo, 32>& foundIndexes,
						 const NamespaceData& nsData) {
	bool hasRootLevelDistincts = false;
	bool hasUnorderedConds = false;
	for (auto it = begin; it != end; ++it) {
		const auto foundIdx = it->Visit(
			[](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, SubQueryFunctionEntry> auto&) -> FoundIndexInfo {
				throw_as_assert;
			},
			[&](const QueryEntry& entry) -> FoundIndexInfo {
				// Consider only isolated root entries with ordered indexes
				auto cur = it, next = it;
				++next;
				const bool isIsolated = cur->operation == OpAnd && (next == end || next->operation != OpOr);
				hasRootLevelDistincts |= entry.IsFieldIndexed() && entry.Distinct();
				if (entry.IsFieldIndexed() && isIsolated) {
					const auto& index = *nsData.indexes[entry.IndexNo()];
					const auto cond = entry.Condition();
					const bool maybeGoodUnorderedCond = (cond == CondEq || cond == CondSet || cond == CondAllSet);
					if (maybeGoodUnorderedCond && IsHashOrBTree(index.Type())) {
						const auto itemsCount = nsData.itemsCount;
						const auto avgSelectivityPercentPerKey = itemsCount ? (index.Size() * 100ull / itemsCount) : 0;
						if (avgSelectivityPercentPerKey * entry.Values().size() < kMaxSelectivityPercentForIdset) {
							hasUnorderedConds = true;
						}
					}
					// Distinct is not compatible with 'unbuiltSortOrders' mode - it will awlays create comparator
					if (index.IsOrdered() && !index.Opts().IsArray() && !entry.Distinct()) {
						if (index::IsOrderedCondition(cond)) {
							return FoundIndexInfo{&index, FoundIndexInfo::ConditionType::Compatible};
						} else if (maybeGoodUnorderedCond) {
							// Do not apply implicit sort if one of those conditions exist
							return FoundIndexInfo{&index, FoundIndexInfo::ConditionType::Incompatible};
						}
					}
				}
				return {};
			},
			[](const concepts::OneOf<JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue, KnnQueryEntry,
									 MultiDistinctQueryEntry, QueryEntriesBracket, QueryFunctionEntry> auto&) noexcept {
				return FoundIndexInfo();
			});
		if (hasRootLevelDistincts && hasUnorderedConds) {
			// If there are some potentially good ordered conditions, combined with disctincts, it's usually better to avoid implicit
			// sorting. Looks like distinc's selectivity does not matter here
			foundIndexes.clear();
			return;
		}
		if (foundIdx.index) {
			auto found = std::find_if(foundIndexes.begin(), foundIndexes.end(),
									  [foundIdx](const FoundIndexInfo& i) { return i.index == foundIdx.index; });
			if (found == foundIndexes.end()) {
				foundIndexes.emplace_back(foundIdx);
			} else {
				found->isFitForSortOptimization &= foundIdx.isFitForSortOptimization;
			}
		}
	}
}

const Index* AdviceSortingIndex(const QueryEntries& qentries, const NamespaceData& nsData) {
	thread_local h_vector<FoundIndexInfo, 32> foundIndexes;
	foundIndexes.clear<false>();
	findMaxIndex(qentries.cbegin(), qentries.cend(), foundIndexes, nsData);
	boost::sort::pdqsort(foundIndexes.begin(), foundIndexes.end(), [](const FoundIndexInfo& l, const FoundIndexInfo& r) noexcept {
		if (l.isFitForSortOptimization > r.isFitForSortOptimization) {
			return true;
		}
		if (l.isFitForSortOptimization == r.isFitForSortOptimization) {
			return l.size > r.size;
		}
		return false;
	});
	if (!foundIndexes.empty() && foundIndexes[0].isFitForSortOptimization) {
		return foundIndexes[0].index;
	}
	return nullptr;
}

bool IsExpectingOrderedResults(const QueryEntry& qe) noexcept {
	const auto cond = qe.Condition();
	if (index::IsOrderedCondition(cond)) {
		return true;
	}
	switch (cond) {
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
		case CondRange:
		case CondAny:
		case CondEq:
		case CondSet:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
			return qe.Values().size() <= 1;
		case CondDWithin:
		case CondKnn:
			return false;
		default:
			std::abort();
	}
}

}  // namespace reindexer::sorting_heuristics