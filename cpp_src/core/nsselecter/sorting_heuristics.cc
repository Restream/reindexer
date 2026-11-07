#include "sorting_heuristics.h"
#include <numeric>
#include "core/query/queryentry.h"
#include "selectctx.h"
#include "selectiteratorcontainer.h"
namespace reindexer::sorting_heuristics {

namespace {

RX_ALWAYS_INLINE bool isIsolatedEntry(OpType op, bool nextIsOr) noexcept { return op == OpAnd && !nextIsOr; }

RX_ALWAYS_INLINE bool isIsolatedEntry(const QueryEntries& qentries, size_t i, size_t next) noexcept {
	return isIsolatedEntry(qentries.GetOperation(i), next != qentries.Size() && qentries.GetOperation(next) == OpOr);
}

enum class [[nodiscard]] DistinctOnSortIndex : uint8_t {
	None = 0,
	Unordered,	// Distinct==sort, unordered condition
	Ordered,	// ordered Distinct==sort with fully packed SelectKey
	WideRange,	// ordered Distinct==sort, SelectKey not fully packed
};

struct [[nodiscard]] CostCalcResults {
	size_t expectedMaxIters;
	std::optional<size_t> bestDistinctUniques;
	DistinctOnSortIndex distinctOnSortIndex = DistinctOnSortIndex::None;
};

bool hasCompletePackedKeys(const SelectKeyResults& results) {
	return std::visit(overloaded{[](const SelectKeyResultsVector& selRes) {
									 if (selRes.empty()) {
										 return false;
									 }
									 for (const SelectKeyResult& res : selRes) {
										 if (!res.GetPackedKeysCount()) {
											 return false;
										 }
									 }
									 return true;
								 },
								 [](const auto&) { return false; }},
					  results.AsVariant());
}

struct [[nodiscard]] FoundIndexInfo {
	enum class [[nodiscard]] ConditionType { Incompatible = 0, Compatible = 1 };

	FoundIndexInfo() noexcept
		: index(nullptr),
		  entry(nullptr),
		  estimate{.keys = 0, .ids = 0, .indexSize = 0, .complete = false},
		  isFitForSortOptimization(false),
		  scoreByKeys(false) {}
	FoundIndexInfo(const Index* i, const QueryEntry* e, ConditionType ct) noexcept
		: index(i),
		  entry(e),
		  estimate{.keys = 0, .ids = 0, .indexSize = i->Size(), .complete = false},
		  isFitForSortOptimization(ct == ConditionType::Compatible),
		  scoreByKeys(false) {}

	const Index* index;
	const QueryEntry* entry;
	Index::OrderedConditionEstimate estimate;
	bool isFitForSortOptimization;
	bool scoreByKeys;
};

class [[nodiscard]] CostCalculator {
public:
	explicit CostCalculator(size_t _expectedMaxIters) noexcept : expectedMaxIters_(_expectedMaxIters) {}
	void BeginSequence() noexcept {
		isInSequence_ = true;
		hasInappositeEntries_ = false;
		curMaxIters_ = 0;
	}
	void EndSequence() noexcept {
		if (isInSequence_ && !hasInappositeEntries_) {
			expectedMaxIters_ = std::min(curMaxIters_, expectedMaxIters_);
		}
		isInSequence_ = false;
		curMaxIters_ = 0;
	}
	bool IsInOrSequence() const noexcept { return isInSequence_; }
	void Add(const SelectKeyResults& results, IsDistinct distinct = IsDistinct_False) {
		std::visit(
			overloaded{
				[&](const SelectKeyResultsVector& selRes) {
					const size_t cost = std::accumulate(selRes.begin(), selRes.end(), size_t{0},
														[limit = expectedMaxIters_](size_t c, const SelectKeyResult& res) noexcept {
															return c + res.EstimateMaxIterations(limit);
														});
					if (isInSequence_) {
						curMaxIters_ += cost;
					} else {
						expectedMaxIters_ = std::min(expectedMaxIters_, cost);
						// Prefer key count already materialised by SelectKey (one SingleSelectKeyResult per btree key
						// when Distinct packed explicit idsets). Avoids a second EstimateOrderedCondition walk.
						// GetPackedKeysCount is nullopt for SingleIterator / Range — do not treat size() as uniques.
						if (distinct) {
							size_t packedKeys = 0;
							for (const SelectKeyResult& res : selRes) {
								const auto keys = res.GetPackedKeysCount();
								if (!keys) {
									packedKeys = 0;
									break;
								}
								packedKeys += *keys;
							}
							if (packedKeys > 0) {
								bestDistinctUniqueKeys_ = std::min(bestDistinctUniqueKeys_, packedKeys);
							}
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
	bool IsExhausted() const noexcept { return expectedMaxIters_ == 0; }
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
	auto distinctOnSortIndex = DistinctOnSortIndex::None;
	for (size_t next, i = 0, sz = qentries.Size(); i != sz; i = next) {
		next = qentries.Next(i);
		const bool calculateEntry = CostCalculator.OnNewEntry(qentries, i, next);
		qentries.Visit(
			i,
			[] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, SubQueryFunctionEntry> auto&)
				RX_POST_LMBD_ALWAYS_INLINE { throw_as_assert; },
			Skip<AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry, QueryFunctionEntry, QueryArithmeticEntry>{},
			[&CostCalculator] RX_PRE_LMBD_ALWAYS_INLINE(
				const concepts::OneOf<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, KnnQueryEntry> auto&)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { CostCalculator.MarkInapposite(); },
			[&](const QueryEntry& qe) {
				if (!qe.IsFieldIndexed()) {
					CostCalculator.MarkInapposite();
					return;
				}
				const bool isIsolated = isIsolatedEntry(qentries, i, next);
				// Fold Distinct==sort detection into this walk (avoids a second qentries pass in IsSortOptimizationEffective).
				// Only isolated entries can become the unbuilt leader (same rule as enableSortIndexOptimize).
				const bool isDistinctOnSortIndex = isIsolated && qe.Distinct() && qe.IndexNo() == ctx.sortingContext.uncommitedIndex;
				if (distinctOnSortIndex == DistinctOnSortIndex::None && isDistinctOnSortIndex) {
					// Ordered starts as WideRange until SelectKey proves packed keys (conservative for costOptimized).
					distinctOnSortIndex =
						index::IsOrderedCondition(qe.Condition()) ? DistinctOnSortIndex::WideRange : DistinctOnSortIndex::Unordered;
				}
				if (qe.IndexNo() == ctx.sortingContext.uncommitedIndex) {
					if (sortIndexSearchState == SortIndexNotFound) {
						if (isIsolated && !IsExpectingOrderedResults(qe)) {
							sortIndexSearchState = SortIndexHasUnorderedConditions;
							return;
						} else {
							sortIndexSearchState = SortIndexFound;
						}
					}
				}

				if (!calculateEntry || CostCalculator.IsExhausted() || sortIndexSearchState == SortIndexHasUnorderedConditions) {
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
					CostCalculator.Add(results, qe.Distinct());
					if (isDistinctOnSortIndex && distinctOnSortIndex == DistinctOnSortIndex::WideRange && hasCompletePackedKeys(results)) {
						distinctOnSortIndex = DistinctOnSortIndex::Ordered;
					}
				} catch (const Error&) {
					CostCalculator.MarkInapposite();
				}
			});
	}
	CostCalculator.EndSequence();

	if (sortIndexSearchState == SortIndexHasUnorderedConditions) {
		return CostCalcResults{.expectedMaxIters = 0, .bestDistinctUniques = std::nullopt, .distinctOnSortIndex = distinctOnSortIndex};
	}
	auto res = CostCalculator.GetResults();
	res.distinctOnSortIndex = distinctOnSortIndex;
	return res;
}

size_t calculateOptimizedCost(size_t costNormal, const QueryEntries& qentries, const SelectCtx& ctx, const NamespaceData& nsData,
							  const RdxContext& rdxCtx) {
	// 'costOptimized == costNormal + 1' reduces the bounded probe performed by res.EstimateMaxIterations()
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
			[&CostCalculator] RX_PRE_LMBD_ALWAYS_INLINE(
				const concepts::OneOf<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, KnnQueryEntry, QueryFunctionEntry,
									  QueryArithmeticEntry> auto&) RX_POST_LMBD_ALWAYS_INLINE noexcept { CostCalculator.MarkInapposite(); },
			[&](const QueryEntry& qe) {
				if (!qe.IsFieldIndexed() || qe.IndexNo() != ctx.sortingContext.uncommitedIndex) {
					CostCalculator.MarkInapposite();
					return;
				}

				auto& index = *nsData.indexes[qe.IndexNo()];

				if (isIsolatedEntry(qentries, i, next)) {
					Index::SelectContext indexSelectContext;
					indexSelectContext.opts.itemsCountInNamespace = nsData.itemsCount;
					indexSelectContext.opts.disableIdSetCache = 1;
					indexSelectContext.opts.unbuiltSortOrders = 1;
					indexSelectContext.opts.indexesNotOptimized = !ctx.sortingContext.enableSortOrders;
					indexSelectContext.opts.inTransaction = ctx.inTransaction;

					try {
						auto results = index.SelectKey(qe.Values(), qe.Condition(), 0, indexSelectContext, rdxCtx);
						CostCalculator.Add(results);
					} catch (std::exception&) {
						CostCalculator.MarkInapposite();
					}
				} else {
					// Non-isolated sorting filters will create scan select results in
					// SelectIteratorContainer::prepareIteratorsForSelectLoop
					CostCalculator.MarkInapposite();
				}
			});
	}
	CostCalculator.EndSequence();
	return CostCalculator.GetResults().expectedMaxIters;
}

size_t nLogN(size_t n) noexcept { return size_t(double(n) * log2(n)); }

// Distinct on the uncommitted sort index is handled by SkipKey in the unbuilt path, so shrinking
// costNormal via bestDistinctUniques for Distinct on another indexes (except the sort one).
// Exception: ReqTotal / force-all — SkipKey cannot help, and Distinct SelectKey competes for real.
size_t adjustNormalCostForDistinct(size_t costNoDistincts, const CostCalcResults& normal, bool needCalcTotal, bool isForceAll) noexcept {
	const bool skipDistinctCostShrink = normal.distinctOnSortIndex != DistinctOnSortIndex::None && !needCalcTotal && !isForceAll;
	if (!skipDistinctCostShrink && normal.bestDistinctUniques) {
		return std::min(nLogN(*normal.bestDistinctUniques), costNoDistincts);
	}
	return costNoDistincts;
}

size_t candidateScore(const FoundIndexInfo& fi) noexcept { return fi.scoreByKeys ? fi.estimate.keys : fi.estimate.ids; }

size_t candidateLowerBound(const FoundIndexInfo& fi) noexcept {
	const size_t value = candidateScore(fi);
	return !fi.estimate.complete && value != std::numeric_limits<size_t>::max() ? value + 1 : value;
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
	if (expectedMaxIterationsNormal <= 150) {
		return false;  // If there is very good filtering condition (case for the issues #1489)
	}
	const size_t totalItemsCount = nsData.itemsCount;
	const bool expectingLimitedIterations = !ctx.isForceAll && !needCalcTotal && ctx.HasLimit();
	// '/ 3' is an empirical constant for post-filter sort cost relative to scan+sort.
	const auto costNormal =
		adjustNormalCostForDistinct(nLogN(expectedMaxIterationsNormal) / 3, expectedNormal, needCalcTotal, ctx.isForceAll);
	if (costNormal >= totalItemsCount) {
		// More effective to iterate over all items via btree than select and sort via the best filter index
		return true;
	}

	// Distinct==Sort + ordered + limit: force unbuilt when Distinct==sort SelectKey was not fully
	// packed — WideRange case where costOptimized probe itself is expensive.
	if (expectedNormal.distinctOnSortIndex == DistinctOnSortIndex::WideRange && expectingLimitedIterations) {
		return true;
	}

	// If query has limit, 'costOptimized' must be calculated as accurate as possible, because it will be used in further calculations.
	// If query must perform full iterations loop, than we may use 'costNormal' as upper limit for 'costOptimized'.
	const size_t maxOptimizedCost = expectingLimitedIterations ? totalItemsCount : costNormal;
	size_t costOptimized = calculateOptimizedCost(maxOptimizedCost, qentries, ctx, nsData, rdxCtx);
	if (costNormal >= costOptimized) {
		return true;  // If max iterations count with btree indexes is better than with any other condition (including sort overhead)
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

static void findOrderedIndexes(QueryEntries::const_iterator begin, QueryEntries::const_iterator end,
							   h_vector<FoundIndexInfo, 32>& foundIndexes, const NamespaceData& nsData) {
	bool hasNonCompatibleDistinct = false;
	bool hasUnorderedConds = false;
	for (auto it = begin; it != end; ++it) {
		const auto foundIdx = it->Visit(
			[](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, SubQueryFunctionEntry> auto&) -> FoundIndexInfo {
				throw_as_assert;
			},
			[&](const QueryEntry& entry) -> FoundIndexInfo {
				// Consider only isolated root entries with ordered indexes
				if (!entry.IsFieldIndexed()) {
					return {};
				}

				auto cur = it, next = it;
				++next;
				const bool isIsolated = isIsolatedEntry(cur->operation, next != end && next->operation == OpOr);
				const auto& index = *nsData.indexes[entry.IndexNo()];
				const bool isOrderedIndex = index.IsOrdered();
				if (entry.Distinct()) {
					// Compatible Distinct leader = Distinct flag on an *ordered* condition after preprocessor
					// merge (Lt/Gt/Range on the same field). Raw DistinctTag is CondAny and is NOT Compatible
					// by itself — do not treat merged ordered Distinct as a reason to abort AdviceSortingIndex.
					bool canBeCompatibleUnbuiltLeader = false;
					if (isIsolated) {
						canBeCompatibleUnbuiltLeader =
							isOrderedIndex && !index.Opts().IsArray() && index::IsOrderedCondition(entry.Condition());
					}
					if (!canBeCompatibleUnbuiltLeader) {
						hasNonCompatibleDistinct = true;
					}
				}
				if (isIsolated) {
					const auto cond = entry.Condition();
					const bool maybeGoodUnorderedCond = (cond == CondEq || cond == CondSet || cond == CondAllSet);
					if (maybeGoodUnorderedCond && IsHashOrBTree(index.Type())) {
						const auto uniqueIdxKeys = index.Size();
						const auto expectedSelectivityPercent = uniqueIdxKeys ? (entry.Values().size() * 100ull / uniqueIdxKeys) : 0;
						if (expectedSelectivityPercent < kMaxSelectivityPercentForIdset) {
							hasUnorderedConds = true;
						}
					}
					if (isOrderedIndex && !index.Opts().IsArray()) {
						if (index::IsOrderedCondition(cond)) {
							return FoundIndexInfo{&index, &entry, FoundIndexInfo::ConditionType::Compatible};
						} else if (maybeGoodUnorderedCond) {
							// Do not apply implicit sort if one of those conditions exist
							return FoundIndexInfo{&index, nullptr, FoundIndexInfo::ConditionType::Incompatible};
						}
					}
				}
				return {};
			},
			[](const concepts::OneOf<JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue, KnnQueryEntry,
									 MultiDistinctQueryEntry, QueryEntriesBracket, QueryFunctionEntry,
									 QueryArithmeticEntry> auto&) noexcept { return FoundIndexInfo(); });
		if (hasNonCompatibleDistinct && hasUnorderedConds) {
			// Selective unordered Eq/Set plus Distinct that cannot lead unbuilt:
			// prefer idset/comparator plans over inventing an implicit ORDER BY on another field.
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
				// Keep a Compatible entry for deferred scoring when available
				if (foundIdx.isFitForSortOptimization) {
					found->entry = foundIdx.entry;
				}
			}
		}
	}
}

// Fills estimate / scoreByKeys; clears isFitForSortOptimization for incomplete (wide) Distinct.
static void probeAdviceCandidate(FoundIndexInfo& fi) {
	assertrx_dbg(fi.isFitForSortOptimization);
	assertrx_dbg(fi.entry);
	assertrx_dbg(index::IsOrderedCondition(fi.entry->Condition()));
	fi.estimate = fi.index->EstimateOrderedCondition(fi.entry->Condition(), fi.entry->Values(), kAdviceOrderedConditionProbeKeyCap);
	fi.scoreByKeys = bool(fi.entry->Distinct());
	if (!fi.estimate.complete && fi.entry->Distinct()) {
		// Wide distinct is usually a bad candidate for main index: it can't use SkipKey() effectively,
		// and also will degrade when sort orders will be built.
		fi.isFitForSortOptimization = false;
	}
}

// Prefer a complete estimate when it is provably no worse than every incomplete lower bound;
// otherwise fall back to the largest Index::Size among compatible candidates.
static const Index* rankAdviceCandidates(h_vector<FoundIndexInfo, 32>& foundIndexes) {
	for (auto& fi : foundIndexes) {
		if (!fi.isFitForSortOptimization) {
			continue;
		}
		probeAdviceCandidate(fi);
	}

	const FoundIndexInfo* bestComplete = nullptr;
	const FoundIndexInfo* fallback = nullptr;
	size_t minIncompleteLowerBound = std::numeric_limits<size_t>::max();
	bool hasIncomplete = false;
	for (const auto& fi : foundIndexes) {
		if (!fi.isFitForSortOptimization) {
			continue;
		}
		const size_t score = candidateScore(fi);
		if (!fallback || fi.estimate.indexSize > fallback->estimate.indexSize ||
			(fi.estimate.indexSize == fallback->estimate.indexSize && score < candidateScore(*fallback))) {
			fallback = &fi;
		}
		if (fi.estimate.complete) {
			if (!bestComplete || score < candidateScore(*bestComplete) ||
				(score == candidateScore(*bestComplete) && fi.estimate.indexSize > bestComplete->estimate.indexSize)) {
				bestComplete = &fi;
			}
		} else {
			hasIncomplete = true;
			minIncompleteLowerBound = std::min(minIncompleteLowerBound, candidateLowerBound(fi));
		}
	}
	assertrx_dbg(fallback);
	if (bestComplete && (!hasIncomplete || candidateScore(*bestComplete) <= minIncompleteLowerBound)) {
		return bestComplete->index;
	}
	return fallback ? fallback->index : nullptr;
}

const Index* AdviceSortingIndex(const QueryEntries& qentries, const NamespaceData& nsData) {
	thread_local h_vector<FoundIndexInfo, 32> foundIndexes;
	foundIndexes.clear<false>();
	findOrderedIndexes(qentries.cbegin(), qentries.cend(), foundIndexes, nsData);

	size_t compatibleCount = 0;
	FoundIndexInfo* singleCompatible = nullptr;
	for (auto& fi : foundIndexes) {
		if (fi.isFitForSortOptimization) {
			++compatibleCount;
			singleCompatible = &fi;
		}
	}
	if (compatibleCount == 0) {
		return nullptr;
	}
	// Single Compatible: probe only for Distinct (reject wide ranges). Non-distinct
	// candidates need no estimate here — ranking already probes when count > 1.
	if (compatibleCount == 1) {
		assertrx_dbg(singleCompatible->entry);
		if (singleCompatible->entry->Distinct()) {
			probeAdviceCandidate(*singleCompatible);
			return singleCompatible->isFitForSortOptimization ? singleCompatible->index : nullptr;
		}
		return singleCompatible->index;
	}
	return rankAdviceCandidates(foundIndexes);
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