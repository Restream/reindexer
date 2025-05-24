#include "nsselecter.h"
#include <sstream>

#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/fields_filter.h"
#include "core/queryresults/joinresults.h"
#include "core/sorting/sortexpression.h"
#include "debug/crashqueryreporter.h"
#include "estl/restricted.h"
#include "forcedsorthelpers.h"
#include "itemcomparator.h"
#include "qresexplainholder.h"
#include "querypreprocessor.h"
#include "tools/assertrx.h"
#include "tools/logger.h"

using namespace std::string_view_literals;

constexpr int kMinIterationsForInnerJoinOptimization = 100;
constexpr int kCancelCheckFrequency = 1024;

namespace reindexer {

static std::string_view rankedTypeToErr(RankedTypeQuery type) {
	using namespace std::string_view_literals;
	switch (type) {
		case RankedTypeQuery::No:
			return "not ranked query"sv;
		case RankedTypeQuery::FullText:
			return "fulltext query"sv;
		case RankedTypeQuery::KnnIP:
			return "knn with inner product metric query"sv;
		case RankedTypeQuery::KnnL2:
			return "knn with L2 metric query"sv;
		case RankedTypeQuery::KnnCos:
			return "knn with cosine metric query"sv;
		case RankedTypeQuery::NotSet:
			break;
	}
	throw_as_assert;
}

template <typename JoinPreResultCtx>
void NsSelecter::operator()(LocalQueryResults& result, SelectCtxWithJoinPreSelect<JoinPreResultCtx>& ctx, const RdxContext& rdxCtx) {
	//	const std::string sql = ctx.query.GetSQL();
	//	std::cout << sql << std::endl;
	const size_t resultInitSize = result.Count();
	ctx.sortingContext.enableSortOrders = ns_->SortOrdersBuilt();
	const LogLevel logLevel = std::max(ns_->config_.logLevel, LogLevel(ctx.query.GetDebugLevel()));

	auto& explain = ctx.explain;
	explain = ExplainCalc(ctx.query.NeedExplain() || logLevel >= LogInfo);
	explain.SetSubQueriesExplains(std::move(ctx.subQueriesExplains));
	ActiveQueryScope queryScope(ctx, ns_->optimizationState_, explain, ns_->locker_.InvalidationType(), ns_->strHolder_.get());

	explain.SetPreselectTime(ctx.preResultTimeTotal);
	explain.StartTiming();

	const auto& aggregationQueryRef = ctx.isMergeQuerySubQuery() ? *ctx.parentQuery : ctx.query;

	auto containSomeAggCount = [&aggregationQueryRef](AggType type) noexcept {
		auto it = std::find_if(aggregationQueryRef.aggregations_.begin(), aggregationQueryRef.aggregations_.end(),
							   [type](const AggregateEntry& agg) { return agg.Type() == type; });
		return it != aggregationQueryRef.aggregations_.end();
	};

	bool needPutCachedTotal = false;
	const auto initTotalCount = result.totalCount;
	const bool containAggCount = containSomeAggCount(AggCount);
	const bool containAggCountCached = !containAggCount && containSomeAggCount(AggCountCached);
	bool needCalcTotal = aggregationQueryRef.CalcTotal() == ModeAccurateTotal || containAggCount;

	QueryCacheKey ckey;
	if (aggregationQueryRef.CalcTotal() == ModeCachedTotal || containAggCountCached) {
		ckey = QueryCacheKey{ctx.query, kCountCachedKeyMode, ctx.joinedSelectors};

		auto cached = ns_->queryCountCache_.Get(ckey);
		if (cached.valid && cached.val.IsInitialized()) {
			result.totalCount += cached.val.totalCount;
			if (logLevel >= LogTrace) {
				logFmt(LogInfo, "[{}] using total count value from cache: {}", ns_->name_, result.totalCount);
			}
		} else {
			needPutCachedTotal = cached.valid;
			needCalcTotal = true;
			if (logLevel >= LogTrace) {
				logFmt(LogTrace, "[{}] total count value for cache will be calculated by query", ns_->name_);
			}
		}
	}

	auto aggregators = getAggregators(aggregationQueryRef.aggregations_, aggregationQueryRef.GetStrictMode());
	QueryPreprocessor qPreproc(QueryEntries{ctx.query.Entries()}, aggregators, ns_, ctx);
	qPreproc.InitIndexedQueries();

	OnConditionInjections explainInjectedOnConditions;
	if (ctx.joinedSelectors) {
		qPreproc.InjectConditionsFromJoins(*ctx.joinedSelectors, explainInjectedOnConditions, logLevel, ctx.inTransaction,
										   ctx.sortingContext.enableSortOrders, rdxCtx);
		explain.PutOnConditionInjections(&explainInjectedOnConditions);
	}

	bool aggregationsOnly = aggregators.size() > 1 || (aggregators.size() == 1 && aggregators[0].Type() != AggDistinct);
	auto rankedTypeQuery = qPreproc.GetRankedTypeQuery();
	if (rankedTypeQuery != RankedTypeQuery::No && rdxCtx.IsShardingParallelExecution()) {
		throw Error{errLogic, "Full text or float vector query by several sharding hosts"};
	}
	if (ctx.isMergeQuery == IsMergeQuery_True && ctx.query.sortingEntries_.empty()) {
		if (ctx.rankedTypeQuery == RankedTypeQuery::NotSet) {
			ctx.rankedTypeQuery = rankedTypeQuery;
		} else {
			if (rankedTypeQuery != ctx.rankedTypeQuery) {
				throw Error{errNotValid,
							"In merge query without sorting all subqueries should contain fulltext or knn with the same metric conditions "
							"at the same time: '{}' VS '{}'",
							rankedTypeToErr(ctx.rankedTypeQuery), rankedTypeToErr(rankedTypeQuery)};
			}
		}
	}
	// Prepare data for select functions
	if (ctx.functions) {
		ftFunc_ = ctx.functions->AddNamespace(ctx.query, *ns_, ctx.nsid, rankedTypeQuery != RankedTypeQuery::No);
	}

	if (!ctx.skipIndexesLookup) {
		qPreproc.Reduce();
	}
	if (rankedTypeQuery == RankedTypeQuery::FullText) {
		qPreproc.ExcludeFtQuery(rdxCtx);
	}
	result.haveRank = rankedTypeQuery != RankedTypeQuery::No;

	explain.AddPrepareTime();

	const FieldsFilter fieldsFilter = ctx.query.SelectFilters().Empty() && ctx.selectBeforeUpdate
										  ? FieldsFilter::AllFields()
										  : FieldsFilter{ctx.query.SelectFilters(), *ns_};
	if (ctx.contextCollectingMode) {
		result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, fieldsFilter, ns_->schema_, ns_->incarnationTag_);
	}

	if (ctx.query.IsWithRank()) {
		if (rankedTypeQuery != RankedTypeQuery::No) {
			result.needOutputRank = true;
		} else {
			throw Error(errLogic, "Rank() is available only for fulltext or knn query");
		}
	}

	SelectIteratorContainer qres(ns_->payloadType_, &ctx);
	QresExplainHolder qresHolder(qres, (explain.IsEnabled() || logLevel >= LogTrace) ? QresExplainHolder::ExplainEnabled::Yes
																					 : QresExplainHolder::ExplainEnabled::No);
	LoopCtx lctx(qres, ctx, qPreproc, aggregators, explain);
	if (!ctx.query.forcedSortOrder_.empty() && !qPreproc.MoreThanOneEvaluation()) {
		ctx.isForceAll = true;
	}
	const bool isForceAll = ctx.isForceAll;
	const bool aggregationsOnlyOrig = aggregationsOnly;
	const bool hasOnlyDistinctAgg = aggregators.size() == 1 && aggregators[0].Type() == AggDistinct;
	const bool hasNoLimits = !ctx.query.HasOffset() && !ctx.query.HasLimit();
	do {
		rankedTypeQuery = qPreproc.GetRankedTypeQuery();
		const IsRanked isRanked{rankedTypeQuery != RankedTypeQuery::No};
		qres.Clear();
		lctx.start = QueryEntry::kDefaultOffset;
		lctx.count = QueryEntry::kDefaultLimit;
		ctx.isForceAll = isForceAll;

		if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
			// all further queries for this join MUST have the same enableSortOrders flag
			ctx.preSelect.Result().enableSortOrders = ctx.sortingContext.enableSortOrders;
		} else if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultExecuteCtx>) {
			// If in current join query sort orders are disabled
			// then preResult query also MUST have disabled flag
			// If assert fails, then possible query has unlock ns
			// or ns->sortOrdersFlag_ was reset under read lock!
			assertrx_throw(ctx.sortingContext.enableSortOrders || !ctx.preSelect.Result().enableSortOrders);
			ctx.sortingContext.enableSortOrders = ctx.preSelect.Result().enableSortOrders;
		}

		// Prepare sorting context
		ctx.sortingContext.forcedMode = qPreproc.ContainsForcedSortOrder();
		SortingEntries sortBy = qPreproc.GetSortingEntries(ctx);
		prepareSortingContext(sortBy, ctx, isRanked, qPreproc.AvailableSelectBySortIndex());

		if (ctx.sortingContext.isOptimizationEnabled()) {
			// Un-built btree index optimization is available for query with
			// Check, is it really possible to use it

			if (isRanked ||	 // Disabled if there are search results
				(!std::is_same_v<JoinPreResultCtx,
								 void> /*&& !ctx.preSelect.Result().btreeIndexOptimizationEnabled*/) ||	 // Disabled in join pre-result
																										 // (TMP: now disable for all right
																										 // queries), TODO: enable right
																										 // queries)
				(qPreproc.Size() && qPreproc.GetQueryEntries().GetOperation(0) == OpNot) ||				 // Not in first condition
				!isSortOptimizationEffective(qPreproc.GetQueryEntries(), ctx,
											 rdxCtx)  // Optimization is not effective (e.g. query contains more effective filters)
			) {
				ctx.sortingContext.resetOptimization();
				ctx.isForceAll = true;
			}

		} else if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
			ctx.preSelect.Result().btreeIndexOptimizationEnabled = false;
		}

		// Add preresults with common conditions of join Queries
		if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultExecuteCtx>) {
			assertrx_throw(ctx.preSelect.Mode() == JoinPreSelectMode::Execute || ctx.preSelect.Mode() == JoinPreSelectMode::ForInjection);
			std::visit(overloaded{[&](const IdSet& ids) {
									  SelectKeyResult res;
									  res.emplace_back(ids);
									  // Iterator Field Kind: Preselect IdSet -> None
									  qres.Append(OpAnd, SelectIterator(std::move(res), false, "-preresult", IteratorFieldKind::None));
								  },
								  [&](const SelectIteratorContainer& iterators) {
									  if (ctx.preSelect.Mode() == JoinPreSelectMode::Execute) {
										  qres.Append(iterators.begin(), iterators.end());
									  }
								  },
								  [](const JoinPreResult::Values&) { throw_as_assert; }},
					   ctx.preSelect.Result().payload);
		}

		qres.PrepareIteratorsForSelectLoop(qPreproc, ctx.sortingContext.sortId(), rankedTypeQuery,
										   (isRanked && (ctx.isMergeQuery == IsMergeQuery_True || !ctx.query.GetMergeQueries().empty()))
											   ? RankSortType::RankAndID
											   : RankSortType::RankOnly,
										   *ns_, ftFunc_, ranks_, rdxCtx);

		explain.AddSelectTime();

		int maxIterations = qres.GetMaxIterations();
		if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
			// Building pre-result for next joins
			auto& preResult = ctx.preSelect.Result();
			if (auto sortFieldEntry = ctx.sortingContext.sortFieldEntryIfOrdered(); sortFieldEntry) {
				preResult.sortOrder = JoinPreResult::SortOrderContext{.index = sortFieldEntry->index, .sortingEntry = sortFieldEntry->data};
			}
			preResult.properties.emplace(qres.GetMaxIterations(true), ns_->config().maxIterationsIdSetPreResult);
			auto& preselectProps = preResult.properties.value();
			assertrx_throw(preselectProps.maxIterationsIdSetPreResult > JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization());
			if ((preResult.storedValuesOptStatus == StoredValuesOptimizationStatus::Enabled) &&
				preselectProps.qresMaxIterations <= JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization()) {
				preResult.payload.template emplace<JoinPreResult::Values>(ns_->payloadType_, ns_->tagsMatcher_);
			} else {
				preselectProps.isLimitExceeded = (maxIterations >= preselectProps.maxIterationsIdSetPreResult);
				preselectProps.isUnorderedIndexSort =
					!preselectProps.isLimitExceeded && (ctx.sortingContext.entries.size() && !ctx.sortingContext.sortIndex());
				preselectProps.btreeIndexOptimizationEnabled = preResult.btreeIndexOptimizationEnabled;
				preselectProps.qresMaxIterations = maxIterations;
				// Return pre-result as QueryIterators if:
				if (preselectProps.isLimitExceeded ||		// 1. We have > QueryIterator which expects more than configured max iterations.
					preselectProps.isUnorderedIndexSort ||	// 2. We have sorted query, by unordered index
					preselectProps.btreeIndexOptimizationEnabled) {	 // 3. We have btree-index that is not committed yet
					preResult.payload.template emplace<SelectIteratorContainer>();
					std::get<SelectIteratorContainer>(preResult.payload).Append(qres.cbegin(), qres.cend());
					if rx_unlikely (logLevel >= LogInfo) {
						logFmt(LogInfo, "Built preResult (expected {} iterations) with {} iterators, q='{}'",
							   preselectProps.qresMaxIterations, qres.Size(), ctx.query.GetSQL());
					}
					return;
				} else {  // Build pre-result as single IdSet
					preResult.payload.template emplace<IdSet>();
					// For building join pre-result always use ASC sort orders
					for (SortingEntry& se : sortBy) {
						se.desc = false;
					}
				}
			}
		}  // pre-select rejected
		else if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultExecuteCtx>) {
			// Main sorting index must be the same during join preselect build and execution
			assertrx_throw(ctx.preSelect.Result().sortOrder.index == ctx.sortingContext.sortIndexIfOrdered());
			if (ctx.preSelect.Mode() == JoinPreSelectMode::ForInjection &&
				maxIterations > long(ctx.preSelect.MainQueryMaxIterations()) * ctx.preSelect.MainQueryMaxIterations()) {
				ctx.preSelect.Reject();
				return;
			}
		} else if (!ctx.sortingContext.isOptimizationEnabled()) {
			static_assert(std::is_same_v<JoinPreResultCtx, void>);
			if (!isRanked && maxIterations > kMinIterationsForInnerJoinOptimization) {
				for (size_t i = 0, size = qres.Size(); i < size; i = qres.Next(i)) {
					// for optimization use only isolated InnerJoin
					if (qres.GetOperation(i) == OpAnd && qres.IsJoinIterator(i) &&
						(qres.Next(i) >= size || qres.GetOperation(qres.Next(i)) != OpOr)) {
						const JoinSelectIterator& jIter = qres.Get<JoinSelectIterator>(i);
						assertrx_throw(ctx.joinedSelectors && ctx.joinedSelectors->size() > jIter.joinIndex);
						JoinedSelector& js = (*ctx.joinedSelectors)[jIter.joinIndex];
						js.AppendSelectIteratorOfJoinIndexData(qres, &maxIterations, ctx.sortingContext.sortId(), ftFunc_, rdxCtx);
					}
				}
			}
		}

		bool reverse = !isRanked && ctx.sortingContext.sortIndex() &&
					   std::visit([](const auto& e) noexcept { return e.data.desc; }, ctx.sortingContext.entries[0]);

		bool hasComparators = false;
		qres.VisitForEach(Skip<JoinSelectIterator, SelectIteratorsBracket, AlwaysTrue, SelectIterator>{},
						  [&hasComparators](OneOf<FieldsComparator, EqualPositionComparator, ComparatorNotIndexed, ComparatorDistinctMulti,
												  Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point,
														   Uuid, FloatVector>>) noexcept { hasComparators = true; });

		if (!qres.HasIdsets()) {
			SelectKeyResult scan;
			std::string_view scanName = "-scan";
			if (ctx.sortingContext.isOptimizationEnabled()) {
				auto it = ns_->indexes_[ctx.sortingContext.uncommitedIndex]->CreateIterator();
				maxIterations = ns_->itemsCount();
				it->SetMaxIterations(maxIterations);
				scan.emplace_back(std::move(it));
			} else {
				// special case - no idset in query
				const auto itemsInUse = ns_->items_.size() - ns_->free_.size();
				const bool haveLotOfFree = (ns_->free_.size() > 200'000) && (ns_->free_.size() > (4 * itemsInUse));
				const bool useSortOrders = ctx.sortingContext.isIndexOrdered() && ctx.sortingContext.enableSortOrders;
				if (haveLotOfFree && !useSortOrders) {
					// Attempt to improve selection time by using dense IdSet, when there are a lot of empty items in namespace
					scanName = "-scan-dns";
					base_idset denseSet;
					denseSet.reserve(itemsInUse);
					for (IdType i = 0, sz = IdType(ns_->items_.size()); i < sz; ++i) {
						if (!ns_->items_[i].IsFree()) {
							denseSet.emplace_back(i);
						}
					}
					scan.emplace_back(make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>(std::move(denseSet)));
					maxIterations = itemsInUse;
				} else {
					// Use ids range
					IdType limit = ns_->items_.size();
					if (useSortOrders) {
						const Index* index = ctx.sortingContext.sortIndex();
						assertrx_throw(index);
						limit = index->SortOrders().size();
					}
					scan.emplace_back(0, limit);
					maxIterations = limit;
				}
			}
			// Iterator Field Kind: -scan. Sorting Context! -> None
			qres.AppendFront<SelectIterator>(OpAnd, std::move(scan), false, std::string(scanName), IteratorFieldKind::None,
											 ForcedFirst_True);
		}
		// Get maximum iterations count, for right calculation comparators costs
		qres.SortByCost(maxIterations);

		// Check IdSet must be 1st
		qres.CheckFirstQuery();

		// Rewind all results iterators
		qres.VisitForEach(
			Skip<JoinSelectIterator, SelectIteratorsBracket, FieldsComparator, AlwaysTrue, EqualPositionComparator>{},
			Restricted<ComparatorNotIndexed, ComparatorDistinctMulti,
					   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>{}(
				[](auto& comp) { comp.ClearDistinctValues(); }),
			[reverse, maxIterations](SelectIterator& it) { it.Start(reverse, maxIterations); });

		// Let iterators choose most efficient algorithm
		assertrx_throw(qres.Size());
		qres.SetExpectMaxIterations(maxIterations);

		explain.AddPostprocessTime();

		// do not calc total by loop, if we have only 1 condition with 1 IdSet
		lctx.calcTotal = needCalcTotal &&
						 (hasComparators || qPreproc.MoreThanOneEvaluation() || qres.Size() > 1 || qres.Get<SelectIterator>(0).size() > 1);

		// Aggregations can be calculated correctly in a single pass over the items array
		// only if there is no limit and offset in the query,
		// or items are already ordered according to the sorting from the query.
		// At the same time, for a single Distinct-aggregation we must select
		// items with unique values, and recalculate the values of Distinct-aggregation
		// on a correctly formed set of items after applying a select loop.
		lctx.calcAggsImmediately =
			hasOnlyDistinctAgg || hasNoLimits || ctx.sortingContext.isOptimizationEnabled() || ctx.sortingContext.sortId();
		aggregationsOnly = aggregationsOnlyOrig && lctx.calcAggsImmediately;

		if (qPreproc.IsFtExcluded()) {
			if (reverse && hasComparators) {
				selectLoop<true, true, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
			} else if (!reverse && hasComparators) {
				selectLoop<false, true, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
			} else if (reverse && !hasComparators) {
				selectLoop<true, false, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
			} else {
				selectLoop<false, false, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
			}
		} else {
			if (reverse && hasComparators && aggregationsOnly) {
				selectLoop<true, true, true>(lctx, result, rdxCtx);
			} else if (!reverse && hasComparators && aggregationsOnly) {
				selectLoop<false, true, true>(lctx, result, rdxCtx);
			} else if (reverse && !hasComparators && aggregationsOnly) {
				selectLoop<true, false, true>(lctx, result, rdxCtx);
			} else if (!reverse && !hasComparators && aggregationsOnly) {
				selectLoop<false, false, true>(lctx, result, rdxCtx);
			} else if (reverse && hasComparators && !aggregationsOnly) {
				selectLoop<true, true, false>(lctx, result, rdxCtx);
			} else if (!reverse && hasComparators && !aggregationsOnly) {
				selectLoop<false, true, false>(lctx, result, rdxCtx);
			} else if (reverse && !hasComparators && !aggregationsOnly) {
				selectLoop<true, false, false>(lctx, result, rdxCtx);
			} else if (!reverse && !hasComparators && !aggregationsOnly) {
				selectLoop<false, false, false>(lctx, result, rdxCtx);
			}

			// Get total count for simple query with 1 condition and 1 IdSet
			if (needCalcTotal && !lctx.calcTotal) {
				if (!ctx.query.Entries().Empty()) {
					result.totalCount += qres.Get<SelectIterator>(0).GetMaxIterations();
				} else {
					result.totalCount += ns_->itemsCount();
				}
			}
		}

		explain.AddLoopTime();
		explain.AddIterations(maxIterations);
		if (!ctx.inTransaction) {
			ThrowOnCancel(rdxCtx);
		}
	} while (qPreproc.NeedNextEvaluation(lctx.start, lctx.count, ctx.matchedAtLeastOnce, qresHolder, needCalcTotal));

	const bool needRecalcDistincts = hasOnlyDistinctAgg && !hasNoLimits;
	if ((aggregationsOnlyOrig && !lctx.calcAggsImmediately) || needRecalcDistincts) {
		if (needRecalcDistincts) {
			aggregators.front().ResetDistinctSet();
		}
		for (auto it = result.begin() + initTotalCount; it != result.end(); ++it) {
			auto& pl = ns_->items_[it.GetItemRef().Id()];
			for (auto& aggregator : aggregators) {
				aggregator.Aggregate(pl);
			}
		}
		if (!needRecalcDistincts) {
			result.Erase(result.Items().begin() + initTotalCount, result.Items().end());
		}
	}

	processLeftJoins(result, ctx, resultInitSize, rdxCtx);
	if (!ctx.sortingContext.expressions.empty()) {
		if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
			std::visit(overloaded{[&](JoinPreResult::Values& values) {
									  for (const auto& it : values) {
										  ItemRef& iref = it.GetItemRef();
										  if (!iref.ValueInitialized()) {
											  iref.SetValue(ns_->items_[iref.Id()]);
										  }
									  }
								  },
								  Restricted<IdSet, SelectIteratorContainer>{}([](const auto&) {})},
					   ctx.preSelect.Result().payload);
		} else {
			for (size_t i = resultInitSize; i < result.Count(); ++i) {
				auto& iref = result.Items().GetItemRef(i);
				if (!iref.ValueInitialized()) {
					iref.SetValue(ns_->items_[iref.Id()]);
				}
			}
		}
	}
	holdFloatVectors(result, ctx, resultInitSize, fieldsFilter);
	if (rx_unlikely(ctx.isMergeQuerySubQuery())) {
		writeAggregationResultMergeSubQuery(result, std::move(aggregators), ctx);
	} else {
		for (auto& aggregator : aggregators) {
			result.aggregationResults.emplace_back(std::move(aggregator).MoveResult());
		}
	}
	//	Put count/count_cached to aggregations
	if (aggregationQueryRef.HasCalcTotal() || containAggCount || containAggCountCached) {
		AggType t = (aggregationQueryRef.CalcTotal() == ModeAccurateTotal || containAggCount) ? AggCount : AggCountCached;
		if (ctx.isMergeQuerySubQuery()) {
			assertrx_dbg(!result.aggregationResults.empty());
			auto& agg = result.aggregationResults.back();
			assertrx_dbg(agg.GetType() == t);
			agg.UpdateValue(result.totalCount);
		} else {
			AggregationResult ret{t, {"*"}, double(result.totalCount)};
			result.aggregationResults.emplace_back(std::move(ret));
		}
	}

	explain.AddPostprocessTime();
	explain.StopTiming();
	explain.SetSortOptimization(ctx.sortingContext.isOptimizationEnabled());
	explain.PutSortIndex(ctx.sortingContext.sortIndex() ? ctx.sortingContext.sortIndex()->Name() : "-"sv);
	if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
		explain.PutCount(std::visit(overloaded{[](const IdSet& ids) noexcept -> size_t { return ids.size(); },
											   [](const JoinPreResult::Values& values) noexcept { return values.Size(); },
											   [](const SelectIteratorContainer&) -> size_t { throw_as_assert; }},
									ctx.preSelect.Result().payload));
	} else {
		explain.PutCount(result.Count());
	}
	explain.PutSelectors(&qresHolder.GetResultsRef());
	explain.PutJoinedSelectors(ctx.joinedSelectors);

	if rx_unlikely (logLevel >= LogInfo) {
		logFmt(LogInfo, "{}", ctx.query.GetSQL());
		explain.LogDump(logLevel);
	}
	if (ctx.query.NeedExplain()) {
		if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
			ctx.preSelect.Result().explainPreSelect = explain.GetJSON();
		} else {
			result.explainResults = explain.GetJSON();
		}
	}
	if rx_unlikely (logLevel >= LogTrace) {
		logFmt(LogInfo, "Query returned: [{}]; total={}", result.Dump(), result.totalCount);
	}

	if (needPutCachedTotal) {
		if rx_unlikely (logLevel >= LogTrace) {
			logFmt(LogInfo, "[{}] put totalCount value into query cache: {} ", ns_->name_, result.totalCount);
		}
		ns_->queryCountCache_.Put(ckey, {static_cast<size_t>(result.totalCount - initTotalCount)});
	}
	if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
		if rx_unlikely (logLevel >= LogTrace) {
			std::visit(overloaded{[&](const IdSet& ids) {
									  logFmt(LogInfo, "Built idset preResult (expected {} iterations) with {} ids, q = '{}'",
											 explain.Iterations(), ids.size(), ctx.query.GetSQL());
								  },
								  [&](const JoinPreResult::Values& values) {
									  logFmt(LogInfo, "Built values preResult (expected {} iterations) with {} values, q = '{}'",
											 explain.Iterations(), values.Size(), ctx.query.GetSQL());
								  },
								  [](const SelectIteratorContainer&) { throw_as_assert; }},
					   ctx.preSelect.Result().payload);
		}
	}
}

template <>
void NsSelecter::holdFloatVectors(LocalQueryResults&, SelectCtxWithJoinPreSelect<JoinPreResultBuildCtx>& ctx, size_t,
								  const FieldsFilter& filter) const {
	if (ctx.floatVectorsHolder) {
		std::visit(
			overloaded{[&](JoinPreResult::Values& values) { ctx.floatVectorsHolder->Add(*ns_, values.begin(), values.end(), filter); },
					   Restricted<IdSet, SelectIteratorContainer>{}([](const auto&) noexcept {})},
			ctx.preSelect.Result().payload);
	}
}

template <>
void NsSelecter::holdFloatVectors(LocalQueryResults& result, SelectCtxWithJoinPreSelect<void>& ctx, size_t offset,
								  const FieldsFilter& filter) const {
	if (ctx.floatVectorsHolder) {
		ctx.floatVectorsHolder->Add(*ns_, result.begin() + offset, result.end(), filter);
	}
}

template <>
void NsSelecter::holdFloatVectors(LocalQueryResults&, SelectCtxWithJoinPreSelect<JoinPreResultExecuteCtx>&, size_t,
								  const FieldsFilter&) const {}

template <>
class NsSelecter::MainNsValueGetter<ItemRefVector::Iterator::RankedIt> {
public:
	explicit MainNsValueGetter(const NamespaceImpl& ns) noexcept : ns_{ns} {}
	const PayloadValue& Value(const ItemRef& itemRef) const noexcept { return ns_.items_[itemRef.Id()]; }
	ConstPayload Payload(const ItemRef& itemRef) const { return ConstPayload{ns_.payloadType_, Value(itemRef)}; }

private:
	const NamespaceImpl& ns_;
};

template <>
class NsSelecter::MainNsValueGetter<ItemRefVector::Iterator::NotRankedIt> {
public:
	explicit MainNsValueGetter(const NamespaceImpl& ns) noexcept : ns_{ns} {}
	const PayloadValue& Value(const ItemRef& itemRef) const noexcept { return ns_.items_[itemRef.Id()]; }
	ConstPayload Payload(const ItemRef& itemRef) const { return ConstPayload{ns_.payloadType_, Value(itemRef)}; }

private:
	const NamespaceImpl& ns_;
};

template <>
class NsSelecter::MainNsValueGetter<JoinPreResult::Values::Iterator> {
public:
	explicit MainNsValueGetter(const NamespaceImpl& ns) noexcept : ns_{ns} {}
	const PayloadValue& Value(const ItemRef& itemRef) const noexcept { return itemRef.Value(); }
	ConstPayload Payload(const ItemRef& itemRef) const { return ConstPayload{ns_.payloadType_, Value(itemRef)}; }

private:
	const NamespaceImpl& ns_;
};

class NsSelecter::JoinedNsValueGetter {
public:
	JoinedNsValueGetter(const NamespaceImpl& ns, const joins::NamespaceResults& jr, size_t nsIdx) noexcept
		: ns_{ns}, joinedResults_{jr}, nsIdx_{nsIdx} {}
	const PayloadValue& Value(const ItemRef& itemRef) const {
		const joins::ItemIterator it{&joinedResults_, itemRef.Id()};
		const auto jfIt = it.at(nsIdx_);
		if (jfIt == it.end() || jfIt.ItemsCount() == 0) {
			throw Error(errQueryExec, "Not found value joined from ns {}", ns_.name_);
		}
		if (jfIt.ItemsCount() > 1) {
			throw Error(errQueryExec, "Found more than 1 value joined from ns {}", ns_.name_);
		}
		return jfIt[0].Value();
	}
	ConstPayload Payload(const ItemRef& itemRef) const { return ConstPayload{ns_.payloadType_, Value(itemRef)}; }

private:
	const NamespaceImpl& ns_;
	const joins::NamespaceResults& joinedResults_;
	const size_t nsIdx_;
};

template <bool desc, bool multiColumnSort, typename It>
It NsSelecter::applyForcedSort(It begin, It end, const ItemComparator& compare, const SelectCtx& ctx, const joins::NamespaceResults* jr) {
	assertrx_throw(!ctx.sortingContext.entries.empty());
	if (ctx.query.GetMergeQueries().size() > 1) {
		throw Error(errLogic, "Force sort could not be applied to 'merged' queries.");
	}
	return std::visit(
		overloaded{
			[](const SortingContext::ExpressionEntry&) -> It { throw Error(errLogic, "Force sort could not be performed by expression."); },
			[&](const SortingContext::FieldEntry& e) {
				return applyForcedSortImpl<desc, multiColumnSort, It>(*ns_, begin, end, compare, ctx.query.forcedSortOrder_,
																	  e.data.expression, MainNsValueGetter<It>{*ns_});
			},
			[&](const SortingContext::JoinedFieldEntry& e) {
				assertrx_throw(ctx.joinedSelectors);
				assertrx_throw(ctx.joinedSelectors->size() >= e.nsIdx);
				assertrx_throw(jr);
				const auto& joinedSelector = (*ctx.joinedSelectors)[e.nsIdx];
				return applyForcedSortImpl<desc, multiColumnSort, It>(*joinedSelector.RightNs(), begin, end, compare,
																	  ctx.query.forcedSortOrder_, std::string{e.field},
																	  JoinedNsValueGetter{*joinedSelector.RightNs(), *jr, e.nsIdx});
			},

		},
		ctx.sortingContext.entries[0]);
}

template <bool desc, bool multiColumnSort, typename It, typename ValueGetter>
It NsSelecter::applyForcedSortImpl(NamespaceImpl& ns, It begin, It end, const ItemComparator& compare,
								   const std::vector<Variant>& forcedSortOrder, const std::string& fieldName,
								   const ValueGetter& valueGetter) {
	if (int idx; ns.getIndexByNameOrJsonPath(fieldName, idx)) {
		if (ns.indexes_[idx]->Opts().IsArray()) {
			throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");
		}
		const KeyValueType fieldType{ns.indexes_[idx]->KeyType()};
		if (idx < ns.indexes_.firstSparsePos()) {
			// implementation for regular indexes
			fast_hash_map<Variant, std::ptrdiff_t> sortMap;
			force_sort_helpers::ForcedMapInserter inserter{sortMap};
			for (const auto& value : forcedSortOrder) {
				inserter.Insert(value.convert(fieldType));
			}
			// clang-tidy reports std::get_temporary_buffer as deprecated
			// NOLINTNEXTLINE (clang-diagnostic-deprecated-declarations)
			const auto boundary = std::stable_partition(
				begin, end, force_sort_helpers::ForcedPartitionerIndexed<desc, ValueGetter>{idx, valueGetter, sortMap});
			const It from = desc ? boundary : begin;
			const It to = desc ? end : boundary;
			std::sort(from, to,
					  force_sort_helpers::ForcedComparatorIndexed<desc, multiColumnSort, ValueGetter>(idx, valueGetter, compare, sortMap));
			return boundary;
		} else if (idx < ns.indexes_.firstCompositePos()) {
			// implementation for sparse indexes
			fast_hash_map<Variant, std::ptrdiff_t> sortMap;
			force_sort_helpers::ForcedMapInserter inserter{sortMap};
			for (const auto& value : forcedSortOrder) {
				inserter.Insert(value.convert(fieldType));
			}
			const auto& idxRef = *ns.indexes_[idx];
			const auto& tagsPath = idxRef.Fields().getTagsPath(0);
			const auto kvt = idxRef.KeyType();

			// clang-tidy reports std::get_temporary_buffer as deprecated
			// NOLINTNEXTLINE (clang-diagnostic-deprecated-declarations)
			const auto boundary = std::stable_partition(begin, end,
														force_sort_helpers::ForcedPartitionerIndexedJsonPath<desc, ValueGetter>{
															tagsPath, kvt, idxRef.Name(), valueGetter, sortMap});
			const It from = desc ? boundary : begin;
			const It to = desc ? end : boundary;
			std::sort(from, to,
					  force_sort_helpers::ForcedComparatorIndexedJsonPath<desc, multiColumnSort, ValueGetter>(tagsPath, kvt, valueGetter,
																											  compare, sortMap));
			return boundary;
		} else {
			// implementation for composite indexes
			const auto& payloadType = ns.payloadType_;
			const FieldsSet& fields = ns.indexes_[idx]->Fields();
			unordered_payload_map<std::ptrdiff_t, false> sortMap(0, PayloadType{payloadType}, FieldsSet{fields});
			force_sort_helpers::ForcedMapInserter inserter{sortMap};
			for (auto value : forcedSortOrder) {
				value.convert(fieldType, &payloadType, &fields);
				inserter.Insert(static_cast<const PayloadValue&>(value));
			}
			// clang-tidy reports std::get_temporary_buffer as deprecated
			// NOLINTNEXTLINE (clang-diagnostic-deprecated-declarations)
			const auto boundary =
				std::stable_partition(begin, end, force_sort_helpers::ForcedPartitionerComposite<desc, ValueGetter>{valueGetter, sortMap});
			const It from = desc ? boundary : begin;
			const It to = desc ? end : boundary;
			std::sort(from, to,
					  force_sort_helpers::ForcedComparatorComposite<desc, multiColumnSort, ValueGetter>{valueGetter, compare, sortMap});
			return boundary;
		}
	} else {
		force_sort_helpers::ForcedSortMap sortMap{forcedSortOrder[0], 0, forcedSortOrder.size()};
		force_sort_helpers::ForcedMapInserter inserter{sortMap};
		for (size_t i = 1, s = forcedSortOrder.size(); i < s; ++i) {
			inserter.Insert(forcedSortOrder[i]);
		}
		const auto tagsPath = ns.tagsMatcher_.path2tag(fieldName);
		// clang-tidy reports std::get_temporary_buffer as deprecated
		// NOLINTNEXTLINE (clang-diagnostic-deprecated-declarations)
		const auto boundary = std::stable_partition(
			begin, end, force_sort_helpers::ForcedPartitionerNotIndexed<desc, ValueGetter>{tagsPath, valueGetter, sortMap});
		const It from = desc ? boundary : begin;
		const It to = desc ? end : boundary;
		std::sort(
			from, to,
			force_sort_helpers::ForcedComparatorNotIndexed<desc, multiColumnSort, ValueGetter>{tagsPath, valueGetter, compare, sortMap});
		return boundary;
	}
}

class GeneralComparator {
public:
	explicit GeneralComparator(const ItemComparator& compare) noexcept : compare_{compare} {}

	bool operator()(const ItemRef& lhs, const ItemRef& rhs) { return compare_(lhs, rhs); }
	bool operator()(const ItemRefRanked& lhs, const ItemRefRanked& rhs) { return compare_(lhs.NotRanked(), rhs.NotRanked()); }

private:
	const ItemComparator& compare_;
};

template <typename It>
void NsSelecter::applyGeneralSort(It itFirst, It itLast, It itEnd, const ItemComparator& comparator, const SelectCtx& ctx) {
	if (ctx.query.GetMergeQueries().size() > 1) {
		throw Error(errLogic, "Sorting cannot be applied to merged queries.");
	}

	std::partial_sort(itFirst, itLast, itEnd, GeneralComparator{comparator});
}

void NsSelecter::setLimitAndOffset(ItemRefVector& queryResult, size_t offset, size_t limit) {
	const unsigned totalRows = queryResult.Size();
	if (offset > 0) {
		auto end = offset < totalRows ? queryResult.begin() + offset : queryResult.end();
		queryResult.Erase(queryResult.begin(), end);
	}
	if (queryResult.Size() > limit) {
		queryResult.Erase(queryResult.begin() + limit, queryResult.end());
	}
}

void NsSelecter::processLeftJoins(LocalQueryResults& qr, SelectCtx& sctx, size_t startPos, const RdxContext& rdxCtx) {
	if (!checkIfThereAreLeftJoins(sctx)) {
		return;
	}
	for (size_t i = startPos; i < qr.Count(); ++i) {
		IdType rowid = qr[i].GetItemRef().Id();
		ConstPayload pl(ns_->payloadType_, ns_->items_[rowid]);
		for (auto& joinedSelector : *sctx.joinedSelectors) {
			if (joinedSelector.Type() == JoinType::LeftJoin) {
				joinedSelector.Process(rowid, sctx.nsid, pl, sctx.floatVectorsHolder, true);
			}
		}
		if (!sctx.inTransaction && (i % kCancelCheckFrequency == 0)) {
			ThrowOnCancel(rdxCtx);
		}
	}
}

bool NsSelecter::checkIfThereAreLeftJoins(SelectCtx& sctx) const {
	if (!sctx.joinedSelectors) {
		return false;
	}
	return std::any_of(sctx.joinedSelectors->begin(), sctx.joinedSelectors->end(),
					   [](const auto& selector) { return selector.Type() == JoinType::LeftJoin; });
}

template <typename It, typename JoinPreResultCtx>
void NsSelecter::sortResults(LoopCtx<JoinPreResultCtx>& ctx, It begin, It end, const SortingOptions& sortingOptions,
							 const joins::NamespaceResults* jr) {
	SelectCtx& sctx = ctx.sctx;
	ctx.explain.StartSort();
#ifdef RX_WITH_STDLIB_DEBUG
	for (const auto& eR : sctx.sortingContext.exprResults) {
		assertrx_dbg(eR.size() == end - begin);
	}
#endif	// RX_WITH_STDLIB_DEBUG

	ItemComparator comparator{*ns_, sctx, jr};
	if (sortingOptions.forcedMode) {
		comparator.BindForForcedSort();
		assertrx_throw(!sctx.query.sortingEntries_.empty());
		if (sctx.query.sortingEntries_[0].desc) {
			if (sctx.sortingContext.entries.size() > 1) {
				end = applyForcedSort<true, true>(begin, end, comparator, sctx, jr);
			} else {
				end = applyForcedSort<true, false>(begin, end, comparator, sctx, jr);
			}
		} else {
			if (sctx.sortingContext.entries.size() > 1) {
				begin = applyForcedSort<false, true>(begin, end, comparator, sctx, jr);
			} else {
				begin = applyForcedSort<false, false>(begin, end, comparator, sctx, jr);
			}
		}
	}
	if (sortingOptions.multiColumn || sortingOptions.usingGeneralAlgorithm) {
		comparator.BindForGeneralSort();
		size_t endPos = end - begin;
		if (sortingOptions.usingGeneralAlgorithm) {
			endPos = std::min(static_cast<size_t>(ctx.qPreproc.Count()) + ctx.qPreproc.Start(), endPos);
		}
		auto last = begin + endPos;
		applyGeneralSort(begin, last, end, comparator, sctx);
	}
	ctx.explain.StopSort();
}

static size_t resultSize(const LocalQueryResults& qr) noexcept { return qr.Count(); }

static void resultReserve(LocalQueryResults& qr, size_t s, IsRanked isRanked) { qr.Items().Reserve(s, isRanked); }
static void resultReserve(FtMergeStatuses&, size_t, IsRanked) {}

template <bool reverse, bool hasComparators, bool aggregationsOnly, typename ResultsT, typename JoinPreResultCtx>
void NsSelecter::selectLoop(LoopCtx<JoinPreResultCtx>& ctx, ResultsT& result, const RdxContext& rdxCtx) {
	static constexpr bool kPreprocessingBeforeFT = !std::is_same_v<ResultsT, LocalQueryResults>;
	static const JoinedSelectors emptyJoinedSelectors;
	const auto selectLoopWard = rdxCtx.BeforeSelectLoop();
	SelectCtxWithJoinPreSelect<JoinPreResultCtx>& sctx = ctx.sctx;
	const auto& joinedSelectors = sctx.joinedSelectors ? *sctx.joinedSelectors : emptyJoinedSelectors;
	SelectIteratorContainer& qres = ctx.qres;
	// Is not using during ft preprocessing
	size_t initCount = 0;
	if constexpr (!kPreprocessingBeforeFT) {
		if constexpr (!std::is_same_v<JoinPreResultCtx, void>) {
			if (auto* values = std::get_if<JoinPreResult::Values>(&sctx.preSelect.Result().payload); values) {
				initCount = values->Size();
			} else {
				initCount = resultSize(result);
			}
		} else {
			initCount = resultSize(result);
		}
	}
	if (!sctx.isForceAll) {
		ctx.start = ctx.qPreproc.Start();
		ctx.count = ctx.qPreproc.Count();
	}

	// reserve query results, if we have only 1 condition with 1 idset
	if (qres.Size() == 1 && qres.IsSelectIterator(0) && qres.Get<SelectIterator>(0).size() == 1) {
		if (const size_t reserve = qres.Get<SelectIterator>(0).GetMaxIterations(ctx.count); reserve < size_t(QueryEntry::kDefaultLimit)) {
			if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
				if (auto* values = std::get_if<JoinPreResult::Values>(&sctx.preSelect.Result().payload); values) {
					values->Reserve(reserve + initCount);
				} else {
					resultReserve(result, initCount + reserve, IsRanked(ranks_));
				}
			} else {
				resultReserve(result, initCount + reserve, IsRanked(ranks_));
			}
		}
	}

	bool finish = (ctx.count == 0) && !sctx.reqMatchedOnceFlag && !ctx.calcTotal && !sctx.matchedAtLeastOnce;

	SortingOptions sortingOptions(sctx.sortingContext);
	const Index* const firstSortIndex = sctx.sortingContext.sortIndexIfOrdered();
	bool multiSortFinished = !(sortingOptions.multiColumnByBtreeIndex && ctx.count > 0);

	VariantArray prevValues;
	size_t multisortLimitLeft = 0;

	assertrx_throw(!qres.Empty());
	assertrx_throw(qres.IsSelectIterator(0));
	SelectIterator& firstIterator = qres.begin()->Value<SelectIterator>();
	IdType rowId = firstIterator.Val();
	while (firstIterator.Next(rowId) && !finish) {
		if ((rowId % kCancelCheckFrequency == 0) && !sctx.inTransaction) {
			ThrowOnCancel(rdxCtx);
		}
		rowId = firstIterator.Val();
		IdType properRowId = rowId;

		if (firstSortIndex) {
			if rx_unlikely (firstSortIndex->SortOrders().size() <= static_cast<size_t>(rowId)) {
				throwIncorrectRowIdInSortOrders(rowId, *firstSortIndex, firstIterator);
			}
			properRowId = firstSortIndex->SortOrders()[rowId];
		}

		assertrx_throw(static_cast<size_t>(properRowId) < ns_->items_.size());
		PayloadValue& pv = ns_->items_[properRowId];
		if (pv.IsFree()) {
			continue;
		}
		if (qres.Process<reverse, hasComparators>(pv, &finish, &rowId, properRowId, !ctx.start && ctx.count)) {
			sctx.matchedAtLeastOnce = true;
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			qres.VisitForEach(
				Skip<SelectIteratorsBracket, JoinSelectIterator, FieldsComparator, AlwaysFalse, AlwaysTrue, EqualPositionComparator>{},
				[rowId](SelectIterator& sit) {
					if (sit.distinct) {
						sit.ExcludeLastSet(rowId);
					}
				},
				Restricted<ComparatorNotIndexed, ComparatorDistinctMulti,
						   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>{}(
					[&pv, properRowId](auto& comp) { comp.ExcludeDistinctValues(pv, properRowId); }));
			if constexpr (!kPreprocessingBeforeFT) {
				const RankT rank = ranks_ ? ranks_->Get(firstIterator.Pos()) : RankT{};
				if ((ctx.start || (ctx.count == 0)) && sortingOptions.multiColumnByBtreeIndex) {
					VariantArray recentValues;
					size_t lastResSize = result.Count();
					getSortIndexValue(sctx.sortingContext, properRowId, recentValues, rank,
									  sctx.nsid < result.joined_.size() ? &result.joined_[sctx.nsid] : nullptr, joinedSelectors,
									  rdxCtx.ShardId());
					if (prevValues.empty() && result.Items().Empty()) {
						prevValues = recentValues;
					} else {
						if (recentValues != prevValues) {
							if (ctx.start) {
								result.Items().Clear();
								multisortLimitLeft = 0;
								lastResSize = 0;
								prevValues = recentValues;
							} else if (!ctx.count) {
								multiSortFinished = true;
							}
						}
					}
					if (!multiSortFinished) {
						addSelectResult<aggregationsOnly>(rank, rowId, properRowId, sctx, ctx.aggregators, result, ctx.calcAggsImmediately,
														  ctx.preselectForFt);
					}
					if (lastResSize < result.Count()) {
						if (ctx.start) {
							++multisortLimitLeft;
						}
					}
				}
				if (ctx.start) {
					--ctx.start;
				} else if (ctx.count) {
					addSelectResult<aggregationsOnly>(rank, rowId, properRowId, sctx, ctx.aggregators, result, ctx.calcAggsImmediately,
													  ctx.preselectForFt);
					--ctx.count;
					if (!ctx.count && sortingOptions.multiColumn && !multiSortFinished) {
						getSortIndexValue(sctx.sortingContext, properRowId, prevValues, rank,
										  sctx.nsid < result.joined_.size() ? &result.joined_[sctx.nsid] : nullptr, joinedSelectors,
										  rdxCtx.ShardId());
					}
				}
				if (!ctx.count && !ctx.calcTotal && multiSortFinished) {
					break;
				}
				result.totalCount += int(ctx.calcTotal);
			} else {
				assertf(static_cast<size_t>(properRowId) < result.rowId2Vdoc->size(),
						"properRowId = {}; rowId = {}; result.rowId2Vdoc->size() = {}", properRowId, rowId, result.rowId2Vdoc->size());
				result.statuses[(*result.rowId2Vdoc)[properRowId]] = 0;
				result.rowIds[properRowId] = true;
			}
		}
	}

	if constexpr (!kPreprocessingBeforeFT) {
		bool toPreResultValues = false;
		if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
			if (auto values = std::get_if<JoinPreResult::Values>(&sctx.preSelect.Result().payload); values) {
				toPreResultValues = true;
				if (sctx.isForceAll) {
					assertrx_throw(!ctx.qPreproc.Start() || !initCount);
					if (ctx.qPreproc.Start() <= values->Size()) {
						ctx.start = 0;
					} else {
						ctx.start = ctx.qPreproc.Start() - values->Size();
					}
				}
				assertrx_throw((std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>));
				if (values->IsRanked()) {
					sortResults(ctx, values->begin().Ranked() + initCount, values->end().Ranked(), sortingOptions,
								sctx.nsid < result.joined_.size() ? &result.joined_[sctx.nsid] : nullptr);
				} else {
					sortResults(ctx, values->begin().NotRanked() + initCount, values->end().NotRanked(), sortingOptions,
								sctx.nsid < result.joined_.size() ? &result.joined_[sctx.nsid] : nullptr);
				}

				const size_t countChange = values->Size() - initCount;
				assertrx_throw(countChange <= ctx.qPreproc.Count());
				ctx.count = ctx.qPreproc.Count() - countChange;
			}
		}
		if (!toPreResultValues) {
			if (sctx.isForceAll) {
				assertrx_throw(!ctx.qPreproc.Start() || !initCount);
				if (ctx.qPreproc.Start() <= result.Count()) {
					ctx.start = 0;
				} else {
					ctx.start = ctx.qPreproc.Start() - result.Count();
				}
			}
			if (sortingOptions.postLoopSortingRequired()) {
				const size_t offset = sctx.isForceAll ? ctx.qPreproc.Start() : multisortLimitLeft;
				if (result.Count() > offset) {
					if (result.haveRank) {
						sortResults(ctx, result.Items().begin().Ranked() + initCount, result.Items().end().Ranked(), sortingOptions,
									sctx.nsid < result.joined_.size() ? &result.joined_[sctx.nsid] : nullptr);
					} else {
						sortResults(ctx, result.Items().begin().NotRanked() + initCount, result.Items().end().NotRanked(), sortingOptions,
									sctx.nsid < result.joined_.size() ? &result.joined_[sctx.nsid] : nullptr);
					}
				}
				setLimitAndOffset(result.Items(), offset, ctx.qPreproc.Count() + initCount);
			} else if (sctx.isForceAll) {
				setLimitAndOffset(result.Items(), ctx.qPreproc.Start(), ctx.qPreproc.Count() + initCount);
			}
			if (sctx.isForceAll) {
				const size_t countChange = result.Count() - initCount;
				assertrx_throw(countChange <= ctx.qPreproc.Count());
				ctx.count = ctx.qPreproc.Count() - countChange;
			}
		}
	}
}

void NsSelecter::getSortIndexValue(const SortingContext& sortCtx, IdType rowId, VariantArray& value, RankT rank,
								   const joins::NamespaceResults* joinResults, const JoinedSelectors& js, int shardId) {
	std::visit(
		overloaded{
			[&](const SortingContext::ExpressionEntry& e) {
				assertrx_throw(e.expression < sortCtx.expressions.size());
				ConstPayload pv(ns_->payloadType_, ns_->items_[rowId]);
				value = VariantArray{
					Variant{sortCtx.expressions[e.expression].Calculate(rowId, pv, joinResults, js, rank, ns_->tagsMatcher_, shardId)}};
			},
			[&](const SortingContext::JoinedFieldEntry& e) {
				assertrx_throw(joinResults);
				value = SortExpression::GetJoinedFieldValues(rowId, *joinResults, js, e.nsIdx, e.field, e.index);
			},
			[&](const SortingContext::FieldEntry& e) {
				if (e.rawData.ptr) {
					value = VariantArray{e.rawData.type.EvaluateOneOf(
						[&e, rowId](KeyValueType::Bool) noexcept { return Variant(*(static_cast<const bool*>(e.rawData.ptr) + rowId)); },
						[&e, rowId](KeyValueType::Int) noexcept { return Variant(*(static_cast<const int*>(e.rawData.ptr) + rowId)); },
						[&e, rowId](KeyValueType::Int64) noexcept {
							return Variant(*(static_cast<const int64_t*>(e.rawData.ptr) + rowId));
						},
						[&e, rowId](KeyValueType::Double) noexcept {
							return Variant(*(static_cast<const double*>(e.rawData.ptr) + rowId));
						},
						[&e, rowId](KeyValueType::String) noexcept {
							return Variant(p_string(static_cast<const std::string_view*>(e.rawData.ptr) + rowId), Variant::noHold);
						},
						[&e, rowId](KeyValueType::Uuid) noexcept { return Variant(*(static_cast<const Uuid*>(e.rawData.ptr) + rowId)); },
						[](KeyValueType::Float) noexcept -> Variant {
							// Indexed fields can not contain float
							throw_as_assert;
						},
						[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
								 KeyValueType::FloatVector>) -> Variant { throw_as_assert; })};
				} else {
					ConstPayload pv(ns_->payloadType_, ns_->items_[rowId]);
					if ((e.data.index != IndexValueType::SetByJsonPath) && !ns_->indexes_[e.data.index]->Opts().IsSparse()) {
						pv.Get(e.data.index, value);
					} else {
						pv.GetByJsonPath(e.data.expression, ns_->tagsMatcher_, value, KeyValueType::Undefined{});
					}
				}
			}},
		sortCtx.getFirstColumnEntry());
}

void NsSelecter::calculateSortExpressions(RankT rank, IdType rowId, IdType properRowId, SelectCtx& sctx, const LocalQueryResults& result) {
	static const JoinedSelectors emptyJoinedSelectors;
	const auto& exprs = sctx.sortingContext.expressions;
	auto& exprResults = sctx.sortingContext.exprResults;
	assertrx_throw(exprs.size() == exprResults.size());
	const ConstPayload pv(ns_->payloadType_, ns_->items_[properRowId]);
	const auto& joinedSelectors = sctx.joinedSelectors ? *sctx.joinedSelectors : emptyJoinedSelectors;
	const auto joinedResultPtr = sctx.nsid < result.joined_.size() ? &result.joined_[sctx.nsid] : nullptr;
	for (size_t i = 0; i < exprs.size(); ++i) {
		exprResults[i].push_back(exprs[i].Calculate(rowId, pv, joinedResultPtr, joinedSelectors, rank, ns_->tagsMatcher_, 0));
	}
}

template <bool aggregationsOnly, typename JoinPreResultCtx>
void NsSelecter::addSelectResult(RankT rank, IdType rowId, IdType properRowId, SelectCtxWithJoinPreSelect<JoinPreResultCtx>& sctx,
								 RVector<Aggregator, 4>& aggregators, LocalQueryResults& result, bool needCalcAggs, bool preselectForFt) {
	if (preselectForFt) {
		return;
	}

	if (needCalcAggs) {
		for (auto& aggregator : aggregators) {
			aggregator.Aggregate(ns_->items_[properRowId]);
		}
	}
	if constexpr (aggregationsOnly) {
		return;
	}
	if (needCalcAggs) {
		if (!aggregators.empty() && aggregators.front().Type() == AggType::AggDistinct && !aggregators.front().DistinctChanged()) {
			return;
		}
	}

	if constexpr (std::is_same_v<JoinPreResultCtx, JoinPreResultBuildCtx>) {
		std::visit(overloaded{[rowId](IdSet& ids) { ids.AddUnordered(rowId); },
							  [&](JoinPreResult::Values& values) {
								  if (!sctx.sortingContext.expressions.empty()) {
									  if (result.haveRank) {
										  values.EmplaceBack(rank, properRowId, sctx.sortingContext.exprResults[0].size(), sctx.nsid);
									  } else {
										  values.EmplaceBack(properRowId, sctx.sortingContext.exprResults[0].size(), sctx.nsid);
									  }
									  calculateSortExpressions(rank, rowId, properRowId, sctx, result);
								  } else {
									  if (result.haveRank) {
										  values.EmplaceBack(rank, properRowId, ns_->items_[properRowId], sctx.nsid);
									  } else {
										  values.EmplaceBack(properRowId, ns_->items_[properRowId], sctx.nsid);
									  }
								  }
							  },
							  [](const SelectIteratorContainer&) { throw_as_assert; }},
				   sctx.preSelect.Result().payload);
	} else {
		if (!sctx.sortingContext.expressions.empty()) {
			if (result.haveRank) {
				result.AddItemRef(rank, properRowId, sctx.sortingContext.exprResults[0].size(), sctx.nsid);
			} else {
				result.AddItemRef(properRowId, sctx.sortingContext.exprResults[0].size(), sctx.nsid);
			}
			calculateSortExpressions(rank, rowId, properRowId, sctx, result);
		} else {
			if (result.haveRank) {
				result.AddItemRef(rank, properRowId, ns_->items_[properRowId], sctx.nsid);
			} else {
				result.AddItemRef(properRowId, ns_->items_[properRowId], sctx.nsid);
			}
		}

		const int kLimitItems = 10000000;
		size_t sz = result.Count();
		if (sz >= kLimitItems && !(sz % kLimitItems)) {
			logFmt(LogWarning, "Too big query results ns='{}',count='{}',rowId='{}',q='{}'", ns_->name_, sz, properRowId,
				   sctx.query.GetSQL());
		}
	}
}

void NsSelecter::checkStrictModeAgg(StrictMode strictMode, std::string_view name, const NamespaceName& nsName,
									const TagsMatcher& tagsMatcher) const {
	if (int index = IndexValueType::SetByJsonPath; ns_->tryGetIndexByName(name, index)) {
		return;
	}

	if (strictMode == StrictModeIndexes) {
		throw Error(errParams,
					"Current query strict mode allows aggregate index fields only. There are no indexes with name '{}' in namespace '{}'",
					name, nsName);
	}
	if (strictMode == StrictModeNames) {
		if (tagsMatcher.path2tag(name).empty()) {
			throw Error(
				errParams,
				"Current query strict mode allows aggregate existing fields only. There are no fields with name '{}' in namespace '{}'",
				name, nsName);
		}
	}
}

RVector<Aggregator, 4> NsSelecter::getAggregators(const std::vector<AggregateEntry>& aggEntries, StrictMode strictMode) const {
	static constexpr int NotFilled = -2;
	RVector<Aggregator, 4> ret;
	h_vector<size_t, 4> distinctIndexes;

	for (const auto& ag : aggEntries) {
		if (ag.Type() == AggCount || ag.Type() == AggCountCached) {
			continue;
		}
		bool compositeIndexFields = false;

		FieldsSet fields;
		h_vector<Aggregator::SortingEntry, 1> sortingEntries;
		sortingEntries.reserve(ag.Sorting().size());
		for (const auto& s : ag.Sorting()) {
			sortingEntries.push_back({(iequals("count"sv, s.expression) ? Aggregator::SortingEntry::Count : NotFilled), s.desc});
		}
		int idx = -1;
		for (size_t i = 0; i < ag.Fields().size(); ++i) {
			size_t fieldsCount = fields.size();
			checkStrictModeAgg(strictMode == StrictModeNotSet ? ns_->config_.strictMode : strictMode, ag.Fields()[i], ns_->name_,
							   ns_->tagsMatcher_);

			for (size_t j = 0; j < sortingEntries.size(); ++j) {
				if (iequals(ag.Fields()[i], ag.Sorting()[j].expression)) {
					sortingEntries[j].field = i;
				}
			}
			if (ns_->getIndexByNameOrJsonPath(ag.Fields()[i], idx)) {
				if (ns_->indexes_[idx]->IsFloatVector()) {
					throw Error(errQueryExec, "Aggregation by float vector index is not allowed: {}", ag.Fields()[i]);
				}
				if (ag.Type() == AggFacet && ag.Fields().size() > 1 && ns_->indexes_[idx]->Opts().IsArray()) {
					throw Error(errQueryExec, "Multifield facet cannot contain an array field");
				}
				if (ns_->indexes_[idx]->Opts().IsSparse()) {
					fields.push_back(ns_->indexes_[idx]->Fields().getTagsPath(0));
				} else if (IsComposite(ns_->indexes_[idx]->Type())) {
					if (ag.Type() == AggDistinct) {
						if (ag.Fields().size() > 1) {
							throw Error(errQueryExec, "DISTINCT multi field not support composite index");
						}
						fields = ns_->indexes_[idx]->Fields();
						compositeIndexFields = true;
					} else {
						throw Error(errQueryExec, "Only DISTINCT aggregations are allowed over composite fields");
					}
				}

				else {
					fields.push_back(idx);
					if (fieldsCount == fields.size()) {
						throw Error(errQueryExec, "Aggregation function fields use one field twice. Field name '{}'", ag.Fields()[i]);
					}
				}
			} else {
				fields.push_back(ns_->tagsMatcher_.path2tag(ag.Fields()[i]));
				if (fieldsCount == fields.size()) {
					throw Error(errQueryExec, "Aggregation function fields use one field twice. Field name '{}'", ag.Fields()[i]);
				}
			}
		}
		for (size_t i = 0; i < sortingEntries.size(); ++i) {
			if (sortingEntries[i].field == NotFilled) {
				throw Error(errQueryExec, "The aggregation {} cannot provide sort by '{}'", AggTypeToStr(ag.Type()),
							ag.Sorting()[i].expression);
			}
		}
		if (ag.Type() == AggDistinct) {
			distinctIndexes.push_back(ret.size());
		}
		ret.emplace_back(ns_->payloadType_, fields, ag.Type(), ag.Fields(), sortingEntries, ag.Limit(), ag.Offset(), compositeIndexFields);
	}

	if (distinctIndexes.size() <= 1) {
		return ret;
	}
	for (const Aggregator& agg : ret) {
		if (agg.Type() == AggDistinct) {
			continue;
		}
		for (const std::string& name : agg.Names()) {
			if (std::find_if(distinctIndexes.cbegin(), distinctIndexes.cend(),
							 [&ret, &name](size_t idx) { return ret[idx].Names()[0] == name; }) == distinctIndexes.cend()) {
				throw Error(errQueryExec, "Cannot be combined several distinct and non distinct aggregator on index {}", name);
			}
		}
	}

	return ret;
}

void NsSelecter::prepareSortIndex(const NamespaceImpl& ns, std::string& column, int& index, bool& skipSortingEntry, StrictMode strictMode) {
	SortExpression::PrepareSortIndex(column, index, ns);
	if (index == IndexValueType::SetByJsonPath) {
		skipSortingEntry |= !validateField(strictMode, column, ns.name_, ns.tagsMatcher_);
	}
}

void NsSelecter::prepareSortJoinedIndex(size_t nsIdx, std::string_view column, int& index,
										const std::vector<JoinedSelector>& joinedSelectors, bool& skipSortingEntry, StrictMode strictMode) {
	assertrx_throw(!column.empty());
	index = IndexValueType::SetByJsonPath;
	assertrx_throw(nsIdx < joinedSelectors.size());
	const auto& js = joinedSelectors[nsIdx];
	std::visit(overloaded{[&](const JoinPreResult::Values& values) {
							  values.payloadType.FieldByName(column, index);
							  if (index == IndexValueType::SetByJsonPath) {
								  skipSortingEntry |= !validateField(strictMode, column, values.nsName, values.tagsMatcher);
							  }
						  },
						  Restricted<IdSet, SelectIteratorContainer>{}([&](const auto&) {
							  js.rightNs_->payloadType_.FieldByName(column, index);
							  if (index == IndexValueType::SetByJsonPath) {
								  skipSortingEntry |= !validateField(strictMode, column, js.rightNs_->name_, js.rightNs_->tagsMatcher_);
							  }
						  })},
			   js.PreResult().payload);
}

bool NsSelecter::validateField(StrictMode strictMode, std::string_view name, const NamespaceName& nsName, const TagsMatcher& tagsMatcher) {
	if (strictMode == StrictModeIndexes) {
		throw Error(errStrictMode,
					"Current query strict mode allows sort by index fields only. There are no indexes with name '{}' in namespace '{}'",
					name, nsName);
	}
	if (tagsMatcher.path2tag(name).empty()) {
		if (strictMode == StrictModeNames) {
			throw Error(
				errStrictMode,
				"Current query strict mode allows sort by existing fields only. There are no fields with name '{}' in namespace '{}'", name,
				nsName);
		}
		return false;
	}
	return true;
}

static void removeQuotesFromExpression(std::string& expression) {
	expression.erase(std::remove(expression.begin(), expression.end(), '"'), expression.end());
}

void NsSelecter::prepareSortingContext(SortingEntries& sortBy, SelectCtx& ctx, IsRanked isRanked, bool availableSelectBySortIndex) const {
	using namespace SortExprFuncs;
	const auto strictMode = ctx.inTransaction
								? StrictModeNone
								: ((ctx.query.GetStrictMode() == StrictModeNotSet) ? ns_->config_.strictMode : ctx.query.GetStrictMode());
	static const JoinedSelectors emptyJoinedSelectors;
	const auto& joinedSelectors = ctx.joinedSelectors ? *ctx.joinedSelectors : emptyJoinedSelectors;
	ctx.sortingContext.entries.clear();
	ctx.sortingContext.expressions.clear();
	for (size_t i = 0; i < sortBy.size(); ++i) {
		SortingEntry& sortingEntry(sortBy[i]);
		assertrx_throw(!sortingEntry.expression.empty());
		SortExpression expr{SortExpression::Parse(sortingEntry.expression, joinedSelectors)};
		if (expr.ByField()) {
			SortingContext::FieldEntry entry{.data = sortingEntry};
			removeQuotesFromExpression(sortingEntry.expression);
			sortingEntry.index = IndexValueType::SetByJsonPath;
			ns_->getIndexByNameOrJsonPath(sortingEntry.expression, sortingEntry.index);
			if (sortingEntry.index >= 0) {
				reindexer::Index* sortIndex = ns_->indexes_[sortingEntry.index].get();
				if (sortIndex->IsFloatVector()) {
					throw Error(errQueryExec, "Ordering by float vector index is not allowed: '{}'", sortingEntry.expression);
				}
				entry.index = sortIndex;
				entry.rawData = SortingContext::RawDataParams(sortIndex->ColumnData(), ns_->payloadType_, sortingEntry.index);
				entry.opts = &sortIndex->Opts().collateOpts_;

				if (i == 0) {
					if (sortIndex->IsOrdered() && !ctx.sortingContext.enableSortOrders && availableSelectBySortIndex) {
						ctx.sortingContext.uncommitedIndex = sortingEntry.index;
						ctx.isForceAll = ctx.sortingContext.forcedMode;
					} else if (!sortIndex->IsOrdered() || isRanked || !ctx.sortingContext.enableSortOrders || !availableSelectBySortIndex) {
						ctx.isForceAll = true;
						entry.index = nullptr;
					}
				}
			} else if (sortingEntry.index == IndexValueType::SetByJsonPath) {
				if (!validateField(strictMode, sortingEntry.expression, ns_->name_, ns_->tagsMatcher_)) {
					continue;
				}
				ctx.isForceAll = true;
			} else {
				std::abort();
			}
			ctx.sortingContext.entries.emplace_back(std::move(entry));
		} else if (expr.ByJoinedField()) {
			auto& je{expr.GetJoinedIndex()};
			const auto& js = joinedSelectors[je.nsIdx];
			assertrx_throw(!std::holds_alternative<JoinPreResult::Values>(js.PreResult().payload));
			bool skip{false};
			int jeIndex = IndexValueType::SetByJsonPath;
			prepareSortIndex(*js.RightNs(), je.column, jeIndex, skip, strictMode);
			if (!skip) {
				ctx.sortingContext.entries.emplace_back(
					SortingContext::JoinedFieldEntry(sortingEntry, je.nsIdx, std::move(je.column), jeIndex));
				ctx.isForceAll = true;
			}
		} else {
			if (!ctx.query.GetMergeQueries().empty()) {
				throw Error(errLogic, "Sorting by expression cannot be applied to merged queries.");
			}
			struct {
				bool skipSortingEntry;
				StrictMode strictMode;
				const JoinedSelectors& joinedSelectors;
			} lCtx{false, strictMode, joinedSelectors};
			expr.VisitForEach(
				Skip<SortExpressionOperation, SortExpressionBracket, SortExprFuncs::Value, SortExprFuncs::SortHash>{},
				[this, &lCtx](SortExprFuncs::Index& exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column, exprIndex.index, lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](JoinedIndex& exprIndex) {
					prepareSortJoinedIndex(exprIndex.nsIdx, exprIndex.column, exprIndex.index, lCtx.joinedSelectors, lCtx.skipSortingEntry,
										   lCtx.strictMode);
				},
				[isRanked](Rank&) {
					if (!isRanked) {
						throw Error(errLogic, "Sorting by rank() is only available for fulltext or knn query");
					}
				},
				[this, &lCtx](DistanceFromPoint& exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column, exprIndex.index, lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](DistanceJoinedIndexFromPoint& exprIndex) {
					prepareSortJoinedIndex(exprIndex.nsIdx, exprIndex.column, exprIndex.index, lCtx.joinedSelectors, lCtx.skipSortingEntry,
										   lCtx.strictMode);
				},
				[this, &lCtx](DistanceBetweenIndexes& exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column1, exprIndex.index1, lCtx.skipSortingEntry, lCtx.strictMode);
					prepareSortIndex(*ns_, exprIndex.column2, exprIndex.index2, lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[this, &lCtx](DistanceBetweenIndexAndJoinedIndex& exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column, exprIndex.index, lCtx.skipSortingEntry, lCtx.strictMode);
					prepareSortJoinedIndex(exprIndex.jNsIdx, exprIndex.jColumn, exprIndex.jIndex, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](DistanceBetweenJoinedIndexes& exprIndex) {
					prepareSortJoinedIndex(exprIndex.nsIdx1, exprIndex.column1, exprIndex.index1, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
					prepareSortJoinedIndex(exprIndex.nsIdx2, exprIndex.column2, exprIndex.index2, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](DistanceBetweenJoinedIndexesSameNs& exprIndex) {
					prepareSortJoinedIndex(exprIndex.nsIdx, exprIndex.column1, exprIndex.index1, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
					prepareSortJoinedIndex(exprIndex.nsIdx, exprIndex.column2, exprIndex.index2, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
				});
			if (lCtx.skipSortingEntry) {
				continue;
			}
			ctx.sortingContext.expressions.emplace_back(std::move(expr));
			ctx.sortingContext.entries.emplace_back(
				SortingContext::ExpressionEntry{sortingEntry, ctx.sortingContext.expressions.size() - 1});
			ctx.isForceAll = true;
		}
	}
	ctx.sortingContext.exprResults.clear();
	ctx.sortingContext.exprResults.resize(ctx.sortingContext.expressions.size());
}

enum class CostCountingPolicy : bool { Any, ExceptTargetSortIdxSeq };

template <CostCountingPolicy countingPolicy>
class CostCalculator {
public:
	explicit CostCalculator(size_t _totalCost) noexcept : totalCost_(_totalCost) {}
	void BeginSequence() noexcept {
		isInSequence_ = true;
		hasInappositeEntries_ = false;
		onlyTargetSortIdxInSequence_ = true;
		curCost_ = 0;
	}
	void EndSequence() noexcept {
		if (isInSequence_ && !hasInappositeEntries_) {
			if ((countingPolicy == CostCountingPolicy::Any) || !onlyTargetSortIdxInSequence_) {
				totalCost_ = std::min(curCost_, totalCost_);
			}
		}
		isInSequence_ = false;
		onlyTargetSortIdxInSequence_ = true;
		curCost_ = 0;
	}
	bool IsInOrSequence() const noexcept { return isInSequence_; }
	void Add(const SelectKeyResults& results, bool isTargetSortIndex) {
		if constexpr (countingPolicy == CostCountingPolicy::ExceptTargetSortIdxSeq) {
			if (!isInSequence_ && isTargetSortIndex) {
				return;
			}
		}
		onlyTargetSortIdxInSequence_ = onlyTargetSortIdxInSequence_ && isTargetSortIndex;
		Add(results);
	}
	void Add(const SelectKeyResults& results) {
		std::visit(
			overloaded{
				[this](const SelectKeyResultsVector& selRes) {
					for (const SelectKeyResult& res : selRes) {
						if (isInSequence_) {
							curCost_ += res.GetMaxIterations(totalCost_);
						} else {
							totalCost_ = res.GetMaxIterations(totalCost_);
						}
					}
				},
				[this](OneOf<ComparatorNotIndexed,
							 Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>) {
					hasInappositeEntries_ = true;
				}},
			results.AsVariant());
	}
	size_t TotalCost() const noexcept { return totalCost_; }
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
	bool onlyTargetSortIdxInSequence_ = true;
	bool hasInappositeEntries_ = false;
	size_t curCost_ = 0;
	size_t totalCost_ = std::numeric_limits<size_t>::max();
};

size_t NsSelecter::calculateNormalCost(const QueryEntries& qentries, SelectCtx& ctx, const RdxContext& rdxCtx) {
	const size_t totalItemsCount = ns_->itemsCount();
	CostCalculator<CostCountingPolicy::ExceptTargetSortIdxSeq> costCalculator(totalItemsCount);
	enum { SortIndexNotFound = 0, SortIndexFound, SortIndexHasUnorderedConditions } sortIndexSearchState = SortIndexNotFound;
	for (size_t next, i = 0, sz = qentries.Size(); i != sz; i = next) {
		next = qentries.Next(i);
		const bool calculateEntry = costCalculator.OnNewEntry(qentries, i, next);
		qentries.Visit(
			i, [] RX_PRE_LMBD_ALWAYS_INLINE(OneOf<SubQueryEntry, SubQueryFieldEntry>) RX_POST_LMBD_ALWAYS_INLINE { throw_as_assert; },
			Skip<AlwaysFalse, AlwaysTrue, DistinctQueryEntry>{},
			[&costCalculator] RX_PRE_LMBD_ALWAYS_INLINE(OneOf<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, KnnQueryEntry>)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { costCalculator.MarkInapposite(); },
			[&](const QueryEntry& qe) {
				if (!qe.IsFieldIndexed()) {
					costCalculator.MarkInapposite();
					return;
				}
				if (qe.IndexNo() == ctx.sortingContext.uncommitedIndex) {
					if (sortIndexSearchState == SortIndexNotFound) {
						const bool isExpectingIdSet =
							qentries.GetOperation(i) == OpAnd && (next == sz || qentries.GetOperation(next) != OpOr);
						if (isExpectingIdSet && !SelectIteratorContainer::IsExpectingOrderedResults(qe)) {
							sortIndexSearchState = SortIndexHasUnorderedConditions;
							return;
						} else {
							sortIndexSearchState = SortIndexFound;
						}
					}
					if (!costCalculator.IsInOrSequence()) {
						// Count cost only for the OR-sequences with mixed indexes: 'ANY_IDX OR TARGET_SORT_IDX',
						// 'TARGET_SORT_IDX OR ANY_IDX1 OR ANY_IDX2', etc.
						return;
					}
				}

				if (!calculateEntry || costCalculator.TotalCost() == 0 || sortIndexSearchState == SortIndexHasUnorderedConditions) {
					return;
				}

				auto& index = ns_->indexes_[qe.IndexNo()];
				if (IsFullText(index->Type())) {
					costCalculator.MarkInapposite();
					return;
				}

				Index::SelectContext indexSelectContext;
				indexSelectContext.opts.disableIdSetCache = 1;
				indexSelectContext.opts.itemsCountInNamespace = totalItemsCount;
				indexSelectContext.opts.indexesNotOptimized = !ctx.sortingContext.enableSortOrders;
				indexSelectContext.opts.inTransaction = ctx.inTransaction;

				try {
					SelectKeyResults results = index->SelectKey(qe.Values(), qe.Condition(), 0, indexSelectContext, rdxCtx);
					costCalculator.Add(results, qe.IndexNo() == ctx.sortingContext.uncommitedIndex);
				} catch (const Error&) {
					costCalculator.MarkInapposite();
				}
			});
	}
	costCalculator.EndSequence();

	if (sortIndexSearchState == SortIndexHasUnorderedConditions) {
		return 0;
	}
	return costCalculator.TotalCost();
}

size_t NsSelecter::calculateOptimizedCost(size_t costNormal, const QueryEntries& qentries, SelectCtx& ctx, const RdxContext& rdxCtx) {
	// 'costOptimized == costNormal + 1' reduces internal iterations count for the tree in the res.GetMaxIterations() call
	CostCalculator<CostCountingPolicy::Any> costCalculator(costNormal + 1);
	for (size_t next, i = 0, sz = qentries.Size(); i != sz; i = next) {
		next = qentries.Next(i);
		if (!costCalculator.OnNewEntry(qentries, i, next)) {
			continue;
		}
		qentries.Visit(
			i, Skip<AlwaysFalse, AlwaysTrue, DistinctQueryEntry>{},
			[] RX_PRE_LMBD_ALWAYS_INLINE(OneOf<SubQueryEntry, SubQueryFieldEntry>) RX_POST_LMBD_ALWAYS_INLINE { throw_as_assert; },
			[&costCalculator] RX_PRE_LMBD_ALWAYS_INLINE(OneOf<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, KnnQueryEntry>)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { costCalculator.MarkInapposite(); },
			[&](const QueryEntry& qe) {
				if (!qe.IsFieldIndexed() || qe.IndexNo() != ctx.sortingContext.uncommitedIndex) {
					costCalculator.MarkInapposite();
					return;
				}

				Index::SelectContext indexSelectContext;
				indexSelectContext.opts.itemsCountInNamespace = ns_->itemsCount();
				indexSelectContext.opts.disableIdSetCache = 1;
				indexSelectContext.opts.unbuiltSortOrders = 1;
				indexSelectContext.opts.indexesNotOptimized = !ctx.sortingContext.enableSortOrders;
				indexSelectContext.opts.inTransaction = ctx.inTransaction;

				try {
					SelectKeyResults results =
						ns_->indexes_[qe.IndexNo()]->SelectKey(qe.Values(), qe.Condition(), 0, indexSelectContext, rdxCtx);
					costCalculator.Add(results);
				} catch (std::exception&) {
					costCalculator.MarkInapposite();
				}
			});
	}
	costCalculator.EndSequence();
	return costCalculator.TotalCost();
}

bool NsSelecter::isSortOptimizationEffective(const QueryEntries& qentries, SelectCtx& ctx, const RdxContext& rdxCtx) {
	if (qentries.Size() == 0) {
		return true;
	}
	if (qentries.Size() == 1 && qentries.Is<QueryEntry>(0)) {
		const auto& qe = qentries.Get<QueryEntry>(0);
		if (qe.IndexNo() == ctx.sortingContext.uncommitedIndex) {
			return SelectIteratorContainer::IsExpectingOrderedResults(qe);
		}
	}

	const size_t expectedMaxIterationsNormal = calculateNormalCost(qentries, ctx, rdxCtx);
	if (expectedMaxIterationsNormal == 0) {
		return false;
	}
	const size_t totalItemsCount = ns_->itemsCount();
	const auto costNormal = size_t(double(expectedMaxIterationsNormal) * log2(expectedMaxIterationsNormal));
	if (costNormal >= totalItemsCount) {
		// Check if it's more effective to iterate over all the items via btree, than select and sort ids via the most effective index
		return true;
	}

	size_t costOptimized = calculateOptimizedCost(costNormal, qentries, ctx, rdxCtx);
	if (costNormal >= costOptimized) {
		return true;  // If max iterations count with btree indexes is better than with any other condition (including sort overhead)
	}
	if (expectedMaxIterationsNormal <= 150) {
		return false;  // If there is very good filtering condition (case for the issues #1489)
	}
	if (ctx.isForceAll || ctx.query.HasLimit()) {
		if (expectedMaxIterationsNormal < 2000) {
			return false;  // Skip attempt to check limit if there is good enough unordered filtering condition
		}
	}
	if (!ctx.isForceAll && ctx.query.HasLimit()) {
		// If optimization will be disabled, selector will must iterate over all the results, ignoring limit
		// Experimental value. It was chosen during debugging request from issue #1402.
		// TODO: It's possible to evaluate this multiplier, based on the query conditions, but the only way to avoid corner cases is to
		// allow user to hint this optimization.
		const size_t limitMultiplier = std::max(size_t(20), size_t(totalItemsCount / expectedMaxIterationsNormal) * 4);
		const auto offset = ctx.query.HasOffset() ? ctx.query.Offset() : 1;
		costOptimized = limitMultiplier * (ctx.query.Limit() + offset);
	}
	return costOptimized <= costNormal;
}

void NsSelecter::writeAggregationResultMergeSubQuery(LocalQueryResults& result, RVector<Aggregator, 4>&& aggregators, SelectCtx& ctx) {
	if (result.aggregationResults.size() < aggregators.size()) {
		throw Error(errQueryExec, "Merged query({}) aggregators count ({}) does not match to the parent query aggregations ({})",
					ctx.query.GetSQL(false), aggregators.size(), result.aggregationResults.size());
	}
	for (size_t i = 0; i < aggregators.size(); i++) {
		AggregationResult r = std::move(aggregators[i]).MoveResult();
		AggregationResult& parentRes = result.aggregationResults[i];
		if (!r.IsEquals(parentRes)) {
			std::stringstream strParentRes;
			std::stringstream strR;
			throw Error(errQueryExec, "Aggregation incorrect ns {} type of parent {} type of query {} parent field {} query field {}",
						ns_->name_, AggTypeToStr(parentRes.GetType()), AggTypeToStr(r.GetType()), parentRes.DumpFields(strParentRes).str(),
						r.DumpFields(strR).str());
		}
		switch (r.GetType()) {
			case AggSum: {
				std::optional<double> newVal = r.GetValue();
				std::optional<double> curVal = parentRes.GetValue();
				if (newVal.has_value()) {
					double cur = 0.0;
					if (curVal.has_value()) {
						cur = curVal.value();
					}
					parentRes.UpdateValue(newVal.value() + cur);
				}
				break;
			}
			case AggMin: {
				std::optional<double> newVal = r.GetValue();
				std::optional<double> curVal = parentRes.GetValue();
				if (newVal.has_value()) {
					if (!curVal.has_value() || newVal.value() < curVal.value()) {
						parentRes.UpdateValue(newVal.value());
					}
				}
				break;
			}
			case AggMax: {
				std::optional<double> newVal = r.GetValue();
				std::optional<double> curVal = parentRes.GetValue();
				if (newVal.has_value()) {
					if (!curVal.has_value() || newVal.value() > curVal.value()) {
						parentRes.UpdateValue(newVal.value());
					}
				}
				break;
			}
			case AggAvg:
			case AggFacet:
			case AggDistinct:
			case AggCount:
			case AggCountCached:
			case AggUnknown:
				throw_as_assert;
		}
	}
}

RX_NO_INLINE void NsSelecter::throwIncorrectRowIdInSortOrders(int rowId, const Index& firstSortIndex, const SelectIterator& firstIterator) {
	throw Error(errLogic, "FirstIterator: {}, firstSortIndex: {}, firstSortIndex size: {}, rowId: {}", firstIterator.name,
				firstSortIndex.Name(), static_cast<int>(firstSortIndex.SortOrders().size()), rowId);
}

template void NsSelecter::operator()(LocalQueryResults&, SelectCtxWithJoinPreSelect<void>&, const RdxContext&);
template void NsSelecter::operator()(LocalQueryResults&, SelectCtxWithJoinPreSelect<JoinPreResultBuildCtx>&, const RdxContext&);
template void NsSelecter::operator()(LocalQueryResults&, SelectCtxWithJoinPreSelect<JoinPreResultExecuteCtx>&, const RdxContext&);

}  // namespace reindexer
