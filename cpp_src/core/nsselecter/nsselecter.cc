#include "nsselecter.h"
#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "crashqueryreporter.h"
#include "estl/multihash_map.h"
#include "explaincalc.h"
#include "itemcomparator.h"
#include "querypreprocessor.h"
#include "sortexpression.h"
#include "tools/logger.h"

using namespace std::string_view_literals;

constexpr int kMinIterationsForInnerJoinOptimization = 100;
constexpr int kMaxIterationsForIdsetPreresult = 10000;
constexpr int kCancelCheckFrequency = 1024;

namespace reindexer {

void NsSelecter::operator()(QueryResults &result, SelectCtx &ctx, const RdxContext &rdxCtx) {
	// const std::string sql = ctx.query.GetSQL();
	// std::cout << sql << std::endl;
	const size_t resultInitSize = result.Count();
	ctx.sortingContext.enableSortOrders = ns_->SortOrdersBuilt();
	if (ns_->config_.logLevel > ctx.query.debugLevel) {
		const_cast<Query *>(&ctx.query)->debugLevel = ns_->config_.logLevel;
	}

	ExplainCalc explain(ctx.query.explain_ || ctx.query.debugLevel >= LogInfo);
	ActiveQueryScope queryScope(ctx, ns_->optimizationState_, explain, ns_->locker_.IsReadOnly(), ns_->strHolder_.get());

	explain.StartTiming();

	auto containSomeAggCount = [&ctx](const AggType &type) {
		auto it = std::find_if(ctx.query.aggregations_.begin(), ctx.query.aggregations_.end(),
							   [&type](const AggregateEntry &agg) { return agg.Type() == type; });
		return it != ctx.query.aggregations_.end();
	};

	bool needPutCachedTotal = false;
	bool containAggCount = containSomeAggCount(AggCount);
	bool containAggCountCached = containAggCount ? false : containSomeAggCount(AggCountCached);

	bool needCalcTotal = ctx.query.calcTotal == ModeAccurateTotal || containAggCount;

	QueryCacheKey ckey;
	if (ctx.query.calcTotal == ModeCachedTotal || containAggCountCached) {
		ckey = QueryCacheKey{ctx.query};

		auto cached = ns_->queryTotalCountCache_->Get(ckey);
		if (cached.valid && cached.val.total_count >= 0) {
			result.totalCount = cached.val.total_count;
			logPrintf(LogTrace, "[%s] using value from cache: %d", ns_->name_, result.totalCount);
		} else {
			needPutCachedTotal = cached.valid;
			logPrintf(LogTrace, "[%s] value for cache will be calculated by query", ns_->name_);
			needCalcTotal = true;
		}
	}

	QueryPreprocessor qPreproc((ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeExecute)
								   ? const_cast<QueryEntries *>(&ctx.query.entries)->MakeLazyCopy()
								   : QueryEntries{ctx.query.entries},
							   ctx.query, ns_, ctx.reqMatchedOnceFlag, ctx.inTransaction);
	if (ctx.joinedSelectors) {
		qPreproc.InjectConditionsFromJoins(*ctx.joinedSelectors, rdxCtx);
	}
	auto aggregators = getAggregators(ctx.query);
	qPreproc.AddDistinctEntries(aggregators);
	const bool aggregationsOnly = aggregators.size() > 1 || (aggregators.size() == 1 && aggregators[0].Type() != AggDistinct);
	qPreproc.InitIndexNumbers();
	bool isFt = qPreproc.ContainsFullTextIndexes();
	// Prepare data for select functions
	if (ctx.functions) {
		fnc_ = ctx.functions->AddNamespace(ctx.query, *ns_, isFt);
	}

	if (isFt) {
		qPreproc.CheckUniqueFtQuery();
		qPreproc.ExcludeFtQuery(*fnc_, rdxCtx);
	}
	if (!ctx.skipIndexesLookup) {
		qPreproc.Reduce(isFt);
	}
	qPreproc.ConvertWhereValues();

	explain.AddPrepareTime();

	if (ctx.contextCollectingMode) {
		result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, ctx.query.selectFilter_), ns_->schema_);
	}

	if (isFt) result.haveRank = true;
	if (ctx.query.IsWithRank()) {
		if (isFt) {
			result.needOutputRank = true;
		} else {
			throw Error(errLogic, "Rank() is available only for fulltext query");
		}
	}

	SelectIteratorContainer qres(ns_->payloadType_, &ctx);
	LoopCtx lctx(qres, ctx, qPreproc, aggregators, explain);
	if (!ctx.query.forcedSortOrder_.empty() && !qPreproc.MoreThanOneEvaluation()) {
		ctx.isForceAll = true;
	}
	const bool isForceAll = ctx.isForceAll;
	do {
		isFt = qPreproc.ContainsFullTextIndexes();
		qres.Clear();
		lctx.start = 0;
		lctx.count = UINT_MAX;
		ctx.isForceAll = isForceAll;

		if (ctx.preResult) {
			if (ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
				// all futher queries for this join MUST have the same enableSortOrders flag
				ctx.preResult->enableSortOrders = ctx.sortingContext.enableSortOrders;
			} else {
				// If in current join query sort orders are disabled
				// then preResult query also MUST have disabled flag
				// If assert fails, then possible query has unlock ns
				// or ns->sortOrdersFlag_ has been reseted under read lock!
				if (!ctx.sortingContext.enableSortOrders) assertrx(!ctx.preResult->enableSortOrders);
				ctx.sortingContext.enableSortOrders = ctx.preResult->enableSortOrders;
			}
		}

		// Prepare sorting context
		ctx.sortingContext.forcedMode = qPreproc.ContainsForcedSortOrder();
		SortingEntries sortBy = qPreproc.GetSortingEntries(ctx);
		prepareSortingContext(sortBy, ctx, isFt, qPreproc.AvailableSelectBySortIndex());

		if (ctx.sortingContext.isOptimizationEnabled()) {
			// Unbuilt btree index optimization is available for query with
			// Check, is it really possible to use it

			if (isFt ||																	 // Disabled if there are search results
				(ctx.preResult /*&& !ctx.preResult->btreeIndexOptimizationEnabled*/) ||	 // Disabled in join preresult (TMP: now disable for
																						 // all right queries), TODO: enable right queries)
				(qPreproc.Size() && qPreproc.GetQueryEntries().GetOperation(0) == OpNot) ||	 // Not in first condition
				!isSortOptimizatonEffective(qPreproc.GetQueryEntries(), ctx,
											rdxCtx)	 // Optimization is not effective (e.g. query contains more effecive filters)
			) {
				ctx.sortingContext.resetOptimization();
				ctx.isForceAll = true;
			}

		} else if (ctx.preResult && (ctx.preResult->executionMode == JoinPreResult::ModeBuild)) {
			ctx.preResult->btreeIndexOptimizationEnabled = false;
		}

		// Add preresults with common conditions of join Queries
		if (ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeExecute) {
			switch (ctx.preResult->dataMode) {
				case JoinPreResult::ModeIdSet: {
					SelectKeyResult res;
					res.emplace_back(std::move(ctx.preResult->ids));
					static const std::string pr = "-preresult";
					qres.Append(OpAnd, SelectIterator(std::move(res), false, pr));
				} break;
				case JoinPreResult::ModeIterators:
					qres.LazyAppend(ctx.preResult->iterators.begin(), ctx.preResult->iterators.end());
					break;
				default:
					assertrx(0);
			}
		}

		qres.PrepareIteratorsForSelectLoop(qPreproc, ctx.sortingContext.sortId(), isFt, *ns_, fnc_, ft_ctx_, rdxCtx);

		explain.AddSelectTime();

		int maxIterations = qres.GetMaxIterations();
		if (ctx.preResult) {
			if (ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
				// Building pre result for next joins

				static_assert(kMaxIterationsForIdsetPreresult > JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization(), "");
				if (ctx.preResult->enableStoredValues &&
					qres.GetMaxIterations(true) <= JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization()) {
					ctx.preResult->dataMode = JoinPreResult::ModeValues;
					ctx.preResult->values.tagsMatcher = ns_->tagsMatcher_;
					ctx.preResult->values.payloadType = ns_->payloadType_;
					// Return preResult as QueryIterators if:
				} else if (maxIterations >=
							   kMaxIterationsForIdsetPreresult ||  // 1. We have > QueryIterator which expects more than 10000 iterations.
						   (ctx.sortingContext.entries.size() &&
							!ctx.sortingContext.sortIndex())				   // 2. We have sorted query, by unordered index
						   || ctx.preResult->btreeIndexOptimizationEnabled) {  // 3. We have btree-index that is not committed yet
					ctx.preResult->iterators.Append(qres.cbegin(), qres.cend());
					if (ctx.query.debugLevel >= LogInfo) {
						logPrintf(LogInfo, "Built preResult (expected %d iterations) with %d iterators, q='%s'", maxIterations, qres.Size(),
								  ctx.query.GetSQL());
					}

					ctx.preResult->dataMode = JoinPreResult::ModeIterators;
					ctx.preResult->executionMode = JoinPreResult::ModeExecute;
					return;
				} else {
					// Build preResult as single IdSet
					ctx.preResult->dataMode = JoinPreResult::ModeIdSet;
					// For building join preresult always use ASC sort orders
					for (SortingEntry &se : sortBy) se.desc = false;
				}
			}
		} else if (!ctx.sortingContext.isOptimizationEnabled()) {
			if (!isFt && maxIterations > kMinIterationsForInnerJoinOptimization) {
				for (size_t i = 0, size = qres.Size(); i < size; i = qres.Next(i)) {
					// for optimization use only isolated InnerJoin
					if (qres.GetOperation(i) == OpAnd && qres.IsJoinIterator(i) &&
						(qres.Next(i) >= size || qres.GetOperation(qres.Next(i)) != OpOr)) {
						const JoinSelectIterator &jIter = qres.Get<JoinSelectIterator>(i);
						assertrx(ctx.joinedSelectors && ctx.joinedSelectors->size() > jIter.joinIndex);
						JoinedSelector &js = (*ctx.joinedSelectors)[jIter.joinIndex];
						js.AppendSelectIteratorOfJoinIndexData(qres, &maxIterations, ctx.sortingContext.sortId(), fnc_, rdxCtx);
					}
				}
			}
		}

		bool reverse = !isFt && ctx.sortingContext.sortIndex() &&
					   std::visit([](const auto &e) noexcept { return e.data.desc; }, ctx.sortingContext.entries[0]);

		bool hasComparators = false;
		qres.ExecuteAppropriateForEach(
			Skip<JoinSelectIterator, SelectIteratorsBracket, AlwaysFalse>{},
			[&hasComparators](const FieldsComparator &) { hasComparators = true; },
			[&hasComparators](const SelectIterator &it) {
				if (it.comparators_.size()) hasComparators = true;
			});

		if (!qres.HasIdsets()) {
			SelectKeyResult scan;
			if (ctx.sortingContext.isOptimizationEnabled()) {
				auto it = ns_->indexes_[ctx.sortingContext.uncommitedIndex]->CreateIterator();
				it->SetMaxIterations(ns_->items_.size());
				scan.emplace_back(it);
				maxIterations = ns_->items_.size();
			} else {
				// special case - no idset in query
				IdType limit = ns_->items_.size();
				if (ctx.sortingContext.isIndexOrdered() && ctx.sortingContext.enableSortOrders) {
					const Index *index = ctx.sortingContext.sortIndex();
					assertrx(index);
					limit = index->SortOrders().size();
				}
				scan.emplace_back(0, limit);
				maxIterations = limit;
			}
			qres.AppendFront(OpAnd, SelectIterator{std::move(scan), false, "-scan", true});
		}
		// Get maximum iterations count, for right calculation comparators costs
		qres.SortByCost(maxIterations);

		// Check idset must be 1st
		qres.CheckFirstQuery();

		// Rewing all results iterators
		qres.ExecuteAppropriateForEach(Skip<JoinSelectIterator, SelectIteratorsBracket, FieldsComparator, AlwaysFalse>{},
									   [reverse, maxIterations](SelectIterator &it) { it.Start(reverse, maxIterations); });

		// Let iterators choose most effecive algorith
		assertrx(qres.Size());
		qres.SetExpectMaxIterations(maxIterations);

		explain.AddPostprocessTime();

		// do not calc total by loop, if we have only 1 condition with 1 idset
		lctx.calcTotal = needCalcTotal &&
						 (hasComparators || qPreproc.MoreThanOneEvaluation() || qres.Size() > 1 || qres.Get<SelectIterator>(0).size() > 1);

		if (qPreproc.IsFtExcluded()) {
			if (reverse && hasComparators) selectLoop<true, true, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
			if (!reverse && hasComparators) selectLoop<false, true, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
			if (reverse && !hasComparators) selectLoop<true, false, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
			if (!reverse && !hasComparators) selectLoop<false, false, false>(lctx, qPreproc.GetFtMergeStatuses(), rdxCtx);
		} else {
			if (reverse && hasComparators && aggregationsOnly) selectLoop<true, true, true>(lctx, result, rdxCtx);
			if (!reverse && hasComparators && aggregationsOnly) selectLoop<false, true, true>(lctx, result, rdxCtx);
			if (reverse && !hasComparators && aggregationsOnly) selectLoop<true, false, true>(lctx, result, rdxCtx);
			if (!reverse && !hasComparators && aggregationsOnly) selectLoop<false, false, true>(lctx, result, rdxCtx);
			if (reverse && hasComparators && !aggregationsOnly) selectLoop<true, true, false>(lctx, result, rdxCtx);
			if (!reverse && hasComparators && !aggregationsOnly) selectLoop<false, true, false>(lctx, result, rdxCtx);
			if (reverse && !hasComparators && !aggregationsOnly) selectLoop<true, false, false>(lctx, result, rdxCtx);
			if (!reverse && !hasComparators && !aggregationsOnly) selectLoop<false, false, false>(lctx, result, rdxCtx);
		}

		// Get total count for simple query with 1 condition and 1 idset
		if (needCalcTotal && !lctx.calcTotal) {
			if (!ctx.query.entries.Empty()) {
				result.totalCount = qres.Get<SelectIterator>(0).GetMaxIterations();
			} else {
				result.totalCount = ns_->items_.size() - ns_->free_.size();
			}
		}
		explain.AddLoopTime();
		explain.AddIterations(maxIterations);
		if (!ctx.inTransaction) {
			ThrowOnCancel(rdxCtx);
		}
	} while (qPreproc.NeedNextEvaluation(lctx.start, lctx.count, ctx.matchedAtLeastOnce));

	processLeftJoins(result, ctx, resultInitSize, rdxCtx);
	if (!ctx.sortingContext.expressions.empty()) {
		if (ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
			if (ctx.preResult->dataMode == JoinPreResult::ModeValues) {
				for (auto &iref : ctx.preResult->values) {
					if (!iref.ValueInitialized()) iref.SetValue(ns_->items_[iref.Id()]);
				}
			}
		} else {
			for (size_t i = resultInitSize; i < result.Items().size(); ++i) {
				auto &iref = result.Items()[i];
				if (!iref.ValueInitialized()) iref.SetValue(ns_->items_[iref.Id()]);
			}
		}
	}
	for (auto &aggregator : aggregators) {
		result.aggregationResults.push_back(aggregator.GetResult());
	}
	//	Put count/count_cached to aggretions
	if (ctx.query.calcTotal != ModeNoTotal || containAggCount || containAggCountCached) {
		AggregationResult ret;
		ret.fields = {"*"};
		ret.type = (ctx.query.calcTotal == ModeAccurateTotal || containAggCount) ? AggCount : AggCountCached;
		ret.SetValue(result.totalCount);
		result.aggregationResults.push_back(ret);
	}

	explain.AddPostprocessTime();
	explain.StopTiming();
	explain.SetSortOptimization(ctx.sortingContext.isOptimizationEnabled());
	explain.PutSortIndex(ctx.sortingContext.sortIndex() ? ctx.sortingContext.sortIndex()->Name() : "-"sv);
	explain.PutCount((ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeBuild)
						 ? (ctx.preResult->dataMode == JoinPreResult::ModeIdSet ? ctx.preResult->ids.size() : ctx.preResult->values.size())
						 : result.Count());
	explain.PutSelectors(&qres);
	explain.PutJoinedSelectors(ctx.joinedSelectors);

	if (ctx.query.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "%s", ctx.query.GetSQL());
		explain.LogDump(ctx.query.debugLevel);
	}
	if (ctx.query.explain_) {
		if (ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
			ctx.preResult->explainPreSelect = explain.GetJSON();
		} else {
			result.explainResults = explain.GetJSON();
		}
	}
	if (ctx.query.debugLevel >= LogTrace) {
		logPrintf(LogInfo, "Query returned: [%s]; total=%d", result.Dump(), result.totalCount);
	}

	if (needPutCachedTotal) {
		logPrintf(LogTrace, "[%s] put totalCount value into query cache: %d ", ns_->name_, result.totalCount);
		ns_->queryTotalCountCache_->Put(ckey, {static_cast<size_t>(result.totalCount)});
	}
	if (ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
		switch (ctx.preResult->dataMode) {
			case JoinPreResult::ModeIdSet:
				if (ctx.query.debugLevel >= LogInfo) {
					logPrintf(LogInfo, "Built idset preResult (expected %d iterations) with %d ids, q = '%s'", explain.Iterations(),
							  ctx.preResult->ids.size(), ctx.query.GetSQL());
				}
				break;
			case JoinPreResult::ModeValues:
				if (ctx.query.debugLevel >= LogInfo) {
					logPrintf(LogInfo, "Built values preResult (expected %d iterations) with %d values, q = '%s'", explain.Iterations(),
							  ctx.preResult->values.size(), ctx.query.GetSQL());
				}
				break;
			default:
				assertrx(0);
		}
		ctx.preResult->executionMode = JoinPreResult::ModeExecute;
	}
}

template <typename It>
const PayloadValue &getValue(const ItemRef &itemRef, const std::vector<PayloadValue> &items);

template <>
const PayloadValue &getValue<ItemRefVector::iterator>(const ItemRef &itemRef, const std::vector<PayloadValue> &items) {
	return items[itemRef.Id()];
}

template <>
const PayloadValue &getValue<JoinPreResult::Values::iterator>(const ItemRef &itemRef, const std::vector<PayloadValue> &) {
	return itemRef.Value();
}

template <>
class NsSelecter::MainNsValueGetter<ItemRefVector::iterator> {
public:
	MainNsValueGetter(const NamespaceImpl &ns) noexcept : ns_{ns} {}
	const PayloadValue &Value(const ItemRef &itemRef) const noexcept { return ns_.items_[itemRef.Id()]; }
	ConstPayload Payload(const ItemRef &itemRef) const noexcept { return ConstPayload{ns_.payloadType_, Value(itemRef)}; }

private:
	const NamespaceImpl &ns_;
};

template <>
class NsSelecter::MainNsValueGetter<JoinPreResult::Values::iterator> {
public:
	MainNsValueGetter(const NamespaceImpl &ns) noexcept : ns_{ns} {}
	const PayloadValue &Value(const ItemRef &itemRef) const noexcept { return itemRef.Value(); }
	ConstPayload Payload(const ItemRef &itemRef) const noexcept { return ConstPayload{ns_.payloadType_, Value(itemRef)}; }

private:
	const NamespaceImpl &ns_;
};

class NsSelecter::JoinedNsValueGetter {
public:
	JoinedNsValueGetter(const NamespaceImpl &ns, const joins::NamespaceResults &jr, size_t nsIdx) noexcept
		: ns_{ns}, joinedResults_{jr}, nsIdx_{nsIdx} {}
	const PayloadValue &Value(const ItemRef &itemRef) const {
		const joins::ItemIterator it{&joinedResults_, itemRef.Id()};
		const auto jfIt = it.at(nsIdx_);
		if (jfIt == it.end() || jfIt.ItemsCount() == 0) {
			throw Error(errQueryExec, "Not found value joined from ns %s", ns_.name_);
		}
		if (jfIt.ItemsCount() > 1) {
			throw Error(errQueryExec, "Found more than 1 value joined from ns %s", ns_.name_);
		}
		return jfIt[0].Value();
	}
	ConstPayload Payload(const ItemRef &itemRef) const noexcept { return ConstPayload{ns_.payloadType_, Value(itemRef)}; }

private:
	const NamespaceImpl &ns_;
	const joins::NamespaceResults &joinedResults_;
	const size_t nsIdx_;
};

template <bool desc, bool multiColumnSort, typename It>
It NsSelecter::applyForcedSort(It begin, It end, const ItemComparator &compare, const SelectCtx &ctx, const joins::NamespaceResults &jr) {
	assertrx_throw(!ctx.sortingContext.entries.empty());
	if (ctx.query.mergeQueries_.size() > 1) throw Error(errLogic, "Force sort could not be applied to 'merged' queries.");
	return std::visit(overloaded{
						  [](const SortingContext::ExpressionEntry &) -> It {
							  throw Error(errLogic, "Force sort could not be performed by expression.");
						  },
						  [&](const SortingContext::FieldEntry &e) {
							  return applyForcedSortImpl<desc, multiColumnSort, It>(*ns_, begin, end, compare, ctx.query.forcedSortOrder_,
																					e.data.expression, MainNsValueGetter<It>{*ns_});
						  },
						  [&](const SortingContext::JoinedFieldEntry &e) {
							  assertrx_throw(ctx.joinedSelectors);
							  assertrx_throw(ctx.joinedSelectors->size() >= e.nsIdx);
							  const auto &joinedSelector = (*ctx.joinedSelectors)[e.nsIdx];
							  return applyForcedSortImpl<desc, multiColumnSort, It>(
								  *joinedSelector.RightNs(), begin, end, compare, ctx.query.forcedSortOrder_, std::string{e.field},
								  JoinedNsValueGetter{*joinedSelector.RightNs(), jr, e.nsIdx});
						  },

					  },
					  ctx.sortingContext.entries[0]);
}

struct RelaxedComparator {
	static bool equal(const Variant &lhs, const Variant &rhs) { return lhs.RelaxCompare<WithString::Yes>(rhs) == 0; }
};

struct RelaxedHasher {
	static std::pair<size_t, size_t> hash(const Variant &v) {
		return v.Type().EvaluateOneOf(
			overloaded{[&v](KeyValueType::Bool) noexcept {
						   return std::pair<size_t, size_t>{0, v.Hash()};
					   },
					   [&v](KeyValueType::Int) noexcept {
						   return std::pair<size_t, size_t>{1, v.Hash()};
					   },
					   [&v](KeyValueType::Int64) noexcept {
						   return std::pair<size_t, size_t>{2, v.Hash()};
					   },
					   [&v](KeyValueType::Double) noexcept {
						   return std::pair<size_t, size_t>{3, v.Hash()};
					   },
					   [&v](KeyValueType::String) noexcept {
						   return std::pair<size_t, size_t>{4, v.Hash()};
					   },
					   [&v](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>)
						   -> std::pair<size_t, size_t> {
						   throw Error{errQueryExec, "Cannot compare value of '%s' type with number of string", v.Type().Name()};
					   }});
	}
	static size_t hash(size_t i, const Variant &v) {
		switch (i) {
			case 0:
				return v.Type().EvaluateOneOf(overloaded{
					[&v](KeyValueType::Bool) noexcept { return v.Hash(); },
					[v](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::String>) {
						return v.convert(KeyValueType::Bool{}).Hash();
					},
					[&v](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> size_t {
						throw Error{errQueryExec, "Cannot compare value of '%s' type with number of string", v.Type().Name()};
					}});
			case 1:
				return v.Type().EvaluateOneOf(overloaded{
					[&v](KeyValueType::Int) noexcept { return v.Hash(); },
					[v](OneOf<KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Double, KeyValueType::String>) {
						return v.convert(KeyValueType::Int{}).Hash();
					},
					[&v](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> size_t {
						throw Error{errQueryExec, "Cannot compare value of '%s' type with number of string", v.Type().Name()};
					}});
			case 2:
				return v.Type().EvaluateOneOf(overloaded{
					[&v](KeyValueType::Int64) noexcept { return v.Hash(); },
					[v](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Double, KeyValueType::String>) {
						return v.convert(KeyValueType::Int64{}).Hash();
					},
					[&v](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> size_t {
						throw Error{errQueryExec, "Cannot compare value of '%s' type with number of string", v.Type().Name()};
					}});
			case 3:
				return v.Type().EvaluateOneOf(overloaded{
					[&v](KeyValueType::Double) noexcept { return v.Hash(); },
					[v](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::String>) {
						return v.convert(KeyValueType::Double{}).Hash();
					},
					[&v](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> size_t {
						throw Error{errQueryExec, "Cannot compare value of '%s' type with number of string", v.Type().Name()};
					}});
			case 4:
				return v.Type().EvaluateOneOf(overloaded{
					[&v](KeyValueType::String) noexcept { return v.Hash(); },
					[v](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) {
						return v.convert(KeyValueType::String{}).Hash();
					},
					[&v](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> size_t {
						throw Error{errQueryExec, "Cannot compare value of '%s' type with number of string", v.Type().Name()};
					}});
			default:
				assertrx_throw(i < 5);
				abort();
		}
	}
};

class ForcedSortMap {
	using MultiMap = MultiHashMap<Variant, size_t, 5, RelaxedHasher, RelaxedComparator>;
	struct SingleTypeMap {
		KeyValueType type_;
		fast_hash_map<Variant, size_t> map_;
	};
	using DataType = std::variant<MultiMap, SingleTypeMap>;

public:
	ForcedSortMap(Variant k, size_t v, size_t size)
		: data_{k.Type().Is<KeyValueType::String>() || k.Type().IsNumeric() ? DataType{MultiMap{size}}
																			: DataType{SingleTypeMap{k.Type(), {}}}} {
		std::visit(overloaded{[&](MultiMap &m) { m.insert(std::move(k), v); }, [&](SingleTypeMap &m) { m.map_.emplace(std::move(k), v); }},
				   data_);
	}
	bool insert(Variant k, size_t v) {
		return std::visit(overloaded{[&](MultiMap &m) { return m.insert(std::move(k), v); },
									 [&](SingleTypeMap &m) {
										 if (!m.type_.IsSame(k.Type())) {
											 throw Error{errQueryExec, "Items of different types in forced sort list"};
										 }
										 return m.map_.emplace(std::move(k), v).second;
									 }},
						  data_);
	}
	bool contain(const Variant &k) const {
		return std::visit(overloaded{[&k](const MultiMap &m) { return m.find(k) != m.cend(); },
									 [&k](const SingleTypeMap &m) {
										 if (!m.type_.IsSame(k.Type())) {
											 throw Error{errQueryExec, "Items of different types in forced sort list"};
										 }
										 return m.map_.find(k) != m.map_.end();
									 }},
						  data_);
	}
	size_t get(const Variant &k) const {
		return std::visit(overloaded{[&k](const MultiMap &m) {
										 const auto it = m.find(k);
										 assertrx_throw(it != m.cend());
										 return it->second;
									 },
									 [&k](const SingleTypeMap &m) {
										 if (!m.type_.IsSame(k.Type())) {
											 throw Error{errQueryExec, "Items of different types in forced sort list"};
										 }
										 const auto it = m.map_.find(k);
										 assertrx_throw(it != m.map_.end());
										 return it->second;
									 }},
						  data_);
	}

private:
	DataType data_;
};

template <bool desc, bool multiColumnSort, typename It, typename ValueGetter>
It NsSelecter::applyForcedSortImpl(NamespaceImpl &ns, It begin, It end, const ItemComparator &compare,
								   const std::vector<Variant> &forcedSortOrder, const std::string &fieldName,
								   const ValueGetter &valueGetter) {
	if (int idx; ns.getIndexByName(fieldName, idx)) {
		if (ns.indexes_[idx]->Opts().IsArray())
			throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");
		const KeyValueType fieldType{ns.indexes_[idx]->KeyType()};
		if (idx < ns.indexes_.firstCompositePos()) {
			// implementation for regular indexes
			fast_hash_map<Variant, ItemRefVector::difference_type> sortMap;
			ItemRefVector::difference_type cost = 0;
			for (auto value : forcedSortOrder) {
				value.convert(fieldType);
				if (!sortMap.emplace(std::move(value), cost).second) {
					// NOLINTNEXTLINE(bugprone-use-after-move)
					throw Error(errQueryExec, "Value '%s' used twice in forced sorting", value.As<std::string>());
				}
				cost++;
			}

			VariantArray keyRefs;
			const auto boundary = std::stable_partition(begin, end, [&](const ItemRef &itemRef) {
				valueGetter.Payload(itemRef).Get(idx, keyRefs);
				if constexpr (desc) {
					return keyRefs.empty() || (sortMap.find(keyRefs[0]) == sortMap.end());
				} else {
					return !keyRefs.empty() && (sortMap.find(keyRefs[0]) != sortMap.end());
				}
			});

			VariantArray lhsItemValue;
			VariantArray rhsItemValue;
			It from, to;
			if constexpr (desc) {
				from = boundary;
				to = end;
			} else {
				from = begin;
				to = boundary;
			}
			std::sort(from, to, [&](const ItemRef &lhs, const ItemRef &rhs) {
				valueGetter.Payload(lhs).Get(idx, lhsItemValue);
				assertrx_throw(!lhsItemValue.empty());
				const auto lhsIt = sortMap.find(lhsItemValue[0]);
				assertrx_throw(lhsIt != sortMap.end());

				valueGetter.Payload(rhs).Get(idx, rhsItemValue);
				assertrx_throw(!rhsItemValue.empty());
				const auto rhsIt = sortMap.find(rhsItemValue[0]);
				assertrx_throw(rhsIt != sortMap.end());

				const auto lhsPos = lhsIt->second;
				const auto rhsPos = rhsIt->second;
				if (lhsPos == rhsPos) {
					if constexpr (multiColumnSort) {
						return compare(lhs, rhs);
					} else {
						if constexpr (desc) {
							return lhs.Id() > rhs.Id();
						} else {
							return lhs.Id() < rhs.Id();
						}
					}
				} else {
					if constexpr (desc) {
						return lhsPos > rhsPos;
					} else {
						return lhsPos < rhsPos;
					}
				}
			});
			return boundary;
		} else {
			// implementation for composite indexes
			const auto &payloadType = ns.payloadType_;
			const FieldsSet &fields = ns.indexes_[idx]->Fields();
			unordered_payload_map<ItemRefVector::difference_type, false> sortMap(0, payloadType, fields);
			ItemRefVector::difference_type cost = 0;
			for (auto value : forcedSortOrder) {
				value.convert(fieldType, &payloadType, &fields);
				if (!sortMap.insert({static_cast<const PayloadValue &>(value), cost}).second) {
					throw Error(errQueryExec, "Value '%s' used twice in forced sorting", value.As<std::string>());
				}
				cost++;
			}

			const auto boundary = std::stable_partition(begin, end, [&](const ItemRef &itemRef) {
				if constexpr (desc) {
					return (sortMap.find(valueGetter.Value(itemRef)) == sortMap.end());
				} else {
					return (sortMap.find(valueGetter.Value(itemRef)) != sortMap.end());
				}
			});

			It from, to;
			if constexpr (desc) {
				from = boundary;
				to = end;
			} else {
				from = begin;
				to = boundary;
			}
			std::sort(from, to, [&](const ItemRef &lhs, const ItemRef &rhs) {
				const auto lhsPos = sortMap.find(valueGetter.Value(lhs))->second;
				const auto rhsPos = sortMap.find(valueGetter.Value(rhs))->second;
				if (lhsPos == rhsPos) {
					if constexpr (multiColumnSort) {
						return compare(lhs, rhs);
					} else {
						if constexpr (desc) {
							return lhs.Id() > rhs.Id();
						} else {
							return lhs.Id() < rhs.Id();
						}
					}
				} else {
					if constexpr (desc) {
						return lhsPos > rhsPos;
					} else {
						return lhsPos < rhsPos;
					}
				}
			});
			return boundary;
		}
	} else {
		ForcedSortMap sortMap{forcedSortOrder[0], 0, forcedSortOrder.size()};
		for (size_t i = 1, s = forcedSortOrder.size(); i < s; ++i) {
			const auto &value = forcedSortOrder[i];
			if (!sortMap.insert(value, i)) {
				throw Error(errQueryExec, "Value '%s' used twice in forced sorting", value.As<std::string>());
			}
		}

		VariantArray keyRefs;
		const auto boundary = std::stable_partition(begin, end, [&](const ItemRef &itemRef) {
			valueGetter.Payload(itemRef).GetByJsonPath(fieldName, ns.tagsMatcher_, keyRefs, KeyValueType::Undefined{});
			if constexpr (desc) {
				return keyRefs.empty() || !sortMap.contain(keyRefs[0]);
			} else {
				return !keyRefs.empty() && sortMap.contain(keyRefs[0]);
			}
		});

		VariantArray lhsItemValue;
		VariantArray rhsItemValue;
		It from, to;
		if constexpr (desc) {
			from = boundary;
			to = end;
		} else {
			from = begin;
			to = boundary;
		}
		std::sort(from, to, [&](const ItemRef &lhs, const ItemRef &rhs) {
			valueGetter.Payload(lhs).GetByJsonPath(fieldName, ns.tagsMatcher_, lhsItemValue, KeyValueType::Undefined{});

			valueGetter.Payload(rhs).GetByJsonPath(fieldName, ns.tagsMatcher_, rhsItemValue, KeyValueType::Undefined{});

			const auto lhsPos = sortMap.get(lhsItemValue[0]);
			const auto rhsPos = sortMap.get(rhsItemValue[0]);
			if (lhsPos == rhsPos) {
				if constexpr (multiColumnSort) {
					return compare(lhs, rhs);
				} else {
					if constexpr (desc) {
						return lhs.Id() > rhs.Id();
					} else {
						return lhs.Id() < rhs.Id();
					}
				}
			} else {
				if constexpr (desc) {
					return lhsPos > rhsPos;
				} else {
					return lhsPos < rhsPos;
				}
			}
		});
		return boundary;
	}
}

template <typename It>
void NsSelecter::applyGeneralSort(It itFirst, It itLast, It itEnd, const ItemComparator &comparator, const SelectCtx &ctx) {
	if (ctx.query.mergeQueries_.size() > 1) {
		throw Error(errLogic, "Sorting cannot be applied to merged queries.");
	}

	std::partial_sort(itFirst, itLast, itEnd, std::cref(comparator));
}

void NsSelecter::setLimitAndOffset(ItemRefVector &queryResult, size_t offset, size_t limit) {
	const unsigned totalRows = queryResult.size();
	if (offset > 0) {
		auto end = offset < totalRows ? queryResult.begin() + offset : queryResult.end();
		queryResult.erase(queryResult.begin(), end);
	}
	if (queryResult.size() > limit) {
		queryResult.erase(queryResult.begin() + limit, queryResult.end());
	}
}

void NsSelecter::processLeftJoins(QueryResults &qr, SelectCtx &sctx, size_t startPos, const RdxContext &rdxCtx) {
	if (!checkIfThereAreLeftJoins(sctx)) return;
	for (size_t i = startPos; i < qr.Count(); ++i) {
		IdType rowid = qr[i].GetItemRef().Id();
		ConstPayload pl(ns_->payloadType_, ns_->items_[rowid]);
		for (auto &joinedSelector : *sctx.joinedSelectors) {
			if (joinedSelector.Type() == JoinType::LeftJoin) joinedSelector.Process(rowid, sctx.nsid, pl, true);
		}
		if (!sctx.inTransaction && (i % kCancelCheckFrequency == 0)) ThrowOnCancel(rdxCtx);
	}
}

bool NsSelecter::checkIfThereAreLeftJoins(SelectCtx &sctx) const {
	if (!sctx.joinedSelectors) return false;
	for (auto &joinedSelector : *sctx.joinedSelectors) {
		if (joinedSelector.Type() == JoinType::LeftJoin) {
			return true;
		}
	}
	return false;
}

template <typename It>
void NsSelecter::sortResults(LoopCtx &ctx, It begin, It end, const SortingOptions &sortingOptions, const joins::NamespaceResults &jr) {
	SelectCtx &sctx = ctx.sctx;
	ctx.explain.StartSort();
#ifndef NDEBUG
	for (const auto &eR : sctx.sortingContext.exprResults) {
		assertrx(eR.size() == end - begin);
	}
#endif

	ItemComparator comparator{*ns_, sctx, jr};
	if (sortingOptions.forcedMode) {
		comparator.BindForForcedSort();
		assertrx(!sctx.query.sortingEntries_.empty());
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

static size_t resultSize(const QueryResults &qr) noexcept { return qr.Count(); }
static size_t resultSize(const FtMergeStatuses &) noexcept {
	assertrx(0);
	abort();
}

static void resultReserve(QueryResults &qr, size_t s) { qr.Items().reserve(s); }
static void resultReserve(FtMergeStatuses &, size_t) {}

template <bool reverse, bool hasComparators, bool aggregationsOnly, typename ResultsT>
void NsSelecter::selectLoop(LoopCtx &ctx, ResultsT &result, const RdxContext &rdxCtx) {
	static constexpr bool kPreprocessingBeforFT = !std::is_same_v<ResultsT, QueryResults>;
	static const JoinedSelectors emptyJoinedSelectors;
	const auto selectLoopWard = rdxCtx.BeforeSelectLoop();
	SelectCtx &sctx = ctx.sctx;
	const auto &joinedSelectors = sctx.joinedSelectors ? *sctx.joinedSelectors : emptyJoinedSelectors;
	SelectIteratorContainer &qres = ctx.qres;
	// Is not using during ft preprocessing
	const size_t initCount =
		kPreprocessingBeforFT ? 0
							  : ((sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) ? sctx.preResult->values.size()
																										   : resultSize(result));

	if (!sctx.isForceAll) {
		ctx.start = ctx.qPreproc.Start();
		ctx.count = ctx.qPreproc.Count();
	}

	// reserve queryresults, if we have only 1 condition with 1 idset
	if (qres.Size() == 1 && qres.IsSelectIterator(0) && qres.Get<SelectIterator>(0).size() == 1) {
		unsigned reserve = std::min(unsigned(qres.Get<SelectIterator>(0).GetMaxIterations()), ctx.count);
		if (sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) {
			sctx.preResult->values.reserve(initCount + reserve);
		} else {
			resultReserve(result, initCount + reserve);
		}
	}

	bool finish = (ctx.count == 0) && !sctx.reqMatchedOnceFlag && !ctx.calcTotal && !sctx.matchedAtLeastOnce;

	SortingOptions sortingOptions(sctx.sortingContext);
	const Index *const firstSortIndex = sctx.sortingContext.sortIndexIfOrdered();
	bool multiSortFinished = !(sortingOptions.multiColumnByBtreeIndex && ctx.count > 0);

	VariantArray prevValues;
	size_t multisortLimitLeft = 0;

	// TODO: nested conditions support. Like (A  OR B OR C) AND (X OR Z)
	assertrx(!qres.Empty());
	assertrx(qres.IsSelectIterator(0));
	SelectIterator &firstIterator = qres.begin()->Value<SelectIterator>();
	IdType rowId = firstIterator.Val();
	while (firstIterator.Next(rowId) && !finish) {
		if (!sctx.inTransaction && (rowId % kCancelCheckFrequency == 0)) ThrowOnCancel(rdxCtx);
		rowId = firstIterator.Val();
		IdType properRowId = rowId;

		if (firstSortIndex) {
			assertf(firstSortIndex->SortOrders().size() > static_cast<size_t>(rowId),
					"FirstIterator: %s, firstSortIndex: %s, firstSortIndex size: %d, rowId: %d", firstIterator.name.c_str(),
					firstSortIndex->Name().c_str(), static_cast<int>(firstSortIndex->SortOrders().size()), rowId);
			properRowId = firstSortIndex->SortOrders()[rowId];
		}

		assertrx(static_cast<size_t>(properRowId) < ns_->items_.size());
		PayloadValue &pv = ns_->items_[properRowId];
		if (pv.IsFree()) continue;
		assertrx(pv.Ptr());
		if (qres.Process<reverse, hasComparators>(pv, &finish, &rowId, properRowId, !ctx.start && ctx.count)) {
			sctx.matchedAtLeastOnce = true;
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			for (auto &it : qres) {
				if (it.HoldsOrReferTo<SelectIterator>() && it.Value<SelectIterator>().distinct) {
					it.Value<SelectIterator>().ExcludeLastSet(pv, rowId, properRowId);
				}
			}
			if constexpr (!kPreprocessingBeforFT) {
				uint8_t proc = ft_ctx_ ? ft_ctx_->Proc(firstIterator.Pos()) : 0;
				if ((ctx.start || (ctx.count == 0)) && sortingOptions.multiColumnByBtreeIndex) {
					VariantArray recentValues;
					size_t lastResSize = result.Count();
					getSortIndexValue(sctx.sortingContext, properRowId, recentValues, proc, result.joined_[sctx.nsid], joinedSelectors);
					if (prevValues.empty() && result.Items().empty()) {
						prevValues = recentValues;
					} else {
						if (recentValues != prevValues) {
							if (ctx.start) {
								result.Items().clear();
								multisortLimitLeft = 0;
								lastResSize = 0;
								prevValues = recentValues;
							} else if (!ctx.count) {
								multiSortFinished = true;
							}
						}
					}
					if (!multiSortFinished) {
						addSelectResult<aggregationsOnly>(proc, rowId, properRowId, sctx, ctx.aggregators, result, ctx.preselectForFt);
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
					addSelectResult<aggregationsOnly>(proc, rowId, properRowId, sctx, ctx.aggregators, result, ctx.preselectForFt);
					--ctx.count;
					if (!ctx.count && sortingOptions.multiColumn && !multiSortFinished)
						getSortIndexValue(sctx.sortingContext, properRowId, prevValues, proc, result.joined_[sctx.nsid], joinedSelectors);
				}
				if (!ctx.count && !ctx.calcTotal && multiSortFinished) break;
				if (ctx.calcTotal) result.totalCount++;
			} else {
				assertf(static_cast<size_t>(properRowId) < result.rowId2Vdoc->size(),
						"properRowId = %d; rowId = %d; result.rowId2Vdoc->size() = %d", properRowId, rowId, result.rowId2Vdoc->size());
				result.statuses[(*result.rowId2Vdoc)[properRowId]] = 0;
				result.rowIds[properRowId] = true;
			}
		}
	}

	if constexpr (!kPreprocessingBeforFT) {
		if (sctx.isForceAll) {
			assertrx(!ctx.qPreproc.Start() || !initCount);
			if (sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) {
				if (ctx.qPreproc.Start() <= sctx.preResult->values.size()) {
					ctx.start = 0;
				} else {
					ctx.start = ctx.qPreproc.Start() - sctx.preResult->values.size();
				}
			} else {
				if (ctx.qPreproc.Start() <= result.Items().size()) {
					ctx.start = 0;
				} else {
					ctx.start = ctx.qPreproc.Start() - result.Items().size();
				}
			}
		}

		if (sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) {
			assertrx(sctx.preResult->executionMode == JoinPreResult::ModeBuild);
			sortResults(ctx, sctx.preResult->values.begin() + initCount, sctx.preResult->values.end(), sortingOptions,
						result.joined_[sctx.nsid]);
		} else if (sortingOptions.postLoopSortingRequired()) {
			const size_t offset = sctx.isForceAll ? ctx.qPreproc.Start() : multisortLimitLeft;
			if (result.Items().size() > offset) {
				sortResults(ctx, result.Items().begin() + initCount, result.Items().end(), sortingOptions, result.joined_[sctx.nsid]);
			}
			setLimitAndOffset(result.Items(), offset, ctx.qPreproc.Count() + initCount);
		}

		if (sctx.isForceAll) {
			const size_t countChange =
				((sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) ? sctx.preResult->values.size()
																						   : result.Items().size()) -
				initCount;
			assertrx(countChange <= ctx.qPreproc.Count());
			ctx.count = ctx.qPreproc.Count() - countChange;
		}
	}
}

void NsSelecter::getSortIndexValue(const SortingContext &sortCtx, IdType rowId, VariantArray &value, uint8_t proc,
								   const joins::NamespaceResults &joinResults, const JoinedSelectors &js) {
	ConstPayload pv(ns_->payloadType_, ns_->items_[rowId]);
	std::visit(overloaded{[&](const SortingContext::ExpressionEntry &e) {
							  assertrx(e.expression < sortCtx.expressions.size());
							  value = VariantArray{Variant{
								  sortCtx.expressions[e.expression].Calculate(rowId, pv, joinResults, js, proc, ns_->tagsMatcher_)}};
						  },
						  [&](const SortingContext::JoinedFieldEntry &e) {
							  value = SortExpression::GetJoinedFieldValues(rowId, joinResults, js, e.nsIdx, e.field, e.index);
						  },
						  [&](const SortingContext::FieldEntry &e) {
							  if ((e.data.index == IndexValueType::SetByJsonPath) || ns_->indexes_[e.data.index]->Opts().IsSparse()) {
								  pv.GetByJsonPath(e.data.expression, ns_->tagsMatcher_, value, KeyValueType::Undefined{});
							  } else {
								  pv.Get(e.data.index, value);
							  }
						  }},
			   sortCtx.getFirstColumnEntry());
}

void NsSelecter::calculateSortExpressions(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &sctx, const QueryResults &result) {
	static const JoinedSelectors emptyJoinedSelectors;
	const auto &exprs = sctx.sortingContext.expressions;
	auto &exprResults = sctx.sortingContext.exprResults;
	assertrx(exprs.size() == exprResults.size());
	const ConstPayload pv(ns_->payloadType_, ns_->items_[properRowId]);
	const auto &joinedSelectors = sctx.joinedSelectors ? *sctx.joinedSelectors : emptyJoinedSelectors;
	for (size_t i = 0; i < exprs.size(); ++i) {
		exprResults[i].push_back(exprs[i].Calculate(rowId, pv, result.joined_[sctx.nsid], joinedSelectors, proc, ns_->tagsMatcher_));
	}
}

template <bool aggregationsOnly>
void NsSelecter::addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
								 QueryResults &result, bool preselectForFt) {
	if (preselectForFt) return;
	for (auto &aggregator : aggregators) aggregator.Aggregate(ns_->items_[properRowId]);
	if constexpr (aggregationsOnly) return;
	if (sctx.preResult && sctx.preResult->executionMode == JoinPreResult::ModeBuild) {
		switch (sctx.preResult->dataMode) {
			case JoinPreResult::ModeIdSet:
				sctx.preResult->ids.AddUnordered(rowId);
				break;
			case JoinPreResult::ModeValues:
				if (!sctx.sortingContext.expressions.empty()) {
					sctx.preResult->values.emplace_back(properRowId, sctx.sortingContext.exprResults[0].size(), proc, sctx.nsid);
					calculateSortExpressions(proc, rowId, properRowId, sctx, result);
				} else {
					sctx.preResult->values.emplace_back(properRowId, ns_->items_[properRowId], proc, sctx.nsid);
				}
				break;
			default:
				abort();
		}
	} else {
		if (!sctx.sortingContext.expressions.empty()) {
			result.Add({properRowId, sctx.sortingContext.exprResults[0].size(), proc, sctx.nsid});
			calculateSortExpressions(proc, rowId, properRowId, sctx, result);
		} else {
			result.Add({properRowId, ns_->items_[properRowId], proc, sctx.nsid});
		}

		const int kLimitItems = 10000000;
		size_t sz = result.Count();
		if (sz >= kLimitItems && !(sz % kLimitItems)) {
			logPrintf(LogWarning, "Too big query results ns='%s',count='%d',rowId='%d',q='%s'", ns_->name_, sz, properRowId,
					  sctx.query.GetSQL());
		}
	}
}

void NsSelecter::checkStrictModeAgg(StrictMode strictMode, const std::string &name, const std::string &nsName,
									const TagsMatcher &tagsMatcher) const {
	if (int index = IndexValueType::SetByJsonPath; ns_->getIndexByName(name, index)) return;

	if (strictMode == StrictModeIndexes) {
		throw Error(errParams,
					"Current query strict mode allows aggregate index fields only. There are no indexes with name '%s' in namespace '%s'",
					name, nsName);
	}
	if (strictMode == StrictModeNames) {
		if (tagsMatcher.path2tag(name).empty()) {
			throw Error(
				errParams,
				"Current query strict mode allows aggregate existing fields only. There are no fields with name '%s' in namespace '%s'",
				name, nsName);
		}
	}
}

h_vector<Aggregator, 4> NsSelecter::getAggregators(const Query &q) const {
	static constexpr int NotFilled = -2;
	h_vector<Aggregator, 4> ret;
	h_vector<size_t, 4> distinctIndexes;

	for (const auto &ag : q.aggregations_) {
		if (ag.Type() == AggCount || ag.Type() == AggCountCached) {
			continue;
		}
		bool compositeIndexFields = false;

		FieldsSet fields;
		h_vector<Aggregator::SortingEntry, 1> sortingEntries;
		sortingEntries.reserve(ag.Sorting().size());
		for (const auto &s : ag.Sorting()) {
			sortingEntries.push_back({(iequals("count"sv, s.expression) ? Aggregator::SortingEntry::Count : NotFilled), s.desc});
		}
		int idx = -1;
		for (size_t i = 0; i < ag.Fields().size(); ++i) {
			checkStrictModeAgg(q.strictMode == StrictModeNotSet ? ns_->config_.strictMode : q.strictMode, ag.Fields()[i], ns_->name_,
							   ns_->tagsMatcher_);

			for (size_t j = 0; j < sortingEntries.size(); ++j) {
				if (iequals(ag.Fields()[i], ag.Sorting()[j].expression)) {
					sortingEntries[j].field = i;
				}
			}
			if (ns_->getIndexByName(ag.Fields()[i], idx)) {
				if (ns_->indexes_[idx]->Opts().IsSparse()) {
					fields.push_back(ns_->indexes_[idx]->Fields().getTagsPath(0));
				} else if (ag.Type() == AggFacet && ag.Fields().size() > 1 && ns_->indexes_[idx]->Opts().IsArray()) {
					throw Error(errQueryExec, "Multifield facet cannot contain an array field");
				} else if (ag.Type() == AggDistinct && IsComposite(ns_->indexes_[idx]->Type())) {
					fields = ns_->indexes_[idx]->Fields();
					compositeIndexFields = true;
				}

				else {
					fields.push_back(idx);
				}
			} else {
				fields.push_back(ns_->tagsMatcher_.path2tag(ag.Fields()[i]));
			}
		}
		for (size_t i = 0; i < sortingEntries.size(); ++i) {
			if (sortingEntries[i].field == NotFilled) {
				throw Error(errQueryExec, "The aggregation %s cannot provide sort by '%s'", AggTypeToStr(ag.Type()),
							ag.Sorting()[i].expression);
			}
		}
		if (ag.Type() == AggDistinct) distinctIndexes.push_back(ret.size());
		ret.emplace_back(ns_->payloadType_, fields, ag.Type(), ag.Fields(), sortingEntries, ag.Limit(), ag.Offset(), compositeIndexFields);
	}

	if (distinctIndexes.size() <= 1) return ret;
	for (const Aggregator &agg : ret) {
		if (agg.Type() == AggDistinct) continue;
		for (const std::string &name : agg.Names()) {
			if (std::find_if(distinctIndexes.cbegin(), distinctIndexes.cend(),
							 [&ret, &name](size_t idx) { return ret[idx].Names()[0] == name; }) == distinctIndexes.cend()) {
				throw Error(errQueryExec, "Cannot be combined several distincts and non distinct aggregator on index %s", name);
			}
		}
	}

	return ret;
}

void NsSelecter::prepareSortIndex(const NamespaceImpl &ns, std::string_view column, int &index, bool &skipSortingEntry,
								  StrictMode strictMode) {
	assertrx(!column.empty());
	index = IndexValueType::SetByJsonPath;
	if (ns.getIndexByName(std::string{column}, index) && ns.indexes_[index]->Opts().IsSparse()) {
		index = IndexValueType::SetByJsonPath;
	}
	if (index == IndexValueType::SetByJsonPath) {
		skipSortingEntry |= !validateField(strictMode, column, ns.name_, ns.tagsMatcher_);
	}
}

void NsSelecter::prepareSortJoinedIndex(size_t nsIdx, std::string_view column, int &index,
										const std::vector<JoinedSelector> &joinedSelectors, bool &skipSortingEntry, StrictMode strictMode) {
	assertrx(!column.empty());
	index = IndexValueType::SetByJsonPath;
	assertrx_throw(nsIdx < joinedSelectors.size());
	const auto &js = joinedSelectors[nsIdx];
	(js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.payloadType : js.rightNs_->payloadType_)
		.FieldByName(std::string{column}, index);
	if (index == IndexValueType::SetByJsonPath) {
		skipSortingEntry |= !validateField(
			strictMode, column, js.joinQuery_._namespace,
			js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.tagsMatcher : js.rightNs_->tagsMatcher_);
	}
}

bool NsSelecter::validateField(StrictMode strictMode, std::string_view name, std::string_view nsName, const TagsMatcher &tagsMatcher) {
	if (strictMode == StrictModeIndexes) {
		throw Error(errParams,
					"Current query strict mode allows sort by index fields only. There are no indexes with name '%s' in namespace '%s'",
					name, nsName);
	}
	if (tagsMatcher.path2tag(name).empty()) {
		if (strictMode == StrictModeNames) {
			throw Error(
				errParams,
				"Current query strict mode allows sort by existing fields only. There are no fields with name '%s' in namespace '%s'", name,
				nsName);
		}
		return false;
	}
	return true;
}

static void removeQuotesFromExpression(std::string &expression) {
	expression.erase(std::remove(expression.begin(), expression.end(), '"'), expression.end());
}

void NsSelecter::prepareSortingContext(SortingEntries &sortBy, SelectCtx &ctx, bool isFt, bool availableSelectBySortIndex) {
	using namespace SortExprFuncs;
	const auto strictMode =
		ctx.inTransaction ? StrictModeNone : ((ctx.query.strictMode == StrictModeNotSet) ? ns_->config_.strictMode : ctx.query.strictMode);
	static const JoinedSelectors emptyJoinedSelectors;
	const auto &joinedSelectors = ctx.joinedSelectors ? *ctx.joinedSelectors : emptyJoinedSelectors;
	ctx.sortingContext.entries.clear();
	ctx.sortingContext.expressions.clear();
	for (size_t i = 0; i < sortBy.size(); ++i) {
		SortingEntry &sortingEntry(sortBy[i]);
		assertrx(!sortingEntry.expression.empty());
		SortExpression expr{SortExpression::Parse(sortingEntry.expression, joinedSelectors)};
		if (expr.ByIndexField()) {
			SortingContext::FieldEntry entry{nullptr, sortingEntry};
			removeQuotesFromExpression(sortingEntry.expression);
			sortingEntry.index = IndexValueType::SetByJsonPath;
			ns_->getIndexByName(sortingEntry.expression, sortingEntry.index);
			if (sortingEntry.index >= 0) {
				reindexer::Index *sortIndex = ns_->indexes_[sortingEntry.index].get();
				entry.index = sortIndex;
				entry.opts = &sortIndex->Opts().collateOpts_;

				if (i == 0) {
					if (sortIndex->IsOrdered() && !ctx.sortingContext.enableSortOrders && availableSelectBySortIndex) {
						ctx.sortingContext.uncommitedIndex = sortingEntry.index;
						ctx.isForceAll = ctx.sortingContext.forcedMode;
					} else if (!sortIndex->IsOrdered() || isFt || !ctx.sortingContext.enableSortOrders || !availableSelectBySortIndex) {
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
		} else if (expr.ByJoinedIndexField()) {
			const auto &je{expr.GetJoinedIndex()};
			SortingContext::JoinedFieldEntry entry{sortingEntry, je.nsIdx, je.column, IndexValueType::SetByJsonPath};
			const auto &js = joinedSelectors[entry.nsIdx];
			assertrx_throw(js.preResult_->dataMode != JoinPreResult::ModeValues);
			bool skip{false};
			prepareSortIndex(*js.RightNs(), entry.field, entry.index, skip, strictMode);
			if (!skip) {
				ctx.sortingContext.entries.emplace_back(std::move(entry));
				ctx.isForceAll = true;
			}
		} else {
			if (!ctx.query.mergeQueries_.empty()) {
				throw Error(errLogic, "Sorting by expression cannot be applied to merged queries.");
			}
			struct {
				bool skipSortingEntry;
				StrictMode strictMode;
				const JoinedSelectors &joinedSelectors;
			} lCtx{false, strictMode, joinedSelectors};
			expr.ExecuteAppropriateForEach(
				Skip<SortExpressionOperation, SortExpressionBracket, SortExprFuncs::Value>{},
				[this, &lCtx](SortExprFuncs::Index &exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column, exprIndex.index, lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](JoinedIndex &exprIndex) {
					prepareSortJoinedIndex(exprIndex.nsIdx, exprIndex.column, exprIndex.index, lCtx.joinedSelectors, lCtx.skipSortingEntry,
										   lCtx.strictMode);
				},
				[isFt](Rank &) {
					if (!isFt) {
						throw Error(errLogic, "Sorting by rank() is only available for full-text query");
					}
				},
				[this, &lCtx](DistanceFromPoint &exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column, exprIndex.index, lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](DistanceJoinedIndexFromPoint &exprIndex) {
					prepareSortJoinedIndex(exprIndex.nsIdx, exprIndex.column, exprIndex.index, lCtx.joinedSelectors, lCtx.skipSortingEntry,
										   lCtx.strictMode);
				},
				[this, &lCtx](DistanceBetweenIndexes &exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column1, exprIndex.index1, lCtx.skipSortingEntry, lCtx.strictMode);
					prepareSortIndex(*ns_, exprIndex.column2, exprIndex.index2, lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[this, &lCtx](DistanceBetweenIndexAndJoinedIndex &exprIndex) {
					prepareSortIndex(*ns_, exprIndex.column, exprIndex.index, lCtx.skipSortingEntry, lCtx.strictMode);
					prepareSortJoinedIndex(exprIndex.jNsIdx, exprIndex.jColumn, exprIndex.jIndex, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](DistanceBetweenJoinedIndexes &exprIndex) {
					prepareSortJoinedIndex(exprIndex.nsIdx1, exprIndex.column1, exprIndex.index1, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
					prepareSortJoinedIndex(exprIndex.nsIdx2, exprIndex.column2, exprIndex.index2, lCtx.joinedSelectors,
										   lCtx.skipSortingEntry, lCtx.strictMode);
				},
				[&lCtx](DistanceBetweenJoinedIndexesSameNs &exprIndex) {
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

bool NsSelecter::isSortOptimizatonEffective(const QueryEntries &qentries, SelectCtx &ctx, const RdxContext &rdxCtx) {
	if (qentries.Size() == 0) {
		return true;
	}
	if (qentries.Size() == 1 && qentries.HoldsOrReferTo<QueryEntry>(0)) {
		const auto &qe = qentries.Get<QueryEntry>(0);
		if (qe.idxNo == ctx.sortingContext.uncommitedIndex && SelectIteratorContainer::IsExpectingOrderedResults(qe)) {
			return true;
		}
	}

	size_t costNormal = ns_->items_.size() - ns_->free_.size();
	enum { SortIndexNotFound = 0, SortIndexFound, SortIndexHasUnorderedConditions } sortIndexSearchState = SortIndexNotFound;

	qentries.ExecuteAppropriateForEach(
		Skip<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse>{},
		[this, &ctx, &rdxCtx, &costNormal, &sortIndexSearchState](const QueryEntry &qe) {
			if (qe.idxNo < 0) return;
			if (qe.idxNo == ctx.sortingContext.uncommitedIndex) {
				if (sortIndexSearchState == SortIndexNotFound && !SelectIteratorContainer::IsExpectingOrderedResults(qe)) {
					sortIndexSearchState = SortIndexHasUnorderedConditions;
				} else {
					sortIndexSearchState = SortIndexFound;
				}
				return;
			}
			if (costNormal == 0) return;

			auto &index = ns_->indexes_[qe.idxNo];
			if (IsFullText(index->Type())) return;

			Index::SelectOpts opts;
			opts.disableIdSetCache = 1;
			opts.itemsCountInNamespace = ns_->items_.size() - ns_->free_.size();
			opts.indexesNotOptimized = !ctx.sortingContext.enableSortOrders;
			opts.inTransaction = ctx.inTransaction;

			try {
				SelectKeyResults reslts = index->SelectKey(qe.values, qe.condition, 0, opts, nullptr, rdxCtx);
				for (const SelectKeyResult &res : reslts) {
					if (res.comparators_.empty()) {
						costNormal = std::min(costNormal, res.GetMaxIterations(costNormal));
					}
				}
			} catch (const Error &) {
			}
		});

	if (sortIndexSearchState == SortIndexHasUnorderedConditions) {
		return false;
	}
	if (costNormal == 0) {
		return false;
	}
	size_t costOptimized = ns_->items_.size() - ns_->free_.size();
	costNormal = size_t(double(costNormal) * log2(costNormal));
	if (costNormal < costOptimized) {
		costOptimized = costNormal + 1;
		qentries.ExecuteAppropriateForEach(Skip<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse>{},
										   [this, &ctx, &rdxCtx, &costOptimized](const QueryEntry &qe) {
											   if (qe.idxNo < 0 || qe.idxNo != ctx.sortingContext.uncommitedIndex) return;

											   Index::SelectOpts opts;
											   opts.itemsCountInNamespace = ns_->items_.size() - ns_->free_.size();
											   opts.disableIdSetCache = 1;
											   opts.unbuiltSortOrders = 1;
											   opts.indexesNotOptimized = !ctx.sortingContext.enableSortOrders;
											   opts.inTransaction = ctx.inTransaction;

											   try {
												   SelectKeyResults reslts = ns_->indexes_[qe.idxNo]->SelectKey(qe.values, qe.condition, 0,
																												opts, nullptr, rdxCtx);
												   for (const SelectKeyResult &res : reslts) {
													   if (res.comparators_.empty()) {
														   costOptimized = std::min(costOptimized, res.GetMaxIterations(costOptimized));
													   }
												   }
											   } catch (const Error &) {
											   }
										   });
	} else {
		return true;
	}

	if (costNormal < costOptimized && !ctx.isForceAll && ctx.query.HasLimit()) {
		// If optimization will be disabled, selecter will must to iterate over all the results, ignoring limit
		// Experimental value. It was chosen during debugging request from issue #1402.
		// TODO: It's possible to evaluate this multiplier, based on the query conditions, but the only way to avoid corner cases is to
		// allow user to hint this optimization.
		constexpr static unsigned kLimitMultiplier = 20;
		const auto offset = ctx.query.HasOffset() ? ctx.query.start : 1;
		costOptimized = kLimitMultiplier * (ctx.query.count + offset);
	}

	return costOptimized <= costNormal;
}

}  // namespace reindexer
