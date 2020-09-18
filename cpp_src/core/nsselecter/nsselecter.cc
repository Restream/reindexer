#include "nsselecter.h"
#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "explaincalc.h"
#include "itemcomparator.h"
#include "querypreprocessor.h"
#include "sortexpression.h"
#include "tools/logger.h"

constexpr int kMinIterationsForInnerJoinOptimization = 100;
constexpr int kMaxIterationsForIdsetPreresult = 10000;

namespace reindexer {

static int GetMaxIterations(const SelectIteratorContainer &iterators, bool withZero = false) {
	int maxIterations = std::numeric_limits<int>::max();
	iterators.ForEachIterator([&maxIterations, withZero](const SelectIterator &it) {
		int cur = it.GetMaxIterations();
		if (it.comparators_.empty() && (cur || withZero) && cur < maxIterations) maxIterations = cur;
	});
	return maxIterations;
}

void NsSelecter::operator()(QueryResults &result, SelectCtx &ctx, const RdxContext &rdxCtx) {
	const size_t resultInitSize = result.Count();
	ctx.sortingContext.enableSortOrders = ns_->sortOrdersBuilt_;
	if (ns_->config_.logLevel > ctx.query.debugLevel) {
		const_cast<Query *>(&ctx.query)->debugLevel = ns_->config_.logLevel;
	}

	ExplainCalc explain(ctx.query.explain_ || ctx.query.debugLevel >= LogInfo);
	explain.StartTiming();

	bool needPutCachedTotal = false;
	bool needCalcTotal = ctx.query.calcTotal == ModeAccurateTotal;

	QueryCacheKey ckey;
	if (ctx.query.calcTotal == ModeCachedTotal) {
		ckey = QueryCacheKey{ctx.query};

		auto cached = ns_->queryCache_->Get(ckey);
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
							   ctx.query, ns_, ctx.reqMatchedOnceFlag);
	auto aggregators = getAggregators(ctx.query);
	qPreproc.AddDistinctEntries(aggregators);
	const bool aggregationsOnly = aggregators.size() > 1 || (aggregators.size() == 1 && aggregators[0].Type() != AggDistinct);
	if (!ctx.skipIndexesLookup) qPreproc.LookupQueryIndexes();

	const bool isFt = qPreproc.ContainsFullTextIndexes();
	if (!ctx.skipIndexesLookup && !isFt) qPreproc.SubstituteCompositeIndexes();
	qPreproc.ConvertWhereValues();

	if (ctx.contextCollectingMode) {
		result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, ctx.query.selectFilter_));
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
		qres.Clear();
		lctx.start = 0;
		lctx.count = UINT_MAX;
		ctx.isForceAll = isForceAll;

		// DO NOT use deducted sort order in the following cases:
		// - query contains explicity specified sort order
		// - query contains FullText query.
		const bool disableOptimizeSortOrder = isFt || !ctx.query.sortingEntries_.empty() || ctx.preResult;
		SortingEntries sortBy = disableOptimizeSortOrder ? ctx.query.sortingEntries_ : qPreproc.DetectOptimalSortOrder();

		if (ctx.preResult) {
			if (ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
				// all futher queries for this join SHOULD have the same enableSortOrders flag
				ctx.preResult->enableSortOrders = ctx.sortingContext.enableSortOrders;
			} else {
				// If in current join query sort orders are disabled
				// then preResult query also SHOULD have disabled flag
				// If assert fails, then possible query has unlock ns
				// or ns->sortOrdersFlag_ has been reseted under read lock!
				if (!ctx.sortingContext.enableSortOrders) assert(!ctx.preResult->enableSortOrders);
				ctx.sortingContext.enableSortOrders = ctx.preResult->enableSortOrders;
			}
		}

		// Prepare sorting context
		ctx.sortingContext.forcedMode = qPreproc.ContainsForcedSortOrder();
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
					res.push_back(SingleSelectKeyResult(ctx.preResult->ids));
					static string pr = "-preresult";
					qres.Append(OpAnd, SelectIterator(res, false, pr));
				} break;
				case JoinPreResult::ModeIterators:
					qres.LazyAppend(ctx.preResult->iterators.begin(), ctx.preResult->iterators.end());
					break;
				default:
					assert(0);
			}
		}

		// Prepare data for select functions
		if (ctx.functions) {
			fnc_ = ctx.functions->AddNamespace(ctx.query, *ns_, isFt);
		}
		explain.AddPrepareTime();

		qres.PrepareIteratorsForSelectLoop(qPreproc.GetQueryEntries(), 0, qPreproc.Size(), ctx.query.equalPositions_,
										   ctx.sortingContext.sortId(), isFt, *ns_, fnc_, ft_ctx_, rdxCtx);

		explain.AddSelectTime();

		int maxIterations = GetMaxIterations(qres);
		if (ctx.preResult) {
			if (ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
				// Building pre result for next joins

				static_assert(kMaxIterationsForIdsetPreresult > JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization(), "");
				if (ctx.preResult->enableStoredValues &&
					GetMaxIterations(qres, true) <= JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization()) {
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
					if (qres.GetOperation(i) == OpAnd && qres.IsValue(i) &&
						(qres.Next(i) >= size || qres.GetOperation(qres.Next(i)) != OpOr)) {
						const SelectIterator &selectIter = qres[i];
						if (selectIter.empty() && selectIter.comparators_.empty() && selectIter.joinIndexes.size() == 1) {
							assert(ctx.joinedSelectors && ctx.joinedSelectors->size() > size_t(selectIter.joinIndexes[0]));
							(*ctx.joinedSelectors)[selectIter.joinIndexes[0]].AppendSelectIteratorOfJoinIndexData(
								qres, &maxIterations, ctx.sortingContext.sortId(), fnc_, rdxCtx);
						}
					}
				}
			}
		}

		bool reverse = !isFt && ctx.sortingContext.sortIndex() && ctx.sortingContext.entries[0].data->desc;

		bool hasComparators = false;
		qres.ForEachIterator([&hasComparators](const SelectIterator &it) {
			if (it.comparators_.size()) hasComparators = true;
		});

		if (!isFt && !qres.HasIdsets()) {
			SelectKeyResult scan;
			if (ctx.sortingContext.isOptimizationEnabled()) {
				auto it = ns_->indexes_[ctx.sortingContext.uncommitedIndex]->CreateIterator();
				it->SetMaxIterations(ns_->items_.size());
				scan.push_back(SingleSelectKeyResult(it));
				maxIterations = ns_->items_.size();
			} else {
				// special case - no idset in query
				IdType limit = ns_->items_.size();
				if (ctx.sortingContext.isIndexOrdered() && ctx.sortingContext.enableSortOrders) {
					Index *index = ctx.sortingContext.sortIndex();
					assert(index);
					limit = index->SortOrders().size();
				}
				scan.push_back(SingleSelectKeyResult(0, limit));
				maxIterations = limit;
			}
			qres.AppendFront(OpAnd, SelectIterator{scan, false, "-scan", true});
		}
		// Get maximum iterations count, for right calculation comparators costs
		qres.SortByCost(maxIterations);

		// Check idset must be 1st
		qres.CheckFirstQuery();

		// Rewing all results iterators
		qres.ForEachIterator([reverse](SelectIterator &it) { it.Start(reverse); });

		// Let iterators choose most effecive algorith
		assert(qres.Size());
		qres.SetExpectMaxIterations(maxIterations);

		explain.AddPostprocessTime();

		// do not calc total by loop, if we have only 1 condition with 1 idset
		lctx.calcTotal = needCalcTotal && (hasComparators || qPreproc.MoreThanOneEvaluation() || qres.Size() > 1 || qres[0].size() > 1);

		if (reverse && hasComparators && aggregationsOnly) selectLoop<true, true, true>(lctx, result, rdxCtx);
		if (!reverse && hasComparators && aggregationsOnly) selectLoop<false, true, true>(lctx, result, rdxCtx);
		if (reverse && !hasComparators && aggregationsOnly) selectLoop<true, false, true>(lctx, result, rdxCtx);
		if (!reverse && !hasComparators && aggregationsOnly) selectLoop<false, false, true>(lctx, result, rdxCtx);
		if (reverse && hasComparators && !aggregationsOnly) selectLoop<true, true, false>(lctx, result, rdxCtx);
		if (!reverse && hasComparators && !aggregationsOnly) selectLoop<false, true, false>(lctx, result, rdxCtx);
		if (reverse && !hasComparators && !aggregationsOnly) selectLoop<true, false, false>(lctx, result, rdxCtx);
		if (!reverse && !hasComparators && !aggregationsOnly) selectLoop<false, false, false>(lctx, result, rdxCtx);

		// Get total count for simple query with 1 condition and 1 idset
		if (needCalcTotal && !lctx.calcTotal) {
			if (!ctx.query.entries.Empty()) {
				result.totalCount = qres[0].GetMaxIterations();
			} else {
				result.totalCount = ns_->items_.size() - ns_->free_.size();
			}
		}
		explain.AddLoopTime();
		explain.AddIterations(maxIterations);
	} while (qPreproc.NeedNextEvaluation(lctx.start, lctx.count, ctx.matchedAtLeastOnce));

	processLeftJoins(result, ctx, resultInitSize);
	for (size_t i = resultInitSize; i < result.Items().size(); ++i) {
		auto &iref = result.Items()[i];
		if (!iref.ValueInitialized()) iref.SetValue(ns_->items_[iref.Id()]);
	}
	for (auto &aggregator : aggregators) {
		result.aggregationResults.push_back(aggregator.GetResult());
	}

	explain.StopTiming();
	explain.SetSortOptimization(ctx.sortingContext.isOptimizationEnabled());
	explain.PutSortIndex(ctx.sortingContext.sortIndex() ? ctx.sortingContext.sortIndex()->Name() : "-"_sv);
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
	if (ctx.query.debugLevel >= LogTrace) result.Dump();

	if (needPutCachedTotal) {
		logPrintf(LogTrace, "[%s] put totalCount value into query cache: %d ", ns_->name_, result.totalCount);
		ns_->queryCache_->Put(ckey, {static_cast<size_t>(result.totalCount)});
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
				assert(0);
		}
		ctx.preResult->executionMode = JoinPreResult::ModeExecute;
	}
}

template <typename It>
const PayloadValue &getValue(const ItemRef &itemRef, const vector<PayloadValue> &items);

template <>
const PayloadValue &getValue<ItemRefVector::iterator>(const ItemRef &itemRef, const vector<PayloadValue> &items) {
	return items[itemRef.Id()];
}

template <>
const PayloadValue &getValue<JoinPreResult::Values::iterator>(const ItemRef &itemRef, const vector<PayloadValue> &) {
	return itemRef.Value();
}

template <bool desc, bool multiColumnSort, typename It>
It NsSelecter::applyForcedSort(It begin, It end, const ItemComparator &compare, const SelectCtx &ctx) {
	assert(!ctx.query.sortingEntries_.empty());
	assert(!ctx.sortingContext.entries.empty());
	if (ctx.sortingContext.entries[0].expression != SortingContext::Entry::NoExpression) {
		throw Error(errLogic, "Force sort could not be performed by expression.");
	}
	if (ctx.query.mergeQueries_.size() > 1) throw Error(errLogic, "Force sort could not be applied to 'merged' queries.");

	auto payloadType = ns_->payloadType_;
	const string &fieldName = ctx.query.sortingEntries_[0].expression;

	int idx = ns_->getIndexByName(fieldName);

	if (ns_->indexes_[idx]->Opts().IsArray()) throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");

	ItemRefVector::difference_type cost = 0;
	KeyValueType fieldType = ns_->indexes_[idx]->KeyType();

	if (idx < ns_->indexes_.firstCompositePos()) {
		// implementation for regular indexes
		fast_hash_map<Variant, ItemRefVector::difference_type> sortMap;
		for (auto value : ctx.query.forcedSortOrder_) {
			value.convert(fieldType);
			sortMap.insert({value, cost});
			cost++;
		}

		VariantArray keyRefs;
		const auto boundary = std::stable_partition(begin, end, [&sortMap, &payloadType, idx, &keyRefs, this](ItemRef &itemRef) {
			ConstPayload(payloadType, getValue<It>(itemRef, ns_->items_)).Get(idx, keyRefs);
			if (desc) {
				return keyRefs.empty() || (sortMap.find(keyRefs[0]) == sortMap.end());
			} else {
				return !keyRefs.empty() && (sortMap.find(keyRefs[0]) != sortMap.end());
			}
		});

		VariantArray lhsItemValue;
		VariantArray rhsItemValue;
		It from, to;
		if (desc) {
			from = boundary;
			to = end;
		} else {
			from = begin;
			to = boundary;
		}
		std::sort(from, to,
				  [&sortMap, &payloadType, idx, &lhsItemValue, &rhsItemValue, &compare, this](const ItemRef &lhs, const ItemRef &rhs) {
					  ConstPayload(payloadType, getValue<It>(lhs, ns_->items_)).Get(idx, lhsItemValue);
					  assertf(!lhsItemValue.empty(), "Item lost in query results%s", "");
					  assertf(sortMap.find(lhsItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");

					  ConstPayload(payloadType, getValue<It>(rhs, ns_->items_)).Get(idx, rhsItemValue);
					  assertf(sortMap.find(rhsItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");
					  assertf(!rhsItemValue.empty(), "Item lost in query results%s", "");

					  const auto lhsPos = sortMap.find(lhsItemValue[0])->second;
					  const auto rhsPos = sortMap.find(rhsItemValue[0])->second;
					  if (lhsPos == rhsPos) {
						  if (multiColumnSort) {
							  return compare(lhs, rhs);
						  } else {
							  if (desc) {
								  return lhs.Id() > rhs.Id();
							  } else {
								  return lhs.Id() < rhs.Id();
							  }
						  }
					  } else {
						  if (desc) {
							  return lhsPos > rhsPos;
						  } else {
							  return lhsPos < rhsPos;
						  }
					  }
				  });
		return boundary;
	} else {
		// implementation for composite indexes
		FieldsSet fields = ns_->indexes_[idx]->Fields();

		unordered_payload_map<ItemRefVector::difference_type, false> sortMap(0, payloadType, fields, CollateOpts{});

		for (auto value : ctx.query.forcedSortOrder_) {
			value.convert(fieldType, &payloadType, &fields);
			sortMap.insert({static_cast<const PayloadValue &>(value), cost});
			cost++;
		}

		const auto boundary = std::stable_partition(begin, end, [&sortMap, this](ItemRef &itemRef) {
			if (desc) {
				return (sortMap.find(getValue<It>(itemRef, ns_->items_)) == sortMap.end());
			} else {
				return (sortMap.find(getValue<It>(itemRef, ns_->items_)) != sortMap.end());
			}
		});

		It from, to;
		if (desc) {
			from = boundary;
			to = end;
		} else {
			from = begin;
			to = boundary;
		}
		std::sort(from, to, [&sortMap, &compare, this](const ItemRef &lhs, const ItemRef &rhs) {
			const auto lhsPos = sortMap.find(getValue<It>(lhs, ns_->items_))->second;
			const auto rhsPos = sortMap.find(getValue<It>(rhs, ns_->items_))->second;
			if (lhsPos == rhsPos) {
				if (multiColumnSort) {
					return compare(lhs, rhs);
				} else {
					if (desc) {
						return lhs.Id() > rhs.Id();
					} else {
						return lhs.Id() < rhs.Id();
					}
				}
			} else {
				if (desc) {
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

	std::partial_sort(itFirst, itLast, itEnd, comparator);
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

void NsSelecter::processLeftJoins(QueryResults &qr, SelectCtx &sctx, size_t startPos) {
	if (!checkIfThereAreLeftJoins(sctx)) return;
	for (size_t i = startPos; i < qr.Count(); ++i) {
		IdType rowid = qr[i].GetItemRef().Id();
		ConstPayload pl(ns_->payloadType_, ns_->items_[rowid]);
		for (auto &joinedSelector : *sctx.joinedSelectors)
			if (joinedSelector.Type() == JoinType::LeftJoin) joinedSelector.Process(rowid, sctx.nsid, pl, true);
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
void NsSelecter::sortResults(LoopCtx &ctx, It begin, It end, const SortingOptions &sortingOptions) {
	SelectCtx &sctx = ctx.sctx;
	ctx.explain.StartSort();
#ifndef NDEBUG
	for (const auto &eR : sctx.sortingContext.exprResults) {
		assert(eR.size() == end - begin);
	}
#endif

	ItemComparatorState comparatorState;
	ItemComparator comparator{*ns_, sctx, comparatorState};
	if (sortingOptions.forcedMode) {
		comparator.BindForForcedSort();
		assert(!sctx.query.sortingEntries_.empty());
		if (sctx.query.sortingEntries_[0].desc) {
			if (sctx.sortingContext.entries.size() > 1) {
				end = applyForcedSort<true, true>(begin, end, comparator, sctx);
			} else {
				end = applyForcedSort<true, false>(begin, end, comparator, sctx);
			}
		} else {
			if (sctx.sortingContext.entries.size() > 1) {
				begin = applyForcedSort<false, true>(begin, end, comparator, sctx);
			} else {
				begin = applyForcedSort<false, false>(begin, end, comparator, sctx);
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

template <bool reverse, bool hasComparators, bool aggregationsOnly>
void NsSelecter::selectLoop(LoopCtx &ctx, QueryResults &result, const RdxContext &rdxCtx) {
	static const JoinedSelectors emptyJoinedSelectors;
	const auto selectLoopWard = rdxCtx.BeforeSelectLoop();
	SelectCtx &sctx = ctx.sctx;
	const auto &joinedSelectors = sctx.joinedSelectors ? *sctx.joinedSelectors : emptyJoinedSelectors;
	SelectIteratorContainer &qres = ctx.qres;
	const size_t initCount =
		(sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) ? sctx.preResult->values.size() : result.Count();

	if (!sctx.isForceAll) {
		ctx.start = ctx.qPreproc.Start();
		ctx.count = ctx.qPreproc.Count();
	}

	// reserve queryresults, if we have only 1 condition with 1 idset
	if (qres.Size() == 1 && qres.IsIterator(0) && qres[0].size() == 1) {
		unsigned reserve = std::min(unsigned(qres[0].GetMaxIterations()), ctx.count);
		if (sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) {
			sctx.preResult->values.reserve(initCount + reserve);
		} else {
			result.Items().reserve(initCount + reserve);
		}
	}

	bool finish = (ctx.count == 0) && !sctx.reqMatchedOnceFlag && !ctx.calcTotal && !sctx.matchedAtLeastOnce;

	SortingOptions sortingOptions(sctx.sortingContext);
	const Index *const firstSortIndex = sctx.sortingContext.sortIndexIfOrdered();
	bool multiSortFinished = !(sortingOptions.multiColumnByBtreeIndex && ctx.count > 0);

	VariantArray prevValues;
	size_t multisortLimitLeft = 0;

	// TODO: nested conditions support. Like (A  OR B OR C) AND (X OR Z)
	assert(!qres.Empty());
	assert(qres.IsIterator(0));
	SelectIterator &firstIterator = qres.begin()->Value();
	IdType rowId = firstIterator.Val();
	while (firstIterator.Next(rowId) && !finish) {
		rowId = firstIterator.Val();
		IdType properRowId = rowId;

		if (firstSortIndex) {
			assertf(firstSortIndex->SortOrders().size() > static_cast<size_t>(rowId),
					"FirstIterator: %s, firstSortIndex: %s, firstSortIndex size: %d, rowId: %d", firstIterator.name.c_str(),
					firstSortIndex->Name().c_str(), static_cast<int>(firstSortIndex->SortOrders().size()), rowId);
			properRowId = firstSortIndex->SortOrders()[rowId];
		}

		assert(static_cast<size_t>(properRowId) < ns_->items_.size());
		PayloadValue &pv = ns_->items_[properRowId];
		if (pv.IsFree()) continue;
		assert(pv.Ptr());
		if (qres.Process<reverse, hasComparators>(pv, &finish, &rowId, properRowId, !ctx.start && ctx.count)) {
			sctx.matchedAtLeastOnce = true;
			uint8_t proc = ft_ctx_ ? ft_ctx_->Proc(firstIterator.Pos()) : 0;
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			for (auto &it : qres) {
				if (it.IsLeaf() && it.Value().distinct) {
					it.Value().ExcludeLastSet(pv, rowId, properRowId);
				}
			}
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
					addSelectResult<aggregationsOnly>(proc, rowId, properRowId, sctx, ctx.aggregators, result);
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
				addSelectResult<aggregationsOnly>(proc, rowId, properRowId, sctx, ctx.aggregators, result);
				--ctx.count;
				if (!ctx.count && sortingOptions.multiColumn && !multiSortFinished)
					getSortIndexValue(sctx.sortingContext, properRowId, prevValues, proc, result.joined_[sctx.nsid], joinedSelectors);
			}
			if (!ctx.count && !ctx.calcTotal && multiSortFinished) break;
			if (ctx.calcTotal) result.totalCount++;
		}
	}

	if (sctx.isForceAll) {
		assert(!ctx.qPreproc.Start() || !initCount);
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
		assert(sctx.preResult->executionMode == JoinPreResult::ModeBuild);
		sortResults(ctx, sctx.preResult->values.begin() + initCount, sctx.preResult->values.end(), sortingOptions);
	} else if (sortingOptions.postLoopSortingRequired()) {
		const size_t offset = sctx.isForceAll ? ctx.qPreproc.Start() : multisortLimitLeft;
		if (result.Items().size() > offset) {
			sortResults(ctx, result.Items().begin() + initCount, result.Items().end(), sortingOptions);
		}
		setLimitAndOffset(result.Items(), offset, ctx.qPreproc.Count() + initCount);
	}

	if (sctx.isForceAll) {
		const size_t countChange =
			((sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) ? sctx.preResult->values.size()
																					   : result.Items().size()) -
			initCount;
		assert(countChange <= ctx.qPreproc.Count());
		ctx.count = ctx.qPreproc.Count() - countChange;
	}
}

void NsSelecter::getSortIndexValue(const SortingContext &sortCtx, IdType rowId, VariantArray &value, uint8_t proc,
								   joins::NamespaceResults &joinResults, const JoinedSelectors &js) {
	const SortingContext::Entry *firstEntry = sortCtx.getFirstColumnEntry();
	ConstPayload pv(ns_->payloadType_, ns_->items_[rowId]);
	if (firstEntry->expression != SortingContext::Entry::NoExpression) {
		assert(firstEntry->expression >= 0 && static_cast<size_t>(firstEntry->expression) < sortCtx.expressions.size());
		value = VariantArray{
			Variant{sortCtx.expressions[firstEntry->expression].Calculate(rowId, pv, joinResults, js, proc, ns_->tagsMatcher_)}};
	} else if ((firstEntry->data->index == IndexValueType::SetByJsonPath) || ns_->indexes_[firstEntry->data->index]->Opts().IsSparse()) {
		pv.GetByJsonPath(firstEntry->data->expression, ns_->tagsMatcher_, value, KeyValueUndefined);
	} else {
		pv.Get(firstEntry->data->index, value);
	}
}

template <bool aggregationsOnly>
void NsSelecter::addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
								 QueryResults &result) {
	static const JoinedSelectors emptyJoinedSelectors;
	for (auto &aggregator : aggregators) aggregator.Aggregate(ns_->items_[properRowId]);
	if (aggregationsOnly) return;
	if (sctx.preResult && sctx.preResult->executionMode == JoinPreResult::ModeBuild) {
		switch (sctx.preResult->dataMode) {
			case JoinPreResult::ModeIdSet:
				sctx.preResult->ids.Add(rowId, IdSet::Unordered, 0);
				break;
			case JoinPreResult::ModeValues:
				sctx.preResult->values.push_back({properRowId, ns_->items_[properRowId], proc, sctx.nsid});
				break;
			default:
				assert(0);
		}
	} else {
		unsigned exprResultIdx = 0u;
		if (!sctx.sortingContext.expressions.empty()) {
			const auto &exprs = sctx.sortingContext.expressions;
			auto &exprResults = sctx.sortingContext.exprResults;
			assert(exprs.size() == exprResults.size());
			exprResultIdx = exprResults[0].size();
			const ConstPayload pv(ns_->payloadType_, ns_->items_[properRowId]);
			const auto &joinedSelectors = sctx.joinedSelectors ? *sctx.joinedSelectors : emptyJoinedSelectors;
			for (size_t i = 0; i < exprs.size(); ++i) {
				exprResults[i].push_back(
					exprs[i].Calculate(rowId, pv, result.joined_[sctx.nsid], joinedSelectors, proc, ns_->tagsMatcher_));
			}
		}
		result.Add({properRowId, exprResultIdx, proc, sctx.nsid}, ns_->payloadType_);

		const int kLimitItems = 10000000;
		size_t sz = result.Count();
		if (sz >= kLimitItems && !(sz % kLimitItems)) {
			logPrintf(LogWarning, "Too big query results ns='%s',count='%d',rowId='%d',q='%s'", ns_->name_, sz, properRowId,
					  sctx.query.GetSQL());
		}
	}
}

h_vector<Aggregator, 4> NsSelecter::getAggregators(const Query &q) const {
	static constexpr int NotFilled = -2;
	h_vector<Aggregator, 4> ret;
	h_vector<size_t, 4> distinctIndexes;

	for (auto &ag : q.aggregations_) {
		bool compositeIndexFields = false;
		if (ag.fields_.empty()) {
			throw Error(errQueryExec, "Empty set of fields for aggregation %s", AggregationResult::aggTypeToStr(ag.type_));
		}
		if (ag.type_ != AggFacet) {
			if (ag.fields_.size() != 1) {
				throw Error(errQueryExec, "For aggregation %s is available exactly one field", AggregationResult::aggTypeToStr(ag.type_));
			}
			if (!ag.sortingEntries_.empty()) {
				throw Error(errQueryExec, "Sort is not available for aggregation %s", AggregationResult::aggTypeToStr(ag.type_));
			}
			if (ag.limit_ != UINT_MAX || ag.offset_ != 0) {
				throw Error(errQueryExec, "Limit or offset are not available for aggregation %s",
							AggregationResult::aggTypeToStr(ag.type_));
			}
		}
		FieldsSet fields;
		h_vector<Aggregator::SortingEntry, 1> sortingEntries(ag.sortingEntries_.size());
		for (size_t i = 0; i < sortingEntries.size(); ++i) {
			sortingEntries[i] = {(iequals("count"_sv, ag.sortingEntries_[i].expression) ? Aggregator::SortingEntry::Count : NotFilled),
								 ag.sortingEntries_[i].desc};
		}
		int idx = -1;
		for (size_t i = 0; i < ag.fields_.size(); ++i) {
			for (size_t j = 0; j < sortingEntries.size(); ++j) {
				if (iequals(ag.fields_[i], ag.sortingEntries_[j].expression)) {
					sortingEntries[j].field = i;
				}
			}
			if (ns_->getIndexByName(ag.fields_[i], idx)) {
				if (ns_->indexes_[idx]->Opts().IsSparse()) {
					fields.push_back(ns_->indexes_[idx]->Fields().getTagsPath(0));
				} else if (ag.type_ == AggFacet && ag.fields_.size() > 1 && ns_->indexes_[idx]->Opts().IsArray()) {
					throw Error(errQueryExec, "Multifield facet cannot contain an array field");
				} else if (ag.type_ == AggDistinct && isComposite(ns_->indexes_[idx]->Type())) {
					fields = ns_->indexes_[idx]->Fields();
					compositeIndexFields = true;
				}

				else {
					fields.push_back(idx);
				}
			} else {
				fields.push_back(ns_->tagsMatcher_.path2tag(ag.fields_[i]));
			}
		}
		for (size_t i = 0; i < sortingEntries.size(); ++i) {
			if (sortingEntries[i].field == NotFilled) {
				throw Error(errQueryExec, "The aggregation %s cannot provide sort by '%s'", AggregationResult::aggTypeToStr(ag.type_),
							ag.sortingEntries_[i].expression);
			}
		}
		if (ag.type_ == AggDistinct) distinctIndexes.push_back(ret.size());
		ret.emplace_back(ns_->payloadType_, fields, ag.type_, ag.fields_, sortingEntries, ag.limit_, ag.offset_, compositeIndexFields);
	}

	if (distinctIndexes.size() <= 1) return ret;
	for (const Aggregator &agg : ret) {
		if (agg.Type() == AggDistinct) continue;
		for (const string &name : agg.Names()) {
			if (std::find_if(distinctIndexes.cbegin(), distinctIndexes.cend(),
							 [&ret, &name](size_t idx) { return ret[idx].Names()[0] == name; }) == distinctIndexes.cend()) {
				throw Error(errQueryExec, "Cannot be combined several distincts and non distinct aggregator on index %s", name);
			}
		}
	}

	return ret;
}

void NsSelecter::prepareSortingContext(SortingEntries &sortBy, SelectCtx &ctx, bool isFt, bool availableSelectBySortIndex) {
	auto strictMode = ctx.query.strictMode == StrictModeNotSet ? ns_->config_.strictMode : ctx.query.strictMode;
	auto validateField = [strictMode](string_view name, const std::string &nsName, const TagsMatcher &tagsMatcher) {
		if (strictMode == StrictModeIndexes) {
			throw Error(errParams,
						"Current query strict mode allows sort by index fields only. There are no indexes with name '%s' in namespace '%s'",
						name, nsName);
		}
		if (tagsMatcher.path2tag(name).empty()) {
			if (strictMode == StrictModeNames) {
				throw Error(
					errParams,
					"Current query strict mode allows sort by existing fields only. There are no fields with name '%s' in namespace '%s'",
					name, nsName);
			}
			return false;
		}
		return true;
	};
	static const JoinedSelectors emptyJoinedSelectors;
	const auto &joinedSelectors = ctx.joinedSelectors ? *ctx.joinedSelectors : emptyJoinedSelectors;
	ctx.sortingContext.entries.clear();
	ctx.sortingContext.expressions.clear();
	for (size_t i = 0; i < sortBy.size(); ++i) {
		SortingEntry &sortingEntry(sortBy[i]);
		SortingContext::Entry sortingCtx;
		sortingCtx.data = &sortingEntry;
		assert(!sortingEntry.expression.empty());
		SortExpression expr{SortExpression::Parse(sortingEntry.expression, joinedSelectors)};
		bool skipSortingEntry = false;
		if (expr.ByIndexField()) {
			sortingEntry.index = IndexValueType::SetByJsonPath;
			ns_->getIndexByName(sortingEntry.expression, sortingEntry.index);
			if (sortingEntry.index >= 0) {
				Index *sortIndex = ns_->indexes_[sortingEntry.index].get();
				sortingCtx.index = sortIndex;
				sortingCtx.opts = &sortIndex->Opts().collateOpts_;

				if (i == 0) {
					if (sortIndex->IsOrdered() && !ctx.sortingContext.enableSortOrders && availableSelectBySortIndex) {
						ctx.sortingContext.uncommitedIndex = sortingEntry.index;
						ctx.isForceAll = ctx.sortingContext.forcedMode;
					} else if (!sortIndex->IsOrdered() || isFt || !ctx.sortingContext.enableSortOrders || !availableSelectBySortIndex) {
						ctx.isForceAll = true;
						sortingCtx.index = nullptr;
					}
				}
			} else if (sortingEntry.index == IndexValueType::SetByJsonPath) {
				if (!validateField(sortingEntry.expression, ns_->name_, ns_->tagsMatcher_)) {
					continue;
				}
				ctx.isForceAll = true;
			} else {
				std::abort();
			}
		} else {
			if (!ctx.query.mergeQueries_.empty()) {
				throw Error(errLogic, "Sorting by expression cannot be applied to merged queries.");
			}
			struct {
				NsSelecter &selecter;
				bool &skipSortingEntry;
				std::function<bool(string_view, const string &, const TagsMatcher &)> validateField;
			} lCtx = {*this, skipSortingEntry, validateField};

			expr.ExecuteAppropriateForEach<SortExpressionIndex, SortExpressionJoinedIndex, SortExpressionFuncRank>(
				[&lCtx](SortExpressionIndex &exprIndex) {
					assert(!exprIndex.column.empty());
					exprIndex.index = IndexValueType::SetByJsonPath;
					if (lCtx.selecter.ns_->getIndexByName(string{exprIndex.column}, exprIndex.index) &&
						lCtx.selecter.ns_->indexes_[exprIndex.index]->Opts().IsSparse()) {
						exprIndex.index = IndexValueType::SetByJsonPath;
					}
					if (exprIndex.index == IndexValueType::SetByJsonPath) {
						lCtx.skipSortingEntry |=
							!lCtx.validateField(exprIndex.column, lCtx.selecter.ns_->name_, lCtx.selecter.ns_->tagsMatcher_);
					}
				},
				[&joinedSelectors, &lCtx](SortExpressionJoinedIndex &exprIndex) {
					assert(!exprIndex.column.empty());
					exprIndex.index = IndexValueType::SetByJsonPath;
					const auto &js = joinedSelectors[exprIndex.fieldIdx];
					(js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.payloadType : js.rightNs_->payloadType_)
						.FieldByName(string{exprIndex.column}, exprIndex.index);
					if (exprIndex.index == IndexValueType::SetByJsonPath) {
						lCtx.skipSortingEntry |=
							!lCtx.validateField(exprIndex.column, js.joinQuery_._namespace,
												js.preResult_->dataMode == JoinPreResult::ModeValues ? js.preResult_->values.tagsMatcher
																									 : js.rightNs_->tagsMatcher_);
					}
				},
				[isFt](SortExpressionFuncRank &) {
					if (!isFt) throw Error(errLogic, "Sort by rank() is available only for fulltext query");
				});
			if (skipSortingEntry) {
				continue;
			}
			ctx.sortingContext.expressions.emplace_back(std::move(expr));
			sortingCtx.expression = ctx.sortingContext.expressions.size() - 1;
			ctx.isForceAll = true;
		}
		ctx.sortingContext.entries.emplace_back(std::move(sortingCtx));
	}
	ctx.sortingContext.exprResults.clear();
	ctx.sortingContext.exprResults.resize(ctx.sortingContext.expressions.size());
}

bool NsSelecter::isSortOptimizatonEffective(const QueryEntries &qentries, SelectCtx &ctx, const RdxContext &rdxCtx) {
	if (qentries.Size() == 0 || (qentries.Size() == 1 && qentries.IsValue(0) && qentries[0].idxNo == ctx.sortingContext.uncommitedIndex))
		return true;

	size_t costNormal = ns_->items_.size() - ns_->free_.size();

	qentries.ForEachEntry([this, &ctx, &rdxCtx, &costNormal](const QueryEntry &qe) {
		if (qe.idxNo < 0 || qe.idxNo == ctx.sortingContext.uncommitedIndex) return;
		if (costNormal == 0) return;

		auto &index = ns_->indexes_[qe.idxNo];
		if (isFullText(index->Type())) return;

		Index::SelectOpts opts;
		opts.disableIdSetCache = 1;
		opts.itemsCountInNamespace = ns_->items_.size() - ns_->free_.size();

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

	size_t costOptimized = ns_->items_.size() - ns_->free_.size();
	costNormal *= 2;
	if (costNormal < costOptimized) {
		costOptimized = costNormal + 1;
		qentries.ForEachEntry([this, &ctx, &rdxCtx, &costOptimized](const QueryEntry &qe) {
			if (qe.idxNo < 0 || qe.idxNo != ctx.sortingContext.uncommitedIndex) return;

			Index::SelectOpts opts;
			opts.itemsCountInNamespace = ns_->items_.size() - ns_->free_.size();
			opts.disableIdSetCache = 1;
			opts.unbuiltSortOrders = 1;

			try {
				SelectKeyResults reslts = ns_->indexes_[qe.idxNo]->SelectKey(qe.values, qe.condition, 0, opts, nullptr, rdxCtx);
				for (const SelectKeyResult &res : reslts) {
					if (res.comparators_.empty()) {
						costOptimized = std::min(costOptimized, res.GetMaxIterations(costOptimized));
					}
				}
			} catch (const Error &) {
			}
		});
	}

	return costOptimized <= costNormal;
}

}  // namespace reindexer
