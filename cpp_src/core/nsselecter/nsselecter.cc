#include "nsselecter.h"
#include "core/namespace.h"
#include "explaincalc.h"
#include "querypreprocessor.h"

constexpr int kMinIterationsForInnerJoinOptimization = 100;
constexpr int kMaxIterationsForIdsetPreresult = 10000;

namespace reindexer {

static int GetMaxIterations(const SelectIteratorContainer &iterators) {
	int maxIterations = std::numeric_limits<int>::max();
	iterators.ForEachIterator([&maxIterations](const SelectIterator &it, OpType) {
		int cur = it.GetMaxIterations();
		if (it.comparators_.empty() && cur && cur < maxIterations) maxIterations = cur;
	});
	return maxIterations;
}

void NsSelecter::operator()(QueryResults &result, SelectCtx &ctx, const RdxContext &rdxCtx) {
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

	QueryPreprocessor qPreproc(ns_, const_cast<QueryEntries *>(&ctx.query.entries));
	QueryEntries tmpWhereEntries(ctx.skipIndexesLookup ? QueryEntries() : qPreproc.LookupQueryIndexes());
	if (!ctx.skipIndexesLookup) {
		qPreproc.SetQueryEntries(&tmpWhereEntries);
	}

	const bool isFt = qPreproc.ContainsFullTextIndexes();
	if (!ctx.skipIndexesLookup && !isFt) qPreproc.SubstituteCompositeIndexes();
	qPreproc.ConvertWhereValues();

	// DO NOT use deducted sort order in the following cases:
	// - query contains explicity specified sort order
	// - query contains FullText query.
	bool disableOptimizeSortOrder = !ctx.query.sortingEntries_.empty() || ctx.preResult;
	SortingEntries sortBy = (isFt || disableOptimizeSortOrder) ? ctx.query.sortingEntries_ : qPreproc.DetectOptimalSortOrder();

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
	prepareSortingContext(sortBy, ctx, isFt);

	if (ctx.sortingContext.isOptimizationEnabled()) {
		// Unbuilt btree index optimization is available for query with
		// Check, is it really possible to use it

		if (isFt ||																	 // Disabled if there are search results
			(ctx.preResult /*&& !ctx.preResult->btreeIndexOptimizationEnabled*/) ||  // Disabled in join preresult (TMP: now disable for all
																					 // right queries), TODO: enable right queries)
			(!tmpWhereEntries.Empty() && tmpWhereEntries.GetOperation(0) == OpNot) ||  // Not in first condition
			!isSortOptimizatonEffective(tmpWhereEntries, ctx,
										rdxCtx)  // Optimization is not effective (e.g. query contains more effecive filters)
		) {
			ctx.sortingContext.resetOptimization();
			ctx.isForceAll = true;
		}

	} else if (ctx.preResult && (ctx.preResult->executionMode == JoinPreResult::ModeBuild)) {
		ctx.preResult->btreeIndexOptimizationEnabled = false;
	}

	// Add preresults with common conditions of join Queries
	SelectIteratorContainer qres(ns_->payloadType_, &ctx);
	if (ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeExecute) {
		switch (ctx.preResult->dataMode) {
			case JoinPreResult::ModeIdSet: {
				SelectKeyResult res;
				res.push_back(SingleSelectKeyResult(ctx.preResult->ids));
				static string pr = "-preresult";
				qres.Append(OpAnd, SelectIterator(res, false, pr));
			} break;
			case JoinPreResult::ModeIterators:
				qres.Append(ctx.preResult->iterators.cbegin(), ctx.preResult->iterators.cend());
				break;
			default:
				assert(0);
		}
	}

	// Prepare data for select functions
	if (ctx.functions) {
		fnc_ = ctx.functions->AddNamespace(ctx.query, *ns_, isFt);
	}
	explain.SetPrepareTime();

	qres.PrepareIteratorsForSelectLoop(qPreproc.GetQueryEntries(), 0, qPreproc.GetQueryEntries().Size(), ctx.query.equalPositions_,
									   ctx.sortingContext.sortId(), isFt, *ns_, fnc_, ft_ctx_, rdxCtx);

	explain.SetSelectTime();

	int maxIterations = GetMaxIterations(qres);
	if (ctx.preResult) {
		if (ctx.preResult->executionMode == JoinPreResult::ModeBuild) {
			// Building pre result for next joins

			static_assert(kMaxIterationsForIdsetPreresult > JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization(), "");
			// Return preResult as QueryIterators if:
			if (maxIterations >= kMaxIterationsForIdsetPreresult ||  // 1. We have > QueryIterator which expects more than 10000 iterations.
				(ctx.sortingContext.entries.size() && !ctx.sortingContext.sortIndex())  // 2. We have sorted query, by unordered index
				|| ctx.preResult->btreeIndexOptimizationEnabled) {						// 3. We have btree-index that is not committed yet
				ctx.preResult->iterators.Append(qres.cbegin(), qres.cend());
				if (ctx.query.debugLevel >= LogInfo) {
					logPrintf(LogInfo, "Built preResult (expected %d iterations) with %d iterators, q='%s'", maxIterations, qres.Size(),
							  ctx.query.GetSQL());
				}

				ctx.preResult->dataMode = JoinPreResult::ModeIterators;
				ctx.preResult->executionMode = JoinPreResult::ModeExecute;
				return;
			} else if (!ctx.preResult->enableStoredValues ||
					   maxIterations > JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization()) {
				// Build preResult as single IdSet
				ctx.preResult->dataMode = JoinPreResult::ModeIdSet;
				// For building join preresult always use ASC sort orders
				for (SortingEntry &se : sortBy) se.desc = false;
			} else {
				ctx.preResult->dataMode = JoinPreResult::ModeValues;
				ctx.preResult->values.tagsMatcher = ns_->tagsMatcher_;
				ctx.preResult->values.payloadType = ns_->payloadType_;
			}
		}
	} else if (!ctx.sortingContext.isOptimizationEnabled()) {
		if (!isFt && maxIterations > kMinIterationsForInnerJoinOptimization) {
			for (size_t i = 0, size = qres.Size(); i < size; i = qres.Next(i)) {
				// for optimization use only isolated InnerJoin
				if (qres.GetOperation(i) == OpAnd && qres.IsValue(i) && (qres.Next(i) >= size || qres.GetOperation(qres.Next(i)) != OpOr)) {
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

	bool hasComparators = false;
	bool reverse = !isFt && ctx.sortingContext.sortIndex() && ctx.sortingContext.entries[0].data->desc;

	qres.ForEachIterator([&hasComparators](const SelectIterator &it, OpType) {
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

	if (ctx.contextCollectingMode) {
		result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, ctx.query.selectFilter_));
	}

	explain.SetPostprocessTime();

	LoopCtx lctx(ctx);
	lctx.qres = &qres;
	lctx.calcTotal = needCalcTotal;
	if (isFt) result.haveProcent = true;
	if (reverse && hasComparators) selectLoop<true, true>(lctx, result, rdxCtx);
	if (!reverse && hasComparators) selectLoop<false, true>(lctx, result, rdxCtx);
	if (reverse && !hasComparators) selectLoop<true, false>(lctx, result, rdxCtx);
	if (!reverse && !hasComparators) selectLoop<false, false>(lctx, result, rdxCtx);

	explain.SetLoopTime();
	explain.StopTiming();
	explain.PutSortIndex(ctx.sortingContext.sortIndex() ? ctx.sortingContext.sortIndex()->Name() : "-"_sv);
	explain.PutCount((ctx.preResult && ctx.preResult->executionMode == JoinPreResult::ModeBuild)
						 ? (ctx.preResult->dataMode == JoinPreResult::ModeIdSet ? ctx.preResult->ids.size() : ctx.preResult->values.size())
						 : result.Count());
	explain.PutSelectors(&qres);
	explain.PutJoinedSelectors(ctx.joinedSelectors);
	explain.SetIterations(maxIterations);

	if (ctx.query.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "%s", ctx.query.GetSQL());
		explain.LogDump(ctx.query.debugLevel);
	}
	if (ctx.query.explain_) {
		result.explainResults = explain.GetJSON();
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
					logPrintf(LogInfo, "Built idset preResult (expected %d iterations) with %d ids, q = '%s'", maxIterations,
							  ctx.preResult->ids.size(), ctx.query.GetSQL());
				}
				break;
			case JoinPreResult::ModeValues:
				if (ctx.query.debugLevel >= LogInfo) {
					logPrintf(LogInfo, "Built values preResult (expected %d iterations) with %d values, q = '%s'", maxIterations,
							  ctx.preResult->values.size(), ctx.query.GetSQL());
				}
				break;
			default:
				assert(0);
		}
		ctx.preResult->executionMode = JoinPreResult::ModeExecute;
	}
}

template <typename Items>
const PayloadValue &getValue(const ItemRef &itemRef, const vector<PayloadValue> &items);

template <>
const PayloadValue &getValue<ItemRefVector>(const ItemRef &itemRef, const vector<PayloadValue> &items) {
	return items[itemRef.Id()];
}

template <>
const PayloadValue &getValue<JoinPreResult::Values>(const ItemRef &itemRef, const vector<PayloadValue> &) {
	return itemRef.Value();
}

template <typename Items>
void NsSelecter::applyForcedSort(Items &items, const SelectCtx &ctx) {
	assert(!ctx.query.sortingEntries_.empty());
	assert(!ctx.sortingContext.entries.empty());
	if (ctx.sortingContext.entries[0].expression != SortingContext::Entry::NoExpression)
		throw Error(errLogic, "Force sort could not be performed by expression.");

	if (ctx.query.mergeQueries_.size() > 1) throw Error(errLogic, "Force sort could not be applied to 'merged' queries.");
	if (ctx.query.sortingEntries_[0].desc) {
		applyForcedSortDesc(items, ctx);
		return;
	}

	auto payloadType = ns_->payloadType_;
	const string &fieldName = ctx.query.sortingEntries_[0].expression;

	int idx = ns_->getIndexByName(fieldName);

	if (ns_->indexes_[idx]->Opts().IsArray()) throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");

	ItemRefVector::difference_type cost = 0;
	KeyValueType fieldType = ns_->indexes_[idx]->KeyType();

	if (idx < ns_->indexes_.firstCompositePos()) {
		// implementation for regular indexes
		fast_hash_map<Variant, ItemRefVector::difference_type> sortMap;
		for (auto value : ctx.query.forcedSortOrder) {
			value.convert(fieldType);
			sortMap.insert({value, cost});
			cost++;
		}

		VariantArray keyRefs;
		auto sortEnd = std::stable_partition(items.begin(), items.end(), [&sortMap, &payloadType, idx, &keyRefs, this](ItemRef &itemRef) {
			ConstPayload(payloadType, getValue<Items>(itemRef, ns_->items_)).Get(idx, keyRefs);
			return !keyRefs.empty() && (sortMap.find(keyRefs[0]) != sortMap.end());
		});

		VariantArray firstItemValue;
		VariantArray secondItemValue;
		std::sort(items.begin(), sortEnd,
				  [&sortMap, &payloadType, idx, &firstItemValue, &secondItemValue, this](const ItemRef &lhs, const ItemRef &rhs) {
					  ConstPayload(payloadType, getValue<Items>(lhs, ns_->items_)).Get(idx, firstItemValue);
					  assertf(!firstItemValue.empty(), "Item lost in query results%s", "");
					  assertf(sortMap.find(firstItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");

					  ConstPayload(payloadType, getValue<Items>(rhs, ns_->items_)).Get(idx, secondItemValue);
					  assertf(sortMap.find(secondItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");
					  assertf(!secondItemValue.empty(), "Item lost in query results%s", "");

					  return sortMap.find(firstItemValue[0])->second < sortMap.find(secondItemValue[0])->second;
				  });
	} else {
		// implementation for composite indexes
		FieldsSet fields = ns_->indexes_[idx]->Fields();

		unordered_payload_map<ItemRefVector::difference_type> sortMap(0, hash_composite(payloadType, fields),
																	  equal_composite(payloadType, fields));

		for (auto value : ctx.query.forcedSortOrder) {
			value.convert(fieldType, &payloadType, &fields);
			sortMap.insert({static_cast<const PayloadValue &>(value), cost});
			cost++;
		}

		auto sortEnd = std::stable_partition(items.begin(), items.end(), [&sortMap, this](ItemRef &itemRef) {
			return (sortMap.find(getValue<Items>(itemRef, ns_->items_)) != sortMap.end());
		});

		std::sort(items.begin(), sortEnd, [&sortMap, this](const ItemRef &lhs, const ItemRef &rhs) {
			return sortMap.find(getValue<Items>(lhs, ns_->items_))->second < sortMap.find(getValue<Items>(rhs, ns_->items_))->second;
		});
	}
}

template <typename Items>
void NsSelecter::applyForcedSortDesc(Items &items, const SelectCtx &ctx) {
	assert(!ctx.query.sortingEntries_.empty());

	auto payloadType = ns_->payloadType_;
	const string &fieldName = ctx.query.sortingEntries_[0].expression;

	int idx = ns_->getIndexByName(fieldName);

	if (ns_->indexes_[idx]->Opts().IsArray()) throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");

	ItemRefVector::difference_type cost = 0;
	KeyValueType fieldType = ns_->indexes_[idx]->KeyType();

	if (idx < ns_->indexes_.firstCompositePos()) {
		// implementation for regular indexes
		fast_hash_map<Variant, ItemRefVector::difference_type> sortMap;
		for (auto value : ctx.query.forcedSortOrder) {
			value.convert(fieldType);
			sortMap.insert({value, cost});
			cost++;
		}

		VariantArray keyRefs;
		auto sortBeg = std::stable_partition(items.begin(), items.end(), [&sortMap, &payloadType, idx, &keyRefs, this](ItemRef &itemRef) {
			ConstPayload(payloadType, getValue<Items>(itemRef, ns_->items_)).Get(idx, keyRefs);
			return keyRefs.empty() || (sortMap.find(keyRefs[0]) == sortMap.end());
		});

		VariantArray firstItemValue;
		VariantArray secondItemValue;
		std::sort(sortBeg, items.end(),
				  [&sortMap, &payloadType, idx, &firstItemValue, &secondItemValue, this](const ItemRef &lhs, const ItemRef &rhs) {
					  ConstPayload(payloadType, getValue<Items>(lhs, ns_->items_)).Get(idx, firstItemValue);
					  assertf(!firstItemValue.empty(), "Item lost in query results%s", "");
					  assertf(sortMap.find(firstItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");

					  ConstPayload(payloadType, getValue<Items>(rhs, ns_->items_)).Get(idx, secondItemValue);
					  assertf(sortMap.find(secondItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");
					  assertf(!secondItemValue.empty(), "Item lost in query results%s", "");

					  return sortMap.find(firstItemValue[0])->second > sortMap.find(secondItemValue[0])->second;
				  });
	} else {
		// implementation for composite indexes
		FieldsSet fields = ns_->indexes_[idx]->Fields();

		unordered_payload_map<ItemRefVector::difference_type> sortMap(0, hash_composite(payloadType, fields),
																	  equal_composite(payloadType, fields));

		for (auto value : ctx.query.forcedSortOrder) {
			value.convert(fieldType, &payloadType, &fields);
			sortMap.insert({static_cast<const PayloadValue &>(value), cost});
			cost++;
		}

		auto sortBeg = std::stable_partition(items.begin(), items.end(), [&sortMap, this](ItemRef &itemRef) {
			return (sortMap.find(getValue<Items>(itemRef, ns_->items_)) == sortMap.end());
		});

		std::sort(sortBeg, items.begin(), [&sortMap, this](const ItemRef &lhs, const ItemRef &rhs) {
			return sortMap.find(getValue<Items>(lhs, ns_->items_))->second > sortMap.find(getValue<Items>(rhs, ns_->items_))->second;
		});
	}
}

template <typename It>
void NsSelecter::applyGeneralSort(It itFirst, It itLast, It itEnd, const SelectCtx &ctx) {
	if (ctx.query.mergeQueries_.size() > 1) {
		throw Error(errLogic, "Sorting cannot be applied to merged queries.");
	}
	const auto &entries = ctx.sortingContext.entries;
	const auto &exprResults = ctx.sortingContext.exprResults;

	if (entries.empty()) return;

	FieldsSet fields;
	auto &payloadType = ns_->payloadType_;
	bool multiSort = entries.size() > 1;
	h_vector<const CollateOpts *, 1> collateOpts;
	vector<pair<size_t, bool>> byExpr, byIndex;
	byExpr.reserve(exprResults.size());
	assert(entries.size() >= exprResults.size());
	byIndex.reserve(entries.size() - exprResults.size());

	for (size_t i = 0; i < entries.size(); ++i) {
		const auto &sortingCtx = entries[i];
		if (sortingCtx.expression != SortingContext::Entry::NoExpression) {
			byExpr.push_back({i, sortingCtx.data->desc});
			continue;
		}
		byIndex.push_back({i, sortingCtx.data->desc});
		int fieldIdx = sortingCtx.data->index;
		if (fieldIdx == IndexValueType::SetByJsonPath || ns_->indexes_[fieldIdx]->Opts().IsSparse()) {
			TagsPath tagsPath;
			if (fieldIdx != IndexValueType::SetByJsonPath) {
				const FieldsSet &fs = ns_->indexes_[fieldIdx]->Fields();
				assert(fs.getTagsPathsLength() > 0);
				tagsPath = fs.getTagsPath(0);
			} else {
				tagsPath = ns_->tagsMatcher_.path2tag(sortingCtx.data->expression);
			}
			if (fields.contains(tagsPath)) {
				throw Error(errQueryExec, "Can't sort by 2 equal indexes: %s", sortingCtx.data->expression);
			}
			fields.push_back(tagsPath);
		} else {
			if (ns_->indexes_[fieldIdx]->Opts().IsArray()) {
				throw Error(errQueryExec, "Sorting cannot be applied to array field.");
			}
			if (fieldIdx >= ns_->indexes_.firstCompositePos()) {
				if (multiSort) {
					throw Error(errQueryExec, "Multicolumn sorting cannot be applied to composite fields: %s", sortingCtx.data->expression);
				}
				fields = ns_->indexes_[fieldIdx]->Fields();
			} else {
				if (fields.contains(fieldIdx)) {
					throw Error(errQueryExec, "You cannot sort by 2 same indexes: %s", sortingCtx.data->expression);
				}
				fields.push_back(fieldIdx);
			}
		}
		collateOpts.push_back(entries[i].opts);
	}
	assert(byExpr.size() == exprResults.size());
	assert(byIndex.size() == entries.size() - exprResults.size());

	std::partial_sort(itFirst, itLast, itEnd, [&](const ItemRef &lhs, const ItemRef &rhs) {
		size_t firstDifferentExprIdx = 0;
		int exprCmpRes = 0;
		for (const auto &eR : exprResults) {
			if (eR[lhs.SortExprResultsIdx()] != eR[rhs.SortExprResultsIdx()]) {
				exprCmpRes = (eR[lhs.SortExprResultsIdx()] > eR[rhs.SortExprResultsIdx()]) ? 1 : -1;
				break;
			}
			++firstDifferentExprIdx;
		}
		assert(exprCmpRes == 0 || firstDifferentExprIdx < byExpr.size());

		size_t firstDifferentFieldIdx = 0;
		int fieldsCmpRes =
			ConstPayload(payloadType, ns_->items_[lhs.Id()]).Compare(ns_->items_[rhs.Id()], fields, firstDifferentFieldIdx, collateOpts);
		assertf(fieldsCmpRes == 0 || firstDifferentFieldIdx < byIndex.size(), "firstDifferentFieldIdx fail %d,%d -> %d",
				int(firstDifferentFieldIdx), int(byIndex.size()), int(fields.size()));

		int cmpRes;
		bool desc;
		if (exprCmpRes != 0) {
			if (fieldsCmpRes == 0 || byExpr[firstDifferentExprIdx].first < byIndex[firstDifferentFieldIdx].first) {
				cmpRes = exprCmpRes;
				desc = byExpr[firstDifferentExprIdx].second;
			} else {
				cmpRes = fieldsCmpRes;
				desc = byIndex[firstDifferentFieldIdx].second;
			}
		} else if (fieldsCmpRes != 0) {
			cmpRes = fieldsCmpRes;
			desc = byIndex[firstDifferentFieldIdx].second;
		} else {
			// If values are equal, then sort by row ID, to give consistent results
			cmpRes = (lhs.Id() > rhs.Id()) ? 1 : ((lhs.Id() < rhs.Id()) ? -1 : 0);
			desc = entries[0].data->desc;
		}

		if (desc) {
			return (cmpRes > 0);
		} else {
			return (cmpRes < 0);
		}
	});
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

void NsSelecter::processLeftJoins(QueryResults &qr, SelectCtx &sctx) {
	if (!checkIfThereAreLeftJoins(sctx)) return;
	for (auto it : qr) {
		IdType rowid = it.GetItemRef().Id();
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

template <typename Items>
void NsSelecter::sortResults(reindexer::SelectCtx &sctx, Items &items, const SortingOptions &sortingOptions) {
#ifndef NDEBUG
	for (const auto &eR : sctx.sortingContext.exprResults) {
		assert(eR.size() == items.size());
	}
#endif
	int endPos = items.size();
	if (sortingOptions.usingGeneralAlgorithm) {
		endPos = std::min<decltype(items.size())>(sctx.query.count + sctx.query.start, items.size());
	}
	auto first = items.begin();
	auto last = items.begin() + endPos;
	auto end = items.end();

	if (sortingOptions.multiColumn || sortingOptions.usingGeneralAlgorithm) {
		applyGeneralSort(first, last, end, sctx);
	}
	if (sortingOptions.forcedMode) {
		applyForcedSort(items, sctx);
	}
}

template <bool reverse, bool hasComparators>
void NsSelecter::selectLoop(LoopCtx &ctx, QueryResults &result, const RdxContext &rdxCtx) {
	const auto selectLoopWard = rdxCtx.BeforeSelectLoop();
	unsigned start = 0;
	unsigned count = UINT_MAX;
	SelectCtx &sctx = ctx.sctx;
	SelectIteratorContainer &qres = *ctx.qres;

	if (!sctx.isForceAll) {
		start = sctx.query.start;
		count = sctx.query.count;
	}
	auto aggregators = getAggregators(sctx.query);
	// do not calc total by loop, if we have only 1 condition with 1 idset
	bool calcTotal = ctx.calcTotal && (qres.Size() > 1 || hasComparators || (qres.IsIterator(0) && qres[0].size() > 1));

	// reserve queryresults, if we have only 1 condition with 1 idset
	if (qres.Size() == 1 && qres.IsIterator(0) && qres[0].size() == 1) {
		unsigned reserve = std::min(unsigned(qres[0].GetMaxIterations()), count);
		result.Items().reserve(reserve);
	}

	bool finish = (count == 0) && !sctx.reqMatchedOnceFlag && !calcTotal;

	SortingOptions sortingOptions(sctx.query, sctx.sortingContext);
	const Index *const firstSortIndex = sctx.sortingContext.sortIndexIfOrdered();
	bool multiSortFinished = !(sortingOptions.multiColumnByBtreeIndex && count > 0);

	VariantArray prevValues;
	size_t multisortLimitLeft = 0;

	// TODO: nested conditions support. Like (A  OR B OR C) AND (X OR Z)
	assert(!qres.Empty());
	assert(qres.IsIterator(0));
	SelectIterator &firstIterator = qres[0];
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
		if (qres.Process<reverse, hasComparators>(pv, &finish, &rowId, properRowId, !start && count)) {
			sctx.matchedAtLeastOnce = true;
			uint8_t proc = ft_ctx_ ? ft_ctx_->Proc(firstIterator.Pos()) : 0;
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			qres.ForEachIterator([](SelectIterator &it) {
				if (it.distinct) it.ExcludeLastSet();
			});
			if ((start || (count == 0)) && sortingOptions.multiColumnByBtreeIndex) {
				VariantArray recentValues;
				size_t lastResSize = result.Count();
				getSortIndexValue(sctx.sortingContext, properRowId, recentValues, proc);
				if (prevValues.empty() && result.Items().empty()) {
					prevValues = recentValues;
				} else {
					if (recentValues != prevValues) {
						if (start) {
							result.Items().clear();
							multisortLimitLeft = 0;
							lastResSize = 0;
							prevValues = recentValues;
						} else if (!count) {
							multiSortFinished = true;
						}
					}
				}
				if (!multiSortFinished) {
					addSelectResult(proc, rowId, properRowId, sctx, aggregators, result);
				}
				if (lastResSize < result.Count()) {
					if (start) {
						++multisortLimitLeft;
					}
				}
			}
			if (start) {
				--start;
			} else if (count) {
				addSelectResult(proc, rowId, properRowId, sctx, aggregators, result);
				--count;
				if (!count && sortingOptions.multiColumn && !multiSortFinished)
					getSortIndexValue(sctx.sortingContext, properRowId, prevValues, proc);
			}
			if (!count && !calcTotal && multiSortFinished) break;
			if (calcTotal) result.totalCount++;
		}
	}

	if (sctx.preResult && sctx.preResult->dataMode == JoinPreResult::ModeValues) {
		assert(sctx.preResult->executionMode == JoinPreResult::ModeBuild);
		sortResults(sctx, sctx.preResult->values, sortingOptions);
	} else if (sortingOptions.postLoopSortingRequired()) {
		sortResults(sctx, result.Items(), sortingOptions);
		const size_t offset = sortingOptions.usingGeneralAlgorithm ? sctx.query.start : multisortLimitLeft;
		setLimitAndOffset(result.Items(), offset, sctx.query.count);
	}
	processLeftJoins(result, sctx);
	for (ItemRef &iref : result.Items()) {
		if (!iref.ValueInitialized()) iref.SetValue(ns_->items_[iref.Id()]);
	}

	for (auto &aggregator : aggregators) {
		result.aggregationResults.push_back(aggregator.GetResult());
	}

	// Get total count for simple query with 1 condition and 1 idset
	if (ctx.calcTotal && !calcTotal) {
		if (!sctx.query.entries.Empty()) {
			result.totalCount = qres[0].GetMaxIterations();
		} else {
			result.totalCount = ns_->items_.size() - ns_->free_.size();
		}
	}
}

void NsSelecter::getSortIndexValue(const SortingContext &sortCtx, IdType rowId, VariantArray &value, uint8_t proc) {
	const SortingContext::Entry *firstEntry = sortCtx.getFirstColumnEntry();
	ConstPayload pv(ns_->payloadType_, ns_->items_[rowId]);
	if (firstEntry->expression != SortingContext::Entry::NoExpression) {
		assert(firstEntry->expression >= 0 && static_cast<size_t>(firstEntry->expression) < sortCtx.expressions.size());
		value = VariantArray{Variant{sortCtx.expressions[firstEntry->expression].Calculate(pv, proc, ns_->tagsMatcher_)}};
	} else if ((firstEntry->data->index == IndexValueType::SetByJsonPath) || ns_->indexes_[firstEntry->data->index]->Opts().IsSparse()) {
		pv.GetByJsonPath(firstEntry->data->expression, ns_->tagsMatcher_, value, KeyValueUndefined);
	} else {
		pv.Get(firstEntry->data->index, value);
	}
}

void NsSelecter::addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
								 QueryResults &result) {
	if (aggregators.size()) {
		for (auto &aggregator : aggregators) aggregator.Aggregate(ns_->items_[properRowId]);
	} else if (sctx.preResult && sctx.preResult->executionMode == JoinPreResult::ModeBuild) {
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
			for (size_t i = 0; i < exprs.size(); ++i) {
				exprResults[i].push_back(exprs[i].Calculate(pv, proc, ns_->tagsMatcher_));
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

h_vector<Aggregator, 4> NsSelecter::getAggregators(const Query &q) {
	static constexpr int NotFilled = -2;
	h_vector<Aggregator, 4> ret;

	for (auto &ag : q.aggregations_) {
		if (ag.fields_.empty()) {
			throw Error(errQueryExec, "Empty set of fields for aggregation %s", AggregationResult::aggTypeToStr(ag.type_));
		}
		if (ag.type_ != AggFacet) {
			if (ag.fields_.size() != 1) {
				throw Error(errQueryExec, "For aggregation %s available exactly one field", AggregationResult::aggTypeToStr(ag.type_));
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
				} else {
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
		ret.push_back(Aggregator(ns_->payloadType_, fields, ag.type_, ag.fields_, sortingEntries, ag.limit_, ag.offset_));
	}

	return ret;
}

void NsSelecter::prepareSortingContext(SortingEntries &sortBy, SelectCtx &ctx, bool isFt) {
	for (size_t i = 0; i < sortBy.size(); ++i) {
		SortingEntry &sortingEntry(sortBy[i]);
		SortingContext::Entry sortingCtx;
		sortingCtx.data = &sortingEntry;
		assert(!sortingEntry.expression.empty());
		SortExpression expr{SortExpression::Parse(sortingEntry.expression)};
		if (expr.JustByIndex()) {
			sortingEntry.index = IndexValueType::SetByJsonPath;
			ns_->getIndexByName(sortingEntry.expression, sortingEntry.index);
			if (sortingEntry.index >= 0) {
				Index *sortIndex = ns_->indexes_[sortingEntry.index].get();
				sortingCtx.index = sortIndex;
				sortingCtx.opts = &sortIndex->Opts().collateOpts_;

				if (i == 0) {
					if (sortIndex->IsOrdered() && !ctx.sortingContext.enableSortOrders) {
						ctx.sortingContext.uncommitedIndex = sortingEntry.index;
						ctx.isForceAll = false;
					} else if (!sortIndex->IsOrdered() || isFt || !ctx.sortingContext.enableSortOrders) {
						ctx.isForceAll = true;
						sortingCtx.index = nullptr;
					}
				}
			} else if (sortingEntry.index == IndexValueType::SetByJsonPath) {
				ctx.isForceAll = true;
			} else {
				std::abort();
			}
		} else {
			if (!ctx.query.mergeQueries_.empty()) {
				throw Error(errLogic, "Sorting by expression cannot be applied to merged queries.");
			}
			expr.ForEachValue([this, isFt](SortExpressionValue &exprValue) {
				switch (exprValue.type) {
					case SortExpressionValue::Index:
						assert(!exprValue.column.empty());
						exprValue.index = IndexValueType::SetByJsonPath;
						if (ns_->getIndexByName(string{exprValue.column}, exprValue.index) &&
							ns_->indexes_[exprValue.index]->Opts().IsSparse()) {
							exprValue.index = IndexValueType::SetByJsonPath;
						}
						break;
					case SortExpressionValue::Rank:
						if (!isFt) throw Error(errLogic, "Sort by rank() available only for fulltext query");
						break;
					case SortExpressionValue::Value:
						break;
				}
			});
			ctx.sortingContext.expressions.push_back(std::move(expr));
			sortingCtx.expression = ctx.sortingContext.expressions.size() - 1;
			ctx.isForceAll = true;
		}
		ctx.sortingContext.entries.push_back(std::move(sortingCtx));
	}
	ctx.sortingContext.exprResults.resize(ctx.sortingContext.expressions.size());
}

bool NsSelecter::isSortOptimizatonEffective(const QueryEntries &qentries, SelectCtx &ctx, const RdxContext &rdxCtx) {
	if (qentries.Size() == 0 || (qentries.Size() == 1 && qentries.IsEntry(0) && qentries[0].idxNo == ctx.sortingContext.uncommitedIndex))
		return true;

	size_t costNormal = ns_->items_.size() - ns_->free_.size();

	qentries.ForEachEntry([this, &ctx, &rdxCtx, &costNormal](const QueryEntry &qe, OpType) {
		if (qe.idxNo < 0 || qe.idxNo == ctx.sortingContext.uncommitedIndex) return;
		if (costNormal == 0) return;

		auto &index = ns_->indexes_[qe.idxNo];
		if (isFullText(index->Type())) return;

		Index::SelectOpts opts;
		opts.disableIdSetCache = 1;

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
		qentries.ForEachEntry([this, &ctx, &rdxCtx, &costOptimized](const QueryEntry &qe, OpType) {
			if (qe.idxNo < 0 || qe.idxNo != ctx.sortingContext.uncommitedIndex) return;

			Index::SelectOpts opts;
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
