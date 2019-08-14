#include "nsselecter.h"
#include "core/namespace.h"
#include "explaincalc.h"
#include "querypreprocessor.h"
#include "tools/logger.h"

namespace reindexer {

void NsSelecter::operator()(QueryResults &result, SelectCtx &ctx, const RdxContext &rdxCtx) {
	ctx.enableSortOrders = ns_->sortOrdersBuilt_;
	if (ns_->config_.logLevel > ctx.query.debugLevel) {
		const_cast<Query *>(&ctx.query)->debugLevel = ns_->config_.logLevel;
	}

	ExplainCalc explain(ctx.query.explain_ || ctx.query.debugLevel >= LogInfo);
	explain.StartTiming();

	bool needPutCachedTotal = false;
	bool forcedSort = !ctx.query.forcedSortOrder.empty();
	bool needCalcTotal = ctx.query.calcTotal == ModeAccurateTotal;

	QueryCacheKey ckey;
	if (ctx.query.calcTotal == ModeCachedTotal) {
		ckey = QueryCacheKey{ctx.query};

		auto cached = ns_->queryCache_->Get(ckey);
		if (cached.valid && cached.val.total_count >= 0) {
			result.totalCount = cached.val.total_count;
			logPrintf(LogTrace, "[*] using value from cache: %d\t namespace: %s", result.totalCount, ns_->name_);
		} else {
			needPutCachedTotal = cached.valid;
			logPrintf(LogTrace, "[*] value for cache will be calculated by query. namespace: %s", ns_->name_);
			needCalcTotal = true;
		}
	}

	QueryPreprocessor qPreproc(ns_, const_cast<QueryEntries *>(&ctx.query.entries));
	QueryEntries tmpWhereEntries(ctx.skipIndexesLookup ? QueryEntries() : qPreproc.LookupQueryIndexes());
	if (!ctx.skipIndexesLookup) {
		qPreproc.SetQueryEntries(&tmpWhereEntries);
	}

	bool isFt = qPreproc.ContainsFullTextIndexes();
	if (!ctx.skipIndexesLookup && !isFt) qPreproc.SubstituteCompositeIndexes();
	qPreproc.ConvertWhereValues();

	// DO NOT use deducted sort order in the following cases:
	// - query contains explicity specified sort order
	// - query contains FullText query.
	bool disableOptimizeSortOrder = !ctx.query.sortingEntries_.empty() || ctx.preResult;
	SortingEntries sortBy = (isFt || disableOptimizeSortOrder) ? ctx.query.sortingEntries_ : qPreproc.DetectOptimalSortOrder();
	prepareSortingIndexes(sortBy);

	if (ctx.preResult) {
		// For building join preresult always use ASC sort orders
		if (ctx.preResult->mode == JoinPreResult::ModeBuild) {
			for (SortingEntry &se : sortBy) se.desc = false;
			// all futher queries for this join SHOULD have the same enableSortOrders flag
			ctx.preResult->enableSortOrders = ctx.enableSortOrders;
		} else {
			// If in current join query sort orders is disabled
			// then preResult query also SHOULD have disabled flag
			// If assert fails, then possible query has unlock ns
			// or ns->sortOrdersFlag_ has been reseted under read lock!
			if (!ctx.enableSortOrders) assert(!ctx.preResult->enableSortOrders);
			ctx.enableSortOrders = ctx.preResult->enableSortOrders;
		}
	}

	// Prepare sorting context
	prepareSortingContext(sortBy, ctx, isFt);

	// Add preresults with common conditions of join Queres
	SelectIteratorContainer qres;
	if (ctx.preResult && ctx.preResult->mode == JoinPreResult::ModeIdSet) {
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(ctx.preResult->ids));
		static string pr = "-preresult";
		qres.Append(OpAnd, SelectIterator(res, false, pr));
	} else if (ctx.preResult && ctx.preResult->mode == JoinPreResult::ModeIterators) {
		qres.Append(ctx.preResult->iterators.cbegin(), ctx.preResult->iterators.cend());
	}

	// Prepare data for select functions
	if (ctx.functions) {
		fnc_ = ctx.functions->AddNamespace(ctx.query, *ns_, isFt);
	}
	explain.SetPrepareTime();

	qres.PrepareIteratorsForSelectLoop(qPreproc.GetQueryEntries(), 0, qPreproc.GetQueryEntries().Size(), ctx.query.equalPositions_,
									   ctx.sortingCtx.sortId(), isFt, *ns_, fnc_, ft_ctx_, rdxCtx);

	explain.SetSelectTime();

	if (ctx.preResult && ctx.preResult->mode == JoinPreResult::ModeBuild) {
		// Building pre result for next joins
		int maxIters = 0;
		qres.ForeachIterator([&maxIters](const SelectIterator &it, OpType) { maxIters = std::max(maxIters, it.GetMaxIterations()); });

		// Return preResult as QueryIterators if:
		if ((qres.Size() == 1 && qres.IsIterator(0) &&
			 qres[0].size() < 3) ||  // 1. We have 1 QueryIterator with not more, than 2 idsets, just put it to preResults
			maxIters >= 10000 ||	 // 2. We have > QueryIterator which expects more than 10000 iterations.
			(ctx.sortingCtx.entries.size() && !ctx.sortingCtx.sortIndex()))  // 3. We have sorted query, by unordered index
		{
			ctx.preResult->iterators.Append(qres.cbegin(), qres.cend());
			if (ctx.query.debugLevel >= LogInfo) {
				WrSerializer ser;
				logPrintf(LogInfo, "%s", ctx.query.GetSQL(ser).Slice());
				logPrintf(LogInfo, "Built prePresult (expected %d iterations) with %d iterators", maxIters, qres.Size());
			}

			ctx.preResult->mode = JoinPreResult::ModeIterators;
			return;
		}
		// Build preResult as single IdSet
	}

	bool hasComparators = false, hasScan = false;
	bool reverse = !isFt && ctx.sortingCtx.sortIndex() && ctx.sortingCtx.entries[0].data->desc;

	qres.ForeachIterator([&hasComparators](const SelectIterator &it, OpType) {
		if (it.comparators_.size()) hasComparators = true;
	});

	if (qres.Empty() || (!isFt && (!qres.HasIdsets() || qres.GetOperation(0) == OpNot))) {
		// special case - no idset in query
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(
			0, IdType(ctx.sortingCtx.sortIndex() ? ctx.sortingCtx.sortIndex()->SortOrders().size() : ns_->items_.size())));
		qres.AppendFront(OpAnd, SelectIterator(res, false, "-scan", true));
		hasScan = ctx.sortingCtx.sortIndex() && !forcedSort ? false : true;
	}

	// Get maximum iterations count, for right calculation comparators costs
	int iters = INT_MAX;
	qres.ForeachIterator([&iters](const SelectIterator &it, OpType) {
		int cur = it.GetMaxIterations();
		if (it.comparators_.empty() && cur && cur < iters) iters = cur;
	});

	qres.SortByCost(iters);

	// Check idset must be 1st
	qres.CheckFirstQuery();

	// Rewing all results iterators
	qres.ForeachIterator([reverse](SelectIterator &it) { it.Start(reverse); });

	// Let iterators choose most effecive algorith
	assert(qres.Size());
	qres.SetExpectMaxIterations(iters);

	if (ctx.contextCollectingMode) {
		result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, ctx.query.selectFilter_));
	}

	explain.SetPostprocessTime();

	LoopCtx lctx(ctx);
	lctx.qres = &qres;
	lctx.calcTotal = needCalcTotal;
	if (isFt) result.haveProcent = true;
	if (reverse && hasComparators && hasScan) selectLoop<true, true, true>(lctx, result, rdxCtx);
	if (!reverse && hasComparators && hasScan) selectLoop<false, true, true>(lctx, result, rdxCtx);
	if (reverse && !hasComparators && hasScan) selectLoop<true, false, true>(lctx, result, rdxCtx);
	if (!reverse && !hasComparators && hasScan) selectLoop<false, false, true>(lctx, result, rdxCtx);
	if (reverse && hasComparators && !hasScan) selectLoop<true, true, false>(lctx, result, rdxCtx);
	if (!reverse && hasComparators && !hasScan) selectLoop<false, true, false>(lctx, result, rdxCtx);
	if (reverse && !hasComparators && !hasScan) selectLoop<true, false, false>(lctx, result, rdxCtx);
	if (!reverse && !hasComparators && !hasScan) selectLoop<false, false, false>(lctx, result, rdxCtx);

	explain.SetLoopTime();
	explain.StopTiming();
	explain.PutSortIndex(ctx.sortingCtx.sortIndex() ? ctx.sortingCtx.sortIndex()->Name() : "-"_sv);
	explain.PutCount((ctx.preResult && ctx.preResult->mode == JoinPreResult::ModeBuild) ? ctx.preResult->ids.size() : result.Count());
	explain.PutSelectors(&qres);
	explain.PutJoinedSelectors(ctx.joinedSelectors);
	explain.SetIterations(iters);

	if (ctx.query.debugLevel >= LogInfo) {
		WrSerializer ser;
		logPrintf(LogInfo, "%s", ctx.query.GetSQL(ser).Slice());
		explain.LogDump(ctx.query.debugLevel);
	}
	if (ctx.query.explain_) {
		result.explainResults = explain.GetJSON();
	}
	if (ctx.query.debugLevel >= LogTrace) result.Dump();

	if (needPutCachedTotal) {
		logPrintf(LogTrace, "[*] put totalCount value into query cache: %d\t namespace: %s\n", result.totalCount, ns_->name_);
		ns_->queryCache_->Put(ckey, {static_cast<size_t>(result.totalCount)});
	}
	if (ctx.preResult && ctx.preResult->mode == JoinPreResult::ModeBuild) {
		ctx.preResult->mode = JoinPreResult::ModeIdSet;
		if (ctx.query.debugLevel >= LogInfo) {
			logPrintf(LogInfo, "Built idset prePresult with %d ids", ctx.preResult->ids.size());
		}
	}
}

void NsSelecter::applyForcedSort(ItemRefVector &queryResult, const SelectCtx &ctx) {
	if (ctx.query.sortingEntries_[0].desc) {
		applyForcedSortDesc(queryResult, ctx);
		return;
	}

	if (ctx.query.mergeQueries_.size() > 1) throw Error(errLogic, "Force sort could not be applied to 'merged' queries.");

	assert(!ctx.query.sortingEntries_.empty());

	auto payloadType = ns_->payloadType_;
	const string &fieldName = ctx.query.sortingEntries_[0].column;

	int idx = ns_->getIndexByName(fieldName);

	if (ns_->indexes_[idx]->Opts().IsArray()) throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");

	VariantArray keyValues = ctx.query.forcedSortOrder;  // make a copy of forcedSortOrder', may be mark it as 'mutable'?
	ItemRefVector::difference_type cost = 0;
	KeyValueType fieldType = ns_->indexes_[idx]->KeyType();

	if (idx < ns_->indexes_.firstCompositePos()) {
		// implementation for regular indexes
		fast_hash_map<Variant, ItemRefVector::difference_type> sortMap;
		for (auto &value : keyValues) {
			value.convert(fieldType);
			sortMap.insert({value, cost});
			cost++;
		}

		VariantArray keyRefs;
		auto sortEnd =
			std::stable_partition(queryResult.begin(), queryResult.end(), [&sortMap, &payloadType, idx, &keyRefs](ItemRef &itemRef) {
				ConstPayload(payloadType, itemRef.value).Get(idx, keyRefs);
				return !keyRefs.empty() && (sortMap.find(keyRefs[0]) != sortMap.end());
			});

		VariantArray firstItemValue;
		VariantArray secondItemValue;
		std::sort(queryResult.begin(), sortEnd,
				  [&sortMap, &payloadType, idx, &firstItemValue, &secondItemValue](const ItemRef &lhs, const ItemRef &rhs) {
					  ConstPayload(payloadType, lhs.value).Get(idx, firstItemValue);
					  assertf(!firstItemValue.empty(), "Item lost in query results%s", "");
					  assertf(sortMap.find(firstItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");

					  ConstPayload(payloadType, rhs.value).Get(idx, secondItemValue);
					  assertf(sortMap.find(secondItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");
					  assertf(!secondItemValue.empty(), "Item lost in query results%s", "");

					  return sortMap.find(firstItemValue[0])->second < sortMap.find(secondItemValue[0])->second;
				  });
	} else {
		// implementation for composite indexes
		FieldsSet fields = ns_->indexes_[idx]->Fields();

		unordered_payload_map<ItemRefVector::difference_type> sortMap(0, hash_composite(payloadType, fields),
																	  equal_composite(payloadType, fields));

		for (auto &value : keyValues) {
			value.convert(fieldType, &payloadType, &fields);
			sortMap.insert({static_cast<const PayloadValue &>(value), cost});
			cost++;
		}

		auto sortEnd = std::stable_partition(queryResult.begin(), queryResult.end(),
											 [&sortMap](ItemRef &itemRef) { return (sortMap.find(itemRef.value) != sortMap.end()); });

		std::sort(queryResult.begin(), sortEnd, [&sortMap](const ItemRef &lhs, const ItemRef &rhs) {
			return sortMap.find(lhs.value)->second < sortMap.find(rhs.value)->second;
		});
	}
}

void NsSelecter::applyForcedSortDesc(ItemRefVector &queryResult, const SelectCtx &ctx) {
	if (ctx.query.mergeQueries_.size() > 1) throw Error(errLogic, "Force sort could not be applied to 'merged' queries.");

	assert(!ctx.query.sortingEntries_.empty());

	auto payloadType = ns_->payloadType_;
	const string &fieldName = ctx.query.sortingEntries_[0].column;

	int idx = ns_->getIndexByName(fieldName);

	if (ns_->indexes_[idx]->Opts().IsArray()) throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");

	VariantArray keyValues = ctx.query.forcedSortOrder;  // make a copy of forcedSortOrder', may be mark it as 'mutable'?
	ItemRefVector::difference_type cost = 0;
	KeyValueType fieldType = ns_->indexes_[idx]->KeyType();

	if (idx < ns_->indexes_.firstCompositePos()) {
		// implementation for regular indexes
		fast_hash_map<Variant, ItemRefVector::difference_type> sortMap;
		for (auto &value : keyValues) {
			value.convert(fieldType);
			sortMap.insert({value, cost});
			cost++;
		}

		VariantArray keyRefs;
		auto sortBeg =
			std::stable_partition(queryResult.begin(), queryResult.end(), [&sortMap, &payloadType, idx, &keyRefs](ItemRef &itemRef) {
				ConstPayload(payloadType, itemRef.value).Get(idx, keyRefs);
				return keyRefs.empty() || (sortMap.find(keyRefs[0]) == sortMap.end());
			});

		VariantArray firstItemValue;
		VariantArray secondItemValue;
		std::sort(sortBeg, queryResult.end(),
				  [&sortMap, &payloadType, idx, &firstItemValue, &secondItemValue](const ItemRef &lhs, const ItemRef &rhs) {
					  ConstPayload(payloadType, lhs.value).Get(idx, firstItemValue);
					  assertf(!firstItemValue.empty(), "Item lost in query results%s", "");
					  assertf(sortMap.find(firstItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");

					  ConstPayload(payloadType, rhs.value).Get(idx, secondItemValue);
					  assertf(sortMap.find(secondItemValue[0]) != sortMap.end(), "Item not found in 'sortMap'%s", "");
					  assertf(!secondItemValue.empty(), "Item lost in query results%s", "");

					  return sortMap.find(firstItemValue[0])->second > sortMap.find(secondItemValue[0])->second;
				  });
	} else {
		// implementation for composite indexes
		FieldsSet fields = ns_->indexes_[idx]->Fields();

		unordered_payload_map<ItemRefVector::difference_type> sortMap(0, hash_composite(payloadType, fields),
																	  equal_composite(payloadType, fields));

		for (auto &value : keyValues) {
			value.convert(fieldType, &payloadType, &fields);
			sortMap.insert({static_cast<const PayloadValue &>(value), cost});
			cost++;
		}

		auto sortBeg = std::stable_partition(queryResult.begin(), queryResult.end(),
											 [&sortMap](ItemRef &itemRef) { return (sortMap.find(itemRef.value) == sortMap.end()); });

		std::sort(sortBeg, queryResult.begin(), [&sortMap](const ItemRef &lhs, const ItemRef &rhs) {
			return sortMap.find(lhs.value)->second > sortMap.find(rhs.value)->second;
		});
	}
}

void NsSelecter::applyGeneralSort(ConstItemIterator itFirst, ConstItemIterator itLast, ConstItemIterator itEnd, const SelectCtx &ctx) {
	if (ctx.query.mergeQueries_.size() > 1) {
		throw Error(errLogic, "Sorting cannot be applied to merged queries.");
	}

	if (ctx.sortingCtx.entries.empty()) return;

	FieldsSet fields;
	auto &payloadType = ns_->payloadType_;
	bool multiSort = ctx.sortingCtx.entries.size() > 1;
	h_vector<const CollateOpts *, 1> collateOpts;

	for (size_t i = 0; i < ctx.sortingCtx.entries.size(); ++i) {
		const auto &sortingCtx = ctx.sortingCtx.entries[i];
		int fieldIdx = sortingCtx.data->index;
		if (fieldIdx == IndexValueType::SetByJsonPath || ns_->indexes_[fieldIdx]->Opts().IsSparse()) {
			TagsPath tagsPath;
			if (fieldIdx != IndexValueType::SetByJsonPath) {
				const FieldsSet &fs = ns_->indexes_[fieldIdx]->Fields();
				assert(fs.getTagsPathsLength() > 0);
				tagsPath = fs.getTagsPath(0);
			} else {
				tagsPath = ns_->tagsMatcher_.path2tag(sortingCtx.data->column);
			}
			if (fields.contains(tagsPath)) {
				throw Error(errQueryExec, "Can't sort by 2 equal indexes: %s", sortingCtx.data->column);
			}
			fields.push_back(tagsPath);
		} else {
			if (ns_->indexes_[fieldIdx]->Opts().IsArray()) {
				throw Error(errQueryExec, "Sorting cannot be applied to array field.");
			}
			if (fieldIdx >= ns_->indexes_.firstCompositePos()) {
				if (multiSort) {
					throw Error(errQueryExec, "Multicolumn sorting cannot be applied to composite fields: %s", sortingCtx.data->column);
				}
				fields = ns_->indexes_[fieldIdx]->Fields();
			} else {
				if (fields.contains(fieldIdx)) {
					throw Error(errQueryExec, "You cannot sort by 2 same indexes: %s", sortingCtx.data->column);
				}
				fields.push_back(fieldIdx);
			}
		}
		collateOpts.push_back(ctx.sortingCtx.entries[i].opts);
	}

	std::partial_sort(itFirst, itLast, itEnd, [&payloadType, &fields, &collateOpts, &ctx](const ItemRef &lhs, const ItemRef &rhs) {
		size_t firstDifferentFieldIdx = 0;
		int cmpRes = ConstPayload(payloadType, lhs.value).Compare(rhs.value, fields, firstDifferentFieldIdx, collateOpts);
		assert(ctx.sortingCtx.entries.size());
		if (ctx.sortingCtx.entries.size() == 1) firstDifferentFieldIdx = 0;
		assertf(firstDifferentFieldIdx < ctx.sortingCtx.entries.size(), "firstDifferentFieldIdx fail %d,%d -> %d",
				int(firstDifferentFieldIdx), int(ctx.sortingCtx.entries.size()), int(fields.size()));

		// If values are equal, then sort by row ID, to give consistent results
		if (cmpRes == 0) cmpRes = (lhs.id > rhs.id) ? 1 : ((lhs.id < rhs.id) ? -1 : 0);

		if (ctx.sortingCtx.entries[firstDifferentFieldIdx].data->desc) {
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

void NsSelecter::processLeftJoins(const ConstPayload &pl, SelectCtx &sctx, IdType properRowId, bool found, bool match) {
	if (match && found)
		for (auto &joinedSelector : *sctx.joinedSelectors)
			if (joinedSelector.type == JoinType::LeftJoin) joinedSelector.func(&joinedSelector, properRowId, sctx.nsid, pl, match);
}

bool NsSelecter::processInnerJoins(const ConstPayload &pl, SelectCtx &sctx, IdType properRowId, bool found, bool match) {
	for (size_t i = 0; i < sctx.joinedSelectors->size(); ++i) {
		auto &joinedSelector = sctx.joinedSelectors->at(i);
		bool res = false;
		joinedSelector.called++;

		if (joinedSelector.type == JoinType::InnerJoin) {
			if (found) {
				res = joinedSelector.func(&joinedSelector, properRowId, sctx.nsid, pl, match);
				found &= res;
			}
		}
		if (joinedSelector.type == JoinType::OrInnerJoin) {
			if (!found || !joinedSelector.nodata) {
				res = joinedSelector.func(&joinedSelector, properRowId, sctx.nsid, pl, match);
				found |= res;
			}
		}
		if (res) {
			joinedSelector.matched++;
		}
		// If not found and next op is not OR then we're done
		if (!found && !(i + 1 < sctx.joinedSelectors->size() && sctx.joinedSelectors->at(i + 1).type == JoinType::OrInnerJoin)) {
			break;
		}
	}
	return found;
}

bool NsSelecter::processJoins(SelectCtx &sctx, IdType properRowId, bool found, bool match, bool hasInnerJoins, bool hasLeftJoins,
							  bool postLoopLimitOffsetProcessing) {
	ConstPayload pl(ns_->payloadType_, ns_->items_[properRowId]);
	if (hasInnerJoins) {
		bool fullSelect = (match && !postLoopLimitOffsetProcessing);
		found = processInnerJoins(pl, sctx, properRowId, found, fullSelect);
	}
	if (!postLoopLimitOffsetProcessing && hasLeftJoins) {
		processLeftJoins(pl, sctx, properRowId, found, match);
	}
	return found;
}

template <bool reverse, bool hasComparators, bool hasScan>
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

	bool hasInnerJoins = false, hasLeftJoins = false;
	if (sctx.joinedSelectors) {
		for (auto &joinedSelector : *sctx.joinedSelectors) {
			hasInnerJoins |= (joinedSelector.type == JoinType::InnerJoin || joinedSelector.type == JoinType::OrInnerJoin);
			hasLeftJoins |= (joinedSelector.type == JoinType::LeftJoin);
		}
	}

	bool generalSort = false;
	bool multisortFinished = true;
	bool multiSortByOrdered = false;
	bool forcedSort = !ctx.sctx.query.forcedSortOrder.empty();
	Index *firstSortIndex = nullptr;
	SelectCtx::SortingCtx::Entry *sortCtx = nullptr;
	bool multiSort = sctx.sortingCtx.entries.size() > 1;
	if (!sctx.sortingCtx.entries.empty()) {
		sortCtx = &sctx.sortingCtx.entries[0];
		generalSort = !sortCtx->index;
		if (sortCtx->index) {
			firstSortIndex = sortCtx->index;
			multiSortByOrdered = multiSort && firstSortIndex;
			multisortFinished = !(multiSortByOrdered && count > 0);
		}
	}

	// When we have non-btree index sorting with limit and(or) offset
	bool postLoopLimitOffsetProcessing = ((multiSort || generalSort || forcedSort) && (sctx.query.start || sctx.query.count != UINT_MAX));

	VariantArray prevValues;
	size_t multisortLimitLeft = 0, multisortLimitRight = 0;

	// TODO: nested conditions support. Like (A  OR B OR C) AND (X OR Z)
	assert(!firstSortIndex || (firstSortIndex->IsOrdered() && ns_->sortOrdersBuilt_));
	assert(!qres.Empty());
	assert(qres.IsIterator(0));
	SelectIterator &firstIterator = qres[0];
	IdType rowId = firstIterator.Val();
	while (firstIterator.Next(rowId) && !finish) {
		rowId = firstIterator.Val();
		IdType properRowId = rowId;

		if (hasScan && ns_->items_[properRowId].IsFree()) continue;
		if (firstSortIndex) {
			assert(firstSortIndex->SortOrders().size() > static_cast<size_t>(rowId));
			properRowId = firstSortIndex->SortOrders()[rowId];
		}

		assert(static_cast<size_t>(properRowId) < ns_->items_.size());
		PayloadValue &pv = ns_->items_[properRowId];
		assert(pv.Ptr());
		bool found = qres.Process<reverse, hasComparators>(pv, &finish, &rowId, properRowId);

		if (found && sctx.joinedSelectors) {
			found = processJoins(sctx, properRowId, found, !start && count, hasInnerJoins, hasLeftJoins, postLoopLimitOffsetProcessing);
		}

		if (found) {
			sctx.matchedAtLeastOnce = true;
			uint8_t proc = ft_ctx_ ? ft_ctx_->Proc(firstIterator.Pos()) : 0;
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			qres.ForeachIterator([](SelectIterator &it) {
				if (it.distinct) it.ExcludeLastSet();
			});
			if ((start || (count == 0)) && multiSortByOrdered) {
				VariantArray recentValues;
				size_t lastResSize = result.Count();
				getSortIndexValue(sortCtx, properRowId, recentValues);
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
							multisortFinished = true;
						}
					}
				}
				if (!multisortFinished) {
					addSelectResult(proc, rowId, properRowId, sctx, aggregators, result);
				}
				if (lastResSize < result.Count()) {
					if (start) {
						++multisortLimitLeft;
					} else if (!count) {
						++multisortLimitRight;
					}
				}
			}
			if (start) {
				--start;
			} else if (count) {
				addSelectResult(proc, rowId, properRowId, sctx, aggregators, result);
				--count;
				if (!count && multiSort && !multisortFinished) getSortIndexValue(sortCtx, properRowId, prevValues);
			}
			if (!count && !calcTotal && multisortFinished) break;
			if (calcTotal) result.totalCount++;
		}
	}

	if (multiSort || generalSort || forcedSort) {
		int endPos = result.Items().size();
		if (generalSort) {
			endPos = std::min(sctx.query.count + sctx.query.start, result.Items().size());
		}
		ItemIterator first = result.Items().begin();
		ItemIterator last = result.Items().begin() + endPos;
		ItemIterator end = result.Items().end();

		if (multiSort || generalSort) {
			applyGeneralSort(first, last, end, sctx);
		}
		if (forcedSort) {
			applyForcedSort(result.Items(), sctx);
		}

		const size_t offset = generalSort ? sctx.query.start : multisortLimitLeft;
		setLimitAndOffset(result.Items(), offset, sctx.query.count);
	}

	if (sctx.joinedSelectors && postLoopLimitOffsetProcessing) {
		for (auto it : result) {
			processJoins(sctx, it.GetItemRef().id, true, true, hasInnerJoins, hasLeftJoins, false);
		}
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

void NsSelecter::getSortIndexValue(const SelectCtx::SortingCtx::Entry *sortCtx, IdType rowId, VariantArray &value) {
	ConstPayload pv(ns_->payloadType_, ns_->items_[rowId]);
	if ((sortCtx->data->index == IndexValueType::SetByJsonPath) || ns_->indexes_[sortCtx->data->index]->Opts().IsSparse()) {
		pv.GetByJsonPath(sortCtx->data->column, ns_->tagsMatcher_, value, KeyValueUndefined);
	} else {
		pv.Get(sortCtx->data->index, value);
	}
}

void NsSelecter::addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, const SelectCtx &sctx,
								 h_vector<Aggregator, 4> &aggregators, QueryResults &result) {
	if (aggregators.size()) {
		for (auto &aggregator : aggregators) aggregator.Aggregate(ns_->items_[properRowId]);
	} else if (sctx.preResult && sctx.preResult->mode == JoinPreResult::ModeBuild) {
		sctx.preResult->ids.Add(rowId, IdSet::Unordered, 0);
	} else {
		result.Add({properRowId, ns_->items_[properRowId], proc, sctx.nsid}, ns_->payloadType_);

		const int kLimitItems = 10000000;
		size_t sz = result.Count();
		if (sz >= kLimitItems && !(sz % kLimitItems)) {
			WrSerializer ser;
			logPrintf(LogWarning, "Too big query results ns='%s',count='%d',rowId='%d',q='%s'", ns_->name_, sz, properRowId,
					  sctx.query.GetSQL(ser).Slice());
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
			sortingEntries[i] = {(iequals("count"_sv, ag.sortingEntries_[i].column) ? Aggregator::SortingEntry::Count : NotFilled),
								 ag.sortingEntries_[i].desc};
		}
		int idx = -1;
		for (size_t i = 0; i < ag.fields_.size(); ++i) {
			for (size_t j = 0; j < sortingEntries.size(); ++j) {
				if (iequals(ag.fields_[i], ag.sortingEntries_[j].column)) {
					sortingEntries[j].field = i;
				}
			}
			if (ns_->getIndexByName(ag.fields_[i], idx)) {
				if (ns_->indexes_[idx]->Opts().IsSparse()) {
					fields.push_back(ns_->indexes_[idx]->Fields().getTagsPath(0));
				} else if (ag.type_ == AggFacet && ns_->indexes_[idx]->Opts().IsArray()) {
					throw Error(errQueryExec, "Can't do facet by array field");
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
							ag.sortingEntries_[i].column);
			}
		}
		ret.push_back(Aggregator(ns_->payloadType_, fields, ag.type_, ag.fields_, sortingEntries, ag.limit_, ag.offset_));
	}

	return ret;
}

void NsSelecter::prepareSortingContext(const SortingEntries &sortBy, SelectCtx &ctx, bool isFt) {
	for (size_t i = 0; i < sortBy.size(); ++i) {
		const SortingEntry &sortingEntry(sortBy[i]);
		SelectCtx::SortingCtx::Entry sortingCtx;
		sortingCtx.data = &sortingEntry;
		if (sortingEntry.index >= 0) {
			Index *sortIndex = ns_->indexes_[sortingEntry.index].get();

			sortingCtx.index = sortIndex;

			if (i == 0) {
				if (!sortIndex->IsOrdered() || isFt || !ctx.enableSortOrders) {
					ctx.isForceAll = true;
					sortingCtx.index = nullptr;  // TODO: get rid of this magic in the future
				}
			}

			sortingCtx.opts = &sortIndex->Opts().collateOpts_;
		} else if (sortingEntry.index != IndexValueType::SetByJsonPath) {
			std::abort();
		} else {
			ctx.isForceAll = true;
		}
		ctx.sortingCtx.entries.push_back(std::move(sortingCtx));
	}
}

void NsSelecter::prepareSortingIndexes(SortingEntries &sortingBy) {
	for (SortingEntry &sortingEntry : sortingBy) {
		assert(!sortingEntry.column.empty());
		sortingEntry.index = IndexValueType::SetByJsonPath;
		ns_->getIndexByName(sortingEntry.column, sortingEntry.index);
	}
}

}  // namespace reindexer
