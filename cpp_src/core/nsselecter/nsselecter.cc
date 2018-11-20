#include <sstream>

#include "core/index/index.h"
#include "core/namespace.h"
#include "explaincalc.h"
#include "nsselecter.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

using std::chrono::high_resolution_clock;
using std::next;
using std::shared_ptr;
using std::string;
using std::stringstream;

namespace reindexer {

void NsSelecter::operator()(QueryResults &result, SelectCtx &ctx) {
	ctx.enableSortOrders = ns_->sortOrdersBuilt_;
	if (ns_->queriesLogLevel_ > ctx.query.debugLevel) {
		const_cast<Query *>(&ctx.query)->debugLevel = ns_->queriesLogLevel_;
	}

	ExplainCalc explain(ctx.query.explain_ || ctx.query.debugLevel >= LogInfo);
	explain.StartTiming();

	bool needPutCachedTotal = false;
	bool forcedSort = !ctx.query.forcedSortOrder.empty();
	bool needCalcTotal = ctx.query.calcTotal == ModeAccurateTotal;

	if (ctx.query.calcTotal == ModeCachedTotal) {
		auto cached = ns_->queryCache_->Get({ctx.query});
		if (cached.key && cached.val.total_count >= 0) {
			result.totalCount = cached.val.total_count;
			logPrintf(LogTrace, "[*] using value from cache: %d\t namespace: %s", result.totalCount, ns_->name_.c_str());
		} else {
			needPutCachedTotal = (cached.key != nullptr);
			logPrintf(LogTrace, "[*] value for cache will be calculated by query. namespace: %s", ns_->name_.c_str());
			needCalcTotal = true;
		}
	}

	const QueryEntries *whereEntries = &ctx.query.entries;
	QueryEntries tmpWhereEntries(ctx.skipIndexesLookup ? QueryEntries() : lookupQueryIndexes(ctx.query.entries));
	if (!ctx.skipIndexesLookup) {
		whereEntries = &tmpWhereEntries;
	}

	bool isFt = containsFullTextIndexes(*whereEntries);
	if (!ctx.skipIndexesLookup && !isFt) substituteCompositeIndexes(tmpWhereEntries);
	convertWhereValues(*const_cast<QueryEntries *>(whereEntries));

	// DO NOT use deducted sort order in the following cases:
	// - query contains explicity specified sort order
	// - query contains FullText query.
	bool disableOptimizeSortOrder = !ctx.query.sortingEntries_.empty() || ctx.preResult;
	SortingEntries sortBy = (isFt || disableOptimizeSortOrder) ? ctx.query.sortingEntries_ : detectOptimalSortOrder(*whereEntries);
	prepareSortingIndexes(sortBy);

	if (ctx.preResult) {
		// For building join preresult always use ASC sort orders
		if (ctx.preResult->mode == SelectCtx::PreResult::ModeBuild) {
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
	RawQueryResult qres;
	if (ctx.preResult && ctx.preResult->mode == SelectCtx::PreResult::ModeIdSet) {
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(ctx.preResult->ids));
		static string pr = "-preresult";
		qres.push_back(SelectIterator(res, OpAnd, false, pr));
	} else if (ctx.preResult && ctx.preResult->mode == SelectCtx::PreResult::ModeIterators) {
		for (auto &it : ctx.preResult->iterators) {
			qres.push_back(it);
		}
	}

	// Prepare data for select functions
	if (ctx.functions) {
		fnc_ = ctx.functions->AddNamespace(ctx.query, *ns_, isFt);
	}
	explain.SetPrepareTime();

	prepareIteratorsForSelectLoop(*whereEntries, qres, ctx.sortingCtx.sortId(), isFt);
	prepareEqualPositionComparator(ctx.query, *whereEntries, qres);

	explain.SetSelectTime();

	if (ctx.preResult && ctx.preResult->mode == SelectCtx::PreResult::ModeBuild) {
		// Building pre result for next joins
		int maxIters = 0;
		for (auto &it : qres) {
			maxIters = std::max(maxIters, it.GetMaxIterations());
		}

		// Return preResult as QueryIterators if:
		if ((qres.size() == 1 &&
			 qres[0].size() < 3) ||  // 1. We have 1 QueryIterator with not more, than 2 idsets, just put it to preResults
			maxIters >= 10000 ||	 // 2. We have > QueryIterator which expects more than 10000 iterations.
			(ctx.sortingCtx.entries.size() && !ctx.sortingCtx.sortIndex()))  // 3. We have sorted query, by unordered index
		{
			for (auto &it : qres) {
				ctx.preResult->iterators.push_back(it);
			}
			if (ctx.query.debugLevel >= LogInfo) {
				WrSerializer ser;
				logPrintf(LogInfo, "%s", ctx.query.GetSQL(ser).c_str());
				logPrintf(LogInfo, "Built prePresult (expected %d iterations) with %d iterators", maxIters, qres.size());
			}

			ctx.preResult->mode = SelectCtx::PreResult::ModeIterators;
			return;
		}
		// Build preResult as single IdSet
	}

	bool hasComparators = false, hasScan = false, hasIdsets = false;
	bool reverse = !isFt && ctx.sortingCtx.sortIndex() && ctx.sortingCtx.entries[0].data->desc;

	for (auto &r : qres) {
		if (r.comparators_.size()) {
			hasComparators = true;
		} else if (r.op != OpNot) {
			hasIdsets = true;
		}
	}

	if (qres.empty() || (!isFt && (!hasIdsets || qres[0].op == OpNot))) {
		// special case - no condition or first result have comparator or not
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(
			0, IdType(ctx.sortingCtx.sortIndex() ? ctx.sortingCtx.sortIndex()->SortOrders().size() : ns_->items_.size())));
		qres.insert(qres.begin(), SelectIterator(res, OpAnd, false, "-scan", true));
		hasScan = ctx.sortingCtx.sortIndex() && !forcedSort ? false : true;
	}

	// Get maximum iterations count, for right calculation comparators costs
	int iters = INT_MAX;
	for (auto r = qres.begin(); r != qres.end(); ++r) {
		int cur = r->GetMaxIterations();
		if (!r->comparators_.size() && cur && cur < iters) iters = cur;
	}

	// Sort by cost
	std::sort(qres.begin(), qres.end(), [iters](const SelectIterator &i1, const SelectIterator &i2) {
		if (i1.distinct < i2.distinct) return true;
		if (i1.distinct > i2.distinct) return false;
		return (i1.Cost(iters) < i2.Cost(iters));
	});

	// Check NOT or comparator must not be 1st
	for (auto r = qres.begin(); r != qres.end(); ++r) {
		if (r->op != OpNot && !r->comparators_.size()) {
			if (r != qres.begin()) {
				// if NOT found in 1-st position swap it with 1-st non NOT
				auto tmp = *r;
				*r = *qres.begin();
				*qres.begin() = tmp;
			}
			break;
		}
	}

	// Rewing all results iterators
	for (auto &r : qres) r.Start(reverse);

	// Let iterators choose most effecive algorith
	assert(qres.size());
	for (auto r = qres.begin() + 1; r != qres.end(); r++) r->SetExpectMaxIterations(iters);

	result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, ctx.query.selectFilter_));

	explain.SetPostprocessTime();

	LoopCtx lctx(ctx);
	lctx.qres = &qres;
	lctx.calcTotal = needCalcTotal;
	if (isFt) result.haveProcent = true;
	if (reverse && hasComparators && hasScan) selectLoop<true, true, true>(lctx, result);
	if (!reverse && hasComparators && hasScan) selectLoop<false, true, true>(lctx, result);
	if (reverse && !hasComparators && hasScan) selectLoop<true, false, true>(lctx, result);
	if (!reverse && !hasComparators && hasScan) selectLoop<false, false, true>(lctx, result);
	if (reverse && hasComparators && !hasScan) selectLoop<true, true, false>(lctx, result);
	if (!reverse && hasComparators && !hasScan) selectLoop<false, true, false>(lctx, result);
	if (reverse && !hasComparators && !hasScan) selectLoop<true, false, false>(lctx, result);
	if (!reverse && !hasComparators && !hasScan) selectLoop<false, false, false>(lctx, result);

	explain.SetLoopTime();
	explain.StopTiming();
	explain.PutSortIndex(ctx.sortingCtx.sortIndex() ? ctx.sortingCtx.sortIndex()->Name() : "-"_sv);
	explain.PutCount((ctx.preResult && ctx.preResult->mode == SelectCtx::PreResult::ModeBuild) ? ctx.preResult->ids.size()
																							   : result.Count());
	explain.PutSelectors(&qres);
	explain.PutJoinedSelectors(ctx.joinedSelectors);
	explain.SetIterations(iters);

	if (ctx.query.debugLevel >= LogInfo) {
		WrSerializer ser;
		logPrintf(LogInfo, "%s", ctx.query.GetSQL(ser).c_str());
		explain.LogDump(ctx.query.debugLevel);
	}
	if (ctx.query.explain_) {
		result.explainResults = explain.GetJSON();
	}
	if (ctx.query.debugLevel >= LogTrace) result.Dump();

	if (needPutCachedTotal) {
		logPrintf(LogTrace, "[*] put totalCount value into query cache: %d\t namespace: %s\n", result.totalCount, ns_->name_.c_str());
		ns_->queryCache_->Put({ctx.query}, {static_cast<size_t>(result.totalCount)});
	}
	if (ctx.preResult && ctx.preResult->mode == SelectCtx::PreResult::ModeBuild) {
		ctx.preResult->mode = SelectCtx::PreResult::ModeIdSet;
		if (ctx.query.debugLevel >= LogInfo) {
			logPrintf(LogInfo, "Built idset prePresult with %d ids", ctx.preResult->ids.size());
		}
	}
}

void NsSelecter::applyCustomSort(ItemRefVector &queryResult, const SelectCtx &ctx) {
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
				throw Error(errQueryExec, "Can't sort by 2 equal indexes: %s", sortingCtx.data->column.c_str());
			}
			fields.push_back(tagsPath);
		} else {
			if (ns_->indexes_[fieldIdx]->Opts().IsArray()) {
				throw Error(errQueryExec, "Sorting cannot be applied to array field.");
			}
			if (fieldIdx >= ns_->indexes_.firstCompositePos()) {
				if (multiSort) {
					throw Error(errQueryExec, "Multicolumn sorting cannot be applied to composite fields: %s",
								sortingCtx.data->column.c_str());
				}
				fields = ns_->indexes_[fieldIdx]->Fields();
			} else {
				if (fields.contains(fieldIdx)) {
					throw Error(errQueryExec, "You cannot sort by 2 same indexes: %s", sortingCtx.data->column.c_str());
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

bool NsSelecter::containsFullTextIndexes(const QueryEntries &entries) {
	bool result = false;
	for (const QueryEntry &entry : entries) {
		if ((entry.idxNo != IndexValueType::SetByJsonPath) && isFullText(ns_->indexes_[entry.idxNo]->Type())) {
			result = true;
			break;
		}
	}
	return result;
}

QueryEntries NsSelecter::lookupQueryIndexes(const QueryEntries &entries) {
	int iidx[maxIndexes];
	for (auto &i : iidx) i = -1;

	QueryEntries ret;
	ret.reserve(entries.size());

	for (auto entry = entries.begin(); entry != entries.end(); entry++) {
		QueryEntry ce = *entry;
		if (ce.idxNo == IndexValueType::NotSet) {
			if (!ns_->getIndexByName(ce.index, ce.idxNo)) {
				ce.idxNo = IndexValueType::SetByJsonPath;
			}
		}

		bool isIndexField = (ce.idxNo != IndexValueType::SetByJsonPath);

		// try merge entries with AND opetator
		auto nextEntry = entry;
		nextEntry++;
		if (isIndexField && (ce.op == OpAnd) && (nextEntry == entries.end() || nextEntry->op == OpAnd)) {
			if (iidx[ce.idxNo] >= 0 && !ns_->indexes_[ce.idxNo]->Opts().IsArray()) {
				if (mergeQueryEntries(&ret[iidx[ce.idxNo]], &ce)) continue;
			} else {
				iidx[ce.idxNo] = ret.size();
			}
		}
		ret.push_back(std::move(ce));
	};
	return ret;
}

void NsSelecter::convertWhereValues(QueryEntries &entries) {
	for (auto &ce : entries) {
		bool isIndexField = (ce.idxNo != IndexValueType::SetByJsonPath);
		KeyValueType keyType = isIndexField ? ns_->indexes_[ce.idxNo]->SelectKeyType() : detectQueryEntryIndexType(ce);
		const FieldsSet *fields = isIndexField ? &ns_->indexes_[ce.idxNo]->Fields() : nullptr;

		if (keyType != KeyValueUndefined) {
			for (auto &key : ce.values) {
				key.convert(keyType, &ns_->payloadType_, fields);
			}
		}
	}
}

void NsSelecter::prepareIteratorsForSelectLoop(const QueryEntries &entries, RawQueryResult &result, unsigned sortId, bool is_ft) {
	bool fullText = false;
	for (size_t i = 0; i < entries.size(); ++i) {
		const QueryEntry &qe(entries[i]);
		TagsPath tagsPath;
		SelectKeyResults selectResults;
		bool sparseIndex = false;
		bool byJsonPath = (qe.idxNo == IndexValueType::SetByJsonPath);
		if (byJsonPath) {
			FieldsSet fields;
			tagsPath = ns_->tagsMatcher_.path2tag(qe.index);
			fields.push_back(tagsPath);

			SelectKeyResult comparisonResult;
			comparisonResult.comparators_.push_back(Comparator(qe.condition, KeyValueUndefined, qe.values, false, qe.distinct,
															   ns_->payloadType_, fields, nullptr, CollateOpts()));
			selectResults.push_back(comparisonResult);
		} else {
			auto &index = ns_->indexes_[qe.idxNo];
			fullText = isFullText(index->Type());
			sparseIndex = index->Opts().IsSparse();

			Index::ResultType type = ns_->sortOrdersBuilt_ ? Index::Optimal : Index::DisableIdSetCache;
			if (is_ft && qe.distinct) throw Error(errQueryExec, "distinct and full text - can't do it");
			if (is_ft)
				type = Index::ForceComparator;
			else if (qe.distinct)
				type = Index::ForceIdset;

			auto ctx = fnc_ ? fnc_->CreateCtx(qe.idxNo) : BaseFunctionCtx::Ptr{};
			if (ctx && ctx->type == BaseFunctionCtx::kFtCtx) ft_ctx_ = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);

			if (index->Opts().GetCollateMode() == CollateUTF8 || fullText) {
				for (auto &key : qe.values) key.EnsureUTF8();
			}
			PerfStatCalculatorST calc(index->GetSelectPerfCounter(), ns_->enablePerfCounters_);
			selectResults = index->SelectKey(qe.values, qe.condition, sortId, type, ctx);
		}
		for (SelectKeyResult &res : selectResults) {
			switch (qe.op) {
				case OpOr:
					if (!result.size()) throw Error(errQueryExec, "OR operator in first condition");
					if (byJsonPath || sparseIndex) {
						result.back().Append(res);
					} else {
						result.back().AppendAndBind(res, ns_->payloadType_, qe.idxNo);
					}
					result.back().distinct |= qe.distinct;
					result.back().name += " OR " + qe.index;
					break;
				case OpNot:
				case OpAnd:
					result.push_back(SelectIterator(res, qe.op, qe.distinct, qe.index, fullText));
					if (!byJsonPath && !sparseIndex) {
						result.back().Bind(ns_->payloadType_, qe.idxNo);
					}
					break;
				default:
					throw Error(errQueryExec, "Unknown operator (code %d) in condition", qe.op);
			}
			if (fullText) {
				result.back().SetUnsorted();
			}
		}
	}
}

void NsSelecter::prepareEqualPositionComparator(const Query &query, const QueryEntries &entries, RawQueryResult &result) {
	if (query.equalPositions_.empty()) return;
	for (const EqualPosition &ep : query.equalPositions_) {
		const QueryEntry &firstQe(entries[ep[0]]);
		KeyValueType type = firstQe.values.size() ? firstQe.values[0].Type() : KeyValueNull;
		Comparator cmp(firstQe.condition, type, firstQe.values, true, firstQe.distinct, ns_->payloadType_, FieldsSet({firstQe.idxNo}));
		for (const QueryEntry &qe : entries) {
			if (qe.idxNo == IndexValueType::SetByJsonPath) {
				cmp.BindEqualPosition(ns_->tagsMatcher_.path2tag(qe.index), qe.values, qe.condition);
			} else if (ns_->indexes_[qe.idxNo]->Opts().IsSparse()) {
				const TagsPath &tp = ns_->indexes_[qe.idxNo]->Fields().getTagsPath(0);
				cmp.BindEqualPosition(tp, qe.values, qe.condition);
			} else {
				cmp.BindEqualPosition(qe.idxNo, qe.values, qe.condition);
			}
		}
		result.push_back(SelectIterator());
		result.back().comparators_.push_back(std::move(cmp));
		result.back().distinct = false;
	}
}

bool NsSelecter::proccessJoin(SelectCtx &sctx, IdType properRowId, bool found, bool match, bool hasInnerJoin) {
	// inner join process
	ConstPayload pl(ns_->payloadType_, ns_->items_[properRowId]);

	if (hasInnerJoin) {
		for (size_t i = 0; i < sctx.joinedSelectors->size(); i++) {
			auto &joinedSelector = sctx.joinedSelectors->at(i);
			bool res = false;
			joinedSelector.called++;

			if (joinedSelector.type == JoinType::InnerJoin) {
				if (found) {
					res = joinedSelector.func(properRowId, sctx.nsid, pl, match);
					found &= res;
				}
			}
			if (joinedSelector.type == JoinType::OrInnerJoin) {
				if (!found || !joinedSelector.nodata) {
					res = joinedSelector.func(properRowId, sctx.nsid, pl, match);
					found |= res;
				}
			}
			if (res) {
				joinedSelector.matched++;
			}
			// If not found, and next op is not OR, then ballout
			if (!found && !(i + 1 < sctx.joinedSelectors->size() && sctx.joinedSelectors->at(i + 1).type == JoinType::OrInnerJoin)) {
				break;
			}
		}
	}
	// left join process
	if (match && found)
		for (auto &joinedSelector : *sctx.joinedSelectors)
			if (joinedSelector.type == JoinType::LeftJoin) joinedSelector.func(properRowId, sctx.nsid, pl, match);
	return found;
}

template <bool reverse, bool hasComparators, bool hasScan>
void NsSelecter::selectLoop(LoopCtx &ctx, QueryResults &result) {
	unsigned start = 0;
	unsigned count = UINT_MAX;
	SelectCtx &sctx = ctx.sctx;

	if (!sctx.isForceAll) {
		start = sctx.query.start;
		count = sctx.query.count;
	}
	auto aggregators = getAggregators(sctx.query);
	// do not calc total by loop, if we have only 1 condition with 1 idset
	bool calcTotal = ctx.calcTotal && (ctx.qres->size() > 1 || hasComparators || (*ctx.qres)[0].size() > 1);

	// reserve queryresults, if we have only 1 condition with 1 idset
	if (ctx.qres->size() == 1 && (*ctx.qres)[0].size() == 1) {
		unsigned reserve = std::min(unsigned(ctx.qres->at(0).GetMaxIterations()), count);
		result.Items().reserve(reserve);
	}

	bool finish = (count == 0) && !sctx.reqMatchedOnceFlag && !calcTotal;

	bool hasInnerJoin = false;
	if (sctx.joinedSelectors) {
		for (auto &joinedSelector : *sctx.joinedSelectors)
			hasInnerJoin |= (joinedSelector.type == JoinType::InnerJoin || joinedSelector.type == JoinType::OrInnerJoin);
	}

	bool isUnordered = false;
	bool multisortFinished = true;
	bool multiSortByOrdered = false;
	Index *firstSortIndex = nullptr;
	SelectCtx::SortingCtx::Entry *sortCtx = nullptr;
	bool multiSort = sctx.sortingCtx.entries.size() > 1;
	if (!sctx.sortingCtx.entries.empty()) {
		sortCtx = &sctx.sortingCtx.entries[0];
		isUnordered = !sortCtx->index;
		if (sortCtx->index) {
			firstSortIndex = sortCtx->index;
			multiSortByOrdered = multiSort && firstSortIndex;
			multisortFinished = !(multiSortByOrdered && count > 0);
		}
	}

	VariantArray prevValues;
	size_t multisortLimitLeft = 0, multisortLimitRight = 0;

	// TODO: nested conditions support. Like (A  OR B OR C) AND (X OR Z)
	assert(!firstSortIndex || (firstSortIndex->IsOrdered() && ns_->sortOrdersBuilt_));
	auto &first = *ctx.qres->begin();
	IdType rowId = first.Val();
	while (first.Next(rowId) && !finish) {
		rowId = first.Val();
		IdType properRowId = rowId;

		if (hasScan && ns_->items_[properRowId].IsFree()) continue;
		if (firstSortIndex) {
			assert(firstSortIndex->SortOrders().size() > static_cast<size_t>(rowId));
			properRowId = firstSortIndex->SortOrders()[rowId];
		}

		bool found = true;
		assert(static_cast<size_t>(properRowId) < ns_->items_.size());
		PayloadValue &pv = ns_->items_[properRowId];
		assert(pv.Ptr());
		for (auto cur = ctx.qres->begin() + 1; cur != ctx.qres->end(); cur++) {
			if (!hasComparators || !cur->TryCompare(pv, properRowId)) {
				while (((reverse && cur->Val() > rowId) || (!reverse && cur->Val() < rowId)) && cur->Next(rowId)) {
				};
				if (cur->End()) {
					finish = true;
					found = false;
				} else if ((reverse && cur->Val() < rowId) || (!reverse && cur->Val() > rowId)) {
					found = false;
				}
			}
			bool isNot = cur->op == OpNot;
			if ((isNot && found) || (!isNot && !found)) {
				found = false;
				for (; cur != ctx.qres->end(); cur++) {
					if (cur->comparators_.size() || cur->op == OpNot || cur->End()) continue;
					if (reverse && cur->Val() < rowId) rowId = cur->Val() + 1;
					if (!reverse && cur->Val() > rowId) rowId = cur->Val() - 1;
				}
				break;
			} else if (isNot && !found) {
				found = true;
				finish = false;
			}
		}

		if (found && sctx.joinedSelectors) {
			found = proccessJoin(sctx, properRowId, found, !start && count, hasInnerJoin);
		}

		if (found) {
			sctx.matchedAtLeastOnce = true;
			uint8_t proc = ft_ctx_ ? ft_ctx_->Proc(first.Pos()) : 0;
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			for (SelectIterator &r : *ctx.qres) {
				if (r.distinct) r.ExcludeLastSet();
			}
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

	if (multiSort || isUnordered) {
		int endPos = result.Items().size();
		if (isUnordered) {
			endPos = std::min(sctx.query.count + sctx.query.start, result.Items().size());
		}
		ItemIterator first = result.Items().begin();
		ItemIterator last = result.Items().begin() + endPos;
		ItemIterator end = result.Items().end();
		applyGeneralSort(first, last, end, sctx);

		if (!ctx.sctx.query.forcedSortOrder.empty()) {
			applyCustomSort(result.Items(), sctx);
		}

		const size_t offset = isUnordered ? sctx.query.start : multisortLimitLeft;
		setLimitAndOffset(result.Items(), offset, sctx.query.count);
	}

	for (auto &aggregator : aggregators) {
		result.aggregationResults.push_back(aggregator.GetResult());
	}

	// Get total count for simple query with 1 condition and 1 idset
	if (ctx.calcTotal && !calcTotal) {
		if (sctx.query.entries.size()) {
			result.totalCount = (*ctx.qres)[0].GetMaxIterations();
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
	} else if (sctx.preResult && sctx.preResult->mode == SelectCtx::PreResult::ModeBuild) {
		sctx.preResult->ids.Add(rowId, IdSet::Unordered, 0);
	} else {
		result.Add({properRowId, ns_->items_[properRowId], proc, sctx.nsid});
	}
}

h_vector<Aggregator, 4> NsSelecter::getAggregators(const Query &q) {
	h_vector<Aggregator, 4> ret;

	for (auto &ag : q.aggregations_) {
		ret.push_back(Aggregator(ag.type_, ag.index_));
		int idx = -1;

		if (ns_->getIndexByName(ag.index_, idx)) {
			if (ns_->indexes_[idx]->Opts().IsSparse()) {
				ret.back().Bind(ns_->payloadType_, -1, ns_->indexes_[idx]->Fields().getTagsPath(0));
			} else {
				ret.back().Bind(ns_->payloadType_, idx, TagsPath());
			}
		} else {
			ret.back().Bind(ns_->payloadType_, -1, ns_->tagsMatcher_.path2tag(ag.index_));
		}
	}

	return ret;
}

void NsSelecter::substituteCompositeIndexes(QueryEntries &entries) {
	FieldsSet fields;
	for (auto cur = entries.begin(), first = entries.begin(); cur != entries.end(); cur++) {
		if ((cur->op != OpAnd) || (cur->condition != CondEq) || cur->idxNo >= ns_->payloadType_.NumFields() || cur->idxNo < 0) {
			// If query already rewritten, then copy current unmatched part
			first = cur + 1;
			fields.clear();
			continue;
		}
		fields.push_back(cur->idxNo);
		int found = getCompositeIndex(fields);
		if ((found >= 0) && !isFullText(ns_->indexes_[found]->Type())) {
			// composite idx found: replace conditions
			PayloadValue d(ns_->payloadType_.TotalSize());
			Payload pl(ns_->payloadType_, d);
			for (auto e = first; e != cur + 1; e++) {
				if (ns_->indexes_[found]->Fields().contains(e->idxNo)) {
					pl.Set(e->idxNo, {e->values[0]});
				} else
					*first++ = *e;
			}
			QueryEntry ce(OpAnd, CondEq, ns_->indexes_[found]->Name(), found);
			ce.values.push_back(Variant(d));
			*first++ = ce;
			cur = entries.erase(first, cur + 1);
			first = cur--;
			fields.clear();
		}
	}
}

SortingEntries NsSelecter::detectOptimalSortOrder(const QueryEntries &entries) {
	Index *maxIdx = nullptr;
	for (auto c = entries.begin(); c != entries.end(); c++) {
		if (((c->idxNo != IndexValueType::SetByJsonPath) && (c->condition == CondGe || c->condition == CondGt || c->condition == CondLe ||
															 c->condition == CondLt || c->condition == CondRange)) &&
			!c->distinct && ns_->indexes_[c->idxNo]->IsOrdered() && !ns_->indexes_[c->idxNo]->Opts().IsArray()) {
			if (!maxIdx || ns_->indexes_[c->idxNo]->Size() > maxIdx->Size()) {
				maxIdx = ns_->indexes_[c->idxNo].get();
			}
		}
	}
	if (maxIdx) {
		SortingEntries sortingEntries;
		sortingEntries.push_back({maxIdx->Name(), false});
		return sortingEntries;
	}
	return SortingEntries();
}

int NsSelecter::getCompositeIndex(const FieldsSet &fields) {
	if (fields.getTagsPathsLength() == 0) {
		for (int i = ns_->indexes_.firstCompositePos(); i < ns_->indexes_.totalSize(); i++) {
			if (ns_->indexes_[i]->Fields().contains(fields)) return i;
		}
	}
	return -1;
}

bool NsSelecter::mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs) {
	if ((lhs->condition == CondEq || lhs->condition == CondSet) && (rhs->condition == CondEq || rhs->condition == CondSet)) {
		// intersect 2 queryenries on same index
		std::sort(lhs->values.begin(), lhs->values.end());
		lhs->values.erase(std::unique(lhs->values.begin(), lhs->values.end()), lhs->values.end());

		std::sort(rhs->values.begin(), rhs->values.end());
		rhs->values.erase(std::unique(rhs->values.begin(), rhs->values.end()), rhs->values.end());

		auto end =
			std::set_intersection(lhs->values.begin(), lhs->values.end(), rhs->values.begin(), rhs->values.end(), lhs->values.begin());
		lhs->values.resize(end - lhs->values.begin());
		lhs->condition = (lhs->values.size() == 1) ? CondEq : CondSet;
		lhs->distinct |= rhs->distinct;
		return true;
	} else if (rhs->condition == CondAny) {
		lhs->distinct |= rhs->distinct;
		return true;
	} else if (lhs->condition == CondAny) {
		rhs->distinct |= lhs->distinct;
		*lhs = std::move(*rhs);
		return true;
	}

	return false;
}

KeyValueType NsSelecter::detectQueryEntryIndexType(const QueryEntry &qentry) const {
	KeyValueType keyType = KeyValueUndefined;
	for (auto &item : ns_->items_) {
		if (!item.IsFree()) {
			Payload pl(ns_->payloadType_, item);
			VariantArray values;
			pl.GetByJsonPath(qentry.index, ns_->tagsMatcher_, values, KeyValueUndefined);
			if (values.size() > 0) keyType = values[0].Type();
			break;
		}
	}
	return keyType;
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
