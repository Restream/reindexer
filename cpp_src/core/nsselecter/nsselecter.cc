#include <sstream>

#include "core/cjson/jsondecoder.h"
#include "core/namespace.h"
#include "nsselecter.h"
#include "tools/stringstools.h"

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::next;
using std::shared_ptr;
using std::string;
using std::stringstream;

namespace reindexer {
#define TIMEPOINT(n)                                  \
	std::chrono::high_resolution_clock::time_point n; \
	if (enableTiming) n = high_resolution_clock::now()
void NsSelecter::operator()(QueryResults &result, const SelectCtx &ctx) {
	Index *sortIndex = nullptr;
	bool unorderedIndexSort = false;
	int collateMode = CollateMode::CollateNone;
	bool forceAllOrigin = ctx.isForceAll;
	bool forcedSort = !ctx.query.forcedSortOrder.empty();

	bool enableTiming = ctx.query.debugLevel >= LogInfo;

	TIMEPOINT(tmStart);

	bool needCalcTotal = ctx.query.calcTotal == ModeAccurateTotal;

	if (ctx.query.calcTotal == ModeCachedTotal) {
		auto cached = ns_->queryCache->Get({ctx.query});
		if (cached.key) {
			result.totalCount = cached.val.total_count;
			logPrintf(LogTrace, "[*] using value from cache: %d\t namespace: %s\n", result.totalCount, ns_->name.c_str());
		} else {
			logPrintf(LogTrace, "[*] value for cache will be calculated by query. namespace: %s\n", ns_->name.c_str());
			needCalcTotal = true;
		}
	}

	QueryEntries whereEntries = lookupQueryIndexes(ctx.query.entries);

	bool containsFullText = isContainsFullText(whereEntries);
	if (!containsFullText) substituteCompositeIndexes(whereEntries);
	auto &sortBy = (containsFullText || ctx.query.sortBy.length()) ? ctx.query.sortBy : getOptimalSortOrder(whereEntries);

	// Check if commit needed
	if (!whereEntries.empty() || !sortBy.empty()) {
		FieldsSet prepareIndexes;
		for (auto &entry : whereEntries) prepareIndexes.push_back(entry.idxNo);
		ns_->commit(Namespace::NSCommitContext(*ns_, CommitContext::MakeIdsets | (sortBy.length() ? CommitContext::MakeSortOrders : 0),
											   &prepareIndexes),
					ctx.lockUpgrader);
	}

	if (!sortBy.empty()) {
		if (containsFullText) {
			logPrintf(LogError,
					  "Full text search on '%s.%s' with foreing sort on '%s' is not "
					  "supported now. sort will be ignored\n",
					  ns_->name.c_str(), ns_->indexes_[whereEntries[0].idxNo]->name.c_str(), ctx.query.sortBy.c_str());
		} else {
			// Query is sorted. Search for sort index
			sortIndex = forcedSort ? nullptr : ns_->indexes_[ns_->getIndexByName(sortBy)].get();
			if (sortIndex && !sortIndex->IsOrdered()) {
				const_cast<SelectCtx &>(ctx).isForceAll = true;
				unorderedIndexSort = true;
				collateMode = sortIndex->opts_.CollateMode;
				sortIndex = nullptr;
			}
		}
	}

	// Add preresults with common conditions of join Queres
	RawQueryResult qres;
	if (ctx.preResult) {
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(ctx.preResult));
		qres.push_back(SelectIterator(res, OpAnd, false, "-preresult"));
	}

	TIMEPOINT(tm1);

	selectWhere(whereEntries, qres, sortIndex ? sortIndex->sort_id : 0, containsFullText);

	TIMEPOINT(tm2);

	bool haveComparators = false, haveScan = false, haveIdsets = false;
	bool reverse = ctx.query.sortDirDesc && sortIndex && !containsFullText;

	for (auto &r : qres)
		if (r.comparators_.size())
			haveComparators = true;
		else
			haveIdsets = true;

	if (qres.empty() || (!containsFullText && (!haveIdsets || qres[0].op == OpNot))) {
		// special case - no condition or first result have comparator or not
		SelectKeyResult res;
		res.push_back(SingleSelectKeyResult(0, IdType(sortIndex ? sortIndex->sort_orders.size() : ns_->items_.size())));
		qres.insert(qres.begin(), SelectIterator(res, OpAnd, false, "-scan", true));
		haveScan = sortIndex && !forcedSort ? false : true;
	}

	// Get maximum iterations count, for right calculation comparators costs
	int iters = INT_MAX;
	for (auto r = qres.begin(); r != qres.end(); ++r) {
		int cur = r->GetMaxIterations();
		if (!r->comparators_.size() && cur && cur < iters) iters = cur;
	}

	// Sort by cost
	std::sort(qres.begin(), qres.end(),
			  [iters](const SelectIterator &i1, const SelectIterator &i2) { return i1.Cost(iters) < i2.Cost(iters); });

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

	result.ctxs.push_back(
		QueryResults::Context(ns_->payloadType_, ns_->tagsMatcher_, JsonPrintFilter(ns_->tagsMatcher_, ctx.query.selectFilter_)));

	TIMEPOINT(tm3);
	LoopCtx lctx(ctx);
	lctx.sortIndex = sortIndex;
	lctx.qres = &qres;
	lctx.ftIndex = containsFullText;
	lctx.calcTotal = needCalcTotal;
	result.haveProcent = containsFullText;
	if (reverse && haveComparators && haveScan) selectLoop<true, true, true>(lctx, result);
	if (!reverse && haveComparators && haveScan) selectLoop<false, true, true>(lctx, result);
	if (reverse && !haveComparators && haveScan) selectLoop<true, false, true>(lctx, result);
	if (!reverse && !haveComparators && haveScan) selectLoop<false, false, true>(lctx, result);
	if (reverse && haveComparators && !haveScan) selectLoop<true, true, false>(lctx, result);
	if (!reverse && haveComparators && !haveScan) selectLoop<false, true, false>(lctx, result);
	if (reverse && !haveComparators && !haveScan) selectLoop<true, false, false>(lctx, result);
	if (!reverse && !haveComparators && !haveScan) selectLoop<false, false, false>(lctx, result);

	TIMEPOINT(tm4);

	if (ctx.query.debugLevel >= LogInfo) {
		logPrintf(LogInfo, ctx.query.Dump().c_str());
		logPrintf(LogInfo,
				  "Got %d items in %d µs [prepare %d µs, select %d µs, postprocess "
				  "%d µs loop %d µs]",
				  int(result.size()), duration_cast<microseconds>(tm4 - tmStart).count(),
				  duration_cast<microseconds>(tm1 - tmStart).count(), duration_cast<microseconds>(tm2 - tm1).count(),
				  duration_cast<microseconds>(tm3 - tm2).count(), duration_cast<microseconds>(tm4 - tm3).count());
		if (ctx.query.debugLevel >= LogTrace) {
			for (auto &r : qres)
				logPrintf(LogInfo, "%s: %d idsets, %d comparators, cost %g, matched %d", r.name.c_str(), r.size(), r.comparators_.size(),
						  r.Cost(iters), r.GetMatchedCount());
			result.Dump();
		}
	}

	if (unorderedIndexSort) {
		applyGeneralSort(result, ctx, collateMode);
	}

	if (!ctx.query.forcedSortOrder.empty()) {
		applyCustomSort(result, ctx);
	}

	if (unorderedIndexSort && !forceAllOrigin) {
		const_cast<SelectCtx &>(ctx).isForceAll = forceAllOrigin;
		setLimitsAfterSortByUnorderedIndex(result, ctx);
	}

	if (ctx.query.calcTotal == ModeCachedTotal && needCalcTotal) {
		logPrintf(LogTrace, "[*] put totalCount value into query cache: %d\t namespace: %s\n", result.totalCount, ns_->name.c_str());
		ns_->queryCache->Put({ctx.query}, {static_cast<size_t>(result.totalCount)});
	}
}

void NsSelecter::applyCustomSort(QueryResults &queryResult, const SelectCtx &ctx) {
	if (ctx.query.mergeQueries_.size() > 1) throw Error(errLogic, "Force sort could not be applied to 'merged' queries.");

	auto payloadType = ns_->payloadType_;
	const string &fieldName = ctx.query.sortBy;
	int idx = payloadType->FieldByName(fieldName);
	if (payloadType->Field(idx).IsArray()) throw Error(errQueryExec, "This type of sorting cannot be applied to a field of array type.");

	QueryResults::difference_type cost = 0;
	KeyValueType fieldType = payloadType->Field(idx).Type();
	fast_hash_map<KeyValue, QueryResults::difference_type> sortMap;
	KeyValues keyValues = ctx.query.forcedSortOrder;  // make a copy of 'forcedSortOrder', may be mark it as 'mutable'?
	for (auto &value : keyValues) {
		value.convert(fieldType);
		sortMap.insert({value, cost});
		cost++;
	}

	KeyRefs keyRefs;
	auto sortEnd = std::stable_partition(queryResult.begin(), queryResult.end(), [&sortMap, &payloadType, idx, &keyRefs](ItemRef &itemRef) {
		ConstPayload(payloadType, itemRef.value).Get(idx, keyRefs);
		return !keyRefs.empty() && (sortMap.find(keyRefs[0]) != sortMap.end());
	});

	KeyRefs firstItemValue;
	KeyRefs secondItemValue;
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
}

void NsSelecter::applyGeneralSort(QueryResults &queryResult, const SelectCtx &ctx, int collateMode) {
	if (ctx.query.mergeQueries_.size() > 1) throw Error(errLogic, "Sorting cannot be applied to merged queries.");

	auto payloadType = ns_->payloadType_;
	bool sortAsc = !ctx.query.sortDirDesc;
	const string &fieldName = ctx.query.sortBy;
	int fieldIdx = payloadType->FieldByName(fieldName);
	if (payloadType->Field(fieldIdx).IsArray()) throw Error(errQueryExec, "Sorting cannot be applied to an array field.");

	KeyRefs firstValue;
	KeyRefs secondValue;
	std::sort(queryResult.begin(), queryResult.end(),
			  [&payloadType, fieldIdx, &firstValue, &secondValue, sortAsc, collateMode](const ItemRef &lhs, const ItemRef &rhs) {
				  ConstPayload(payloadType, lhs.value).Get(fieldIdx, firstValue);
				  ConstPayload(payloadType, rhs.value).Get(fieldIdx, secondValue);

				  assert(!firstValue.empty());
				  assert(!secondValue.empty());

				  bool comparisonResult = false;
				  if (sortAsc) {
					  comparisonResult = (firstValue[0].Compare(secondValue[0], collateMode) < 0);
				  } else {
					  comparisonResult = (firstValue[0].Compare(secondValue[0], collateMode) > 0);
				  }
				  return comparisonResult;
			  });
}

void NsSelecter::setLimitsAfterSortByUnorderedIndex(QueryResults &queryResult, const SelectCtx &ctx) {
	if (ctx.isForceAll) return;

	const unsigned offset = ctx.query.start;
	const unsigned limit = ctx.query.count;
	const unsigned totalRows = queryResult.size();

	if (offset > 0) {
		queryResult.erase(queryResult.begin(), queryResult.begin() + offset);
	}

	if (totalRows > limit) {
		queryResult.erase(queryResult.begin() + limit, queryResult.end());
	}
}

bool NsSelecter::isContainsFullText(const QueryEntries &entries) {
	for (auto &entrie : entries) {
		if (ns_->indexes_[entrie.idxNo]->type == IndexFullText || ns_->indexes_[entrie.idxNo]->type == IndexNewFullText ||
			ns_->indexes_[entrie.idxNo]->type == IndexCompositeText) {
			return true;
		}
	}
	return false;
}

QueryEntries NsSelecter::lookupQueryIndexes(const QueryEntries &entries) {
	int iidx[maxIndexes];
	for (auto &i : iidx) i = -1;

	QueryEntries ret;
	for (auto c = entries.begin(); c != entries.end(); c++) {
		QueryEntry cur = *c;
		if (cur.idxNo < 0) cur.idxNo = ns_->getIndexByName(cur.index);
		if (cur.idxNo < ns_->payloadType_->NumFields())
			for (auto &key : cur.values) key.convert(ns_->indexes_[cur.idxNo]->KeyType());

		// try merge entries with AND opetator
		auto nextc = c;
		nextc++;
		if (cur.op == OpAnd && (nextc == entries.end() || nextc->op == OpAnd)) {
			if (iidx[cur.idxNo] >= 0) {
				if (mergeQueryEntries(&ret[iidx[cur.idxNo]], &cur)) continue;

			} else {
				iidx[cur.idxNo] = ret.size();
			}
		}
		ret.push_back(cur);
	};
	return ret;
}
void NsSelecter::selectWhere(const QueryEntries &entries, RawQueryResult &result, unsigned sortId, bool is_ft) {
	for (auto &qe : entries) {
		bool is_ft_current = (ns_->indexes_[qe.idxNo]->type == IndexFullText || ns_->indexes_[qe.idxNo]->type == IndexNewFullText ||
							  ns_->indexes_[qe.idxNo]->type == IndexCompositeText);
		Index::ResultType type = Index::Optimal;
		if (is_ft && qe.distinct) throw Error(errQueryExec, "distinct and full text - can't do it");
		if (is_ft)
			type = Index::ForceComparator;
		else if (qe.distinct)
			type = Index::ForceIdset;
		auto select_result = ns_->indexes_[qe.idxNo]->SelectKey(qe.values, qe.condition, sortId, type);
		if (select_result.ctx) {
			// We can't use more then one ctx rigth now becouse full text always use
			// ctxs[0] If you wan't to add ctx to other index plz fix select loop ctx
			// logic
			assert(result.ctxs.empty());
			result.ctxs.push_back(select_result.ctx);
		}
		for (auto res : select_result) {
			switch (qe.op) {
				case OpOr:
					if (!result.size()) throw Error(errQueryExec, "OR operator in first condition");
					result.back().AppendAndBind(res, ns_->payloadType_.get(), qe.idxNo);
					result.back().distinct |= qe.distinct;
					result.back().name += " OR " + qe.index;
					break;
				case OpNot:
				case OpAnd:
					result.push_back(SelectIterator(res, qe.op, qe.distinct, qe.index, is_ft_current));
					result.back().Bind(ns_->payloadType_.get(), qe.idxNo);
					break;
				default:
					throw Error(errQueryExec, "Unknown operator (code %d) in condition", qe.op);
			}
			if (is_ft_current) {
				result.back().SetUnsorted();
			}
		}
	}
}

template <bool reverse, bool haveComparators, bool haveScan>
void NsSelecter::selectLoop(const LoopCtx &ctx, QueryResults &result) {
	unsigned start = 0;
	unsigned count = UINT_MAX;
	if (!ctx.isForceAll) {
		start = ctx.query.start;
		count = ctx.query.count;
	}
	bool finish = (count == 0);
	auto aggregators = getAggregators(ctx.query);
	// do not calc total by loop, if we have only 1 condition with 1 idset
	bool calcTotal = ctx.calcTotal /*.query.calcTotal*/ && (ctx.qres->size() > 1 || haveComparators || (*ctx.qres)[0].size() > 1);

	bool haveInnerJoin = false;
	if (ctx.joinedSelectors) {
		for (auto &joinedSelector : *ctx.joinedSelectors)
			haveInnerJoin |= (joinedSelector.type == JoinType::InnerJoin || joinedSelector.type == JoinType::OrInnerJoin);
	}
	// TODO: nested conditions support. Like (A  OR B OR C) AND (X OR Z)
	auto &first = *ctx.qres->begin();
	IdType val = first.Val();
	assert(!ctx.sortIndex || ctx.sortIndex->IsOrdered());
	while (first.Next(val) && !finish) {
		val = first.Val();
		IdType realVal = val;

		if (haveScan && ns_->items_[realVal].IsFree()) continue;
		if (haveComparators && ctx.sortIndex) {
			assert(ctx.sortIndex->sort_orders.size() > static_cast<size_t>(val));
			realVal = ctx.sortIndex->sort_orders[val];
		}

		bool found = true;
		for (auto cur = ctx.qres->begin() + 1; cur != ctx.qres->end(); cur++) {
			if (!haveComparators || !cur->TryCompare(&ns_->items_[realVal], realVal)) {
				while (((reverse && cur->Val() > val) || (!reverse && cur->Val() < val)) && cur->Next(val)) {
				};

				if (cur->End()) {
					finish = true;
					found = false;
				} else if ((reverse && cur->Val() < val) || (!reverse && cur->Val() > val)) {
					found = false;
				}
			}
			bool isNot = cur->op == OpNot;
			if ((isNot && found) || (!isNot && !found)) {
				found = false;
				for (; cur != ctx.qres->end(); cur++) {
					if (cur->comparators_.size() || cur->op == OpNot || cur->End()) continue;
					if (reverse && cur->Val() < val) val = cur->Val() + 1;
					if (!reverse && cur->Val() > val) val = cur->Val() - 1;
				}
				break;
			} else if (isNot && !found) {
				found = true;
				finish = false;
			}
		}
		if (found && ctx.joinedSelectors && ctx.joinedSelectors->size()) {
			bool match = !start && count;
			if (!haveComparators && ctx.sortIndex) {
				assert(ctx.sortIndex->sort_orders.size() > static_cast<size_t>(val));
				realVal = ctx.sortIndex->sort_orders[val];
			}

			// inner join process
			ConstPayload pl(ns_->payloadType_, ns_->items_[realVal]);
			if (haveInnerJoin && ctx.joinedSelectors) {
				for (auto &joinedSelector : *ctx.joinedSelectors) {
					if (joinedSelector.type == JoinType::InnerJoin) found &= joinedSelector.func(realVal, pl, match);
					if (joinedSelector.type == JoinType::OrInnerJoin) found |= joinedSelector.func(realVal, pl, match);
				}
			}

			// left join process
			if (match && found && ctx.joinedSelectors)
				for (auto &joinedSelector : *ctx.joinedSelectors)
					if (joinedSelector.type == JoinType::LeftJoin) joinedSelector.func(realVal, pl, match);
		}

		if (found) {
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			for (auto &r : *ctx.qres)
				if (r.distinct) r.ExcludeLastSet();

			if (start)
				--start;
			else if (count) {
				if (!haveComparators && ctx.sortIndex) {
					assert(ctx.sortIndex->sort_orders.size() > static_cast<size_t>(val));
					realVal = ctx.sortIndex->sort_orders[val];
				}
				--count;
				uint8_t proc = 0;
				if (ctx.ftIndex && !ctx.qres->ctxs.empty() && ctx.qres->ctxs[0]) {
					proc = ctx.qres->ctxs[0]->Proc(first.Pos());
				}
				if (aggregators.size()) {
					for (auto &aggregator : aggregators) aggregator.Aggregate(ns_->items_[realVal], realVal);
				} else if (ctx.rsltAsSrtOrdrs) {
					result.Add({val, 0, PayloadValue(), proc, ctx.nsid});
				} else {
					result.Add({realVal, ns_->items_[realVal].GetVersion(), ns_->items_[realVal], proc, ctx.nsid});
				}

				if (!count && !calcTotal) break;
			}
			if (calcTotal) result.totalCount++;
		}
	}
	for (auto &aggregator : aggregators) {
		result.aggregationResults.push_back(aggregator.GetResult());
	}

	// Get total count for simple query with 1 condition and 1 idset
	if (ctx.calcTotal && !calcTotal) result.totalCount = (*ctx.qres)[0].GetMaxIterations();
}

h_vector<Aggregator, 4> NsSelecter::getAggregators(const Query &q) {
	h_vector<Aggregator, 4> ret;

	for (auto &ag : q.aggregations_) {
		int idx = ns_->getIndexByName(ag.index_);
		ret.push_back(Aggregator(ns_->indexes_[idx]->KeyType(), ns_->indexes_[idx]->opts_.IsArray, nullptr, ag.type_));
		ret.back().Bind(ns_->payloadType_.get(), idx);
	}

	return ret;
}
void NsSelecter::substituteCompositeIndexes(QueryEntries &entries) {
	FieldsSet fields;
	for (auto cur = entries.begin(), first = entries.begin(); cur != entries.end(); cur++) {
		if (cur->op != OpAnd || (cur->condition != CondEq)) {
			// If query already rewritten, then copy current unmatached part
			first = cur + 1;
			fields.clear();
			continue;
		}
		fields.push_back(cur->idxNo);
		int found = getCompositeIndex(fields);
		if (found >= 0) {
			// composite idx found: replace conditions
			PayloadValue d(ns_->payloadType_->TotalSize());
			Payload pl(ns_->payloadType_, d);
			for (auto e = first; e != cur + 1; e++) {
				if (ns_->indexes_[found]->fields_.contains(e->idxNo)) {
					KeyRefs kr;
					kr.push_back(KeyRef(e->values[0]));
					pl.Set(e->idxNo, kr);
				} else
					*first++ = *e;
			}
			QueryEntry ce(OpAnd, CondEq, ns_->indexes_[found]->name, found);
			ce.values.push_back(KeyValue(d));
			*first++ = ce;
			cur = entries.erase(first, cur + 1);
			first = cur--;
			fields.clear();
		}
	}
}

const string &NsSelecter::getOptimalSortOrder(const QueryEntries &entries) {
	Index *maxIdx = nullptr;
	static string no = "";
	for (auto c = entries.begin(); c != entries.end(); c++) {
		if ((c->condition == CondGe || c->condition == CondGt || c->condition == CondLe || c->condition == CondLt ||
			 c->condition == CondRange) &&
			!c->distinct && ns_->indexes_[c->idxNo]->sort_id) {
			if (!maxIdx || ns_->indexes_[c->idxNo]->Size() > maxIdx->Size()) {
				maxIdx = ns_->indexes_[c->idxNo].get();
			}
		}
	}

	return maxIdx ? maxIdx->name : no;
}

int NsSelecter::getCompositeIndex(const FieldsSet &fields) {
	for (size_t f = ns_->payloadType_->NumFields(); f < ns_->indexes_.size(); f++)
		if (ns_->indexes_[f]->fields_.contains(fields)) return f;
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
}  // namespace reindexer
