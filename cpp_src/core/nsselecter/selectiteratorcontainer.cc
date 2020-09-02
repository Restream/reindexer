#include "selectiteratorcontainer.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/nsselecter.h"
#include "core/rdxcontext.h"

namespace reindexer {

void SelectIteratorContainer::SortByCost(int expectedIterations) {
	thread_local h_vector<unsigned, 16> indexes;
	thread_local h_vector<double, 16> costs;
	if (indexes.size() < container_.size()) {
		indexes.resize(container_.size());
		costs.resize(container_.size());
	}
	for (size_t i = 0; i < container_.size(); ++i) {
		indexes[i] = i;
	}
	sortByCost(indexes, costs, 0, container_.size(), expectedIterations);
	for (size_t i = 0; i < container_.size(); ++i) {
		if (indexes[i] != i) {
			size_t positionOfTmp = i + 1;
			for (; positionOfTmp < indexes.size(); ++positionOfTmp) {
				if (indexes[positionOfTmp] == i) break;
			}
			assert(positionOfTmp < indexes.size());
			Container::value_type tmp = std::move(container_[i]);
			container_[i] = std::move(container_[indexes[i]]);
			container_[indexes[i]] = std::move(tmp);
			indexes[positionOfTmp] = indexes[i];
		}
	}
}

void SelectIteratorContainer::sortByCost(span<unsigned> indexes, span<double> costs, unsigned from, unsigned to, int expectedIterations) {
	for (size_t cur = from, next; cur < to; cur = next) {
		next = cur + Size(indexes[cur]);
		if (!IsValue(indexes[cur])) {
			sortByCost(indexes, costs, cur + 1, next, expectedIterations);
			if (next < to && GetOperation(indexes[next]) == OpOr && IsValue(indexes[next]) && (*this)[indexes[next]].distinct) {
				throw Error(errQueryExec, "OR operator between bracket and distinct query");
			}
		} else if (next < to && GetOperation(indexes[next]) == OpOr) {
			if (IsValue(indexes[next])) {
				if ((*this)[indexes[cur]].distinct != (*this)[indexes[next]].distinct) {
					throw Error(errQueryExec, "OR operator between distinct and non distinct queries");
				}
			} else {
				if ((*this)[indexes[cur]].distinct) {
					throw Error(errQueryExec, "OR operator between distinct query and bracket");
				}
			}
		}
	}
	for (size_t cur = from, next; cur < to; cur = next) {
		next = cur + Size(indexes[cur]);
		const double cst = fullCost(indexes, cur, from, to, expectedIterations);
		for (size_t j = cur; j < next; ++j) {
			costs[indexes[j]] = cst;
		}
	}
	std::stable_sort(indexes.begin() + from, indexes.begin() + to, [&costs](unsigned i1, unsigned i2) { return costs[i1] < costs[i2]; });
	moveJoinsToTheBeginingOfORs(indexes, from, to);
}

void SelectIteratorContainer::moveJoinsToTheBeginingOfORs(span<unsigned> indexes, unsigned from, unsigned to) {
	size_t firstNotJoin = from;
	for (size_t cur = from, next; cur < to; cur = next) {
		next = cur + Size(indexes[cur]);
		if (GetOperation(indexes[cur]) != OpOr) {
			firstNotJoin = cur;
		} else if (IsValue(indexes[cur]) && !(*this)[indexes[cur]].joinIndexes.empty()) {
			while (firstNotJoin < cur && (GetOperation(firstNotJoin) == OpNot ||
										  (IsValue(indexes[firstNotJoin]) && !(*this)[indexes[firstNotJoin]].joinIndexes.empty()))) {
				firstNotJoin += Size(indexes[firstNotJoin]);
			}
			if (firstNotJoin < cur) {
				SetOperation(GetOperation(indexes[firstNotJoin]), indexes[cur]);
				SetOperation(OpOr, indexes[firstNotJoin]);
				size_t tmp = indexes[cur];
				for (size_t i = cur; i > firstNotJoin; --i) indexes[i] = indexes[i - 1];
				indexes[firstNotJoin] = tmp;
			}
			++firstNotJoin;
		}
	}
}

double SelectIteratorContainer::cost(span<unsigned> indexes, unsigned cur, int expectedIterations) const {
	if (IsValue(indexes[cur])) {
		return (*this)[indexes[cur]].Cost(expectedIterations);
	} else {
		return cost(indexes, cur + 1, cur + Size(indexes[cur]), expectedIterations);
	}
}

double SelectIteratorContainer::cost(span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const {
	double result = 0.0;
	for (size_t cur = from; cur < to; cur += Size(indexes[cur])) {
		result += cost(indexes, cur, expectedIterations);
	}
	return result;
}

double SelectIteratorContainer::fullCost(span<unsigned> indexes, unsigned cur, unsigned from, unsigned to, int expectedIterations) const {
	double result = 0.0;
	for (size_t i = from; i <= cur; i += Size(indexes[i])) {
		if (GetOperation(indexes[i]) != OpOr) from = i;
	}
	for (; from <= cur || (from < to && GetOperation(indexes[from]) == OpOr); from += Size(indexes[from])) {
		result += cost(indexes, from, expectedIterations);
	}
	return result;
}

bool SelectIteratorContainer::isIdset(const_iterator it, const_iterator end) {
	return it->operation == OpAnd && it->IsLeaf() && it->Value().comparators_.empty() &&
		   it->Value().joinIndexes.empty() &&  // !it->Value().empty() &&
		   (++it == end || it->operation != OpOr);
}

bool SelectIteratorContainer::HasIdsets() const {
	for (const_iterator it = cbegin(), end = cend(); it != end; ++it) {
		if (isIdset(it, end)) return true;
	}
	return false;
}

void SelectIteratorContainer::CheckFirstQuery() {
	for (auto it = begin(); it != end(); ++it) {
		if (isIdset(it, cend())) {
			if (it != begin()) {
				// if first idset is not on the 1-st position move it to the 1-st position
				Container::iterator src = (++it).PlainIterator() - 1;
				const Container::iterator dst = begin().PlainIterator();
				auto tmp = std::move(*src);
				for (; src != dst; --src) *src = std::move(*(src - 1));
				*dst = std::move(tmp);
			}
			return;
		}
	}
	assert(0);
}

// Let iterators choose most effective algorithm
void SelectIteratorContainer::SetExpectMaxIterations(int expectedIterations) {
	assert(!Empty());
	assert(IsIterator(0));
	for (Container::iterator it = container_.begin() + 1; it != container_.end(); ++it) {
		if (it->IsLeaf()) it->Value().SetExpectMaxIterations(expectedIterations);
	}
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry &qe, const NamespaceImpl &ns, StrictMode strictMode) {
	SelectKeyResults selectResults;

	FieldsSet fields;
	TagsPath tagsPath = ns.tagsMatcher_.path2tag(qe.index);
	if (!tagsPath.empty()) {
		SelectKeyResult comparisonResult;
		fields.push_back(tagsPath);
		comparisonResult.comparators_.emplace_back(qe.condition, KeyValueUndefined, qe.values, false, qe.distinct, ns.payloadType_, fields,
												   nullptr, CollateOpts());
		selectResults.emplace_back(std::move(comparisonResult));
	} else if (strictMode == StrictModeNone) {
		SelectKeyResult res;
		// Ignore non-index/non-existing fields
		if (qe.condition == CondEmpty) {
			res.emplace_back(SingleSelectKeyResult(IdType(0), IdType(ns.items_.size())));
		} else {
			res.emplace_back(SingleSelectKeyResult(IdType(0), IdType(0)));
		}
		selectResults.emplace_back(std::move(res));
	} else {
		throw Error(
			errParams,
			"Current query strict mode allows filtering by existing fields only. There are no fields with name '%s' in namespace '%s'",
			qe.index, ns.name_);
	}

	return selectResults;
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry &qe, bool enableSortIndexOptimize, const NamespaceImpl &ns,
															unsigned sortId, bool isQueryFt, SelectFunction::Ptr selectFnc, bool &isIndexFt,
															bool &isIndexSparse, FtCtx::Ptr &ftCtx, const RdxContext &rdxCtx) {
	auto &index = ns.indexes_[qe.idxNo];
	isIndexFt = isFullText(index->Type());
	isIndexSparse = index->Opts().IsSparse();

	Index::SelectOpts opts;
	opts.itemsCountInNamespace = ns.items_.size() - ns.free_.size();
	if (!ns.sortOrdersBuilt_) opts.disableIdSetCache = 1;
	if (isQueryFt) {
		opts.forceComparator = 1;
	}
	if (ctx_->sortingContext.isOptimizationEnabled()) {
		if (enableSortIndexOptimize) {
			opts.unbuiltSortOrders = 1;
		} else {
			opts.forceComparator = 1;
		}
	}
	if (qe.distinct) {
		opts.distinct = 1;
	}

	auto ctx = selectFnc ? selectFnc->CreateCtx(qe.idxNo) : BaseFunctionCtx::Ptr{};
	if (ctx && ctx->type == BaseFunctionCtx::kFtCtx) ftCtx = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);

	if (index->Opts().GetCollateMode() == CollateUTF8 || isIndexFt) {
		for (auto &key : qe.values) key.EnsureUTF8();
	}
	PerfStatCalculatorMT calc(index->GetSelectPerfCounter(), ns.enablePerfCounters_);
	return index->SelectKey(qe.values, qe.condition, sortId, opts, ctx, rdxCtx);
}

void SelectIteratorContainer::processJoinEntry(const QueryEntry &qe, OpType op) {
	bool newIterator = false;
	switch (op) {
		case OpAnd:
		case OpNot: {
			newIterator = true;
		} break;
		case OpOr: {
			const iterator node = lastAppendedOrClosed();
			if (node == this->end()) throw Error(errQueryExec, "OR operator in first condition or after left join");
			if (node->IsLeaf()) {
				if (node->IsRef()) {
					node->SetValue(node->Value());
				}
				node->Value().joinIndexes.push_back(qe.joinIndex);
			} else {
				newIterator = true;
			}
		} break;
	}
	if (newIterator) {
		SelectIterator it;
		it.joinIndexes.push_back(qe.joinIndex);
		Append(op, it);
	}
}

void SelectIteratorContainer::processQueryEntryResults(SelectKeyResults &selectResults, OpType op, const NamespaceImpl &ns,
													   const QueryEntry &qe, bool isIndexFt, bool isIndexSparse, bool nonIndexField) {
	for (SelectKeyResult &res : selectResults) {
		switch (op) {
			case OpOr: {
				const iterator last = lastAppendedOrClosed();
				if (last == this->end()) throw Error(errQueryExec, "OR operator in first condition or after left join ");
				if (last->IsLeaf() && !last->Value().distinct) {
					if (last->IsRef()) {
						last->SetValue(last->Value());
					}
					SelectIterator &it = last->Value();
					if (nonIndexField || isIndexSparse) {
						it.Append(res);
					} else {
						it.AppendAndBind(res, ns.payloadType_, qe.idxNo);
					}
					it.name += " or " + qe.index;
					break;
				}  // else fallthrough
			}	   // fallthrough
			case OpNot:
			case OpAnd:
				Append(op, SelectIterator(res, qe.distinct, qe.index, isIndexFt));
				if (!nonIndexField && !isIndexSparse) {
					// last appended is always a leaf
					const auto lastAppendedIt = lastAppendedOrClosed();
					if (lastAppendedIt->IsRef()) {
						lastAppendedIt->SetValue(lastAppendedIt->Value());
					}
					lastAppendedIt->Value().Bind(ns.payloadType_, qe.idxNo);
				}
				break;
			default:
				throw Error(errQueryExec, "Unknown operator (code %d) in condition", op);
		}
		if (isIndexFt) {
			// last appended is always a leaf
			lastAppendedOrClosed()->Value().SetUnsorted();
		}
	}
}

void SelectIteratorContainer::processEqualPositions(const std::multimap<unsigned, EqualPosition> &equalPositions, size_t begin, size_t end,
													const NamespaceImpl &ns, const QueryEntries &queries) {
	const auto eqPoses = equalPositions.equal_range(begin);
	for (auto it = eqPoses.first; it != eqPoses.second; ++it) {
		assert(!it->second.empty());
		const QueryEntry &firstQe(queries[it->second[0]]);
		if (firstQe.condition == CondEmpty || (firstQe.condition == CondSet && firstQe.values.empty())) {
			throw Error(errLogic, "Condition IN(with empty parameter list), IS NULL, IS EMPTY not allowed for equal position!");
		}

		KeyValueType type = firstQe.values.size() ? firstQe.values[0].Type() : KeyValueNull;
		Comparator cmp(firstQe.condition, type, firstQe.values, true, firstQe.distinct, ns.payloadType_, FieldsSet({firstQe.idxNo}));

		for (auto qeIdxIt = it->second.begin(); qeIdxIt != it->second.end(); ++qeIdxIt) {
			if (queries.GetOperation(*qeIdxIt) != OpAnd ||
				(queries.Next(*qeIdxIt) < end && queries.GetOperation(queries.Next(*qeIdxIt)) == OpOr))
				throw Error(errLogic, "Only AND operation allowed for equal position!");
			const QueryEntry &qe = queries[*qeIdxIt];
			if (qe.condition == CondEmpty || (qe.condition == CondSet && qe.values.empty())) {
				throw Error(errLogic, "Condition IN(with empty parameter list), IS NULL, IS EMPTY not allowed for equal position!");
			}
			if (qe.idxNo == IndexValueType::SetByJsonPath) {
				cmp.BindEqualPosition(ns.tagsMatcher_.path2tag(qe.index), qe.values, qe.condition);
			} else if (ns.indexes_[qe.idxNo]->Opts().IsSparse()) {
				const TagsPath &tp = ns.indexes_[qe.idxNo]->Fields().getTagsPath(0);
				cmp.BindEqualPosition(tp, qe.values, qe.condition);
			} else {
				cmp.BindEqualPosition(qe.idxNo, qe.values, qe.condition);
			}
		}

		SelectIterator selectIt;
		selectIt.comparators_.emplace_back(std::move(cmp));
		selectIt.distinct = false;
		Append(OpAnd, std::move(selectIt));
	}
}

void SelectIteratorContainer::PrepareIteratorsForSelectLoop(const QueryEntries &queries, size_t begin, size_t end,
															const std::multimap<unsigned, EqualPosition> &equalPositions, unsigned sortId,
															bool isQueryFt, const NamespaceImpl &ns, SelectFunction::Ptr selectFnc,
															FtCtx::Ptr &ftCtx, const RdxContext &rdxCtx) {
	size_t next = 0;
	bool sortIndexCreated = false;
	for (size_t i = begin; i < end; i = queries.Next(i)) {
		next = queries.Next(i);
		auto op = queries.GetOperation(i);
		if (queries.IsValue(i)) {
			const QueryEntry &qe = queries[i];
			if (qe.idxNo != IndexValueType::SetByJsonPath && isFullText(ns.indexes_[qe.idxNo]->Type()) &&
				(op == OpOr || (i + 1 < end && queries.GetOperation(i + 1) == OpOr))) {
				throw Error(errLogic, "OR operation is not allowed with fulltext index");
			}
			SelectKeyResults selectResults;

			if (qe.joinIndex == QueryEntry::kNoJoins) {
				bool isIndexFt = false, isIndexSparse = false;
				bool nonIndexField = (qe.idxNo == IndexValueType::SetByJsonPath);

				if (nonIndexField) {
					auto strictMode = ns.config_.strictMode;
					if (ctx_ && ctx_->query.strictMode != StrictModeNotSet) {
						strictMode = ctx_->query.strictMode;
					}
					selectResults = processQueryEntry(qe, ns, strictMode);
				} else {
					const bool enableSortIndexOptimize = !sortIndexCreated && (op == OpAnd) && !qe.distinct && (begin == 0) &&
														 (ctx_->sortingContext.uncommitedIndex == qe.idxNo) &&
														 (next == end || queries.GetOperation(next) != OpOr);
					selectResults = processQueryEntry(qe, enableSortIndexOptimize, ns, sortId, isQueryFt, selectFnc, isIndexFt,
													  isIndexSparse, ftCtx, rdxCtx);
					if (enableSortIndexOptimize) sortIndexCreated = true;
				}
				processQueryEntryResults(selectResults, op, ns, qe, isIndexFt, isIndexSparse, nonIndexField);
			} else {
				processJoinEntry(qe, op);
			}
		} else {
			OpenBracket(op);
			PrepareIteratorsForSelectLoop(queries, i + 1, queries.Next(i), equalPositions, sortId, isQueryFt, ns, selectFnc, ftCtx, rdxCtx);
			CloseBracket();
		}
	}
	processEqualPositions(equalPositions, begin, end, ns, queries);
}

bool SelectIteratorContainer::processJoins(SelectIterator &it, const ConstPayload &pl, IdType rowId, bool match) {
	bool joinResult = false;
	for (size_t i = 0; i < it.joinIndexes.size(); ++i) {
		auto &joinedSelector = (*ctx_->joinedSelectors)[it.joinIndexes[i]];
		switch (joinedSelector.Type()) {
			case JoinType::InnerJoin:
				assert(i == 0);
				joinResult = joinedSelector.Process(rowId, ctx_->nsid, pl, match);
				break;
			case JoinType::OrInnerJoin:
				joinResult |= joinedSelector.Process(rowId, ctx_->nsid, pl, match);
				break;
			default:
				break;
		}
	}
	return joinResult;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::checkIfSatisfyCondition(SelectIterator &it, PayloadValue &pv, bool *finish, IdType rowId, IdType properRowId,
													  bool match) {
	bool result = true;
	const bool pureJoinIterator = (it.empty() && it.comparators_.empty() && !it.joinIndexes.empty());
	if (!pureJoinIterator && (!hasComparators || !it.TryCompare(pv, properRowId))) {
		while (((reverse && it.Val() > rowId) || (!reverse && it.Val() < rowId)) && it.Next(rowId)) {
		}
		if (it.End()) {
			*finish = true;
			result = false;
		} else if ((reverse && it.Val() < rowId) || (!reverse && it.Val() > rowId)) {
			result = false;
		}
	}
	if (!it.joinIndexes.empty()) {
		assert(ctx_->joinedSelectors);
		ConstPayload pl(*pt_, pv);
		const bool joinResult = processJoins(it, pl, properRowId, match);
		result = (result && !pureJoinIterator) || joinResult;
	}
	return result;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue &pv, bool *finish, IdType rowId,
														  IdType properRowId, bool match) {
	bool result = true;
	bool currentFinish = false;
	for (iterator it = begin; it != end; ++it) {
		if (it->operation == OpOr) {
			// no short-circuit evaluation for TRUE OR JOIN
			// suggest that all JOINs in chain of OR ... OR ... OR ... OR will be before all not JOINs (see SortByCost)
			if (result && (!it->IsLeaf() || it->Value().joinIndexes.empty())) continue;
		} else {
			if (!result) break;
		}
		bool lastFinish = false;
		bool lastResult;
		if (it->IsLeaf()) {
			lastResult = checkIfSatisfyCondition<reverse, hasComparators>(it->Value(), pv, &lastFinish, rowId, properRowId, match);
		} else {
			lastResult =
				checkIfSatisfyAllConditions<reverse, hasComparators>(it.begin(), it.end(), pv, &lastFinish, rowId, properRowId, match);
		}
		if (it->operation == OpOr) {
			result |= lastResult;
			currentFinish &= (!result && lastFinish);
		} else if (lastResult == (it->operation == OpNot)) {
			result = false;
			currentFinish = lastFinish;
		} else {
			result = true;
			currentFinish = false;
		}
	}
	if (!result) *finish = currentFinish;
	return result;
}

template <bool reverse>
IdType SelectIteratorContainer::next(const_iterator it, IdType from) {
	if (it->IsLeaf()) {
		const SelectIterator &siter = it->Value();
		if (siter.comparators_.size() || siter.joinIndexes.size() || siter.End()) return from;
		if (reverse && siter.Val() < from) return siter.Val() + 1;
		if (!reverse && siter.Val() > from) return siter.Val() - 1;
		return from;
	} else {
		return getNextItemId<reverse>(it.cbegin(), it.cend(), from);
	}
}

template <bool reverse>
IdType SelectIteratorContainer::getNextItemId(const_iterator begin, const_iterator end, IdType from) {
	IdType result = from;
	for (const_iterator it = begin; it != end; ++it) {
		switch (it->operation) {
			case OpOr:
				if (reverse) {
					result = std::max(result, next<reverse>(it, from));
				} else {
					result = std::min(result, next<reverse>(it, from));
				}
				break;
			case OpAnd:
				from = result;
				result = next<reverse>(it, from);
				break;
			case OpNot:
				break;
		}
	}
	return result;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::Process(PayloadValue &pv, bool *finish, IdType *rowId, IdType properRowId, bool match) {
	auto it = begin();
	if (checkIfSatisfyAllConditions<reverse, hasComparators>(++it, end(), pv, finish, *rowId, properRowId, match)) {
		return true;
	} else {
		*rowId = getNextItemId<reverse>(cbegin(), cend(), *rowId);
		return false;
	}
}

template bool SelectIteratorContainer::Process<false, false>(PayloadValue &, bool *, IdType *, IdType, bool);
template bool SelectIteratorContainer::Process<false, true>(PayloadValue &, bool *, IdType *, IdType, bool);
template bool SelectIteratorContainer::Process<true, false>(PayloadValue &, bool *, IdType *, IdType, bool);
template bool SelectIteratorContainer::Process<true, true>(PayloadValue &, bool *, IdType *, IdType, bool);

}  // namespace reindexer
