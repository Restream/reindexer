#include "selectiteratorcontainer.h"
#include "core/index/index.h"
#include "core/namespace.h"
#include "core/nsselecter/nsselecter.h"
#include "core/rdxcontext.h"

namespace reindexer {

void SelectIteratorContainer::sortByCost(iterator from, iterator to, int expectedIterations) {
	for (iterator it = from; it != to; ++it) {
		if (!it->IsLeaf()) {
			sortByCost(it->begin(it), it->end(it), expectedIterations);
		} else if (it->Value().distinct && (it->Op == OpOr || (it + 1 != to && (it + 1)->Op == OpOr))) {
			throw Error(errQueryExec, "OR operator with distinct query");
		}
	}
	h_vector<size_t, 4> indexes;
	h_vector<double, 4> costs;
	indexes.resize(from.DistanceTo(to));
	costs.resize(indexes.size());
	size_t i = 0;
	for (iterator it = from; it != to; ++it, ++i) {
		indexes[i] = i;
		costs[i] = fullCost(it, from, to, expectedIterations);
	}
	std::stable_sort(indexes.begin(), indexes.end(), [&from, &costs](size_t i1, size_t i2) {
		const const_iterator it1 = from + i1, it2 = from + i2;
		if (it1->IsLeaf()) {
			if (it2->IsLeaf()) {
				if (it1->Value().distinct < it2->Value().distinct) return true;
				if (it1->Value().distinct > it2->Value().distinct) return false;
			} else {
				if (it1->Value().distinct) return false;
			}
		} else if (it2->IsLeaf() && it2->Value().distinct) {
			return true;
		}
		return costs[i1] < costs[i2];
	});
	Container tmp;
	tmp.reserve(std::distance(from.PlainIterator(), to.PlainIterator()));
	for (const size_t i : indexes) {
		for (Container::iterator it = (from + i).PlainIterator(), end = (from + i + 1).PlainIterator(); it != end; ++it) {
			tmp.emplace_back(std::move(*it));
		}
	}
	for (Container::iterator dst = from.PlainIterator(), src = tmp.begin(), end = tmp.end(); src != end; ++src, ++dst) {
		*dst = std::move(*src);
	}
}

double SelectIteratorContainer::cost(const_iterator it, int expectedIterations) const {
	if (it->IsLeaf()) {
		return it->Value().Cost(expectedIterations);
	} else {
		return cost(it->cbegin(it), it->cend(it), expectedIterations);
	}
}

double SelectIteratorContainer::cost(const_iterator from, const_iterator to, int expectedIterations) const {
	double result = 0.0;
	for (const_iterator it = from; it != to; ++it) result += cost(it, expectedIterations);
	return result;
}

double SelectIteratorContainer::fullCost(const_iterator it, const_iterator begin, const_iterator end, int expectedIterations) const {
	double result = cost(it, expectedIterations);
	for (const_iterator i = begin; i != it;) {
		++i;
		if (i->Op != OpOr) begin = i;
	}
	for (; begin != it; ++begin) {
		result += cost(begin, expectedIterations);
	}
	for (++it; it != end && it->Op == OpOr; ++it) {
		result += cost(it, expectedIterations);
	}
	return result;
}

bool SelectIteratorContainer::isIdset(const_iterator it, const_iterator end) {
	return it->Op == OpAnd && it->IsLeaf() && it->Value().comparators_.empty() && !it->Value().empty() && (++it == end || it->Op != OpOr);
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
		if ((*it)->IsLeaf()) (*it)->Value().SetExpectMaxIterations(expectedIterations);
	}
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry &qe, const Namespace &ns) {
	FieldsSet fields;
	TagsPath tagsPath = ns.tagsMatcher_.path2tag(qe.index);
	fields.push_back(tagsPath);

	SelectKeyResults selectResults;
	SelectKeyResult comparisonResult;
	comparisonResult.comparators_.push_back(
		Comparator(qe.condition, KeyValueUndefined, qe.values, false, qe.distinct, ns.payloadType_, fields, nullptr, CollateOpts()));
	selectResults.push_back(comparisonResult);
	return selectResults;
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry &qe, const Namespace &ns, unsigned sortId, bool isQueryFt,
															SelectFunction::Ptr selectFnc, bool &isIndexFt, bool &isIndexSparse,
															FtCtx::Ptr &ftCtx, const RdxContext &rdxCtx) {
	auto &index = ns.indexes_[qe.idxNo];
	isIndexFt = isFullText(index->Type());
	isIndexSparse = index->Opts().IsSparse();

	Index::SelectOpts opts;
	if (!ns.sortOrdersBuilt_) opts.disableIdSetCache = 1;
	if (isQueryFt) opts.forceComparator = 1;
	if (qe.distinct) opts.distinct = 1;

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

void SelectIteratorContainer::processQueryEntryResults(SelectKeyResults &selectResults, const QueryEntries &queries, int qeIdx,
													   const Namespace &ns, const QueryEntry &qe, bool isIndexFt, bool isIndexSparse,
													   bool nonIndexField) {
	for (SelectKeyResult &res : selectResults) {
		const OpType op = queries.GetOperation(qeIdx);
		switch (op) {
			case OpOr: {
				const iterator last = lastAppendedOrClosed();
				if (last == this->end()) throw Error(errQueryExec, "OR operator in first condition or after left join ");
				if (last->IsLeaf()) {
					SelectIterator &it = last->Value();
					if (nonIndexField || isIndexSparse) {
						it.Append(res);
					} else {
						it.AppendAndBind(res, ns.payloadType_, qe.idxNo);
					}
					it.distinct |= qe.distinct;
					it.name += " OR " + qe.index;
					break;
				}  // else fallthrough
			}	  // fallthrough
			case OpNot:
			case OpAnd:
				Append(op, SelectIterator(res, qe.distinct, qe.index, isIndexFt));
				if (!nonIndexField && !isIndexSparse) {
					// last appended is always a leaf
					lastAppendedOrClosed()->Value().Bind(ns.payloadType_, qe.idxNo);
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
													const Namespace &ns, const QueryEntries &queries) {
	const auto eqPoses = equalPositions.equal_range(begin);
	for (auto it = eqPoses.first; it != eqPoses.second; ++it) {
		assert(!it->second.empty());
		const QueryEntry &firstQe(queries[it->second[0]]);
		KeyValueType type = firstQe.values.size() ? firstQe.values[0].Type() : KeyValueNull;
		Comparator cmp(firstQe.condition, type, firstQe.values, true, firstQe.distinct, ns.payloadType_, FieldsSet({firstQe.idxNo}));
		for (size_t i = begin; i < end; i = queries.Next(i)) {
			if (queries.GetOperation(i) != OpAnd || (queries.Next(i) < end && queries.GetOperation(queries.Next(i)) == OpOr))
				throw Error(errLogic, "Only AND operation allowed for equal position!");
			const QueryEntry &qe = queries[i];
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
		selectIt.comparators_.push_back(std::move(cmp));
		selectIt.distinct = false;
		Append(OpAnd, std::move(selectIt));
	}
}

void SelectIteratorContainer::PrepareIteratorsForSelectLoop(const QueryEntries &queries, size_t begin, size_t end,
															const std::multimap<unsigned, EqualPosition> &equalPositions, unsigned sortId,
															bool isQueryFt, const Namespace &ns, SelectFunction::Ptr selectFnc,
															FtCtx::Ptr &ftCtx, const RdxContext &rdxCtx) {
	for (size_t i = begin; i < end; i = queries.Next(i)) {
		if (queries.IsEntry(i)) {
			const QueryEntry &qe = queries[i];
			SelectKeyResults selectResults;

			if (qe.joinIndex == QueryEntry::kNoJoins) {
				bool isIndexFt = false, isIndexSparse = false;
				bool nonIndexField = (qe.idxNo == IndexValueType::SetByJsonPath);

				if (nonIndexField) {
					selectResults = processQueryEntry(qe, ns);
				} else {
					selectResults = processQueryEntry(qe, ns, sortId, isQueryFt, selectFnc, isIndexFt, isIndexSparse, ftCtx, rdxCtx);
				}

				processQueryEntryResults(selectResults, queries, i, ns, qe, isIndexFt, isIndexSparse, nonIndexField);
			} else {
				processJoinEntry(qe, queries.GetOperation(i));
			}
		} else {
			OpenBracket(queries.GetOperation(i));
			PrepareIteratorsForSelectLoop(queries, i + 1, queries.Next(i), equalPositions, sortId, isQueryFt, ns, selectFnc, ftCtx, rdxCtx);
			CloseBracket();
		}
	}
	processEqualPositions(equalPositions, begin, end, ns, queries);
}

bool SelectIteratorContainer::processJoins(SelectIterator &it, const ConstPayload &pl, IdType rowId, bool found, bool match) {
	for (size_t i = 0; i < it.joinIndexes.size(); ++i) {
		auto &joinedSelector = (*ctx_->joinedSelectors)[it.joinIndexes[i]];
		joinedSelector.called++;

		bool result = false;
		if ((joinedSelector.type == JoinType::InnerJoin) && found) {
			result = joinedSelector.func(&joinedSelector, rowId, ctx_->nsid, pl, match);
			found &= result;
		}
		if (joinedSelector.type == JoinType::OrInnerJoin) {
			if (!found || !joinedSelector.nodata) {
				result = joinedSelector.func(&joinedSelector, rowId, ctx_->nsid, pl, match);
				found |= result;
			}
		}

		if (result) joinedSelector.matched++;
	}
	return found;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::checkIfSatisfyCondition(SelectIterator &it, PayloadValue &pv, bool *finish, IdType rowId, IdType properRowId,
													  OpType op, bool found, bool match) {
	bool result = true;
	bool pureJoinIterator = (it.empty() && it.joinIndexes.size() > 0);
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
	if (it.joinIndexes.size() > 0) {
		if ((op == OpAnd && result) || (op == OpNot && result) || (op == OpOr && (!result || !found))) {
			assert(ctx_->joinedSelectors);
			ConstPayload pl(*pt_, pv);
			result = processJoins(it, pl, properRowId, found, match);
		}
	}
	return result;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue &pv, bool *finish, IdType rowId,
														  IdType properRowId, bool match) {
	bool result = true;
	bool currentFinish = false;
	for (iterator it = begin; it != end; ++it) {
		if (it->Op == OpOr) {
			if (result) continue;
		} else {
			if (!result) break;
		}
		bool lastFinish = false;
		if (it->IsLeaf()) {
			result =
				checkIfSatisfyCondition<reverse, hasComparators>(it->Value(), pv, &lastFinish, rowId, properRowId, it->Op, result, match);
		} else {
			result = checkIfSatisfyAllConditions<reverse, hasComparators>(it->begin(it), it->end(it), pv, &lastFinish, rowId, properRowId,
																		  match);
		}
		if (result == (it->Op == OpNot)) {
			result = false;
			if (it->Op == OpOr) {
				currentFinish = currentFinish && lastFinish;
			} else {
				currentFinish = lastFinish;
			}
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
		if (siter.comparators_.size() || siter.End()) return from;
		if (reverse && siter.Val() < from) return siter.Val() + 1;
		if (!reverse && siter.Val() > from) return siter.Val() - 1;
		return from;
	} else {
		return getNextItemId<reverse>(it->cbegin(it), it->cend(it), from);
	}
}

template <bool reverse>
IdType SelectIteratorContainer::getNextItemId(const_iterator begin, const_iterator end, IdType from) {
	IdType result = from;
	for (const_iterator it = begin; it != end; ++it) {
		switch (it->Op) {
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
	if (checkIfSatisfyAllConditions<reverse, hasComparators>(begin() + 1, end(), pv, finish, *rowId, properRowId, match)) {
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
