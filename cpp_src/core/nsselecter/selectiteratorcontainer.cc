#include "selectiteratorcontainer.h"
#include "core/index/index.h"
#include "core/namespace.h"
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
	return it->Op == OpAnd && it->IsLeaf() && it->Value().comparators_.empty() && (++it == end || it->Op != OpOr);
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

void SelectIteratorContainer::PrepareIteratorsForSelectLoop(const QueryEntries &queries, size_t begin, size_t end,
															const std::multimap<unsigned, EqualPosition> &equalPositions, unsigned sortId,
															bool isFt, const Namespace &ns, SelectFunction::Ptr selectFnc,
															FtCtx::Ptr &ftCtx, const RdxContext &rdxCtx) {
	bool fullText = false;
	const auto eqPoses = equalPositions.equal_range(begin);
	for (size_t i = begin; i < end; i = queries.Next(i)) {
		if (!queries.IsEntry(i)) {
			OpenBracket(queries.GetOperation(i));
			PrepareIteratorsForSelectLoop(queries, i + 1, queries.Next(i), equalPositions, sortId, isFt, ns, selectFnc, ftCtx, rdxCtx);
			CloseBracket();
		} else {
			const QueryEntry &qe = queries[i];
			TagsPath tagsPath;
			SelectKeyResults selectResults;
			bool sparseIndex = false;
			bool byJsonPath = (qe.idxNo == IndexValueType::SetByJsonPath);
			if (byJsonPath) {
				FieldsSet fields;
				tagsPath = ns.tagsMatcher_.path2tag(qe.index);
				fields.push_back(tagsPath);

				SelectKeyResult comparisonResult;
				comparisonResult.comparators_.push_back(Comparator(qe.condition, KeyValueUndefined, qe.values, false, qe.distinct,
																   ns.payloadType_, fields, nullptr, CollateOpts()));
				selectResults.push_back(comparisonResult);
			} else {
				auto &index = ns.indexes_[qe.idxNo];
				fullText = isFullText(index->Type());
				sparseIndex = index->Opts().IsSparse();

				Index::SelectOpts opts;
				if (!ns.sortOrdersBuilt_) opts.disableIdSetCache = 1;
				if (isFt) opts.forceComparator = 1;
				if (qe.distinct) opts.distinct = 1;

				auto ctx = selectFnc ? selectFnc->CreateCtx(qe.idxNo) : BaseFunctionCtx::Ptr{};
				if (ctx && ctx->type == BaseFunctionCtx::kFtCtx) ftCtx = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);

				if (index->Opts().GetCollateMode() == CollateUTF8 || fullText) {
					for (auto &key : qe.values) key.EnsureUTF8();
				}
				PerfStatCalculatorMT calc(index->GetSelectPerfCounter(), ns.enablePerfCounters_);
				selectResults = index->SelectKey(qe.values, qe.condition, sortId, opts, ctx, rdxCtx);
			}
			for (SelectKeyResult &res : selectResults) {
				const OpType op = queries.GetOperation(i);
				switch (op) {
					case OpOr: {
						const iterator last = lastAppendedOrClosed();
						if (last == this->end()) throw Error(errQueryExec, "OR operator in first condition");
						if (last->IsLeaf()) {
							SelectIterator &it = last->Value();
							if (byJsonPath || sparseIndex) {
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
						Append(op, SelectIterator(res, qe.distinct, qe.index, fullText));
						if (!byJsonPath && !sparseIndex) {
							lastAppendedOrClosed()->Value().Bind(ns.payloadType_, qe.idxNo);  // last appended is leaf any way
						}
						break;
					default:
						throw Error(errQueryExec, "Unknown operator (code %d) in condition", op);
				}
				if (fullText) {
					lastAppendedOrClosed()->Value().SetUnsorted();  // last appended is leaf any way
				}
			}
		}
	}
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

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::find(SelectIterator &it, PayloadValue &pv, bool *finish, IdType rowId, IdType properRowId) {
	if (!hasComparators || !it.TryCompare(pv, properRowId)) {
		while (((reverse && it.Val() > rowId) || (!reverse && it.Val() < rowId)) && it.Next(rowId)) {
		}
		if (it.End()) {
			*finish = true;
			return false;
		} else if ((reverse && it.Val() < rowId) || (!reverse && it.Val() > rowId)) {
			return false;
		}
	}
	return true;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::find(iterator begin, iterator end, PayloadValue &pv, bool *finish, IdType rowId, IdType properRowId) {
	bool found = true;
	bool currentFinish = false;
	for (iterator it = begin; it != end; ++it) {
		if (it->Op == OpOr) {
			if (found) continue;
		} else {
			if (!found) break;
		}
		bool lastFinish = false;
		if (it->IsLeaf()) {
			found = find<reverse, hasComparators>(it->Value(), pv, &lastFinish, rowId, properRowId);
		} else {
			found = find<reverse, hasComparators>(it->begin(it), it->end(it), pv, &lastFinish, rowId, properRowId);
		}
		if (found == (it->Op == OpNot)) {
			found = false;
			if (it->Op == OpOr) {
				currentFinish = currentFinish && lastFinish;
			} else {
				currentFinish = lastFinish;
			}
		} else {
			found = true;
			currentFinish = false;
		}
	}
	if (!found) *finish = currentFinish;
	return found;
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
		return iterate<reverse>(it->cbegin(it), it->cend(it), from);
	}
}

template <bool reverse>
IdType SelectIteratorContainer::iterate(const_iterator begin, const_iterator end, IdType from) {
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
bool SelectIteratorContainer::Process(PayloadValue &pv, bool *finish, IdType *rowId, IdType properRowId) {
	if (find<reverse, hasComparators>(begin() + 1, end(), pv, finish, *rowId, properRowId)) {
		return true;
	} else {
		*rowId = iterate<reverse>(cbegin(), cend(), *rowId);
		return false;
	}
}

template bool SelectIteratorContainer::Process<false, false>(PayloadValue &, bool *, IdType *, IdType);
template bool SelectIteratorContainer::Process<false, true>(PayloadValue &, bool *, IdType *, IdType);
template bool SelectIteratorContainer::Process<true, false>(PayloadValue &, bool *, IdType *, IdType);
template bool SelectIteratorContainer::Process<true, true>(PayloadValue &, bool *, IdType *, IdType);

}  // namespace reindexer
