#include "selectiteratorcontainer.h"

#include <numeric>
#include <sstream>
#include "core/namespace/namespaceimpl.h"
#include "core/rdxcontext.h"
#include "nsselecter.h"
#include "querypreprocessor.h"

namespace reindexer {

void SelectIteratorContainer::SortByCost(int expectedIterations) {
	markBracketsHavingJoins(begin(), end());
	thread_local h_vector<unsigned, 16> indexes;
	thread_local h_vector<double, 16> costs;
	if (indexes.size() < container_.size()) {
		indexes.resize(container_.size());
		costs.resize(container_.size());
	}
	std::iota(indexes.begin(), indexes.begin() + container_.size(), 0);
	sortByCost(indexes, costs, 0, container_.size(), expectedIterations);
	for (size_t i = 0; i < container_.size(); ++i) {
		if (indexes[i] != i) {
			size_t positionOfTmp = i + 1;
			for (; positionOfTmp < indexes.size(); ++positionOfTmp) {
				if (indexes[positionOfTmp] == i) break;
			}
			assertrx(positionOfTmp < indexes.size());
			Container::value_type tmp = std::move(container_[i]);
			container_[i] = std::move(container_[indexes[i]]);
			container_[indexes[i]] = std::move(tmp);
			indexes[positionOfTmp] = indexes[i];
		}
	}
}

void SelectIteratorContainer::sortByCost(span<unsigned> indexes, span<double> costs, unsigned from, unsigned to, int expectedIterations) {
	for (unsigned cur = from, next; cur < to; cur = next) {
		next = cur + Size(indexes[cur]);
		if (!IsSelectIterator(indexes[cur])) {
			sortByCost(indexes, costs, cur + 1, next, expectedIterations);
			if (next < to && GetOperation(indexes[next]) == OpOr && IsSelectIterator(indexes[next]) &&
				Get<SelectIterator>(indexes[next]).distinct) {
				throw Error(errQueryExec, "OR operator between distinct query and bracket or join");
			}
		} else if (next < to && GetOperation(indexes[next]) == OpOr) {
			if (IsSelectIterator(indexes[next])) {
				if (Get<SelectIterator>(indexes[cur]).distinct != Get<SelectIterator>(indexes[next]).distinct) {
					throw Error(errQueryExec, "OR operator between distinct and non distinct queries");
				}
			} else {
				if (Get<SelectIterator>(indexes[cur]).distinct) {
					throw Error(errQueryExec, "OR operator between distinct query and bracket or join");
				}
			}
		}
	}
	for (unsigned cur = from, next; cur < to; cur = next) {
		next = cur + Size(indexes[cur]);
		const double cst = fullCost(indexes, cur, from, to, expectedIterations);
		for (unsigned j = cur; j < next; ++j) {
			costs[indexes[j]] = cst;
		}
	}
	std::stable_sort(indexes.begin() + from, indexes.begin() + to, [&costs](unsigned i1, unsigned i2) { return costs[i1] < costs[i2]; });
	moveJoinsToTheBeginingOfORs(indexes, from, to);
}

bool SelectIteratorContainer::markBracketsHavingJoins(iterator begin, iterator end) noexcept {
	bool result = false;
	for (iterator it = begin; it != end; ++it) {
		result = it->InvokeAppropriate<bool>(
					 [it](SelectIteratorsBracket &b) { return (b.haveJoins = markBracketsHavingJoins(it.begin(), it.end())); },
					 [](SelectIterator &) { return false; }, [](JoinSelectIterator &) { return true; },
					 [](FieldsComparator &) { return false; }, [](AlwaysFalse &) { return false; }) ||
				 result;
	}
	return result;
}

bool SelectIteratorContainer::haveJoins(size_t i) const noexcept {
	return container_[i].InvokeAppropriate<bool>(
		[](const SelectIteratorsBracket &b) { return b.haveJoins; }, [](const SelectIterator &) { return false; },
		[](const FieldsComparator &) { return false; }, [](const JoinSelectIterator &) { return true; },
		[](const AlwaysFalse &) { return false; });
}

void SelectIteratorContainer::moveJoinsToTheBeginingOfORs(span<unsigned> indexes, unsigned from, unsigned to) {
	thread_local h_vector<unsigned, 16> buffer;
	buffer.resize(indexes.size());
	unsigned firstNotJoin = from;
	for (unsigned cur = from, next; cur < to; cur = next) {
		const unsigned curSize = Size(indexes[cur]);
		next = cur + curSize;
		if (GetOperation(indexes[cur]) != OpOr) {
			firstNotJoin = cur;
		} else if (haveJoins(indexes[cur])) {
			while (firstNotJoin < cur && (GetOperation(indexes[firstNotJoin]) == OpNot || haveJoins(indexes[firstNotJoin]))) {
				firstNotJoin += Size(indexes[firstNotJoin]);
			}
			if (firstNotJoin < cur) {
				SetOperation(GetOperation(indexes[firstNotJoin]), indexes[cur]);
				SetOperation(OpOr, indexes[firstNotJoin]);
				if (IsJoinIterator(indexes[cur])) {
					unsigned tmp = indexes[cur];
					for (unsigned i = cur; i > firstNotJoin; --i) indexes[i] = indexes[i - 1];
					indexes[firstNotJoin] = tmp;
				} else {
					memcpy(&buffer[0], &indexes[cur], sizeof(unsigned) * curSize);
					for (unsigned i = cur - 1; i >= firstNotJoin; --i) indexes[i + curSize] = indexes[i];
					memcpy(&indexes[firstNotJoin], &buffer[0], sizeof(unsigned) * curSize);
				}
			}
			firstNotJoin += curSize;
		}
	}
}

double SelectIteratorContainer::cost(span<unsigned> indexes, unsigned cur, int expectedIterations) const {
	return container_[indexes[cur]].InvokeAppropriate<double>(
		[&](const SelectIteratorsBracket &) { return cost(indexes, cur + 1, cur + Size(indexes[cur]), expectedIterations); },
		[expectedIterations](const SelectIterator &sit) { return sit.Cost(expectedIterations); },
		[](const JoinSelectIterator &jit) { return jit.Cost(); },
		[expectedIterations](const FieldsComparator &c) { return c.Cost(expectedIterations); }, [](const AlwaysFalse &) { return 1; });
}

double SelectIteratorContainer::cost(span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const {
	double result = 0.0;
	for (unsigned cur = from; cur < to; cur += Size(indexes[cur])) {
		result += cost(indexes, cur, expectedIterations);
	}
	return result;
}

double SelectIteratorContainer::fullCost(span<unsigned> indexes, unsigned cur, unsigned from, unsigned to, int expectedIterations) const {
	double result = 0.0;
	for (unsigned i = from; i <= cur; i += Size(indexes[i])) {
		if (GetOperation(indexes[i]) != OpOr) from = i;
	}
	for (; from <= cur || (from < to && GetOperation(indexes[from]) == OpOr); from += Size(indexes[from])) {
		result += cost(indexes, from, expectedIterations);
	}
	return result;
}

bool SelectIteratorContainer::isIdset(const_iterator it, const_iterator end) {
	return it->operation == OpAnd && it->HoldsOrReferTo<SelectIterator>() &&
		   it->Value<SelectIterator>().comparators_.empty() &&	// !it->Value().empty() &&
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
	assertrx(0);
}

// Let iterators choose most effective algorithm
void SelectIteratorContainer::SetExpectMaxIterations(int expectedIterations) {
	assertrx(!Empty());
	assertrx(HoldsOrReferTo<SelectIterator>(0));
	for (Container::iterator it = container_.begin() + 1; it != container_.end(); ++it) {
		if (it->HoldsOrReferTo<SelectIterator>()) {
			if (it->IsRef()) it->SetValue(it->Value<SelectIterator>());
			it->Value<SelectIterator>().SetExpectMaxIterations(expectedIterations);
		}
	}
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry &qe, const NamespaceImpl &ns, StrictMode strictMode) {
	SelectKeyResults selectResults;

	FieldsSet fields;
	TagsPath tagsPath = ns.tagsMatcher_.path2tag(qe.index);

	// TODO: it may be necessary to remove or change this switch after QueryEntry refactoring
	switch (qe.condition) {
		case CondAny:
		case CondEmpty:
		case CondAllSet:
		case CondEq:
		case CondSet:
			break;
		case CondRange:
		case CondDWithin:
			if (qe.values.size() != 2) {
				throw Error(errParams, "For condition %s required exactly 2 arguments, but provided %d", CondTypeToStr(qe.condition),
							qe.values.size());
			}
			break;
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
		case CondLike:
			if (qe.values.size() != 1) {
				throw Error(errParams, "For condition %s required exactly 1 argument, but provided %d", CondTypeToStr(qe.condition),
							qe.values.size());
			}
			break;
	}

	if (!tagsPath.empty()) {
		SelectKeyResult comparisonResult;
		fields.push_back(tagsPath);
		comparisonResult.comparators_.emplace_back(qe.condition, KeyValueType::Null{}, qe.values, false, qe.distinct, ns.payloadType_,
												   fields, nullptr, CollateOpts());
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

template <bool left>
void SelectIteratorContainer::processField(FieldsComparator &fc, std::string_view field, int idxNo, const NamespaceImpl &ns) const {
	const bool nonIndexField = (idxNo == IndexValueType::SetByJsonPath);
	if (nonIndexField) {
		TagsPath tagsPath = ns.tagsMatcher_.path2tag(field);
		if (tagsPath.empty()) {
			throw Error{errQueryExec, "Only existing fields can be compared. There are no fields with name '%s' in namespace '%s'", field,
						ns.name_};
		}
		if constexpr (left) {
			fc.SetLeftField(tagsPath);
		} else {
			fc.SetRightField(tagsPath);
		}
	} else {
		auto &index = ns.indexes_[idxNo];
		if constexpr (left) {
			fc.SetCollateOpts(index->Opts().collateOpts_);
			fc.SetLeftField(index->Fields(), index->KeyType(), index->Opts().IsArray());
		} else {
			fc.SetRightField(index->Fields(), index->KeyType(), index->Opts().IsArray());
		}
	}
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry &qe, bool enableSortIndexOptimize, const NamespaceImpl &ns,
															unsigned sortId, bool isQueryFt, SelectFunction::Ptr &selectFnc,
															bool &isIndexFt, bool &isIndexSparse, FtCtx::Ptr &ftCtx,
															QueryPreprocessor &qPreproc, const RdxContext &rdxCtx) {
	auto &index = ns.indexes_[qe.idxNo];
	isIndexFt = IsFullText(index->Type());
	isIndexSparse = index->Opts().IsSparse();

	Index::SelectOpts opts;
	opts.itemsCountInNamespace = ns.items_.size() - ns.free_.size();
	if (!ns.SortOrdersBuilt()) opts.disableIdSetCache = 1;
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
	opts.maxIterations = GetMaxIterations();
	opts.indexesNotOptimized = !ctx_->sortingContext.enableSortOrders;
	opts.inTransaction = ctx_->inTransaction;

	auto ctx = selectFnc ? selectFnc->CreateCtx(qe.idxNo) : BaseFunctionCtx::Ptr{};
	if (ctx && ctx->type == BaseFunctionCtx::kFtCtx) ftCtx = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);

	if (index->Opts().GetCollateMode() == CollateUTF8 || isIndexFt) {
		for (auto &key : qe.values) key.EnsureUTF8();
	}
	PerfStatCalculatorMT calc(index->GetSelectPerfCounter(), ns.enablePerfCounters_);
	if (qPreproc.IsFtPreselected()) {
		return index->SelectKey(qe.values, qe.condition, opts, ctx, qPreproc.MoveFtPreselect(), rdxCtx);
	} else {
		return index->SelectKey(qe.values, qe.condition, sortId, opts, ctx, rdxCtx);
	}
}

void SelectIteratorContainer::processJoinEntry(const JoinQueryEntry &jqe, OpType op) {
	auto &js = (*ctx_->joinedSelectors)[jqe.joinIndex];
	if (js.JoinQuery().joinEntries_.empty()) throw Error(errQueryExec, "Join without ON conditions");
	if (js.JoinQuery().joinEntries_[0].op_ == OpOr) throw Error(errQueryExec, "The first ON condition cannot have OR operation");
	if (js.Type() != InnerJoin && js.Type() != OrInnerJoin) throw Error(errLogic, "Not INNER JOIN in QueryEntry");
	if (js.Type() == OrInnerJoin) {
		if (op == OpNot) throw Error(errQueryExec, "NOT operator with or_inner_join");
		js.SetType(InnerJoin);
		op = OpOr;
	}
	if (op == OpOr && lastAppendedOrClosed() == this->end()) {
		throw Error(errQueryExec, "OR operator in first condition or after left join");
	}
	Append(op, JoinSelectIterator{static_cast<size_t>(jqe.joinIndex)});
}

void SelectIteratorContainer::processQueryEntryResults(SelectKeyResults &selectResults, OpType op, const NamespaceImpl &ns,
													   const QueryEntry &qe, bool isIndexFt, bool isIndexSparse, bool nonIndexField,
													   std::optional<OpType> nextOp) {
	if (selectResults.empty()) {
		if (op == OpAnd) {
			Append(OpAnd, AlwaysFalse{});
		}
		return;
	}
	for (SelectKeyResult &res : selectResults) {
		switch (op) {
			case OpOr: {
				const iterator last = lastAppendedOrClosed();
				if (last == this->end()) {
					throw Error(errQueryExec, "OR operator in first condition or after left join ");
				}
				if (last->HoldsOrReferTo<SelectIterator>() && !last->Value<SelectIterator>().distinct && last->operation != OpNot) {
					if (last->IsRef()) {
						last->SetValue(last->Value<SelectIterator>());
					}
					SelectIterator &it = last->Value<SelectIterator>();
					if (nonIndexField || isIndexSparse) {
						it.Append(res);
					} else {
						it.AppendAndBind(res, ns.payloadType_, qe.idxNo);
					}
					it.name += " or " + qe.index;
					break;
				}
			}
				[[fallthrough]];
			case OpNot:
			case OpAnd:
				// Iterator Field Kind: Query entry results. Field known.
				Append(op, SelectIterator(res, qe.distinct, qe.index,
										  qe.idxNo < 0 ? IteratorFieldKind::NonIndexed : IteratorFieldKind::Indexed, isIndexFt));
				if (!nonIndexField && !isIndexSparse) {
					// last appended is always a SelectIterator
					const auto lastAppendedIt = lastAppendedOrClosed();
					if (lastAppendedIt->IsRef()) {
						lastAppendedIt->SetValue(lastAppendedIt->Value<SelectIterator>());
					}
					SelectIterator &lastAppended = lastAppendedIt->Value<SelectIterator>();
					lastAppended.Bind(ns.payloadType_, qe.idxNo);
					lastAppended.SetNotOperationFlag(op == OpNot);
					const auto maxIterations = lastAppended.GetMaxIterations();
					const int cur = op == OpNot ? ns.items_.size() - maxIterations : maxIterations;
					if (lastAppended.comparators_.empty() && (!nextOp.has_value() || nextOp.value() != OpOr)) {
						if (cur && cur < maxIterations_) maxIterations_ = cur;
						if (!cur) wasZeroIterations_ = true;
					}
				}
				break;
			default:
				throw Error(errQueryExec, "Unknown operator (code %d) in condition", op);
		}
		if (isIndexFt) {
			// last appended is always a SelectIterator
			lastAppendedOrClosed()->Value<SelectIterator>().SetUnsorted();
		}
	}
}

void SelectIteratorContainer::processEqualPositions(const std::vector<EqualPositions> &equalPositions, const NamespaceImpl &ns,
													const QueryEntries &queries) {
	for (const auto &eqPos : equalPositions) {
		const QueryEntry &firstQe{queries.Get<QueryEntry>(eqPos.queryEntriesPositions[0])};
		if (firstQe.condition == CondEmpty || (firstQe.condition == CondSet && firstQe.values.empty())) {
			throw Error(errLogic, "Condition IN(with empty parameter list), IS NULL, IS EMPTY not allowed for equal position!");
		}

		const KeyValueType type = firstQe.values.size() ? firstQe.values[0].Type() : KeyValueType::Null{};
		Comparator cmp(firstQe.condition, type, firstQe.values, true, firstQe.distinct, ns.payloadType_, FieldsSet({firstQe.idxNo}));

		for (size_t i = 0; i < eqPos.queryEntriesPositions.size(); ++i) {
			const QueryEntry &qe = queries.Get<QueryEntry>(eqPos.queryEntriesPositions[i]);
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
		InsertAfter(eqPos.positionToInsertIterator, OpAnd, std::move(selectIt));
	}
}

std::vector<SelectIteratorContainer::EqualPositions> SelectIteratorContainer::prepareEqualPositions(const QueryEntries &queries,
																									size_t begin, size_t end) {
	static const auto getFieldsStr = [](auto begin, auto end) {
		std::stringstream str;
		for (auto it = begin; it != end; ++it) {
			if (it != begin) str << ", ";
			str << *it;
		}
		return str.str();
	};
	const auto &eqPos = (begin == 0 ? queries.equalPositions : queries.Get<QueryEntriesBracket>(begin - 1).equalPositions);
	std::vector<EqualPositions> result{eqPos.size()};
	for (size_t i = 0; i < eqPos.size(); ++i) {
		if (eqPos[i].size() < 2) {
			throw Error(errLogic, "equal positions should contain 2 or more fields");
		}
		std::unordered_set<std::string_view> epFields{eqPos[i].begin(), eqPos[i].end()};
		const auto getEpFieldsStr = [&eqPos, i]() { return getFieldsStr(eqPos[i].cbegin(), eqPos[i].cend()); };
		if (eqPos[i].size() != epFields.size()) {
			throw Error(errParams, "equal positions fields should be unique: [%s]", getEpFieldsStr());
		}
		std::unordered_set<std::string_view> foundFields;
		result[i].queryEntriesPositions.reserve(eqPos[i].size());
		for (size_t j = begin, next; j < end; j = next) {
			next = queries.Next(j);
			queries.InvokeAppropriate<void>(
				j, Skip<QueryEntriesBracket, JoinQueryEntry, AlwaysFalse>{},
				[&](const QueryEntry &eq) {
					if (foundFields.find(eq.index) != foundFields.end()) {
						throw Error(errParams, "Equal position field '%s' found twice in enclosing bracket; equal position fields: [%s]",
									eq.index, getEpFieldsStr());
					}
					const auto it = epFields.find(eq.index);
					if (it == epFields.end()) return;
					if (queries.GetOperation(j) != OpAnd || (next < end && queries.GetOperation(next) == OpOr)) {
						throw Error(errParams,
									"Only AND operation allowed for equal position; equal position field with not AND operation: '%s'; "
									"equal position fields: [%s]",
									eq.index, getEpFieldsStr());
					}
					result[i].queryEntriesPositions.push_back(j);
					foundFields.insert(epFields.extract(it));
				},
				[&](const BetweenFieldsQueryEntry &eq) {  // TODO equal positions for BetweenFieldsQueryEntry #1092
					if (epFields.find(eq.firstIndex) != epFields.end()) {
						throw Error(
							errParams,
							"Equal positions for conditions between fields are not supported; field: '%s'; equal position fields: [%s]",
							eq.firstIndex, getEpFieldsStr());
					}
					if (epFields.find(eq.secondIndex) != epFields.end()) {
						throw Error(
							errParams,
							"Equal positions for conditions between fields are not supported; field: '%s'; equal position fields: [%s]",
							eq.secondIndex, getEpFieldsStr());
					}
				});
		}
		if (!epFields.empty()) {
			throw Error(errParams, "Equal position fields [%s] are not found in enclosing bracket; equal position fields: [%s]",
						getFieldsStr(epFields.cbegin(), epFields.cend()), getEpFieldsStr());
		}
	}
	return result;
}

void SelectIteratorContainer::PrepareIteratorsForSelectLoop(QueryPreprocessor &qPreproc, unsigned sortId, bool isFt,
															const NamespaceImpl &ns, SelectFunction::Ptr &selectFnc, FtCtx::Ptr &ftCtx,
															const RdxContext &rdxCtx) {
	prepareIteratorsForSelectLoop(qPreproc, 0, qPreproc.Size(), sortId, isFt, ns, selectFnc, ftCtx, rdxCtx);
}

bool SelectIteratorContainer::prepareIteratorsForSelectLoop(QueryPreprocessor &qPreproc, size_t begin, size_t end, unsigned sortId,
															bool isQueryFt, const NamespaceImpl &ns, SelectFunction::Ptr &selectFnc,
															FtCtx::Ptr &ftCtx, const RdxContext &rdxCtx) {
	const auto &queries = qPreproc.GetQueryEntries();
	auto equalPositions = prepareEqualPositions(queries, begin, end);
	bool sortIndexFound = false;
	bool containFT = false;
	for (size_t i = begin, next = begin; i != end; i = next) {
		next = queries.Next(i);
		const OpType op = queries.GetOperation(i);
		containFT = queries.InvokeAppropriate<bool>(
						i,
						[&](const QueryEntriesBracket &) {
							OpenBracket(op);
							const bool contFT =
								prepareIteratorsForSelectLoop(qPreproc, i + 1, next, sortId, isQueryFt, ns, selectFnc, ftCtx, rdxCtx);
							if (contFT && (op == OpOr || (next < end && queries.GetOperation(next) == OpOr))) {
								throw Error(errLogic, "OR operation is not allowed with bracket containing fulltext index");
							}
							CloseBracket();
							return contFT;
						},
						[&](const QueryEntry &qe) {
							const bool isFT = qe.idxNo != IndexValueType::SetByJsonPath && IsFullText(ns.indexes_[qe.idxNo]->Type());
							if (isFT && (op == OpOr || (next < end && queries.GetOperation(next) == OpOr))) {
								throw Error(errLogic, "OR operation is not allowed with fulltext index");
							}
							SelectKeyResults selectResults;

							bool isIndexFt = false, isIndexSparse = false;
							const bool nonIndexField = (qe.idxNo == IndexValueType::SetByJsonPath);

							if (nonIndexField) {
								auto strictMode = ns.config_.strictMode;
								if (ctx_) {
									if (ctx_->inTransaction) {
										strictMode = StrictModeNone;
									} else if (ctx_->query.strictMode != StrictModeNotSet) {
										strictMode = ctx_->query.strictMode;
									}
								}
								selectResults = processQueryEntry(qe, ns, strictMode);
							} else {
								bool enableSortIndexOptimize = (ctx_->sortingContext.uncommitedIndex == qe.idxNo) && !sortIndexFound &&
															   (op == OpAnd) && !qe.distinct && (begin == 0) &&
															   (next == end || queries.GetOperation(next) != OpOr);
								if (enableSortIndexOptimize) {
									if (!IsExpectingOrderedResults(qe)) {
										// Disable sorting index optimization if it somehow has incompatible conditions
										enableSortIndexOptimize = false;
									}
									sortIndexFound = true;
								}
								selectResults = processQueryEntry(qe, enableSortIndexOptimize, ns, sortId, isQueryFt, selectFnc, isIndexFt,
																  isIndexSparse, ftCtx, qPreproc, rdxCtx);
							}
							std::optional<OpType> nextOp;
							if (next != end) {
								nextOp = queries.GetOperation(next);
							}
							processQueryEntryResults(selectResults, op, ns, qe, isIndexFt, isIndexSparse, nonIndexField, nextOp);
							if (op != OpOr) {
								for (auto &ep : equalPositions) {
									const auto lastPosition = ep.queryEntriesPositions.back();
									if (i == lastPosition || (i > lastPosition && !ep.foundOr)) {
										ep.positionToInsertIterator = Size() - 1;
									}
								}
							} else {
								for (auto &ep : equalPositions) {
									if (i > ep.queryEntriesPositions.back()) ep.foundOr = true;
								}
							}
							return isFT;
						},
						[this, op](const JoinQueryEntry &jqe) {
							processJoinEntry(jqe, op);
							return false;
						},
						[&](const BetweenFieldsQueryEntry &qe) {
							FieldsComparator fc{qe.firstIndex, qe.Condition(), qe.secondIndex, ns.payloadType_};
							processField<true>(fc, qe.firstIndex, qe.firstIdxNo, ns);
							processField<false>(fc, qe.secondIndex, qe.secondIdxNo, ns);
							Append(op, std::move(fc));
							return false;
						},
						[this, op](const AlwaysFalse &) {
							Append(op, AlwaysFalse{});
							return false;
						}) ||
					containFT;
	}
	processEqualPositions(equalPositions, ns, queries);
	return containFT;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::checkIfSatisfyCondition(SelectIterator &it, PayloadValue &pv, bool *finish, IdType rowId,
													  IdType properRowId) {
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

bool SelectIteratorContainer::checkIfSatisfyCondition(JoinSelectIterator &it, PayloadValue &pv, IdType properRowId, bool match) {
	assertrx(ctx_->joinedSelectors);
	ConstPayload pl(*pt_, pv);
	auto &joinedSelector = (*ctx_->joinedSelectors)[it.joinIndex];
	return joinedSelector.Process(properRowId, ctx_->nsid, pl, match);
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
			if (result) {
				// check what it does not holds join
				if (it->InvokeAppropriate<bool>([](const SelectIteratorsBracket &b) { return !b.haveJoins; },
												[](const SelectIterator &) { return true; },
												[](const JoinSelectIterator &) { return false; },
												[](const FieldsComparator &) { return true; }, [](const AlwaysFalse &) { return true; }))
					continue;
			}
		} else {
			if (!result) break;
		}
		bool lastFinish = false;
		const bool lastResult = it->InvokeAppropriate<bool>(
			[&](SelectIteratorsBracket &) {
				return checkIfSatisfyAllConditions<reverse, hasComparators>(it.begin(), it.end(), pv, &lastFinish, rowId, properRowId,
																			match);
			},
			[&](SelectIterator &sit) { return checkIfSatisfyCondition<reverse, hasComparators>(sit, pv, &lastFinish, rowId, properRowId); },
			[&](JoinSelectIterator &jit) { return checkIfSatisfyCondition(jit, pv, properRowId, match); },
			[&pv](FieldsComparator &c) { return c.Compare(pv); }, [](AlwaysFalse &) { return false; });
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
	return it->InvokeAppropriate<IdType>(
		[it, from](const SelectIteratorsBracket &) { return getNextItemId<reverse>(it.cbegin(), it.cend(), from); },
		[from](const SelectIterator &sit) {
			if (sit.comparators_.size()) return from;
			if constexpr (reverse) {
				if (sit.End()) return std::numeric_limits<IdType>::lowest();
				if (sit.Val() < from) return sit.Val() + 1;
			} else {
				if (sit.End()) return std::numeric_limits<IdType>::max();
				if (sit.Val() > from) return sit.Val() - 1;
			}
			return from;
		},
		[from](const JoinSelectIterator &) { return from; }, [from](const FieldsComparator &) { return from; },
		[](const AlwaysFalse &) {
			if constexpr (reverse) {
				return std::numeric_limits<IdType>::lowest();
			} else {
				return std::numeric_limits<IdType>::max();
			}
		});
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

std::string SelectIteratorContainer::Dump() const {
	WrSerializer ser;
	dump(0, cbegin(), cend(), *ctx_->joinedSelectors, ser);
	return std::string{ser.Slice()};
}

void SelectIteratorContainer::dump(size_t level, const_iterator begin, const_iterator end,
								   const std::vector<JoinedSelector> &joinedSelectors, WrSerializer &ser) {
	for (const_iterator it = begin; it != end; ++it) {
		for (size_t i = 0; i < level; ++i) {
			ser << "   ";
		}
		if (it != begin || it->operation != OpAnd) {
			ser << it->operation << ' ';
		}
		it->InvokeAppropriate<void>(
			[&](const SelectIteratorsBracket &) {
				ser << "(\n";
				dump(level + 1, it.cbegin(), it.cend(), joinedSelectors, ser);
				for (size_t i = 0; i < level; ++i) {
					ser << "   ";
				}
				ser << ')';
			},
			[&ser](const SelectIterator &sit) { ser << sit.Dump(); },
			[&ser, &joinedSelectors](const JoinSelectIterator &jit) { jit.Dump(ser, joinedSelectors); },
			[&ser](const FieldsComparator &c) { ser << c.Dump(); }, [&ser](const AlwaysFalse &) { ser << "Always False"; });
		ser << '\n';
	}
}

void JoinSelectIterator::Dump(WrSerializer &ser, const std::vector<JoinedSelector> &joinedSelectors) const {
	const auto &js = joinedSelectors.at(joinIndex);
	const auto &q = js.JoinQuery();
	ser << js.Type() << " (" << q.GetSQL() << ") ON ";
	ser << '(';
	for (const auto &jqe : q.joinEntries_) {
		if (&jqe != &q.joinEntries_.front()) {
			ser << ' ' << jqe.op_ << ' ';
		} else {
			assertrx(jqe.op_ == OpAnd);
		}
		ser << q._namespace << '.' << jqe.joinIndex_ << ' ' << jqe.condition_ << ' ' << jqe.index_;
	}
	ser << ')';
}

}  // namespace reindexer
