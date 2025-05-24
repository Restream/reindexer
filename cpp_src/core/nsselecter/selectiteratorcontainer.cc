#include "selectiteratorcontainer.h"

#include <numeric>
#include <span>
#include <sstream>
#include "core/index/float_vector/float_vector_index.h"
#include "core/index/float_vector/knn_ctx.h"
#include "core/namespace/namespaceimpl.h"
#include "core/rdxcontext.h"
#include "core/type_consts_helpers.h"
#include "estl/restricted.h"
#include "estl/stable_sort.h"
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
				if (indexes[positionOfTmp] == i) {
					break;
				}
			}
			assertrx_throw(positionOfTmp < indexes.size());
			Container::value_type tmp = std::move(container_[i]);
			container_[i] = std::move(container_[indexes[i]]);
			container_[indexes[i]] = std::move(tmp);
			indexes[positionOfTmp] = indexes[i];
		}
	}
}

void SelectIteratorContainer::sortByCost(std::span<unsigned> indexes, std::span<double> costs, unsigned from, unsigned to,
										 int expectedIterations) {
	for (unsigned cur = from, next; cur < to; cur = next) {
		next = cur + Size(indexes[cur]);
		if (IsSubTree(indexes[cur])) {
			sortByCost(indexes, costs, cur + 1, next, expectedIterations);
			if (next < to && GetOperation(indexes[next]) == OpOr && IsDistinct(indexes[next])) {
				throw Error(errQueryExec, "OR operator between distinct query and bracket or join");
			}
		} else if (next < to && GetOperation(indexes[next]) == OpOr) {
			const bool curDistinct = IsDistinct(indexes[cur]);
			if (curDistinct != IsDistinct(indexes[next])) {
				throw Error(errQueryExec, "OR operator between distinct and non distinct queries");
			}
			if (curDistinct && (IsSubTree(indexes[next]) && IsJoinIterator(indexes[next]))) {
				throw Error(errQueryExec, "OR operator between distinct query and bracket or join");
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
	reindexer::stable_sort(indexes.begin() + from, indexes.begin() + to,
						   [&costs](unsigned i1, unsigned i2) noexcept { return costs[i1] < costs[i2]; });
	moveJoinsToTheBeginningOfORs(indexes, from, to);
}

bool SelectIteratorContainer::markBracketsHavingJoins(iterator begin, iterator end) noexcept {
	bool result = false;
	for (iterator it = begin; it != end; ++it) {
		result = it->Visit(
					 [it] RX_PRE_LMBD_ALWAYS_INLINE(SelectIteratorsBracket & b)
						 RX_POST_LMBD_ALWAYS_INLINE noexcept { return (b.haveJoins = markBracketsHavingJoins(it.begin(), it.end())); },
					 [] RX_PRE_LMBD_ALWAYS_INLINE(
						 OneOf<SelectIterator, FieldsComparator, AlwaysTrue, EqualPositionComparator, ComparatorNotIndexed,
							   ComparatorDistinctMulti,
							   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>)
						 RX_POST_LMBD_ALWAYS_INLINE noexcept { return false; },
					 [] RX_PRE_LMBD_ALWAYS_INLINE(JoinSelectIterator&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; }) ||
				 result;
	}
	return result;
}

bool SelectIteratorContainer::haveJoins(size_t i) const noexcept {
	return container_[i].Visit(
		[] RX_PRE_LMBD_ALWAYS_INLINE(const SelectIteratorsBracket& b) RX_POST_LMBD_ALWAYS_INLINE noexcept { return b.haveJoins; },
		[] RX_PRE_LMBD_ALWAYS_INLINE(
			OneOf<SelectIterator, FieldsComparator, EqualPositionComparator, ComparatorNotIndexed, ComparatorDistinctMulti,
				  Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>)
			RX_POST_LMBD_ALWAYS_INLINE noexcept { return false; },
		[] RX_PRE_LMBD_ALWAYS_INLINE(OneOf<JoinSelectIterator, AlwaysTrue>) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; });
}

void SelectIteratorContainer::moveJoinsToTheBeginningOfORs(std::span<unsigned> indexes, unsigned from, unsigned to) {
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
					for (unsigned i = cur; i > firstNotJoin; --i) {
						indexes[i] = indexes[i - 1];
					}
					indexes[firstNotJoin] = tmp;
				} else {
					memcpy(&buffer[0], &indexes[cur], sizeof(unsigned) * curSize);
					for (unsigned i = cur + curSize - 1, e = firstNotJoin + curSize; i >= e; --i) {
						indexes[i] = indexes[i - curSize];
					}
					memcpy(&indexes[firstNotJoin], &buffer[0], sizeof(unsigned) * curSize);
				}
			}
			firstNotJoin += curSize;
		}
	}
}

double SelectIteratorContainer::cost(std::span<unsigned> indexes, unsigned cur, int expectedIterations) const noexcept {
	return container_[indexes[cur]].Visit(
		[&] RX_PRE_LMBD_ALWAYS_INLINE(const SelectIteratorsBracket&)
			RX_POST_LMBD_ALWAYS_INLINE noexcept { return cost(indexes, cur + 1, cur + Size(indexes[cur]), expectedIterations); },
		Restricted<SelectIterator, FieldsComparator, EqualPositionComparator, ComparatorNotIndexed, ComparatorDistinctMulti,
				   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>{}(
			[expectedIterations] RX_PRE_LMBD_ALWAYS_INLINE(const auto& c)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { return c.Cost(expectedIterations); }),
		[] RX_PRE_LMBD_ALWAYS_INLINE(const JoinSelectIterator& jit) RX_POST_LMBD_ALWAYS_INLINE noexcept { return jit.Cost(); },
		[expectedIterations] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysTrue&)
			RX_POST_LMBD_ALWAYS_INLINE noexcept -> double { return expectedIterations; });
}

double SelectIteratorContainer::cost(std::span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const noexcept {
	double result = 0.0;
	for (unsigned cur = from; cur < to; cur += Size(indexes[cur])) {
		result += cost(indexes, cur, expectedIterations);
	}
	return result;
}

double SelectIteratorContainer::fullCost(std::span<unsigned> indexes, unsigned cur, unsigned from, unsigned to,
										 int expectedIterations) const noexcept {
	double result = 0.0;
	for (unsigned i = from; i <= cur; i += Size(indexes[i])) {
		if (GetOperation(indexes[i]) != OpOr) {
			from = i;
		}
	}
	for (; from <= cur || (from < to && GetOperation(indexes[from]) == OpOr); from += Size(indexes[from])) {
		result += cost(indexes, from, expectedIterations);
	}
	return result;
}

bool SelectIteratorContainer::isIdset(const_iterator it, const_iterator end) {
	return it->operation == OpAnd && it->Is<SelectIterator>() && (++it == end || it->operation != OpOr);  // && !it->Value().empty()
}

bool SelectIteratorContainer::HasIdsets() const {
	for (const_iterator it = cbegin(), end = cend(); it != end; ++it) {
		if (isIdset(it, end)) {
			return true;
		}
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
				for (; src != dst; --src) {
					*src = std::move(*(src - 1));
				}
				*dst = std::move(tmp);
			}
			return;
		}
	}
	throw_as_assert;
}

// Let iterators choose most effective algorithm
void SelectIteratorContainer::SetExpectMaxIterations(int expectedIterations) {
	assertrx_throw(!Empty());
	assertrx_throw(Is<SelectIterator>(0));
	for (Container::iterator it = container_.begin() + 1; it != container_.end(); ++it) {
		if (it->Is<SelectIterator>()) {
			it->Value<SelectIterator>().SetExpectMaxIterations(expectedIterations);
		}
	}
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry& qe, const NamespaceImpl& ns, StrictMode strictMode) {
	if (!qe.HaveEmptyField()) {
		return ComparatorNotIndexed{qe.FieldName(), qe.Condition(), qe.Values(), ns.payloadType_, qe.Fields().getTagsPath(0),
									qe.Distinct()};
	} else if (strictMode == StrictModeNone) {
		// Ignore non-index/non-existing fields
		if (qe.Condition() == CondEmpty) {
			return SelectKeyResult{{SingleSelectKeyResult{IdType(0), IdType(ns.items_.size())}}};
		} else {
			return SelectKeyResult{{SingleSelectKeyResult{IdType(0), IdType(0)}}};
		}
	} else {
		throw Error(
			errStrictMode,
			"Current query strict mode allows filtering by existing fields only. There are no fields with name '{}' in namespace '{}'",
			qe.FieldName(), ns.name_);
	}
}

template <bool left>
void SelectIteratorContainer::processField(FieldsComparator& fc, const QueryField& field, const NamespaceImpl& ns) const {
	if (field.IsFieldIndexed()) {
		auto& index = ns.indexes_[field.IndexNo()];
		if constexpr (left) {
			fc.SetLeftField(field.Fields(), field.FieldType(), index->Opts().IsArray(), index->Opts().collateOpts_);
		} else {
			fc.SetRightField(field.Fields(), field.FieldType(), index->Opts().IsArray());
		}
	} else if (field.HaveEmptyField()) {
		throw Error{errQueryExec, "Only existing fields can be compared. There are no fields with name '{}' in namespace '{}'",
					field.FieldName(), ns.name_};
	} else {
		if constexpr (left) {
			fc.SetLeftField(field.Fields());
		} else {
			fc.SetRightField(field.Fields());
		}
	}
}

SelectKeyResults SelectIteratorContainer::processQueryEntry(const QueryEntry& qe, bool enableSortIndexOptimize, const NamespaceImpl& ns,
															unsigned sortId, RankedTypeQuery rankedTypeQuery, RankSortType rankSortType,
															FtFunction::Ptr& ftFunc, RanksHolder::Ptr& ranks, reindexer::IsRanked& isRanked,
															IsSparse& isIndexSparse, QueryPreprocessor& qPreproc,
															const RdxContext& rdxCtx) {
	auto& index = ns.indexes_[qe.IndexNo()];
	const bool isFullText = index->IsFulltext();
	isRanked = reindexer::IsRanked(isFullText);
	isIndexSparse = index->Opts().IsSparse();

	Index::SelectContext selectCtx;
	selectCtx.opts.rankSortType = unsigned(rankSortType);
	selectCtx.opts.itemsCountInNamespace = ns.itemsCount();
	if (!ns.SortOrdersBuilt()) {
		selectCtx.opts.disableIdSetCache = 1;
	}
	if (rankedTypeQuery != RankedTypeQuery::No) {
		selectCtx.opts.forceComparator = 1;
	}
	if (ctx_->sortingContext.isOptimizationEnabled()) {
		if (enableSortIndexOptimize) {
			selectCtx.opts.unbuiltSortOrders = 1;
		} else {
			selectCtx.opts.forceComparator = 1;
		}
	}
	if (qe.Distinct()) {
		selectCtx.opts.distinct = 1;
	}
	selectCtx.opts.maxIterations = GetMaxIterations();
	selectCtx.opts.indexesNotOptimized = !ctx_->sortingContext.enableSortOrders;
	selectCtx.opts.inTransaction = ctx_->inTransaction;
	if (ftFunc) {
		selectCtx.selectFuncCtx.emplace(*ftFunc, ranks, qe.IndexNo());
	}

	if (index->Opts().GetCollateMode() == CollateUTF8 || isFullText) {
		for (auto& key : qe.Values()) {
			key.EnsureUTF8();
		}
	}
	PerfStatCalculatorMT calc(index->GetSelectPerfCounter(), ns.enablePerfCounters_);
	if (qPreproc.IsFtPreselected()) {
		return index->SelectKey(qe.Values(), qe.Condition(), selectCtx, qPreproc.MoveFtPreselect(), rdxCtx);
	} else {
		return index->SelectKey(qe.Values(), qe.Condition(), sortId, selectCtx, rdxCtx);
	}
}

void SelectIteratorContainer::processJoinEntry(const JoinQueryEntry& jqe, OpType op) {
	auto& js = (*ctx_->joinedSelectors)[jqe.joinIndex];
	if (js.JoinQuery().joinEntries_.empty()) {
		throw Error(errQueryExec, "Join without ON conditions");
	}
	if (js.JoinQuery().joinEntries_[0].Operation() == OpOr) {
		throw Error(errQueryExec, "The first ON condition cannot have OR operation");
	}
	if (js.Type() != InnerJoin && js.Type() != OrInnerJoin) {
		throw Error(errLogic, "Not INNER JOIN in QueryEntry");
	}
	if (js.Type() == OrInnerJoin) {
		if (op == OpNot) {
			throw Error(errQueryExec, "NOT operator with or_inner_join");
		}
		js.SetType(InnerJoin);
		op = OpOr;
	}
	if (op == OpOr && lastAppendedOrClosed() == this->end()) {
		throw Error(errQueryExec, "OR operator in first condition or after left join");
	}
	Append(op, JoinSelectIterator{static_cast<size_t>(jqe.joinIndex)});
}

void SelectIteratorContainer::processQueryEntryResults(SelectKeyResults&& selectResults, OpType op, const NamespaceImpl& ns,
													   const QueryEntry& qe, reindexer::IsRanked isRanked, IsSparse isIndexSparse,
													   std::optional<OpType> nextOp) {
	std::visit(
		overloaded{
			[&](SelectKeyResultsVector& selResults) {
				if (selResults.empty()) {
					if (op == OpAnd) {
						SelectKeyResult zeroScan;
						zeroScan.emplace_back(0, 0);
						Append(OpAnd,
							   SelectIterator{std::move(zeroScan), false, "always_false", IteratorFieldKind::None, ForcedFirst_True});
					}
					return;
				}
				for (SelectKeyResult& res : selResults) {
					switch (op) {
						case OpOr: {
							const iterator last = lastAppendedOrClosed();
							if (last == this->end()) {
								throw Error(errQueryExec, "OR operator in first condition or after left join");
							}
							if (last->Is<SelectIterator>() && !last->Value<SelectIterator>().distinct && last->operation != OpNot) {
								using namespace std::string_view_literals;
								SelectIterator& it = last->Value<SelectIterator>();
								it.Append(std::move(res));
								it.name.append(" or "sv).append(qe.FieldName());
								break;
							}
						}
							[[fallthrough]];
						case OpNot:
						case OpAnd:
							// Iterator Field Kind: Query entry results. Field known.
							Append<SelectIterator>(op, std::move(res), qe.Distinct(), std::string(qe.FieldName()),
												   qe.IndexNo() < 0 ? IteratorFieldKind::NonIndexed : IteratorFieldKind::Indexed,
												   ForcedFirst{*isRanked});
							if (qe.IsFieldIndexed() && !isIndexSparse) {
								// last appended is always a SelectIterator
								SelectIterator& lastAppended = lastAppendedOrClosed()->Value<SelectIterator>();
								lastAppended.SetNotOperationFlag(op == OpNot);
								if (!nextOp.has_value() || nextOp.value() != OpOr) {
									const auto maxIterations = lastAppended.GetMaxIterations();
									const int cur = op == OpNot ? ns.items_.size() - maxIterations : maxIterations;
									if (cur && cur < maxIterations_) {
										maxIterations_ = cur;
									}
									if (!cur) {
										wasZeroIterations_ = true;
									}
								}
							}
							break;
						default:
							throw Error(errQueryExec, "Unknown operator (code {}) in condition", int(op));
					}
					if (isRanked) {
						// last appended is always a SelectIterator
						lastAppendedOrClosed()->Value<SelectIterator>().SetUnsorted();
					}
				}
			},
			Restricted<ComparatorNotIndexed,
					   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>{}(
				[&](auto& c) {
					c.SetNotOperationFlag(op == OpNot);
					Append(op, std::move(c));
				})},
		selectResults.AsVariant());
}

void SelectIteratorContainer::processEqualPositions(const std::vector<EqualPositions>& equalPositions, const NamespaceImpl& ns,
													const QueryEntries& queries) {
	for (const auto& eqPos : equalPositions) {
		EqualPositionComparator cmp{ns.payloadType_};
		for (size_t i = 0, s = eqPos.size(); i < s; ++i) {
			const QueryEntry& qe = queries.Get<QueryEntry>(eqPos[i]);
			if (qe.Condition() == CondEmpty || (qe.Condition() == CondSet && qe.Values().empty())) {
				throw Error(errLogic, "Condition IN(with empty parameter list), IS NULL, IS EMPTY not allowed for equal position!");
			}
			assertrx_throw(qe.Fields().size() == 1);
			if (qe.Fields()[0] == IndexValueType::SetByJsonPath) {
				cmp.BindField(qe.FieldName(), qe.Fields().getFieldsPath(0), qe.Values(), qe.Condition());
			} else {
				const size_t idxNo = qe.Fields()[0];
				assertrx_throw(idxNo < ns.indexes_.size());
				cmp.BindField(qe.FieldName(), idxNo, qe.Values(), qe.Condition(), ns.indexes_[idxNo]->Opts().collateOpts_);
			}
		}
		Append(OpAnd, std::move(cmp));
	}
}

std::vector<SelectIteratorContainer::EqualPositions> SelectIteratorContainer::prepareEqualPositions(const QueryEntries& queries,
																									size_t begin, size_t end) {
	static const auto getFieldsStr = [](auto begin, auto end) {
		std::stringstream str;
		for (auto it = begin; it != end; ++it) {
			if (it != begin) {
				str << ", ";
			}
			str << *it;
		}
		return str.str();
	};
	const auto& eqPos = (begin == 0 ? queries.equalPositions : queries.Get<QueryEntriesBracket>(begin - 1).equalPositions);
	std::vector<EqualPositions> result{eqPos.size()};
	for (size_t i = 0; i < eqPos.size(); ++i) {
		if (eqPos[i].size() < 2) {
			throw Error(errLogic, "equal positions should contain 2 or more fields");
		}
		std::unordered_set<std::string_view> epFields{eqPos[i].begin(), eqPos[i].end()};
		const auto getEpFieldsStr = [&eqPos, i]() { return getFieldsStr(eqPos[i].cbegin(), eqPos[i].cend()); };
		if (eqPos[i].size() != epFields.size()) {
			throw Error(errParams, "equal positions fields should be unique: [{}]", getEpFieldsStr());
		}
		std::unordered_set<std::string_view> foundFields;
		result[i].reserve(eqPos[i].size());
		for (size_t j = begin, next; j < end; j = next) {
			next = queries.Next(j);
			queries.Visit(
				j, Skip<QueryEntriesBracket, JoinQueryEntry, AlwaysFalse, AlwaysTrue, KnnQueryEntry, DistinctQueryEntry>{},
				[](OneOf<SubQueryEntry, SubQueryFieldEntry>) { throw_as_assert; },
				[&](const QueryEntry& eq) {
					if (foundFields.find(eq.FieldName()) != foundFields.end()) {
						throw Error(errParams, "Equal position field '{}' found twice in enclosing bracket; equal position fields: [{}]",
									eq.FieldName(), getEpFieldsStr());
					}
					const auto it = epFields.find(eq.FieldName());
					if (it == epFields.end()) {
						return;
					}
					if (queries.GetOperation(j) != OpAnd || (next < end && queries.GetOperation(next) == OpOr)) {
						throw Error(errParams,
									"Only AND operation allowed for equal position; equal position field with not AND operation: '{}'; "
									"equal position fields: [{}]",
									eq.FieldName(), getEpFieldsStr());
					}
					result[i].push_back(j);
					foundFields.insert(epFields.extract(it));
				},
				[&](const BetweenFieldsQueryEntry& eq) {  // TODO equal positions for BetweenFieldsQueryEntry #1092
					if (epFields.find(eq.LeftFieldName()) != epFields.end()) {
						throw Error(
							errParams,
							"Equal positions for conditions between fields are not supported; field: '{}'; equal position fields: [{}]",
							eq.LeftFieldName(), getEpFieldsStr());
					}
					if (epFields.find(eq.RightFieldName()) != epFields.end()) {
						throw Error(
							errParams,
							"Equal positions for conditions between fields are not supported; field: '{}'; equal position fields: [{}]",
							eq.RightFieldName(), getEpFieldsStr());
					}
				});
		}
		if (!epFields.empty()) {
			throw Error(errParams, "Equal position fields [{}] are not found in enclosing bracket; equal position fields: [{}]",
						getFieldsStr(epFields.cbegin(), epFields.cend()), getEpFieldsStr());
		}
	}
	return result;
}

SelectKeyResult SelectIteratorContainer::processKnnQueryEntry(const KnnQueryEntry& qe, const NamespaceImpl& ns, RanksHolder::Ptr& ranks,
															  const RdxContext& rdxCtx) {
	const FloatVectorIndex& idx = static_cast<const FloatVectorIndex&>(*ns.indexes_[qe.IndexNo()]);
	assertrx_throw(!ranks);
	ranks = make_intrusive<RanksHolder>();
	KnnCtx knnCtx{ranks};
	knnCtx.NeedSort(NeedSort(ctx_->sortingContext.entries.empty()));

	return idx.Select(qe.Value(), qe.Params(), knnCtx, rdxCtx);
}

void SelectIteratorContainer::PrepareIteratorsForSelectLoop(QueryPreprocessor& qPreproc, unsigned sortId, RankedTypeQuery rankedTypeQuery,
															RankSortType rankSortType, const NamespaceImpl& ns, FtFunction::Ptr& ftFunc,
															RanksHolder::Ptr& ranks, const RdxContext& rdxCtx) {
	const auto containRanked =
		prepareIteratorsForSelectLoop(qPreproc, 0, qPreproc.Size(), sortId, rankedTypeQuery, rankSortType, ns, ftFunc, ranks, rdxCtx);
	(void)containRanked;
}

ContainRanked SelectIteratorContainer::prepareIteratorsForSelectLoop(QueryPreprocessor& qPreproc, size_t begin, size_t end, unsigned sortId,
																	 RankedTypeQuery rankedTypeQuery, RankSortType rankSortType,
																	 const NamespaceImpl& ns, FtFunction::Ptr& ftFunc,
																	 RanksHolder::Ptr& ranks, const RdxContext& rdxCtx) {
	const auto& queries = qPreproc.GetQueryEntries();
	auto equalPositions = prepareEqualPositions(queries, begin, end);
	bool sortIndexFound = false;
	ContainRanked containRanked = ContainRanked_False;
	for (size_t i = begin, next = begin; i != end; i = next) {
		next = queries.Next(i);
		const OpType op = queries.GetOperation(i);
		containRanked =
			queries.Visit(
				i, [](OneOf<SubQueryEntry, SubQueryFieldEntry>) -> ContainRanked { throw_as_assert; },
				[&](const QueryEntriesBracket&) {
					OpenBracket(op);
					const ContainRanked contRanked = prepareIteratorsForSelectLoop(qPreproc, i + 1, next, sortId, rankedTypeQuery,
																				   rankSortType, ns, ftFunc, ranks, rdxCtx);
					if (contRanked && (op != OpAnd || (next < end && queries.GetOperation(next) == OpOr))) {
						throw Error(errLogic, "OR and NOT operations are not allowed with bracket containing fulltext or knn condition");
					}
					CloseBracket();
					return contRanked;
				},
				[&](const DistinctQueryEntry& qe) {
					std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>> rawData;
					const FieldsSet& fieldSet = qe.FieldNames();
					assertrx_throw(fieldSet.size() > 1);
					rawData.reserve(fieldSet.size());
					int tagPathIdx = 0;
					for (const auto& f : fieldSet) {
						if (f != IndexValueType::SetByJsonPath) {
							if (ns.indexes_[f]->Type() == IndexRTree) {
								throw Error(errLogic, "Rtree index is not supported in the distinct aggregator. Field name '{}'",
											ns.indexes_[f]->Name());
							}
							const void* raw = ns.indexes_[f]->ColumnData();
							if (raw) {
								rawData.emplace_back(std::in_place_index<0>, raw, ns.indexes_[f]->SelectKeyType());
							} else {
								rawData.emplace_back(std::in_place_index<1>, f);
							}

						} else {
							rawData.emplace_back(std::in_place_index<2>, fieldSet.getTagsPath(tagPathIdx));
							tagPathIdx++;
						}
					}
					Append<ComparatorDistinctMulti>(op, ns.payloadType_, qe.FieldNames(), std::move(rawData));
					return ContainRanked_False;
				},
				[&](const QueryEntry& qe) {
					const ContainRanked isFT = ContainRanked(qe.IsFieldIndexed() && IsFullText(ns.indexes_[qe.IndexNo()]->Type()));
					if (isFT && (op != OpAnd || (next < end && queries.GetOperation(next) == OpOr))) {
						throw Error(errLogic, "OR and NOT operations are not allowed with fulltext index");
					}
					SelectKeyResults selectResults;

					reindexer::IsRanked isRanked = IsRanked_False;
					IsSparse isIndexSparse = IsSparse_False;
					if (qe.IsFieldIndexed()) {
						bool enableSortIndexOptimize = (ctx_->sortingContext.uncommitedIndex == qe.IndexNo()) && !sortIndexFound &&
													   (op == OpAnd) && !qe.Distinct() && (begin == 0) &&
													   (next == end || queries.GetOperation(next) != OpOr);
						if (enableSortIndexOptimize) {
							if (!IsExpectingOrderedResults(qe)) {
								// Disable sorting index optimization if it somehow has incompatible conditions
								enableSortIndexOptimize = false;
							}
							sortIndexFound = true;
						}
						selectResults = processQueryEntry(qe, enableSortIndexOptimize, ns, sortId, rankedTypeQuery, rankSortType, ftFunc,
														  ranks, isRanked, isIndexSparse, qPreproc, rdxCtx);
					} else {
						auto strictMode = ns.config_.strictMode;
						if (ctx_) {
							if (ctx_->inTransaction) {
								strictMode = StrictModeNone;
							} else if (ctx_->query.GetStrictMode() != StrictModeNotSet) {
								strictMode = ctx_->query.GetStrictMode();
							}
						}
						selectResults = processQueryEntry(qe, ns, strictMode);
					}
					std::optional<OpType> nextOp;
					if (next != end) {
						nextOp = queries.GetOperation(next);
					}
					processQueryEntryResults(std::move(selectResults), op, ns, qe, isRanked, isIndexSparse, nextOp);
					return isFT;
				},
				[this, op](const JoinQueryEntry& jqe) {
					processJoinEntry(jqe, op);
					return ContainRanked_False;
				},
				[&](const BetweenFieldsQueryEntry& qe) {
					FieldsComparator fc{qe.LeftFieldName(), qe.Condition(), qe.RightFieldName(), ns.payloadType_};
					processField<true>(fc, qe.LeftFieldData(), ns);
					processField<false>(fc, qe.RightFieldData(), ns);
					Append(op, std::move(fc));
					return ContainRanked_False;
				},
				[this, op](const AlwaysFalse&) {
					SelectKeyResult zeroScan;
					zeroScan.emplace_back(0, 0);
					Append<SelectIterator>(op, std::move(zeroScan), false, "always_false", IteratorFieldKind::None, ForcedFirst_True);
					return ContainRanked_False;
				},
				[this, op](const AlwaysTrue&) {
					Append(op, AlwaysTrue{});
					return ContainRanked_False;
				},
				[&](const KnnQueryEntry& qe) {
					if (op != OpAnd || (next < end && queries.GetOperation(next) == OpOr)) {
						throw Error(errLogic, "OR and NOT operations are not allowed with knn condition");
					}
					Append<SelectIterator>(op, processKnnQueryEntry(qe, ns, ranks, rdxCtx), false, qe.FieldName(),
										   IteratorFieldKind::Indexed, ForcedFirst_True);
					lastAppendedOrClosed()->Value<SelectIterator>().SetUnsorted();
					return ContainRanked_True;
				}) ||
			containRanked;
	}
	processEqualPositions(equalPositions, ns, queries);
	return containRanked;
}

template <bool reverse>
RX_ALWAYS_INLINE bool SelectIteratorContainer::checkIfSatisfyCondition(SelectIterator& it, bool* finish, IdType rowId) {
	IdType val = it.Val();
	if constexpr (reverse) {
		for (; val > rowId && it.Next(rowId); val = it.Val()) {
		}
	} else {
		for (; val < rowId && it.Next(rowId); val = it.Val()) {
		}
	}
	if (it.End()) {
		*finish = true;
		return false;
	}
	if constexpr (reverse) {
		return val >= rowId;
	} else {
		return val <= rowId;
	}
}

RX_ALWAYS_INLINE bool SelectIteratorContainer::checkIfSatisfyCondition(JoinSelectIterator& it, PayloadValue& pv, IdType properRowId,
																	   bool match) {
	assertrx_throw(ctx_->joinedSelectors);
	ConstPayload pl(*pt_, pv);
	auto& joinedSelector = (*ctx_->joinedSelectors)[it.joinIndex];
	return joinedSelector.Process(properRowId, ctx_->nsid, pl, ctx_->floatVectorsHolder, match);
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue& pv, bool* finish, IdType rowId,
														  IdType properRowId, bool match) {
	bool result = true;
	bool currentFinish = false;
	for (iterator it = begin; it != end; ++it) {
		const auto op = it->operation;
		if (op == OpOr) {
			// no short-circuit evaluation for TRUE OR JOIN
			// suggest that all JOINs in chain of OR ... OR ... OR ... OR will be before all not JOINs (see SortByCost)
			if (result) {
				// check what it does not holds join
				if (it->Visit([](const SelectIteratorsBracket& b) noexcept { return !b.haveJoins; },
							  [](const JoinSelectIterator&) noexcept { return false; },
							  [](OneOf<SelectIterator, FieldsComparator, AlwaysTrue, EqualPositionComparator, ComparatorNotIndexed,
									   ComparatorDistinctMulti,
									   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid,
												FloatVector>>) noexcept { return true; })) {
					continue;
				}
			}
		} else {
			if (!result) {
				break;
			}
		}
		bool lastFinish = false;
		const bool lastResult = it->Visit(
			[&] RX_PRE_LMBD_ALWAYS_INLINE(SelectIteratorsBracket&) RX_POST_LMBD_ALWAYS_INLINE {
				return checkIfSatisfyAllConditions<reverse, hasComparators>(it.begin(), it.end(), pv, &lastFinish, rowId, properRowId,
																			match);
			},
			[&] RX_PRE_LMBD_ALWAYS_INLINE(SelectIterator & sit)
				RX_POST_LMBD_ALWAYS_INLINE { return checkIfSatisfyCondition<reverse>(sit, &lastFinish, rowId); },
			[&] RX_PRE_LMBD_ALWAYS_INLINE(JoinSelectIterator & jit)
				RX_POST_LMBD_ALWAYS_INLINE { return checkIfSatisfyCondition(jit, pv, properRowId, match); },
			Restricted<FieldsComparator, EqualPositionComparator, ComparatorNotIndexed, ComparatorDistinctMulti,
					   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>{}(
				[&pv, properRowId] RX_PRE_LMBD_ALWAYS_INLINE(auto& c) RX_POST_LMBD_ALWAYS_INLINE { return c.Compare(pv, properRowId); }),
			[] RX_PRE_LMBD_ALWAYS_INLINE(AlwaysTrue&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; });
		if (op == OpOr) {
			result |= lastResult;
			currentFinish &= (!result && lastFinish);
		} else if (lastResult == (op == OpNot)) {
			result = false;
			currentFinish = lastFinish;
		} else {
			result = true;
			currentFinish = false;
		}
	}
	if (!result) {
		*finish = currentFinish;
	}
	return result;
}

template <bool reverse>
IdType SelectIteratorContainer::getNextItemId(const_iterator begin, const_iterator end, IdType from) {
	IdType result = from;
	for (const_iterator it = begin; it != end; ++it) {
		switch (it->operation) {
			case OpOr: {
				auto next = it->Visit(
					[it, from] RX_PRE_LMBD_ALWAYS_INLINE(const SelectIteratorsBracket&)
						RX_POST_LMBD_ALWAYS_INLINE { return getNextItemId<reverse>(it.cbegin(), it.cend(), from); },
					[from] RX_PRE_LMBD_ALWAYS_INLINE(const SelectIterator& sit) RX_POST_LMBD_ALWAYS_INLINE {
						if constexpr (reverse) {
							if (sit.End()) {
								return std::numeric_limits<IdType>::lowest();
							}
							if (const auto val = sit.Val(); val < from) {
								return val + 1;
							}
						} else {
							if (sit.End()) {
								return std::numeric_limits<IdType>::max();
							}
							if (const auto val = sit.Val(); val > from) {
								return val - 1;
							}
						}
						return from;
					},
					[from] RX_PRE_LMBD_ALWAYS_INLINE(
						const OneOf<
							JoinSelectIterator, FieldsComparator, EqualPositionComparator, ComparatorNotIndexed, ComparatorDistinctMulti,
							AlwaysTrue,
							Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>)
						RX_POST_LMBD_ALWAYS_INLINE { return from; },
					[] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysFalse&) RX_POST_LMBD_ALWAYS_INLINE {
						return reverse ? std::numeric_limits<IdType>::lowest() : std::numeric_limits<IdType>::max();
					});
				if constexpr (reverse) {
					result = std::max(result, next);
				} else {
					result = std::min(result, next);
				}
			} break;
			case OpAnd:
				from = result;
				result = it->Visit(
					[it, from] RX_PRE_LMBD_ALWAYS_INLINE(const SelectIteratorsBracket&)
						RX_POST_LMBD_ALWAYS_INLINE { return getNextItemId<reverse>(it.cbegin(), it.cend(), from); },
					[from] RX_PRE_LMBD_ALWAYS_INLINE(const SelectIterator& sit) RX_POST_LMBD_ALWAYS_INLINE {
						if constexpr (reverse) {
							if (sit.End()) {
								return std::numeric_limits<IdType>::lowest();
							}
							if (const auto val = sit.Val(); val < from) {
								return val + 1;
							}
						} else {
							if (sit.End()) {
								return std::numeric_limits<IdType>::max();
							}
							if (const auto val = sit.Val(); val > from) {
								return val - 1;
							}
						}
						return from;
					},
					[from] RX_PRE_LMBD_ALWAYS_INLINE(
						const OneOf<
							JoinSelectIterator, FieldsComparator, EqualPositionComparator, ComparatorNotIndexed, AlwaysTrue,
							ComparatorDistinctMulti,
							Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>)
						RX_POST_LMBD_ALWAYS_INLINE { return from; },
					[] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysFalse&) RX_POST_LMBD_ALWAYS_INLINE {
						return reverse ? std::numeric_limits<IdType>::lowest() : std::numeric_limits<IdType>::max();
					});
				break;
			case OpNot:
				break;
		}
	}
	return result;
}

template <bool reverse, bool hasComparators>
bool SelectIteratorContainer::Process(PayloadValue& pv, bool* finish, IdType* rowId, IdType properRowId, bool match) {
	if (auto it = begin(); checkIfSatisfyAllConditions<reverse, hasComparators>(++it, end(), pv, finish, *rowId, properRowId, match)) {
		return true;
	}
	*rowId = getNextItemId<reverse>(cbegin(), cend(), *rowId);
	return false;
}

template bool SelectIteratorContainer::Process<false, false>(PayloadValue&, bool*, IdType*, IdType, bool);
template bool SelectIteratorContainer::Process<false, true>(PayloadValue&, bool*, IdType*, IdType, bool);
template bool SelectIteratorContainer::Process<true, false>(PayloadValue&, bool*, IdType*, IdType, bool);
template bool SelectIteratorContainer::Process<true, true>(PayloadValue&, bool*, IdType*, IdType, bool);

std::string SelectIteratorContainer::Dump() const {
	WrSerializer ser;
	dump(0, cbegin(), cend(), *ctx_->joinedSelectors, ser);
	return std::string{ser.Slice()};
}

void SelectIteratorContainer::dump(size_t level, const_iterator begin, const_iterator end,
								   const std::vector<JoinedSelector>& joinedSelectors, WrSerializer& ser) {
	for (const_iterator it = begin; it != end; ++it) {
		for (size_t i = 0; i < level; ++i) {
			ser << "   ";
		}
		if (it != begin || it->operation != OpAnd) {
			ser << it->operation << ' ';
		}
		it->Visit(
			[&](const SelectIteratorsBracket&) {
				ser << "(\n";
				dump(level + 1, it.cbegin(), it.cend(), joinedSelectors, ser);
				for (size_t i = 0; i < level; ++i) {
					ser << "   ";
				}
				ser << ')';
			},
			Restricted<SelectIterator, FieldsComparator, EqualPositionComparator, ComparatorNotIndexed, ComparatorDistinctMulti,
					   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>{}(
				[&ser](const auto& c) { ser << c.Dump(); }),
			[&ser, &joinedSelectors](const JoinSelectIterator& jit) { jit.Dump(ser, joinedSelectors); },
			[&ser](const AlwaysTrue&) { ser << "Always True"; });
		ser << '\n';
	}
}

void JoinSelectIterator::Dump(WrSerializer& ser, const std::vector<JoinedSelector>& joinedSelectors) const {
	const auto& js = joinedSelectors.at(joinIndex);
	const auto& q = js.JoinQuery();
	ser << js.Type() << " (" << q.GetSQL() << ") ON ";
	ser << '(';
	for (const auto& jqe : q.joinEntries_) {
		if (&jqe != &q.joinEntries_.front()) {
			ser << ' ' << jqe.Operation() << ' ';
		} else {
			assertrx_throw(jqe.Operation() == OpAnd);
		}
		ser << q.NsName() << '.' << jqe.RightFieldName() << ' ' << jqe.Condition() << ' ' << jqe.LeftFieldName();
	}
	ser << ')';
}

}  // namespace reindexer
