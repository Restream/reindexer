#include "selectiteratorcontainer.h"

#include <numeric>
#include <span>
#include <sstream>
#include "core/index/float_vector/float_vector_index.h"
#include "core/index/float_vector/knn_ctx.h"
#include "core/namespace/namespaceimpl.h"
#include "core/rdxcontext.h"
#include "core/sorting/reranker.h"
#include "core/type_consts_helpers.h"
#include "estl/stable_sort.h"
#include "nsselecter.h"
#include "querypreprocessor.h"
#include "tools/use_pmr.h"

#ifdef USE_PMR
#include <memory_resource>
#endif	// USE_PMR

namespace reindexer {

constexpr std::string_view kForbiddenCondsErrorMsg =
	"Conditions IN(with empty parameter list), IS NULL, KNN and DWithin are not allowed for equal position";

void SelectIteratorContainer::SortByCost(int expectedIterations) {
	std::ignore = markBracketsHavingJoins(begin(), end());
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
			const reindexer::IsDistinct curDistinct = IsDistinct(indexes[cur]);
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
		result =
			it->Visit(
				[it] RX_PRE_LMBD_ALWAYS_INLINE(SelectIteratorsBracket & b)
					RX_POST_LMBD_ALWAYS_INLINE noexcept { return (b.haveJoins = markBracketsHavingJoins(it.begin(), it.end())); },
				[] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SelectIterator, AlwaysTrue, KnnRawSelectResult, ComparatorsPackT> auto&)
					RX_POST_LMBD_ALWAYS_INLINE noexcept { return false; },
				[] RX_PRE_LMBD_ALWAYS_INLINE(JoinSelectIterator&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; }) ||
			result;
	}
	return result;
}

bool SelectIteratorContainer::haveJoins(size_t i) const noexcept {
	return container_[i].Visit(
		[] RX_PRE_LMBD_ALWAYS_INLINE(const SelectIteratorsBracket& b) RX_POST_LMBD_ALWAYS_INLINE noexcept { return b.haveJoins; },
		[] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SelectIterator, AlwaysTrue, KnnRawSelectResult, ComparatorsPackT> auto&)
			RX_POST_LMBD_ALWAYS_INLINE noexcept { return false; },
		[] RX_PRE_LMBD_ALWAYS_INLINE(const JoinSelectIterator&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; });
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
		[expectedIterations] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SelectIterator, ComparatorsPackT> auto& c)
			RX_POST_LMBD_ALWAYS_INLINE noexcept { return c.Cost(expectedIterations); },
		[] RX_PRE_LMBD_ALWAYS_INLINE(const JoinSelectIterator& jit) RX_POST_LMBD_ALWAYS_INLINE noexcept { return jit.Cost(); },
		[expectedIterations] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysTrue&)
			RX_POST_LMBD_ALWAYS_INLINE noexcept -> double { return expectedIterations; },
		[] RX_PRE_LMBD_ALWAYS_INLINE(const KnnRawSelectResult&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return 0.0; });
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

h_vector<SelectKeyResults, 2> SelectIteratorContainer::processQueryEntry(const QueryEntry& qe, bool enableSortIndexOptimize,
																		 const NamespaceImpl& ns, unsigned sortId,
																		 QueryRankType queryRankType, RankSortType rankSortType,
																		 FtFunction::Ptr& ftFunc, RanksHolder::Ptr& ranks,
																		 reindexer::IsRanked& isRanked, StrictMode strictMode,
																		 QueryPreprocessor& qPreproc, const RdxContext& rdxCtx) {
	auto& index = ns.indexes_[qe.IndexNo()];
	isRanked = reindexer::IsRanked(index->IsFulltext() && !qe.IsDistinctOnly());

	Index::SelectContext selectCtx;
	selectCtx.opts.rankSortType = unsigned(rankSortType);
	selectCtx.opts.itemsCountInNamespace = ns.itemsCount();
	if (!ns.SortOrdersBuilt()) {
		selectCtx.opts.disableIdSetCache = 1;
	}
	if (queryRankType != QueryRankType::No) {
		selectCtx.opts.forceComparator = 1;
	}
	assertrx_throw(ctx_);
	if (ctx_->sortingContext.isOptimizationEnabled()) {
		if (enableSortIndexOptimize) {
			selectCtx.opts.unbuiltSortOrders = 1;
		} else {
			selectCtx.opts.forceComparator = 1;
		}
	}
	if (qe.Distinct()) {
		selectCtx.opts.distinct = 1;
		// Have to use distinct comparator with forced sort optimization for two-steps select implementation
		if (qPreproc.HasForcedSortOptimizationQueryEntry() && !qPreproc.IsSecondForcedSortStage()) {
			selectCtx.opts.forceComparator = 1;
		}
	}
	selectCtx.opts.maxIterations = GetMaxIterations();
	selectCtx.opts.indexesNotOptimized = !ctx_->sortingContext.enableSortOrders;
	selectCtx.opts.inTransaction = ctx_->inTransaction;
	selectCtx.opts.strictMode = strictMode;
	if (ftFunc && isRanked) {
		selectCtx.selectFuncCtx.emplace(*ftFunc, ranks, qe.IndexNo());
	}

	if (index->Opts().GetCollateMode() == CollateUTF8 || isRanked) {
		for (auto& key : qe.Values()) {
			key.EnsureUTF8();
		}
	}

	h_vector<SelectKeyResults, 2> result;
	PerfStatCalculatorMT calc(index->GetSelectPerfCounter(), ns.enablePerfCounters_);
	if (qPreproc.IsFtPreselected()) {
		result.emplace_back(index->SelectKey(qe.Values(), qe.Condition(), selectCtx, qPreproc.MoveFtPreselect(), rdxCtx));
	} else {
		result.emplace_back(index->SelectKey(qe.Values(), qe.Condition(), sortId, selectCtx, rdxCtx));
		if (qe.Distinct() && index->Opts().IsArray() && !result.back().IsComparator()) {
			// We have to use post-filtering via distinct comparator for array-indexes
			selectCtx.opts.forceComparator = 1;
			result.emplace_back(index->SelectKey(qe.Values(), qe.Condition(), sortId, selectCtx, rdxCtx));
		}
	}

	return result;
}

void SelectIteratorContainer::processJoinEntry(const JoinQueryEntry& jqe, OpType op) {
	assertrx_throw(ctx_);
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
	std::ignore = Append(op, JoinSelectIterator{static_cast<size_t>(jqe.joinIndex)});
}

void SelectIteratorContainer::processQueryEntryResults(SelectKeyResults&& selectResults, OpType op, const NamespaceImpl& ns,
													   const QueryEntry& qe, reindexer::IsRanked isRanked, std::optional<OpType> nextOp) {
	std::visit(overloaded{[&](SelectKeyResultsVector& selResults) {
							  if (selResults.empty()) {
								  if (op == OpAnd) {
									  SelectKeyResult zeroScan;
									  zeroScan.emplace_back(0, 0);
									  std::ignore = Append(OpAnd, SelectIterator{std::move(zeroScan), IsDistinct_False, "always_false",
																				 IndexValueType::NotSet, ForcedFirst_True});
								  }
								  return;
							  }

							  for (SelectKeyResult& res : selResults) {
								  switch (op) {
									  case OpOr: {
										  const iterator last = lastAppendedOrClosed();
										  if (last == end()) {
											  throw Error(errQueryExec, "OR operator in first condition or after left join");
										  }
										  if (last->Is<SelectIterator>() && !last->Value<SelectIterator>().IsDistinct() &&
											  last->operation != OpNot) {
											  using namespace std::string_view_literals;
											  SelectIterator& it = last->Value<SelectIterator>();
											  it.Append(std::move(res));
											  it.name.append(" or "sv).append(qe.FieldName());
											  break;
										  }
									  }
										  [[fallthrough]];
									  case OpNot:
									  case OpAnd: {
										  // Iterator Field Kind: Query entry results. Field known.
										  [[maybe_unused]] auto inserted =
											  Append<SelectIterator>(op, std::move(res), qe.Distinct(), std::string(qe.FieldName()),
																	 qe.IndexNo(), ForcedFirst{*isRanked});
										  assertrx_throw(inserted == 1);
										  // last appended is always a SelectIterator
										  SelectIterator& lastAppended = lastAppendedOrClosed()->Value<SelectIterator>();
										  lastAppended.SetNotOperationFlag(op == OpNot);
										  if (!nextOp.has_value() || nextOp.value() != OpOr) {
											  const auto maxIterations = lastAppended.GetMaxIterations();
											  const int cur = op == OpNot ? ns.items_.size() - maxIterations : maxIterations;
											  maxIterations_ = (maxIterations_ > cur) ? cur : maxIterations_;
										  }
										  break;
									  }
									  default:
										  throw Error(errQueryExec, "Unknown operator (code {}) in condition", int(op));
								  }
								  if (isRanked) {
									  // last appended is always a SelectIterator
									  lastAppendedOrClosed()->Value<SelectIterator>().SetUnsorted();
								  }
							  }
						  },
						  [&](concepts::OneOf<ComparatorNotIndexed, Template<ComparatorIndexed, bool, int, int64_t, double, key_string,
																			 PayloadValue, Point, Uuid, FloatVector>> auto& c) {
							  if (op == OpOr && lastAppendedOrClosed() == end()) {
								  throw Error(errQueryExec, "OR operator in first condition or after left join");
							  }
							  c.SetNotOperationFlag(op == OpNot);
							  std::ignore = Append(op, std::move(c));
						  }},
			   selectResults.AsVariant());
}
template <typename EqCompT>
void SelectIteratorContainer::bindFieldEqualPositions(const NamespaceImpl& ns, const EqualPositions& eqPos, const QueryEntries& queries,
													  const EqualPosition_t& eqPosStr) {
	EqCompT cmp{ns.payloadType_, &ns.tagsMatcher_};
	for (size_t i = 0, size = eqPos.size(); i < size; ++i) {
		const QueryEntry& qe = queries.Get<QueryEntry>(eqPos[i]);
		const auto cond = qe.Condition();
		switch (cond) {
			case CondDWithin:
			case CondKnn:
			case CondEmpty:
				throw Error(errQueryExec, kForbiddenCondsErrorMsg);
			case CondEq:
			case CondSet:
				if (qe.Values().empty()) {
					throw Error(errQueryExec, kForbiddenCondsErrorMsg);
				}
				break;
			case CondLike:
			case CondAny:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondRange:
			case CondAllSet:
				break;
			default:
				throw Error(errLogic, "Unknown condition value for equal position: {}", int(cond));
		}
		assertrx_throw(qe.Fields().size() == 1);
		if constexpr (std::is_same_v<EqCompT, EqualPositionComparator>) {
			if (qe.Fields()[0] == IndexValueType::SetByJsonPath) {
				cmp.BindField(qe.FieldName(), qe.Fields().getFieldsPath(0), qe.Values(), qe.Condition());
			} else {
				const size_t idxNo = qe.Fields()[0];
				assertrx_throw(idxNo < ns.indexes_.size());
				cmp.BindField(qe.FieldName(), idxNo, qe.Values(), qe.Condition(), ns.indexes_[idxNo]->Opts().collateOpts_);
			}
		} else {
			cmp.BindField(qe.FieldName(), qe.Values(), qe.Condition(), eqPosStr[i]);
		}
	}
	std::ignore = Append(OpAnd, std::move(cmp));
}

void SelectIteratorContainer::processEqualPositions(const h_vector<EqualPositions, 2>& equalPositions, const NamespaceImpl& ns,
													const QueryEntries& queries, const EqualPositions_t& eqPosStr) {
	std::vector<FieldPath> paths;
	for (size_t k = 0; k < equalPositions.size(); k++) {
		paths.resize(0);
		auto& eqPos = equalPositions[k];
		paths.resize(eqPosStr[k].size());
		unsigned int arrayPathCount = 0;
		for (size_t i = 0, s = eqPos.size(); i < s; ++i) {
			equal_position_helpers::ParseStrPath(eqPosStr[k][i], paths[i]);
			bool isArray = false;
			for (const auto& part : paths[i]) {
				if (part.flags == FieldPathPartFlags::Array) {
					isArray = true;
					break;
				}
			}
			if (isArray) {
				arrayPathCount++;
			}
		}
		if (arrayPathCount == 0) {
			bindFieldEqualPositions<EqualPositionComparator>(ns, eqPos, queries, eqPosStr[k]);
		} else {
			assertrx_throw(arrayPathCount == paths.size());
			bindFieldEqualPositions<GroupingEqualPositionComparator>(ns, eqPos, queries, eqPosStr[k]);
		}
	}
}

h_vector<SelectIteratorContainer::EqualPositions, 2> SelectIteratorContainer::prepareEqualPositions(const NamespaceImpl& ns,
																									const QueryEntries& queries,
																									size_t begin, size_t end) {
	static const auto getFieldsStr = [](auto begin, auto end, auto getValue) {
		std::stringstream str;
		for (auto it = begin; it != end; ++it) {
			auto val = getValue(it);
			if (!val.empty()) {
				if (str.tellp() != std::streampos(0)) {
					str << ", ";
				}
				str << val;
			}
		}
		return std::move(str).str();
	};

	// If it was possible to substitute an index into the condition, it was already substituted.
	// It was not substituted in two cases:
	// 1. It doesn't exist.
	// 2. The index has more than one path.
	const EqualPositions_t& eqPosFunctions =
		(begin == 0 ? queries.equalPositions : queries.Get<QueryEntriesBracket>(begin - 1).equalPositions);
	h_vector<EqualPositions, 2> result{unsigned(eqPosFunctions.size())};
	for (size_t i = 0, sz = eqPosFunctions.size(); i < sz; ++i) {
		if (eqPosFunctions[i].size() < 2) {
			throw Error(errLogic, "equal positions should contain 2 or more fields");
		}

		const auto getEpFieldsStr = [&eqPosFunctions, i, &ns]() {
			return getFieldsStr(eqPosFunctions[i].cbegin(), eqPosFunctions[i].cend(), [&ns](auto& it) {
				std::string res(*it);
				int indNo = 0;
				if (ns.tryGetIndexByJsonPath(*it, indNo, EnableMultiJsonPath_True)) {
					res += '(';
					res += ns.indexes_[indNo]->Name();
					res += ')';
				}
				return res;
			});
		};

		fast_hash_map<std::string, int> epFieldsJson;  // json, argument num
		fast_hash_map<int, int> epFieldsIndexed;	   // index, argument num
		fast_hash_set<int> subResult;
		unsigned int arrayMarkerCount = 0;
		FieldPath path;
		for (size_t j = 0, sz = eqPosFunctions[i].size(); j < sz; ++j) {
			path.clear();

			equal_position_helpers::ParseStrPath(eqPosFunctions[i][j], path);
			for (const auto& v : path) {
				if (v.flags == FieldPathPartFlags::Array) {
					arrayMarkerCount++;
					break;
				}
			}
			std::string simplePath;
			bool flagFirst = true;
			for (const auto& v : path) {
				if (!flagFirst) {
					simplePath += '.';
				}
				simplePath += v.name;
				flagFirst = false;
			}
			int indNo = IndexValueType::NotSet;
			if (ns.tryGetIndexByName(simplePath, indNo)) {	// index name
				if (epFieldsIndexed.find(indNo) != epFieldsIndexed.end()) {
					throw Error(errParams, "equal positions fields should be unique: [{}]", getEpFieldsStr());
				}
				epFieldsIndexed.emplace(indNo, j);
			} else {  // json
				if (ns.tryGetIndexByJsonPath(simplePath, indNo, EnableMultiJsonPath_True)) {
					epFieldsIndexed.emplace(indNo, j);
				}
				if (epFieldsJson.find(simplePath) != epFieldsJson.end()) {
					throw Error(errParams, "equal positions fields should be unique: [{}]", getEpFieldsStr());
				}
				epFieldsJson.emplace(std::move(simplePath), j);
			}
		}

		if (arrayMarkerCount > 0 && arrayMarkerCount < eqPosFunctions[i].size()) {
			throw Error(errParams, "All equal positions fields should contain array marker");
		}

		result[i].reserve(eqPosFunctions[i].size());

		auto checkFound = [&](const QueryEntry& qEntry, auto& epFields, const auto& val) -> bool {
			auto found = epFields.find(val);
			if (found == epFields.end()) {
				return false;
			}

			auto foundRecord = subResult.find(found->second);
			if (foundRecord != subResult.end()) {
				throw Error(errParams, "Equal position field '{}' found twice in enclosing bracket; equal position fields: [{}]",
							qEntry.FieldName(), getEpFieldsStr());
			}
			subResult.insert(found->second);
			return true;
		};

		for (size_t j = begin, next; j < end; j = next) {
			next = queries.Next(j);
			queries.Visit(
				j, Skip<QueryEntriesBracket, JoinQueryEntry, AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry>{},
				[](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) { throw_as_assert; },
				[](const KnnQueryEntry&) { throw Error(errQueryExec, kForbiddenCondsErrorMsg); },
				[&](const QueryEntry& qEntry) {
					if (qEntry.IsFieldIndexed()) {
						if (!checkFound(qEntry, epFieldsIndexed, qEntry.IndexNo())) {
							return;
						}
					} else {
						if (!checkFound(qEntry, epFieldsJson, qEntry.FieldName())) {
							return;
						}
					}
					if (queries.GetOperation(j) != OpAnd || (next < end && queries.GetOperation(next) == OpOr)) {
						throw Error(errParams,
									"Only AND operation allowed for equal position; equal position field with not AND operation: '{}'; "
									"equal position fields: [{}]",
									qEntry.FieldName(), getEpFieldsStr());
					}

					result[i].push_back(j);
				},
				[&](const BetweenFieldsQueryEntry& eq) {  // TODO equal positions for BetweenFieldsQueryEntry #1092
					int indxNoLeft = eq.LeftIdxNo();
					if ((indxNoLeft == IndexValueType::NotSet && epFieldsJson.find(eq.LeftFieldName()) != epFieldsJson.end()) ||
						(indxNoLeft >= 0 && epFieldsIndexed.find(indxNoLeft) != epFieldsIndexed.end())) {
						throw Error(
							errParams,
							"Equal positions for conditions between fields are not supported; field: '{}'; equal position fields: [{}]",
							eq.LeftFieldName(), getEpFieldsStr());
					}
					int indxNoRight = eq.RightIdxNo();
					if ((indxNoRight == IndexValueType::NotSet && epFieldsJson.find(eq.RightFieldName()) != epFieldsJson.end()) ||
						(indxNoRight >= 0 && epFieldsIndexed.find(indxNoRight) != epFieldsIndexed.end())) {
						throw Error(
							errParams,
							"Equal positions for conditions between fields are not supported; field: '{}'; equal position fields: [{}]",
							eq.RightFieldName(), getEpFieldsStr());
					}

				});
		}
		if (result[i].size() != eqPosFunctions[i].size()) {
			std::string errFields;
			for (size_t j = 0, sz = eqPosFunctions[i].size(); j < sz; ++j) {
				if (subResult.find(j) == subResult.end()) {
					if (!errFields.empty()) {
						errFields += ' ';
					}
					errFields += eqPosFunctions[i][j];
				}
			}
			throw Error(errParams, "Equal position fields [{}] are not found in enclosing bracket; equal position fields: [{}]", errFields,
						getEpFieldsStr());
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
	assertrx_throw(ctx_);
	knnCtx.NeedSort(NeedSort(ctx_->sortingContext.entries.empty()));
	return idx.Select(qe.Value(), qe.Params(), knnCtx, rdxCtx);
}

KnnRawResult SelectIteratorContainer::processKnnQueryEntryRaw(const KnnQueryEntry& qe, const NamespaceImpl& ns, const RdxContext& rdxCtx) {
	const FloatVectorIndex& idx = static_cast<const FloatVectorIndex&>(*ns.indexes_[qe.IndexNo()]);
	if (qe.Format() != KnnQueryEntry::DataFormatType::Vector) {
		logFmt(LogWarning, "KnnQueryEntry data format is not 'vector'. Create empty KNN result");
		return KnnRawResult(EmptyKnnRawResult{}, VectorMetric::L2);
	}

	return idx.SelectRaw(qe.Value(), qe.Params(), rdxCtx);
}

void SelectIteratorContainer::PrepareIteratorsForSelectLoop(QueryPreprocessor& qPreproc, unsigned sortId, QueryRankType queryRankType,
															RankSortType rankSortType, const NamespaceImpl& ns, FtFunction::Ptr& ftFunc,
															RanksHolder::Ptr& ranks, const RdxContext& rdxCtx) {
	const auto containRanked =
		prepareIteratorsForSelectLoop(qPreproc, 0, qPreproc.Size(), sortId, queryRankType, rankSortType, ns, ftFunc, ranks, rdxCtx);
	(void)containRanked;
}

void SelectIteratorContainer::ExplainJSON(int iters, JsonBuilder& builder, const std::vector<JoinedSelector>* js) const {
	std::ignore = explainJSON(cbegin(), cend(), iters, builder, js);
	if (!preservedDistincts_.Empty()) [[unlikely]] {
		std::ignore = explainJSON(preservedDistincts_.cbegin(), preservedDistincts_.cend(), iters, builder, nullptr);
	}
}

void SelectIteratorContainer::Clear(bool preserveDistincts) {
	if (preserveDistincts) [[unlikely]] {
		assertrx_throw(preservedDistincts_.Empty());
		for (size_t i = 0, sz = Size(); i < sz; ++i) {
			Visit(
				i, Skip<JoinSelectIterator, AlwaysFalse, AlwaysTrue, KnnRawSelectResult>{},
				[this, &i](const SelectIteratorsBracket& bracket) {
					// Check that brackets does not contain distincts
					for (size_t bracketEnd = i + bracket.Size(); i < bracketEnd; ++i) {
						Visit(i, Skip<SelectIteratorsBracket, JoinSelectIterator, AlwaysFalse, AlwaysTrue, KnnRawSelectResult>{},
							  [](const concepts::OneOf<SelectIterator, ComparatorsPackT> auto& e) {
								  if (e.IsDistinct()) [[unlikely]] {
									  throw Error(errLogic, "Unexpected distinct inside bracket");
								  }
							  });
					}
				},
				[]([[maybe_unused]] SelectIterator& it) {
					// Iterator's state can not be migrated between execution steps
					assertrx_throw(!it.IsDistinct());
				},
				[this, i](concepts::OneOf<ComparatorsPackT> auto& comp) {
					if (comp.IsDistinct()) {
						std::ignore = preservedDistincts_.Append(GetOperation(i), std::move(comp));
					}
				});
			assertrx_throw(preservedDistincts_.Empty() || preservedDistincts_.GetOperation(0) == OpAnd);
		}
	}
	clear();

	maxIterations_ = std::numeric_limits<int>::max();
}

[[noreturn]] static void throwORbetweenRankedAndNotRanked() {
	throw Error(errLogic, "OR operation is not allowed between ranked and not ranked conditions");
}

void SelectIteratorContainer::throwIfNotFt(const QueryEntries& queries, size_t i, const NamespaceImpl& ns) {
	if (!queries.Is<QueryEntry>(i)) {
		throwORbetweenRankedAndNotRanked();
	}
	const QueryEntry& qe = queries.Get<QueryEntry>(i);
	if (!qe.IsFieldIndexed() || !IsFullText(ns.indexes_[qe.IndexNo()]->Type())) {
		throwORbetweenRankedAndNotRanked();
	}
}

typename SelectIteratorContainer::iterator SelectIteratorContainer::lastAppendedOrClosed() {
	auto it = container_.begin(), end = container_.end();
	if (!activeBrackets_.empty()) {
		it += (activeBrackets_.back() + 1);
	}
	if (it == end) {
		return this->end();
	}
	iterator i = it, i2 = it, e = end;
	while (++i2 != e) {
		i = i2;
	}
	return i;
}

ContainRanked SelectIteratorContainer::prepareIteratorsForSelectLoop(QueryPreprocessor& qPreproc, size_t begin, size_t end, unsigned sortId,
																	 QueryRankType queryRankType, RankSortType rankSortType,
																	 const NamespaceImpl& ns, FtFunction::Ptr& ftFunc,
																	 RanksHolder::Ptr& ranks, const RdxContext& rdxCtx) {
	const auto& queries = qPreproc.GetQueryEntries();
	auto equalPositions = prepareEqualPositions(ns, queries, begin, end);
	bool sortIndexFound = false;
	ContainRanked containRanked = ContainRanked_False;
	for (size_t i = begin, prev = begin, next = begin; i != end; prev = std::exchange(i, next)) {
		next = queries.Next(i);
		const OpType op = queries.GetOperation(i);
		containRanked |= queries.Visit(
			i, [](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) -> ContainRanked { throw_as_assert; },
			[&](const QueryEntriesBracket&) {
				OpenBracket(op);
				const ContainRanked contRanked =
					prepareIteratorsForSelectLoop(qPreproc, i + 1, next, sortId, queryRankType, rankSortType, ns, ftFunc, ranks, rdxCtx);
				if (contRanked && (op != OpAnd || (next < end && queries.GetOperation(next) == OpOr))) {
					throw Error(errLogic, "OR and NOT operations are not allowed with bracket containing fulltext or knn condition");
				}
				CloseBracket();
				return contRanked;
			},
			[&](const MultiDistinctQueryEntry& qe) {
				if (hasDistinctComparatorsFromPreviousStage()) {
					return ContainRanked_False;
				}

				std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>> rawData;
				std::vector<int> fieldsIndex;
				std::vector<std::pair<const void*, KeyValueType>> fieldsColumn;
				const FieldsSet& fieldSet = qe.FieldNames();
				const size_t fieldSetSize = fieldSet.size();
				assertrx_throw(fieldSetSize > 1);
				rawData.reserve(fieldSetSize);
				fieldsIndex.reserve(fieldSetSize);
				fieldsColumn.reserve(fieldSetSize);
				int tagPathIdx = 0;
				unsigned int indexedColumnCount = 0;
				unsigned int indexedScalarCount = 0;
				unsigned int indexedArrayCount = 0;
				std::string comparatorName("MultiDistinct");
				for (const auto& f : fieldSet) {
					if (f != IndexValueType::SetByJsonPath) {
						if (ns.indexes_[f]->Type() == IndexRTree) {
							throw Error(errLogic, "Rtree index is not supported in the distinct aggregator. Field name '{}'",
										ns.indexes_[f]->Name());
						}
						const void* raw = ns.indexes_[f]->ColumnData();
						if (raw) {
							++indexedColumnCount;
							rawData.emplace_back(std::in_place_index<0>, raw, ns.indexes_[f]->SelectKeyType());
							fieldsColumn.emplace_back(raw, ns.indexes_[f]->SelectKeyType());
						} else {
							ns.indexes_[f]->Opts().IsArray() ? indexedArrayCount++ : indexedScalarCount++;
							rawData.emplace_back(std::in_place_index<1>, f);
							fieldsIndex.emplace_back(f);
						}
						comparatorName += ' ';
						comparatorName += ns.indexes_[f]->Name();
					} else {
						rawData.emplace_back(std::in_place_index<2>, fieldSet.getTagsPath(tagPathIdx));
						comparatorName += ' ';
						comparatorName += ns.tagsMatcher_.Path2Name(fieldSet.getTagsPath(tagPathIdx));
						tagPathIdx++;
					}
				}
				if (indexedColumnCount == fieldSetSize) {
					ComparatorDistinctMultiColumnGetter getter{std::move(fieldsColumn)};
					std::ignore = Append<ComparatorDistinctMultiScalarBase<ComparatorDistinctMultiColumnGetter>>(
						op, std::move(comparatorName), std::move(getter));

				} else if (indexedScalarCount == fieldSetSize) {
					ComparatorDistinctMultiIndexedGetter getter{ns.payloadType_, std::move(fieldsIndex)};
					std::ignore = Append<ComparatorDistinctMultiScalarBase<ComparatorDistinctMultiIndexedGetter>>(
						op, std::move(comparatorName), std::move(getter));
				} else if (indexedArrayCount == fieldSetSize) {
					std::ignore =
						Append<ComparatorDistinctMultiArray>(op, ns.payloadType_, std::move(comparatorName), std::move(fieldsIndex));

				} else if (indexedColumnCount + indexedScalarCount == fieldSetSize) {
					ComparatorDistinctMultiScalarGetter getter{ns.payloadType_, std::move(rawData)};
					std::ignore = Append<ComparatorDistinctMultiScalarBase<ComparatorDistinctMultiScalarGetter>>(
						op, std::move(comparatorName), std::move(getter));
				} else {
					std::ignore = Append<ComparatorDistinctMulti>(op, ns.payloadType_, std::move(comparatorName), qe.FieldNames(),
																  std::move(rawData));
				}
				return ContainRanked_False;
			},
			[&](const QueryEntry& qe) {
				const ContainRanked isFT =
					ContainRanked(qe.IsFieldIndexed() && !qe.IsDistinctOnly() && IsFullText(ns.indexes_[qe.IndexNo()]->Type()));
				if (isFT) {
					switch (op) {
						case OpAnd:
							break;
						case OpNot:
							throw Error(errLogic, "NOT operation is not allowed with fulltext index: '{}'",
										ns.indexes_[qe.IndexNo()]->Name());
						case OpOr:
							if (!queries.Is<KnnQueryEntry>(prev)) {
								throwORbetweenRankedAndNotRanked();
							}
							break;
						default:
							throw_as_assert;
					}
					if (next != end && queries.GetOperation(next) == OpOr && !queries.Is<KnnQueryEntry>(next)) {
						throwORbetweenRankedAndNotRanked();
					}
				}
				h_vector<SelectKeyResults, 2> selectResults;

				reindexer::IsRanked isRanked = IsRanked_False;
				auto strictMode = ns.config_.strictMode;
				if (ctx_) {
					if (ctx_->inTransaction) {
						strictMode = StrictModeNone;
					} else if (ctx_->query.GetStrictMode() != StrictModeNotSet) {
						strictMode = ctx_->query.GetStrictMode();
					}
				}
				if (qe.IsFieldIndexed()) {
					bool enableSortIndexOptimize = ctx_ && (ctx_->sortingContext.uncommitedIndex == qe.IndexNo()) && !sortIndexFound &&
												   (op == OpAnd) && !qe.Distinct() && (begin == 0) &&
												   (next == end || queries.GetOperation(next) != OpOr);
					if (enableSortIndexOptimize) {
						if (!IsExpectingOrderedResults(qe)) {
							// Disable sorting index optimization if it somehow has incompatible conditions
							enableSortIndexOptimize = false;
						}
						sortIndexFound = true;
					}
					selectResults = processQueryEntry(qe, enableSortIndexOptimize, ns, sortId, queryRankType, rankSortType, ftFunc, ranks,
													  isRanked, strictMode, qPreproc, rdxCtx);
					if (qe.Distinct() && hasDistinctComparatorsFromPreviousStage() &&
						std::all_of(selectResults.begin(), selectResults.end(), [](const auto& v) { return v.IsComparator(); })) {
						return ContainRanked_False;
					}
				} else {
					if (qe.Distinct() && hasDistinctComparatorsFromPreviousStage()) {
						return ContainRanked_False;
					}
					selectResults.emplace_back(processQueryEntry(qe, ns, strictMode));
				}
				std::optional<OpType> nextOp;
				if (next != end) {
					nextOp = queries.GetOperation(next);
				}
				for (auto&& res : selectResults) {
					processQueryEntryResults(std::move(res), op, ns, qe, isRanked, nextOp);
				}
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
				std::ignore = Append(op, std::move(fc));
				return ContainRanked_False;
			},
			[this, op](const AlwaysFalse&) {
				SelectKeyResult zeroScan;
				zeroScan.emplace_back(0, 0);
				std::ignore = Append<SelectIterator>(op, std::move(zeroScan), IsDistinct_False, "always_false", IndexValueType::NotSet,
													 ForcedFirst_True);
				return ContainRanked_False;
			},
			[this, op](const AlwaysTrue&) {
				std::ignore = Append(op, AlwaysTrue{});
				return ContainRanked_False;
			},
			[&](const KnnQueryEntry& qe) {
				switch (op) {
					case OpAnd:
						break;
					case OpNot:
						throw Error(errLogic, "NOT operation is not allowed with knn condition");
					case OpOr:
						throwIfNotFt(queries, prev, ns);
						break;
					default:
						throw_as_assert;
				}
				if (next != end && queries.GetOperation(next) == OpOr) {
					throwIfNotFt(queries, next, ns);
				}

				if (queryRankType == QueryRankType::Hybrid) {
					if (qe.Format() != KnnQueryEntry::DataFormatType::Vector) {
						// NOTE: earlier we will save error in query data
						// check if current operation is not OR and the next operation, if any, is not OR too
						if ((queries.GetOperation(i) != OpOr) && (next == end || queries.GetOperation(next) != OpOr)) {
							throw Error(errNetwork, qe.Data());
						}

						// processed at next levels but logs an error
						logFmt(LogError, "{}", qe.Data());
					}
					std::ignore = Append<KnnRawSelectResult>(op, processKnnQueryEntryRaw(qe, ns, rdxCtx), qe.IndexNo());
				} else {
					if (qe.Format() != KnnQueryEntry::DataFormatType::Vector) {
						throw Error(errNetwork, qe.Data());	 // NOTE: earlier we will save error in query data
					}
					[[maybe_unused]] auto inserted = Append<SelectIterator>(
						op, processKnnQueryEntry(qe, ns, ranks, rdxCtx), IsDistinct_False, qe.FieldName(), qe.IndexNo(), ForcedFirst_True);
					assertrx_throw(inserted == 1);
					lastAppendedOrClosed()->Value<SelectIterator>().SetUnsorted();
				}
				return ContainRanked_True;
			});
	}
	const EqualPositions_t& eqPosStr = (begin == 0 ? queries.equalPositions : queries.Get<QueryEntriesBracket>(begin - 1).equalPositions);
	processEqualPositions(equalPositions, ns, queries, eqPosStr);
	return containRanked;
}

bool SelectIteratorContainer::checkIfSatisfyAllConditions(iterator begin, iterator end, const PayloadValue& pv, bool* finish, IdType rowId,
														  IdType properRowId, bool withJoinedItems) {
	bool result = true;
	bool orChainFinish = false;
	for (auto it = begin; it != end; ++it) {
		const auto op = it->operation;
		if (op == OpOr) {
			// no short-circuit evaluation for TRUE OR JOIN
			// suggest that all JOINs in chain of OR ... OR ... OR ... OR will be before all not JOINs (see SortByCost)
			if (result) {
				// check what it does not holds join
				if (it->Visit([](const SelectIteratorsBracket& b) noexcept { return !b.haveJoins; },
							  [](const JoinSelectIterator&) noexcept { return false; },
							  [](const concepts::OneOf<SelectIterator, AlwaysTrue, ComparatorsPackT> auto&) noexcept { return true; },
							  [](const KnnRawSelectResult&) -> bool { throw_as_assert; })) {
					continue;
				}
			}
		} else {
			if (!result) {
				break;
			}
		}
		bool lastFinish = false;
		const bool lastResult =
			it->Visit([](const KnnRawSelectResult&) -> bool { throw_as_assert; },
					  [&] RX_PRE_LMBD_ALWAYS_INLINE(SelectIteratorsBracket&) RX_POST_LMBD_ALWAYS_INLINE {
						  return checkIfSatisfyAllConditions(it.begin(), it.end(), pv, &lastFinish, rowId, properRowId, withJoinedItems);
					  },
					  [&] RX_PRE_LMBD_ALWAYS_INLINE(SelectIterator & sit) RX_POST_LMBD_ALWAYS_INLINE {
						  const bool res = sit.Compare(rowId);
						  lastFinish = sit.End();
						  return res;
					  },
					  [&] RX_PRE_LMBD_ALWAYS_INLINE(JoinSelectIterator & jit) RX_POST_LMBD_ALWAYS_INLINE {
						  assertrx_throw(ctx_ && ctx_->joinedSelectors);
						  ConstPayload pl(*pt_, pv);
						  auto& joinedSelector = (*ctx_->joinedSelectors)[jit.joinIndex];
						  return joinedSelector.Process(properRowId, ctx_->nsid, pl, ctx_->floatVectorsHolder, withJoinedItems);
					  },
					  [&pv, properRowId] RX_PRE_LMBD_ALWAYS_INLINE(concepts::OneOf<ComparatorsPackT> auto& c)
						  RX_POST_LMBD_ALWAYS_INLINE { return c.Compare(pv, properRowId); },
					  [] RX_PRE_LMBD_ALWAYS_INLINE(AlwaysTrue&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; });
		switch (op) {
			case OpOr:
				result |= lastResult;
				orChainFinish &= lastFinish;
				break;
			case OpAnd:
				result = lastResult;
				*finish |= orChainFinish;
				orChainFinish = lastFinish;
				break;
			case OpNot:
				result = !lastResult;
				*finish |= orChainFinish;
				orChainFinish = false;
				break;
		}
	}
	*finish |= orChainFinish;

	return result;
}

template <bool reverse>
IdType SelectIteratorContainer::getNextItemId(const_iterator begin, const_iterator end, IdType from) {
	IdType result = from;
	for (const_iterator it = begin; it != end; ++it) {
		switch (it->operation) {
			case OpOr: {
				auto next = it->Visit(
					[](const KnnRawSelectResult&) -> int { throw_as_assert; },
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
					[from] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<JoinSelectIterator, AlwaysTrue, ComparatorsPackT> auto&)
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
					[](const KnnRawSelectResult&) -> int { throw_as_assert; },
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
					[from] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<JoinSelectIterator, AlwaysTrue, ComparatorsPackT> auto&)
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

template IdType SelectIteratorContainer::getNextItemId<true>(const_iterator, const_iterator, IdType);
template IdType SelectIteratorContainer::getNextItemId<false>(const_iterator, const_iterator, IdType);

bool SelectIteratorContainer::isRanked(const_iterator it, const NamespaceImpl& ns) {
	return it->Visit(
		[](const concepts::OneOf<JoinSelectIterator, SelectIteratorsBracket, AlwaysTrue, ComparatorsPackT> auto&) noexcept {
			return false;
		},
		[](const KnnRawSelectResult&) noexcept { return true; },
		[&ns](const SelectIterator& si) noexcept {
			if (si.IndexNo() < 0) {
				return false;
			}
			assertrx_throw(size_t(si.IndexNo()) < ns.indexes_.size());
			const auto& idx = *ns.indexes_[si.IndexNo()];
			assertrx_throw(!idx.IsFloatVector());
			return idx.IsFulltext() || idx.IsFloatVector();
		});
}

std::pair<SelectIteratorContainer::iterator, SelectIteratorContainer::iterator> SelectIteratorContainer::findFirstRanked(
	iterator begin, iterator end, const NamespaceImpl& ns) {
	for (auto it = begin; it != end; ++it) {
		if (it->IsSubTree()) {
			const auto [i, e] = findFirstRanked(it.begin(), it.end(), ns);
			if (i != e) {
				return {i, e};
			}
		}
		if (isRanked(it, ns)) {
			return {it, end};
		}
	}
	return {end, end};
}

enum class SelectIteratorContainer::MergeType : bool { Intersection, Union };

template <bool desc>
struct [[nodiscard]] IdRank {
	IdRank() noexcept = default;
	IdRank(IdType id, RankT rank) noexcept : id{id}, rank{rank} {}

	IdType id;
	RankT rank;
};

bool operator<(const IdRank<false>& lhs, const IdRank<false>& rhs) noexcept {
	if (lhs.rank < rhs.rank) {
		return true;
	} else if (lhs.rank > rhs.rank) {
		return false;
	} else {
		return lhs.id < rhs.id;
	}
}

bool operator<(const IdRank<true>& lhs, const IdRank<true>& rhs) noexcept {
	if (lhs.rank > rhs.rank) {
		return true;
	} else if (lhs.rank < rhs.rank) {
		return false;
	} else {
		return lhs.id > rhs.id;
	}
}

#ifdef USE_PMR

template <bool desc>
using Merged = std::pmr::set<IdRank<desc>>;

#else  // USE_PMR

template <bool desc>
struct [[nodiscard]] IdRankHash {
	size_t operator()(const IdRank<desc>& idRank) const noexcept {
		std::hash<IdType> hasher;
		return hasher(idRank.id);
	}
};

template <bool desc>
struct [[nodiscard]] IdRankEq {
	bool operator()(const IdRank<desc>& lhs, const IdRank<desc>& rhs) const noexcept { return lhs.id == rhs.id; }
};

template <bool desc>
using Merged = fast_hash_set<IdRank<desc>, IdRankHash<desc>, IdRankEq<desc>, std::less<IdRank<desc>>>;

#endif	// USE_PMR

class [[nodiscard]] SelectIteratorContainer::MergerRanked {
	template <MergeType, bool desc, VectorMetric>
	friend class MergerRankedImpl;

public:
	MergerRanked(const SelectIterator& ftSelectIter, std::span<const RankT> ftRanks, std::span<const size_t> ftPositions) noexcept
		: ftRanks_{ftRanks}, ftPositions_{ftPositions} {
		assertrx_throw(ftSelectIter.size() == 1);
		assertrx_throw(ftSelectIter[0].tempIds_);
		ftIds_ = *ftSelectIter[0].tempIds_;
	}

	template <MergeType mergeType, bool desc, VectorMetric metric>
	MergerRankedImpl<mergeType, desc, metric> Get(Merged<desc>& result);

private:
	IdSetRef ftIds_;
	std::span<const RankT> ftRanks_;
	std::span<const size_t> ftPositions_;
};

template <SelectIteratorContainer::MergeType mergeType, bool desc, VectorMetric metric>
class [[nodiscard]] SelectIteratorContainer::MergerRankedImpl {
public:
	MergerRankedImpl(Merged<desc>& result, IdSetRef ftIds, std::span<const RankT> ftRanks, std::span<const size_t> ftPositions) noexcept
		: result_{result}, ftIds_{ftIds}, ftRanks_{ftRanks}, ftPositions_{ftPositions} {
		assertrx_throw(ftIds_.size() == ftRanks_.size());
	}

	void operator()(const RerankerLinear& reranker, EmptyKnnRawResult&) const {
		if constexpr (mergeType == MergeType::Union) {
			std::vector<bool> ftAdded(ftIds_.size(), false);
			justFt(reranker, ftAdded);
		}
	}

	void operator()(const RerankerLinear& reranker, const IvfKnnRawResult& knnRes) const {
		const auto& knnIds = knnRes.Ids();
		const auto& knnDists = knnRes.Dists();
		assertrx_throw(knnIds.size() == knnDists.size());
		std::vector<bool> ftAdded;
		if constexpr (mergeType == MergeType::Union) {
			ftAdded.resize(ftIds_.size(), false);
		}
		for (size_t i = 0, s = knnIds.size(); i < s && knnIds[i] >= 0; ++i) {
			const IdType id = knnIds[i];
			const auto ftIt = std::lower_bound(ftIds_.begin(), ftIds_.end(), id);
			if (ftIt != ftIds_.end() && *ftIt == id) {
				const size_t ftN = ftIt - ftIds_.begin();
				result_.emplace(id, reranker.Calculate(knnDists[i], ftRanks_[ftN]));
				if constexpr (mergeType == MergeType::Union) {
					ftAdded[ftN] = true;
				}
			} else if constexpr (mergeType == MergeType::Union) {
				result_.emplace(id, reranker.CalculateJustKnn(knnDists[i]));
			}
		}
		if constexpr (mergeType == MergeType::Union) {
			justFt(reranker, ftAdded);
		}
	}

	void operator()(const RerankerLinear& reranker, HnswKnnRawResult& knnRes) const {
		std::vector<bool> ftAdded;
		if constexpr (mergeType == MergeType::Union) {
			ftAdded.resize(ftIds_.size(), false);
		}
		for (; !knnRes.empty(); knnRes.pop()) {
			const auto [knnDist, id] = knnRes.top();
			const auto ftIt = std::lower_bound(ftIds_.begin(), ftIds_.end(), id);
			if (ftIt != ftIds_.end() && *ftIt == IdType(id)) {
				const size_t ftN = ftIt - ftIds_.begin();
				if constexpr (metric == VectorMetric::L2) {
					result_.emplace(IdType(id), reranker.Calculate(knnDist, ftRanks_[ftN]));
				} else {
					result_.emplace(IdType(id), reranker.Calculate(-knnDist, ftRanks_[ftN]));
				}
				if constexpr (mergeType == MergeType::Union) {
					ftAdded[ftN] = true;
				}
			} else if constexpr (mergeType == MergeType::Union) {
				if constexpr (metric == VectorMetric::L2) {
					result_.emplace(IdType(id), reranker.CalculateJustKnn(knnDist));
				} else {
					result_.emplace(IdType(id), reranker.CalculateJustKnn(-knnDist));
				}
			}
		}
		if constexpr (mergeType == MergeType::Union) {
			justFt(reranker, ftAdded);
		}
	}

	void operator()(const RerankerRRF& reranker, const EmptyKnnRawResult&) const {
		if constexpr (mergeType == MergeType::Union) {
			std::vector<bool> ftAdded(ftIds_.size(), false);
			justFt(reranker, ftAdded);
		}
	}

	void operator()(const RerankerRRF& reranker, const IvfKnnRawResult& knnRes) const {
		assertrx_throw(ftIds_.size() == ftPositions_.size());
		const auto& knnIds = knnRes.Ids();
		const auto& knnDists = knnRes.Dists();
		assertrx_throw(knnIds.size() == knnDists.size());
		std::vector<bool> ftAdded;
		if constexpr (mergeType == MergeType::Union) {
			ftAdded.resize(ftIds_.size(), false);
		}
		if (!knnIds.empty()) {
			auto lastKnnDist = knnDists.front();
			const auto ftBegin = ftIds_.begin();
			for (size_t i = 0, s = knnIds.size(), knnPos = 1; i < s && knnIds[i] >= 0; ++i) {
				if constexpr (metric == VectorMetric::L2) {
					if (lastKnnDist < knnDists[i]) {
						lastKnnDist = knnDists[i];
						knnPos = i + 1;
					}
				} else {
					if (lastKnnDist > knnDists[i]) {
						lastKnnDist = knnDists[i];
						knnPos = i + 1;
					}
				}
				const IdType id = knnIds[i];
				const auto ftIt = std::lower_bound(ftIds_.begin(), ftIds_.end(), id);
				if (ftIt != ftIds_.end() && *ftIt == id) {
					const size_t ftN = ftIt - ftBegin;
					result_.emplace(id, reranker.Calculate(knnPos, ftPositions_[ftN]));
					if constexpr (mergeType == MergeType::Union) {
						ftAdded[ftN] = true;
					}
				} else if constexpr (mergeType == MergeType::Union) {
					result_.emplace(id, reranker.CalculateSingle(knnPos));
				}
			}
		}
		if constexpr (mergeType == MergeType::Union) {
			justFt(reranker, ftAdded);
		}
	}

	void operator()(const RerankerRRF& reranker, HnswKnnRawResult& knnRes) const {
		assertrx_throw(ftIds_.size() == ftPositions_.size());
		const auto ftBegin = ftIds_.begin();
		std::vector<bool> ftAdded;
		if constexpr (mergeType == MergeType::Union) {
			ftAdded.resize(ftIds_.size(), false);
		}
		if (!knnRes.empty()) {
			std::vector<IdRank<desc>> knnResReversed;
			knnResReversed.resize(knnRes.size());
			for (size_t i = knnRes.size(); i > 0; --i, knnRes.pop()) {
				const auto [knnDist, id] = knnRes.top();
				knnResReversed[i - 1] = {int(id), RankT(knnDist)};
			}
			auto lastKnnDist = knnResReversed.front().rank;
			for (size_t knnPos = 1, i = 0, s = knnResReversed.size(); i < s; ++i) {
				const auto [id, knnDist] = knnResReversed[i];
				if (lastKnnDist < knnDist) {
					lastKnnDist = knnDist;
					knnPos = i + 1;
				}
				const auto ftIt = std::lower_bound(ftBegin, ftIds_.end(), id);
				if (ftIt != ftIds_.end() && *ftIt == IdType(id)) {
					const size_t ftN = ftIt - ftBegin;
					result_.emplace(id, reranker.Calculate(knnPos, ftPositions_[ftN]));
					if constexpr (mergeType == MergeType::Union) {
						ftAdded[ftN] = true;
					}
				} else if constexpr (mergeType == MergeType::Union) {
					result_.emplace(id, reranker.CalculateSingle(knnPos));
				}
			}
		}
		if constexpr (mergeType == MergeType::Union) {
			justFt(reranker, ftAdded);
		}
	}

private:
	void justFt(const RerankerLinear& reranker, const std::vector<bool>& ftAdded) const {
		assertrx_throw(ftAdded.size() == ftIds_.size());
		for (size_t i = 0, s = ftIds_.size(); i < s; ++i) {
			if (!ftAdded[i]) {
				result_.emplace(ftIds_[i], reranker.CalculateJustFt(ftRanks_[i]));
			}
		}
	}

	void justFt(const RerankerRRF& reranker, const std::vector<bool>& ftAdded) const {
		assertrx_throw(ftAdded.size() == ftIds_.size());
		for (size_t i = 0, s = ftIds_.size(); i < s; ++i) {
			if (!ftAdded[i]) {
				result_.emplace(ftIds_[i], reranker.CalculateSingle(ftPositions_[i]));
			}
		}
	}
	Merged<desc>& result_;
	IdSetRef ftIds_;
	std::span<const RankT> ftRanks_;
	std::span<const size_t> ftPositions_;
};

template <SelectIteratorContainer::MergeType mergeType, bool desc, VectorMetric metric>
SelectIteratorContainer::MergerRankedImpl<mergeType, desc, metric> SelectIteratorContainer::MergerRanked::Get(Merged<desc>& result) {
	return MergerRankedImpl<mergeType, desc, metric>{result, ftIds_, ftRanks_, ftPositions_};
}

template <bool desc>
void SelectIteratorContainer::mergeRanked(RanksHolder::Ptr& ranks, const Reranker& reranker, const NamespaceImpl& ns) {
	using namespace std::string_view_literals;
	assertrx_throw(ranks);
	const auto [itFirst, last] = findFirstRanked(begin(), end(), ns);
	assertrx_throw(itFirst != last);
	auto itSecond = itFirst;
	for (++itSecond; itSecond != last && !isRanked(itSecond, ns); ++itSecond) {
	}
	assertrx_throw(itSecond != last);
	assertrx_throw(itFirst->Is<KnnRawSelectResult>() || itSecond->Is<KnnRawSelectResult>());
	assertrx_throw(itFirst->Is<SelectIterator>() || itSecond->Is<SelectIterator>());
	auto& knnRes = itFirst->Is<KnnRawSelectResult>() ? itFirst->Value<KnnRawSelectResult>() : itSecond->Value<KnnRawSelectResult>();
	const auto& ftSI = itFirst->Is<SelectIterator>() ? itFirst->Value<SelectIterator>() : itSecond->Value<SelectIterator>();
	assertrx_throw(ns.indexes_[ftSI.IndexNo()]->IsFulltext());

	MergeType mergeType = MergeType::Intersection;
	assertrx_throw(itFirst->operation == OpAnd);
	if (auto afterFirstIt = itFirst; ++afterFirstIt == itSecond) {
		assertrx_throw(itSecond->operation != OpNot);
		mergeType = (itSecond->operation == OpAnd) ? MergeType::Intersection : MergeType::Union;
	} else {
		assertrx_throw(afterFirstIt->operation != OpOr);
		assertrx_throw(itSecond->operation == OpAnd);
	}
#ifndef NDEBUG
	auto afterSecondIt = itSecond;
	assertrx_throw(++afterSecondIt == last || afterSecondIt->operation != OpOr);
#endif	// NDEBUG

	const size_t bufSize = mergeType == MergeType::Intersection ? std::min(ranks->GetRanksSpan().size(), knnRes.RawResult().Size())
																: ranks->GetRanksSpan().size() + knnRes.RawResult().Size();
#ifdef USE_PMR
	std::vector<std::byte> buffer;
	buffer.resize((sizeof(typename Merged<desc>::node_type) + sizeof(typename Merged<desc>::value_type)) * bufSize);
	std::pmr::monotonic_buffer_resource pool(buffer.data(), buffer.size());
	Merged<desc> merged{&pool};
#else	// USE_PMR
	Merged<desc> merged(bufSize);
#endif	// USE_PMR
	MergerRanked merger(ftSI, ranks->GetRanksSpan(), ranks->GetPositionsSpan());
	switch (mergeType) {
		case MergeType::Intersection:
			switch (knnRes.RawResult().Metric()) {
				case VectorMetric::L2:
					std::visit(merger.Get<MergeType::Intersection, desc, VectorMetric::L2>(merged), reranker.AsVariant(),
							   knnRes.RawResult().AsVariant());
					break;
				case VectorMetric::InnerProduct:
					std::visit(merger.Get<MergeType::Intersection, desc, VectorMetric::InnerProduct>(merged), reranker.AsVariant(),
							   knnRes.RawResult().AsVariant());
					break;
				case VectorMetric::Cosine:
					std::visit(merger.Get<MergeType::Intersection, desc, VectorMetric::Cosine>(merged), reranker.AsVariant(),
							   knnRes.RawResult().AsVariant());
					break;
				default:
					throw_as_assert;
			}
			break;
		case MergeType::Union:
			switch (knnRes.RawResult().Metric()) {
				case VectorMetric::L2:
					std::visit(merger.Get<MergeType::Union, desc, VectorMetric::L2>(merged), reranker.AsVariant(),
							   knnRes.RawResult().AsVariant());
					break;
				case VectorMetric::InnerProduct:
					std::visit(merger.Get<MergeType::Union, desc, VectorMetric::InnerProduct>(merged), reranker.AsVariant(),
							   knnRes.RawResult().AsVariant());
					break;
				case VectorMetric::Cosine:
					std::visit(merger.Get<MergeType::Union, desc, VectorMetric::Cosine>(merged), reranker.AsVariant(),
							   knnRes.RawResult().AsVariant());
					break;
				default:
					throw_as_assert;
			}
			break;
		default:
			throw_as_assert;
	}
	h_vector<RankT, 128> mergedRanks;
	base_idset mergedIdset;
	mergedRanks.reserve(merged.size());
	mergedIdset.reserve(merged.size());
#ifdef USE_PMR
	for (const auto [id, rank] : merged) {
#else	// USE_PMR
	std::vector<IdRank<desc>> mergedSorted;
	mergedSorted.reserve(merged.size());
	mergedSorted.assign(merged.begin(), merged.end());
	boost::sort::pdqsort_branchless(mergedSorted.begin(), mergedSorted.end());
	for (const auto [id, rank] : mergedSorted) {
#endif	// USE_PMR
		mergedRanks.push_back(rank);
		mergedIdset.push_back(id);
	}
	IdSet::Ptr resSet = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
	resSet->SetUnordered(IdSetPlain{std::move(mergedIdset)});
	ranks = make_intrusive<RanksHolder>();
	ranks->Add(std::move(mergedRanks));
	SelectKeyResult selectResult;
	selectResult.emplace_back(std::move(resSet));
	itFirst->Emplace<SelectIterator>(std::move(selectResult), IsDistinct_False, "-reranked"sv, IndexValueType::NotSet, ForcedFirst_True);
	itFirst->Value<SelectIterator>().SetUnsorted();
	Erase(itSecond);
}

void SelectIteratorContainer::MergeRanked(RanksHolder::Ptr& ranks, const Reranker& reranker, const NamespaceImpl& ns) {
	if (reranker.Desc()) {
		mergeRanked<true>(ranks, reranker, ns);
	} else {
		mergeRanked<false>(ranks, reranker, ns);
	}
}

std::string SelectIteratorContainer::Dump() const {
	WrSerializer ser;
	assertrx_throw(ctx_);
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
		it->Visit([&ser](const KnnRawSelectResult&) { ser << "KNN RAW TODO"; },
				  [&](const SelectIteratorsBracket&) {
					  ser << "(\n";
					  dump(level + 1, it.cbegin(), it.cend(), joinedSelectors, ser);
					  for (size_t i = 0; i < level; ++i) {
						  ser << "   ";
					  }
					  ser << ')';
				  },
				  [&ser](const concepts::OneOf<SelectIterator, ComparatorsPackT> auto& c) { ser << c.Dump(); },
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
