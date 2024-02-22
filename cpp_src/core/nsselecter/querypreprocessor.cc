#include "querypreprocessor.h"

#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/payload/fieldsset.h"
#include "core/query/dsl/dslencoder.h"
#include "core/query/queryentry.h"
#include "core/sorting/sortexpression.h"
#include "estl/overloaded.h"
#include "nsselecter.h"
#include "qresexplainholder.h"
#include "substitutionhelpers.h"

namespace reindexer {

QueryPreprocessor::QueryPreprocessor(QueryEntries &&queries, NamespaceImpl *ns, const SelectCtx &ctx)
	: QueryEntries(std::move(queries)),
	  ns_(*ns),
	  query_{ctx.query},
	  strictMode_(ctx.inTransaction ? StrictModeNone
									: ((query_.GetStrictMode() == StrictModeNotSet) ? ns_.config_.strictMode : query_.GetStrictMode())),
	  start_(query_.Offset()),
	  count_(query_.Limit()),
	  forcedSortOrder_(!query_.forcedSortOrder_.empty()),
	  reqMatchedOnce_(ctx.reqMatchedOnceFlag),
	  isMergeQuery_(ctx.isMergeQuery == IsMergeQuery::Yes) {
	if (forcedSortOrder_ && (start_ > QueryEntry::kDefaultOffset || count_ < QueryEntry::kDefaultLimit)) {
		assertrx_throw(!query_.sortingEntries_.empty());
		static const std::vector<JoinedSelector> emptyJoinedSelectors;
		const auto &sEntry = query_.sortingEntries_[0];
		if (SortExpression::Parse(sEntry.expression, emptyJoinedSelectors).ByField()) {
			VariantArray values;
			values.reserve(query_.forcedSortOrder_.size());
			for (const auto &v : query_.forcedSortOrder_) values.push_back(v);
			desc_ = sEntry.desc;
			QueryField fld{sEntry.expression};
			SetQueryField(fld, ns_);
			Append<QueryEntry>(desc_ ? OpNot : OpAnd, std::move(fld), query_.forcedSortOrder_.size() == 1 ? CondEq : CondSet,
							   std::move(values));
			queryEntryAddedByForcedSortOptimization_ = true;
		}
	}
	if (isMergeQuery_) {
		if (QueryEntry::kDefaultLimit - start_ > count_) {
			count_ += start_;
		} else {
			count_ = QueryEntry::kDefaultLimit;
		}
		start_ = QueryEntry::kDefaultOffset;
	}
}

void QueryPreprocessor::ExcludeFtQuery(const RdxContext &rdxCtx) {
	if (queryEntryAddedByForcedSortOptimization_ || Size() <= 1) return;
	for (auto it = begin(), next = it, endIt = end(); it != endIt; it = next) {
		++next;
		if (it->Is<QueryEntry>() && it->Value<QueryEntry>().IsFieldIndexed()) {
			auto &index = ns_.indexes_[it->Value<QueryEntry>().IndexNo()];
			if (!IsFastFullText(index->Type())) continue;
			if (it->operation != OpAnd || (next != endIt && next->operation == OpOr) || !index->EnablePreselectBeforeFt()) break;
			ftPreselect_ = index->FtPreselect(rdxCtx);
			start_ = QueryEntry::kDefaultOffset;
			count_ = QueryEntry::kDefaultLimit;
			forcedSortOrder_ = false;
			ftEntry_ = std::move(it->Value<QueryEntry>());
			const size_t pos = it.PlainIterator() - cbegin().PlainIterator();
			Erase(pos, pos + 1);
			break;
		}
	}
}

bool QueryPreprocessor::NeedNextEvaluation(unsigned start, unsigned count, bool &matchedAtLeastOnce,
										   QresExplainHolder &qresHolder) noexcept {
	if (evaluationsCount_++) return false;
	if (queryEntryAddedByForcedSortOptimization_) {
		container_.back().operation = desc_ ? OpAnd : OpNot;
		assertrx_throw(start <= start_);
		start_ = start;
		assertrx_throw(count <= count_);
		count_ = count;
		return count_ || (reqMatchedOnce_ && !matchedAtLeastOnce);
	} else if (ftEntry_) {
		if (!matchedAtLeastOnce) return false;
		qresHolder.BackupContainer();
		if (isMergeQuery_) {
			if (QueryEntry::kDefaultLimit - query_.Offset() > query_.Limit()) {
				count_ = query_.Limit() + query_.Offset();
			} else {
				count_ = QueryEntry::kDefaultLimit;
			}
			start_ = QueryEntry::kDefaultOffset;
		} else {
			start_ = query_.Offset();
			count_ = query_.Limit();
		}
		forcedSortOrder_ = !query_.forcedSortOrder_.empty();
		clear();
		Append(OpAnd, std::move(*ftEntry_));
		ftEntry_ = std::nullopt;
		matchedAtLeastOnce = false;
		equalPositions.clear();
		return true;
	}
	return false;
}

void QueryPreprocessor::checkStrictMode(const QueryField &field) const {
	if (field.IsFieldIndexed()) return;
	switch (strictMode_) {
		case StrictModeIndexes:
			throw Error(errStrictMode,
						"Current query strict mode allows filtering by indexes only. There are no indexes with name '%s' in namespace '%s'",
						field.FieldName(), ns_.name_);
		case StrictModeNames:
			if (field.HaveEmptyField()) {
				throw Error(errStrictMode,
							"Current query strict mode allows filtering by existing fields only. There are no fields with name '%s' in "
							"namespace '%s'",
							field.FieldName(), ns_.name_);
			}
		case StrictModeNotSet:
		case StrictModeNone:
			return;
	}
}

class JoinOnExplainEnabled;
class JoinOnExplainDisabled;

void QueryPreprocessor::InjectConditionsFromJoins(JoinedSelectors &js, OnConditionInjections &expalainOnInjections, LogLevel logLevel,
												  const RdxContext &rdxCtx) {
	const bool needExplain = query_.GetExplain() || logLevel >= LogInfo;
	if (needExplain) {
		injectConditionsFromJoins<JoinOnExplainEnabled>(0, container_.size(), js, expalainOnInjections, rdxCtx);
	} else {
		injectConditionsFromJoins<JoinOnExplainDisabled>(0, container_.size(), js, expalainOnInjections, rdxCtx);
	}
}

void QueryPreprocessor::Reduce(bool isFt) {
	bool changed;
	do {
		changed = removeBrackets();
		changed = LookupQueryIndexes() || changed;
		if (!isFt) changed = SubstituteCompositeIndexes() || changed;
	} while (changed);
}

bool QueryPreprocessor::removeBrackets() {
	const size_t startSize = Size();
	removeBrackets(0, startSize);
	return startSize != Size();
}

bool QueryPreprocessor::canRemoveBracket(size_t i) const {
	if (Size(i) < 2) {
		throw Error{errQueryExec, "Bracket cannot be empty"};
	}
	const size_t next = Next(i);
	const OpType op = GetOperation(i);
	if (op != OpAnd && GetOperation(i + 1) != OpAnd) return false;
	if (next == Next(i + 1)) return true;
	return op == OpAnd && (next == Size() || GetOperation(next) != OpOr);
}

size_t QueryPreprocessor::removeBrackets(size_t begin, size_t end) {
	if (begin != end && GetOperation(begin) == OpOr) {
		throw Error{errQueryExec, "OR operator in first condition or after left join"};
	}
	size_t deleted = 0;
	for (size_t i = begin; i < end - deleted; i = Next(i)) {
		if (!IsSubTree(i)) continue;
		deleted += removeBrackets(i + 1, Next(i));
		if (canRemoveBracket(i)) {
			if (const OpType op = GetOperation(i); op != OpAnd) {
				SetOperation(op, i + 1);
			}
			Erase(i, i + 1);
			++deleted;
		}
	}
	return deleted;
}

void QueryPreprocessor::InitIndexNumbers() {
	ExecuteAppropriateForEach(
		Skip<QueryEntriesBracket, JoinQueryEntry, AlwaysFalse, AlwaysTrue>{}, [](const SubQueryEntry &) { assertrx(0); },
		[](const SubQueryFieldEntry &) { assertrx(0); },
		[this](QueryEntry &entry) {
			if (!entry.FieldsHaveBeenSet()) {
				SetQueryField(entry.FieldData(), ns_);
			}
			checkStrictMode(entry.FieldData());
		},
		[this](BetweenFieldsQueryEntry &entry) {
			if (!entry.FieldsHaveBeenSet()) {
				SetQueryField(entry.LeftFieldData(), ns_);
				SetQueryField(entry.RightFieldData(), ns_);
			}
			checkStrictMode(entry.LeftFieldData());
			checkStrictMode(entry.RightFieldData());
		});
}

size_t QueryPreprocessor::lookupQueryIndexes(uint16_t dst, uint16_t srcBegin, uint16_t srcEnd) {
	assertrx_throw(dst <= srcBegin);
	h_vector<uint16_t, kMaxIndexes> iidx(kMaxIndexes, uint16_t(0));
	size_t merged = 0;
	for (size_t src = srcBegin, nextSrc; src < srcEnd; src = nextSrc) {
		nextSrc = Next(src);
		const bool changeDst = container_[src].InvokeAppropriate<bool>(
			[](const SubQueryEntry &) -> bool {
				assertrx_throw(0);
				abort();
			},
			[](const SubQueryFieldEntry &) -> bool {
				assertrx_throw(0);
				abort();
			},
			[&](const QueryEntriesBracket &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
				const size_t mergedInBracket = lookupQueryIndexes(dst + 1, src + 1, nextSrc);
				container_[dst].Value<QueryEntriesBracket>().Erase(mergedInBracket);
				merged += mergedInBracket;
				return true;
			},
			[&](QueryEntry &entry) {
				if (entry.IsFieldIndexed()) {
					// try merge entries with AND opetator
					if ((GetOperation(src) == OpAnd) && (nextSrc >= srcEnd || GetOperation(nextSrc) != OpOr)) {
						if (size_t(entry.IndexNo()) >= iidx.size()) {
							const auto oldSize = iidx.size();
							iidx.resize(entry.IndexNo() + 1);
							std::fill(iidx.begin() + oldSize, iidx.begin() + iidx.size(), 0);
						}
						auto &iidxRef = iidx[entry.IndexNo()];
						if (iidxRef > 0 && !ns_.indexes_[entry.IndexNo()]->Opts().IsArray()) {
							if (mergeQueryEntries(iidxRef - 1, src)) {
								++merged;
								return false;
							}
						} else {
							assertrx_throw(dst < std::numeric_limits<uint16_t>::max() - 1);
							iidxRef = dst + 1;
						}
					}
				}
				if (dst != src) container_[dst] = std::move(container_[src]);
				return true;
			},
			[dst, src, this](JoinQueryEntry &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
				return true;
			},
			[dst, src, this](BetweenFieldsQueryEntry &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
				return true;
			},
			[dst, src, this](AlwaysFalse &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
				return true;
			},
			[dst, src, this](AlwaysTrue &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
				return true;
			});
		if (changeDst) dst = Next(dst);
	}
	return merged;
}

void QueryPreprocessor::CheckUniqueFtQuery() const {
	bool found = false;
	ExecuteAppropriateForEach(
		Skip<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue>{},
		[](const SubQueryEntry &) { assertrx_throw(0); }, [](const SubQueryFieldEntry &) { assertrx_throw(0); },
		[&](const QueryEntry &qe) {
			if (qe.IsFieldIndexed() && IsFullText(ns_.indexes_[qe.IndexNo()]->Type())) {
				if (found) {
					throw Error{errQueryExec, "Query cannot contain more than one full text condition"};
				} else {
					found = true;
				}
			}
		});
}

bool QueryPreprocessor::ContainsFullTextIndexes() const {
	for (auto it = cbegin().PlainIterator(), end = cend().PlainIterator(); it != end; ++it) {
		if (it->Is<QueryEntry>() && it->Value<QueryEntry>().IsFieldIndexed() &&
			IsFullText(ns_.indexes_[it->Value<QueryEntry>().IndexNo()]->Type())) {
			return true;
		}
	}
	return false;
}

[[nodiscard]] SortingEntries QueryPreprocessor::GetSortingEntries(const SelectCtx &ctx) const {
	if (ftEntry_) return {};
	// DO NOT use deducted sort order in the following cases:
	// - query contains explicity specified sort order
	// - query contains FullText query.
	const bool disableOptimizedSortOrder = !query_.sortingEntries_.empty() || ContainsFullTextIndexes() || ctx.preResult;
	// Queries with ordered indexes may have different selection plan depending on filters' values.
	// This may lead to items reordering when SingleRange becomes main selection method.
	// By default all the results are ordereb by internal IDs, but with SingleRange results will be ordered by values first.
	// So we're trying to order results by values in any case, even if there are no SingleRange in selection plan.
	return disableOptimizedSortOrder ? query_.sortingEntries_ : detectOptimalSortOrder();
}

const std::vector<int> *QueryPreprocessor::getCompositeIndex(int field) const {
	auto f = ns_.indexesToComposites_.find(field);
	if (f != ns_.indexesToComposites_.end()) {
		return &f->second;
	}
	return nullptr;
}

static void createCompositeKeyValues(const h_vector<std::pair<int, VariantArray>, 4> &values, const PayloadType &plType, Payload &pl,
									 VariantArray &ret, unsigned n) {
	const auto &v = values[n];
	for (auto it = v.second.cbegin(), end = v.second.cend(); it != end; ++it) {
		pl.Set(v.first, *it);
		if (n + 1 < values.size()) {
			createCompositeKeyValues(values, plType, pl, ret, n + 1);
		} else {
			PayloadValue pv(*(pl.Value()));
			pv.Clone();
			ret.emplace_back(std::move(pv));
		}
	}
}

static VariantArray createCompositeKeyValues(const h_vector<std::pair<int, VariantArray>, 4> &values, const PayloadType &plType,
											 uint32_t resultSetSize) {
	PayloadValue d(plType.TotalSize());
	Payload pl(plType, d);
	VariantArray ret;
	ret.reserve(resultSetSize);
	createCompositeKeyValues(values, plType, pl, ret, 0);
	return ret;
}

size_t QueryPreprocessor::substituteCompositeIndexes(const size_t from, const size_t to) {
	using composite_substitution_helpers::CompositeSearcher;
	using composite_substitution_helpers::EntriesRanges;
	using composite_substitution_helpers::CompositeValuesCountLimits;

	size_t deleted = 0;
	CompositeSearcher searcher(ns_);
	for (size_t cur = from, end = to; cur < end; cur = Next(cur), end = to - deleted) {
		if (IsSubTree(cur)) {
			const auto &bracket = Get<QueryEntriesBracket>(cur);
			auto bracketSize = bracket.Size();
			deleted += substituteCompositeIndexes(cur + 1, cur + bracketSize);
			continue;
		}
		if (!Is<QueryEntry>(cur) || GetOperation(cur) != OpAnd) {
			continue;
		}
		const auto next = Next(cur);
		if ((next < end && GetOperation(next) == OpOr)) {
			continue;
		}
		auto &qe = Get<QueryEntry>(cur);
		if ((qe.Condition() != CondEq && qe.Condition() != CondSet) || !qe.IsFieldIndexed() ||
			qe.IndexNo() >= ns_.payloadType_.NumFields()) {
			continue;
		}

		const std::vector<int> *found = getCompositeIndex(qe.IndexNo());
		if (!found || found->empty()) {
			continue;
		}
		searcher.Add(qe.IndexNo(), *found, cur);
	}

	EntriesRanges deleteRanges;
	h_vector<std::pair<int, VariantArray>, 4> values;
	auto resIdx = searcher.GetResult();
	while (resIdx >= 0) {
		auto &res = searcher[resIdx];
		values.clear<false>();
		uint32_t resultSetSize = 0;
		uint32_t maxSetSize = 0;
		for (auto i : res.entries) {
			auto &qe = Get<QueryEntry>(i);
			if rx_unlikely (!res.fields.contains(qe.IndexNo())) {
				throw Error(errLogic, "Error during composite index's fields substitution (this should not happen)");
			}
			maxSetSize = std::max(maxSetSize, qe.Values().size());
			resultSetSize = (resultSetSize == 0) ? qe.Values().size() : (resultSetSize * qe.Values().size());
		}
		constexpr static CompositeValuesCountLimits kCompositeSetLimits;
		if (resultSetSize != maxSetSize) {
			// Do not perform substitution if result set size becoms larger than initial indexes set size
			// and this size is greater than limit
			// TODO: This is potential customization point for the user's hints system
			if (resultSetSize > kCompositeSetLimits[res.entries.size()]) {
				resIdx = searcher.RemoveUnusedAndGetNext(resIdx);
				continue;
			}
		}
		for (auto i : res.entries) {
			auto &qe = Get<QueryEntry>(i);
			qe.ConvertValuesToFieldType();
			const int idxNo = qe.IndexNo();
			values.emplace_back(idxNo, std::move(qe).Values());
		}
		{
			VariantArray qValues = createCompositeKeyValues(values, ns_.payloadType_, resultSetSize);
			const auto first = res.entries.front();
			SetOperation(OpAnd, first);
			QueryField fld{ns_.indexes_[res.idx]->Name()};
			setQueryIndex(fld, res.idx, ns_);
			container_[first].Emplace<QueryEntry>(std::move(fld), qValues.size() == 1 ? CondEq : CondSet, std::move(qValues));
		}
		deleteRanges.Add(span(res.entries.data() + 1, res.entries.size() - 1));
		resIdx = searcher.RemoveUsedAndGetNext(resIdx);
	}
	for (auto rit = deleteRanges.rbegin(); rit != deleteRanges.rend(); ++rit) {
		Erase(rit->From(), rit->To());
		deleted += rit->Size();
	}
	return deleted;
}

void QueryPreprocessor::convertWhereValues(QueryEntry &qe) const { qe.ConvertValuesToFieldType(ns_.payloadType_); }

void QueryPreprocessor::convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const {
	for (auto it = begin; it != end; ++it) {
		it->InvokeAppropriate<void>(
			Skip<JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue>{}, [](const SubQueryEntry &) { assertrx_throw(0); },
			[](const SubQueryFieldEntry &) { assertrx_throw(0); },
			[this, &it](const QueryEntriesBracket &) { convertWhereValues(it.begin(), it.end()); },
			[this](QueryEntry &qe) { convertWhereValues(qe); });
	}
}

[[nodiscard]] SortingEntries QueryPreprocessor::detectOptimalSortOrder() const {
	if (!AvailableSelectBySortIndex()) return {};
	if (const Index *maxIdx = findMaxIndex(cbegin(), cend())) {
		SortingEntries sortingEntries;
		sortingEntries.emplace_back(maxIdx->Name(), false);
		return sortingEntries;
	}
	return SortingEntries();
}

[[nodiscard]] const Index *QueryPreprocessor::findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const {
	thread_local h_vector<FoundIndexInfo, 32> foundIndexes;
	foundIndexes.clear<false>();
	findMaxIndex(begin, end, foundIndexes);
	boost::sort::pdqsort(foundIndexes.begin(), foundIndexes.end(), [](const FoundIndexInfo &l, const FoundIndexInfo &r) noexcept {
		if (l.isFitForSortOptimization > r.isFitForSortOptimization) {
			return true;
		}
		if (l.isFitForSortOptimization == r.isFitForSortOptimization) {
			return l.size > r.size;
		}
		return false;
	});
	if (foundIndexes.size() && foundIndexes[0].isFitForSortOptimization) {
		return foundIndexes[0].index;
	}
	return nullptr;
}

void QueryPreprocessor::findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end,
									 h_vector<FoundIndexInfo, 32> &foundIndexes) const {
	for (auto it = begin; it != end; ++it) {
		const FoundIndexInfo foundIdx = it->InvokeAppropriate<FoundIndexInfo>(
			[](const SubQueryEntry &) -> FoundIndexInfo {
				assertrx_throw(0);
				abort();
			},
			[](const SubQueryFieldEntry &) -> FoundIndexInfo {
				assertrx_throw(0);
				abort();
			},
			[this, &it, &foundIndexes](const QueryEntriesBracket &) {
				findMaxIndex(it.cbegin(), it.cend(), foundIndexes);
				return FoundIndexInfo();
			},
			[this](const QueryEntry &entry) -> FoundIndexInfo {
				if (entry.IsFieldIndexed() && !entry.Distinct()) {
					const auto idxPtr = ns_.indexes_[entry.IndexNo()].get();
					if (idxPtr->IsOrdered() && !idxPtr->Opts().IsArray()) {
						if (IsOrderedCondition(entry.Condition())) {
							return FoundIndexInfo{idxPtr, FoundIndexInfo::ConditionType::Compatible};
						} else if (entry.Condition() == CondAny || entry.Values().size() > 1) {
							return FoundIndexInfo{idxPtr, FoundIndexInfo::ConditionType::Incompatible};
						}
					}
				}
				return FoundIndexInfo();
			},
			[](const JoinQueryEntry &) noexcept { return FoundIndexInfo(); },
			[](const BetweenFieldsQueryEntry &) noexcept { return FoundIndexInfo(); },
			[](const AlwaysFalse &) noexcept { return FoundIndexInfo(); }, [](const AlwaysTrue &) noexcept { return FoundIndexInfo(); });
		if (foundIdx.index) {
			auto found = std::find_if(foundIndexes.begin(), foundIndexes.end(),
									  [foundIdx](const FoundIndexInfo &i) { return i.index == foundIdx.index; });
			if (found == foundIndexes.end()) {
				foundIndexes.emplace_back(foundIdx);
			} else {
				found->isFitForSortOptimization &= foundIdx.isFitForSortOptimization;
			}
		}
	}
}

bool QueryPreprocessor::mergeQueryEntries(size_t lhs, size_t rhs) {
	QueryEntry *lqe = &Get<QueryEntry>(lhs);
	QueryEntry &rqe = Get<QueryEntry>(rhs);
	if ((lqe->Condition() == CondEq || lqe->Condition() == CondSet) && (rqe.Condition() == CondEq || rqe.Condition() == CondSet)) {
		// intersect 2 queryentries on the same index
		if rx_unlikely (lqe->Values().empty()) {
			return true;
		}
		const bool distinct = lqe->Distinct() || rqe.Distinct();
		VariantArray setValues;
		if (rx_likely(!rqe.Values().empty())) {
			convertWhereValues(*lqe);
			convertWhereValues(rqe);
			auto &&[first, second] = lqe->Values().size() < rqe.Values().size()
										 ? std::make_pair(std::move(*lqe).Values(), std::move(rqe).Values())
										 : std::make_pair(std::move(rqe).Values(), std::move(*lqe).Values());

			setValues.reserve(first.size());
			constexpr size_t kMinArraySizeToUseHashSet = 250;
			if (second.size() < kMinArraySizeToUseHashSet) {
				// Intersect via binary search + sort for small vectors
				boost::sort::pdqsort(first.begin(), first.end());
				for (auto &&v : second) {
					if (std::binary_search(first.begin(), first.end(), v)) {
						setValues.emplace_back(std::move(v));
					}
				}
			} else {
				// Intersect via hash_set for large vectors
				reindexer::fast_hash_set<reindexer::Variant> set;
				set.reserve(first.size() * 2);
				for (auto &&v : first) {
					set.emplace(std::move(v));
				}
				for (auto &&v : second) {
					if (set.erase(v)) {
						setValues.emplace_back(std::move(v));
					}
				}
			}
		}

		lqe->SetCondAndValues(CondSet, std::move(setValues));
		lqe->Distinct(distinct);
		return true;
	} else if (rqe.Condition() == CondAny) {
		if (!lqe->Distinct() && rqe.Distinct()) {
			lqe->Distinct(true);
		}
		return true;
	} else if (lqe->Condition() == CondAny) {
		const bool distinct = lqe->Distinct() || rqe.Distinct();
		container_[lhs].SetValue(std::move(rqe));
		Get<QueryEntry>(lhs).Distinct(distinct);
		return true;
	}

	return false;
}

void QueryPreprocessor::AddDistinctEntries(const h_vector<Aggregator, 4> &aggregators) {
	bool wasAdded = false;
	for (auto &ag : aggregators) {
		if (ag.Type() != AggDistinct) continue;
		assertrx_throw(ag.Names().size() == 1);
		Append<QueryEntry>(wasAdded ? OpOr : OpAnd, ag.Names()[0], QueryEntry::DistinctTag{});
		wasAdded = true;
	}
}

std::pair<CondType, VariantArray> QueryPreprocessor::queryValuesFromOnCondition(std::string &explainStr, AggType &oAggType,
																				NamespaceImpl &rightNs, Query joinQuery,
																				const QueryJoinEntry &joinEntry, CondType condition,
																				const RdxContext &rdxCtx) {
	size_t limit;
	const auto &rNsCfg = rightNs.Config();
	if (rNsCfg.maxPreselectSize == 0) {
		limit = std::max<int64_t>(rNsCfg.minPreselectSize, rightNs.ItemsCount() * rNsCfg.maxPreselectPart);
	} else if (rNsCfg.maxPreselectPart == 0.0) {
		limit = rNsCfg.maxPreselectSize;
	} else {
		limit =
			std::min(std::max<int64_t>(rNsCfg.minPreselectSize, rightNs.ItemsCount() * rNsCfg.maxPreselectPart), rNsCfg.maxPreselectSize);
	}
	joinQuery.Explain(query_.GetExplain());
	joinQuery.Limit(limit + 2);
	joinQuery.Offset(QueryEntry::kDefaultOffset);
	joinQuery.sortingEntries_.clear();
	joinQuery.forcedSortOrder_.clear();
	joinQuery.aggregations_.clear();
	switch (condition) {
		case CondEq:
		case CondSet:
			joinQuery.Distinct(joinEntry.RightFieldName());
			oAggType = AggType::AggDistinct;
			break;
		case CondLt:
		case CondLe:
			joinQuery.Aggregate(AggMax, {joinEntry.RightFieldName()});
			oAggType = AggType::AggMax;
			break;
		case CondGt:
		case CondGe:
			joinQuery.Aggregate(AggMin, {joinEntry.RightFieldName()});
			oAggType = AggType::AggMin;
			break;
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			throw Error(errQueryExec, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
	}

	SelectCtx ctx{joinQuery, nullptr};
	LocalQueryResults qr;
	rightNs.Select(qr, ctx, rdxCtx);
	if (qr.Count() > limit) return {CondAny, {}};
	assertrx_throw(qr.aggregationResults.size() == 1);
	auto &aggRes = qr.aggregationResults[0];
	explainStr = qr.explainResults;
	switch (condition) {
		case CondEq:
		case CondSet: {
			assertrx_throw(aggRes.type == AggDistinct);
			VariantArray values;
			values.reserve(aggRes.distincts.size());
			for (Variant &distValue : aggRes.distincts) {
				if (distValue.Type().Is<KeyValueType::Composite>()) {
					ConstPayload pl(aggRes.payloadType, distValue.operator const PayloadValue &());
					values.emplace_back(pl.GetComposite(aggRes.distinctsFields, joinEntry.RightCompositeFieldsTypes()));
				} else {
					values.emplace_back(std::move(distValue));
				}
			}
			return {CondSet, std::move(values)};
		}
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			if (auto value = aggRes.GetValue()) {
				return {condition, {Variant{*value}}};
			} else {
				return {CondAny, {}};
			}
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		default:
			throw Error(errQueryExec, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
	}
}

std::pair<CondType, VariantArray> QueryPreprocessor::queryValuesFromOnCondition(CondType condition, const QueryJoinEntry &joinEntry,
																				const JoinedSelector &joinedSelector,
																				const CollateOpts &collate) {
	switch (condition) {
		case CondEq:
		case CondSet:
			return {CondSet, joinedSelector.readValuesFromPreResult(joinEntry)};
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe: {
			const JoinPreResult::Values &values = joinedSelector.preResult_->values;
			VariantArray buffer, keyValues;
			for (const ItemRef &item : values) {
				assertrx_throw(!item.Value().IsFree());
				const ConstPayload pl{values.payloadType, item.Value()};
				pl.GetByFieldsSet(joinEntry.RightFields(), buffer, joinEntry.RightFieldType(), joinEntry.RightCompositeFieldsTypes());
				for (Variant &v : buffer) {
					if (keyValues.empty()) {
						keyValues.emplace_back(std::move(v));
					} else {
						const auto cmp = keyValues[0].Compare(v, collate);
						if (condition == CondLt || condition == CondLe) {
							if (cmp < 0) {
								keyValues[0] = std::move(v);
							}
						} else {
							if (cmp > 0) {
								keyValues[0] = std::move(v);
							}
						}
					}
				}
			}
			return {condition, std::move(keyValues)};
		} break;
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		default:
			throw Error(errQueryExec, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
	}
}

template <typename JS>
void QueryPreprocessor::briefDump(size_t from, size_t to, const std::vector<JS> &joinedSelectors, WrSerializer &ser) const {
	{
		for (auto it = from; it < to; it = Next(it)) {
			if (it != from || container_[it].operation != OpAnd) {
				ser << container_[it].operation << ' ';
			}
			container_[it].InvokeAppropriate<void>(
				[](const SubQueryEntry &) { assertrx_throw(0); }, [](const SubQueryFieldEntry &) { assertrx_throw(0); },
				[&](const QueryEntriesBracket &b) {
					ser << "(";
					briefDump(it + 1, Next(it), joinedSelectors, ser);
					dumpEqualPositions(0, ser, b.equalPositions);
					ser << ")";
				},
				[&ser](const QueryEntry &qe) { ser << qe.DumpBrief() << ' '; },
				[&joinedSelectors, &ser](const JoinQueryEntry &jqe) { ser << jqe.Dump(joinedSelectors) << ' '; },
				[&ser](const BetweenFieldsQueryEntry &qe) { ser << qe.Dump() << ' '; },
				[&ser](const AlwaysFalse &) { ser << "AlwaysFalse" << ' '; }, [&ser](const AlwaysTrue &) { ser << "AlwaysTrue" << ' '; });
		}
	}
}

template <typename ExplainPolicy>
size_t QueryPreprocessor::injectConditionsFromJoins(size_t from, size_t to, JoinedSelectors &js, OnConditionInjections &explainOnInjections,
													const RdxContext &rdxCtx) {
	using namespace std::string_view_literals;

	size_t injectedCount = 0;
	for (size_t cur = from; cur < to; cur = Next(cur)) {
		container_[cur].InvokeAppropriate<void>(
			[](const SubQueryEntry &) { assertrx_throw(0); }, [](const SubQueryFieldEntry &) { assertrx_throw(0); },
			Skip<QueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue>{},
			[&](const QueryEntriesBracket &) {
				size_t injCount = injectConditionsFromJoins<ExplainPolicy>(cur + 1, Next(cur), js, explainOnInjections, rdxCtx);
				to += injCount;
				injectedCount += injCount;
				assertrx_throw(to <= container_.size());
			},
			[&](const JoinQueryEntry &jqe) {
				assertrx_throw(js.size() > jqe.joinIndex);
				JoinedSelector &joinedSelector = js[jqe.joinIndex];
				const bool byValues = joinedSelector.PreResult() && joinedSelector.PreResult()->dataMode == JoinPreResult::ModeValues;

				auto explainJoinOn = ExplainPolicy::AppendJoinOnExplain(explainOnInjections);
				explainJoinOn.Init(jqe, js, byValues);

				// Checking if we are able to preselect something from RightNs, or there are preselected results
				if (!byValues) {
					const auto &rNsCfg = joinedSelector.RightNs()->Config();
					if (rNsCfg.maxPreselectSize == 0 && rNsCfg.maxPreselectPart == 0.0) {
						explainJoinOn.Skipped("maxPreselectSize and maxPreselectPart == 0"sv);
						return;
					}
				} else {
					if (!joinedSelector.PreResult()->values.IsPreselectAllowed()) {
						explainJoinOn.Skipped("Preselect is not allowed"sv);
						return;
					}
				}
				const auto &joinEntries = joinedSelector.joinQuery_.joinEntries_;
				// LeftJoin-s shall not be in QueryEntries container_ by construction
				assertrx_throw(joinedSelector.Type() == InnerJoin || joinedSelector.Type() == OrInnerJoin);
				// Checking if we have anything to inject into main Where clause
				bool foundANDOrOR = false;
				for (const auto &je : joinEntries) {
					if (je.Operation() != OpNot) {
						foundANDOrOR = true;
						break;
					}
				}
				if (!foundANDOrOR) {
					explainJoinOn.Skipped("And or Or operators not found"sv);
					return;
				}

				OpType op = GetOperation(cur);
				if (joinedSelector.Type() == OrInnerJoin) {
					if (op == OpNot) throw Error(errQueryExec, "OR INNER JOIN with operation NOT");
					op = OpOr;
					joinedSelector.SetType(InnerJoin);
				}

				// inserting Bracket for JoinQuery itself into ExpressionTree
				SetOperation(OpAnd, cur);
				// !!!Warning jqe reference will be invalidated after EncloseInBracket
				EncloseInBracket(cur, cur + 1, op);
				++cur;
				++to;
				++injectedCount;

				explainJoinOn.ReserveOnEntries(joinEntries.size());

				size_t count = 0;
				bool prevIsSkipped = false;
				size_t orChainLength = 0;
				for (size_t i = 0, s = joinEntries.size(); i < s; ++i) {
					const QueryJoinEntry &joinEntry = joinEntries[i];
					auto explainEntry = explainJoinOn.AppendOnEntryExplain();
					explainEntry.InitialCondition(joinEntry, joinedSelector);
					CondType condition = joinEntry.Condition();
					OpType operation = joinEntry.Operation();
					switch (operation) {
						case OpNot:
							orChainLength = 0;
							switch (condition) {
								case CondLt:
									condition = CondGe;
									break;
								case CondLe:
									condition = CondGt;
									break;
								case CondGt:
									condition = CondLe;
									break;
								case CondGe:
									condition = CondLt;
									break;
								case CondEq:
								case CondSet:
									prevIsSkipped = true;
									explainEntry.Skipped("Skipped due to condition Eq|Set with operation Not."sv);
									continue;
								case CondAny:
								case CondRange:
								case CondAllSet:
								case CondEmpty:
								case CondLike:
								case CondDWithin:
									throw Error(errQueryExec, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
							}
							operation = OpAnd;
							break;
						case OpOr:
							explainEntry.OrChainPart(true);
							if (prevIsSkipped) {
								continue;
							}
							++orChainLength;
							break;
						case OpAnd:
							orChainLength = 0;
							break;
					}
					CondType queryCondition{CondAny};
					VariantArray values;
					if (byValues) {
						assertrx_throw(joinedSelector.itemQuery_.Entries().Is<QueryEntry>(i));
						assertrx_throw(joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i).FieldName() == joinEntry.RightFieldName());
						CollateOpts collate;
						if (joinEntry.IsLeftFieldIndexed()) {
							collate = ns_.indexes_[joinEntry.LeftIdxNo()]->Opts().collateOpts_;
						}
						std::tie(queryCondition, values) = queryValuesFromOnCondition(condition, joinEntry, joinedSelector, collate);
					} else {
						bool skip = false;
						switch (condition) {
							case CondAny:
							case CondEmpty:
							case CondLike:
							case CondDWithin:
								explainEntry.Skipped("Skipped due to unsupperted on condition"sv);
								skip = true;
								break;
							case CondRange:
							case CondLt:
							case CondLe:
							case CondGt:
							case CondGe:
								joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i).FieldType().EvaluateOneOf(
									[&skip,
									 &explainEntry](OneOf<KeyValueType::String, KeyValueType::Composite, KeyValueType::Tuple,
														  KeyValueType::Uuid, KeyValueType::Null, KeyValueType::Undefined>) noexcept {
										skip = true;
										explainEntry.Skipped(
											"Skipped due to condition Lt|Le|Gt|Ge|Range with not indexed or not numeric field."sv);
									},
									[](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) noexcept {
									});
								break;
							case CondEq:
							case CondSet:
							case CondAllSet:
								joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i).FieldType().EvaluateOneOf(
									[&skip, &explainEntry](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept {
										skip = true;
										explainEntry.Skipped("Skipped due to condition Eq|Set|AllSet with composite index."sv);
									},
									[](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
											 KeyValueType::String, KeyValueType::Uuid, KeyValueType::Null,
											 KeyValueType::Undefined>) noexcept {});
								break;
						}
						if (!skip) {
							std::string explainSelect;
							AggType selectAggType;
							std::tie(queryCondition, values) =
								queryValuesFromOnCondition(explainSelect, selectAggType, *joinedSelector.RightNs(),
														   joinedSelector.JoinQuery(), joinEntry, condition, rdxCtx);
							explainEntry.ExplainSelect(std::move(explainSelect), selectAggType);
						}
					}
					if (!values.empty()) {
						Insert(cur, operation, QueryEntry{QueryField(joinEntry.LeftFieldData()), queryCondition, std::move(values)});
						explainEntry.Succeed(Get<QueryEntry>(cur));
						++cur;
						++count;
						prevIsSkipped = false;
					} else {
						explainEntry.Skipped("Skipped as cannot obtain values from right namespace."sv);
						if (operation == OpOr) {
							Erase(cur - orChainLength, cur);
							cur -= orChainLength;
							count -= orChainLength;
							// Marking On-injections as fail for removed entries.
							explainJoinOn.FailOnEntriesAsOrChain(orChainLength);
						}
						prevIsSkipped = true;
					}
				}  // end of entries processing

				if (count > 0) {
					EncloseInBracket(cur - count, cur, OpAnd);

					explainJoinOn.Succeed(
						[this, cur, count, &js](WrSerializer &ser) { briefDump(cur - count, Next(cur - count), js, ser); });

					++cur;
					injectedCount += count + 1;
					to += count + 1;
				} else {
					explainJoinOn.Skipped("Skipped as there are no injected conditions");
				}
			});
	}
	return injectedCount;
}

class JoinOnExplainDisabled {
	JoinOnExplainDisabled() noexcept = default;
	struct OnEntryExplain {
		OnEntryExplain() noexcept = default;

		RX_ALWAYS_INLINE void InitialCondition(const QueryJoinEntry &, const JoinedSelector &) const noexcept {}
		RX_ALWAYS_INLINE void Succeed(const QueryEntry &) const noexcept {}
		RX_ALWAYS_INLINE void Skipped(std::string_view) const noexcept {}
		RX_ALWAYS_INLINE void OrChainPart(bool) const noexcept {}
		RX_ALWAYS_INLINE void ExplainSelect(std::string &&, AggType) const noexcept {}
	};

public:
	[[nodiscard]] RX_ALWAYS_INLINE static JoinOnExplainDisabled AppendJoinOnExplain(OnConditionInjections &) noexcept { return {}; }

	RX_ALWAYS_INLINE void Init(const JoinQueryEntry &, const JoinedSelectors &, bool) const noexcept {}
	RX_ALWAYS_INLINE void Succeed(const std::function<void(WrSerializer &)> &) const noexcept {}
	RX_ALWAYS_INLINE void Skipped(std::string_view) const noexcept {}
	RX_ALWAYS_INLINE void ReserveOnEntries(size_t) const noexcept {}
	[[nodiscard]] RX_ALWAYS_INLINE OnEntryExplain AppendOnEntryExplain() const noexcept { return {}; }

	RX_ALWAYS_INLINE void FailOnEntriesAsOrChain(size_t) const noexcept {}
};

class JoinOnExplainEnabled {
	using time_point_t = ExplainCalc::Clock::time_point;
	struct OnEntryExplain {
		OnEntryExplain(ConditionInjection &explainEntry) noexcept : startTime_(ExplainCalc::Clock::now()), explainEntry_(explainEntry) {}
		~OnEntryExplain() noexcept { explainEntry_.totalTime_ = ExplainCalc::Clock::now() - startTime_; }
		OnEntryExplain(const OnEntryExplain &) = delete;
		OnEntryExplain(OnEntryExplain &&) = delete;
		OnEntryExplain &operator=(const OnEntryExplain &) = delete;
		OnEntryExplain &operator=(OnEntryExplain &&) = delete;

		void InitialCondition(const QueryJoinEntry &joinEntry, const JoinedSelector &joinedSelector) {
			explainEntry_.initCond = joinEntry.DumpCondition(joinedSelector);
		}
		void Succeed(const QueryEntry &newEntry) {
			explainEntry_.succeed = true;
			explainEntry_.reason = "";
			explainEntry_.newCond = newEntry.DumpBrief();
			explainEntry_.valuesCount = newEntry.Values().size();
		}

		void Skipped(std::string_view reason) noexcept {
			if (explainEntry_.reason.empty()) {
				explainEntry_.reason = reason;
			}
			explainEntry_.succeed = false;
		}

		void OrChainPart(bool orChainPart) noexcept { explainEntry_.orChainPart_ = orChainPart; }
		void ExplainSelect(std::string &&explain, AggType aggType) noexcept {
			explainEntry_.explain = std::move(explain);
			explainEntry_.aggType = aggType;
		}

	private:
		time_point_t startTime_;
		ConditionInjection &explainEntry_;
	};

	JoinOnExplainEnabled(const JoinOnExplainEnabled &) = delete;
	JoinOnExplainEnabled(JoinOnExplainEnabled &&) = delete;
	JoinOnExplainEnabled &operator=(const JoinOnExplainEnabled &) = delete;
	JoinOnExplainEnabled &operator=(JoinOnExplainEnabled &&) = delete;

	JoinOnExplainEnabled(JoinOnInjection &joinOn) noexcept : explainJoinOn_(joinOn), startTime_(ExplainCalc::Clock::now()) {}

public:
	[[nodiscard]] static JoinOnExplainEnabled AppendJoinOnExplain(OnConditionInjections &explainOnInjections) {
		return {explainOnInjections.emplace_back()};
	}
	~JoinOnExplainEnabled() noexcept { explainJoinOn_.totalTime_ = ExplainCalc::Clock::now() - startTime_; }

	void Init(const JoinQueryEntry &jqe, const JoinedSelectors &js, bool byValues) {
		const JoinedSelector &joinedSelector = js[jqe.joinIndex];
		explainJoinOn_.rightNsName = joinedSelector.RightNsName();
		explainJoinOn_.joinCond = jqe.DumpOnCondition(js);
		explainJoinOn_.type = byValues ? JoinOnInjection::ByValue : JoinOnInjection::Select;
	}
	void Succeed(const std::function<void(WrSerializer &)> &setInjectedCond) {
		explainJoinOn_.succeed = true;
		setInjectedCond(explainJoinOn_.injectedCond);
	}
	void Skipped(std::string_view reason) noexcept {
		if (explainJoinOn_.reason.empty()) {
			explainJoinOn_.reason = reason;
		}
		explainJoinOn_.succeed = false;
	}

	void ReserveOnEntries(size_t count) { explainJoinOn_.conditions.reserve(count); }
	[[nodiscard]] OnEntryExplain AppendOnEntryExplain() { return {explainJoinOn_.conditions.emplace_back()}; };

	void FailOnEntriesAsOrChain(size_t orChainLength) {
		using namespace std::string_view_literals;
		auto &conditions = explainJoinOn_.conditions;
		assertrx(conditions.size() >= orChainLength);
		// Marking On-injections as fail for removed entries.
		for (size_t jsz = conditions.size(), j = jsz - orChainLength; j < jsz; ++j) {
			conditions[j].succeed = false;
			conditions[j].orChainPart_ = true;
		}
	}

private:
	JoinOnInjection &explainJoinOn_;
	time_point_t startTime_;
};

void QueryPreprocessor::setQueryIndex(QueryField &qField, int idxNo, const NamespaceImpl &ns) {
	const auto &idx = *ns.indexes_[idxNo];
	std::vector<KeyValueType> compositeFieldsTypes;
	if (idxNo >= ns.indexes_.firstCompositePos()) {
#ifndef NDEBUG
		const bool ftIdx = IsFullText(idx.Type());
#endif
		for (const auto f : ns.indexes_[idxNo]->Fields()) {
			if (f == IndexValueType::SetByJsonPath) {
				// not indexed fields allowed only in ft composite indexes
				assertrx_throw(ftIdx);
				compositeFieldsTypes.push_back(KeyValueType::String{});
			} else {
				assertrx_throw(f <= ns.indexes_.firstCompositePos());
				compositeFieldsTypes.push_back(ns.indexes_[f]->SelectKeyType());
			}
		}
	}
	qField.SetIndexData(idxNo, FieldsSet(idx.Fields()), idx.KeyType(), idx.SelectKeyType(), std::move(compositeFieldsTypes));
}

void QueryPreprocessor::SetQueryField(QueryField &qField, const NamespaceImpl &ns) {
	int idxNo = IndexValueType::SetByJsonPath;
	if (ns.getIndexByNameOrJsonPath(qField.FieldName(), idxNo)) {
		setQueryIndex(qField, idxNo, ns);
	} else {
		qField.SetField({ns.tagsMatcher_.path2tag(qField.FieldName())});
	}
}

}  // namespace reindexer
