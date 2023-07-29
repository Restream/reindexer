#include "querypreprocessor.h"
#include "core/index/index.h"
#include "core/index/indextext/indextext.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/nsselecter/sortexpression.h"
#include "core/payload/fieldsset.h"
#include "core/query/queryentry.h"
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
									: ((query_.strictMode == StrictModeNotSet) ? ns_.config_.strictMode : query_.strictMode)),
	  start_(query_.start),
	  count_(query_.count),
	  forcedSortOrder_(!query_.forcedSortOrder_.empty()),
	  reqMatchedOnce_(ctx.reqMatchedOnceFlag) {
	if (forcedSortOrder_ && (start_ > QueryEntry::kDefaultOffset || count_ < QueryEntry::kDefaultLimit)) {
		assertrx(!query_.sortingEntries_.empty());
		static const std::vector<JoinedSelector> emptyJoinedSelectors;
		const auto &sEntry = query_.sortingEntries_[0];
		if (SortExpression::Parse(sEntry.expression, emptyJoinedSelectors).ByIndexField()) {
			QueryEntry qe;
			qe.values.reserve(query_.forcedSortOrder_.size());
			for (const auto &v : query_.forcedSortOrder_) qe.values.push_back(v);
			qe.condition = query_.forcedSortOrder_.size() == 1 ? CondEq : CondSet;
			qe.index = sEntry.expression;
			if (!ns_.getIndexByNameOrJsonPath(qe.index, qe.idxNo)) {
				qe.idxNo = IndexValueType::SetByJsonPath;
			}
			desc_ = sEntry.desc;
			Append(desc_ ? OpNot : OpAnd, std::move(qe));
			queryEntryAddedByForcedSortOptimization_ = true;
		}
	}
	if (ctx.isMergeQuery == IsMergeQuery::Yes) {
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
		if (it->HoldsOrReferTo<QueryEntry>() && it->Value<QueryEntry>().idxNo != IndexValueType::SetByJsonPath) {
			const auto indexNo = it->Value<QueryEntry>().idxNo;
			auto &index = ns_.indexes_[indexNo];
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
		assertrx(start <= start_);
		start_ = start;
		assertrx(count <= count_);
		count_ = count;
		return count_ || (reqMatchedOnce_ && !matchedAtLeastOnce);
	} else if (ftEntry_) {
		if (!matchedAtLeastOnce) return false;
		qresHolder.BackupContainer();
		start_ = query_.start;
		count_ = query_.count;
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

void QueryPreprocessor::checkStrictMode(const std::string &index, int idxNo) const {
	if (idxNo != IndexValueType::SetByJsonPath) return;
	switch (strictMode_) {
		case StrictModeIndexes:
			throw Error(errParams,
						"Current query strict mode allows filtering by indexes only. There are no indexes with name '%s' in namespace '%s'",
						index, ns_.name_);
		case StrictModeNames:
			if (ns_.tagsMatcher_.path2tag(index).empty()) {
				throw Error(errParams,
							"Current query strict mode allows filtering by existing fields only. There are no fields with name '%s' in "
							"namespace '%s'",
							index, ns_.name_);
			}
		case StrictModeNotSet:
		case StrictModeNone:
			return;
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
		throw Error{errParams, "Bracket cannot be empty"};
	}
	const size_t next = Next(i);
	const OpType op = GetOperation(i);
	if (op != OpAnd && GetOperation(i + 1) != OpAnd) return false;
	if (next == Next(i + 1)) return true;
	return op == OpAnd && (next == Size() || GetOperation(next) != OpOr);
}

size_t QueryPreprocessor::removeBrackets(size_t begin, size_t end) {
	if (begin != end && GetOperation(begin) == OpOr) {
		throw Error{errParams, "First condition cannot be with operation OR"};
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
		Skip<QueryEntriesBracket, JoinQueryEntry, AlwaysFalse>{},
		[this](QueryEntry &entry) {
			if (entry.idxNo == IndexValueType::NotSet) {
				if (!ns_.getIndexByNameOrJsonPath(entry.index, entry.idxNo)) {
					entry.idxNo = IndexValueType::SetByJsonPath;
				}
			}
			checkStrictMode(entry.index, entry.idxNo);
		},
		[this](BetweenFieldsQueryEntry &entry) {
			if (entry.firstIdxNo == IndexValueType::NotSet) {
				if (!ns_.getIndexByNameOrJsonPath(entry.firstIndex, entry.firstIdxNo)) {
					entry.firstIdxNo = IndexValueType::SetByJsonPath;
				}
			}
			checkStrictMode(entry.firstIndex, entry.firstIdxNo);
			if (entry.secondIdxNo == IndexValueType::NotSet) {
				if (!ns_.getIndexByNameOrJsonPath(entry.secondIndex, entry.secondIdxNo)) {
					entry.secondIdxNo = IndexValueType::SetByJsonPath;
				}
			}
			checkStrictMode(entry.secondIndex, entry.secondIdxNo);
		});
}

size_t QueryPreprocessor::lookupQueryIndexes(uint16_t dst, uint16_t srcBegin, uint16_t srcEnd) {
	assertrx(dst <= srcBegin);
	h_vector<uint16_t, kMaxIndexes> iidx(kMaxIndexes, uint16_t(0));
	size_t merged = 0;
	for (size_t src = srcBegin, nextSrc; src < srcEnd; src = nextSrc) {
		nextSrc = Next(src);
		const bool changeDst = container_[src].InvokeAppropriate<bool>(
			[&](const QueryEntriesBracket &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
				const size_t mergedInBracket = lookupQueryIndexes(dst + 1, src + 1, nextSrc);
				container_[dst].Value<QueryEntriesBracket>().Erase(mergedInBracket);
				merged += mergedInBracket;
				return true;
			},
			[&](QueryEntry &entry) {
				const bool isIndexField = (entry.idxNo >= 0);
				if (isIndexField) {
					// try merge entries with AND opetator
					if ((GetOperation(src) == OpAnd) && (nextSrc >= srcEnd || GetOperation(nextSrc) != OpOr)) {
						if (size_t(entry.idxNo) >= iidx.size()) {
							const auto oldSize = iidx.size();
							iidx.resize(size_t(entry.idxNo) + 1);
							std::fill(iidx.begin() + oldSize, iidx.begin() + iidx.size(), 0);
						}
						auto &iidxRef = iidx[entry.idxNo];
						if (iidxRef > 0 && !ns_.indexes_[entry.idxNo]->Opts().IsArray()) {
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
			});
		if (changeDst) dst = Next(dst);
	}
	return merged;
}

void QueryPreprocessor::CheckUniqueFtQuery() const {
	bool found = false;
	ExecuteAppropriateForEach(Skip<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse>{}, [&](const QueryEntry &qe) {
		if (qe.idxNo != IndexValueType::SetByJsonPath && IsFullText(ns_.indexes_[qe.idxNo]->Type())) {
			if (found) {
				throw Error{errParams, "Query cannot contain more than one full text condition"};
			} else {
				found = true;
			}
		}
	});
}

bool QueryPreprocessor::ContainsFullTextIndexes() const {
	for (auto it = cbegin().PlainIterator(), end = cend().PlainIterator(); it != end; ++it) {
		if (it->HoldsOrReferTo<QueryEntry>() && it->Value<QueryEntry>().idxNo != IndexValueType::SetByJsonPath &&
			IsFullText(ns_.indexes_[it->Value<QueryEntry>().idxNo]->Type())) {
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

static void createCompositeKeyValues(const h_vector<std::pair<int, VariantArray>, 4> &values, const PayloadType &plType,
									 VariantArray &ret) {
	PayloadValue d(plType.TotalSize());
	Payload pl(plType, d);
	createCompositeKeyValues(values, plType, pl, ret, 0);
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
		if (!HoldsOrReferTo<QueryEntry>(cur) || GetOperation(cur) != OpAnd) {
			continue;
		}
		const auto next = Next(cur);
		if ((next < end && GetOperation(next) == OpOr)) {
			continue;
		}
		auto &qe = Get<QueryEntry>(cur);
		if ((qe.condition != CondEq && qe.condition != CondSet) || qe.idxNo >= ns_.payloadType_.NumFields() || qe.idxNo < 0) {
			continue;
		}

		const std::vector<int> *found = getCompositeIndex(qe.idxNo);
		if (!found || found->empty()) {
			continue;
		}
		searcher.Add(qe.idxNo, *found, cur);
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
			if (rx_unlikely(!res.fields.contains(qe.idxNo))) {
				throw Error(errLogic, "Error during composite index's fields substitution (this should not happen)");
			}
			if (rx_unlikely(qe.condition == CondEq && qe.values.size() == 0)) {
				throw Error(errParams, "Condition EQ must have at least 1 argument, but provided 0");
			}
			maxSetSize = std::max(maxSetSize, qe.values.size());
			resultSetSize = (resultSetSize == 0) ? qe.values.size() : (resultSetSize * qe.values.size());
		}
		static const CompositeValuesCountLimits kCompositeSetLimits;
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
			const auto idxKeyType = ns_.indexes_[qe.idxNo]->KeyType();
			for (auto &v : qe.values) {
				v.convert(idxKeyType);
			}
			values.emplace_back(qe.idxNo, std::move(qe.values));
		}
		{
			QueryEntry ce(CondSet, ns_.indexes_[res.idx]->Name(), res.idx);
			ce.values.reserve(resultSetSize);
			createCompositeKeyValues(values, ns_.payloadType_, ce.values);
			if (ce.values.size() == 1) {
				ce.condition = CondEq;
			}
			const auto first = res.entries.front();
			SetOperation(OpAnd, first);
			container_[first].SetValue(std::move(ce));
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

void QueryPreprocessor::convertWhereValues(QueryEntry *qe) const {
	const FieldsSet *fields = nullptr;
	KeyValueType keyType{KeyValueType::Undefined{}};
	const bool isIndexField = (qe->idxNo != IndexValueType::SetByJsonPath);
	if (isIndexField) {
		keyType = ns_.indexes_[qe->idxNo]->SelectKeyType();
		fields = &ns_.indexes_[qe->idxNo]->Fields();
	}
	if (!keyType.Is<KeyValueType::Undefined>()) {
		if (qe->condition != CondDWithin) {
			for (auto &key : qe->values) {
				key.convert(keyType, &ns_.payloadType_, fields);
			}
		}
	}
}

void QueryPreprocessor::convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const {
	for (auto it = begin; it != end; ++it) {
		it->InvokeAppropriate<void>(
			Skip<JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse>{},
			[this, &it](const QueryEntriesBracket &) { convertWhereValues(it.begin(), it.end()); },
			[this](QueryEntry &qe) { convertWhereValues(&qe); });
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
			[this, &it, &foundIndexes](const QueryEntriesBracket &) {
				findMaxIndex(it.cbegin(), it.cend(), foundIndexes);
				return FoundIndexInfo();
			},
			[this](const QueryEntry &entry) -> FoundIndexInfo {
				if (entry.idxNo != IndexValueType::SetByJsonPath && !entry.distinct) {
					const auto idxPtr = ns_.indexes_[entry.idxNo].get();
					if (idxPtr->IsOrdered() && !idxPtr->Opts().IsArray()) {
						if (IsOrderedCondition(entry.condition)) {
							return FoundIndexInfo{idxPtr, FoundIndexInfo::ConditionType::Compatible};
						} else if (entry.condition == CondAny || entry.values.size() > 1) {
							return FoundIndexInfo{idxPtr, FoundIndexInfo::ConditionType::Incompatible};
						}
					}
				}
				return FoundIndexInfo();
			},
			[](const JoinQueryEntry &) { return FoundIndexInfo(); }, [](const BetweenFieldsQueryEntry &) { return FoundIndexInfo(); },
			[](const AlwaysFalse &) { return FoundIndexInfo(); });
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
	if ((lqe->condition == CondEq || lqe->condition == CondSet) && (rqe.condition == CondEq || rqe.condition == CondSet)) {
		// intersect 2 queryentries on the same index
		if (rx_unlikely(lqe->values.empty())) {
			return true;
		}
		if (container_[lhs].IsRef()) {
			container_[lhs].SetValue(const_cast<const QueryEntry &>(*lqe));
			lqe = &Get<QueryEntry>(lhs);
		}
		VariantArray setValues;
		if (rx_likely(!rqe.values.empty())) {
			convertWhereValues(lqe);
			convertWhereValues(&rqe);
			VariantArray *first = &lqe->values;
			VariantArray *second = &rqe.values;
			if (lqe->values.size() > rqe.values.size()) {
				std::swap(first, second);
			}
			setValues.reserve(first->size());
			constexpr size_t kMinArraySizeToUseHashSet = 250;
			if (second->size() < kMinArraySizeToUseHashSet) {
				// Intersect via binary search + sort for small vectors
				std::sort(first->begin(), first->end());
				for (auto &&v : *second) {
					if (std::binary_search(first->begin(), first->end(), v)) {
						setValues.emplace_back(std::move(v));
					}
				}
			} else {
				// Intersect via hash_set for large vectors
				reindexer::fast_hash_set<reindexer::Variant> set;
				set.reserve(first->size() * 2);
				for (auto &&v : *first) {
					set.emplace(std::move(v));
				}
				for (auto &&v : *second) {
					if (set.erase(v)) {
						setValues.emplace_back(std::move(v));
					}
				}
			}
		}

		lqe->values = std::move(setValues);
		lqe->condition = (lqe->values.size() == 1) ? CondEq : CondSet;
		lqe->distinct |= rqe.distinct;
		return true;
	} else if (rqe.condition == CondAny) {
		if (!lqe->distinct && rqe.distinct) {
			if (container_[lhs].IsRef()) {
				container_[lhs].SetValue(const_cast<const QueryEntry &>(*lqe));
				lqe = &Get<QueryEntry>(lhs);
			}
			lqe->distinct = true;
		}
		return true;
	} else if (lqe->condition == CondAny) {
		const bool distinct = lqe->distinct || rqe.distinct;
		if (container_[rhs].IsRef()) {
			container_[lhs].SetValue(const_cast<const QueryEntry &>(rqe));
		} else {
			container_[lhs].SetValue(std::move(rqe));
		}
		Get<QueryEntry>(lhs).distinct = distinct;
		return true;
	}

	return false;
}

void QueryPreprocessor::AddDistinctEntries(const h_vector<Aggregator, 4> &aggregators) {
	bool wasAdded = false;
	for (auto &ag : aggregators) {
		if (ag.Type() != AggDistinct) continue;
		QueryEntry qe;
		assertrx(ag.Names().size() == 1);
		qe.index = ag.Names()[0];
		qe.condition = CondAny;
		qe.distinct = true;
		Append(wasAdded ? OpOr : OpAnd, std::move(qe));
		wasAdded = true;
	}
}

void QueryPreprocessor::fillQueryEntryFromOnCondition(QueryEntry &queryEntry, NamespaceImpl &rightNs, Query joinQuery,
													  std::string joinIndex, CondType condition, KeyValueType valuesType,
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
	joinQuery.count = limit + 2;
	joinQuery.start = 0;
	joinQuery.sortingEntries_.clear();
	joinQuery.forcedSortOrder_.clear();
	joinQuery.aggregations_.clear();
	switch (condition) {
		case CondEq:
		case CondSet:
			joinQuery.Distinct(std::move(joinIndex));
			break;
		case CondLt:
		case CondLe:
			joinQuery.Aggregate(AggMax, {std::move(joinIndex)});
			break;
		case CondGt:
		case CondGe:
			joinQuery.Aggregate(AggMin, {std::move(joinIndex)});
			break;
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			throw Error(errParams, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
	}
	SelectCtx ctx{joinQuery, nullptr};
	QueryResults qr;
	rightNs.Select(qr, ctx, rdxCtx);
	if (qr.Count() > limit) return;
	assertrx(qr.aggregationResults.size() == 1);
	switch (condition) {
		case CondEq:
		case CondSet: {
			assertrx(qr.aggregationResults[0].type == AggDistinct);
			queryEntry.values.reserve(qr.aggregationResults[0].distincts.size());
			assertrx(qr.aggregationResults[0].distinctsFields.size() == 1);
			const auto field = qr.aggregationResults[0].distinctsFields[0];
			for (Variant &distValue : qr.aggregationResults[0].distincts) {
				if (distValue.Type().Is<KeyValueType::Composite>()) {
					ConstPayload pl(qr.aggregationResults[0].payloadType, distValue.operator const PayloadValue &());
					VariantArray v;
					if (field == IndexValueType::SetByJsonPath) {
						assertrx(qr.aggregationResults[0].distinctsFields.getTagsPathsLength() == 1);
						pl.GetByJsonPath(qr.aggregationResults[0].distinctsFields.getTagsPath(0), v, valuesType);
					} else {
						pl.Get(field, v);
					}
					assertrx(v.size() == 1);
					queryEntry.values.emplace_back(std::move(v[0]));
				} else {
					queryEntry.values.emplace_back(std::move(distValue));
				}
			}
			queryEntry.condition = (queryEntry.values.size() == 1) ? CondEq : CondSet;
			break;
		}
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			if (auto value = qr.aggregationResults[0].GetValue()) {
				queryEntry.condition = condition;
				queryEntry.values.emplace_back(*value);
			}
			break;
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			throw Error(errParams, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
	}
}

template <bool byJsonPath>
void QueryPreprocessor::fillQueryEntryFromOnCondition(QueryEntry &queryEntry, std::string_view joinIndex, CondType condition,
													  const JoinedSelector &joinedSelector, KeyValueType valuesType, const int rightIdxNo,
													  const CollateOpts &collate) {
	JoinPreResult::Values &values = joinedSelector.preResult_->values;
	switch (condition) {
		case CondEq:
		case CondSet: {
			joinedSelector.readValuesFromPreResult<byJsonPath>(queryEntry.values, valuesType, rightIdxNo, joinIndex);
			queryEntry.condition = (queryEntry.values.size() == 1) ? CondEq : CondSet;
			return;
		}
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe: {
			queryEntry.condition = condition;
			VariantArray buffer;
			for (const ItemRef &item : values) {
				buffer.clear();
				assertrx(!item.Value().IsFree());
				const ConstPayload pl{values.payloadType, item.Value()};
				if constexpr (byJsonPath) {
					pl.GetByJsonPath(joinIndex, values.tagsMatcher, buffer, valuesType);
				} else {
					pl.Get(rightIdxNo, buffer);
				}
				for (Variant &v : buffer) {
					if (queryEntry.values.empty()) {
						queryEntry.values.emplace_back(std::move(v));
					} else {
						const auto cmp = queryEntry.values[0].Compare(v, collate);
						if (condition == CondLt || condition == CondLe) {
							if (cmp < 0) {
								queryEntry.values[0] = std::move(v);
							}
						} else {
							if (cmp > 0) {
								queryEntry.values[0] = std::move(v);
							}
						}
					}
				}
			}
		} break;
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			throw Error(errParams, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
	}
}

void QueryPreprocessor::injectConditionsFromJoins(size_t from, size_t to, JoinedSelectors &js, const RdxContext &rdxCtx) {
	for (size_t cur = from; cur < to; cur = Next(cur)) {
		container_[cur].InvokeAppropriate<void>(
			Skip<QueryEntry, BetweenFieldsQueryEntry, AlwaysFalse>{},
			[&js, cur, this, &rdxCtx](const QueryEntriesBracket &) { injectConditionsFromJoins(cur + 1, Next(cur), js, rdxCtx); },
			[&](const JoinQueryEntry &jqe) {
				assertrx(js.size() > jqe.joinIndex);
				JoinedSelector &joinedSelector = js[jqe.joinIndex];
				const bool byValues = joinedSelector.PreResult()->dataMode == JoinPreResult::ModeValues;
				if (!byValues) {
					const auto &rNsCfg = joinedSelector.RightNs()->Config();
					if (rNsCfg.maxPreselectSize == 0 && rNsCfg.maxPreselectPart == 0.0) return;
				} else {
					if (!joinedSelector.PreResult()->values.IsPreselectAllowed()) return;
				}
				assertrx(joinedSelector.Type() == InnerJoin || joinedSelector.Type() == OrInnerJoin);
				const auto &joinEntries = joinedSelector.joinQuery_.joinEntries_;
				bool foundANDOrOR = false;
				for (const auto &je : joinEntries) {
					if (je.op_ != OpNot) {
						foundANDOrOR = true;
						break;
					}
				}
				if (!foundANDOrOR) return;
				OpType op = GetOperation(cur);
				if (joinedSelector.Type() == OrInnerJoin) {
					if (op == OpNot) throw Error(errParams, "OR INNER JOIN with operation NOT");
					op = OpOr;
					joinedSelector.SetType(InnerJoin);
				}
				SetOperation(OpAnd, cur);
				EncloseInBracket(cur, cur + 1, op);
				++cur;
				size_t count = 0;
				bool prevIsSkipped = false;
				size_t orChainLength = 0;
				for (size_t i = 0, s = joinEntries.size(); i < s; ++i) {
					const QueryJoinEntry &joinEntry = joinEntries[i];
					CondType condition = joinEntry.condition_;
					OpType operation = joinEntry.op_;
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
									continue;
								case CondAny:
								case CondRange:
								case CondAllSet:
								case CondEmpty:
								case CondLike:
								case CondDWithin:
									throw Error(errParams, "Unsupported condition in ON statment: %s", CondTypeToStr(condition));
							}
							operation = OpAnd;
							break;
						case OpOr:
							if (prevIsSkipped) continue;
							++orChainLength;
							break;
						case OpAnd:
							orChainLength = 0;
							break;
					}
					QueryEntry newEntry;
					newEntry.index = joinEntry.index_;
					newEntry.idxNo = IndexValueType::SetByJsonPath;
					KeyValueType valuesType = KeyValueType::Undefined{};
					CollateOpts collate;
					if (ns_.getIndexByNameOrJsonPath(newEntry.index, newEntry.idxNo)) {
						const Index &index = *ns_.indexes_[newEntry.idxNo];
						valuesType = index.SelectKeyType();
						collate = index.Opts().collateOpts_;
					}
					if (byValues) {
						assertrx(joinedSelector.itemQuery_.entries.HoldsOrReferTo<QueryEntry>(i));
						const QueryEntry &qe = joinedSelector.itemQuery_.entries.Get<QueryEntry>(i);
						assertrx(qe.index == joinEntry.joinIndex_);
						const int rightIdxNo = qe.idxNo;
						if (rightIdxNo == IndexValueType::SetByJsonPath) {
							fillQueryEntryFromOnCondition<true>(newEntry, joinEntry.joinIndex_, condition, joinedSelector, valuesType,
																rightIdxNo, collate);
						} else {
							fillQueryEntryFromOnCondition<false>(newEntry, joinEntry.joinIndex_, condition, joinedSelector, valuesType,
																 rightIdxNo, collate);
						}
					} else {
						bool skip = false;
						switch (condition) {
							case CondLt:
							case CondLe:
							case CondGt:
							case CondGe: {
								const QueryEntry &qe = joinedSelector.itemQuery_.entries.Get<QueryEntry>(i);
								skip = qe.idxNo != IndexValueType::SetByJsonPath && joinedSelector.RightNs()->indexes_[qe.idxNo]->IsUuid();
								break;
							}
							case CondEq:
							case CondSet:
							case CondAllSet:
							case CondAny:
							case CondEmpty:
							case CondRange:
							case CondLike:
							case CondDWithin:
								break;
						}
						if (!skip) {
							fillQueryEntryFromOnCondition(newEntry, *joinedSelector.RightNs(), joinedSelector.JoinQuery(),
														  joinEntry.joinIndex_, condition, valuesType, rdxCtx);
						}
					}
					if (!newEntry.values.empty()) {
						Insert(cur, operation, std::move(newEntry));
						++cur;
						++count;
						prevIsSkipped = false;
					} else {
						if (operation == OpOr) {
							Erase(cur - orChainLength, cur);
							count -= orChainLength;
						}
						prevIsSkipped = true;
					}
				}
				if (count > 0) {
					EncloseInBracket(cur - count, cur, OpAnd);
					++cur;
					to += count + 2;
				}
			});
	}
}

}  // namespace reindexer
