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

namespace reindexer {

QueryPreprocessor::QueryPreprocessor(QueryEntries &&queries, const Query &query, NamespaceImpl *ns, bool reqMatchedOnce, bool inTransaction)
	: QueryEntries(std::move(queries)),
	  ns_(*ns),
	  strictMode_(inTransaction ? StrictModeNone : ((query.strictMode == StrictModeNotSet) ? ns_.config_.strictMode : query.strictMode)),
	  start_(query.start),
	  count_(query.count),
	  forcedSortOrder_(!query.forcedSortOrder_.empty()),
	  reqMatchedOnce_(reqMatchedOnce),
	  query_{query} {
	if (forcedSortOrder_ && (start_ > 0 || count_ < UINT_MAX)) {
		assertrx(!query.sortingEntries_.empty());
		static const std::vector<JoinedSelector> emptyJoinedSelectors;
		const auto &sEntry = query.sortingEntries_[0];
		if (SortExpression::Parse(sEntry.expression, emptyJoinedSelectors).ByIndexField()) {
			QueryEntry qe;
			qe.values.reserve(query.forcedSortOrder_.size());
			for (const auto &v : query.forcedSortOrder_) qe.values.push_back(v);
			qe.condition = query.forcedSortOrder_.size() == 1 ? CondEq : CondSet;
			qe.index = sEntry.expression;
			if (!ns_.getIndexByName(qe.index, qe.idxNo)) {
				qe.idxNo = IndexValueType::SetByJsonPath;
			}
			desc_ = sEntry.desc;
			Append(desc_ ? OpNot : OpAnd, std::move(qe));
			queryEntryAddedByForcedSortOptimization_ = true;
		}
	}
}

void QueryPreprocessor::ExcludeFtQuery(const SelectFunction &fnCtx, const RdxContext &rdxCtx) {
	if (queryEntryAddedByForcedSortOptimization_ || Size() <= 1) return;
	for (auto it = begin(), next = it, endIt = end(); it != endIt; it = next) {
		++next;
		if (it->HoldsOrReferTo<QueryEntry>() && it->Value<QueryEntry>().idxNo != IndexValueType::SetByJsonPath) {
			const auto indexNo = it->Value<QueryEntry>().idxNo;
			auto &index = ns_.indexes_[indexNo];
			if (!IsFastFullText(index->Type())) continue;
			if (it->operation != OpAnd || (next != endIt && next->operation == OpOr) || !index->EnablePreselectBeforeFt()) break;
			ftPreselect_ = index->FtPreselect(*this, indexNo, fnCtx, rdxCtx);
			std::visit(overloaded{[&](const FtMergeStatuses &) {
									  start_ = 0;
									  count_ = UINT_MAX;
									  forcedSortOrder_ = false;
									  ftEntry_ = std::move(it->Value<QueryEntry>());
									  const size_t pos = it.PlainIterator() - cbegin().PlainIterator();
									  Erase(pos, pos + 1);
								  },
								  [&](const PreselectedFtIdSetCache::Iterator &) {
									  const size_t pos = it.PlainIterator() - cbegin().PlainIterator();
									  if (pos != 0) {
										  container_[0] = std::move(container_[pos]);
									  }
									  Erase(1, Size());
								  }},
					   *ftPreselect_);
			break;
		}
	}
}

bool QueryPreprocessor::NeedNextEvaluation(unsigned start, unsigned count, bool &matchedAtLeastOnce) noexcept {
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
		start_ = query_.start;
		count_ = query_.count;
		forcedSortOrder_ = !query_.forcedSortOrder_.empty();
		Erase(1, container_.size());
		container_[0].SetValue(std::move(*ftEntry_));
		container_[0].operation = OpAnd;
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
		default:
			return;
	}
}

void QueryPreprocessor::Reduce(bool isFt) {
	bool changed;
	do {
		changed = LookupQueryIndexes();
		if (!isFt) changed = SubstituteCompositeIndexes() || changed;
		changed = removeBrackets() || changed;
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
				if (!ns_.getIndexByName(entry.index, entry.idxNo)) {
					entry.idxNo = IndexValueType::SetByJsonPath;
				}
			}
			checkStrictMode(entry.index, entry.idxNo);
		},
		[this](BetweenFieldsQueryEntry &entry) {
			if (entry.firstIdxNo == IndexValueType::NotSet) {
				if (!ns_.getIndexByName(entry.firstIndex, entry.firstIdxNo)) {
					entry.firstIdxNo = IndexValueType::SetByJsonPath;
				}
			}
			checkStrictMode(entry.firstIndex, entry.firstIdxNo);
			if (entry.secondIdxNo == IndexValueType::NotSet) {
				if (!ns_.getIndexByName(entry.secondIndex, entry.secondIdxNo)) {
					entry.secondIdxNo = IndexValueType::SetByJsonPath;
				}
			}
			checkStrictMode(entry.secondIndex, entry.secondIdxNo);
		});
}

size_t QueryPreprocessor::lookupQueryIndexes(size_t dst, const size_t srcBegin, const size_t srcEnd) {
	assertrx(dst <= srcBegin);
	h_vector<int, maxIndexes> iidx(maxIndexes);
	std::fill(iidx.begin(), iidx.begin() + maxIndexes, -1);
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
				const bool isIndexField = (entry.idxNo != IndexValueType::SetByJsonPath);
				if (isIndexField) {
					// try merge entries with AND opetator
					if ((GetOperation(src) == OpAnd) && (nextSrc >= srcEnd || GetOperation(nextSrc) != OpOr)) {
						if (static_cast<size_t>(entry.idxNo) >= iidx.size()) {
							const auto oldSize = iidx.size();
							iidx.resize(entry.idxNo + 1);
							std::fill(iidx.begin() + oldSize, iidx.begin() + iidx.size(), -1);
						}
						if (iidx[entry.idxNo] >= 0 && !ns_.indexes_[entry.idxNo]->Opts().IsArray()) {
							if (mergeQueryEntries(iidx[entry.idxNo], src)) {
								++merged;
								return false;
							}
						} else {
							iidx[entry.idxNo] = dst;
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

SortingEntries QueryPreprocessor::GetSortingEntries(bool havePreresult) const {
	if (ftEntry_) return {};
	// DO NOT use deducted sort order in the following cases:
	// - query contains explicity specified sort order
	// - query contains FullText query.
	const bool disableOptimizeSortOrder = ContainsFullTextIndexes() || !query_.sortingEntries_.empty() || havePreresult;
	return disableOptimizeSortOrder ? query_.sortingEntries_ : detectOptimalSortOrder();
}

int QueryPreprocessor::getCompositeIndex(const FieldsSet &fields) const {
	if (fields.getTagsPathsLength() == 0) {
		for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); i++) {
			if (ns_.indexes_[i]->Fields().contains(fields)) return i;
		}
	}
	return -1;
}

static void createCompositeKeyValues(const h_vector<std::pair<int, VariantArray>, 4> &values, const PayloadType &plType, Payload *pl,
									 VariantArray &ret, int n) {
	PayloadValue d(plType.TotalSize());
	Payload pl1(plType, d);
	if (!pl) pl = &pl1;

	assertrx(n >= 0 && n < static_cast<int>(values.size()));
	const auto &v = values[n];
	for (auto it = v.second.cbegin(), end = v.second.cend(); it != end; ++it) {
		pl->Set(v.first, {*it});
		if (n + 1 < static_cast<int>(values.size())) {
			createCompositeKeyValues(values, plType, pl, ret, n + 1);
		} else {
			PayloadValue pv(*pl->Value());
			pv.Clone();
			ret.push_back(Variant(std::move(pv)));
		}
	}
}

size_t QueryPreprocessor::substituteCompositeIndexes(const size_t from, const size_t to) {
	FieldsSet fields;
	size_t deleted = 0;
	for (size_t cur = from, first = from, end = to; cur < end; cur = Next(cur), end = to - deleted) {
		if (!HoldsOrReferTo<QueryEntry>(cur) || GetOperation(cur) != OpAnd || (Next(cur) < end && GetOperation(Next(cur)) == OpOr) ||
			(Get<QueryEntry>(cur).condition != CondEq && Get<QueryEntry>(cur).condition != CondSet) ||
			Get<QueryEntry>(cur).idxNo >= ns_.payloadType_.NumFields() || Get<QueryEntry>(cur).idxNo < 0) {
			// If query already rewritten, then copy current unmatched part
			first = Next(cur);
			fields.clear();
			if (container_[cur].IsSubTree()) {
				deleted += substituteCompositeIndexes(cur + 1, Next(cur));
			}
			continue;
		}
		fields.push_back(Get<QueryEntry>(cur).idxNo);
		int found = getCompositeIndex(fields);
		if ((found >= 0) && !IsFullText(ns_.indexes_[found]->Type())) {
			// composite idx found: replace conditions
			h_vector<std::pair<int, VariantArray>, 4> values;
			for (size_t i = first; i <= cur; i = Next(i)) {
				if (ns_.indexes_[found]->Fields().contains(Get<QueryEntry>(i).idxNo)) {
					values.emplace_back(Get<QueryEntry>(i).idxNo, std::move(Get<QueryEntry>(i).values));
				} else {
					SetOperation(GetOperation(i), first);
					container_[first] = std::move(container_[i]);
					first = Next(first);
				}
			}
			{
				QueryEntry ce(CondSet, ns_.indexes_[found]->Name(), found);
				createCompositeKeyValues(values, ns_.payloadType_, nullptr, ce.values, 0);
				if (ce.values.size() == 1) {
					ce.condition = CondEq;
				}
				SetOperation(OpAnd, first);
				container_[first].SetValue(std::move(ce));
			}
			deleted += (Next(cur) - Next(first));
			Erase(Next(first), Next(cur));
			cur = first;
			first = Next(first);
			fields.clear();
		}
	}
	return deleted;
}

void QueryPreprocessor::convertWhereValues(QueryEntry *qe) const {
	const FieldsSet *fields = nullptr;
	KeyValueType keyType = KeyValueUndefined;
	const bool isIndexField = (qe->idxNo != IndexValueType::SetByJsonPath);
	if (isIndexField) {
		keyType = ns_.indexes_[qe->idxNo]->SelectKeyType();
		fields = &ns_.indexes_[qe->idxNo]->Fields();
	}
	if (keyType != KeyValueUndefined) {
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

SortingEntries QueryPreprocessor::detectOptimalSortOrder() const {
	if (!AvailableSelectBySortIndex()) return {};
	if (const Index *maxIdx = findMaxIndex(cbegin(), cend())) {
		SortingEntries sortingEntries;
		sortingEntries.emplace_back(maxIdx->Name(), false);
		return sortingEntries;
	}
	return SortingEntries();
}

const Index *QueryPreprocessor::findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const {
	const Index *maxIdx = nullptr;
	for (auto it = begin; it != end; ++it) {
		const Index *foundIdx = it->InvokeAppropriate<const Index *>(
			[this, &it](const QueryEntriesBracket &) { return findMaxIndex(it.cbegin(), it.cend()); },
			[this](const QueryEntry &entry) -> const Index * {
				if (((entry.idxNo != IndexValueType::SetByJsonPath) &&
					 (entry.condition == CondGe || entry.condition == CondGt || entry.condition == CondLe || entry.condition == CondLt ||
					  entry.condition == CondRange)) &&
					!entry.distinct && ns_.indexes_[entry.idxNo]->IsOrdered() && !ns_.indexes_[entry.idxNo]->Opts().IsArray()) {
					return ns_.indexes_[entry.idxNo].get();
				} else {
					return nullptr;
				}
			},
			[](const JoinQueryEntry &) { return nullptr; }, [](const BetweenFieldsQueryEntry &) { return nullptr; },
			[](const AlwaysFalse &) { return nullptr; });
		if (!maxIdx || (foundIdx && foundIdx->Size() > maxIdx->Size())) {
			maxIdx = foundIdx;
		}
	}
	return maxIdx;
}

bool QueryPreprocessor::mergeQueryEntries(size_t lhs, size_t rhs) {
	QueryEntry *lqe = &Get<QueryEntry>(lhs);
	QueryEntry &rqe = Get<QueryEntry>(rhs);
	if ((lqe->condition == CondEq || lqe->condition == CondSet) && (rqe.condition == CondEq || rqe.condition == CondSet)) {
		// intersect 2 queryenries on same index

		convertWhereValues(lqe);
		std::sort(lqe->values.begin(), lqe->values.end());
		lqe->values.erase(std::unique(lqe->values.begin(), lqe->values.end()), lqe->values.end());

		convertWhereValues(&rqe);
		std::sort(rqe.values.begin(), rqe.values.end());
		rqe.values.erase(std::unique(rqe.values.begin(), rqe.values.end()), rqe.values.end());

		VariantArray setValues;
		setValues.reserve(std::min(lqe->values.size(), rqe.values.size()));
		std::set_intersection(lqe->values.begin(), lqe->values.end(), rqe.values.begin(), rqe.values.end(), std::back_inserter(setValues));
		if (setValues.empty()) {
			container_[lhs].SetValue(AlwaysFalse{});
		} else {
			if (container_[lhs].IsRef()) {
				container_[lhs].SetValue(const_cast<const QueryEntry &>(*lqe));
				lqe = &Get<QueryEntry>(lhs);
			}
			lqe->condition = (setValues.size() == 1) ? CondEq : CondSet;
			lqe->values = std::move(setValues);
			lqe->distinct |= rqe.distinct;
		}
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
		rqe.distinct |= lqe->distinct;
		if (container_[rhs].IsRef()) {
			container_[lhs].SetValue(const_cast<const QueryEntry &>(rqe));
		} else {
			container_[lhs].SetValue(std::move(rqe));
		}
		return true;
	}

	return false;
}

KeyValueType QueryPreprocessor::detectQueryEntryFieldType(const QueryEntry &qentry) const {
	KeyValueType keyType = KeyValueUndefined;
	for (auto &item : ns_.items_) {
		if (!item.IsFree()) {
			Payload pl(ns_.payloadType_, item);
			VariantArray values;
			pl.GetByJsonPath(qentry.index, ns_.tagsMatcher_, values, KeyValueUndefined);
			if (values.size() > 0) keyType = values[0].Type();
			break;
		}
	}
	return keyType;
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
		default:
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
				if (distValue.Type() == KeyValueComposite) {
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
			queryEntry.condition = condition;
			queryEntry.values.emplace_back(qr.aggregationResults[0].value);
			break;
		default:
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
		default:
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
								default:
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
					KeyValueType valuesType = KeyValueUndefined;
					CollateOpts collate;
					if (ns_.getIndexByName(newEntry.index, newEntry.idxNo)) {
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
						fillQueryEntryFromOnCondition(newEntry, *joinedSelector.RightNs(), joinedSelector.JoinQuery(), joinEntry.joinIndex_,
													  condition, valuesType, rdxCtx);
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
