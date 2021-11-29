#include "querypreprocessor.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/nsselecter/sortexpression.h"
#include "core/payload/fieldsset.h"
#include "nsselecter.h"

namespace reindexer {

QueryPreprocessor::QueryPreprocessor(QueryEntries &&queries, const Query &query, NamespaceImpl *ns, bool reqMatchedOnce)
	: QueryEntries(std::move(queries)),
	  ns_(*ns),
	  strictMode_(query.strictMode == StrictModeNotSet ? ns_.config_.strictMode : query.strictMode),
	  start_(query.start),
	  count_(query.count),
	  forcedSortOrder_(!query.forcedSortOrder_.empty()),
	  reqMatchedOnce_(reqMatchedOnce) {
	if (forcedSortOrder_ && (start_ > 0 || count_ < UINT_MAX)) {
		assert(!query.sortingEntries_.empty());
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

size_t QueryPreprocessor::lookupQueryIndexes(size_t dst, size_t srcBegin, size_t srcEnd) {
	assert(dst <= srcBegin);
	int iidx[maxIndexes];
	std::fill(iidx, iidx + maxIndexes, -1);
	size_t merged = 0;
	for (size_t src = srcBegin, nextSrc; src < srcEnd; src = nextSrc) {
		nextSrc = Next(src);
		container_[src].InvokeAppropriate<void>(
			[&](const Bracket &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
				const size_t mergedInBracket = lookupQueryIndexes(dst + 1, src + 1, nextSrc);
				container_[dst].Value<Bracket>().Erase(mergedInBracket);
				merged += mergedInBracket;
			},
			[&](QueryEntry &entry) {
				if (entry.idxNo == IndexValueType::NotSet) {
					if (!ns_.getIndexByName(entry.index, entry.idxNo)) {
						entry.idxNo = IndexValueType::SetByJsonPath;
					}
				}
				checkStrictMode(entry.index, entry.idxNo);

				const bool isIndexField = (entry.idxNo != IndexValueType::SetByJsonPath);
				if (isIndexField) {
					// try merge entries with AND opetator
					if ((GetOperation(src) == OpAnd) && (nextSrc >= srcEnd || GetOperation(nextSrc) != OpOr)) {
						if (iidx[entry.idxNo] >= 0 && !ns_.indexes_[entry.idxNo]->Opts().IsArray()) {
							if (mergeQueryEntries(iidx[entry.idxNo], src)) {
								++merged;
								return;
							}
						} else {
							iidx[entry.idxNo] = dst;
						}
					}
				}
				if (dst != src) container_[dst] = std::move(container_[src]);
			},
			[dst, src, this](JoinQueryEntry &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
			},
			[dst, src, this](BetweenFieldsQueryEntry &entry) {
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
				if (dst != src) container_[dst] = std::move(container_[src]);
			},
			[dst, src, this](AlwaysFalse &) {
				if (dst != src) container_[dst] = std::move(container_[src]);
			});
		dst = Next(dst);
	}
	return merged;
}

bool QueryPreprocessor::ContainsFullTextIndexes() const {
	for (auto it = cbegin().PlainIterator(), end = cend().PlainIterator(); it != end; ++it) {
		if (it->HoldsOrReferTo<QueryEntry>() && it->Value<QueryEntry>().idxNo != IndexValueType::SetByJsonPath &&
			isFullText(ns_.indexes_[it->Value<QueryEntry>().idxNo]->Type())) {
			return true;
		}
	}
	return false;
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

	assert(n >= 0 && n < static_cast<int>(values.size()));
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

size_t QueryPreprocessor::substituteCompositeIndexes(size_t from, size_t to) {
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
		if ((found >= 0) && !isFullText(ns_.indexes_[found]->Type())) {
			// composite idx found: replace conditions
			h_vector<std::pair<int, VariantArray>, 4> values;
			CondType condition = CondEq;
			for (size_t i = first; i <= cur; i = Next(i)) {
				if (ns_.indexes_[found]->Fields().contains(Get<QueryEntry>(i).idxNo)) {
					values.emplace_back(Get<QueryEntry>(i).idxNo, std::move(Get<QueryEntry>(i).values));
					if (values.back().second.size() > 1) condition = CondSet;
				} else {
					SetOperation(GetOperation(i), first);
					container_[first] = container_[i];
					first = Next(first);
				}
			}
			{
				QueryEntry ce(condition, ns_.indexes_[found]->Name(), found);
				createCompositeKeyValues(values, ns_.payloadType_, nullptr, ce.values, 0);
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
			[this, &it](const Bracket &) { convertWhereValues(it.begin(), it.end()); },
			[this](QueryEntry &qe) { convertWhereValues(&qe); });
	}
}

SortingEntries QueryPreprocessor::DetectOptimalSortOrder() const {
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
			[this, &it](const Bracket &) { return findMaxIndex(it.cbegin(), it.cend()); },
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
		if (container_[lhs].IsRef()) {
			container_[lhs].SetValue(const_cast<const QueryEntry &>(*lqe));
			lqe = &Get<QueryEntry>(lhs);
		}
		lqe->condition = (setValues.size() == 1) ? CondEq : CondSet;
		lqe->values = std::move(setValues);
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
		assert(ag.Names().size() == 1);
		qe.index = ag.Names()[0];
		qe.condition = CondAny;
		qe.distinct = true;
		Append(wasAdded ? OpOr : OpAnd, std::move(qe));
		wasAdded = true;
	}
}

void QueryPreprocessor::injectConditionsFromJoins(size_t from, size_t to, JoinedSelectors &js, const RdxContext &rdxCtx) {
	for (size_t cur = from; cur < to; cur = Next(cur)) {
		container_[cur].InvokeAppropriate<void>(
			Skip<QueryEntry, BetweenFieldsQueryEntry, AlwaysFalse>{},
			[&js, cur, this, &rdxCtx](const Bracket &) { injectConditionsFromJoins(cur + 1, Next(cur), js, rdxCtx); },
			[&](const JoinQueryEntry &jqe) {
				assert(js.size() > jqe.joinIndex);
				JoinedSelector &joinedSelector = js[jqe.joinIndex];
				if (joinedSelector.PreResult()->dataMode == JoinPreResult::ModeValues) return;
				const auto &rNsCfg = joinedSelector.RightNs()->Config();
				if (rNsCfg.maxPreselectSize == 0 && rNsCfg.maxPreselectPart == 0.0) return;
				assert(joinedSelector.Type() == InnerJoin || joinedSelector.Type() == OrInnerJoin);
				const auto &joinEntries = joinedSelector.JoinQuery().joinEntries_;
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
				for (const auto &joinEntry : joinEntries) {
					if (joinEntry.op_ == OpNot) continue;
					size_t limit;
					if (rNsCfg.maxPreselectSize == 0) {
						limit =
							std::max<int64_t>(rNsCfg.minPreselectSize, joinedSelector.RightNs()->ItemsCount() * rNsCfg.maxPreselectPart);
					} else if (rNsCfg.maxPreselectPart == 0.0) {
						limit = rNsCfg.maxPreselectSize;
					} else {
						limit = std::min(
							std::max<int64_t>(rNsCfg.minPreselectSize, joinedSelector.RightNs()->ItemsCount() * rNsCfg.maxPreselectPart),
							rNsCfg.maxPreselectSize);
					}
					Query query{joinedSelector.JoinQuery()};
					query.count = limit + 2;
					query.start = 0;
					query.sortingEntries_.clear();
					query.forcedSortOrder_.clear();
					query.aggregations_.clear();
					switch (joinEntry.condition_) {
						case CondEq:
						case CondSet:
							query.Distinct(joinEntry.joinIndex_);
							break;
						case CondLt:
						case CondLe:
							query.Aggregate(AggMax, {joinEntry.joinIndex_});
							break;
						case CondGt:
						case CondGe:
							query.Aggregate(AggMin, {joinEntry.joinIndex_});
							break;
						default:
							throw Error(errParams, "Unsupported condition in ON statment: %s", CondTypeToStr(joinEntry.condition_));
					}
					SelectCtx ctx{query};
					QueryResults qr;
					joinedSelector.RightNs()->Select(qr, ctx, rdxCtx);
					if (qr.Count() > limit) continue;
					assert(qr.aggregationResults.size() == 1);
					QueryEntry newEntry;
					newEntry.index = joinEntry.index_;
					if (!ns_.getIndexByName(newEntry.index, newEntry.idxNo)) {
						newEntry.idxNo = IndexValueType::SetByJsonPath;
					}
					switch (joinEntry.condition_) {
						case CondEq:
						case CondSet: {
							assert(qr.aggregationResults[0].type == AggDistinct);
							newEntry.condition = CondSet;
							newEntry.values.reserve(qr.aggregationResults[0].distincts.size());
							assert(qr.aggregationResults[0].distinctsFields.size() == 1);
							const auto field = qr.aggregationResults[0].distinctsFields[0];
							for (Variant &distValue : qr.aggregationResults[0].distincts) {
								if (distValue.Type() == KeyValueComposite) {
									ConstPayload pl(qr.aggregationResults[0].payloadType, distValue.operator const PayloadValue &());
									VariantArray v;
									if (field == IndexValueType::SetByJsonPath) {
										assert(qr.aggregationResults[0].distinctsFields.getTagsPathsLength() == 1);
										pl.GetByJsonPath(qr.aggregationResults[0].distinctsFields.getTagsPath(0), v, KeyValueUndefined);
									} else {
										pl.Get(field, v);
									}
									assert(v.size() == 1);
									newEntry.values.emplace_back(std::move(v[0]));
								} else {
									newEntry.values.emplace_back(std::move(distValue));
								}
							}
							break;
						}
						case CondLt:
						case CondLe:
						case CondGt:
						case CondGe:
							newEntry.condition = joinEntry.condition_;
							newEntry.values.emplace_back(qr.aggregationResults[0].value);
							break;
						default:
							throw Error(errParams, "Unsupported condition in ON statment: %s", CondTypeToStr(joinEntry.condition_));
					}
					Insert(cur, joinEntry.op_, std::move(newEntry));
					++cur;
					++count;
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
