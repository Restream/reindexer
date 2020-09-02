#include "querypreprocessor.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/nsselecter/sortexpression.h"
#include "core/payload/fieldsset.h"

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

size_t QueryPreprocessor::lookupQueryIndexes(size_t dst, size_t srcBegin, size_t srcEnd) {
	assert(dst <= srcBegin);
	int iidx[maxIndexes];
	std::fill(iidx, iidx + maxIndexes, -1);
	size_t merged = 0;
	for (size_t src = srcBegin, nextSrc; src < srcEnd; src = nextSrc) {
		nextSrc = Next(src);
		if (!IsValue(src)) {
			if (dst != src) container_[dst] = std::move(container_[src]);
			const size_t mergedInBracket = lookupQueryIndexes(dst + 1, src + 1, nextSrc);
			container_[dst].Value<Bracket>().Erase(mergedInBracket);
			merged += mergedInBracket;
		} else {
			QueryEntry &entry = container_[src].Value();
			if (entry.idxNo == IndexValueType::NotSet) {
				if (!ns_.getIndexByName(entry.index, entry.idxNo)) {
					entry.idxNo = IndexValueType::SetByJsonPath;
				}
			}

			bool isIndexField = (entry.idxNo != IndexValueType::SetByJsonPath);

			// try merge entries with AND opetator
			if (isIndexField && (GetOperation(src) == OpAnd) && (nextSrc >= srcEnd || GetOperation(nextSrc) != OpOr)) {
				if (iidx[entry.idxNo] >= 0 && !ns_.indexes_[entry.idxNo]->Opts().IsArray()) {
					if (mergeQueryEntries(iidx[entry.idxNo], src)) {
						++merged;
						continue;
					}
				} else {
					iidx[entry.idxNo] = dst;
				}
			}
			if (dst != src) container_[dst] = std::move(container_[src]);
		}
		dst = Next(dst);
	}
	return merged;
}

bool QueryPreprocessor::ContainsFullTextIndexes() const {
	for (auto it = cbegin().PlainIterator(), end = cend().PlainIterator(); it != end; ++it) {
		if (it->IsLeaf() && it->Value().idxNo != IndexValueType::SetByJsonPath && isFullText(ns_.indexes_[it->Value().idxNo]->Type())) {
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
		if (!IsValue(cur) || GetOperation(cur) != OpAnd || (Next(cur) < end && GetOperation(Next(cur)) == OpOr) ||
			((*this)[cur].condition != CondEq && (*this)[cur].condition != CondSet) || (*this)[cur].idxNo >= ns_.payloadType_.NumFields() ||
			(*this)[cur].idxNo < 0) {
			// If query already rewritten, then copy current unmatched part
			first = Next(cur);
			fields.clear();
			if (!IsValue(cur)) {
				deleted += substituteCompositeIndexes(cur + 1, Next(cur));
			}
			continue;
		}
		fields.push_back((*this)[cur].idxNo);
		int found = getCompositeIndex(fields);
		if ((found >= 0) && !isFullText(ns_.indexes_[found]->Type())) {
			// composite idx found: replace conditions
			h_vector<std::pair<int, VariantArray>, 4> values;
			CondType condition = CondEq;
			for (size_t i = first; i <= cur; i = Next(i)) {
				if (ns_.indexes_[found]->Fields().contains((*this)[i].idxNo)) {
					values.emplace_back((*this)[i].idxNo, std::move((*this)[i].values));
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
	bool isIndexField = (qe->idxNo != IndexValueType::SetByJsonPath);
	KeyValueType keyType = isIndexField ? ns_.indexes_[qe->idxNo]->SelectKeyType() : detectQueryEntryIndexType(*qe);
	const FieldsSet *fields = isIndexField ? &ns_.indexes_[qe->idxNo]->Fields() : nullptr;

	if (strictMode_ == StrictModeIndexes && !isIndexField && qe->joinIndex == QueryEntry::kNoJoins) {
		throw Error(errParams,
					"Current query strict mode allows filtering by indexes only. There are no indexes with name '%s' in namespace '%s'",
					qe->index, ns_.name_);
	}

	if (keyType != KeyValueUndefined) {
		for (auto &key : qe->values) {
			key.convert(keyType, &ns_.payloadType_, fields);
		}
	} else if (!isIndexField && qe->joinIndex == QueryEntry::kNoJoins) {
		if (strictMode_ == StrictModeNames && ns_.tagsMatcher_.path2tag(qe->index).empty()) {
			throw Error(
				errParams,
				"Current query strict mode allows filtering by existing fields only. There are no fields with name '%s' in namespace '%s'",
				qe->index, ns_.name_);
		}
	}
}

void QueryPreprocessor::convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const {
	for (auto it = begin; it != end; ++it) {
		if (it->IsLeaf()) {
			convertWhereValues(&it->Value());
		} else {
			convertWhereValues(it.begin(), it.end());
		}
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
		if (it->IsLeaf()) {
			const QueryEntry &entry = it->Value();
			if (((entry.idxNo != IndexValueType::SetByJsonPath) &&
				 (entry.condition == CondGe || entry.condition == CondGt || entry.condition == CondLe || entry.condition == CondLt ||
				  entry.condition == CondRange)) &&
				!entry.distinct && ns_.indexes_[entry.idxNo]->IsOrdered() && !ns_.indexes_[entry.idxNo]->Opts().IsArray()) {
				if (!maxIdx || ns_.indexes_[entry.idxNo]->Size() > maxIdx->Size()) {
					maxIdx = ns_.indexes_[entry.idxNo].get();
				}
			}
		} else {
			const Index *foundIdx = findMaxIndex(it.cbegin(), it.cend());
			if (!maxIdx || (foundIdx && foundIdx->Size() > maxIdx->Size())) {
				maxIdx = foundIdx;
			}
		}
	}
	return maxIdx;
}

bool QueryPreprocessor::mergeQueryEntries(size_t lhs, size_t rhs) {
	QueryEntry *lqe = &container_[lhs].Value();
	QueryEntry &rqe = container_[rhs].Value();
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
		if (!container_[lhs].Holds<QueryEntry>()) {
			container_[lhs].SetValue(static_cast<const QueryEntry &>(*lqe));
			lqe = &container_[lhs].Value();
		}
		lqe->condition = (setValues.size() == 1) ? CondEq : CondSet;
		lqe->values = std::move(setValues);
		lqe->distinct |= rqe.distinct;
		return true;
	} else if (rqe.condition == CondAny) {
		if (!lqe->distinct && rqe.distinct) {
			if (!container_[lhs].Holds<QueryEntry>()) {
				container_[lhs].SetValue(static_cast<const QueryEntry &>(*lqe));
				lqe = &container_[lhs].Value();
			}
			lqe->distinct = true;
		}
		return true;
	} else if (lqe->condition == CondAny) {
		rqe.distinct |= lqe->distinct;
		if (container_[rhs].Holds<QueryEntry>()) {
			container_[lhs].SetValue(std::move(rqe));
		} else {
			container_[lhs].SetValue(const_cast<const QueryEntry &>(rqe));
		}
		return true;
	}

	return false;
}

KeyValueType QueryPreprocessor::detectQueryEntryIndexType(const QueryEntry &qentry) const {
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

}  // namespace reindexer
