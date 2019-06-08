#include "querypreprocessor.h"
#include "core/index/index.h"
#include "core/namespace.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/payload/fieldsset.h"

namespace reindexer {

QueryEntries QueryPreprocessor::LookupQueryIndexes() const {
	QueryEntries result;
	result.Reserve(queries_->Size());
	lookupQueryIndexes(&result, queries_->cbegin(), queries_->cend());
	return result;
}

void QueryPreprocessor::lookupQueryIndexes(QueryEntries *dst, QueryEntries::const_iterator srcBegin,
										   QueryEntries::const_iterator srcEnd) const {
	int iidx[maxIndexes];
	for (int &i : iidx) i = -1;
	for (auto it = srcBegin; it != srcEnd; ++it) {
		if (!it->IsLeaf()) {
			dst->OpenBracket(it->Op);
			lookupQueryIndexes(dst, it->cbegin(it), it->cend(it));
			dst->CloseBracket();
		} else {
			QueryEntry entry = it->Value();
			if (entry.idxNo == IndexValueType::NotSet) {
				if (!ns_.getIndexByName(entry.index, entry.idxNo)) {
					entry.idxNo = IndexValueType::SetByJsonPath;
				}
			}

			bool isIndexField = (entry.idxNo != IndexValueType::SetByJsonPath);

			// try merge entries with AND opetator
			const auto nextEntry = it + 1;
			if (isIndexField && (it->Op == OpAnd) && (nextEntry == srcEnd || nextEntry->Op == OpAnd)) {
				if (iidx[entry.idxNo] >= 0 && !ns_.indexes_[entry.idxNo]->Opts().IsArray()) {
					if (mergeQueryEntries(&(*dst)[iidx[entry.idxNo]], &entry)) continue;
				} else {
					iidx[entry.idxNo] = dst->Size();
				}
			}
			dst->Append(it->Op, std::move(entry));
		}
	}
}

bool QueryPreprocessor::ContainsFullTextIndexes() const {
	for (auto it = queries_->cbegin().PlainIterator(), end = queries_->cend().PlainIterator(); it != end; ++it) {
		if ((*it)->IsLeaf() && (*it)->Value().idxNo != IndexValueType::SetByJsonPath &&
			isFullText(ns_.indexes_[(*it)->Value().idxNo]->Type())) {
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

size_t QueryPreprocessor::substituteCompositeIndexes(size_t from, size_t to) const {
	FieldsSet fields;
	size_t deleted = 0;
	for (size_t cur = from, first = from, end = to; cur < end; cur = queries_->Next(cur), end = to - deleted) {
		if (!queries_->IsEntry(cur) || queries_->GetOperation(cur) != OpAnd ||
			(queries_->Next(cur) < end && queries_->GetOperation(queries_->Next(cur)) == OpOr) || (*queries_)[cur].condition != CondEq ||
			(*queries_)[cur].idxNo >= ns_.payloadType_.NumFields() || (*queries_)[cur].idxNo < 0) {
			// If query already rewritten, then copy current unmatched part
			first = queries_->Next(cur);
			fields.clear();
			if (!queries_->IsEntry(cur)) {
				deleted += substituteCompositeIndexes(cur + 1, queries_->Next(cur));
			}
			continue;
		}
		fields.push_back((*queries_)[cur].idxNo);
		int found = getCompositeIndex(fields);
		if ((found >= 0) && !isFullText(ns_.indexes_[found]->Type())) {
			// composite idx found: replace conditions
			PayloadValue d(ns_.payloadType_.TotalSize());
			Payload pl(ns_.payloadType_, d);
			for (size_t i = first; i <= cur; i = queries_->Next(i)) {
				if (ns_.indexes_[found]->Fields().contains((*queries_)[i].idxNo)) {
					pl.Set((*queries_)[i].idxNo, {(*queries_)[i].values[0]});
				} else {
					queries_->SetOperation(queries_->GetOperation(i), first);
					(*queries_)[first] = (*queries_)[i];
					first = queries_->Next(first);
				}
			}
			QueryEntry ce(CondEq, ns_.indexes_[found]->Name(), found);
			ce.values.push_back(Variant(d));
			queries_->SetOperation(OpAnd, first);
			(*queries_)[first] = ce;
			deleted += (queries_->Next(cur) - queries_->Next(first));
			queries_->Erase(queries_->Next(first), queries_->Next(cur));
			cur = first;
			first = queries_->Next(first);
			fields.clear();
		}
	}
	return deleted;
}

void QueryPreprocessor::convertWhereValues(QueryEntry *qe) const {
	bool isIndexField = (qe->idxNo != IndexValueType::SetByJsonPath);
	KeyValueType keyType = isIndexField ? ns_.indexes_[qe->idxNo]->SelectKeyType() : detectQueryEntryIndexType(*qe);
	const FieldsSet *fields = isIndexField ? &ns_.indexes_[qe->idxNo]->Fields() : nullptr;

	if (keyType != KeyValueUndefined) {
		for (auto &key : qe->values) {
			key.convert(keyType, &ns_.payloadType_, fields);
		}
	}
}

void QueryPreprocessor::convertWhereValues(QueryEntries::iterator begin, QueryEntries::iterator end) const {
	for (auto it = begin; it != end; ++it) {
		if (it->IsLeaf()) {
			convertWhereValues(&it->Value());
		} else {
			convertWhereValues(it->begin(it), it->end(it));
		}
	}
}

SortingEntries QueryPreprocessor::DetectOptimalSortOrder() const {
	if (const Index *maxIdx = findMaxIndex(queries_->cbegin(), queries_->cend())) {
		SortingEntries sortingEntries;
		sortingEntries.push_back({maxIdx->Name(), false});
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
			const Index *foundIdx = findMaxIndex(it->cbegin(it), it->cend(it));
			if (!maxIdx || (foundIdx && foundIdx->Size() > maxIdx->Size())) {
				maxIdx = foundIdx;
			}
		}
	}
	return maxIdx;
}

bool QueryPreprocessor::mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs) const {
	if ((lhs->condition == CondEq || lhs->condition == CondSet) && (rhs->condition == CondEq || rhs->condition == CondSet)) {
		// intersect 2 queryenries on same index

		convertWhereValues(lhs);
		std::sort(lhs->values.begin(), lhs->values.end());
		lhs->values.erase(std::unique(lhs->values.begin(), lhs->values.end()), lhs->values.end());

		convertWhereValues(rhs);
		std::sort(rhs->values.begin(), rhs->values.end());
		rhs->values.erase(std::unique(rhs->values.begin(), rhs->values.end()), rhs->values.end());

		auto end =
			std::set_intersection(lhs->values.begin(), lhs->values.end(), rhs->values.begin(), rhs->values.end(), lhs->values.begin());
		lhs->values.resize(end - lhs->values.begin());
		lhs->condition = (lhs->values.size() == 1) ? CondEq : CondSet;
		lhs->distinct |= rhs->distinct;
		return true;
	} else if (rhs->condition == CondAny) {
		lhs->distinct |= rhs->distinct;
		return true;
	} else if (lhs->condition == CondAny) {
		rhs->distinct |= lhs->distinct;
		*lhs = std::move(*rhs);
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

}  // namespace reindexer
