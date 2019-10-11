#include "queryentry.h"
#include <stdlib.h>
#include "query.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

bool QueryEntry::operator==(const QueryEntry &obj) const {
	if (condition != obj.condition) return false;
	if (index != obj.index) return false;
	if (idxNo != obj.idxNo) return false;
	if (distinct != obj.distinct) return false;
	if (values != obj.values) return false;
	if (joinIndex != obj.joinIndex) return false;
	return true;
}

bool QueryEntry::operator!=(const QueryEntry &obj) const { return !operator==(obj); }

template <typename T>
EqualPosition QueryEntries::DetermineEqualPositionIndexes(unsigned start, const T &fields) const {
	if (fields.size() < 2) throw Error(errLogic, "Amount of fields with equal index position should be 2 or more!");
	EqualPosition result;
	for (size_t i = start; i < Size(); i = Next(i)) {
		if (!container_[i]->IsLeaf()) continue;
		for (const auto &f : fields) {
			if (container_[i]->Value().index == f) {
				result.push_back(i);
				break;
			}
		}
	}
	return result;
}

template <typename T>
std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes(const T &fields) const {
	const unsigned start = activeBrackets_.empty() ? 0u : activeBrackets_.back() + 1u;
	return {start, DetermineEqualPositionIndexes(start, fields)};
}

// Explicit instantiations
template EqualPosition QueryEntries::DetermineEqualPositionIndexes<vector<string>>(unsigned, const vector<string> &) const;
template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<vector<string>>(const vector<string> &) const;
template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<h_vector<string, 4>>(
	const h_vector<string, 4> &) const;
template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<std::initializer_list<string>>(
	const std::initializer_list<string> &) const;

void QueryEntries::serialize(const_iterator it, const_iterator to, WrSerializer &ser) {
	for (; it != to; ++it) {
		if (it->IsLeaf()) {
			const QueryEntry &entry = it->Value();
			if (entry.joinIndex == QueryEntry::kNoJoins) {
				entry.distinct ? ser.PutVarUint(QueryDistinct) : ser.PutVarUint(QueryCondition);
				ser.PutVString(entry.index);
				if (entry.distinct) continue;
				ser.PutVarUint(it->Op);
				ser.PutVarUint(entry.condition);
				ser.PutVarUint(entry.values.size());
				for (auto &kv : entry.values) ser.PutVariant(kv);
			} else {
				ser.PutVarUint(QueryJoinCondition);
				ser.PutVarUint((it->Op == OpAnd) ? JoinType::InnerJoin : JoinType::OrInnerJoin);
				ser.PutVarUint(entry.joinIndex);
			}
		} else {
			ser.PutVarUint(QueryOpenBracket);
			ser.PutVarUint(it->Op);
			serialize(it->cbegin(it), it->cend(it), ser);
			ser.PutVarUint(QueryCloseBracket);
		}
	}
}

bool UpdateEntry::operator==(const UpdateEntry &obj) const {
	if (column != obj.column) return false;
	if (values != obj.values) return false;
	if (isExpression != obj.isExpression) return false;
	return true;
}

bool UpdateEntry::operator!=(const UpdateEntry &obj) const { return !operator==(obj); }

bool QueryJoinEntry::operator==(const QueryJoinEntry &obj) const {
	if (op_ != obj.op_) return false;
	if (condition_ != obj.condition_) return false;
	if (index_ != obj.index_) return false;
	if (joinIndex_ != obj.joinIndex_) return false;
	if (idxNo != obj.idxNo) return false;
	return true;
}

bool AggregateEntry::operator==(const AggregateEntry &obj) const {
	return fields_ == obj.fields_ && type_ == obj.type_ && sortingEntries_ == obj.sortingEntries_ && limit_ == obj.limit_ &&
		   offset_ == obj.offset_;
}

bool AggregateEntry::operator!=(const AggregateEntry &obj) const { return !operator==(obj); }

bool SortingEntry::operator==(const SortingEntry &obj) const {
	if (column != obj.column) return false;
	if (desc != obj.desc) return false;
	if (index != obj.index) return false;
	return true;
}

bool SortingEntry::operator!=(const SortingEntry &obj) const { return !operator==(obj); }

const char *condNames[] = {"IS NOT NULL", "=", "<", "<=", ">", ">=", "RANGE", "IN", "ALLSET", "IS NULL", "LIKE"};

string QueryEntry::Dump() const {
	string result;
	if (distinct) {
		result = "Distinct index: " + index;
	} else {
		result = index + ' ';

		if (condition < sizeof(condNames) / sizeof(condNames[0])) result += string(condNames[condition]) + " ";

		bool severalValues = (values.size() > 1);
		if (severalValues) result += "(";
		for (auto &v : values) {
			if (&v != &*values.begin()) result += ",";
			result += "'" + v.As<string>() + "'";
		}
		result += (severalValues) ? ") " : " ";
	}
	return result;
}

}  // namespace reindexer
