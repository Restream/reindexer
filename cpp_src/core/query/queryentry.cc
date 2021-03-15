#include "queryentry.h"
#include <cstdlib>
#include <unordered_set>
#include "core/payload/payloadiface.h"
#include "query.h"
#include "tools/serializer.h"
#include "tools/string_regexp_functions.h"
#include "tools/stringstools.h"

namespace reindexer {

bool QueryEntry::operator==(const QueryEntry &obj) const noexcept {
	if (condition != obj.condition) return false;
	if (index != obj.index) return false;
	if (idxNo != obj.idxNo) return false;
	if (distinct != obj.distinct) return false;
	if (values != obj.values) return false;
	if (joinIndex != obj.joinIndex) return false;
	return true;
}

template <typename T>
EqualPosition QueryEntries::DetermineEqualPositionIndexes(unsigned start, const T &fields) const {
	if (fields.size() < 2) throw Error(errLogic, "Amount of fields with equal index position should be 2 or more!");
	int fieldIdx = 1;
	h_vector<typename T::value_type, 2> uniqueFields;
	for (const auto &field : fields) {
		for (size_t i = 0; i < uniqueFields.size(); ++i) {
			if (field == uniqueFields[i]) throw Error(errParams, "equal_position() argument [%d] is duplicate", fieldIdx);
		}
		uniqueFields.push_back(field);
		++fieldIdx;
	}
	EqualPosition result;
	for (size_t i = start; i < Size(); ++i) {
		if (!IsValue(i)) continue;
		for (const auto &field : fields) {
			if (operator[](i).index == field) {
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

bool QueryEntries::CheckIfSatisfyConditions(const ConstPayload &pl, TagsMatcher &tm) const {
	return checkIfSatisfyConditions(cbegin(), cend(), pl, tm);
}

void QueryEntries::serialize(const_iterator it, const_iterator to, WrSerializer &ser) {
	for (; it != to; ++it) {
		if (it->IsLeaf()) {
			const QueryEntry &entry = it->Value();
			if (entry.joinIndex == QueryEntry::kNoJoins) {
				entry.distinct ? ser.PutVarUint(QueryDistinct) : ser.PutVarUint(QueryCondition);
				ser.PutVString(entry.index);
				if (entry.distinct) continue;
				ser.PutVarUint(it->operation);
				ser.PutVarUint(entry.condition);
				if (entry.condition == CondDWithin) {
					if (entry.values.size() != 2) {
						throw Error(errLogic, "Condition DWithin must have exact 2 value, but %d values was provided", entry.values.size());
					}
					ser.PutVarUint(3);
					if (entry.values[0].Type() == KeyValueTuple) {
						const Point point = static_cast<Point>(entry.values[0]);
						ser.PutDouble(point.x);
						ser.PutDouble(point.y);
						ser.PutVariant(entry.values[1]);
					} else {
						const Point point = static_cast<Point>(entry.values[1]);
						ser.PutDouble(point.x);
						ser.PutDouble(point.y);
						ser.PutVariant(entry.values[0]);
					}
				} else {
					ser.PutVarUint(entry.values.size());
					for (auto &kv : entry.values) ser.PutVariant(kv);
				}
			} else {
				ser.PutVarUint(QueryJoinCondition);
				ser.PutVarUint((it->operation == OpAnd) ? JoinType::InnerJoin : JoinType::OrInnerJoin);
				ser.PutVarUint(entry.joinIndex);
			}
		} else {
			ser.PutVarUint(QueryOpenBracket);
			ser.PutVarUint(it->operation);
			serialize(it.cbegin(), it.cend(), ser);
			ser.PutVarUint(QueryCloseBracket);
		}
	}
}

bool UpdateEntry::operator==(const UpdateEntry &obj) const {
	if (isExpression != obj.isExpression) return false;
	if (column != obj.column) return false;
	if (mode != obj.mode) return false;
	if (values != obj.values) return false;
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
	if (expression != obj.expression) return false;
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

bool QueryEntries::checkIfSatisfyConditions(const_iterator begin, const_iterator end, const ConstPayload &pl, TagsMatcher &tagsMatcher) {
	assert(begin != end && begin->operation != OpOr);
	bool result = true;
	for (auto it = begin; it != end; ++it) {
		if (it->operation == OpOr) {
			if (result) continue;
		} else if (!result) {
			break;
		}
		bool lastResult;
		if (it->IsLeaf()) {
			lastResult = checkIfSatisfyCondition(it->Value(), pl, tagsMatcher);
		} else {
			lastResult = checkIfSatisfyConditions(it.cbegin(), it.cend(), pl, tagsMatcher);
		}
		result = (lastResult != (it->operation == OpNot));
	}
	return result;
}

bool QueryEntries::checkIfSatisfyCondition(const QueryEntry &qEntry, const ConstPayload &pl, TagsMatcher &tagsMatcher) {
	VariantArray values;
	if (qEntry.idxNo == IndexValueType::SetByJsonPath) {
		pl.GetByJsonPath(qEntry.index, tagsMatcher, values, KeyValueUndefined);
	} else {
		pl.Get(qEntry.idxNo, values);
	}
	switch (qEntry.condition) {
		case CondType::CondAny:
			return !values.empty();
		case CondType::CondEmpty:
			return values.empty();
		case CondType::CondEq:
			if (values.size() > qEntry.values.size()) return false;
			for (const auto &v : values) {
				auto it = qEntry.values.cbegin();
				for (; it != qEntry.values.cend(); ++it) {
					if (it->RelaxCompare(v) == 0) break;
				}
				if (it == qEntry.values.cend()) return false;
			}
			return true;
		case CondType::CondSet:
			for (const auto &lhs : values) {
				for (const auto &rhs : qEntry.values) {
					if (lhs.RelaxCompare(rhs) == 0) return true;
				}
			}
			return false;
		case CondType::CondAllSet:
			if (values.size() < qEntry.values.size()) return false;
			for (const auto &v : qEntry.values) {
				auto it = values.cbegin();
				for (; it != values.cend(); ++it) {
					if (it->RelaxCompare(v) == 0) break;
				}
				if (it == values.cend()) return false;
			}
			return true;
		case CondType::CondLt:
		case CondType::CondLe: {
			auto lit = values.cbegin();
			auto rit = qEntry.values.cbegin();
			for (; lit != values.cend() && rit != qEntry.values.cend(); ++lit, ++rit) {
				const int res = lit->RelaxCompare(*rit);
				if (res < 0) return true;
				if (res > 0) return false;
			}
			if (lit == values.cend() && ((rit == qEntry.values.cend()) == (qEntry.condition == CondType::CondLe))) return true;
			return false;
		}
		case CondType::CondGt:
		case CondType::CondGe: {
			auto lit = values.cbegin();
			auto rit = qEntry.values.cbegin();
			for (; lit != values.cend() && rit != qEntry.values.cend(); ++lit, ++rit) {
				const int res = lit->RelaxCompare(*rit);
				if (res > 0) return true;
				if (res < 0) return false;
			}
			if (rit == qEntry.values.cend() && ((lit == values.cend()) == (qEntry.condition == CondType::CondGe))) return true;
			return false;
		}
		case CondType::CondRange:
			if (qEntry.values.size() != 2)
				throw Error(errParams, "For ranged query reuqired 2 arguments, but provided %d", qEntry.values.size());
			for (const auto &v : values) {
				if (v.RelaxCompare(qEntry.values[0]) < 0 || v.RelaxCompare(qEntry.values[1]) > 0) return false;
			}
			return true;
		case CondType::CondLike:
			if (qEntry.values.size() != 1) {
				throw Error(errLogic, "Condition LIKE must have exact 1 value, but %d values was provided", qEntry.values.size());
			}
			if (qEntry.values[0].Type() != KeyValueString) {
				throw Error(errLogic, "Condition LIKE must have value of string type, but %d value was provided", qEntry.values[0].Type());
			}
			for (const auto &v : values) {
				if (v.Type() != KeyValueString) {
					throw Error(errLogic, "Condition LIKE must be applied to data of string type, but %d was provided", v.Type());
				}
				if (matchLikePattern(string_view(v), string_view(qEntry.values[0]))) return true;
			}
			return false;
		case CondType::CondDWithin:
			if (qEntry.values.size() != 2) {
				throw Error(errLogic, "Condition DWithin must have exact 2 value, but %d values was provided", qEntry.values.size());
			}
			Point point;
			double distance;
			if (qEntry.values[0].Type() == KeyValueTuple) {
				point = qEntry.values[0].As<Point>();
				distance = qEntry.values[1].As<double>();
			} else {
				point = qEntry.values[1].As<Point>();
				distance = qEntry.values[0].As<double>();
			}
			return DWithin(static_cast<Point>(values), point, distance);
		default:
			assert(0);
	}
	return false;
}

}  // namespace reindexer
