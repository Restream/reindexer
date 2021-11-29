#include "queryentry.h"
#include <cstdlib>
#include <unordered_set>
#include "core/payload/payloadiface.h"
#include "query.h"
#include "tools/serializer.h"
#include "tools/string_regexp_functions.h"
#include "tools/stringstools.h"

namespace reindexer {

bool QueryEntry::operator==(const QueryEntry &obj) const {
	return condition == obj.condition && index == obj.index && idxNo == obj.idxNo && distinct == obj.distinct &&
		   values.RelaxCompare(obj.values) == 0;
}

std::string QueryEntry::Dump() const {
	WrSerializer ser;
	if (distinct) {
		ser << "Distinct index: " << index;
	} else {
		ser << index << ' ' << condition << ' ';
		const bool severalValues = (values.size() > 1);
		if (severalValues) ser << '(';
		for (auto &v : values) {
			if (&v != &*values.begin()) ser << ',';
			ser << '\'' << v.As<std::string>() << '\'';
		}
		if (severalValues) ser << ')';
	}
	return std::string{ser.Slice()};
}

BetweenFieldsQueryEntry::BetweenFieldsQueryEntry(std::string fstIdx, CondType cond, std::string sndIdx)
	: firstIndex{std::move(fstIdx)}, secondIndex{std::move(sndIdx)}, condition_{cond} {
	if (condition_ == CondAny || condition_ == CondEmpty || condition_ == CondDWithin) {
		throw Error{errLogic, "Condition '%s' is inapplicable between two fields", std::string{CondTypeToStr(condition_)}};
	}
}

bool BetweenFieldsQueryEntry::operator==(const BetweenFieldsQueryEntry &other) const noexcept {
	return firstIdxNo == other.firstIdxNo && secondIdxNo == other.secondIdxNo && Condition() == other.Condition() &&
		   firstIndex == other.firstIndex && secondIndex == other.secondIndex;
}

std::string BetweenFieldsQueryEntry::Dump() const {
	WrSerializer ser;
	ser << firstIndex << ' ' << Condition() << ' ' << secondIndex;
	return std::string{ser.Slice()};
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
		if (!HoldsOrReferTo<QueryEntry>(i)) continue;
		const QueryEntry &qe = Get<QueryEntry>(i);
		for (const auto &field : fields) {
			if (qe.index == field) {
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
		const OpType op = it->operation;
		it->InvokeAppropriate<void>(
			[&ser, op, &it](const Bracket &) {
				ser.PutVarUint(QueryOpenBracket);
				ser.PutVarUint(op);
				serialize(it.cbegin(), it.cend(), ser);
				ser.PutVarUint(QueryCloseBracket);
			},
			[&ser, op](const QueryEntry &entry) {
				entry.distinct ? ser.PutVarUint(QueryDistinct) : ser.PutVarUint(QueryCondition);
				ser.PutVString(entry.index);
				if (entry.distinct) return;
				ser.PutVarUint(op);
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
			},
			[&ser, op](const JoinQueryEntry &jqe) {
				ser.PutVarUint(QueryJoinCondition);
				ser.PutVarUint((op == OpAnd) ? JoinType::InnerJoin : JoinType::OrInnerJoin);
				ser.PutVarUint(jqe.joinIndex);
			},
			[&ser, op](const BetweenFieldsQueryEntry &entry) {
				ser.PutVarUint(QueryBetweenFieldsCondition);
				ser.PutVarUint(op);
				ser.PutVString(entry.firstIndex);
				ser.PutVarUint(entry.Condition());
				ser.PutVString(entry.secondIndex);
			},
			[&ser, op](const AlwaysFalse &) {
				ser.PutVarUint(QueryAlwaysFalseCondition);
				ser.PutVarUint(op);
			});
	}
}

bool UpdateEntry::operator==(const UpdateEntry &obj) const noexcept {
	if (isExpression != obj.isExpression) return false;
	if (column != obj.column) return false;
	if (mode != obj.mode) return false;
	if (values != obj.values) return false;
	return true;
}

bool QueryJoinEntry::operator==(const QueryJoinEntry &obj) const noexcept {
	if (op_ != obj.op_) return false;
	if (static_cast<unsigned>(condition_) != obj.condition_) return false;
	if (index_ != obj.index_) return false;
	if (joinIndex_ != obj.joinIndex_) return false;
	if (idxNo != obj.idxNo) return false;
	return true;
}

bool AggregateEntry::operator==(const AggregateEntry &obj) const noexcept {
	return fields_ == obj.fields_ && type_ == obj.type_ && sortingEntries_ == obj.sortingEntries_ && limit_ == obj.limit_ &&
		   offset_ == obj.offset_;
}

bool SortingEntry::operator==(const SortingEntry &obj) const noexcept {
	if (expression != obj.expression) return false;
	if (desc != obj.desc) return false;
	if (index != obj.index) return false;
	return true;
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
		const bool lastResult = it->InvokeAppropriate<bool>(
			[&it, &pl, &tagsMatcher](const Bracket &) { return checkIfSatisfyConditions(it.cbegin(), it.cend(), pl, tagsMatcher); },
			[&pl, &tagsMatcher](const QueryEntry &qe) { return checkIfSatisfyCondition(qe, pl, tagsMatcher); },
			[&pl, &tagsMatcher](const BetweenFieldsQueryEntry &qe) { return checkIfSatisfyCondition(qe, pl, tagsMatcher); },
			[](const JoinQueryEntry &) -> bool { abort(); }, [](const AlwaysFalse &) { return false; });
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
	return checkIfSatisfyCondition(values, qEntry.condition, qEntry.values);
}

bool QueryEntries::checkIfSatisfyCondition(const BetweenFieldsQueryEntry &qEntry, const ConstPayload &pl, TagsMatcher &tagsMatcher) {
	VariantArray lValues;
	if (qEntry.firstIdxNo == IndexValueType::SetByJsonPath) {
		pl.GetByJsonPath(qEntry.firstIndex, tagsMatcher, lValues, KeyValueUndefined);
	} else {
		pl.Get(qEntry.firstIdxNo, lValues);
	}
	VariantArray rValues;
	if (qEntry.secondIdxNo == IndexValueType::SetByJsonPath) {
		pl.GetByJsonPath(qEntry.secondIndex, tagsMatcher, rValues, KeyValueUndefined);
	} else {
		pl.Get(qEntry.secondIdxNo, rValues);
	}
	return checkIfSatisfyCondition(lValues, qEntry.Condition(), rValues);
}

bool QueryEntries::checkIfSatisfyCondition(const VariantArray &lValues, CondType condition, const VariantArray &rValues) {
	switch (condition) {
		case CondType::CondAny:
			return !lValues.empty();
		case CondType::CondEmpty:
			return lValues.empty();
		case CondType::CondEq:
		case CondType::CondSet:
			for (const auto &lhs : lValues) {
				for (const auto &rhs : rValues) {
					if (lhs.RelaxCompare(rhs) == 0) return true;
				}
			}
			return false;
		case CondType::CondAllSet:
			if (lValues.size() < rValues.size()) return false;
			for (const auto &v : rValues) {
				auto it = lValues.cbegin();
				for (; it != lValues.cend(); ++it) {
					if (it->RelaxCompare(v) == 0) break;
				}
				if (it == lValues.cend()) return false;
			}
			return true;
		case CondType::CondLt:
		case CondType::CondLe: {
			auto lit = lValues.cbegin();
			auto rit = rValues.cbegin();
			for (; lit != lValues.cend() && rit != rValues.cend(); ++lit, ++rit) {
				const int res = lit->RelaxCompare(*rit);
				if (res < 0) return true;
				if (res > 0) return false;
			}
			if (lit == lValues.cend() && ((rit == rValues.cend()) == (condition == CondType::CondLe))) return true;
			return false;
		}
		case CondType::CondGt:
		case CondType::CondGe: {
			auto lit = lValues.cbegin();
			auto rit = rValues.cbegin();
			for (; lit != lValues.cend() && rit != rValues.cend(); ++lit, ++rit) {
				const int res = lit->RelaxCompare(*rit);
				if (res > 0) return true;
				if (res < 0) return false;
			}
			if (rit == rValues.cend() && ((lit == lValues.cend()) == (condition == CondType::CondGe))) return true;
			return false;
		}
		case CondType::CondRange:
			if (rValues.size() != 2) throw Error(errParams, "For ranged query reuqired 2 arguments, but provided %d", rValues.size());
			for (const auto &v : lValues) {
				if (v.RelaxCompare(rValues[0]) < 0 || v.RelaxCompare(rValues[1]) > 0) return false;
			}
			return true;
		case CondType::CondLike:
			if (rValues.size() != 1) {
				throw Error(errLogic, "Condition LIKE must have exact 1 value, but %d values was provided", rValues.size());
			}
			if (rValues[0].Type() != KeyValueString) {
				throw Error(errLogic, "Condition LIKE must have value of string type, but %d value was provided", rValues[0].Type());
			}
			for (const auto &v : lValues) {
				if (v.Type() != KeyValueString) {
					throw Error(errLogic, "Condition LIKE must be applied to data of string type, but %d was provided", v.Type());
				}
				if (matchLikePattern(std::string_view(v), std::string_view(rValues[0]))) return true;
			}
			return false;
		case CondType::CondDWithin:
			if (rValues.size() != 2) {
				throw Error(errLogic, "Condition DWithin must have exact 2 value, but %d values was provided", rValues.size());
			}
			Point point;
			double distance;
			if (rValues[0].Type() == KeyValueTuple) {
				point = rValues[0].As<Point>();
				distance = rValues[1].As<double>();
			} else {
				point = rValues[1].As<Point>();
				distance = rValues[0].As<double>();
			}
			return DWithin(static_cast<Point>(lValues), point, distance);
		default:
			assert(0);
	}
	return false;
}

}  // namespace reindexer
