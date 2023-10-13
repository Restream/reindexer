#include "queryentry.h"

#include <cstdlib>
#include <unordered_set>
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/joinedselectormock.h"
#include "core/payload/payloadiface.h"
#include "query.h"
#include "tools/serializer.h"
#include "tools/string_regexp_functions.h"
#include "tools/stringstools.h"

namespace reindexer {

template <typename JS>
std::string JoinQueryEntry::Dump(const std::vector<JS> &joinedSelectors) const {
	WrSerializer ser;
	const auto &js = joinedSelectors.at(joinIndex);
	const auto &q = js.JoinQuery();
	ser << js.Type() << " (" << q.GetSQL() << ") ON ";
	ser << '(';
	for (const auto &jqe : q.joinEntries_) {
		if (&jqe != &q.joinEntries_.front()) {
			ser << ' ' << jqe.op_ << ' ';
		} else {
			assertrx(jqe.op_ == OpAnd);
		}
		ser << q._namespace << '.' << jqe.joinIndex_ << ' ' << InvertJoinCondition(jqe.condition_) << ' ' << jqe.index_;
	}
	ser << ')';
	return std::string{ser.Slice()};
}
template std::string JoinQueryEntry::Dump(const JoinedSelectors &) const;
template std::string JoinQueryEntry::Dump(const std::vector<JoinedSelectorMock> &) const;

template <typename JS>
std::string JoinQueryEntry::DumpOnCondition(const std::vector<JS> &joinedSelectors) const {
	WrSerializer ser;
	const auto &js = joinedSelectors.at(joinIndex);
	const auto &q = js.JoinQuery();
	ser << js.Type() << " ON (";
	for (const auto &jqe : q.joinEntries_) {
		if (&jqe != &q.joinEntries_.front()) {
			ser << ' ' << jqe.op_ << ' ';
		}
		ser << q._namespace << '.' << jqe.joinIndex_ << ' ' << InvertJoinCondition(jqe.condition_) << ' ' << jqe.index_;
	}
	ser << ')';
	return std::string{ser.Slice()};
}
template std::string JoinQueryEntry::DumpOnCondition(const JoinedSelectors &) const;

bool QueryEntry::operator==(const QueryEntry &obj) const {
	return condition == obj.condition && index == obj.index && idxNo == obj.idxNo && distinct == obj.distinct &&
		   values.RelaxCompare<WithString::Yes>(obj.values) == 0;
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

std::string QueryEntry::DumpBrief() const {
	WrSerializer ser;
	{
		ser << index << ' ' << condition << ' ';
		const bool severalValues = (values.size() > 1);
		if (severalValues) {
			ser << "(...)";
		} else {
			ser << '\'' << values.front().As<std::string>() << '\'';
		}
	}
	return std::string(ser.Slice());
}

AggregateEntry::AggregateEntry(AggType type, h_vector<std::string, 1> fields, SortingEntries sort, unsigned limit, unsigned offset)
	: type_(type), fields_(std::move(fields)), sortingEntries_{std::move(sort)}, limit_(limit), offset_(offset) {
	switch (type_) {
		case AggFacet:
			if (fields_.empty()) {
				throw Error(errQueryExec, "Empty set of fields for aggregation %s", AggTypeToStr(type_));
			}
			break;
		case AggDistinct:
		case AggMin:
		case AggMax:
		case AggSum:
		case AggAvg:
			if (fields_.size() != 1) {
				throw Error{errQueryExec, "For aggregation %s is available exactly one field", AggTypeToStr(type_)};
			}
			break;
		case AggCount:
		case AggCountCached:
			if (!fields_.empty()) {
				throw Error(errQueryExec, "Not empty set of fields for aggregation %s", AggTypeToStr(type_));
			}
			break;
		case AggUnknown:
			throw Error{errQueryExec, "Unknown aggregation type"};
	}
	switch (type_) {
		case AggDistinct:
		case AggMin:
		case AggMax:
		case AggSum:
		case AggAvg:
		case AggCount:
		case AggCountCached:
			if (limit_ != QueryEntry::kDefaultLimit || offset_ != QueryEntry::kDefaultOffset) {
				throw Error(errQueryExec, "Limit or offset are not available for aggregation %s", AggTypeToStr(type_));
			}
			if (!sortingEntries_.empty()) {
				throw Error(errQueryExec, "Sort is not available for aggregation %s", AggTypeToStr(type_));
			}
			break;
		case AggUnknown:
			throw Error{errQueryExec, "Unknown aggregation type"};
		case AggFacet:
			break;
	}
}

void AggregateEntry::AddSortingEntry(SortingEntry sorting) {
	if (type_ != AggFacet) {
		throw Error(errQueryExec, "Sort is not available for aggregation %s", AggTypeToStr(type_));
	}
	sortingEntries_.emplace_back(std::move(sorting));
}

void AggregateEntry::SetLimit(unsigned l) {
	if (type_ != AggFacet) {
		throw Error(errQueryExec, "Limit or offset are not available for aggregation %s", AggTypeToStr(type_));
	}
	limit_ = l;
}

void AggregateEntry::SetOffset(unsigned o) {
	if (type_ != AggFacet) {
		throw Error(errQueryExec, "Limit or offset are not available for aggregation %s", AggTypeToStr(type_));
	}
	offset_ = o;
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

void QueryEntries::serialize(const_iterator it, const_iterator to, WrSerializer &ser) {
	for (; it != to; ++it) {
		const OpType op = it->operation;
		it->InvokeAppropriate<void>(
			[&ser, op, &it](const QueryEntriesBracket &) {
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
					if (entry.values[0].Type().Is<KeyValueType::Tuple>()) {
						const Point point = static_cast<Point>(entry.values[0]);
						ser.PutDouble(point.X());
						ser.PutDouble(point.Y());
						ser.PutVariant(entry.values[1]);
					} else {
						const Point point = static_cast<Point>(entry.values[1]);
						ser.PutDouble(point.X());
						ser.PutDouble(point.Y());
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
	return isExpression_ == obj.isExpression_ && column_ == obj.column_ && mode_ == obj.mode_ && values_ == obj.values_;
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
	assertrx(begin != end && begin->operation != OpOr);
	bool result = true;
	for (auto it = begin; it != end; ++it) {
		if (it->operation == OpOr) {
			if (result) continue;
		} else if (!result) {
			break;
		}
		const bool lastResult = it->InvokeAppropriate<bool>(
			[&it, &pl, &tagsMatcher](const QueryEntriesBracket &) {
				return checkIfSatisfyConditions(it.cbegin(), it.cend(), pl, tagsMatcher);
			},
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
		pl.GetByJsonPath(qEntry.index, tagsMatcher, values, KeyValueType::Undefined{});
	} else {
		pl.Get(qEntry.idxNo, values);
	}
	return checkIfSatisfyCondition(values, qEntry.condition, qEntry.values);
}

bool QueryEntries::checkIfSatisfyCondition(const BetweenFieldsQueryEntry &qEntry, const ConstPayload &pl, TagsMatcher &tagsMatcher) {
	VariantArray lValues;
	if (qEntry.firstIdxNo == IndexValueType::SetByJsonPath) {
		pl.GetByJsonPath(qEntry.firstIndex, tagsMatcher, lValues, KeyValueType::Undefined{});
	} else {
		pl.Get(qEntry.firstIdxNo, lValues);
	}
	VariantArray rValues;
	if (qEntry.secondIdxNo == IndexValueType::SetByJsonPath) {
		pl.GetByJsonPath(qEntry.secondIndex, tagsMatcher, rValues, KeyValueType::Undefined{});
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
					if (lhs.RelaxCompare<WithString::Yes>(rhs) == 0) return true;
				}
			}
			return false;
		case CondType::CondAllSet:
			if (lValues.size() < rValues.size()) return false;
			for (const auto &v : rValues) {
				auto it = lValues.cbegin();
				for (; it != lValues.cend(); ++it) {
					if (it->RelaxCompare<WithString::Yes>(v) == 0) break;
				}
				if (it == lValues.cend()) return false;
			}
			return true;
		case CondType::CondLt:
		case CondType::CondLe: {
			auto lit = lValues.cbegin();
			auto rit = rValues.cbegin();
			for (; lit != lValues.cend() && rit != rValues.cend(); ++lit, ++rit) {
				const int res = lit->RelaxCompare<WithString::Yes>(*rit);
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
				const int res = lit->RelaxCompare<WithString::Yes>(*rit);
				if (res > 0) return true;
				if (res < 0) return false;
			}
			if (rit == rValues.cend() && ((lit == lValues.cend()) == (condition == CondType::CondGe))) return true;
			return false;
		}
		case CondType::CondRange:
			if (rValues.size() != 2) throw Error(errParams, "For ranged query reuqired 2 arguments, but provided %d", rValues.size());
			for (const auto &v : lValues) {
				if (v.RelaxCompare<WithString::Yes>(rValues[0]) < 0 || v.RelaxCompare<WithString::Yes>(rValues[1]) > 0) return false;
			}
			return true;
		case CondType::CondLike:
			if (rValues.size() != 1) {
				throw Error(errLogic, "Condition LIKE must have exact 1 value, but %d values was provided", rValues.size());
			}
			if (!rValues[0].Type().Is<KeyValueType::String>()) {
				throw Error(errLogic, "Condition LIKE must have value of string type, but %s value was provided", rValues[0].Type().Name());
			}
			for (const auto &v : lValues) {
				if (!v.Type().Is<KeyValueType::String>()) {
					throw Error(errLogic, "Condition LIKE must be applied to data of string type, but %s was provided", v.Type().Name());
				}
				if (matchLikePattern(std::string_view(v), std::string_view(rValues[0]))) return true;
			}
			return false;
		case CondType::CondDWithin: {
			if (rValues.size() != 2) {
				throw Error(errLogic, "Condition DWithin must have exact 2 value, but %d values was provided", rValues.size());
			}
			Point point;
			double distance;
			if (rValues[0].Type().Is<KeyValueType::Tuple>()) {
				point = rValues[0].As<Point>();
				distance = rValues[1].As<double>();
			} else {
				point = rValues[1].As<Point>();
				distance = rValues[0].As<double>();
			}
			return DWithin(static_cast<Point>(lValues), point, distance);
		}
		default:
			assertrx(0);
	}
	return false;
}

}  // namespace reindexer
