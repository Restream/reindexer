#include "queryentry.h"

#include <cstdlib>
#include <unordered_set>
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/joinedselectormock.h"
#include "core/payload/payloadiface.h"
#include "query.h"
#include "tools/serializer.h"
#include "tools/string_regexp_functions.h"

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
			ser << ' ' << jqe.Operation() << ' ';
		} else {
			assertrx(jqe.Operation() == OpAnd);
		}
		ser << q.NsName() << '.' << jqe.RightFieldName() << ' ' << InvertJoinCondition(jqe.Condition()) << ' ' << jqe.LeftFieldName();
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
			ser << ' ' << jqe.Operation() << ' ';
		}
		ser << q.NsName() << '.' << jqe.RightFieldName() << ' ' << InvertJoinCondition(jqe.Condition()) << ' ' << jqe.LeftFieldName();
	}
	ser << ')';
	return std::string{ser.Slice()};
}
template std::string JoinQueryEntry::DumpOnCondition(const JoinedSelectors &) const;

bool QueryField::operator==(const QueryField &other) const noexcept {
	if (fieldName_ != other.fieldName_ || idxNo_ != other.idxNo_ || fieldsSet_ != other.fieldsSet_ ||
		!fieldType_.IsSame(other.fieldType_) || !selectType_.IsSame(other.selectType_) ||
		compositeFieldsTypes_.size() != other.compositeFieldsTypes_.size()) {
		return false;
	}
	for (size_t i = 0, s = compositeFieldsTypes_.size(); i < s; ++i) {
		if (!compositeFieldsTypes_[i].IsSame(other.compositeFieldsTypes_[i])) {
			return false;
		}
	}
	return true;
}

void QueryField::SetField(FieldsSet &&fields) & {
	assertrx_throw(fields.size() == 1);
	assertrx_throw(fields[0] == IndexValueType::SetByJsonPath);
	assertrx_throw(idxNo_ == IndexValueType::NotSet);
	idxNo_ = IndexValueType::SetByJsonPath;
	fieldsSet_ = std::move(fields);
}

static void checkIndexData([[maybe_unused]] int idxNo, [[maybe_unused]] const FieldsSet &fields, KeyValueType fieldType,
						   [[maybe_unused]] const std::vector<KeyValueType> &compositeFieldsTypes) {
	assertrx_throw(idxNo >= 0);
	if (fieldType.Is<KeyValueType::Composite>()) {
		assertrx_throw(fields.size() == compositeFieldsTypes.size());
	} else {
		assertrx_throw(fields.size() == 1);
		assertrx_throw(compositeFieldsTypes.empty());
	}
}

void QueryField::SetIndexData(int idxNo, FieldsSet &&fields, KeyValueType fieldType, KeyValueType selectType,
							  std::vector<KeyValueType> &&compositeFieldsTypes) & {
	checkIndexData(idxNo, fields, fieldType, compositeFieldsTypes);
	idxNo_ = idxNo;
	fieldsSet_ = std::move(fields);
	fieldType_ = fieldType;
	selectType_ = selectType;
	compositeFieldsTypes_ = std::move(compositeFieldsTypes);
}

bool QueryField::HaveEmptyField() const noexcept {
	size_t tagsNo = 0;
	for (auto f : Fields()) {
		if (f == IndexValueType::SetByJsonPath) {
			if (Fields().getTagsPath(tagsNo).empty()) {
				return true;
			}
			++tagsNo;
		}
	}
	return Fields().empty();
}

bool QueryEntry::operator==(const QueryEntry &other) const noexcept {
	return QueryField::operator==(other) && condition_ == other.condition_ && distinct_ == other.distinct_ &&
		   values_.RelaxCompare<WithString::Yes>(other.values_) == 0;
}

template <unsigned flags>
void QueryEntry::verify(CondType cond, const VariantArray &values) {
	if constexpr (flags & kIgnoreEmptyValues) {
		if (values.empty()) {
			return;
		}
	}
	const auto checkArgsCount = [&](size_t argsCountReq) {
		if (values.size() != argsCountReq) {
			throw Error{errLogic, "Condition %s must have exact %d argument, but %d arguments was provided", CondTypeToStr(cond),
						argsCountReq, values.size()};
		}
	};
	switch (cond) {
		case CondEq:
		case CondSet:
		case CondAllSet:
			break;
		case CondAny:
		case CondEmpty:
			if (!values.empty() && !(values.size() == 1 && values[0].Type().Is<KeyValueType::Null>())) {
				throw Error{errLogic, "Condition %s must have no argument or single null argument, but %d not null arguments was provided",
							CondTypeToStr(cond), values.size()};
			}
			break;
		case CondGe:
		case CondGt:
		case CondLt:
		case CondLe:
			checkArgsCount(1);
			break;
		case CondLike:
			checkArgsCount(1);
			if (!values[0].Type().Is<KeyValueType::String>()) {
				throw Error{errLogic, "Condition %s must have string argument, but %d argument was provided", CondTypeToStr(cond),
							values[0].Type().Name()};
			}
			break;
		case CondRange:
		case CondDWithin:
			checkArgsCount(2);
			break;
	}
}
template void QueryEntry::verify<0u>(CondType, const VariantArray &);
template void QueryEntry::verify<QueryEntry::kIgnoreEmptyValues>(CondType, const VariantArray &);

std::string QueryEntry::Dump() const {
	WrSerializer ser;
	if (Distinct()) {
		ser << "Distinct index: " << FieldName();
	} else {
		ser << FieldName() << ' ' << condition_ << ' ';
		const bool severalValues = (Values().size() > 1);
		if (severalValues) ser << '(';
		for (auto &v : Values()) {
			if (&v != &*Values().begin()) ser << ',';
			ser << '\'' << v.As<std::string>() << '\'';
		}
		if (severalValues) ser << ')';
	}
	return std::string{ser.Slice()};
}

std::string QueryEntry::DumpBrief() const {
	WrSerializer ser;
	{
		ser << FieldName() << ' ' << Condition() << ' ';
		const bool severalValues = (Values().size() > 1);
		if (severalValues) {
			ser << "(...)";
		} else {
			ser << '\'' << Values().front().As<std::string>() << '\'';
		}
	}
	return std::string(ser.Slice());
}

AggregateEntry::AggregateEntry(AggType type, h_vector<std::string, 1> &&fields, SortingEntries &&sort, unsigned limit, unsigned offset)
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

void AggregateEntry::AddSortingEntry(SortingEntry &&sorting) {
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

bool BetweenFieldsQueryEntry::operator==(const BetweenFieldsQueryEntry &other) const noexcept {
	return leftField_ == other.leftField_ && rightField_ == other.rightField_ && Condition() == other.Condition();
}

std::string BetweenFieldsQueryEntry::Dump() const {
	WrSerializer ser;
	ser << LeftFieldName() << ' ' << Condition() << ' ' << RightFieldName();
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
				entry.Distinct() ? ser.PutVarUint(QueryDistinct) : ser.PutVarUint(QueryCondition);
				ser.PutVString(entry.FieldName());
				if (entry.Distinct()) return;
				ser.PutVarUint(op);
				ser.PutVarUint(entry.Condition());
				if (entry.Condition() == CondDWithin) {
					if (entry.Values().size() != 2) {
						throw Error(errLogic, "Condition DWithin must have exact 2 value, but %d values was provided",
									entry.Values().size());
					}
					ser.PutVarUint(3);
					if (entry.Values()[0].Type().Is<KeyValueType::Tuple>()) {
						const Point point = static_cast<Point>(entry.Values()[0]);
						ser.PutDouble(point.X());
						ser.PutDouble(point.Y());
						ser.PutVariant(entry.Values()[1]);
					} else {
						const Point point = static_cast<Point>(entry.Values()[1]);
						ser.PutDouble(point.X());
						ser.PutDouble(point.Y());
						ser.PutVariant(entry.Values()[0]);
					}
				} else {
					ser.PutVarUint(entry.Values().size());
					for (auto &kv : entry.Values()) ser.PutVariant(kv);
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
				ser.PutVString(entry.LeftFieldName());
				ser.PutVarUint(entry.Condition());
				ser.PutVString(entry.RightFieldName());
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

bool QueryJoinEntry::operator==(const QueryJoinEntry &other) const noexcept {
	return op_ == other.op_ && condition_ == other.condition_ && leftField_ == other.leftField_ && rightField_ == other.rightField_;
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

bool QueryEntries::checkIfSatisfyConditions(const_iterator begin, const_iterator end, const ConstPayload &pl) {
	assertrx(begin != end && begin->operation != OpOr);
	bool result = true;
	for (auto it = begin; it != end; ++it) {
		if (it->operation == OpOr) {
			if (result) continue;
		} else if (!result) {
			break;
		}
		const bool lastResult = it->InvokeAppropriate<bool>(
			[&it, &pl](const QueryEntriesBracket &) { return checkIfSatisfyConditions(it.cbegin(), it.cend(), pl); },
			[&pl](const QueryEntry &qe) { return checkIfSatisfyCondition(qe, pl); },
			[&pl](const BetweenFieldsQueryEntry &qe) { return checkIfSatisfyCondition(qe, pl); },
			[](const JoinQueryEntry &) -> bool { abort(); }, [](const AlwaysFalse &) { return false; });
		result = (lastResult != (it->operation == OpNot));
	}
	return result;
}

bool QueryEntries::checkIfSatisfyCondition(const QueryEntry &qEntry, const ConstPayload &pl) {
	VariantArray values;
	pl.GetByFieldsSet(qEntry.Fields(), values, qEntry.FieldType(), qEntry.CompositeFieldsTypes());
	return checkIfSatisfyCondition(values, qEntry.Condition(), qEntry.Values());
}

bool QueryEntries::checkIfSatisfyCondition(const BetweenFieldsQueryEntry &qEntry, const ConstPayload &pl) {
	VariantArray lValues;
	pl.GetByFieldsSet(qEntry.LeftFields(), lValues, qEntry.LeftFieldType(), qEntry.LeftCompositeFieldsTypes());
	VariantArray rValues;
	pl.GetByFieldsSet(qEntry.RightFields(), rValues, qEntry.RightFieldType(), qEntry.RightCompositeFieldsTypes());
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
			for (const auto &v : lValues) {
				if (v.RelaxCompare<WithString::Yes>(rValues[0]) < 0 || v.RelaxCompare<WithString::Yes>(rValues[1]) > 0) return false;
			}
			return true;
		case CondType::CondLike:
			for (const auto &v : lValues) {
				if (!v.Type().Is<KeyValueType::String>()) {
					throw Error(errLogic, "Condition LIKE must be applied to data of string type, but %s was provided", v.Type().Name());
				}
				if (matchLikePattern(std::string_view(v), std::string_view(rValues[0]))) return true;
			}
			return false;
		case CondType::CondDWithin: {
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

template <typename JS>
std::string QueryJoinEntry::DumpCondition(const JS &joinedSelector, bool needOp) const {
	WrSerializer ser;
	const auto &q = joinedSelector.JoinQuery();
	if (needOp) {
		ser << ' ' << op_ << ' ';
	}
	ser << q.NsName() << '.' << RightFieldName() << ' ' << InvertJoinCondition(condition_) << ' ' << LeftFieldName();
	return std::string{ser.Slice()};
}
template std::string QueryJoinEntry::DumpCondition(const JoinedSelector &, bool) const;

void QueryEntries::dumpEqualPositions(size_t level, WrSerializer &ser, const EqualPositions_t &equalPositions) {
	for (const auto &eq : equalPositions) {
		for (size_t i = 0; i < level; ++i) {
			ser << "   ";
		}
		ser << "equal_poisition(";
		for (size_t i = 0, s = eq.size(); i < s; ++i) {
			if (i != 0) ser << ", ";
			ser << eq[i];
		}
		ser << ")\n";
	}
}

template <typename JS>
void QueryEntries::dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JS> &joinedSelectors, WrSerializer &ser) {
	for (const_iterator it = begin; it != end; ++it) {
		for (size_t i = 0; i < level; ++i) {
			ser << "   ";
		}
		if (it != begin || it->operation != OpAnd) {
			ser << it->operation << ' ';
		}
		it->InvokeAppropriate<void>(
			[&](const QueryEntriesBracket &b) {
				ser << "(\n";
				dump(level + 1, it.cbegin(), it.cend(), joinedSelectors, ser);
				dumpEqualPositions(level + 1, ser, b.equalPositions);
				for (size_t i = 0; i < level; ++i) {
					ser << "   ";
				}
				ser << ")\n";
			},
			[&ser](const QueryEntry &qe) { ser << qe.Dump() << '\n'; },
			[&joinedSelectors, &ser](const JoinQueryEntry &jqe) { ser << jqe.Dump(joinedSelectors) << '\n'; },
			[&ser](const BetweenFieldsQueryEntry &qe) { ser << qe.Dump() << '\n'; },
			[&ser](const AlwaysFalse &) { ser << "AlwaysFalse" << 'n'; });
	}
}

}  // namespace reindexer
