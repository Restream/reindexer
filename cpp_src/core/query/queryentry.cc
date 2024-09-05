#include "queryentry.h"

#include <cstdlib>
#include <unordered_set>
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/joinedselectormock.h"
#include "core/payload/payloadiface.h"
#include "core/type_consts.h"
#include "query.h"
#include "tools/serializer.h"
#include "tools/string_regexp_functions.h"

namespace reindexer {

template <typename JS>
std::string JoinQueryEntry::Dump(const std::vector<JS>& joinedSelectors) const {
	WrSerializer ser;
	const auto& js = joinedSelectors.at(joinIndex);
	const auto& q = js.JoinQuery();
	ser << js.Type() << " (" << q.GetSQL() << ") ON ";
	ser << '(';
	for (const auto& jqe : q.joinEntries_) {
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
template std::string JoinQueryEntry::Dump(const JoinedSelectors&) const;
template std::string JoinQueryEntry::Dump(const std::vector<JoinedSelectorMock>&) const;

template <typename JS>
std::string JoinQueryEntry::DumpOnCondition(const std::vector<JS>& joinedSelectors) const {
	WrSerializer ser;
	const auto& js = joinedSelectors.at(joinIndex);
	const auto& q = js.JoinQuery();
	ser << js.Type() << " ON (";
	for (const auto& jqe : q.joinEntries_) {
		if (&jqe != &q.joinEntries_.front()) {
			ser << ' ' << jqe.Operation() << ' ';
		}
		ser << q.NsName() << '.' << jqe.RightFieldName() << ' ' << InvertJoinCondition(jqe.Condition()) << ' ' << jqe.LeftFieldName();
	}
	ser << ')';
	return std::string{ser.Slice()};
}
template std::string JoinQueryEntry::DumpOnCondition(const JoinedSelectors&) const;

bool QueryField::operator==(const QueryField& other) const noexcept {
	if (fieldName_ != other.fieldName_ || idxNo_ != other.idxNo_ || fieldsSet_ != other.fieldsSet_ ||
		!fieldType_.IsSame(other.fieldType_) || !selectType_.IsSame(other.selectType_) ||
		compositeFieldsTypes_.size() != other.compositeFieldsTypes_.size()) {
		return false;
	}
	return std::equal(
		compositeFieldsTypes_.begin(), compositeFieldsTypes_.end(), other.compositeFieldsTypes_.begin(),
		[](const CompositeTypesVecT::value_type& l, const CompositeTypesVecT::value_type& r) noexcept { return l.IsSame(r); });
}

void QueryField::SetField(FieldsSet&& fields) & {
	assertrx_throw(fields.size() == 1);
	assertrx_throw(fields[0] == IndexValueType::SetByJsonPath);
	assertrx_throw(idxNo_ == IndexValueType::NotSet);
	idxNo_ = IndexValueType::SetByJsonPath;
	fieldsSet_ = std::move(fields);
}

static void checkIndexData([[maybe_unused]] int idxNo, [[maybe_unused]] const FieldsSet& fields, KeyValueType fieldType,
						   [[maybe_unused]] const QueryField::CompositeTypesVecT& compositeFieldsTypes) {
	assertrx_throw(idxNo >= 0);
	if (fieldType.Is<KeyValueType::Composite>()) {
		assertrx_throw(fields.size() == compositeFieldsTypes.size());
	} else {
		assertrx_throw(fields.size() == 1);
		assertrx_throw(compositeFieldsTypes.empty());
	}
}

void QueryField::SetIndexData(int idxNo, FieldsSet&& fields, KeyValueType fieldType, KeyValueType selectType,
							  QueryField::CompositeTypesVecT&& compositeFieldsTypes) & {
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

bool QueryEntry::operator==(const QueryEntry& other) const noexcept {
	return QueryField::operator==(other) && condition_ == other.condition_ && distinct_ == other.distinct_ &&
		   values_.RelaxCompare<WithString::Yes, NotComparable::Return>(other.values_) == ComparationResult::Eq;
}

template <VerifyQueryEntryFlags flags>
void VerifyQueryEntryValues(CondType cond, const VariantArray& values) {
	if constexpr (flags & VerifyQueryEntryFlags::ignoreEmptyValues) {
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
				throw Error{errLogic, "Condition %s must have string argument, but %s argument was provided", CondTypeToStr(cond),
							values[0].Type().Name()};
			}
			break;
		case CondRange:
		case CondDWithin:
			checkArgsCount(2);
			break;
	}
}
template void VerifyQueryEntryValues<VerifyQueryEntryFlags::null>(CondType, const VariantArray&);
template void VerifyQueryEntryValues<VerifyQueryEntryFlags::ignoreEmptyValues>(CondType, const VariantArray&);

std::string QueryEntry::Dump() const {
	WrSerializer ser;
	if (Distinct()) {
		ser << "Distinct index: ";
	}
	ser << FieldName() << ' ' << condition_ << ' ';
	const bool severalValues = (Values().size() > 1);
	if (severalValues) {
		ser << '(';
	}
	for (auto& v : Values()) {
		if (&v != &*Values().begin()) {
			ser << ',';
		}
		ser << '\'' << v.As<std::string>() << '\'';
	}
	if (severalValues) {
		ser << ')';
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

AggregateEntry::AggregateEntry(AggType type, h_vector<std::string, 1>&& fields, SortingEntries&& sort, unsigned limit, unsigned offset)
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

void AggregateEntry::AddSortingEntry(SortingEntry&& sorting) {
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

bool BetweenFieldsQueryEntry::operator==(const BetweenFieldsQueryEntry& other) const noexcept {
	return leftField_ == other.leftField_ && rightField_ == other.rightField_ && Condition() == other.Condition();
}

std::string BetweenFieldsQueryEntry::Dump() const {
	WrSerializer ser;
	ser << LeftFieldName() << ' ' << Condition() << ' ' << RightFieldName();
	return std::string{ser.Slice()};
}

void BetweenFieldsQueryEntry::checkCondition(CondType cond) const {
	if (cond == CondAny || cond == CondEmpty || cond == CondDWithin) {
		throw Error{errLogic, "Condition '%s' is inapplicable between two fields", CondTypeToStr(cond)};
	}
}

void QueryEntries::serialize(CondType cond, const VariantArray& values, WrSerializer& ser) {
	ser.PutVarUint(cond);
	if (cond == CondDWithin) {
		assertrx_throw(values.size() == 2);
		ser.PutVarUint(3);
		if (values[0].Type().Is<KeyValueType::Tuple>()) {
			const Point point = static_cast<Point>(values[0]);
			ser.PutDouble(point.X());
			ser.PutDouble(point.Y());
			ser.PutVariant(values[1]);
		} else {
			const Point point = static_cast<Point>(values[1]);
			ser.PutDouble(point.X());
			ser.PutDouble(point.Y());
			ser.PutVariant(values[0]);
		}
	} else {
		ser.PutVarUint(values.size());
		for (auto& kv : values) {
			ser.PutVariant(kv);
		}
	}
}

void QueryEntries::serialize(const_iterator it, const_iterator to, WrSerializer& ser, const std::vector<Query>& subQueries) {
	for (; it != to; ++it) {
		const OpType op = it->operation;
		it->Visit(
			[&ser, op, &subQueries](const SubQueryEntry& sqe) {
				ser.PutVarUint(QuerySubQueryCondition);
				ser.PutVarUint(op);
				{
					const auto sizePosSaver = ser.StartVString();
					subQueries.at(sqe.QueryIndex()).Serialize(ser);
				}
				serialize(sqe.Condition(), sqe.Values(), ser);
			},
			[&ser, op, &subQueries](const SubQueryFieldEntry& sqe) {
				ser.PutVarUint(QueryFieldSubQueryCondition);
				ser.PutVarUint(op);
				ser.PutVString(sqe.FieldName());
				ser.PutVarUint(sqe.Condition());
				{
					const auto sizePosSaver = ser.StartVString();
					subQueries.at(sqe.QueryIndex()).Serialize(ser);
				}
			},
			[&](const QueryEntriesBracket&) {
				ser.PutVarUint(QueryOpenBracket);
				ser.PutVarUint(op);
				serialize(it.cbegin(), it.cend(), ser, subQueries);
				ser.PutVarUint(QueryCloseBracket);
			},
			[&ser, op](const QueryEntry& entry) {
				entry.Distinct() ? ser.PutVarUint(QueryDistinct) : ser.PutVarUint(QueryCondition);
				ser.PutVString(entry.FieldName());
				if (entry.Distinct()) {
					return;
				}
				ser.PutVarUint(op);
				serialize(entry.Condition(), entry.Values(), ser);
			},
			[&ser, op](const JoinQueryEntry& jqe) {
				ser.PutVarUint(QueryJoinCondition);
				ser.PutVarUint((op == OpAnd) ? JoinType::InnerJoin : JoinType::OrInnerJoin);
				ser.PutVarUint(jqe.joinIndex);
			},
			[&ser, op](const BetweenFieldsQueryEntry& entry) {
				ser.PutVarUint(QueryBetweenFieldsCondition);
				ser.PutVarUint(op);
				ser.PutVString(entry.LeftFieldName());
				ser.PutVarUint(entry.Condition());
				ser.PutVString(entry.RightFieldName());
			},
			[&ser, op](const AlwaysFalse&) {
				ser.PutVarUint(QueryAlwaysFalseCondition);
				ser.PutVarUint(op);
			},
			[&ser, op](const AlwaysTrue&) {
				ser.PutVarUint(QueryAlwaysTrueCondition);
				ser.PutVarUint(op);
			});
	}
}

bool UpdateEntry::operator==(const UpdateEntry& obj) const noexcept {
	return isExpression_ == obj.isExpression_ && column_ == obj.column_ && mode_ == obj.mode_ && values_ == obj.values_;
}

bool QueryJoinEntry::operator==(const QueryJoinEntry& other) const noexcept {
	return op_ == other.op_ && condition_ == other.condition_ && leftField_ == other.leftField_ && rightField_ == other.rightField_;
}

bool AggregateEntry::operator==(const AggregateEntry& obj) const noexcept {
	return fields_ == obj.fields_ && type_ == obj.type_ && sortingEntries_ == obj.sortingEntries_ && limit_ == obj.limit_ &&
		   offset_ == obj.offset_;
}

bool SortingEntry::operator==(const SortingEntry& obj) const noexcept {
	if (expression != obj.expression) {
		return false;
	}
	if (desc != obj.desc) {
		return false;
	}
	if (index != obj.index) {
		return false;
	}
	return true;
}

bool QueryEntries::checkIfSatisfyConditions(const_iterator begin, const_iterator end, const ConstPayload& pl) {
	assertrx_dbg(begin != end && begin->operation != OpOr);
	bool result = true;
	for (auto it = begin; it != end; ++it) {
		if (it->operation == OpOr) {
			if (result) {
				continue;
			}
		} else if (!result) {
			break;
		}
		const bool lastResult = it->Visit(
			[] RX_PRE_LMBD_ALWAYS_INLINE(const SubQueryEntry&) RX_POST_LMBD_ALWAYS_INLINE -> bool {
				assertrx_throw(0);
				abort();
			},
			[] RX_PRE_LMBD_ALWAYS_INLINE(const SubQueryFieldEntry&) RX_POST_LMBD_ALWAYS_INLINE -> bool {
				assertrx_throw(0);
				abort();
			},
			[&it, &pl] RX_PRE_LMBD_ALWAYS_INLINE(const QueryEntriesBracket&)
				RX_POST_LMBD_ALWAYS_INLINE { return checkIfSatisfyConditions(it.cbegin(), it.cend(), pl); },
			[&pl] RX_PRE_LMBD_ALWAYS_INLINE(const QueryEntry& qe) RX_POST_LMBD_ALWAYS_INLINE { return checkIfSatisfyCondition(qe, pl); },
			[&pl] RX_PRE_LMBD_ALWAYS_INLINE(const BetweenFieldsQueryEntry& qe)
				RX_POST_LMBD_ALWAYS_INLINE { return checkIfSatisfyCondition(qe, pl); },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const JoinQueryEntry&) RX_POST_LMBD_ALWAYS_INLINE -> bool {
				assertrx_throw(0);
				abort();
			},
			[] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysFalse&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return false; },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysTrue&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; });
		result = (lastResult != (it->operation == OpNot));
	}
	return result;
}

bool QueryEntries::checkIfSatisfyCondition(const QueryEntry& qEntry, const ConstPayload& pl) {
	VariantArray values;
	pl.GetByFieldsSet(qEntry.Fields(), values, qEntry.FieldType(), qEntry.CompositeFieldsTypes());
	return CheckIfSatisfyCondition(values, qEntry.Condition(), qEntry.Values());
}

bool QueryEntries::checkIfSatisfyCondition(const BetweenFieldsQueryEntry& qEntry, const ConstPayload& pl) {
	VariantArray lValues;
	pl.GetByFieldsSet(qEntry.LeftFields(), lValues, qEntry.LeftFieldType(), qEntry.LeftCompositeFieldsTypes());
	VariantArray rValues;
	pl.GetByFieldsSet(qEntry.RightFields(), rValues, qEntry.RightFieldType(), qEntry.RightCompositeFieldsTypes());
	return CheckIfSatisfyCondition(lValues, qEntry.Condition(), rValues);
}

bool QueryEntries::CheckIfSatisfyCondition(const VariantArray& lValues, CondType condition, const VariantArray& rValues) {
	switch (condition) {
		case CondType::CondAny:
			return !lValues.empty();
		case CondType::CondEmpty:
			return lValues.empty();
		case CondType::CondEq:
		case CondType::CondSet:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					if (lhs.RelaxCompare<WithString::Yes, NotComparable::Return>(rhs) == ComparationResult::Eq) {
						return true;
					}
				}
			}
			return false;
		case CondType::CondAllSet:
			if (lValues.size() < rValues.size()) {
				return false;
			}
			for (const auto& v : rValues) {
				auto it = lValues.cbegin();
				for (; it != lValues.cend(); ++it) {
					if (it->RelaxCompare<WithString::Yes, NotComparable::Return>(v) == ComparationResult::Eq) {
						break;
					}
				}
				if (it == lValues.cend()) {
					return false;
				}
			}
			return true;
		case CondType::CondLt:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					if (lhs.RelaxCompare<WithString::Yes, NotComparable::Return>(rhs) == ComparationResult::Lt) {
						return true;
					}
				}
			}
			return false;
		case CondType::CondLe:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					if (lhs.RelaxCompare<WithString::Yes, NotComparable::Return>(rhs) & ComparationResult::Le) {
						return true;
					}
				}
			}
			return false;
		case CondType::CondGt:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					if (lhs.RelaxCompare<WithString::Yes, NotComparable::Return>(rhs) == ComparationResult::Gt) {
						return true;
					}
				}
			}
			return false;
		case CondType::CondGe:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					if (lhs.RelaxCompare<WithString::Yes, NotComparable::Return>(rhs) & ComparationResult::Ge) {
						return true;
					}
				}
			}
			return false;
		case CondType::CondRange:
			for (const auto& v : lValues) {
				if (v.RelaxCompare<WithString::Yes, NotComparable::Return>(rValues[0]) == ComparationResult::Lt ||
					v.RelaxCompare<WithString::Yes, NotComparable::Return>(rValues[1]) == ComparationResult::Gt) {
					return false;
				}
			}
			return true;
		case CondType::CondLike:
			for (const auto& v : lValues) {
				if (!v.Type().Is<KeyValueType::String>()) {
					throw Error(errLogic, "Condition LIKE must be applied to data of string type, but %s was provided", v.Type().Name());
				}
				if (matchLikePattern(std::string_view(v), std::string_view(rValues[0]))) {
					return true;
				}
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

template <InjectionDirection injectionDirection>
size_t QueryEntries::InjectConditionsFromOnConditions(size_t position, const h_vector<QueryJoinEntry, 1>& joinEntries,
													  const QueryEntries& joinedQueryEntries, size_t joinedQueryNo,
													  const std::vector<std::unique_ptr<Index>>* indexesFrom) {
	size_t injectedCount = 0;
	for (size_t i = 0, s = joinEntries.size(); i < s; ++i) {
		const QueryJoinEntry& jEntry = joinEntries[i];
		if (i + 1 < s && joinEntries[i + 1].Operation() == OpOr) {
			continue;
		}
		CondType condition = jEntry.Condition();
		switch (jEntry.Operation()) {
			case OpAnd:
				break;
			case OpOr:
				continue;
			case OpNot:
				switch (condition) {
					case CondLt:
					case CondLe:
					case CondGt:
					case CondGe:
						condition = InvertNotCondition(condition);
						break;
					case CondEq:
					case CondSet:
					case CondAllSet:
					case CondRange:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						continue;
				}
				break;
		}
		if constexpr (injectionDirection == InjectionDirection::FromMain) {
			injectedCount +=
				injectConditionsFromOnCondition(position + injectedCount, jEntry.RightFieldName(), jEntry.LeftFieldName(), condition,
												joinedQueryEntries, QueryEntry::InjectedFromMain, joinedQueryNo, indexesFrom);
		} else {
			injectedCount +=
				injectConditionsFromOnCondition(position + injectedCount, jEntry.LeftFieldName(), jEntry.RightFieldName(), condition,
												joinedQueryEntries, joinedQueryNo, QueryEntry::InjectedFromMain, indexesFrom);
		}
	}
	return injectedCount;
}
template size_t QueryEntries::InjectConditionsFromOnConditions<InjectionDirection::FromMain>(size_t, const h_vector<QueryJoinEntry, 1>&,
																							 const QueryEntries&, size_t,
																							 const std::vector<std::unique_ptr<Index>>*);
template size_t QueryEntries::InjectConditionsFromOnConditions<InjectionDirection::IntoMain>(size_t, const h_vector<QueryJoinEntry, 1>&,
																							 const QueryEntries&, size_t,
																							 const std::vector<std::unique_ptr<Index>>*);

size_t QueryEntries::injectConditionsFromOnCondition(size_t position, const std::string& fieldName, const std::string& joinedFieldName,
													 CondType condition, const QueryEntries& joinedQueryEntries, size_t injectedFrom,
													 size_t injectingInto, const std::vector<std::unique_ptr<Index>>* indexesFrom) {
	switch (condition) {
		case CondEq:
		case CondSet:
		case CondAllSet:
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			break;
		case CondRange:
		case CondAny:
		case CondEmpty:
		case CondDWithin:
		case CondLike:
			return 0;
	}
	size_t injectedCount = 0;
	for (size_t j = 0, next, size = joinedQueryEntries.Size(); j < size; j = next) {
		next = joinedQueryEntries.Next(j);
		if (next < size && joinedQueryEntries.GetOperation(next) == OpOr) {
			continue;
		}
		if (!joinedQueryEntries.Is<QueryEntry>(j)) {
			continue;
		}
		const QueryEntry& qe = joinedQueryEntries.Get<QueryEntry>(j);
		if (qe.IsInjectedFrom(injectingInto)) {
			continue;
		}
		CondType entryCondition = qe.Condition();
		switch (joinedQueryEntries.GetOperation(j)) {
			case OpOr:
				continue;
			case OpAnd:
				break;
			case OpNot:
				switch (entryCondition) {
					case CondLt:
					case CondLe:
					case CondGt:
					case CondGe:
						entryCondition = InvertNotCondition(entryCondition);
						break;
					case CondEq:
					case CondSet:
					case CondAllSet:
					case CondRange:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						continue;
				}
				break;
		}
		if (qe.FieldName() != joinedFieldName) {
			continue;
		}
		switch (condition) {
			case CondEq:
			case CondSet:
				switch (entryCondition) {
					case CondEq:
					case CondSet:
					case CondAllSet:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondSet, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondLt:
					case CondLe:
					case CondGt:
					case CondGe:
						Emplace<QueryEntry>(position, OpAnd, fieldName, entryCondition, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondRange:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						break;
				}
				break;
			case CondAllSet:
				switch (entryCondition) {
					case CondEq:
					case CondSet:
					case CondAllSet:
					case CondLt:
					case CondLe:
					case CondGt:
					case CondGe:
						Emplace<QueryEntry>(position, OpAnd, fieldName, entryCondition, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondRange:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						break;
				}
				break;
			case CondLt:
				switch (entryCondition) {
					case CondEq:
					case CondSet:
					case CondAllSet: {
						if (!qe.IsFieldIndexed() || !indexesFrom) {	 // TODO relax compare for not indexed after !1438 merge
							break;
						}
						const CollateOpts& collate = (*indexesFrom)[qe.IndexNo()]->Opts().collateOpts_;
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondLt,
											VariantArray{*std::max_element(qe.Values().begin(), qe.Values().end(), Variant::Less{collate})},
											injectedFrom);
						++injectedCount;
						++position;
					} break;
					case CondLt:
					case CondLe:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondLt, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondRange:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondLt, VariantArray{qe.Values()[1]}, injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondGt:
					case CondGe:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						break;
				}
				break;
			case CondLe:
				switch (entryCondition) {
					case CondEq:
					case CondSet:
					case CondAllSet: {
						if (!qe.IsFieldIndexed() || !indexesFrom) {	 // TODO relax compare for not indexed after !1438 merge
							break;
						}
						const CollateOpts& collate = (*indexesFrom)[qe.IndexNo()]->Opts().collateOpts_;
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondLe,
											VariantArray{*std::max_element(qe.Values().begin(), qe.Values().end(), Variant::Less{collate})},
											injectedFrom);
						++injectedCount;
						++position;
					} break;
					case CondLt:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondLt, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondLe:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondLe, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondRange:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondLe, VariantArray{qe.Values()[1]}, injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondGt:
					case CondGe:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						break;
				}
				break;
			case CondGt:
				switch (entryCondition) {
					case CondEq:
					case CondSet:
					case CondAllSet: {
						if (!qe.IsFieldIndexed() || !indexesFrom) {	 // TODO relax compare for not indexed after !1438 merge
							break;
						}
						const CollateOpts& collate = (*indexesFrom)[qe.IndexNo()]->Opts().collateOpts_;
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondGt,
											VariantArray{*std::min_element(qe.Values().begin(), qe.Values().end(), Variant::Less{collate})},
											injectedFrom);
						++injectedCount;
						++position;
					} break;
					case CondGt:
					case CondGe:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondGt, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondRange:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondGt, VariantArray{qe.Values()[0]}, injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondLt:
					case CondLe:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						break;
				}
				break;
			case CondGe:
				switch (entryCondition) {
					case CondEq:
					case CondSet:
					case CondAllSet: {
						if (!qe.IsFieldIndexed() || !indexesFrom) {	 // TODO relax compare for not indexed after !1438 merge
							break;
						}
						const CollateOpts& collate = (*indexesFrom)[qe.IndexNo()]->Opts().collateOpts_;
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondGe,
											VariantArray{*std::min_element(qe.Values().begin(), qe.Values().end(), Variant::Less{collate})},
											injectedFrom);
						++injectedCount;
						++position;
					} break;
					case CondGt:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondGt, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondGe:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondGe, qe.Values(), injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondRange:
						Emplace<QueryEntry>(position, OpAnd, fieldName, CondGe, VariantArray{qe.Values()[0]}, injectedFrom);
						++injectedCount;
						++position;
						break;
					case CondLt:
					case CondLe:
					case CondAny:
					case CondEmpty:
					case CondDWithin:
					case CondLike:
						break;
				}
				break;
			case CondRange:
			case CondAny:
			case CondEmpty:
			case CondDWithin:
			case CondLike:
				break;
		}
	}
	return injectedCount;
}

template <typename JS>
std::string QueryJoinEntry::DumpCondition(const JS& joinedSelector, bool needOp) const {
	WrSerializer ser;
	const auto& q = joinedSelector.JoinQuery();
	if (needOp) {
		ser << ' ' << op_ << ' ';
	}
	ser << q.NsName() << '.' << RightFieldName() << ' ' << InvertJoinCondition(condition_) << ' ' << LeftFieldName();
	return std::string{ser.Slice()};
}
template std::string QueryJoinEntry::DumpCondition(const JoinedSelector&, bool) const;

void QueryEntries::dumpEqualPositions(size_t level, WrSerializer& ser, const EqualPositions_t& equalPositions) {
	for (const auto& eq : equalPositions) {
		for (size_t i = 0; i < level; ++i) {
			ser << "   ";
		}
		ser << "equal_positions(";
		for (size_t i = 0, s = eq.size(); i < s; ++i) {
			if (i != 0) {
				ser << ", ";
			}
			ser << eq[i];
		}
		ser << ")\n";
	}
}

std::string SubQueryEntry::Dump(const std::vector<Query>& subQueries) const {
	std::stringstream ss;
	ss << '(' << subQueries.at(QueryIndex()).GetSQL() << ") " << Condition() << ' ';
	if (Values().size() > 1) {
		ss << '[';
	}
	for (size_t i = 0, s = Values().size(); i != s; ++i) {
		if (i != 0) {
			ss << ',';
		}
		ss << '\'' << Values()[i].As<std::string>() << '\'';
	}
	if (Values().size() > 1) {
		ss << ']';
	}
	return ss.str();
}

std::string SubQueryFieldEntry::Dump(const std::vector<Query>& subQueries) const {
	std::stringstream ss;
	ss << FieldName() << ' ' << Condition() << " (" << subQueries.at(QueryIndex()).GetSQL() << ')';
	return ss.str();
}

void SubQueryFieldEntry::checkCondition(CondType cond) const {
	if (cond == CondAny || cond == CondEmpty) {
		throw Error{errQueryExec, "Condition %s with field and subquery", cond == CondAny ? "Any" : "Empty"};
	}
}
template <typename JS>
void QueryEntries::dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JS>& joinedSelectors,
						const std::vector<Query>& subQueries, WrSerializer& ser) {
	for (const_iterator it = begin; it != end; ++it) {
		for (size_t i = 0; i < level; ++i) {
			ser << "   ";
		}
		if (it != begin || it->operation != OpAnd) {
			ser << it->operation << ' ';
		}
		it->Visit([&ser, subQueries](const SubQueryEntry& sqe) { ser << sqe.Dump(subQueries); },
				  [&ser, subQueries](const SubQueryFieldEntry& sqe) { ser << sqe.Dump(subQueries); },
				  [&](const QueryEntriesBracket& b) {
					  ser << "(\n";
					  dump(level + 1, it.cbegin(), it.cend(), joinedSelectors, subQueries, ser);
					  dumpEqualPositions(level + 1, ser, b.equalPositions);
					  for (size_t i = 0; i < level; ++i) {
						  ser << "   ";
					  }
					  ser << ")\n";
				  },
				  [&ser](const QueryEntry& qe) { ser << qe.Dump() << '\n'; },
				  [&joinedSelectors, &ser](const JoinQueryEntry& jqe) { ser << jqe.Dump(joinedSelectors) << '\n'; },
				  [&ser](const BetweenFieldsQueryEntry& qe) { ser << qe.Dump() << '\n'; },
				  [&ser](const AlwaysFalse&) { ser << "AlwaysFalse\n"; }, [&ser](const AlwaysTrue&) { ser << "AlwaysTrue\n"; });
	}
}
template void QueryEntries::dump(size_t, const_iterator, const_iterator, const std::vector<JoinedSelector>&, const std::vector<Query>&,
								 WrSerializer&);
template void QueryEntries::dump(size_t, const_iterator, const_iterator, const std::vector<JoinedSelectorMock>&, const std::vector<Query>&,
								 WrSerializer&);

}  // namespace reindexer
