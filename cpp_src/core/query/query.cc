#include "core/query/query.h"
#include "core/query/dsl/dslencoder.h"
#include "core/query/dsl/dslparser.h"
#include "core/query/sql/sqlencoder.h"
#include "core/query/sql/sqlparser.h"
#include "core/type_consts_helpers.h"
#include "tools/serializer.h"

namespace reindexer {

using namespace std::string_view_literals;

const std::string_view kLsnIndexName = "#lsn"sv;
const std::string_view kSlaveVersionIndexName = "#slave_version"sv;

void Query::checkSubQuery() const {
	if rx_unlikely (type_ != QuerySelect) {
		throw Error{errQueryExec, "Subquery should be select"};
	}
	if rx_unlikely (!joinQueries_.empty()) {
		throw Error{errQueryExec, "Join cannot be in subquery"};
	}
	if rx_unlikely (!mergeQueries_.empty()) {
		throw Error{errQueryExec, "Merge cannot be in subquery"};
	}
	if rx_unlikely (!subQueries_.empty()) {
		throw Error{errQueryExec, "Subquery cannot be in subquery"};
	}
	if rx_unlikely (!selectFunctions_.empty()) {
		throw Error{errQueryExec, "Select function cannot be in subquery"};
	}
	if rx_unlikely (!updateFields_.empty()) {
		throw Error{errQueryExec, "Subquery cannot update"};
	}
	if rx_unlikely (withRank_) {
		throw Error{errQueryExec, "Subquery cannot request rank"};
	}
	if rx_unlikely (isSystemNamespaceNameFast(NsName())) {
		throw Error{errQueryExec, "Queries to system namespaces ('%s') are not supported inside subquery", NsName()};
	}
	if rx_unlikely (IsWALQuery()) {
		throw Error{errQueryExec, "WAL queries are not supported inside subquery"};
	}
}

void Query::checkSubQueryNoData() const {
	if rx_unlikely (!aggregations_.empty()) {
		throw Error{errQueryExec, "Aggregaton cannot be in subquery with condition Any or Empty"};
	}
	if rx_unlikely (HasLimit() && Limit() != 0) {
		throw Error{errQueryExec, "Limit cannot be in subquery with condition Any or Empty"};
	}
	if rx_unlikely (HasOffset()) {
		throw Error{errQueryExec, "Offset cannot be in subquery with condition Any or Empty"};
	}
	if rx_unlikely (calcTotal_ != ModeNoTotal) {
		throw Error{errQueryExec, "Total request cannot be in subquery with condition Any or Empty"};
	}
	if rx_unlikely (!selectFilter_.empty()) {
		throw Error{errQueryExec, "Select fields filter cannot be in subquery with condition Any or Empty"};
	}
	checkSubQuery();
}

void Query::checkSubQueryWithData() const {
	if rx_unlikely ((aggregations_.size() + selectFilter_.size() + (calcTotal_ == ModeNoTotal ? 0 : 1)) != 1) {
		throw Error{errQueryExec, "Subquery should contain exactly one of aggregation, select field filter or total request"};
	}
	if (!aggregations_.empty()) {
		switch (aggregations_[0].Type()) {
			case AggDistinct:
			case AggUnknown:
			case AggFacet:
				throw Error{errQueryExec, "Aggregation %s cannot be in subquery", AggTypeToStr(aggregations_[0].Type())};
			case AggMin:
			case AggMax:
			case AggAvg:
			case AggSum:
			case AggCount:
			case AggCountCached:
				break;
		}
	}
	checkSubQuery();
}

void Query::VerifyForUpdate() const {
	if (!subQueries_.empty()) {
		throw Error{errQueryExec, "UPDATE and DELETE query cannot contain subqueries"};
	}
	if (!joinQueries_.empty()) {
		throw Error{errQueryExec, "UPDATE and DELETE query cannot contain join"};
	}
}

bool Query::operator==(const Query &obj) const {
	if (entries_ != obj.entries_ || aggregations_ != obj.aggregations_ ||

		NsName() != obj.NsName() || sortingEntries_ != obj.sortingEntries_ || CalcTotal() != obj.CalcTotal() || Offset() != obj.Offset() ||
		Limit() != obj.Limit() || debugLevel_ != obj.debugLevel_ || strictMode_ != obj.strictMode_ || selectFilter_ != obj.selectFilter_ ||
		selectFunctions_ != obj.selectFunctions_ || joinQueries_ != obj.joinQueries_ || mergeQueries_ != obj.mergeQueries_ ||
		updateFields_ != obj.updateFields_ || subQueries_ != obj.subQueries_ || forcedSortOrder_.size() != obj.forcedSortOrder_.size()) {
		return false;
	}
	for (size_t i = 0, s = forcedSortOrder_.size(); i < s; ++i) {
		if (forcedSortOrder_[i].RelaxCompare<WithString::Yes>(obj.forcedSortOrder_[i]) != 0) {
			return false;
		}
	}

	return true;
}

bool JoinedQuery::operator==(const JoinedQuery &obj) const {
	if (joinEntries_ != obj.joinEntries_) return false;
	if (joinType != obj.joinType) return false;
	return Query::operator==(obj);
}
Query Query::FromSQL(std::string_view q) { return SQLParser::Parse(q); }

Error Query::FromJSON(std::string_view dsl) { return dsl::Parse(dsl, *this); }

std::string Query::GetJSON() const { return dsl::toDsl(*this); }

WrSerializer &Query::GetSQL(WrSerializer &ser, bool stripArgs) const { return SQLEncoder(*this).GetSQL(ser, stripArgs); }
WrSerializer &Query::GetSQL(WrSerializer &ser, QueryType realType, bool stripArgs) const {
	return SQLEncoder(*this, realType).GetSQL(ser, stripArgs);
}

std::string Query::GetSQL(bool stripArgs) const {
	WrSerializer ser;
	return std::string(GetSQL(ser, stripArgs).Slice());
}

std::string Query::GetSQL(QueryType realType) const {
	WrSerializer ser;
	return std::string(SQLEncoder(*this, realType).GetSQL(ser, false).Slice());
}

void Query::Join(JoinedQuery &&jq) & {
	switch (jq.joinType) {
		case JoinType::Merge:
			if (nextOp_ != OpAnd) {
				throw Error(errParams, "Merge query with %s operation", OpTypeToStr(nextOp_));
			}
			mergeQueries_.emplace_back(std::move(jq));
			return;
		case JoinType::LeftJoin:
			if (nextOp_ != OpAnd) {
				throw Error(errParams, "Left join with %s operation", OpTypeToStr(nextOp_));
			}
			break;
		case JoinType::OrInnerJoin:
			if (nextOp_ == OpNot) {
				throw Error(errParams, "Or inner join with %s operation", OpTypeToStr(nextOp_));
			}
			nextOp_ = OpOr;
			[[fallthrough]];
		case JoinType::InnerJoin:
			entries_.Append(nextOp_, JoinQueryEntry(joinQueries_.size()));
			nextOp_ = OpAnd;
			break;
	}
	joinQueries_.emplace_back(std::move(jq));
	adoptNested(joinQueries_.back());
}

VariantArray Query::deserializeValues(Serializer &ser, CondType cond) {
	VariantArray values;
	auto cnt = ser.GetVarUint();
	if (cond == CondDWithin) {
		if (cnt != 3) {
			throw Error(errParseBin, "Expected point and distance for DWithin");
		}
		VariantArray point;
		point.reserve(2);
		point.emplace_back(ser.GetVariant().EnsureHold());
		point.emplace_back(ser.GetVariant().EnsureHold());
		values.reserve(2);
		values.emplace_back(std::move(point));
		values.emplace_back(ser.GetVariant().EnsureHold());
	} else {
		values.reserve(cnt);
		while (cnt--) values.emplace_back(ser.GetVariant().EnsureHold());
	}
	return values;
}

void Query::deserialize(Serializer &ser, bool &hasJoinConditions) {
	bool end = false;
	std::vector<std::pair<size_t, EqualPosition_t>> equalPositions;
	while (!end && !ser.Eof()) {
		QueryItemType qtype = QueryItemType(ser.GetVarUint());
		switch (qtype) {
			case QueryCondition: {
				const auto fieldName = ser.GetVString();
				const OpType op = OpType(ser.GetVarUint());
				const CondType condition = CondType(ser.GetVarUint());
				VariantArray values = deserializeValues(ser, condition);
				entries_.Append<QueryEntry>(op, std::string{fieldName}, condition, std::move(values));
				break;
			}
			case QueryBetweenFieldsCondition: {
				OpType op = OpType(ser.GetVarUint());
				std::string firstField{ser.GetVString()};
				CondType condition = static_cast<CondType>(ser.GetVarUint());
				std::string secondField{ser.GetVString()};
				entries_.Append<BetweenFieldsQueryEntry>(op, std::move(firstField), condition, std::move(secondField));
				break;
			}
			case QueryAlwaysFalseCondition: {
				const OpType op = OpType(ser.GetVarUint());
				entries_.Append<AlwaysFalse>(op);
				break;
			}
			case QueryAlwaysTrueCondition: {
				const OpType op = OpType(ser.GetVarUint());
				entries_.Append<AlwaysTrue>(op);
				break;
			}
			case QueryJoinCondition: {
				uint64_t type = ser.GetVarUint();
				assertrx(type != JoinType::LeftJoin);
				JoinQueryEntry joinEntry(ser.GetVarUint());
				hasJoinConditions = true;
				// NOLINTNEXTLINE(performance-move-const-arg)
				entries_.Append((type == JoinType::OrInnerJoin) ? OpOr : OpAnd, std::move(joinEntry));
				break;
			}
			case QueryAggregation: {
				const AggType type = static_cast<AggType>(ser.GetVarUint());
				size_t fieldsCount = ser.GetVarUint();
				h_vector<std::string, 1> fields;
				fields.reserve(fieldsCount);
				while (fieldsCount--) fields.emplace_back(std::string(ser.GetVString()));
				auto pos = ser.Pos();
				bool aggEnd = false;
				aggregations_.emplace_back(type, std::move(fields));
				auto &ae = aggregations_.back();
				while (!ser.Eof() && !aggEnd) {
					int atype = ser.GetVarUint();
					switch (atype) {
						case QueryAggregationSort: {
							auto fieldName = ser.GetVString();
							ae.AddSortingEntry({std::string(fieldName), ser.GetVarUint() != 0});
							break;
						}
						case QueryAggregationLimit:
							ae.SetLimit(ser.GetVarUint());
							break;
						case QueryAggregationOffset:
							ae.SetOffset(ser.GetVarUint());
							break;
						default:
							ser.SetPos(pos);
							aggEnd = true;
					}
					pos = ser.Pos();
				}
				break;
			}
			case QueryDistinct: {
				const auto fieldName = ser.GetVString();
				if (!fieldName.empty()) {
					entries_.Append<QueryEntry>(OpAnd, std::string{fieldName}, QueryEntry::DistinctTag{});
				}
				break;
			}
			case QuerySortIndex: {
				SortingEntry sortingEntry;
				sortingEntry.expression = std::string(ser.GetVString());
				sortingEntry.desc = bool(ser.GetVarUint());
				if (sortingEntry.expression.length()) {
					sortingEntries_.push_back(std::move(sortingEntry));
				}
				int cnt = ser.GetVarUint();
				if (cnt != 0 && sortingEntries_.size() != 1) {
					throw Error(errParams, "Forced sort order is allowed for the first sorting entry only");
				}
				forcedSortOrder_.reserve(cnt);
				while (cnt--) forcedSortOrder_.push_back(ser.GetVariant().EnsureHold());
				break;
			}
			case QueryJoinOn: {
				const OpType op = static_cast<OpType>(ser.GetVarUint());
				const CondType condition = static_cast<CondType>(ser.GetVarUint());
				std::string leftFieldName{ser.GetVString()};
				std::string rightFieldName{ser.GetVString()};
				reinterpret_cast<JoinedQuery *>(this)->joinEntries_.emplace_back(op, condition, std::move(leftFieldName),
																				 std::move(rightFieldName));
				break;
			}
			case QueryDebugLevel:
				Debug(ser.GetVarUint());
				break;
			case QueryStrictMode:
				Strict(StrictMode(ser.GetVarUint()));
				break;
			case QueryLimit:
				count_ = ser.GetVarUint();
				break;
			case QueryOffset:
				start_ = ser.GetVarUint();
				break;
			case QueryReqTotal:
				calcTotal_ = CalcTotalMode(ser.GetVarUint());
				break;
			case QuerySelectFilter:
				selectFilter_.emplace_back(ser.GetVString());
				break;
			case QueryEqualPosition: {
				const unsigned bracketPosition = ser.GetVarUint();
				const unsigned fieldsCount = ser.GetVarUint();
				equalPositions.emplace_back(bracketPosition, fieldsCount);
				for (auto &field : equalPositions.back().second) field = ser.GetVString();
				break;
			}
			case QueryExplain:
				Explain(true);
				break;
			case QueryWithRank:
				withRank_ = true;
				break;
			case QuerySelectFunction:
				selectFunctions_.emplace_back(ser.GetVString());
				break;
			case QueryDropField: {
				Drop(ser.GetVString());
				break;
			}
			case QueryUpdateFieldV2: {
				VariantArray val;
				std::string field(ser.GetVString());
				bool isArray = ser.GetVarUint();
				int numValues = ser.GetVarUint();
				bool hasExpressions = false;
				while (numValues--) {
					hasExpressions = ser.GetVarUint();
					val.emplace_back(ser.GetVariant().EnsureHold());
				}
				Set(std::move(field), std::move(val.MarkArray(isArray)), hasExpressions);
				break;
			}
			case QueryUpdateField: {
				VariantArray val;
				std::string field(ser.GetVString());
				int numValues = ser.GetVarUint();
				bool isArray = numValues > 1;
				bool hasExpressions = false;
				while (numValues--) {
					hasExpressions = ser.GetVarUint();
					val.emplace_back(ser.GetVariant().EnsureHold());
				}
				Set(std::move(field), std::move(val.MarkArray(isArray)), hasExpressions);
				break;
			}
			case QueryUpdateObject: {
				VariantArray val;
				std::string field(ser.GetVString());
				bool hasExpressions = false;
				int numValues = ser.GetVarUint();
				val.MarkArray(ser.GetVarUint() == 1);
				while (numValues--) {
					hasExpressions = ser.GetVarUint();
					val.emplace_back(ser.GetVariant().EnsureHold());
				}
				SetObject(std::move(field), std::move(val), hasExpressions);
				break;
			}
			case QueryOpenBracket: {
				OpType op = OpType(ser.GetVarUint());
				entries_.OpenBracket(op);
				break;
			}
			case QueryCloseBracket:
				entries_.CloseBracket();
				break;
			case QueryEnd:
				end = true;
				break;
			case QuerySubQueryCondition: {
				OpType op = OpType(ser.GetVarUint());
				Serializer subQuery{ser.GetVString()};
				CondType condition = CondType(ser.GetVarUint());
				VariantArray values = deserializeValues(ser, condition);
				NextOp(op);
				Where(Query::Deserialize(subQuery), condition, std::move(values));
				break;
			}
			case QueryFieldSubQueryCondition: {
				OpType op = OpType(ser.GetVarUint());
				const auto fieldName = ser.GetVString();
				CondType condition = CondType(ser.GetVarUint());
				Serializer subQuery{ser.GetVString()};
				NextOp(op);
				Where(fieldName, condition, Query::Deserialize(subQuery));
				break;
			}
			case QueryAggregationSort:
			case QueryAggregationOffset:
			case QueryAggregationLimit:
			default:
				throw Error(errParseBin, "Unknown type %d while parsing binary buffer", qtype);
		}
	}
	for (auto &&eqPos : equalPositions) {
		if (eqPos.first == 0) {
			entries_.equalPositions.emplace_back(std::move(eqPos.second));
		} else {
			entries_.Get<QueryEntriesBracket>(eqPos.first - 1).equalPositions.emplace_back(std::move(eqPos.second));
		}
	}
	return;
}

void Query::Serialize(WrSerializer &ser, uint8_t mode) const {
	ser.PutVString(NsName());
	entries_.Serialize(ser, subQueries_);

	if (!(mode & SkipAggregations)) {
		for (const auto &agg : aggregations_) {
			ser.PutVarUint(QueryAggregation);
			ser.PutVarUint(agg.Type());
			ser.PutVarUint(agg.Fields().size());
			for (const auto &field : agg.Fields()) {
				ser.PutVString(field);
			}
			for (const auto &se : agg.Sorting()) {
				ser.PutVarUint(QueryAggregationSort);
				ser.PutVString(se.expression);
				ser.PutVarUint(se.desc);
			}
			if (agg.Limit() != QueryEntry::kDefaultLimit) {
				ser.PutVarUint(QueryAggregationLimit);
				ser.PutVarUint(agg.Limit());
			}
			if (agg.Offset() != QueryEntry::kDefaultOffset) {
				ser.PutVarUint(QueryAggregationOffset);
				ser.PutVarUint(agg.Offset());
			}
		}
	}

	if (!(mode & SkipSortEntries)) {
		for (const auto &sortingEntry : sortingEntries_) {
			ser.PutVarUint(QuerySortIndex);
			ser.PutVString(sortingEntry.expression);
			ser.PutVarUint(sortingEntry.desc);
			int cnt = forcedSortOrder_.size();
			ser.PutVarUint(cnt);
			for (auto &kv : forcedSortOrder_) ser.PutVariant(kv);
		}
	}

	if (mode & WithJoinEntries) {
		for (const auto &qje : reinterpret_cast<const JoinedQuery *>(this)->joinEntries_) {
			ser.PutVarUint(QueryJoinOn);
			ser.PutVarUint(qje.Operation());
			ser.PutVarUint(qje.Condition());
			ser.PutVString(qje.LeftFieldName());
			ser.PutVString(qje.RightFieldName());
		}
	}

	for (const auto &equalPoses : entries_.equalPositions) {
		ser.PutVarUint(QueryEqualPosition);
		ser.PutVarUint(0);
		ser.PutVarUint(equalPoses.size());
		for (const auto &ep : equalPoses) ser.PutVString(ep);
	}
	for (size_t i = 0; i < entries_.Size(); ++i) {
		if (entries_.IsSubTree(i)) {
			const auto &bracket = entries_.Get<QueryEntriesBracket>(i);
			for (const auto &equalPoses : bracket.equalPositions) {
				ser.PutVarUint(QueryEqualPosition);
				ser.PutVarUint(i + 1);
				ser.PutVarUint(equalPoses.size());
				for (const auto &ep : equalPoses) ser.PutVString(ep);
			}
		}
	}

	if (!(mode & SkipExtraParams)) {
		ser.PutVarUint(QueryDebugLevel);
		ser.PutVarUint(debugLevel_);

		if (strictMode_ != StrictModeNotSet) {
			ser.PutVarUint(QueryStrictMode);
			ser.PutVarUint(int(strictMode_));
		}
	}

	if (!(mode & SkipLimitOffset)) {
		if (HasLimit()) {
			ser.PutVarUint(QueryLimit);
			ser.PutVarUint(Limit());
		}
		if (HasOffset()) {
			ser.PutVarUint(QueryOffset);
			ser.PutVarUint(Offset());
		}
	}

	if (!(mode & SkipExtraParams)) {
		if (HasCalcTotal()) {
			ser.PutVarUint(QueryReqTotal);
			ser.PutVarUint(CalcTotal());
		}

		for (const auto &sf : selectFilter_) {
			ser.PutVarUint(QuerySelectFilter);
			ser.PutVString(sf);
		}

		if (explain_) {
			ser.PutVarUint(QueryExplain);
		}

		if (withRank_) {
			ser.PutVarUint(QueryWithRank);
		}
	}

	for (const auto &field : updateFields_) {
		if (field.Mode() == FieldModeSet) {
			ser.PutVarUint(QueryUpdateFieldV2);
			ser.PutVString(field.Column());
			ser.PutVarUint(field.Values().IsArrayValue());
			ser.PutVarUint(field.Values().size());
			for (const Variant &val : field.Values()) {
				ser.PutVarUint(field.IsExpression());
				ser.PutVariant(val);
			}
		} else if (field.Mode() == FieldModeDrop) {
			ser.PutVarUint(QueryDropField);
			ser.PutVString(field.Column());
		} else {
			throw Error(errLogic, "Unsupported item modification mode = %d", field.Mode());
		}
	}

	ser.PutVarUint(QueryEnd);  // finita la commedia... of root query

	if (!(mode & SkipJoinQueries)) {
		for (const auto &jq : joinQueries_) {
			if (!(mode & SkipLeftJoinQueries) || jq.joinType != JoinType::LeftJoin) {
				ser.PutVarUint(static_cast<int>(jq.joinType));
				jq.Serialize(ser, WithJoinEntries);
			}
		}
	}

	if (!(mode & SkipMergeQueries)) {
		for (const auto &mq : mergeQueries_) {
			ser.PutVarUint(static_cast<int>(mq.joinType));
			mq.Serialize(ser, (mode | WithJoinEntries) & (~SkipSortEntries));
		}
	}
}

Query Query::Deserialize(Serializer &ser) {
	Query res(ser.GetVString());
	bool hasJoinConditions = false;
	res.deserialize(ser, hasJoinConditions);

	bool nested = false;
	while (!ser.Eof()) {
		auto joinType = JoinType(ser.GetVarUint());
		JoinedQuery q1(std::string(ser.GetVString()));
		q1.joinType = joinType;
		q1.deserialize(ser, hasJoinConditions);
		res.adoptNested(q1);
		if (joinType == JoinType::Merge) {
			res.mergeQueries_.emplace_back(std::move(q1));
			nested = true;
		} else {
			Query &q = nested ? res.mergeQueries_.back() : res;
			if (joinType != JoinType::LeftJoin && !hasJoinConditions) {
				const size_t joinIdx = res.joinQueries_.size();
				res.entries_.Append<JoinQueryEntry>((joinType == JoinType::OrInnerJoin) ? OpOr : OpAnd, joinIdx);
			}
			q.joinQueries_.emplace_back(std::move(q1));
			q.adoptNested(q.joinQueries_.back());
		}
	}
	return res;
}

Query &Query::Join(JoinType joinType, std::string leftField, std::string rightField, CondType cond, OpType op, Query &&qr) & {
	auto jq = JoinedQuery{joinType, std::move(qr)};
	jq.joinEntries_.emplace_back(op, cond, std::move(leftField), std::move(rightField));
	Join(std::move(jq));
	return *this;
}

Query &Query::Join(JoinType joinType, std::string leftField, std::string rightField, CondType cond, OpType op, const Query &qr) & {
	auto jq = JoinedQuery{joinType, qr};
	jq.joinEntries_.emplace_back(op, cond, std::move(leftField), std::move(rightField));
	Join(std::move(jq));
	return *this;
}

Query &Query::Merge(const Query &q) & {
	mergeQueries_.emplace_back(JoinType::Merge, q);
	adoptNested(mergeQueries_.back());
	return *this;
}

Query &Query::Merge(Query &&q) & {
	mergeQueries_.emplace_back(JoinType::Merge, std::move(q));
	adoptNested(mergeQueries_.back());
	return *this;
}

void Query::AddJoinQuery(JoinedQuery &&jq) {
	adoptNested(jq);
	joinQueries_.emplace_back(std::move(jq));
}

Query &Query::SortStDistance(std::string_view field, Point p, bool desc) & {
	if (field.empty()) {
		throw Error(errParams, "Field name for ST_Distance can not be empty");
	}
	sortingEntries_.emplace_back(fmt::sprintf("ST_Distance(%s,ST_GeomFromText('point(%.12f %.12f)'))", field, p.X(), p.Y()), desc);
	return *this;
}

Query &Query::SortStDistance(std::string_view field1, std::string_view field2, bool desc) & {
	if (field1.empty() || field2.empty()) {
		throw Error(errParams, "Fields names for ST_Distance can not be empty");
	}
	sortingEntries_.emplace_back(fmt::sprintf("ST_Distance(%s,%s)", field1, field2), desc);
	return *this;
}

void Query::walkNested(bool withSelf, bool withMerged, bool withSubQueries,
					   const std::function<void(Query &q)> &visitor) noexcept(noexcept(visitor(std::declval<Query &>()))) {
	if (withSelf) visitor(*this);
	if (withMerged) {
		for (auto &mq : mergeQueries_) visitor(mq);
		if (withSubQueries) {
			for (auto &mq : mergeQueries_) {
				for (auto &nq : mq.subQueries_) {
					nq.walkNested(true, true, true, visitor);
				}
			}
		}
	}
	for (auto &jq : joinQueries_) visitor(jq);
	for (auto &mq : mergeQueries_) {
		for (auto &jq : mq.joinQueries_) visitor(jq);
	}
	if (withSubQueries) {
		for (auto &nq : subQueries_) {
			nq.walkNested(true, withMerged, true, visitor);
		}
	}
}

void Query::WalkNested(bool withSelf, bool withMerged, bool withSubQueries, const std::function<void(const Query &q)> &visitor) const
	noexcept(noexcept(visitor(std::declval<Query>()))) {
	if (withSelf) visitor(*this);
	if (withMerged) {
		for (auto &mq : mergeQueries_) visitor(mq);
		if (withSubQueries) {
			for (auto &mq : mergeQueries_) {
				for (auto &nq : mq.subQueries_) {
					nq.WalkNested(true, true, true, visitor);
				}
			}
		}
	}
	for (auto &jq : joinQueries_) visitor(jq);
	for (auto &mq : mergeQueries_) {
		for (auto &jq : mq.joinQueries_) visitor(jq);
	}
	if (withSubQueries) {
		for (auto &nq : subQueries_) {
			nq.WalkNested(true, withMerged, true, visitor);
		}
	}
}

bool Query::IsWALQuery() const noexcept {
	if (entries_.Size() == 1 && entries_.Is<QueryEntry>(0) && kLsnIndexName == entries_.Get<QueryEntry>(0).FieldName()) {
		return true;
	} else if (entries_.Size() == 2 && entries_.Is<QueryEntry>(0) && entries_.Is<QueryEntry>(1)) {
		const auto &index0 = entries_.Get<QueryEntry>(0).FieldName();
		const auto &index1 = entries_.Get<QueryEntry>(1).FieldName();
		return (kLsnIndexName == index0 && kSlaveVersionIndexName == index1) ||
			   (kLsnIndexName == index1 && kSlaveVersionIndexName == index0);
	}
	return false;
}

}  // namespace reindexer
