#include <string>
#include <string_view>
#include "core/enums.h"
#include "core/query/dsl/dslencoder.h"
#include "core/query/dsl/dslparser.h"
#include "core/query/expression/expression.h"
#include "core/query/query_impl.h"
#include "core/query/sql/sql_formatters.h"
#include "core/query/sql/sqlencoder.h"
#include "core/query/sql/sqlparser.h"
#include "core/type_consts_helpers.h"
#include "tools/scope_guard.h"
#include "tools/serilize/serializer.h"
#include "tools/serilize/wrserializer.h"

namespace {

constexpr std::string_view kOrNotOpErrorMsg =
	"'OR NOT' operation is not supported yet. Use version with brackets instead: 'OR ( NOT ... )'";

}  // namespace

namespace reindexer {

using namespace std::string_view_literals;

void Query::checkSubQuery() const {
	if (type_ != QuerySelect) [[unlikely]] {
		throw Error{errQueryExec, "Subquery should be select"};
	}
	if (!joinQueries_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "Join cannot be in subquery"};
	}
	if (!mergeQueries_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "Merge cannot be in subquery"};
	}
	if (!subQueries_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "Subquery cannot be in subquery"};
	}
	if (!selectFunctions_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "Select function cannot be in subquery"};
	}
	if (!updateFields_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "Subquery cannot update"};
	}
	if (withRank_) [[unlikely]] {
		throw Error{errQueryExec, "Subquery cannot request rank"};
	}
	if (isSystemNamespaceNameFast(NsName())) [[unlikely]] {
		throw Error{errQueryExec, "Queries to system namespaces ('{}') are not supported inside subquery", NsName()};
	}
	validateWalQueryNoJoinMergeSubquery();
}

void Query::checkJoinedSubQuery() const {
	if (entries_.ContainsKnnCondition()) [[unlikely]] {
		throw Error{errQueryExec, "KNN condition cannot be in joined subquery"};
	}
	validateWalQueryNoJoinMergeSubquery();
}

void Query::checkSubQueryNoData() const {
	if (!aggregations_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "Aggregation cannot be in subquery with condition Any or Empty"};
	}
	if (hasLimit() && limit() != 0) [[unlikely]] {
		throw Error{errQueryExec, "Limit cannot be in subquery with condition Any or Empty"};
	}
	if (hasOffset()) [[unlikely]] {
		throw Error{errQueryExec, "Offset cannot be in subquery with condition Any or Empty"};
	}
	if (calcTotal_ != ModeNoTotal) [[unlikely]] {
		throw Error{errQueryExec, "Total request cannot be in subquery with condition Any or Empty"};
	}
	if (!selectFilter_.OnlyAllRegularFields()) [[unlikely]] {
		throw Error{errQueryExec, "Select fields filter cannot be in subquery with condition Any or Empty"};
	}
	checkSubQuery();
}

void Query::checkSubQueryWithData() const {
	if ((aggregations_.size() + selectFilter_.Fields().size() + (calcTotal_ == ModeNoTotal ? 0 : 1)) != 1 ||
		(selectFilter_.ExplicitAllRegularFields() && !selectFilter_.Fields().empty()) || selectFilter_.AllVectorFields()) [[unlikely]] {
		throw Error{errQueryExec, "Subquery should contain exactly one of aggregation, select field filter or total request"};
	}
	if (!aggregations_.empty()) {
		switch (aggregations_[0].Type()) {
			case AggDistinct:
			[[unlikely]]
			case AggUnknown:
			[[unlikely]]
			case AggFacet:
				[[unlikely]] throw Error{errQueryExec, "Aggregation {} cannot be in subquery", AggTypeToStr(aggregations_[0].Type())};
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

void Query::checkFunctionForLeftExpression(FunctionType type) {
	if (type != FunctionFlatArrayLen) [[unlikely]] {
		throw Error(errParams, "Function '{}' is not supported as left expression", functions::TypeToName(type));
	}
}

void Query::checkFunctionForRightExpression(FunctionType type) {
	if (type != FunctionNow) [[unlikely]] {
		throw Error(errParams, "Function '{}' is not supported as right expression", functions::TypeToName(type));
	}
}

void Query::validateWalLsnEntry() const {
	if (!joinQueries_.empty() || !mergeQueries_.empty() || !subQueries_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "WAL queries cannot contain join, merge or subquery"};
	}
	if (entries_.Size() != 0) [[unlikely]] {
		throw Error(errLogic, "Query to WAL should contain condition '{} > number' or '{} is not null' only", kLsnIndexName, kLsnIndexName);
	}
}

void Query::validateWalQueryNoJoinMergeSubquery() const {
	if (IsWALQuery()) [[unlikely]] {
		throw Error{errQueryExec, "WAL queries cannot be used in merge, join or subquery"sv};
	}
}

void Query::checkAddNotWalCondition() const {
	if (IsWALQuery()) [[unlikely]] {
		throw Error{errQueryExec, "WAL query can contain only '{} > number' or '{} is not null'", kLsnIndexName, kLsnIndexName};
	}
}

void Query::verifyForUpdate() const {
	for (const auto& jq : joinQueries_) {
		if (!(jq.getJoinType() == JoinType::InnerJoin || jq.getJoinType() == JoinType::OrInnerJoin)) [[unlikely]] {
			throw Error{errQueryExec, "UPDATE and DELETE query can contain only inner join"};
		}
	}
}

void Query::verifyForUpdateTransaction() const {
	if (!joinQueries_.empty()) [[unlikely]] {
		throw Error{errQueryExec, "UPDATE and DELETE query cannot contain join"};
	}
	verifyForUpdate();
}

Query::Query(Query&& other) noexcept = default;
Query::Query(const Query& other) = default;

Query& Query::Or() & {
	assertrx_dbg(nextOp_ == OpAnd);
	if (nextOp_ == OpNot) [[unlikely]] {
		throw Error(errParams, kOrNotOpErrorMsg);
	}
	if (entries_.Empty()) [[unlikely]] {
		throw Error(errParams, "OR operator in first condition or after left join");
	}
	nextOp_ = OpOr;
	return *this;
}

Query& Query::Not() & {
	assertrx_dbg(nextOp_ == OpAnd);
	if (nextOp_ == OpOr) [[unlikely]] {
		throw Error(errParams, kOrNotOpErrorMsg);
	}
	nextOp_ = OpNot;
	return *this;
}

bool Query::operator==(const Query& obj) const {
	if (entries_ != obj.entries_ || aggregations_ != obj.aggregations_ || NsName() != obj.NsName() ||
		sortingEntries_ != obj.sortingEntries_ || calcTotal() != obj.calcTotal() || offset() != obj.offset() || limit() != obj.limit() ||
		debugLevel_ != obj.debugLevel_ || strictMode_ != obj.strictMode_ || selectFilter_ != obj.selectFilter_ ||
		selectFunctions_ != obj.selectFunctions_ || joinQueries_ != obj.joinQueries_ || mergeQueries_ != obj.mergeQueries_ ||
		updateFields_ != obj.updateFields_ || subQueries_ != obj.subQueries_ || forcedSortOrder_.size() != obj.forcedSortOrder_.size()) {
		return false;
	}
	for (size_t i = 0, s = forcedSortOrder_.size(); i < s; ++i) {
		if (forcedSortOrder_[i].RelaxCompare<WithString::Yes, NotComparable::Return, kDefaultNullsHandling>(obj.forcedSortOrder_[i]) !=
			ComparationResult::Eq) {
			return false;
		}
	}
	return true;
}

Query Query::FromSQL(std::string_view q) { return SQLParser::Parse(q); }

Query Query::FromJSON(std::string_view dsl) {
	Query q;
	dsl::Parse(dsl, Impl(q));
	return q;
}

std::string Query::GetJSON() const { return dsl::toDsl(Impl(*this)); }

void Query::getSQL(WrSerializer& ser, bool stripArgs, Pretty pretty) const {
	if (pretty) {
		PrettySqlFormatter formatter(ser);
		SQLEncoder(Impl(*this), formatter).DumpSQL(stripArgs);
	} else {
		SingleLineSqlFormatter formatter(ser);
		SQLEncoder(Impl(*this), formatter).DumpSQL(stripArgs);
	}
}

void Query::getSQL(WrSerializer& ser, QueryType realType, bool stripArgs) const {
	SingleLineSqlFormatter formatter(ser);
	SQLEncoder(Impl(*this), realType, formatter).DumpSQL(stripArgs);
}

std::string Query::GetSQL(bool stripArgs) const {
	WrSerializer ser;
	getSQL(ser, stripArgs);
	return std::string(ser.Slice());
}

std::string Query::getSQL(QueryType realType, Pretty pretty) const {
	WrSerializer ser;
	if (pretty) {
		PrettySqlFormatter formatter(ser);
		SQLEncoder(Impl(*this), realType, formatter).DumpSQL(false);
	} else {
		SingleLineSqlFormatter formatter(ser);
		SQLEncoder(Impl(*this), realType, formatter).DumpSQL(false);
	}
	return std::string(ser.Slice());
}

template <typename Q>
void Query::addConditionSubQuery(OpType op, Q&& subQ, CondType cond, VariantArray&& values) {
	validateWalQueryNoJoinMergeSubquery();
	subQ.validateWalQueryNoJoinMergeSubquery();
	subQueries_.emplace_back(std::forward<Q>(subQ));
	auto& subQuery = subQueries_.back();
	auto guard = MakeScopeGuard([this]() noexcept { return subQueries_.pop_back(); });
	adoptNested(subQuery);
	addConditionSubQueryImpl(op, subQuery, cond, std::move(values));
	guard.Disable();
}

void Query::addConditionSubQueryImpl(OpType op, Query& subQuery, CondType cond, VariantArray&& values) {
	validateWalQueryNoJoinMergeSubquery();
	if (cond == CondEmpty || cond == CondAny) {
		subQuery.checkSubQueryNoData();
		subQuery.Limit(0);
	} else {
		subQuery.checkSubQueryWithData();
		if (!subQuery.selectFilter_.Fields().empty() && !subQuery.hasLimit() && !subQuery.hasOffset()) {
			// Converts main query condition to subquery condition
			subQuery.sortingEntries_.clear();
			subQuery.addCondition<QueryEntry>(OpAnd, std::move(subQuery.selectFilter_.Fields()[0]), cond, std::move(values));
			subQuery.selectFilter_.Clear();
			return addConditionSubQueryImpl(op, subQuery, CondAny, VariantArray{});
		} else if (subQuery.hasCalcTotal() || (!subQuery.aggregations_.empty() && (subQuery.aggregations_[0].Type() == AggCount ||
																				   subQuery.aggregations_[0].Type() == AggCountCached))) {
			subQuery.Limit(0);
		}
	}
	std::ignore = entries_.Append<SubQueryEntry>(op, cond, subQueries_.size() - 1, std::move(values));
}

void Query::addConditionSubQuery(OpType op, Query&& subQuery, CondType cond, VariantArray values) {
	addConditionSubQuery<>(op, std::move(subQuery), cond, std::move(values));
}

namespace concepts {

template <typename T>
concept IsQuery = std::is_same_v<std::remove_cvref_t<T>, Query>;

}  // namespace concepts

template <concepts::IsQuery LHS>
static LHS&& forwardSubQuery(LHS&& query, auto&&) noexcept {
	return std::forward<LHS>(query);
}
template <concepts::IsQuery RHS>
static RHS&& forwardSubQuery(auto&&, RHS&& query) noexcept {
	return std::forward<RHS>(query);
}

template <concepts::IsQuery LHS>
static const LHS& constRefSubQuery(LHS&& query, auto&&) noexcept {
	return query;
}
template <concepts::IsQuery RHS>
static const RHS& constRefSubQuery(auto&&, RHS&& query) noexcept {
	return query;
}

template <typename QE, typename LHS, typename RHS>
void Query::addConditionSubQuery(OpType op, LHS&& lhs, CondType cond, RHS&& rhs) {
	if (cond == CondDWithin) [[unlikely]] {
		throw Error(errLogic, "DWithin between field and subquery");
	}
	validateWalQueryNoJoinMergeSubquery();
	constRefSubQuery(lhs, rhs).checkSubQueryWithData();
	subQueries_.emplace_back(forwardSubQuery(std::forward<LHS>(lhs), std::forward<RHS>(rhs)));
	auto guard = MakeScopeGuard([this]() noexcept { return subQueries_.pop_back(); });
	Query& subQuery = subQueries_.back();
	adoptNested(subQuery);
	if (subQuery.hasCalcTotal() || (!subQuery.aggregations_.empty() &&
									(subQuery.aggregations_[0].Type() == AggCount || subQuery.aggregations_[0].Type() == AggCountCached))) {
		subQuery.Limit(0);
	}
	if constexpr (concepts::IsQuery<LHS>) {
		// NOLINTNEXTLINE(bugprone-use-after-move)
		std::ignore = entries_.Append<QE>(op, subQueries_.size() - 1, cond, std::forward<RHS>(rhs));
	} else {
		// NOLINTNEXTLINE(bugprone-use-after-move)
		std::ignore = entries_.Append<QE>(op, std::forward<LHS>(lhs), cond, subQueries_.size() - 1);
	}
	guard.Disable();
}

void Query::addConditionSubQuery(OpType op, std::string field, CondType cond, Query&& subQuery) {
	addConditionSubQuery<SubQueryFieldEntry>(op, std::move(field), cond, std::move(subQuery));
}
void Query::addConditionFunctionSubQuery(OpType op, functions::FunctionVariant&& function, CondType cond, Query&& subQuery) {
	checkFunctionForLeftExpression(std::visit([](const auto& fn) { return fn.Type(); }, function));
	addConditionSubQuery<SubQueryFunctionEntry>(op, std::move(function), cond, std::move(subQuery));
}
void Query::addConditionSubQueryFunction(OpType op, Query&& subQuery, CondType cond, functions::FunctionVariant&& function) {
	checkFunctionForRightExpression(std::visit([](const auto& fn) { return fn.Type(); }, function));
	addConditionSubQuery<SubQueryFunctionEntry>(op, std::move(subQuery), cond, std::move(function));
}

bool Query::tryUpdateQueryEntryInplace(size_t i, VariantArray& values) {
	QueryEntryValidator<QueryEntry>::Validate(*this, entries_.GetOperation(i), values);
	return entries_.TryUpdateInplace<QueryEntry>(i, values);
}

Query& Query::EqualPositions(EqualPosition_t&& ep) & {
	if (ep.size() < 2) [[unlikely]] {
		throw Error(errParams, "EqualPosition must have at least 2 field. Fields: [{}]", ep.size() == 1 ? ep[0] : "");
	}
	QueryEntriesBracket* bracketPointer = entries_.LastOpenBracket();

	if (bracketPointer == nullptr) {
		entries_.equalPositions.emplace_back(std::move(ep));
	} else {
		bracketPointer->equalPositions.emplace_back(std::move(ep));
	}
	return *this;
}

Query& Query::Aggregate(AggType type, h_vector<std::string, 1> fields, const std::vector<std::pair<std::string, bool>>& sort,
						unsigned limit, unsigned offset) & {
	if (!canAddAggregation(type)) [[unlikely]] {
		throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
	}
	SortingEntries sorting;
	sorting.reserve(sort.size());
	for (const auto& s : sort) {
		sorting.emplace_back(s.first, Desc(s.second));
	}
	aggregations_.emplace_back(type, std::move(fields), std::move(sorting), limit, offset);
	return *this;
}

Query& Query::Aggregate(AggType type, h_vector<std::string, 1>&& fields, SortingEntries&& sort, unsigned limit, unsigned offset) & {
	if (!canAddAggregation(type)) [[unlikely]] {
		throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
	}
	aggregations_.emplace_back(type, std::move(fields), std::move(sort), limit, offset);
	return *this;
}

void Query::join(OpType op, JoinedQuery&& jq) {
	assertrx_dbg(nextOp_ == OpAnd);
	validateWalQueryNoJoinMergeSubquery();
	jq.checkJoinedSubQuery();
	switch (jq.getJoinType()) {
		case JoinType::Merge:
			if (op != OpAnd) [[unlikely]] {
				throw Error(errParams, "Merge query with {} operation", OpTypeToStr(op));
			}
			mergeQueries_.emplace_back(std::move(jq));
			adoptNested(mergeQueries_.back());
			return;
		case JoinType::LeftJoin:
			if (op != OpAnd) [[unlikely]] {
				throw Error(errParams, "Left join with {} operation", OpTypeToStr(op));
			}
			break;
		case JoinType::OrInnerJoin:
			if (op == OpNot) [[unlikely]] {
				throw Error(errParams, "Or inner join with {} operation", OpTypeToStr(op));
			}
			op = OpOr;
			[[fallthrough]];
		case JoinType::InnerJoin:
			std::ignore = entries_.Append(op, JoinQueryEntry(joinQueries_.size()));
			break;
	}
	joinQueries_.emplace_back(std::move(jq));
	adoptNested(joinQueries_.back());
}

void Query::checkSetObjectValue(const Variant& value) const {
	if (!value.Type().Is<KeyValueType::String>()) [[unlikely]] {
		throw Error(errLogic, "Unexpected variant type in SetObject: {}. Expecting KeyValueType::String with JSON-content",
					value.Type().Name());
	}
}

VariantArray Query::deserializeValues(Serializer& ser, CondType cond) const {
	VariantArray values;
	if (cond == CondDWithin) {
		if (const auto cnt = ser.GetVarUInt(); cnt != 3) [[unlikely]] {
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
		auto cnt = ser.GetVarUIntCount();
		values.reserve(static_cast<size_t>(cnt));
		while (cnt--) {
			values.emplace_back(ser.GetVariant().EnsureHold());
		}
	}
	return values;
}

void Query::deserializeJoinOn(Serializer&) { throw Error(errLogic, "Unexpected call. JoinOn actual only for JoinQuery"); }

void Query::deserialize(Serializer& ser) { deserialize(ser, QueryFormatV2); }

void Query::deserialize(Serializer& ser, QueryFormat queryFormat) {
	bool end = false;
	std::vector<std::pair<size_t, EqualPosition_t>> equalPositions;
	while (!end && !ser.Eof()) {
		QueryItemType qtype = QueryItemType(ser.GetVarUInt());
		switch (qtype) {
			case QueryCondition: {
				const auto fieldName = ser.GetVString();
				const OpType op = OpType(ser.GetVarUInt());
				const CondType condition = CondType(ser.GetVarUInt());
				VariantArray values = deserializeValues(ser, condition);
				addCondition<QueryEntry>(op, std::string{fieldName}, condition, std::move(values));
				break;
			}
			case QueryKnnCondition: {
				const auto fieldName = ser.GetVString();
				const OpType op = OpType(ser.GetVarUInt());
				const auto vect = ser.GetFloatVectorView();
				addCondition<KnnQueryEntry>(op, std::string{fieldName}, vect, KnnSearchParams::Deserialize(ser));
				break;
			}
			case QueryKnnConditionExt: {
				const auto fieldName = ser.GetVString();
				const auto op = OpType(ser.GetVarUInt());
				const auto fmt = KnnQueryEntry::DataFormatType(ser.GetVarUInt());
				switch (fmt) {
					case KnnQueryEntry::DataFormatType::String: {
						const auto text = ser.GetVString();
						addCondition<KnnQueryEntry>(op, std::string{fieldName}, std::string(text), KnnSearchParams::Deserialize(ser));
						break;
					}
					case KnnQueryEntry::DataFormatType::Vector: {
						const auto vect = ser.GetFloatVectorView();
						addCondition<KnnQueryEntry>(op, std::string{fieldName}, vect, KnnSearchParams::Deserialize(ser));
						break;
					}
					case KnnQueryEntry::DataFormatType::None:
					[[unlikely]]
					default:
						[[unlikely]] throw Error(errParams, "Unexpected type for KNN condition: {}", int(fmt));
				}
				break;
			}
			case QueryBetweenFieldsCondition: {
				OpType op = OpType(ser.GetVarUInt());
				std::string firstField{ser.GetVString()};
				CondType condition = static_cast<CondType>(ser.GetVarUInt());
				std::string secondField{ser.GetVString()};
				addCondition<BetweenFieldsQueryEntry>(op, std::move(firstField), condition, std::move(secondField));
				break;
			}
			case QueryAlwaysFalseCondition: {
				const OpType op = OpType(ser.GetVarUInt());
				addCondition<AlwaysFalse>(op);
				break;
			}
			case QueryAlwaysTrueCondition: {
				const OpType op = OpType(ser.GetVarUInt());
				addCondition<AlwaysTrue>(op);
				break;
			}
			case QueryJoinCondition: {
				uint64_t type = ser.GetVarUInt();
				assertrx(type != JoinType::LeftJoin);
				JoinQueryEntry joinEntry(ser.GetVarUInt());
				addCondition<JoinQueryEntry>((type == JoinType::OrInnerJoin) ? OpOr : OpAnd, std::move(joinEntry));
				break;
			}
			case QueryJoinOn: {
				deserializeJoinOn(ser);
				break;
			}
			case QueryAggregation: {
				const AggType type = static_cast<AggType>(ser.GetVarUInt());
				size_t fieldsCount = static_cast<size_t>(ser.GetVarUIntCount());
				h_vector<std::string, 1> fields;
				fields.reserve(fieldsCount);
				while (fieldsCount--) {
					fields.emplace_back(std::string(ser.GetVString()));
				}
				auto pos = ser.Pos();
				bool aggEnd = false;
				aggregations_.emplace_back(type, std::move(fields));
				auto& ae = aggregations_.back();
				while (!ser.Eof() && !aggEnd) {
					auto atype = ser.GetVarUInt();
					switch (atype) {
						case QueryAggregationSort: {
							auto fieldName = ser.GetVString();
							ae.AddSortingEntry({std::string(fieldName), Desc{ser.GetVarUInt() != 0}});
							break;
						}
						case QueryAggregationLimit:
							ae.SetLimit(ser.GetVarUInt());
							break;
						case QueryAggregationOffset:
							ae.SetOffset(ser.GetVarUInt());
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
					addCondition<QueryEntry>(OpAnd, std::string{fieldName}, QueryEntry::DistinctTag{});
				}
				break;
			}
			case QuerySortIndex: {
				SortingEntry sortingEntry;
				sortingEntry.expression = std::string(ser.GetVString());
				sortingEntry.desc = Desc(bool(ser.GetVarUInt()));
				if (sortingEntry.expression.length()) {
					sortingEntries_.push_back(std::move(sortingEntry));
				}
				auto cnt = ser.GetVarUIntCount();
				if (cnt != 0 && sortingEntries_.size() != 1) [[unlikely]] {
					throw Error(errParams, "Forced sort order is allowed for the first sorting entry only");
				}
				forcedSortOrder_.reserve(static_cast<size_t>(cnt));
				while (cnt--) {
					auto v = ser.GetVariant();
					if (v.IsNullValue()) [[unlikely]] {
						throw Error(errParams, "Null-values are not supported in forced sorting");
					}
					forcedSortOrder_.emplace_back(std::move(v.EnsureHold()));
				}
				break;
			}
			case QueryDebugLevel:
				Debug(ser.GetVarUInt());
				break;
			case QueryStrictMode:
				Strict(StrictMode(ser.GetVarUInt()));
				break;
			case QueryLimit:
				limit_ = ser.GetVarUInt();
				break;
			case QueryOffset:
				offset_ = ser.GetVarUInt();
				break;
			case QueryReqTotal:
				calcTotal_ = CalcTotalMode(ser.GetVarUInt());
				break;
			case QuerySelectFilter:
				selectFilter_.Add(ser.GetVString(), *this);
				break;
			case QueryEqualPosition: {
				const unsigned bracketPosition = ser.GetVarUInt();
				const unsigned fieldsCount = ser.GetVarUInt();
				equalPositions.emplace_back(bracketPosition, fieldsCount);
				for (auto& field : equalPositions.back().second) {
					field = ser.GetVString();
				}
				break;
			}
			case QueryExplain:
				Explain(true);
				break;
			case QueryLocal:
				local_ = true;
				break;
			case QueryWithRank:
				withRank_ = true;
				break;
			case QuerySelectFunction:
				AddFunction(ser.GetVString());
				break;
			case QueryDropField: {
				Drop(ser.GetVString());
				break;
			}
			case QueryUpdateFieldV2: {
				VariantArray val;
				std::string field(ser.GetVString());
				bool isArray = ser.GetVarUInt();
				auto numValues = ser.GetVarUInt();
				auto hasExpressions = HasExpression_False;
				while (numValues--) {
					hasExpressions = HasExpression(ser.GetVarUInt());
					val.emplace_back(ser.GetVariant().EnsureHold());
				}
				Set(std::move(field), std::move(val.MarkArray(isArray)), hasExpressions);
				break;
			}
			case QueryUpdateField: {
				VariantArray val;
				std::string field(ser.GetVString());
				auto numValues = ser.GetVarUInt();
				bool isArray = numValues > 1;
				auto hasExpressions = HasExpression_False;
				while (numValues--) {
					hasExpressions = HasExpression(ser.GetVarUInt());
					val.emplace_back(ser.GetVariant().EnsureHold());
				}
				Set(std::move(field), std::move(val.MarkArray(isArray)), hasExpressions);
				break;
			}
			case QueryUpdateObject: {
				VariantArray val;
				std::string field(ser.GetVString());
				auto hasExpressions = HasExpression_False;
				auto numValues = ser.GetVarUInt();
				std::ignore = val.MarkArray(ser.GetVarUInt() == 1);
				while (numValues--) {
					hasExpressions = HasExpression(ser.GetVarUInt());
					val.emplace_back(ser.GetVariant().EnsureHold());
				}
				SetObject(std::move(field), std::move(val), hasExpressions);
				break;
			}
			case QueryOpenBracket: {
				OpType op = OpType(ser.GetVarUInt());
				nextOp(op);
				OpenBracket();
				break;
			}
			case QueryCloseBracket:
				CloseBracket();
				break;
			case QueryEnd:
				end = true;
				break;
			case QuerySubQueryCondition: {
				OpType op = OpType(ser.GetVarUInt());
				Serializer subQuery{ser.GetVString()};
				CondType condition = CondType(ser.GetVarUInt());
				VariantArray values = deserializeValues(ser, condition);
				addConditionSubQuery(op, Query::deserialize<Query>(subQuery, queryFormat), condition, std::move(values));
				break;
			}
			case QueryFieldSubQueryCondition: {
				OpType op = OpType(ser.GetVarUInt());
				const auto fieldName = ser.GetVString();
				CondType condition = CondType(ser.GetVarUInt());
				Serializer subQuery{ser.GetVString()};
				addConditionSubQuery(op, std::string(fieldName), condition, Query::deserialize<Query>(subQuery, queryFormat));
				break;
			}
			case QueryFunctionSubQueryCondition:
			[[unlikely]]
			case QueryFunction:
				[[unlikely]] throw Error{errParseBin, "Serialization type={} is deprecated", int(qtype)};
			case QueryExpressions: {
				auto left = expressions::Deserialize(ser, queryFormat);
				auto leftType = expressions::GetValueType(left);
				OpType op = OpType(ser.GetVarUInt());
				CondType condition = CondType(ser.GetVarUInt());
				auto right = expressions::Deserialize(ser, queryFormat);
				auto rightType = expressions::GetValueType(right);
				expressions::ValidateExpressions(leftType, rightType, expressions::ValidationType::Full);
				auto throwUnsupportedCombo = [&]() {
					throw Error{errParseBin, "Unsupported expression combination: {} vs {}", expressions::ExpressionTypeToString(leftType),
								expressions::ExpressionTypeToString(rightType)};
				};
				switch (leftType) {
					case ExpressionTypeField: {
						std::string fieldName = std::get<std::string>(std::move(left));
						switch (rightType) {
							case ExpressionTypeValues:
								addCondition<QueryEntry>(op, std::move(fieldName), condition, std::get<VariantArray>(std::move(right)));
								break;
							case ExpressionTypeExpression:
								std::visit([&](auto& fn) { addConditionFunction(op, std::move(fieldName), condition, std::move(fn)); },
										   std::get<functions::FunctionVariant>(right));
								break;
							case ExpressionTypeSubQuery:
								addConditionSubQuery(op, std::move(fieldName), condition, std::get<Query>(std::move(right)));
								break;
							case ExpressionTypeField:
								addCondition<BetweenFieldsQueryEntry>(op, std::move(fieldName), condition,
																	  std::get<std::string>(std::move(right)));
								break;
							case ExpressionTypeArithmetic:
								addCondition<QueryArithmeticEntry>(op, std::move(fieldName), condition,
																   std::get<expressions::ArithmeticExpression>(std::move(right)));
								break;
							default:
								[[unlikely]] throwUnsupportedCombo();
						}
						break;
					}
					case ExpressionTypeArithmetic: {
						auto expr = std::get<expressions::ArithmeticExpression>(std::move(left));
						switch (rightType) {
							case ExpressionTypeValues:
								addCondition<QueryArithmeticEntry>(op, std::move(expr), condition,
																   std::get<VariantArray>(std::move(right)));
								break;
							case ExpressionTypeField:
								addCondition<QueryArithmeticEntry>(op, std::move(expr), condition, std::get<std::string>(std::move(right)));
								break;
							case ExpressionTypeArithmetic:
								addCondition<QueryArithmeticEntry>(op, std::move(expr), condition,
																   std::get<expressions::ArithmeticExpression>(std::move(right)));
								break;
							case ExpressionTypeExpression:
							case ExpressionTypeSubQuery:
							default:
								[[unlikely]] throwUnsupportedCombo();
						}
						break;
					}
					case ExpressionTypeExpression: {
						functions::FunctionVariant func = std::get<functions::FunctionVariant>(std::move(left));
						if (rightType == ExpressionTypeValues) {
							std::visit(
								[&](auto& fn) {
									addConditionFunction(op, std::move(fn), condition, std::get<VariantArray>(std::move(right)));
								},
								func);
						} else if (rightType == ExpressionTypeSubQuery) {
							addConditionFunctionSubQuery(op, std::move(func), condition, std::get<Query>(std::move(right)));
						} else {
							throwUnsupportedCombo();
						}
						break;
					}
					case ExpressionTypeSubQuery: {
						Query subquery = std::get<Query>(std::move(left));
						if (rightType == ExpressionTypeValues) {
							addConditionSubQuery(op, std::move(subquery), condition, std::get<VariantArray>(std::move(right)));
						} else if (rightType == ExpressionTypeExpression) {
							addConditionSubQueryFunction(op, std::move(subquery), condition,
														 std::get<functions::FunctionVariant>(std::move(right)));
						} else {
							throwUnsupportedCombo();
						}
						break;
					}
					case ExpressionTypeValues:
					[[unlikely]]
					default:
						[[unlikely]] throwUnsupportedCombo();
				}
				break;
			}
			case QueryAggregationSort:
			[[unlikely]]
			case QueryAggregationOffset:
			[[unlikely]]
			case QueryAggregationLimit:
			[[unlikely]]
			default:
				[[unlikely]] throw Error(errParseBin, "Unknown type {} while parsing binary buffer", int(qtype));
		}
	}
	for (auto&& eqPos : equalPositions) {
		if (eqPos.first == 0) {
			entries_.equalPositions.emplace_back(std::move(eqPos.second));
		} else {
			const auto bracketIdx = eqPos.first - 1;
			if (!entries_.Is<QueryEntriesBracket>(bracketIdx)) [[unlikely]] {
				throw Error(errParseBin, "Invalid bracket offset in equal_position: {}", bracketIdx);
			}
			entries_.Get<QueryEntriesBracket>(bracketIdx).equalPositions.emplace_back(std::move(eqPos.second));
		}
	}
}

void Query::serializeJoinEntries(WrSerializer&) const { throw Error(errLogic, "Unexpected call. JoinEntries actual only for JoinQuery"); }

void Query::serialize(WrSerializer& ser, uint8_t mode, QueryFormat queryFormat) const {
	const bool withJoinQueries{!(mode & SkipJoinQueries)};
	if (queryFormat == QueryFormatV2) {
		ser.PutVarUint(QueryFormatV2);
	} else if (withJoinQueries) {
		for (const auto& jq : joinQueries_) {
			if (!jq.joinQueries().empty()) [[unlikely]] {
				throw Error(errParams, "Nested JOINs are not supported by QueryFormatV1");
			}
		}
	}
	ser.PutVString(NsName());
	entries_.Serialize(ser, subQueries_, queryFormat);

	if (!(mode & SkipAggregations)) {
		for (const auto& agg : aggregations_) {
			ser.PutVarUint(QueryAggregation);
			ser.PutVarUint(agg.Type());
			ser.PutVarUint(agg.Fields().size());
			for (const auto& field : agg.Fields()) {
				ser.PutVString(field);
			}
			for (const auto& se : agg.Sorting()) {
				ser.PutVarUint(QueryAggregationSort);
				ser.PutVString(se.expression);
				ser.PutVarUint(*se.desc);
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
		for (size_t i = 0, size = sortingEntries_.size(); i < size; ++i) {
			const auto& sortginEntry = sortingEntries_[i];
			ser.PutVarUint(QuerySortIndex);
			ser.PutVString(sortginEntry.expression);
			ser.PutVarUint(*sortginEntry.desc);
			if (i == 0) {
				int cnt = forcedSortOrder_.size();
				ser.PutVarUint(cnt);
				for (auto& kv : forcedSortOrder_) {
					ser.PutVariant(kv);
				}
			} else {
				ser.PutVarUint(0);
			}
		}
	}

	if (mode & WithJoinEntries) {
		serializeJoinEntries(ser);
	}

	for (const auto& equalPoses : entries_.equalPositions) {
		ser.PutVarUint(QueryEqualPosition);
		ser.PutVarUint(0);
		ser.PutVarUint(equalPoses.size());
		for (const auto& ep : equalPoses) {
			ser.PutVString(ep);
		}
	}
	for (size_t i = 0; i < entries_.Size(); ++i) {
		if (entries_.IsSubTree(i)) {
			const auto& bracket = entries_.Get<QueryEntriesBracket>(i);
			for (const auto& equalPoses : bracket.equalPositions) {
				ser.PutVarUint(QueryEqualPosition);
				ser.PutVarUint(i + 1);
				ser.PutVarUint(equalPoses.size());
				for (const auto& ep : equalPoses) {
					ser.PutVString(ep);
				}
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

	for (const auto& funcText : selectFunctions_) {
		ser.PutVarUint(QueryItemType::QuerySelectFunction);
		ser.PutVString(funcText);
	}

	if (!(mode & SkipLimitOffset)) {
		if (hasLimit()) {
			ser.PutVarUint(QueryLimit);
			ser.PutVarUint(limit());
		}
		if (hasOffset()) {
			ser.PutVarUint(QueryOffset);
			ser.PutVarUint(offset());
		}
	}

	if (!(mode & SkipExtraParams)) {
		if (hasCalcTotal()) {
			ser.PutVarUint(QueryReqTotal);
			ser.PutVarUint(calcTotal());
		}

		for (const auto& sf : selectFilter_.Fields()) {
			ser.PutVarUint(QuerySelectFilter);
			ser.PutVString(sf);
		}
		if (selectFilter_.AllRegularFields() && !selectFilter_.Empty()) {
			ser.PutVarUint(QuerySelectFilter);
			ser.PutVString(FieldsNamesFilter::kAllRegularFieldsName);
		}
		if (selectFilter_.AllVectorFields()) {
			ser.PutVarUint(QuerySelectFilter);
			ser.PutVString(FieldsNamesFilter::kAllVectorFieldsName);
		}

		if (explain_) {
			ser.PutVarUint(QueryExplain);
		}
		if (local_) {
			ser.PutVarUint(QueryLocal);
		}
		if (withRank_) {
			ser.PutVarUint(QueryWithRank);
		}
	}

	for (const auto& field : updateFields_) {
		if (field.Mode() == FieldModeSet) {
			ser.PutVarUint(QueryUpdateFieldV2);
			ser.PutVString(field.Column());
			ser.PutVarUint(field.Values().IsArrayValue());
			ser.PutVarUint(field.Values().size());
			for (const Variant& val : field.Values()) {
				ser.PutVarUint(field.IsExpression());
				ser.PutVariant(val);
			}
		} else if (field.Mode() == FieldModeDrop) {
			ser.PutVarUint(QueryDropField);
			ser.PutVString(field.Column());
		} else if (field.Mode() == FieldModeSetJson) {
			ser.PutVarUint(QueryUpdateObject);
			ser.PutVString(field.Column());
			ser.PutVarUint(field.Values().size());
			ser.PutVarUint(field.Values().IsArrayValue());
			for (const Variant& val : field.Values()) {
				ser.PutVarUint(field.IsExpression());
				ser.PutVariant(val);
			}
		} else [[unlikely]] {
			throw Error(errLogic, "Unsupported item modification mode = {}", int(field.Mode()));
		}
	}

	ser.PutVarUint(QueryEnd);  // finita la commedia... of root query

	if (queryFormat == QueryFormatV2) {
		ser.PutVarUint(withJoinQueries ? static_cast<int>(joinQueries_.size()) : 0);
		if (withJoinQueries) {
			for (const auto& jq : joinQueries_) {
				if (!(mode & SkipLeftJoinQueries) || jq.getJoinType() != JoinType::LeftJoin) {
					ser.PutVarUint(static_cast<int>(jq.getJoinType()));
					jq.serialize(ser, WithJoinEntries, queryFormat);
				}
			}
		}

		const bool withMergeQueries{!(mode & SkipMergeQueries)};
		ser.PutVarUint(withMergeQueries ? static_cast<int>(mergeQueries_.size()) : 0);
		if (withMergeQueries) {
			for (const auto& mq : mergeQueries_) {
				ser.PutVarUint(static_cast<int>(mq.getJoinType()));
				mq.serialize(ser, (mode | WithJoinEntries) & (~SkipSortEntries), queryFormat);
			}
		}
	} else {
		if (withJoinQueries) {
			for (const auto& jq : joinQueries_) {
				if (!(mode & SkipLeftJoinQueries) || jq.getJoinType() != JoinType::LeftJoin) {
					ser.PutVarUint(static_cast<int>(jq.getJoinType()));
					jq.serialize(ser, WithJoinEntries, queryFormat);
				}
			}
		}

		if (!(mode & SkipMergeQueries)) {
			for (const auto& mq : mergeQueries_) {
				ser.PutVarUint(static_cast<int>(mq.getJoinType()));
				mq.serialize(ser, (mode | WithJoinEntries) & (~SkipSortEntries), queryFormat);
			}
		}
	}
}

template <typename T>
T Query::deserializeImpl(Serializer& ser, QueryFormat queryFormat, auto... args) {
	auto validateJoinType = [](JoinType joinType) {
		if (joinType < JoinType::LeftJoin || joinType > JoinType::Merge) [[unlikely]] {
			throw Error(errParams, "Unexpected join type in serialized query: {}", int(joinType));
		}
	};
	std::function<void(const Query&)> checkJoinEntries;
	checkJoinEntries = [&checkJoinEntries](const Query& q) {
		q.entries().VisitForEach(
			[size = q.joinQueries().size()](const JoinQueryEntry& qe) {
				if (qe.joinIndex >= size) [[unlikely]] {
					throw Error(errQueryExec, "Invalid index for joined query after deserialization.");
				}
			},
			[](const auto&) noexcept {});
		for (const auto& jq : q.joinQueries()) {
			checkJoinEntries(jq);
		}
	};
	auto checkJoinEntriesV1 = [](const Query& q) {
		q.entries().VisitForEach(
			[size = q.joinQueries().size()](const JoinQueryEntry& qe) {
				if (qe.joinIndex >= size) [[unlikely]] {
					throw Error(errQueryExec, "Invalid index for joined query after deserialization.");
				}
			},
			[](const auto&) noexcept {});
	};

	if (queryFormat == QueryFormatV2) {
		if (const uint64_t format{ser.GetVarUInt()}; format != QueryFormatV2) [[unlikely]] {
			throw Error(errParseBin, "Unsupported Query format version='{}'", format);
		}
	}

	T res{args..., ser.GetVString()};
	res.deserialize(ser, queryFormat);

	if (queryFormat == QueryFormatV2) {
		const auto joinQueriesCount = ser.GetVarUIntCount();
		if (joinQueriesCount > 0) {
			res.validateWalQueryNoJoinMergeSubquery();
			res.joinQueries_.reserve(static_cast<size_t>(joinQueriesCount));
			for (size_t i = 0; i < joinQueriesCount; ++i) {
				const auto joinType{JoinType(ser.GetVarUInt())};
				validateJoinType(joinType);
				res.joinQueries_.emplace_back(JoinedQuery::deserializeImpl<JoinedQuery>(ser, queryFormat, joinType));
				res.joinQueries_.back().validateWalQueryNoJoinMergeSubquery();
				res.adoptNested(res.joinQueries_.back());
			}
		}

		const auto mergeQueriesCount = ser.GetVarUIntCount();
		if (mergeQueriesCount > 0) {
			res.mergeQueries_.reserve(static_cast<size_t>(mergeQueriesCount));
			for (size_t i = 0; i < mergeQueriesCount; ++i) {
				const auto mergeType{JoinType(ser.GetVarUInt())};
				if (mergeType != JoinType::Merge) [[unlikely]] {
					throw Error(errParams, "Unexpected merge query type in serialized query: {}", int(mergeType));
				}
				res.mergeQueries_.emplace_back(JoinedQuery::deserializeImpl<JoinedQuery>(ser, queryFormat, mergeType));
				res.mergeQueries_.back().validateWalQueryNoJoinMergeSubquery();
				res.adoptNested(res.mergeQueries_.back());
			}
		}

		checkJoinEntries(res);
		for (const auto& mergeQuery : res.mergeQueries()) {
			checkJoinEntries(mergeQuery);
		}
	} else {
		bool nested{false};
		while (!ser.Eof()) {
			auto joinType{JoinType(ser.GetVarUInt())};
			validateJoinType(joinType);
			res.validateWalQueryNoJoinMergeSubquery();
			JoinedQuery q1{joinType, Query{std::string(ser.GetVString())}};
			q1.deserialize(ser, queryFormat);
			q1.validateWalQueryNoJoinMergeSubquery();
			res.adoptNested(q1);
			if (joinType == JoinType::Merge) {
				res.mergeQueries_.emplace_back(std::move(q1));
				nested = true;
			} else {
				Query& q{nested ? res.mergeQueries_.back() : res};
				q.joinQueries_.emplace_back(std::move(q1));
				q.adoptNested(q.joinQueries_.back());
			}
		}
		checkJoinEntriesV1(res);
		for (const auto& mergeQuery : res.mergeQueries()) {
			checkJoinEntriesV1(mergeQuery);
		}
	}

	return res;
}
template Query Query::deserialize<Query>(Serializer& ser, QueryFormat queryFormat);

Query Query::Deserialize(Serializer& ser, QueryFormat queryFormat) { return deserialize<Query>(ser, queryFormat); }
void Query::Serialize(WrSerializer& ser, QueryFormat queryFormat) const { serialize(ser, Normal, queryFormat); }

Query& Query::Merge(Query&& q) & {
	validateWalQueryNoJoinMergeSubquery();
	q.validateWalQueryNoJoinMergeSubquery();
	mergeQueries_.emplace_back(JoinType::Merge, std::move(q));
	adoptNested(mergeQueries_.back());
	return *this;
}

Query& Query::SortStDistance(std::string_view field, Point p, SortOrder sortOrder) & {
	if (field.empty()) [[unlikely]] {
		throw Error(errParams, "Field name for ST_Distance can not be empty");
	}
	sortingEntries_.emplace_back(fmt::format("ST_Distance({},ST_GeomFromText('point({:.12f} {:.12f})'))", field, p.X(), p.Y()),
								 Desc{sortOrder == SortOrder::Desc});
	return *this;
}

Query& Query::SortStDistance(std::string_view field1, std::string_view field2, SortOrder sortOrder) & {
	if (field1.empty() || field2.empty()) [[unlikely]] {
		throw Error(errParams, "Fields names for ST_Distance can not be empty");
	}
	sortingEntries_.emplace_back(fmt::format("ST_Distance({},{})", field1, field2), Desc{sortOrder == SortOrder::Desc});
	return *this;
}

void Query::walkNested(bool withSelf, bool withMerged, bool withSubQueries,
					   const std::function<void(Query& q)>& visitor) noexcept(noexcept(visitor(std::declval<Query&>()))) {
	if (withSelf) {
		visitor(*this);
	}
	if (withMerged) {
		for (auto& mq : mergeQueries_) {
			visitor(mq);
		}
		if (withSubQueries) {
			for (auto& mq : mergeQueries_) {
				for (auto& nq : mq.subQueries_) {
					nq.walkNested(true, true, true, visitor);
				}
			}
		}
	}
	for (auto& jq : joinQueries_) {
		jq.walkNested(true, withMerged, withSubQueries, visitor);
	}
	for (auto& mq : mergeQueries_) {
		for (auto& jq : mq.joinQueries_) {
			jq.walkNested(true, withMerged, withSubQueries, visitor);
		}
	}
	if (withSubQueries) {
		for (auto& nq : subQueries_) {
			nq.walkNested(true, withMerged, true, visitor);
		}
	}
}

void ConstQueryImpl::WalkNested(bool withSelf, bool withMerged, bool withSubQueries,
								const std::function<void(ConstQueryImpl)>& visitor) const {
	if (withSelf) {
		visitor(*this);
	}
	if (withMerged) {
		for (const auto& mq : MergeQueries()) {
			visitor(Impl(mq));
		}
		if (withSubQueries) {
			for (const auto& mq : MergeQueries()) {
				for (const auto& nq : mq.subQueries()) {
					Impl(nq).WalkNested(true, true, true, visitor);
				}
			}
		}
	}
	for (const auto& jq : JoinQueries()) {
		Impl(jq).WalkNested(true, withMerged, withSubQueries, visitor);
	}
	for (const auto& mq : MergeQueries()) {
		for (const auto& jq : mq.joinQueries()) {
			Impl(jq).WalkNested(true, withMerged, withSubQueries, visitor);
		}
	}
	if (withSubQueries) {
		for (const auto& nq : SubQueries()) {
			Impl(nq).WalkNested(true, withMerged, true, visitor);
		}
	}
}

bool Query::hasJoinQueries() const noexcept {
	bool hasJoins = false;
	Impl(*this).WalkNested(true, true, false, [&hasJoins](ConstQueryImpl q) noexcept { hasJoins |= !q.JoinQueries().empty(); });
	return hasJoins;
}

void Query::replaceSubQuery(size_t i, Query&& query) {
	query.validateWalQueryNoJoinMergeSubquery();
	subQueries_.at(i) = std::move(query);
}
void Query::replaceJoinQuery(size_t i, JoinedQuery&& query) {
	query.validateWalQueryNoJoinMergeSubquery();
	joinQueries_.at(i) = std::move(query);
}
void Query::replaceMergeQuery(size_t i, JoinedQuery&& query) {
	query.validateWalQueryNoJoinMergeSubquery();
	mergeQueries_.at(i) = std::move(query);
}
bool Query::hasVolatileExpressions() const noexcept {
	bool has = false;
	entries_.VisitForEach(
		[&has](const QueryArithmeticEntry& qe) noexcept { has = has || qe.UsesNow(); },
		[&has](const QueryFunctionEntry& qe) noexcept { has = has || std::holds_alternative<functions::Now>(qe.FunctionVariant()); },
		[&has](const SubQueryFunctionEntry& qe) noexcept { has = has || std::holds_alternative<functions::Now>(qe.FunctionVariant()); },
		[](const auto&) noexcept {});
	if (has) {
		return true;
	}
	for (const auto& jq : joinQueries_) {
		if (jq.hasVolatileExpressions()) {
			return true;
		}
	}
	for (const auto& mq : mergeQueries_) {
		if (mq.hasVolatileExpressions()) {
			return true;
		}
	}
	for (const auto& nq : subQueries_) {
		if (nq.hasVolatileExpressions()) {
			return true;
		}
	}
	return false;
}
std::span<JoinedQuery> Query::getJoinQueriesSpan() & noexcept { return {joinQueries_.data(), joinQueries_.size()}; }

void JoinedQuery::deserializeJoinOn(Serializer& ser) {
	const OpType op = static_cast<OpType>(ser.GetVarUInt());
	const CondType condition = static_cast<CondType>(ser.GetVarUInt());
	std::string leftFieldName{ser.GetVString()};
	std::string rightFieldName{ser.GetVString()};
	if (joinEntries_.empty() && op == OpOr) [[unlikely]] {
		throw Error{errLogic, "OR operator in first condition in ON"};
	}
	joinEntries_.emplace_back(op, std::move(leftFieldName), condition, std::move(rightFieldName));
}

void JoinedQuery::serializeJoinEntries(WrSerializer& ser) const {
	for (const auto& qje : joinEntries_) {
		ser.PutVarUint(QueryJoinOn);
		ser.PutVarUint(qje.Operation());
		ser.PutVarUint(qje.Condition());
		ser.PutVString(qje.LeftFieldName());
		ser.PutVString(qje.RightFieldName());
	}
}

Query::OnHelper Query::Join(JoinType joinType, Query&& q) & {
	return {*this, std::exchange(nextOp_, OpAnd), JoinedQuery(joinType, std::move(q))};
}
Query::OnHelperR Query::Join(JoinType joinType, Query&& q) && {
	return {std::move(*this), std::exchange(nextOp_, OpAnd), JoinedQuery(joinType, std::move(q))};
}

}  // namespace reindexer
