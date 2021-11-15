#pragma once

#include <functional>
#include <initializer_list>
#include "core/keyvalue/geometry.h"
#include "queryentry.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class WrSerializer;
class Serializer;
class JoinedQuery;

constexpr std::string_view kAggregationWithSelectFieldsMsgError =
	"Not allowed to combine aggregation functions and fields' filter in a single query";

/// @class Query
/// Allows to select data from DB.
/// Analog to ansi-sql select query.
class Query {
	template <typename Q>
	class OnHelperTempl;
	template <typename Q>
	class OnHelperGroup {
	public:
		OnHelperGroup &&Not() &&noexcept {
			op_ = OpNot;
			return std::move(*this);
		}
		OnHelperGroup &&Or() &&noexcept {
			op_ = OpOr;
			return std::move(*this);
		}
		OnHelperGroup &&On(std::string index, CondType cond, std::string joinIndex) &&;
		Q CloseBracket() &&noexcept { return std::forward<Q>(q_); }

	private:
		OnHelperGroup(Q q, JoinedQuery &jq) noexcept : q_{std::forward<Q>(q)}, jq_{jq} {}
		Q q_;
		JoinedQuery &jq_;
		OpType op_{OpAnd};
		friend class OnHelperTempl<Q>;
	};
	template <typename Q>
	class OnHelperTempl {
	public:
		OnHelperTempl &&Not() &&noexcept {
			op_ = OpNot;
			return std::move(*this);
		}
		Q On(std::string index, CondType cond, std::string joinIndex) &&;
		OnHelperGroup<Q> OpenBracket() &&noexcept { return {std::forward<Q>(q_), jq_}; }

	private:
		OnHelperTempl(Q q, JoinedQuery &jq) noexcept : q_{std::forward<Q>(q)}, jq_{jq} {}
		Q q_;
		JoinedQuery &jq_;
		OpType op_{OpAnd};
		friend class Query;
	};
	using OnHelper = OnHelperTempl<Query &>;
	using OnHelperR = OnHelperTempl<Query &&>;

public:
	/// Creates an object for certain namespace with appropriate settings.
	/// @param nsName - name of the namespace the data to be selected from.
	/// @param start - number of the first row to get from selected set. Analog to sql OFFSET Offset.
	/// @param count - number of rows to get from result set. Analog to sql LIMIT RowsCount.
	/// @param calcTotal - calculation mode.
	explicit Query(const string &nsName, unsigned start = 0, unsigned count = UINT_MAX, CalcTotalMode calcTotal = ModeNoTotal);

	/// Creates an empty object.
	Query() = default;

	/// Allows to compare 2 Query objects.
	bool operator==(const Query &) const;

	/// Parses pure sql select query and initializes Query object data members as a result.
	/// @param q - sql query.
	void FromSQL(std::string_view q);

	/// Logs query in 'Select field1, ... field N from namespace ...' format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	WrSerializer &GetSQL(WrSerializer &ser, bool stripArgs = false) const;

	/// Logs query in 'Select field1, ... field N from namespace ...' format.
	/// @param stripArgs - replace condition values with '?'
	/// @return Query in SQL format
	string GetSQL(bool stripArgs = false) const;

	/// Parses JSON dsl set.
	/// @param dsl - dsl set.
	/// @return always returns errOk or throws an exception.
	Error FromJSON(const string &dsl);

	/// returns structure of a query in JSON dsl format
	string GetJSON() const;

	/// Enable explain query
	/// @param on - signaling on/off
	/// @return Query object ready to be executed
	Query &Explain(bool on = true) & {
		explain_ = on;
		return *this;
	}
	Query &&Explain(bool on = true) && { return std::move(Explain(on)); }

	/// Adds a condition with a single value. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param val - value of index to be compared with.
	/// @return Query object ready to be executed.
	template <typename Input>
	Query &Where(const string &idx, CondType cond, Input val) & {
		return Where(idx, cond, {val});
	}
	template <typename Input>
	Query &&Where(const string &idx, CondType cond, Input val) && {
		return std::move(Where<Input>(idx, cond, {val}));
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param l - list of index values to be compared with.
	/// @return Query object ready to be executed.
	template <typename T>
	Query &Where(const string &idx, CondType cond, std::initializer_list<T> l) & {
		QueryEntry qe;
		qe.condition = cond;
		qe.index = idx;
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(Variant(*it));
		entries.Append(nextOp_, std::move(qe));
		nextOp_ = OpAnd;
		return *this;
	}
	template <typename T>
	Query &&Where(const string &idx, CondType cond, std::initializer_list<T> l) && {
		return std::move(Where<T>(idx, cond, std::move(l)));
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param l - vector of index values to be compared with.
	/// @return Query object ready to be executed.
	template <typename T>
	Query &Where(const string &idx, CondType cond, const std::vector<T> &l) & {
		QueryEntry qe;
		qe.condition = cond;
		qe.index = idx;
		qe.values.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(Variant(*it));
		entries.Append(nextOp_, std::move(qe));
		nextOp_ = OpAnd;
		return *this;
	}
	template <typename T>
	Query &&Where(const string &idx, CondType cond, const std::vector<T> &l) && {
		return std::move(Where<T>(idx, cond, l));
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param l - vector of index values to be compared with.
	/// @return Query object ready to be executed.
	Query &Where(const string &idx, CondType cond, const VariantArray &l) & {
		QueryEntry qe;
		qe.condition = cond;
		qe.index = idx;
		qe.values.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(Variant(*it));
		entries.Append(nextOp_, std::move(qe));
		nextOp_ = OpAnd;
		return *this;
	}
	Query &&Where(const string &idx, CondType cond, const VariantArray &l) && { return std::move(Where(idx, cond, l)); }

	/// Adds a condition with several values to a composite index.
	/// @param idx - index name.
	/// @param cond - type of condition.
	/// @param l - list of values to be compared according to the order
	/// of indexes in composite index name.
	/// There can be maximum 2 VariantArray objects in l: in case of CondRange condition,
	/// in all other cases amount of elements in l would be striclty equal to 1.
	/// For example, composite index name is "bookid+price", so l[0][0] (and l[1][0]
	/// in case of CondRange) belongs to "bookid" and l[0][1] (and l[1][1] in case of CondRange)
	/// belongs to "price" indexes.
	/// @return Query object ready to be executed.
	Query &WhereComposite(const string &idx, CondType cond, std::initializer_list<VariantArray> l) & {
		QueryEntry qe;
		qe.condition = cond;
		qe.index = idx;
		qe.values.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) {
			qe.values.push_back(Variant(*it));
		}
		entries.Append(nextOp_, std::move(qe));
		nextOp_ = OpAnd;
		return *this;
	}
	Query &&WhereComposite(const string &idx, CondType cond, std::initializer_list<VariantArray> l) && {
		return std::move(WhereComposite(idx, cond, std::move(l)));
	}
	Query &WhereComposite(const string &idx, CondType cond, const vector<VariantArray> &v) & {
		QueryEntry qe;
		qe.condition = cond;
		qe.index = idx;
		qe.values.reserve(v.size());
		for (auto it = v.begin(); it != v.end(); it++) {
			qe.values.push_back(Variant(*it));
		}
		entries.Append(nextOp_, std::move(qe));
		nextOp_ = OpAnd;
		return *this;
	}
	Query &&WhereComposite(const string &idx, CondType cond, const vector<VariantArray> &v) && {
		return std::move(WhereComposite(idx, cond, v));
	}

	Query &DWithin(const string &idx, Point p, double distance) & {
		QueryEntry qe;
		qe.condition = CondDWithin;
		qe.index = idx;
		qe.values.reserve(2);
		qe.values.emplace_back(p);
		qe.values.emplace_back(distance);
		entries.Append(nextOp_, std::move(qe));
		nextOp_ = OpAnd;
		return *this;
	}
	Query &&DWithin(const string &idx, Point p, double distance) && { return std::move(DWithin(idx, p, distance)); }

	/// Sets a new value for a field.
	/// @param field - field name.
	/// @param value - new value.
	/// @param hasExpressions - true: value has expresions in it
	template <typename ValueType>
	Query &Set(string field, ValueType value, bool hasExpressions = false) & {
		return Set(std::move(field), {value}, hasExpressions);
	}
	template <typename ValueType>
	Query &&Set(string field, ValueType value, bool hasExpressions = false) && {
		return std::move(Set<ValueType>(std::move(field), std::move(value), hasExpressions));
	}
	/// Sets a new value for a field.
	/// @param field - field name.
	/// @param l - new value.
	/// @param hasExpressions - true: value has expresions in it
	template <typename ValueType>
	Query &Set(string field, std::initializer_list<ValueType> l, bool hasExpressions = false) & {
		VariantArray value;
		value.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) value.emplace_back(*it);
		return Set(std::move(field), std::move(value), hasExpressions);
	}
	template <typename ValueType>
	Query &&Set(string field, std::initializer_list<ValueType> l, bool hasExpressions = false) && {
		return std::move(Set<ValueType>(std::move(field), std::move(l), hasExpressions));
	}
	/// Sets a new value for a field.
	/// @param field - field name.
	/// @param l - new value.
	/// @param hasExpressions - true: value has expresions in it
	template <typename T>
	Query &Set(string field, const std::vector<T> &l, bool hasExpressions = false) & {
		VariantArray value;
		value.reserve(l.size());
		value.MarkArray();
		for (auto it = l.begin(); it != l.end(); it++) value.emplace_back(*it);
		return Set(std::move(field), std::move(value), hasExpressions);
	}
	template <typename T>
	Query &&Set(string field, const std::vector<T> &l, bool hasExpressions = false) && {
		return std::move(Set<T>(std::move(field), l, hasExpressions));
	}
	/// Sets a new value for a field.
	/// @param field - field name.
	/// @param value - new value.
	/// @param hasExpressions - true: value has expresions in it
	Query &Set(string field, VariantArray value, bool hasExpressions = false) & {
		updateFields_.emplace_back(std::move(field), std::move(value), FieldModeSet, hasExpressions);
		return *this;
	}
	Query &&Set(string field, VariantArray value, bool hasExpressions = false) && {
		return std::move(Set(std::move(field), std::move(value), hasExpressions));
	}
	/// Sets a value for a field as an object.
	/// @param field - field name.
	/// @param value - new value.
	/// @param hasExpressions - true: value has expresions in it
	template <typename ValueType>
	Query &SetObject(string field, ValueType value, bool hasExpressions = false) & {
		return SetObject(std::move(field), {value}, hasExpressions);
	}
	template <typename ValueType>
	Query &&SetObject(string field, ValueType value, bool hasExpressions = false) && {
		return std::move(SetObject<ValueType>(std::move(field), std::move(value), hasExpressions));
	}
	/// Sets a new value for a field as an object.
	/// @param field - field name.
	/// @param l - new value.
	/// @param hasExpressions - true: value has expresions in it
	template <typename ValueType>
	Query &SetObject(string field, std::initializer_list<ValueType> l, bool hasExpressions = false) & {
		VariantArray value;
		value.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) value.emplace_back(Variant(*it));
		return SetObject(std::move(field), std::move(value), hasExpressions);
	}
	template <typename ValueType>
	Query &&SetObject(string field, std::initializer_list<ValueType> l, bool hasExpressions = false) && {
		return std::move(SetObject<ValueType>(std::move(field), std::move(l), hasExpressions));
	}
	/// Sets a new value for a field as an object.
	/// @param field - field name.
	/// @param l - new value.
	/// @param hasExpressions - true: value has expresions in it
	template <typename T>
	Query &SetObject(string field, const std::vector<T> &l, bool hasExpressions = false) & {
		VariantArray value;
		value.reserve(l.size());
		value.MarkArray();
		for (auto it = l.begin(); it != l.end(); it++) value.emplace_back(Variant(*it));
		return SetObject(std::move(field), std::move(value), hasExpressions);
	}
	template <typename T>
	Query &&SetObject(string field, const std::vector<T> &l, bool hasExpressions = false) && {
		return std::move(SetObject<T>(std::move(field), l, hasExpressions));
	}
	/// Sets a value for a field as an object.
	/// @param field - field name.
	/// @param value - new value.
	/// @param hasExpressions - true: value has expresions in it
	Query &SetObject(string field, VariantArray value, bool hasExpressions = false) &;
	Query &&SetObject(string field, VariantArray value, bool hasExpressions = false) && {
		return std::move(SetObject(std::move(field), std::move(value), hasExpressions));
	}
	/// Drops a value for a field.
	/// @param field - field name.
	Query &Drop(string field) & {
		updateFields_.emplace_back(std::move(field), VariantArray(), FieldModeDrop);
		return *this;
	}
	Query &&Drop(string field) && { return std::move(Drop(std::move(field))); }

	/// Add sql-function to query.
	/// @param function - function declaration.
	void AddFunction(const string &function) { selectFunctions_.push_back(std::move(function)); }

	/// Adds equal position fields to arrays queries.
	/// @param equalPosition - list of fields with equal array index position.
	void AddEqualPosition(const h_vector<string> &equalPosition) {
		equalPositions_.emplace(entries.DetermineEqualPositionIndexes(equalPosition));
	}
	void AddEqualPosition(const vector<string> &equalPosition) {
		equalPositions_.emplace(entries.DetermineEqualPositionIndexes(equalPosition));
	}
	void AddEqualPosition(std::initializer_list<string> l) { equalPositions_.emplace(entries.DetermineEqualPositionIndexes(l)); }

	/// Joins namespace with another namespace. Analog to sql JOIN.
	/// @param joinType - type of Join (Inner, Left or OrInner).
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param op - operation type (and, or, not).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	Query &Join(JoinType joinType, const string &index, const string &joinIndex, CondType cond, OpType op, Query &&qr) &;
	Query &&Join(JoinType joinType, const string &index, const string &joinIndex, CondType cond, OpType op, Query &&qr) && {
		return std::move(Join(joinType, index, joinIndex, cond, op, std::move(qr)));
	}
	Query &Join(JoinType joinType, const string &index, const string &joinIndex, CondType cond, OpType op, const Query &qr) &;
	Query &&Join(JoinType joinType, const string &index, const string &joinIndex, CondType cond, OpType op, const Query &qr) && {
		return std::move(Join(joinType, index, joinIndex, cond, op, qr));
	}

	OnHelper Join(JoinType joinType, Query &&q) &;
	OnHelper Join(JoinType joinType, const Query &q) &;
	OnHelperR Join(JoinType joinType, Query &&q) &&;
	OnHelperR Join(JoinType joinType, const Query &q) &&;

	/// @public
	/// Inner Join of this namespace with another one.
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	Query &InnerJoin(const string &index, const string &joinIndex, CondType cond, Query &&qr) & {  // -V1071
		return Join(JoinType::InnerJoin, index, joinIndex, cond, OpAnd, std::move(qr));
	}
	Query &&InnerJoin(const string &index, const string &joinIndex, CondType cond, Query &&qr) && {
		return std::move(InnerJoin(index, joinIndex, cond, std::move(qr)));
	}
	Query &InnerJoin(const string &index, const string &joinIndex, CondType cond, const Query &qr) & {
		return Join(JoinType::InnerJoin, index, joinIndex, cond, OpAnd, qr);
	}
	Query &&InnerJoin(const string &index, const string &joinIndex, CondType cond, const Query &qr) && {
		return std::move(InnerJoin(index, joinIndex, cond, qr));
	}

	/// Left Join of this namespace with another one.
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	Query &LeftJoin(const string &index, const string &joinIndex, CondType cond, Query &&qr) & {
		return Join(JoinType::LeftJoin, index, joinIndex, cond, OpAnd, std::move(qr));
	}
	Query &&LeftJoin(const string &index, const string &joinIndex, CondType cond, Query &&qr) && {
		return std::move(LeftJoin(index, joinIndex, cond, std::move(qr)));
	}
	Query &LeftJoin(const string &index, const string &joinIndex, CondType cond, const Query &qr) & {
		return Join(JoinType::LeftJoin, index, joinIndex, cond, OpAnd, qr);
	}
	Query &&LeftJoin(const string &index, const string &joinIndex, CondType cond, const Query &qr) && {
		return std::move(LeftJoin(index, joinIndex, cond, qr));
	}

	/// OrInnerJoin of this namespace with another one.
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return a reference to a query object ready to be executed.
	Query &OrInnerJoin(const string &index, const string &joinIndex, CondType cond, Query &&qr) & {
		return Join(JoinType::OrInnerJoin, index, joinIndex, cond, OpAnd, std::move(qr));
	}
	Query &&OrInnerJoin(const string &index, const string &joinIndex, CondType cond, Query &&qr) && {
		return std::move(OrInnerJoin(index, joinIndex, cond, std::move(qr)));
	}
	Query &OrInnerJoin(const string &index, const string &joinIndex, CondType cond, const Query &qr) & {
		return Join(JoinType::OrInnerJoin, index, joinIndex, cond, OpAnd, qr);
	}
	Query &&OrInnerJoin(const string &index, const string &joinIndex, CondType cond, const Query &qr) && {
		return std::move(OrInnerJoin(index, joinIndex, cond, qr));
	}

	/// Changes debug level.
	/// @param level - debug level.
	/// @return Query object.
	Query &Debug(int level) & {
		debugLevel = level;
		return *this;
	}
	Query &&Debug(int level) && { return std::move(Debug(level)); }

	/// Changes strict mode.
	/// @param mode - strict mode.
	/// @return Query object.
	Query &Strict(StrictMode mode) & {
		strictMode = mode;
		return *this;
	}
	Query &&Strict(StrictMode mode) && { return std::move(Strict(mode)); }

	/// Performs sorting by certain column. Analog to sql ORDER BY.
	/// @param sort - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	Query &Sort(const string &sort, bool desc) & {	// -V1071
		if (sort.length()) sortingEntries_.push_back({sort, desc});
		return *this;
	}
	Query &&Sort(const string &sort, bool desc) && { return std::move(Sort(sort, desc)); }

	/// Performs sorting by certain column. Analog to sql ORDER BY.
	/// @param sort - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @param forcedSortOrder - list of values for forced sort order.
	/// @return Query object.
	template <typename T>
	Query &Sort(const string &sort, bool desc, std::initializer_list<T> forcedSortOrder) & {
		if (!forcedSortOrder_.empty()) throw Error(errParams, "Allowed only one forced sort order");
		sortingEntries_.push_back({sort, desc});
		for (const T &v : forcedSortOrder) forcedSortOrder_.emplace_back(v);
		return *this;
	}
	template <typename T>
	Query &&Sort(const string &sort, bool desc, std::initializer_list<T> forcedSortOrder) && {
		return std::move(Sort<T>(sort, desc, std::move(forcedSortOrder)));
	}

	/// Performs sorting by certain column. Analog to sql ORDER BY.
	/// @param sort - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @param forcedSortOrder - list of values for forced sort order.
	/// @return Query object.
	template <typename T>
	Query &Sort(const string &sort, bool desc, const T &forcedSortOrder) & {
		if (!forcedSortOrder_.empty()) throw Error(errParams, "Allowed only one forced sort order");
		sortingEntries_.push_back({sort, desc});
		for (const auto &v : forcedSortOrder) forcedSortOrder_.emplace_back(v);
		return *this;
	}
	template <typename T>
	Query &&Sort(const string &sort, bool desc, const T &forcedSortOrder) && {
		return std::move(Sort<T>(sort, desc, forcedSortOrder));
	}

	/// Performs distinct for a certain index.
	/// @param indexName - name of index for distict operation.
	Query &Distinct(const string &indexName) & {
		if (indexName.length()) {
			AggregateEntry aggEntry{AggDistinct, {indexName}};
			aggregations_.emplace_back(std::move(aggEntry));
		}
		return *this;
	}
	Query &&Distinct(const string &indexName) && { return std::move(Distinct(indexName)); }

	/// Sets list of columns in this namespace to be finally selected.
	/// @param l - list of columns to be selected.
	Query &Select(std::initializer_list<const char *> l) & {
		if (!CanAddSelectFilter()) {
			throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
		}
		selectFilter_.insert(selectFilter_.begin(), l.begin(), l.end());
		return *this;
	}
	Query &&Select(std::initializer_list<const char *> l) && { return std::move(Select(std::move(l))); }

	/// Adds an aggregate function for certain column.
	/// Analog to sql aggregate functions (min, max, avg, etc).
	/// @param type - aggregation function type (Sum, Avg).
	/// @param fields - names of the fields to be aggregated.
	/// @param sort - vector of sorting column names and descending (if true) or ascending (otherwise) flags.
	/// Use column name 'count' to sort by facet's count value.
	/// @param limit - number of rows to get from result set.
	/// @param offset - index of the first row to get from result set.
	/// @return Query object ready to be executed.
	Query &Aggregate(AggType type, const h_vector<string, 1> &fields, const vector<pair<string, bool>> &sort = {},
					 unsigned limit = UINT_MAX, unsigned offset = 0) & {
		if (!CanAddAggregation(type)) {
			throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
		}
		AggregateEntry aggEntry{type, fields, limit, offset};
		aggEntry.sortingEntries_.reserve(sort.size());
		for (const auto &s : sort) {
			aggEntry.sortingEntries_.push_back({s.first, s.second});
		}
		aggregations_.push_back(aggEntry);
		return *this;
	}
	Query &&Aggregate(AggType type, const h_vector<string, 1> &fields, const vector<pair<string, bool>> &sort = {},
					  unsigned limit = UINT_MAX, unsigned offset = 0) && {
		return std::move(Aggregate(type, fields, sort, limit, offset));
	}

	/// Sets next operation type to Or.
	/// @return Query object.
	Query &Or() & {
		nextOp_ = OpOr;
		return *this;
	}
	Query &&Or() && { return std::move(Or()); }

	/// Sets next operation type to Not.
	/// @return Query object.
	Query &Not() & {
		nextOp_ = OpNot;
		return *this;
	}
	Query &&Not() && { return std::move(Not()); }

	/// Insert open bracket to order logic operations.
	/// @return Query object.
	Query &OpenBracket() & {
		entries.OpenBracket(nextOp_);
		nextOp_ = OpAnd;
		return *this;
	}
	Query &&OpenBracket() && { return std::move(OpenBracket()); }

	/// Insert close bracket to order logic operations.
	/// @return Query object.
	Query &CloseBracket() & {
		entries.CloseBracket();
		return *this;
	}
	Query &&CloseBracket() && { return std::move(CloseBracket()); }

	/// Sets the limit of selected rows.
	/// Analog to sql LIMIT rowsNumber.
	/// @param limit - number of rows to get from result set.
	/// @return Query object.
	Query &Limit(unsigned limit) &noexcept {
		count = limit;
		return *this;
	}
	Query &&Limit(unsigned limit) &&noexcept { return std::move(Limit(limit)); }

	/// Sets the number of the first selected row from result query.
	/// Analog to sql LIMIT OFFSET.
	/// @param offset - index of the first row to get from result set.
	/// @return Query object.
	Query &Offset(unsigned offset) &noexcept {
		start = offset;
		return *this;
	}
	Query &&Offset(unsigned offset) &&noexcept { return std::move(Offset(offset)); }

	/// Set the total count calculation mode to Accurate
	/// @return Query object
	Query &ReqTotal() &noexcept {
		calcTotal = ModeAccurateTotal;
		return *this;
	}
	Query &&ReqTotal() &&noexcept { return std::move(ReqTotal()); }

	/// Set the total count calculation mode to Cached.
	/// It will be use LRUCache for total count result
	/// @return Query object
	Query &CachedTotal() &noexcept {
		calcTotal = ModeCachedTotal;
		return *this;
	}
	Query &&CachedTotal() &&noexcept { return std::move(CachedTotal()); }

	/// Output fulltext rank
	/// Allowed only with fulltext query
	/// @return Query object
	Query &WithRank() &noexcept {
		withRank_ = true;
		return *this;
	}
	Query &&WithRank() &&noexcept { return std::move(WithRank()); }
	bool IsWithRank() const noexcept { return withRank_; }

	/// Can we add aggregation functions
	/// or new select fields to a current query?
	bool CanAddAggregation(AggType type) const noexcept { return type == AggDistinct || (selectFilter_.empty()); }
	bool CanAddSelectFilter() const noexcept {
		return aggregations_.empty() || (aggregations_.size() == 1 && aggregations_.front().type_ == AggDistinct);
	}

	/// Serializes query data to stream.
	/// @param ser - serializer object for write.
	/// @param mode - serialization mode.
	void Serialize(WrSerializer &ser, uint8_t mode = Normal) const;

	/// Deserializes query data from stream.
	/// @param ser - serializer object.
	void Deserialize(Serializer &ser);

	void WalkNested(bool withSelf, bool withMerged, std::function<void(const Query &q)> visitor) const;

	bool HasLimit() const noexcept { return count != UINT_MAX; }
	bool HasOffset() const noexcept { return start != 0; }
	bool IsWALQuery() const noexcept;
	const h_vector<UpdateEntry, 0> &UpdateFields() const noexcept { return updateFields_; }
	QueryType Type() const { return type_; }
	const string &Namespace() const { return _namespace; }

protected:
	void deserialize(Serializer &ser, bool &hasJoinConditions);

public:
	string _namespace;						   /// Name of the namespace.
	unsigned start = 0;						   /// First row index from result set.
	unsigned count = UINT_MAX;				   /// Number of rows from result set.
	int debugLevel = 0;						   /// Debug level.
	StrictMode strictMode = StrictModeNotSet;  /// Strict mode.
	bool explain_ = false;					   /// Explain query if true
	CalcTotalMode calcTotal = ModeNoTotal;	   /// Calculation mode.
	QueryType type_ = QuerySelect;			   /// Query type
	OpType nextOp_ = OpAnd;					   /// Next operation constant.
	SortingEntries sortingEntries_;			   /// Sorting data.
	h_vector<Variant, 0> forcedSortOrder_;	   /// Keys that always go first - before any ordered values.
	vector<JoinedQuery> joinQueries_;		   /// List of queries for join.
	vector<JoinedQuery> mergeQueries_;		   /// List of merge queries.
	h_vector<string, 1> selectFilter_;		   /// List of columns in a final result set.
	h_vector<string, 0> selectFunctions_;	   /// List of sql functions

	std::multimap<unsigned, EqualPosition> equalPositions_;	 /// List of same position fields for queries with arrays
	QueryEntries entries;

	vector<AggregateEntry> aggregations_;

private:
	h_vector<UpdateEntry, 0> updateFields_;	 /// List of fields (and values) for update.
	bool withRank_ = false;
	friend class SQLParser;
};

class JoinedQuery : public Query {
public:
	JoinedQuery() = default;
	JoinedQuery(JoinType jt, const Query &q) : Query(q), joinType{jt} {}
	JoinedQuery(JoinType jt, Query &&q) : Query(std::move(q)), joinType{jt} {}
	using Query::Query;
	bool operator==(const JoinedQuery &obj) const;

	JoinType joinType{JoinType::LeftJoin};	   /// Default join type.
	h_vector<QueryJoinEntry, 1> joinEntries_;  /// Condition for join. Filled in each subqueries, empty in  root query
};

template <typename Q>
Q Query::OnHelperTempl<Q>::On(std::string index, CondType cond, std::string joinIndex) && {
	jq_.joinEntries_.emplace_back(op_, cond, std::move(index), std::move(joinIndex));
	return std::forward<Q>(q_);
}

template <typename Q>
Query::OnHelperGroup<Q> &&Query::OnHelperGroup<Q>::On(std::string index, CondType cond, std::string joinIndex) && {
	jq_.joinEntries_.emplace_back(op_, cond, std::move(index), std::move(joinIndex));
	op_ = OpAnd;
	return std::move(*this);
}

}  // namespace reindexer
