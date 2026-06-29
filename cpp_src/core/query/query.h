#pragma once

#include <initializer_list>
#include <string>
#include "core/impl/impl.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/variant.h"
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "estl/expected.h"
#include "estl/forward_like.h"
#include "estl/h_vector.h"
#include "queryentry.h"
#include "tools/errors.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

namespace impl {

class Query;
class JoinedQuery;

}  // namespace impl

namespace concepts {
// Concept for the iterable sequence, that can not be converted into single Variant
template <typename T>
concept PossibleMultiVariantContainer = concepts::Iterable<T> && !concepts::ConvertibleToVariant<T>;
}  // namespace concepts

/// @class Query
/// Allows to select data from DB.
/// Analog to ansi-sql select query.
// NOLINTBEGIN(clang-analyzer-optin.performance.Padding)
class [[nodiscard]] Query {
	friend class impl::Impl<const Query&>;
	friend class impl::Impl<Query&>;
	friend class impl::Impl<Query&&>;

	template <typename Q>
	class [[nodiscard]] OnHelperTempl;
	using OnHelper = OnHelperTempl<Query&>;
	using OnHelperR = OnHelperTempl<Query&&>;

public:
	/// @class ValuesWrapper
	/// Allows to wrap input values.
	/// Helps to provide single interface in Query for VariantArrays, std-containers, single values and iterable sequences.
	class [[nodiscard]] ValuesWrapper : public VariantArray {
	public:
		ValuesWrapper(const ValuesWrapper&) = delete;
		ValuesWrapper(ValuesWrapper&&) = delete;
		ValuesWrapper& operator=(const ValuesWrapper&) = delete;
		ValuesWrapper& operator=(ValuesWrapper&&) = delete;

		ValuesWrapper() noexcept = default;
		template <concepts::ConvertibleToVariant T>
		ValuesWrapper(T&& arg) : VariantArray{Variant{std::forward<T>(arg)}} {}

		template <concepts::PossibleMultiVariantContainer T>
		ValuesWrapper(T&& seq) {
			if constexpr (concepts::HasSize<T>) {
				reserve(seq.size());
			}
			for (auto&& v : seq) {
				emplace_back(forward_like<T>(v));
			}
		}
		template <concepts::ConvertibleToVariant T>
		ValuesWrapper(std::initializer_list<T> seq) {
			reserve(seq.size());
			for (auto& v : seq) {
				emplace_back(v);
			}
		}
		ValuesWrapper(VariantArray&& va) noexcept : VariantArray{std::move(va)} {}
		ValuesWrapper(VariantArray& va) : VariantArray{va} {}
		ValuesWrapper(const VariantArray& va) : VariantArray{va} {}
		[[nodiscard]] VariantArray&& Extract() && noexcept { return std::move(*this); }
	};

	Query() = delete;
	virtual ~Query() = default;

	Query(Query&& other) noexcept;
	Query(const Query& other) noexcept;
	Query& operator=(Query&& other) noexcept = default;
	Query& operator=(const Query& other) = delete;

	Query(impl::Query&& other) noexcept;

	/// Creates an object for certain namespace with appropriate settings.
	/// @param nsName - name of the namespace the data to be selected from.
	/// @param offset - number of the first row to get from selected set. Analog to sql OFFSET Offset.
	/// @param limit - number of rows to get from result set. Analog to sql LIMIT RowsCount.
	/// @param calcTotal - calculation mode.
	template <concepts::ConvertibleToString Str>
	explicit Query(Str&& nsName, unsigned offset = QueryEntry::kDefaultOffset, unsigned limit = QueryEntry::kDefaultLimit,
				   CalcTotalMode calcTotal = ModeNoTotal) {
		initImpl(std::string(std::forward<Str>(nsName)), offset, limit, calcTotal);
	}

	Query& Update() & noexcept;
	[[nodiscard]] Query&& Update() && noexcept { return std::move(Update()); }
	Query& Delete() & noexcept;
	[[nodiscard]] Query&& Delete() && noexcept { return std::move(Delete()); }

	/// Parses pure sql select query and initializes Query object data members as a result.
	/// @param q - sql query.
	[[nodiscard]] static Query FromSQL(std::string_view q) noexcept;
	[[nodiscard]] Expected<std::string> GetSQL() const noexcept;

	/// Parses JSON dsl set. Throws Error-exception on errors
	/// @param dsl - dsl set.
	/// @return Result query
	static Query FromJSON(std::string_view dsl) noexcept;

	void SetNsName(std::string nsName) & noexcept;

	/// Sets the limit of selected rows.
	/// Analog to sql LIMIT rowsNumber.
	/// @param limit - number of rows to get from result set.
	/// @return Query object.
	Query& Limit(unsigned limit) & noexcept;
	[[nodiscard]] Query&& Limit(unsigned limit) && noexcept { return std::move(Limit(limit)); }

	/// Sets the number of the first selected row from result query.
	/// Analog to sql LIMIT OFFSET.
	/// @param offset - index of the first row to get from result set.
	/// @return Query object.
	Query& Offset(unsigned offset) & noexcept;
	[[nodiscard]] Query&& Offset(unsigned offset) && noexcept { return std::move(Offset(offset)); }

	/// Set the total count calculation mode to Accurate
	/// @return Query object
	Query& ReqTotal() & noexcept;
	[[nodiscard]] Query&& ReqTotal() && noexcept { return std::move(ReqTotal()); }

	/// Set the total count calculation mode to Cached.
	/// It will be use LRUCache for total count result
	/// @return Query object
	Query& CachedTotal() & noexcept;
	[[nodiscard]] Query&& CachedTotal() && noexcept { return std::move(CachedTotal()); }

	/// Mark query as 'local'. Local queries will always be executed on the current shard, ignoring sharding proxy logic
	Query& Local(bool on = true) & noexcept;
	Query&& Local(bool on = true) && noexcept { return std::move(Local(on)); }

	/// Output fulltext rank
	/// Allowed only with fulltext query
	/// @return Query object
	Query& WithRank() & noexcept;
	[[nodiscard]] Query&& WithRank() && noexcept { return std::move(WithRank()); }

	/// Changes strict mode.
	/// @param mode - strict mode.
	/// @return Query object.
	Query& Strict(StrictMode mode) & noexcept;
	[[nodiscard]] Query&& Strict(StrictMode mode) && noexcept { return std::move(Strict(mode)); }

	/// Enable explain query
	/// @param on - signaling on/off
	/// @return Query object ready to be executed
	Query& Explain(bool on = true) & noexcept;
	[[nodiscard]] Query&& Explain(bool on = true) && noexcept { return std::move(Explain(on)); }

	/// Changes debug level.
	/// @param level - debug level.
	/// @return Query object.
	Query& Debug(int level) & noexcept;
	[[nodiscard]] Query&& Debug(int level) && noexcept { return std::move(Debug(level)); }

	/// Sets a new value for a field.
	/// @param field - field name.
	/// @param values - new value (or values).
	/// @param hasExpressions - true: value has expressions in it
	Query& Set(std::string field, ValuesWrapper values, bool hasExpressions = false) & noexcept;
	[[nodiscard]] Query&& Set(std::string field, ValuesWrapper values, bool hasExpressions = false) && noexcept {
		return std::move(Set(std::move(field), std::move(values).Extract(), hasExpressions));
	}

	/// Sets a value for a field as an object.
	/// @param field - field name.
	/// @param values - new value (or values).
	/// @param hasExpressions - true: value has expressions in it
	Query& SetObject(std::string field, ValuesWrapper values, bool hasExpressions = false) & noexcept;
	[[nodiscard]] Query&& SetObject(std::string field, ValuesWrapper values, bool hasExpressions = false) && noexcept {
		return std::move(SetObject(std::move(field), std::move(values).Extract(), hasExpressions));
	}

	/// Drops a value for a field.
	/// @param field - field name.
	Query& Drop(std::string field) & noexcept;
	[[nodiscard]] Query&& Drop(std::string field) && noexcept { return std::move(Drop(std::move(field))); }

	/// Performs sorting by certain column. Same as sql 'ORDER BY'.
	/// @param field - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	Query& Sort(std::string field, bool desc) & noexcept;
	[[nodiscard]] Query&& Sort(std::string field, bool desc) && noexcept { return std::move(Sort(std::move(field), desc)); }

	/// Performs sorting by certain column. Analog to sql ORDER BY.
	/// @param field - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @param forcedSortOrder - list of values for forced sort order.
	/// @return Query object.
	Query& Sort(std::string field, bool desc, ValuesWrapper forcedSortOrder) & noexcept;
	[[nodiscard]] Query&& Sort(std::string field, bool desc, ValuesWrapper forcedSortOrder) && noexcept {
		return std::move(Sort(std::move(field), desc, std::move(forcedSortOrder).Extract()));
	}

	/// Performs sorting by ST_Distance() expressions for geometry index. Sorting function will use distance between field and target point.
	/// @param field - field's name. This field must contain Point.
	/// @param p - target point.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	Query& SortStDistance(std::string field, reindexer::Point p, bool desc) & noexcept;
	[[nodiscard]] Query&& SortStDistance(std::string field, reindexer::Point p, bool desc) && noexcept {
		return std::move(SortStDistance(std::move(field), p, desc));
	}

	/// Performs sorting by ST_Distance() expressions for geometry index. Sorting function will use distance 2 fields.
	/// @param field1 - first field name. This field must contain Point.
	/// @param field2 - second field name.This field must contain Point.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	Query& SortStDistance(std::string field1, std::string field2, bool desc) & noexcept;
	[[nodiscard]] Query&& SortStDistance(std::string field1, std::string field2, bool desc) && noexcept {
		return std::move(SortStDistance(std::move(field1), std::move(field2), desc));
	}

	/// Sets list of columns in this namespace to be finally selected.
	/// The columns should be specified in the same case as the jsonpaths corresponding to them.
	/// Non-existent fields and fields in the wrong case are ignored.
	/// If there are no fields in this list that meet these conditions, then the filter works as "*".
	/// @param field - column to be selected.
	Query& Select(std::string field) & noexcept;
	[[nodiscard]] Query&& Select(std::string field) && noexcept { return std::move(Select(std::move(field))); }

	/// Force to select all columns, including vector fields, that will not be selected by default
	Query& SelectAllFields() & noexcept;
	[[nodiscard]] Query&& SelectAllFields() && noexcept { return std::move(SelectAllFields()); }

	/// Add sql-function to query.
	/// @param function - function declaration.
	void AddFunction(std::string function) noexcept;

	/// Performs 'distinct' for a indexes or fields.
	/// @param fields - names of indexes or fields for distinct operation.
	Query& Distinct(h_vector<std::string, 1> fields) & noexcept;
	[[nodiscard]] Query&& Distinct(h_vector<std::string, 1> fields) && noexcept { return std::move(Distinct(std::move(fields))); }

	/// Adds an aggregate function for certain column.
	/// Analog to sql aggregate functions (min, max, avg, etc).
	/// @param type - aggregation function type (Sum, Avg).
	/// @param fields - names of the fields to be aggregated.
	/// @param sort - vector of sorting column names and descending (if true) or ascending (otherwise) flags.
	/// Use column name 'count' to sort by facet's count value.
	/// @param limit - number of rows to get from result set.
	/// @param offset - index of the first row to get from result set.
	/// @return Query object ready to be executed.
	Query& Aggregate(AggType type, h_vector<std::string, 1> fields, const std::vector<std::pair<std::string, bool>>& sort = {},
					 unsigned limit = QueryEntry::kDefaultLimit, unsigned offset = QueryEntry::kDefaultOffset) & noexcept;
	[[nodiscard]] Query&& Aggregate(AggType type, h_vector<std::string, 1> fields,
									const std::vector<std::pair<std::string, bool>>& sort = {}, unsigned limit = QueryEntry::kDefaultLimit,
									unsigned offset = QueryEntry::kDefaultOffset) && noexcept {
		return std::move(Aggregate(type, std::move(fields), sort, limit, offset));
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param field - field used in condition clause.
	/// @param cond - type of condition.
	/// @param values - sequence of index values to be compared with.
	/// @return Query object ready to be executed.
	Query& Where(std::string field, CondType cond, ValuesWrapper values) & noexcept;
	[[nodiscard]] Query&& Where(std::string field, CondType cond, ValuesWrapper values) && noexcept {
		return std::move(Where(std::move(field), cond, std::move(values).Extract()));
	}

	/// Adds a condition with several values to a composite index.
	/// @param idx - composite index name.
	/// @param cond - type of condition.
	/// @param l - list of values to be compared according to the order
	/// of indexes in composite index name.
	/// There can be maximum 2 VariantArray objects in l: in case of CondRange condition,
	/// in all other cases amount of elements in l would be strictly equal to 1.
	/// For example, composite index name is "bookid+price", so l[0][0] (and l[1][0]
	/// in case of CondRange) belongs to "bookid" and l[0][1] (and l[1][1] in case of CondRange)
	/// belongs to "price" indexes.
	/// @return Query object ready to be executed.
	Query& WhereComposite(std::string idx, CondType cond, std::span<const VariantArray> v) & noexcept;
	[[nodiscard]] Query&& WhereComposite(std::string idx, CondType cond, std::span<const VariantArray> v) && noexcept {
		return std::move(WhereComposite(std::move(idx), cond, v));
	}
	Query& WhereComposite(std::string idx, CondType cond, std::initializer_list<VariantArray> l) & noexcept {
		return WhereComposite(std::move(idx), cond, std::span<const VariantArray>(l.begin(), l.end()));
	}
	[[nodiscard]] Query&& WhereComposite(std::string idx, CondType cond, std::initializer_list<VariantArray> l) && noexcept {
		return std::move(WhereComposite(std::move(idx), cond, std::span<const VariantArray>(l.begin(), l.end())));
	}

	/// Adds a condition to compare two fields of the same document.
	/// @param firstIdx - left index name.
	/// @param cond - type of condition.
	/// @param secondIdx - right index name.
	/// @return Query object ready to be executed.
	Query& WhereBetweenFields(std::string firstIdx, CondType cond, std::string secondIdx) & noexcept;
	[[nodiscard]] Query&& WhereBetweenFields(std::string firstIdx, CondType cond, std::string secondIdx) && noexcept {
		return std::move(WhereBetweenFields(std::move(firstIdx), cond, std::move(secondIdx)));
	}

	/// Adds geospatial condition to find all points near target 'p' within the 'distance'.
	/// @param field - geospatial index name.
	/// @param p - point, that will be treat as the center of the boarding circle.
	/// @param distance - distance of the search (radius of the boarding circle).
	/// @return Query object ready to be executed.
	Query& DWithin(std::string field, Point p, double distance) & noexcept;
	[[nodiscard]] Query&& DWithin(std::string field, Point p, double distance) && noexcept {
		return std::move(DWithin(std::move(field), p, distance));
	}

	/// Adds nested query and applies geospatial condition to it's results.
	/// @param q - nested query, that has to return some sequence of points.
	/// @param p - point, that will be treat as the center of the boarding circle.
	/// @param distance - distance of the search (radius of the boarding circle).
	/// @return Query object ready to be executed.
	Query& DWithin(Query&& q, Point p, double distance) & noexcept;
	[[nodiscard]] Query&& DWithin(Query&& q, Point p, double distance) && noexcept { return std::move(DWithin(std::move(q), p, distance)); }

	/// Vectors search. Adds KNN-condition to get K nearest neighbors of the vector.
	/// @param field - vector index name.
	/// @param vec - target float vector.
	/// @param params - search params, depending on the specific vector index type.
	/// @return Query object ready to be executed.
	Query& WhereKNN(std::string field, FloatVector vec, KnnSearchParams params) & noexcept;
	[[nodiscard]] Query&& WhereKNN(std::string field, FloatVector vec, KnnSearchParams params) && noexcept {
		return std::move(WhereKNN(std::move(field), std::move(vec), std::move(params)));
	}
	Query& WhereKNN(std::string field, ConstFloatVectorView vec, KnnSearchParams params) & noexcept;
	[[nodiscard]] Query&& WhereKNN(std::string field, ConstFloatVectorView vec, KnnSearchParams params) && noexcept {
		return std::move(WhereKNN(std::move(field), vec, std::move(params)));
	}

	/// Vectors search with autoembedding. Adds KNN-condition to get K nearest neighbors of the 'data'.
	/// @param field - vector index name. This index has to have configured embedder.
	/// @param data - data to search. This will be sent to the 'query_embedder' to get corresponding vector and this vector will be used in
	/// KNN search.
	/// @param params - search params, depending on the specific vector index type.
	/// @return Query object ready to be executed.
	Query& WhereKNN(std::string field, std::string data, KnnSearchParams params) & noexcept;
	[[nodiscard]] Query&& WhereKNN(std::string field, std::string data, KnnSearchParams params) && noexcept {
		return std::move(WhereKNN(std::move(field), std::move(data), std::move(params)));
	}

	/// Adds nested query and applies condition with passed 'values' to it's results.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @param cond - type of condition.
	/// @param values - sequence of values, that will be compared with nested query results.
	/// @return Query object ready to be executed.
	Query& Where(Query&& q, CondType cond, ValuesWrapper values) & noexcept;
	[[nodiscard]] Query&& Where(Query&& q, CondType cond, ValuesWrapper values) && noexcept {
		return std::move(Where(std::move(q), cond, std::move(values).Extract()));
	}

	/// Adds a condition to compare 'field' values and nested query results.
	/// @param field - target field name.
	/// @param cond - type of condition.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @return Query object ready to be executed.
	Query& Where(std::string&& field, CondType cond, Query&& q) & noexcept;
	[[nodiscard]] Query&& Where(std::string field, CondType cond, Query&& q) && noexcept {
		return std::move(Where(std::move(field), cond, std::move(q)));
	}

	/// Adds a condition with a user-defined function.
	/// @param function - function object used in condition clause.
	/// @param cond - type of condition.
	/// @param values - sequence of index values to be compared with.
	/// @return Query object ready to be executed.
	template <concepts::Function Function>
	Query& Where(Function&& function, CondType cond, ValuesWrapper values) & noexcept;
	template <concepts::Function Function>
	[[nodiscard]] Query&& Where(Function&& function, CondType cond, ValuesWrapper values) && noexcept {
		return std::move(Where(std::forward<Function>(function), cond, std::move(values).Extract()));
	}

	/// Adds a condition with a user-defined function.
	/// @param field - field name.
	/// @param cond - type of condition.
	/// @param function - function object used in condition clause.
	/// @return Query object ready to be executed.
	template <concepts::Function Function>
	Query& Where(std::string field, CondType cond, Function&& function) & noexcept;
	template <concepts::Function Function>
	[[nodiscard]] Query&& Where(std::string field, CondType cond, Function&& function) && noexcept {
		return std::move(Where(std::move(field), cond, std::forward<Function>(function)));
	}

	/// Adds a condition with a user-defined function.
	/// @param field - field name.
	/// @param cond - type of condition.
	/// @param function - function object used in condition clause.
	/// @return Query object ready to be executed.
	Query& Where(std::string field, CondType cond, functions::FunctionVariant&& function) & noexcept;
	[[nodiscard]] Query&& Where(std::string field, CondType cond, functions::FunctionVariant&& function) && noexcept {
		return std::move(Where(std::move(field), cond, std::move(function)));
	}

	/// Adds a condition with a user-defined function.
	/// @param function - function object used in condition clause.
	/// @param cond - type of condition.
	/// @param values - sequence of index values to be compared with.
	/// @return Query object ready to be executed.
	Query& Where(functions::FunctionVariant&& function, CondType cond, ValuesWrapper values) & noexcept;
	[[nodiscard]] Query&& Where(functions::FunctionVariant&& function, CondType cond, ValuesWrapper values) && noexcept {
		return std::move(Where(std::move(function), cond, std::move(values).Extract()));
	}

	/// Adds a condition to compare user-defined function values and nested query results.
	/// @param function - function object used in condition clause.
	/// @param cond - type of condition.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @return Query object ready to be executed.
	Query& Where(functions::FunctionVariant&& function, CondType cond, Query&& q) & noexcept;
	[[nodiscard]] Query&& Where(functions::FunctionVariant&& function, CondType cond, Query&& q) && noexcept {
		return std::move(Where(std::move(function), cond, std::move(q)));
	}

	/// Adds a condition to compare user-defined function values and nested query results.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @param cond - type of condition.
	/// @param function - function object used in condition clause.
	/// @return Query object ready to be executed.
	Query& Where(Query&& q, CondType cond, functions::FunctionVariant&& function) & noexcept;
	[[nodiscard]] Query&& Where(Query&& q, CondType cond, functions::FunctionVariant&& function) && noexcept {
		return std::move(Where(std::move(q), cond, std::move(function)));
	}

	/// Adds equal position fields to arrays queries.
	/// @param equalPosition - list of fields with equal array index position.
	Query& EqualPositions(EqualPosition_t&& equalPosition) & noexcept;
	[[nodiscard]] Query&& EqualPositions(EqualPosition_t&& equalPosition) && noexcept {
		return std::move(EqualPositions(std::move(equalPosition)));
	}
	Query& EqualPositions(std::span<std::string> equalPosition) & noexcept;
	[[nodiscard]] Query&& EqualPositions(std::span<std::string> equalPosition) && noexcept {
		return std::move(EqualPositions(equalPosition));
	}

	Query& Merge(const Query& q) & noexcept;
	[[nodiscard]] Query&& Merge(const Query& q) && noexcept { return std::move(Merge(q)); }
	Query& Merge(Query&& q) & noexcept;
	[[nodiscard]] Query&& Merge(Query&& q) && noexcept { return std::move(Merge(std::move(q))); }

	/// Sets next operation type to Or.
	/// @return Query object.
	Query& Or() & noexcept;
	[[nodiscard]] Query&& Or() && noexcept { return std::move(Or()); }

	/// Sets next operation type to Not.
	/// @return Query object.
	Query& Not() & noexcept;
	[[nodiscard]] Query&& Not() && noexcept { return std::move(Not()); }
	/// Sets next operation type to And.
	/// @return Query object.
	Query& And() & noexcept {
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& And() && noexcept { return std::move(And()); }

	/// Joins namespace with another namespace. Analog to sql JOIN.
	/// @param joinType - type of Join (Inner, Left or OrInner).
	/// @param q - query of the namespace that is going to be joined with this one.
	/// @param op - operation type (and, or, not).
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param rightField - name of the field in the namespace of q Query object.
	/// @return Query object ready to be executed.
	Query& Join(JoinType joinType, Query&& q, OpType op, std::string leftField, CondType cond, std::string rightField) & noexcept;
	[[nodiscard]] Query&& Join(JoinType joinType, Query&& q, OpType op, std::string leftField, CondType cond,
							   std::string rightField) && noexcept {
		return std::move(Join(joinType, std::move(q), op, std::move(leftField), cond, std::move(rightField)));
	}
	Query& Join(JoinType joinType, const Query& q, OpType op, std::string leftField, CondType cond, std::string rightField) & noexcept;
	[[nodiscard]] Query&& Join(JoinType joinType, const Query& q, OpType op, std::string leftField, CondType cond,
							   std::string rightField) && noexcept {
		return std::move(Join(joinType, q, op, std::move(leftField), cond, std::move(rightField)));
	}

	/// Inner Join of this namespace with another one.
	/// @param q - query of the namespace that is going to be joined with this one.
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param rightField - name of the field in the namespace of q Query object.
	/// @return Query object ready to be executed.
	Query& InnerJoin(Query&& q, std::string leftField, CondType cond, std::string rightField) & noexcept {
		return Join(JoinType::InnerJoin, std::move(q), OpAnd, std::move(leftField), cond, std::move(rightField));
	}
	[[nodiscard]] Query&& InnerJoin(Query&& q, std::string leftField, CondType cond, std::string rightField) && noexcept {
		return std::move(Join(JoinType::InnerJoin, std::move(q), OpAnd, std::move(leftField), cond, std::move(rightField)));
	}
	Query& InnerJoin(const Query& q, std::string leftField, CondType cond, std::string rightField) & noexcept {
		return Join(JoinType::InnerJoin, q, OpAnd, std::move(leftField), cond, std::move(rightField));
	}
	[[nodiscard]] Query&& InnerJoin(const Query& q, std::string leftField, CondType cond, std::string rightField) && noexcept {
		return std::move(Join(JoinType::InnerJoin, q, OpAnd, std::move(leftField), cond, std::move(rightField)));
	}

	/// Left Join of this namespace with another one.
	/// @param q - query of the namespace that is going to be joined with this one.
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param rightField - name of the field in the namespace of q Query object.
	/// @return Query object ready to be executed.
	Query& LeftJoin(Query&& q, std::string leftField, CondType cond, std::string rightField) & noexcept {
		return Join(JoinType::LeftJoin, std::move(q), OpAnd, std::move(leftField), cond, std::move(rightField));
	}
	[[nodiscard]] Query&& LeftJoin(Query&& q, std::string leftField, CondType cond, std::string rightField) && noexcept {
		return std::move(Join(JoinType::LeftJoin, std::move(q), OpAnd, std::move(leftField), cond, std::move(rightField)));
	}
	Query& LeftJoin(const Query& q, std::string leftField, CondType cond, std::string rightField) & noexcept {
		return Join(JoinType::LeftJoin, q, OpAnd, std::move(leftField), cond, std::move(rightField));
	}
	[[nodiscard]] Query&& LeftJoin(const Query& q, std::string leftField, CondType cond, std::string rightField) && noexcept {
		return std::move(Join(JoinType::LeftJoin, q, OpAnd, std::move(leftField), cond, std::move(rightField)));
	}

	OnHelper Join(JoinType joinType, Query&& q) & noexcept;
	OnHelper Join(JoinType joinType, const Query& q) & noexcept;
	OnHelperR Join(JoinType joinType, Query&& q) && noexcept;
	OnHelperR Join(JoinType joinType, const Query& q) && noexcept;

	/// Insert open bracket to order logic operations.
	/// @return Query object.
	Query& OpenBracket() & noexcept;
	[[nodiscard]] Query&& OpenBracket() && noexcept { return std::move(OpenBracket()); }

	/// Insert close bracket to order logic operations.
	/// @return Query object.
	Query& CloseBracket() & noexcept;
	[[nodiscard]] Query&& CloseBracket() && noexcept { return std::move(CloseBracket()); }

private:
	template <typename Q>
	class [[nodiscard]] OnHelperGroup {
		friend class OnHelperTempl<Q>;

	public:
		[[nodiscard]] OnHelperGroup&& Not() && noexcept {
			nextOnOp_ = OpNot;
			return std::move(*this);
		}
		[[nodiscard]] OnHelperGroup&& Or() && noexcept {
			nextOnOp_ = OpOr;
			return std::move(*this);
		}
		[[nodiscard]] OnHelperGroup&& On(std::string index, CondType cond, std::string joinIndex) && noexcept;
		[[nodiscard]] Q CloseBracket() && noexcept;

	private:
		OnHelperGroup(Q q, OpType op, std::unique_ptr<impl::JoinedQuery>&& jq) noexcept
			: mainQuery_{std::forward<Q>(q)}, op_{op}, joiningQuery_{std::move(jq)} {
			assertrx(!mainQuery_.state_.ok() || joiningQuery_);	 // TODO _dbg
		}

		Q mainQuery_;
		OpType op_{OpAnd};
		std::unique_ptr<impl::JoinedQuery> joiningQuery_;
		OpType nextOnOp_{OpAnd};
	};

	template <typename Q>
	class [[nodiscard]] OnHelperTempl {
		friend class Query;

	public:
		[[nodiscard]] OnHelperTempl&& Not() && noexcept {
			assertrx(!mainQuery_.state_.ok() || joiningQuery_);	 // TODO _dbg
			nextOnOp_ = OpNot;
			return std::move(*this);
		}
		[[nodiscard]] Q On(std::string index, CondType cond, std::string joinIndex) && noexcept;
		[[nodiscard]] OnHelperGroup<Q> OpenBracket() && noexcept { return {std::forward<Q>(mainQuery_), op_, std::move(joiningQuery_)}; }

	private:
		OnHelperTempl(Q mainQuery, OpType op, std::unique_ptr<impl::JoinedQuery> joiningQuery) noexcept
			: mainQuery_{std::forward<Q>(mainQuery)}, op_{op}, joiningQuery_{std::move(joiningQuery)} {
			assertrx(!mainQuery_.state_.ok() || joiningQuery_);	 // TODO _dbg
		}

		Q mainQuery_;
		OpType op_{OpAnd};
		std::unique_ptr<impl::JoinedQuery> joiningQuery_;
		OpType nextOnOp_{OpAnd};
	};

	explicit Query(Error&& err) noexcept;

	impl::Query& impl() & noexcept {
		assertrx(impl_);		// TODO _dbg
		assertrx(state_.ok());	// TODO _dbg
		return *impl_;
	}
	const impl::Query& impl() const& noexcept {
		assertrx(impl_);		// TODO _dbg
		assertrx(state_.ok());	// TODO _dbg
		return *impl_;
	}
	auto impl() const&& = delete;
	template <typename... Args>
	void initImpl(Args&&...) noexcept;

	std::shared_ptr<impl::Query> impl_;
	Error state_;
	OpType nextOp_ = OpAnd;
};

extern template void Query::initImpl(std::string&&, unsigned&, unsigned&, CalcTotalMode&);

}  // namespace reindexer
