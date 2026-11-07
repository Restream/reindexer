#pragma once

#include <initializer_list>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include "core/enums.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/system_index_names.h"
#include "core/query/fields_names_filter.h"
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "estl/forward_like.h"
#include "estl/h_vector.h"
#include "queryentry.h"
#include "tools/errors.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

constexpr std::string_view kAggregationWithSelectFieldsMsgError =
	"Not allowed to combine aggregation functions and fields' filter in a single query";

class JoinedQuery;
class QueryImpl;
class ConstQueryImpl;
class JoinedQueryImpl;
class ConstJoinedQueryImpl;
class Query;

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
	friend class QueryImpl;
	friend class ConstQueryImpl;

	template <typename QE>
	struct [[nodiscard]] QueryEntryValidator {
		static void Validate(const Query& q, OpType, const auto&...) { q.checkAddNotWalCondition(); }
	};

	template <typename Q>
	class OnHelperTempl;
	using OnHelper = OnHelperTempl<Query&>;
	using OnHelperR = OnHelperTempl<Query&&>;

	template <typename Q>
	class OnHelperGroup;

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

public:
	Query() noexcept = default;
	virtual ~Query() = default;

	Query(Query&& other) noexcept;
	Query(const Query& other);
	Query& operator=(Query&& other) noexcept = default;
	Query& operator=(const Query& other) = delete;
	[[nodiscard]] bool operator==(const Query&) const;

	/// Creates an object for certain namespace with appropriate settings.
	/// @param nsName - name of the namespace the data to be selected from.
	template <concepts::ConvertibleToString Str>
	explicit Query(Str&& nsName) : namespace_(std::forward<Str>(nsName)) {}

	Query& Delete() & noexcept {
		type_ = QueryDelete;
		return *this;
	}
	[[nodiscard]] Query&& Delete() && noexcept { return std::move(Delete()); }

	/// Parses pure sql select query and initializes Query object data members as a result.
	/// @param q - sql query.
	[[nodiscard]] static Query FromSQL(std::string_view q);
	[[nodiscard]] std::string GetSQL(bool stripArgs = false) const;

	static Query FromJSON(std::string_view dsl);
	[[nodiscard]] std::string GetJSON() const;

	/// Parses query from the binary format (the one used by bindings and cproto).
	/// @param ser - serializer with query data.
	/// @param queryFormat - query format version.
	static Query Deserialize(Serializer& ser, QueryFormat queryFormat);
	/// Writes query in the binary format (the one used by bindings and cproto).
	/// @param ser - serializer to write query data to.
	/// @param queryFormat - query format version.
	void Serialize(WrSerializer& ser, QueryFormat queryFormat) const;

	/// Sets the limit of selected rows.
	/// Analog to sql LIMIT rowsNumber.
	/// @param limit - number of rows to get from result set.
	/// @return Query object.
	Query& Limit(unsigned limit) & noexcept {
		limit_ = limit;
		return *this;
	}
	[[nodiscard]] Query&& Limit(unsigned limit) && noexcept { return std::move(Limit(limit)); }

	/// Sets the number of the first selected row from result query.
	/// Analog to sql LIMIT OFFSET.
	/// @param offset - index of the first row to get from result set.
	/// @return Query object.
	Query& Offset(unsigned offset) & noexcept {
		offset_ = offset;
		return *this;
	}
	[[nodiscard]] Query&& Offset(unsigned offset) && noexcept { return std::move(Offset(offset)); }

	/// Set the total count calculation mode to Accurate
	/// @return Query object
	Query& ReqTotal() & noexcept {
		calcTotal(ModeAccurateTotal);
		return *this;
	}
	[[nodiscard]] Query&& ReqTotal() && noexcept { return std::move(ReqTotal()); }

	/// Set the total count calculation mode to Cached.
	/// It will be use LRUCache for total count result
	/// @return Query object
	Query& CachedTotal() & noexcept {
		calcTotal(ModeCachedTotal);
		return *this;
	}
	[[nodiscard]] Query&& CachedTotal() && noexcept { return std::move(CachedTotal()); }

	/// Mark query as 'local'. Local queries will always be executed on the current shard, ignoring sharding proxy logic
	Query& Local(bool on = true) & noexcept {
		local_ = on;
		return *this;
	}
	[[nodiscard]] Query&& Local(bool on = true) && noexcept { return std::move(Local(on)); }

	/// Output fulltext rank
	/// Allowed only with fulltext query
	/// @return Query object
	Query& WithRank() & noexcept {
		withRank_ = true;
		return *this;
	}
	[[nodiscard]] Query&& WithRank() && noexcept { return std::move(WithRank()); }

	/// Changes strict mode.
	/// @param mode - strict mode.
	/// @return Query object.
	Query& Strict(StrictMode mode) & noexcept {
		walkNested(true, true, true, [mode](Query& q) noexcept { q.strictMode_ = mode; });
		return *this;
	}
	[[nodiscard]] Query&& Strict(StrictMode mode) && noexcept { return std::move(Strict(mode)); }

	/// Enable explain query
	/// @param on - signaling on/off
	/// @return Query object ready to be executed
	Query& Explain(bool on = true) & noexcept {
		walkNested(true, true, true, [on](Query& q) noexcept { q.explain_ = on; });
		return *this;
	}
	[[nodiscard]] Query&& Explain(bool on = true) && noexcept { return std::move(Explain(on)); }

	/// Changes debug level.
	/// @param level - debug level.
	/// @return Query object.
	Query& Debug(int level) & noexcept {
		walkNested(true, true, true, [level](Query& q) noexcept { q.debugLevel_ = level; });
		return *this;
	}
	[[nodiscard]] Query&& Debug(int level) && noexcept { return std::move(Debug(level)); }

	/// Sets a new value for a field.
	/// @param field - field name.
	/// @param values - new value (or values).
	/// @param hasExpressions - true: value has expressions in it
	template <concepts::ConvertibleToString Str>
	Query& Set(Str&& field, ValuesWrapper values, HasExpression hasExpressions = HasExpression_False) & {
		type_ = QueryUpdate;
		updateFields_.emplace_back(std::forward<Str>(field), std::move(values).Extract(), FieldModeSet, *hasExpressions);
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Set(Str&& field, ValuesWrapper values, HasExpression hasExpressions = HasExpression_False) && {
		return std::move(Set(std::forward<Str>(field), std::move(values).Extract(), hasExpressions));
	}

	/// Sets a value for a field as an object.
	/// @param field - field name.
	/// @param values - new value (or values).
	/// @param hasExpressions - true: value has expressions in it
	template <concepts::ConvertibleToString Str>
	Query& SetObject(Str&& field, ValuesWrapper values, HasExpression hasExpressions = HasExpression_False) & {
		type_ = QueryUpdate;
		for (const auto& it : values) {
			checkSetObjectValue(it);
		}
		updateFields_.emplace_back(std::forward<Str>(field), std::move(values).Extract(), FieldModeSetJson, *hasExpressions);
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& SetObject(Str&& field, ValuesWrapper values, HasExpression hasExpressions = HasExpression_False) && {
		return std::move(SetObject(std::forward<Str>(field), std::move(values).Extract(), hasExpressions));
	}

	/// Drops a value for a field.
	/// @param field - field name.
	template <concepts::ConvertibleToString Str>
	Query& Drop(Str&& field) & {
		type_ = QueryUpdate;
		updateFields_.emplace_back(std::forward<Str>(field), VariantArray{}, FieldModeDrop, *HasExpression_False);
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Drop(Str&& field) && {
		return std::move(Drop(std::forward<Str>(field)));
	}

	/// Performs sorting by certain column. Same as sql 'ORDER BY'.
	/// @param field - sorting column name.
	/// @param sortOrder - is sorting direction descending or ascending.
	/// @param forcedSortOrder - list of values for forced sort order.
	/// @return Query object.
	template <concepts::ConvertibleToString Str>
	Query& Sort(Str&& sort, SortOrder sortOrder, ValuesWrapper forcedSortOrder = {}) & {
		if (!sortingEntries_.empty() && !forcedSortOrder.empty()) [[unlikely]] {
			throw Error(errParams, "Forced sort order is allowed for the first sorting entry only");
		}
		SortingEntry entry{std::forward<Str>(sort), Desc{sortOrder == SortOrder::Desc}};
		if (!entry.expression.empty()) {  // Ignore empty sort expression
			if (std::ranges::any_of(forcedSortOrder, [](const auto& v) noexcept { return v.IsNullValue(); })) [[unlikely]] {
				throw Error(errParams, "Null-values are not supported in forced sorting");
			}
			sortingEntries_.emplace_back(std::move(entry));
			if (!forcedSortOrder.empty()) {
				forcedSortOrder_ = std::move(forcedSortOrder);
			}
		}
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Sort(Str&& field, SortOrder sortOrder, ValuesWrapper forcedSortOrder = {}) && {
		return std::move(Sort(std::forward<Str>(field), sortOrder, std::move(forcedSortOrder).Extract()));
	}

	/// Performs sorting by ST_Distance() expressions for geometry index. Sorting function will use distance between field and target point.
	/// @param field - field's name. This field must contain Point.
	/// @param p - target point.
	/// @param sortOrder - is sorting direction descending or ascending.
	/// @return Query object.
	Query& SortStDistance(std::string_view field, reindexer::Point p, SortOrder sortOrder) &;
	[[nodiscard]] Query&& SortStDistance(std::string_view field, reindexer::Point p, SortOrder sortOrder) && {
		return std::move(SortStDistance(field, p, sortOrder));
	}

	/// Performs sorting by ST_Distance() expressions for geometry index. Sorting function will use distance 2 fields.
	/// @param field1 - first field name. This field must contain Point.
	/// @param field2 - second field name.This field must contain Point.
	/// @param sortOrder - is sorting direction descending or ascending.
	/// @return Query object.
	Query& SortStDistance(std::string_view field1, std::string_view field2, SortOrder sortOrder) &;
	[[nodiscard]] Query&& SortStDistance(std::string_view field1, std::string_view field2, SortOrder sortOrder) && {
		return std::move(SortStDistance(field1, field2, sortOrder));
	}

	/// Sets list of columns in this namespace to be finally selected.
	/// The columns should be specified in the same case as the jsonpaths corresponding to them.
	/// Non-existent fields and fields in the wrong case are ignored.
	/// If there are no fields in this list that meet these conditions, then the filter works as "*".
	/// @param field - column to be selected.
	template <concepts::ConvertibleToString... Str>
	Query& Select(Str&&... fields) & {
		static_assert(sizeof...(Str) > 0);
		if (!canAddSelectFilter()) [[unlikely]] {
			throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
		}
		(selectFilter_.Add(std::forward<Str>(fields), *this), ...);
		return *this;
	}
	template <concepts::ConvertibleToString... Str>
	[[nodiscard]] Query&& Select(Str&&... fields) && {
		return std::move(Select(std::forward<Str>(fields)...));
	}

	/// Force to select all columns, including vector fields, that will not be selected by default
	Query& SelectAllFields() & {
		selectFilter_.SetAllRegularFields();
		selectFilter_.SetAllVectorFields();
		return *this;
	}

	[[nodiscard]] Query&& SelectAllFields() && noexcept { return std::move(SelectAllFields()); }

	/// Add sql-function to query.
	/// @param function - function declaration.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& AddFunction(Str&& function) & {
		selectFunctions_.emplace_back(std::forward<Str>(function));
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& AddFunction(Str&& function) && {
		return std::move(AddFunction(std::forward<Str>(function)));
	}

	/// Performs 'distinct' for a indexes or fields.
	/// @param fields - names of indexes or fields for distinct operation.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString... Str>
	Query& Distinct(Str&&... fields) & {
		return Aggregate(AggDistinct, {std::string{std::forward<Str>(fields)}...});
	}
	template <concepts::ConvertibleToString... Str>
	[[nodiscard]] Query&& Distinct(Str&&... fields) && {
		return std::move(Distinct(std::forward<Str>(fields)...));
	}

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
					 unsigned limit = QueryEntry::kDefaultLimit, unsigned offset = QueryEntry::kDefaultOffset) &;
	[[nodiscard]] Query&& Aggregate(AggType type, h_vector<std::string, 1> fields,
									const std::vector<std::pair<std::string, bool>>& sort = {}, unsigned limit = QueryEntry::kDefaultLimit,
									unsigned offset = QueryEntry::kDefaultOffset) && {
		return std::move(Aggregate(type, std::move(fields), sort, limit, offset));
	}
	Query& Aggregate(AggType type, h_vector<std::string, 1>&& fields, SortingEntries&& sort, unsigned limit, unsigned offset) &;
	[[nodiscard]] Query&& Aggregate(AggType type, h_vector<std::string, 1>&& fields, SortingEntries&& sort, unsigned limit,
									unsigned offset) && {
		return std::move(Aggregate(type, std::move(fields), std::move(sort), limit, offset));
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param field - field used in condition clause.
	/// @param cond - type of condition.
	/// @param values - sequence of index values to be compared with.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& Where(Str&& field, CondType cond, ValuesWrapper values) & {
		addCondition<QueryEntry>(nextOp_, std::forward<Str>(field), cond, std::move(values).Extract());
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Where(Str&& field, CondType cond, ValuesWrapper values) && {
		return std::move(Where(std::forward<Str>(field), cond, std::move(values).Extract()));
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
	template <concepts::ConvertibleToString Str>
	Query& WhereComposite(Str&& idx, CondType cond, std::span<const VariantArray> v) & {
		VariantArray values;
		values.reserve(v.size());
		for (auto it = v.begin(); it != v.end(); it++) {
			values.emplace_back(*it);
		}
		addCondition<QueryEntry>(nextOp_, std::forward<Str>(idx), cond, std::move(values));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& WhereComposite(Str&& idx, CondType cond, std::span<const VariantArray> v) && {
		return std::move(WhereComposite(std::forward<Str>(idx), cond, v));
	}
	template <concepts::ConvertibleToString Str>
	Query& WhereComposite(Str&& idx, CondType cond, std::initializer_list<VariantArray> l) & {
		return WhereComposite(std::forward<Str>(idx), cond, std::span<const VariantArray>(l.begin(), l.end()));
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& WhereComposite(Str&& idx, CondType cond, std::initializer_list<VariantArray> l) && {
		return std::move(WhereComposite(std::forward<Str>(idx), cond, std::span<const VariantArray>(l.begin(), l.end())));
	}

	/// Adds a condition to compare two fields of the same document.
	/// @param firstIdx - left index name.
	/// @param cond - type of condition.
	/// @param secondIdx - right index name.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	Query& WhereBetweenFields(Str1&& firstIdx, CondType cond, Str2&& secondIdx) & {
		addCondition<BetweenFieldsQueryEntry>(nextOp_, std::forward<Str1>(firstIdx), cond, std::forward<Str2>(secondIdx));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] Query&& WhereBetweenFields(Str1&& firstIdx, CondType cond, Str2&& secondIdx) && {
		return std::move(WhereBetweenFields(std::forward<Str1>(firstIdx), cond, std::forward<Str2>(secondIdx)));
	}

	/// Adds geospatial condition to find all points near target 'p' within the 'distance'.
	/// @param field - geospatial index name.
	/// @param p - point, that will be treat as the center of the boarding circle.
	/// @param distance - distance of the search (radius of the boarding circle).
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& DWithin(Str&& field, Point p, double distance) & {
		addCondition<QueryEntry>(nextOp_, std::forward<Str>(field), CondDWithin, VariantArray::Create(p, distance));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& DWithin(Str&& field, Point p, double distance) && {
		return std::move(DWithin(std::forward<Str>(field), p, distance));
	}

	/// Adds nested query and applies geospatial condition to it's results.
	/// @param subQuery - nested query, that has to return some sequence of points.
	/// @param p - point, that will be treat as the center of the boarding circle.
	/// @param distance - distance of the search (radius of the boarding circle).
	/// @return Query object ready to be executed.
	Query& DWithin(Query&& subQuery, Point p, double distance) & {
		return Where(std::move(subQuery), CondDWithin, VariantArray::Create(p, distance));
	}
	[[nodiscard]] Query&& DWithin(Query&& q, Point p, double distance) && { return std::move(DWithin(std::move(q), p, distance)); }

	/// Vectors search. Adds KNN-condition to get K nearest neighbors of the vector.
	/// @param field - vector index name.
	/// @param vec - target float vector.
	/// @param params - search params, depending on the specific vector index type.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& WhereKNN(Str&& field, FloatVector vec, KnnSearchParams params) & {
		addCondition<KnnQueryEntry>(nextOp_, std::forward<Str>(field), std::move(vec), std::move(params));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& WhereKNN(Str&& field, FloatVector vec, KnnSearchParams params) && {
		return std::move(WhereKNN(std::forward<Str>(field), std::move(vec), std::move(params)));
	}
	template <concepts::ConvertibleToString Str>
	Query& WhereKNN(Str&& field, ConstFloatVectorView vec, KnnSearchParams params) & {
		addCondition<KnnQueryEntry>(nextOp_, std::forward<Str>(field), FloatVector(vec), std::move(params));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& WhereKNN(Str&& field, ConstFloatVectorView vec, KnnSearchParams params) && {
		return std::move(WhereKNN(std::forward<Str>(field), vec, std::move(params)));
	}

	/// Adds a WHERE condition with an arithmetic expression on the left and literal values on the right.
	Query& Where(expressions::ArithmeticExpression&& expr, CondType cond, ValuesWrapper values) & {
		addCondition<QueryArithmeticEntry>(nextOp_, std::move(expr), cond, std::move(values).Extract());
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& Where(expressions::ArithmeticExpression&& expr, CondType cond, ValuesWrapper values) && {
		return std::move(Where(std::move(expr), cond, std::move(values).Extract()));
	}

	/// Adds a WHERE condition comparing a field to an arithmetic expression.
	template <concepts::ConvertibleToString Str>
	Query& Where(Str&& field, CondType cond, expressions::ArithmeticExpression&& expr) & {
		addCondition<QueryArithmeticEntry>(nextOp_, std::forward<Str>(field), cond, std::move(expr));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Where(Str&& field, CondType cond, expressions::ArithmeticExpression&& expr) && {
		return std::move(Where(std::forward<Str>(field), cond, std::move(expr)));
	}

	/// Adds a WHERE condition comparing two arithmetic expressions.
	Query& Where(expressions::ArithmeticExpression&& left, CondType cond, expressions::ArithmeticExpression&& right) & {
		addCondition<QueryArithmeticEntry>(nextOp_, std::move(left), cond, std::move(right));
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& Where(expressions::ArithmeticExpression&& left, CondType cond, expressions::ArithmeticExpression&& right) && {
		return std::move(Where(std::move(left), cond, std::move(right)));
	}

	/// Adds a WHERE condition comparing an arithmetic expression to a field.
	template <concepts::ConvertibleToString Str>
	Query& Where(expressions::ArithmeticExpression&& expr, CondType cond, Str&& field) & {
		addCondition<QueryArithmeticEntry>(nextOp_, std::move(expr), cond, std::forward<Str>(field));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Where(expressions::ArithmeticExpression&& expr, CondType cond, Str&& field) && {
		return std::move(Where(std::move(expr), cond, std::forward<Str>(field)));
	}

	/// Vectors search with autoembedding. Adds KNN-condition to get K nearest neighbors of the 'data'.
	/// @param field - vector index name. This index has to have configured embedder.
	/// @param data - data to search. This will be sent to the 'query_embedder' to get corresponding vector and this vector will be used in
	/// KNN search.
	/// @param params - search params, depending on the specific vector index type.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	Query& WhereKNN(Str1&& field, Str2&& data, KnnSearchParams params) & {
		addCondition<KnnQueryEntry>(nextOp_, std::forward<Str1>(field), std::forward<Str2>(data), std::move(params));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] Query&& WhereKNN(Str1&& field, Str2&& data, KnnSearchParams params) && {
		return std::move(WhereKNN(std::forward<Str1>(field), std::forward<Str2>(data), std::move(params)));
	}

	/// Adds nested query and applies condition with passed 'values' to it's results.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @param cond - type of condition.
	/// @param values - sequence of values, that will be compared with nested query results.
	/// @return Query object ready to be executed.
	Query& Where(Query&& q, CondType cond, ValuesWrapper values) & {
		addConditionSubQuery(nextOp_, std::move(q), cond, std::move(values).Extract());
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& Where(Query&& q, CondType cond, ValuesWrapper values) && {
		return std::move(Where(std::move(q), cond, std::move(values).Extract()));
	}

	/// Adds a condition to compare 'field' values and nested query results.
	/// @param field - target field name.
	/// @param cond - type of condition.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& Where(Str&& field, CondType cond, Query&& q) & {
		addConditionSubQuery(nextOp_, std::forward<Str>(field), cond, std::move(q));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Where(Str&& field, CondType cond, Query&& q) && {
		return std::move(Where(std::forward<Str>(field), cond, std::move(q)));
	}

	/// Adds a condition with a user-defined function.
	/// @param function - function object used in condition clause.
	/// @param cond - type of condition.
	/// @param values - sequence of index values to be compared with.
	/// @return Query object ready to be executed.
	template <concepts::Function Function>
	Query& Where(Function&& function, CondType cond, ValuesWrapper values) & {
		addConditionFunction(nextOp_, std::forward<Function>(function), cond, std::move(values).Extract());
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::Function Function>
	[[nodiscard]] Query&& Where(Function&& function, CondType cond, ValuesWrapper values) && {
		return std::move(Where(std::forward<Function>(function), cond, std::move(values).Extract()));
	}

	/// Adds a condition with a user-defined function.
	/// @param field - field name.
	/// @param cond - type of condition.
	/// @param function - function object used in condition clause.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str, concepts::Function Function>
	Query& Where(Str&& field, CondType cond, Function&& function) & {
		addConditionFunction(nextOp_, std::forward<Str>(field), cond, std::forward<Function>(function));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str, concepts::Function Function>
	[[nodiscard]] Query&& Where(Str&& field, CondType cond, Function&& function) && {
		return std::move(Where(std::forward<Str>(field), cond, std::forward<Function>(function)));
	}

	/// Adds a condition with a user-defined function.
	/// @param field - field name.
	/// @param cond - type of condition.
	/// @param function - function object used in condition clause.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& Where(Str&& field, CondType cond, functions::FunctionVariant&& function) & {
		std::visit([&](auto& fn) { addConditionFunction(nextOp_, std::forward<Str>(field), cond, std::move(fn)); }, function);
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Where(Str&& field, CondType cond, functions::FunctionVariant&& function) && {
		return std::move(Where(std::forward<Str>(field), cond, std::move(function)));
	}

	/// Adds a condition with a user-defined function.
	/// @param function - function object used in condition clause.
	/// @param cond - type of condition.
	/// @param values - sequence of index values to be compared with.
	/// @return Query object ready to be executed.
	Query& Where(functions::FunctionVariant&& function, CondType cond, ValuesWrapper values) & {
		std::visit([&](auto& fn) { addConditionFunction(nextOp_, std::move(fn), cond, std::move(values).Extract()); }, function);
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& Where(functions::FunctionVariant&& function, CondType cond, ValuesWrapper values) && {
		return std::move(Where(std::move(function), cond, std::move(values).Extract()));
	}

	/// Adds a condition to compare user-defined function values and nested query results.
	/// @param function - function object used in condition clause.
	/// @param cond - type of condition.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @return Query object ready to be executed.
	Query& Where(functions::FunctionVariant&& function, CondType cond, Query&& q) & {
		addConditionFunctionSubQuery(nextOp_, std::move(function), cond, std::move(q));
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& Where(functions::FunctionVariant&& function, CondType cond, Query&& q) && {
		return std::move(Where(std::move(function), cond, std::move(q)));
	}

	/// Adds a condition to compare user-defined function values and nested query results.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @param cond - type of condition.
	/// @param function - function object used in condition clause.
	/// @return Query object ready to be executed.
	Query& Where(Query&& q, CondType cond, functions::FunctionVariant&& function) & {
		addConditionSubQueryFunction(nextOp_, std::move(q), cond, std::move(function));
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& Where(Query&& q, CondType cond, functions::FunctionVariant&& function) && {
		return std::move(Where(std::move(q), cond, std::move(function)));
	}

	/// Adds equal position fields to arrays queries.
	/// @param equalPosition - list of fields with equal array index position.
	Query& EqualPositions(EqualPosition_t&& equalPosition) &;
	[[nodiscard]] Query&& EqualPositions(EqualPosition_t&& equalPosition) && { return std::move(EqualPositions(std::move(equalPosition))); }
	template <concepts::ConvertibleToString... Str>
	Query& EqualPositions(Str&&... fields) & {
		return EqualPositions(EqualPosition_t{std::forward<Str>(fields)...});
	}
	template <concepts::ConvertibleToString... Str>
	[[nodiscard]] Query&& EqualPositions(Str&&... fields) && {
		return std::move(EqualPositions(std::forward<Str>(fields)...));
	}

	Query& Merge(Query&& q) &;
	[[nodiscard]] Query&& Merge(Query&& q) && { return std::move(Merge(std::move(q))); }

	/// Sets next operation type to Or.
	/// @return Query object.
	Query& Or() &;
	[[nodiscard]] Query&& Or() && { return std::move(Or()); }

	/// Sets next operation type to Not.
	/// @return Query object.
	Query& Not() &;
	[[nodiscard]] Query&& Not() && { return std::move(Not()); }
	/// Sets next operation type to And.
	/// @return Query object.
	Query& And() & noexcept {
		assertrx_dbg(nextOp_ == OpAnd);
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
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	Query& Join(JoinType joinType, Query&& q, OpType op, Str1&& leftField, CondType cond, Str2&& rightField) &;
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] Query&& Join(JoinType joinType, Query&& q, OpType op, Str1&& leftField, CondType cond, Str2&& rightField) && {
		return std::move(Join(joinType, std::move(q), op, std::forward<Str1>(leftField), cond, std::forward<Str2>(rightField)));
	}

	/// Inner Join of this namespace with another one.
	/// @param q - query of the namespace that is going to be joined with this one.
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param rightField - name of the field in the namespace of q Query object.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	Query& InnerJoin(Query&& q, Str1&& leftField, CondType cond, Str2&& rightField) & {
		return Join(JoinType::InnerJoin, std::move(q), OpAnd, std::forward<Str1>(leftField), cond, std::forward<Str2>(rightField));
	}
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] Query&& InnerJoin(Query&& q, Str1&& leftField, CondType cond, Str2&& rightField) && {
		return std::move(
			Join(JoinType::InnerJoin, std::move(q), OpAnd, std::forward<Str1>(leftField), cond, std::forward<Str2>(rightField)));
	}

	/// Left Join of this namespace with another one.
	/// @param q - query of the namespace that is going to be joined with this one.
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param rightField - name of the field in the namespace of q Query object.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	Query& LeftJoin(Query&& q, Str1&& leftField, CondType cond, Str2&& rightField) & {
		return Join(JoinType::LeftJoin, std::move(q), OpAnd, std::forward<Str1>(leftField), cond, std::forward<Str2>(rightField));
	}
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] Query&& LeftJoin(Query&& q, Str1&& leftField, CondType cond, Str2&& rightField) && {
		return std::move(
			Join(JoinType::LeftJoin, std::move(q), OpAnd, std::forward<Str1>(leftField), cond, std::forward<Str2>(rightField)));
	}

	OnHelper Join(JoinType joinType, Query&& q) &;
	OnHelperR Join(JoinType joinType, Query&& q) &&;

	/// Insert open bracket to order logic operations.
	/// @return Query object.
	Query& OpenBracket() & {
		checkAddNotWalCondition();
		entries_.OpenBracket(nextOp_);
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& OpenBracket() && { return std::move(OpenBracket()); }

	/// Insert close bracket to order logic operations.
	/// @return Query object.
	Query& CloseBracket() & {
		entries_.CloseBracket();
		return *this;
	}
	[[nodiscard]] Query&& CloseBracket() && { return std::move(CloseBracket()); }

	/// Query reads namespace WAL (`#lsn` condition)
	[[nodiscard]] bool IsWALQuery() const noexcept { return *isWalQuery_; }

protected:
	[[nodiscard]] const std::string& NsName() const& noexcept { return namespace_; }

private:
	template <concepts::ConvertibleToString Str>
	void setNsName(Str&& nsName) & {
		namespace_ = std::forward<Str>(nsName);
	}

	void nextOp(OpType op) noexcept {
		assertrx_dbg(nextOp_ == OpAnd);
		nextOp_ = op;
	}
	QueryType type() const noexcept { return type_; }
	void type(QueryType type) noexcept { type_ = type; }

	void getSQL(WrSerializer& ser, bool stripArgs = false, Pretty pretty = Pretty_False) const;
	void getSQL(WrSerializer& ser, QueryType realType, bool stripArgs = false) const;
	[[nodiscard]] std::string getSQL(QueryType realType, Pretty pretty = Pretty_False) const;

	void calcTotal(CalcTotalMode total) noexcept { calcTotal_ = total; }
	CalcTotalMode calcTotal() const noexcept { return calcTotal_; }
	[[nodiscard]] bool hasCalcTotal() const noexcept { return calcTotal_ != ModeNoTotal; }

	[[nodiscard]] unsigned limit() const noexcept { return limit_; }
	[[nodiscard]] bool hasLimit() const noexcept { return limit_ != kQueryMaxLimit; }

	[[nodiscard]] unsigned offset() const noexcept { return offset_; }
	[[nodiscard]] bool hasOffset() const noexcept { return offset_ != kQueryMinOffset; }

	[[nodiscard]] bool isLocal() const noexcept { return local_; }

	[[nodiscard]] bool isWithRank() const noexcept { return withRank_; }

	StrictMode getStrictMode() const noexcept { return strictMode_; }

	[[nodiscard]] bool needExplain() const noexcept { return explain_; }

	[[nodiscard]] int debugLevel() const noexcept { return debugLevel_; }

	[[nodiscard]] bool canAddAggregation(AggType type) const noexcept { return type == AggDistinct || (selectFilter_.Fields().empty()); }
	[[nodiscard]] bool canAddSelectFilter() const noexcept {
		return aggregations_.empty() || (aggregations_.size() == 1 && aggregations_.front().Type() == AggDistinct);
	}

	void set(UpdateEntry&& entry) {
		type_ = QueryUpdate;
		updateFields_.push_back(std::move(entry));
	}

	void clearSorting() noexcept {
		sortingEntries_.clear();
		forcedSortOrder_.clear();
	}

	void clearAggregations() noexcept { aggregations_.clear(); }

	[[nodiscard]] bool hasJoinQueries() const noexcept;

	void reserveQueryEntries(size_t s) & { entries_.Reserve(s); }
	template <typename T, typename... Args>
	void addCondition(OpType op, Args&&... args) {
		QueryEntryValidator<T>::Validate(*this, op, args...);
		std::ignore = entries_.Append<T>(op, std::forward<Args>(args)...);
	}
	void addConditionSubQuery(OpType, Query&&, CondType, VariantArray);
	void addConditionSubQuery(OpType, std::string field, CondType, Query&&);

	template <concepts::Function Function>
	void addConditionFunction(OpType op, Function&& function, CondType cond, VariantArray values) {
		QueryEntryValidator<QueryFunctionEntry>::Validate(*this, op, function, cond, values);
		checkFunctionForLeftExpression(function.Type());
		std::ignore = entries_.Append<QueryFunctionEntry>(op, std::forward<Function>(function), cond, std::move(values));
	}
	template <concepts::ConvertibleToString Str, concepts::Function Function>
	void addConditionFunction(OpType op, Str&& field, CondType cond, Function&& function) {
		QueryEntryValidator<QueryFunctionEntry>::Validate(*this, op, field, cond, function);
		checkFunctionForRightExpression(function.Type());
		std::ignore = entries_.Append<QueryFunctionEntry>(op, std::forward<Str>(field), cond, std::forward<Function>(function));
	}
	template <concepts::ConvertibleToString Str>
	void addConditionFunction(OpType op, Str&& field, CondType cond, functions::FunctionVariant&& function) {
		std::visit([&](auto& fn) { addConditionFunction(op, std::forward<Str>(field), cond, std::move(fn)); }, function);
	}
	void addConditionFunction(OpType op, functions::FunctionVariant&& function, CondType cond, VariantArray values) {
		std::visit([&](auto& fn) { addConditionFunction(op, std::move(fn), cond, std::move(values)); }, function);
	}
	void addConditionFunctionSubQuery(OpType, functions::FunctionVariant&&, CondType, Query&&);
	void addConditionSubQueryFunction(OpType, Query&&, CondType, functions::FunctionVariant&&);

	void join(OpType, JoinedQuery&&);

	/// Serializes query data to stream.
	/// @param ser - serializer object for write.
	/// @param mode - serialization mode.
	/// @param queryFormat - query format version.
	void serialize(WrSerializer& ser, uint8_t mode, QueryFormat queryFormat) const;
	/// Deserializes query data from stream.
	/// @param ser - serializer object.
	template <typename T = Query>
	[[nodiscard]] static T deserialize(Serializer& ser, QueryFormat queryFormat) {
		return deserializeImpl<T>(ser, queryFormat);
	}

	template <typename T, typename... Args>
	[[nodiscard]] size_t replaceQueryEntry(size_t i, Args&&... args) {
		QueryEntryValidator<T>::Validate(*this, entries_.GetOperation(i), args...);
		return entries_.SetValue(i, T{std::forward<Args>(args)...});
	}
	[[nodiscard]] bool tryUpdateQueryEntryInplace(size_t i, VariantArray& values);
	template <JoinConditionInsertionDirection insertionDirection>
	[[nodiscard]] size_t insertConditionsFromOnConditions(size_t position, const h_vector<QueryJoinEntry, 1>& joinEntries,
														  const QueryEntries& joinedQueryEntries, size_t joinedQueryNo,
														  const std::vector<std::unique_ptr<Index>>* indexesFrom) {
		return entries_.InsertConditionsFromOnConditions<insertionDirection>(position, joinEntries, joinedQueryEntries, joinedQueryNo,
																			 indexesFrom);
	}

	void verifyForUpdate() const;
	void verifyForUpdateTransaction() const;

	void replaceSubQuery(size_t, Query&&);
	void replaceJoinQuery(size_t, JoinedQuery&&);
	void replaceMergeQuery(size_t, JoinedQuery&&);

	[[nodiscard]] const std::vector<JoinedQuery>& joinQueries() const& noexcept { return joinQueries_; }
	[[nodiscard]] std::span<JoinedQuery> getJoinQueriesSpan() & noexcept;
	[[nodiscard]] const std::vector<JoinedQuery>& mergeQueries() const& noexcept { return mergeQueries_; }
	[[nodiscard]] const std::vector<Query>& subQueries() const& noexcept { return subQueries_; }
	[[nodiscard]] const std::vector<AggregateEntry>& aggregations() const& noexcept { return aggregations_; }
	[[nodiscard]] const QueryEntries& entries() const& noexcept { return entries_; }
	[[nodiscard]] const SortingEntries& getSortingEntries() const& noexcept { return sortingEntries_; }
	[[nodiscard]] const std::vector<std::string>& selectFunctions() const& noexcept { return selectFunctions_; }
	[[nodiscard]] const std::vector<UpdateEntry>& updateFields() const& noexcept { return updateFields_; }
	[[nodiscard]] const FieldsNamesFilter& selectFilters() const& noexcept { return selectFilter_; }
	[[nodiscard]] const VariantArray& forcedSortOrder() const& noexcept { return forcedSortOrder_; }

	auto joinQueries() const&& = delete;
	auto mergeQueries() const&& = delete;
	auto subQueries() const&& = delete;
	auto aggregations() const&& = delete;
	auto entries() const&& = delete;
	auto getSortingEntries() const&& = delete;
	auto updateFields() const&& = delete;
	auto selectFilters() const&& = delete;
	auto forcedSortOrder() const&& = delete;
	auto nsName() const&& = delete;
	auto selectFunctions() const&& = delete;

	template <typename T = Query>
	[[nodiscard]] static T deserializeImpl(Serializer&, QueryFormat, auto...);

	template <typename QE, typename LHS, typename RHS>
	void addConditionSubQuery(OpType, LHS&&, CondType, RHS&&);
	template <typename Q>
	void addConditionSubQuery(OpType, Q&& subQuery, CondType, VariantArray&&);
	void addConditionSubQueryImpl(OpType, Query& subQuery, CondType, VariantArray&&);
	void checkSetObjectValue(const Variant& value) const;
	void walkNested(bool withSelf, bool withMerged, bool withSubQueries,
					const std::function<void(Query& q)>& visitor) noexcept(noexcept(visitor(std::declval<Query&>())));
	void adoptNested(Query& nested) const noexcept { nested.Strict(strictMode_).Explain(explain_).Debug(debugLevel_); }
	void validateWalLsnEntry() const;
	void validateWalQueryNoJoinMergeSubquery() const;
	void checkAddNotWalCondition() const;
	void checkSubQueryNoData() const;
	void checkSubQueryWithData() const;
	void checkSubQuery() const;
	void checkJoinedSubQuery() const;
	static void checkFunctionForLeftExpression(FunctionType);
	static void checkFunctionForRightExpression(FunctionType);
	[[nodiscard]] bool hasVolatileExpressions() const noexcept;
	[[nodiscard]] std::optional<int64_t> executionNowNsec() const noexcept { return executionNowNsec_; }
	void executionNowNsec(int64_t value) noexcept { executionNowNsec_ = value; }

	virtual void serializeJoinEntries(WrSerializer&) const;
	void deserialize(Serializer&);
	void deserialize(Serializer& ser, QueryFormat queryFormat);
	VariantArray deserializeValues(Serializer&, CondType) const;
	virtual void deserializeJoinOn(Serializer&);

	std::string namespace_;
	QueryType type_ = QuerySelect;
	unsigned offset_ = kQueryMinOffset;
	unsigned limit_ = kQueryMaxLimit;
	CalcTotalMode calcTotal_ = ModeNoTotal;
	bool local_ = false;
	bool withRank_ = false;
	StrictMode strictMode_ = StrictModeNotSet;
	bool explain_ = false;
	int debugLevel_ = 0;
	IsWalQuery isWalQuery_ = IsWalQuery_False;

	FieldsNamesFilter selectFilter_;
	std::vector<std::string> selectFunctions_;
	std::vector<AggregateEntry> aggregations_;
	std::vector<UpdateEntry> updateFields_;
	QueryEntries entries_;
	std::vector<JoinedQuery> joinQueries_;
	std::vector<JoinedQuery> mergeQueries_;
	std::vector<Query> subQueries_;
	SortingEntries sortingEntries_;
	VariantArray forcedSortOrder_;

	OpType nextOp_ = OpAnd;
	/// Shared now() snapshot for a single execution; not serialized.
	std::optional<int64_t> executionNowNsec_;
};
// NOLINTEND(clang-analyzer-optin.performance.Padding)

class [[nodiscard]] JoinedQuery final : public Query {
	friend class Query;
	friend class JoinedQueryImpl;
	friend class ConstJoinedQueryImpl;

public:
	JoinedQuery() noexcept = default;
	JoinedQuery(JoinType jt, const Query& q) : Query(q), joinType_{jt} {}
	JoinedQuery(JoinType jt, Query&& q) : Query(std::move(q)), joinType_{jt} {}
	JoinedQuery(const JoinedQuery&) = default;
	JoinedQuery(JoinedQuery&&) noexcept = default;
	JoinedQuery& operator=(JoinedQuery&&) noexcept = default;
	[[nodiscard]] bool operator==(const JoinedQuery&) const = default;

	JoinedQuery(JoinType, const JoinedQuery&&) noexcept = delete;
	JoinedQuery(JoinType, JoinedQuery&&) noexcept = delete;
	JoinedQuery(JoinType, const JoinedQuery&) noexcept = delete;
	JoinedQuery(JoinType, JoinedQuery&) noexcept = delete;

private:
	JoinedQuery(JoinType jt, std::string_view nsName) : Query{nsName}, joinType_{jt} {}

	[[nodiscard]] const std::string& rightNsName() const& noexcept { return NsName(); }
	JoinType getJoinType() const noexcept { return joinType_; }
	[[nodiscard]] const h_vector<QueryJoinEntry, 1>& joinEntries() const& noexcept { return joinEntries_; }

	auto rightNsName() const&& = delete;
	auto joinEntries() const&& = delete;

	void setJoinType(JoinType type) noexcept { joinType_ = type; }
	void emplaceBackOnEntry(OpType op, std::string leftField, CondType cond, std::string rightField,
							ReverseNsOrder reverseNsOrder = ReverseNsOrder_False) {
		if (joinEntries_.empty() && op == OpOr) [[unlikely]] {
			throw Error{errParams, "OR operator in first condition in ON"};
		}
		joinEntries_.emplace_back(op, std::move(leftField), cond, std::move(rightField), reverseNsOrder);
	}

	void deserializeJoinOn(Serializer&) override;
	void serializeJoinEntries(WrSerializer&) const override;

	JoinType joinType_{JoinType::LeftJoin};
	h_vector<QueryJoinEntry, 1> joinEntries_;  /// Condition for join. Filled in each subqueries, empty in root query
};

template <>
struct [[nodiscard]] Query::QueryEntryValidator<QueryEntry> {
	template <typename Field, typename... Rest>
	static void Validate(Query& q, OpType, const Field& field, const Rest&...) {
		if constexpr (concepts::ConvertibleToString<Field>) {
			if (std::string_view{field} == kLsnIndexName) {
				q.validateWalLsnEntry();
				q.isWalQuery_ = IsWalQuery_True;
			} else {
				q.checkAddNotWalCondition();
			}
		}
	}
};

template <>
struct [[nodiscard]] Query::QueryEntryValidator<JoinQueryEntry> {
	static void Validate(const Query& q, OpType, const auto&...) { q.validateWalQueryNoJoinMergeSubquery(); }
};

template <>
struct [[nodiscard]] Query::QueryEntryValidator<KnnQueryEntry> {
	static void Validate(const Query& q, OpType op, const auto& field, const auto&, const KnnSearchParams& params) {
		if (op == OpNot) [[unlikely]] {
			throw Error(errLogic, "NOT operation is not allowed with knn condition");
		}
		if constexpr (concepts::ConvertibleToString<decltype(field)>) {
			if (std::string_view{field} == kLsnIndexName) [[unlikely]] {
				throw Error{errQueryExec, "WAL query can contain only '{} > number' or '{} is not null'", kLsnIndexName, kLsnIndexName};
			}
		}
		q.checkAddNotWalCondition();
		params.Validate();
	}
};

template <typename Q>
class [[nodiscard]] Query::OnHelperGroup {
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
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] OnHelperGroup&& On(Str1&& index, CondType cond, Str2&& joinIndex) && {
		joiningQuery_.emplaceBackOnEntry(nextOnOp_, std::forward<Str1>(index), cond, std::forward<Str2>(joinIndex));
		nextOnOp_ = OpAnd;
		return std::move(*this);
	}
	[[nodiscard]] Q CloseBracket() && {
		mainQuery_.join(op_, std::move(joiningQuery_));
		return std::forward<Q>(mainQuery_);
	}

private:
	OnHelperGroup(Q q, OpType op, JoinedQuery&& jq) noexcept : mainQuery_{std::forward<Q>(q)}, op_{op}, joiningQuery_{std::move(jq)} {}

	Q mainQuery_;
	OpType op_{OpAnd};
	JoinedQuery joiningQuery_;
	OpType nextOnOp_{OpAnd};
};

template <typename Q>
class [[nodiscard]] Query::OnHelperTempl {
	friend class Query;

public:
	[[nodiscard]] OnHelperTempl&& Not() && noexcept {
		nextOnOp_ = OpNot;
		return std::move(*this);
	}
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] Q On(Str1&& index, CondType cond, Str2&& joinIndex) && {
		joiningQuery_.emplaceBackOnEntry(nextOnOp_, std::forward<Str1>(index), cond, std::forward<Str2>(joinIndex));
		mainQuery_.join(op_, std::move(joiningQuery_));
		return std::forward<Q>(mainQuery_);
	}
	[[nodiscard]] OnHelperGroup<Q> OpenBracket() && noexcept { return {std::forward<Q>(mainQuery_), op_, std::move(joiningQuery_)}; }

private:
	OnHelperTempl(Q mainQuery, OpType op, JoinedQuery&& joiningQuery) noexcept
		: mainQuery_{std::forward<Q>(mainQuery)}, op_{op}, joiningQuery_{std::move(joiningQuery)} {}

	Q mainQuery_;
	OpType op_{OpAnd};
	JoinedQuery joiningQuery_;
	OpType nextOnOp_{OpAnd};
};

template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
Query& Query::Join(JoinType joinType, Query&& q, OpType op, Str1&& leftField, CondType cond, Str2&& rightField) & {
	auto jq = JoinedQuery{joinType, std::move(q)};
	jq.emplaceBackOnEntry(op, std::forward<Str1>(leftField), cond, std::forward<Str2>(rightField));
	join(std::exchange(nextOp_, OpAnd), std::move(jq));
	return *this;
}

}  // namespace reindexer
