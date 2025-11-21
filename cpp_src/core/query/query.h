#pragma once

#include <functional>
#include <initializer_list>
#include "core/keyvalue/geometry.h"
#include "estl/concepts.h"
#include "estl/forward_like.h"
#include "fields_names_filter.h"
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
constexpr std::string_view kOrNotOpErrorMsg =
	"'OR NOT' operation is not supported yet. Use version with brackets instead: 'OR ( NOT ... )'";

namespace concepts {
// Concept for the iterable sequence, that can not be converted into single Variant
template <typename T>
concept PossibleMultiVariantContainer = concepts::Iterable<T> && !concepts::ConvertibleToVariant<T>;
}  // namespace concepts

/// @class Query
/// Allows to select data from DB.
/// Analog to ansi-sql select query.
class [[nodiscard]] Query {
public:
	/// Creates an object for certain namespace with appropriate settings.
	/// @param nsName - name of the namespace the data to be selected from.
	/// @param start - number of the first row to get from selected set. Analog to sql OFFSET Offset.
	/// @param count - number of rows to get from result set. Analog to sql LIMIT RowsCount.
	/// @param calcTotal - calculation mode.
	template <concepts::ConvertibleToString Str>
	explicit Query(Str&& nsName, unsigned start = QueryEntry::kDefaultOffset, unsigned count = QueryEntry::kDefaultLimit,
				   CalcTotalMode calcTotal = ModeNoTotal)
		: namespace_(std::forward<Str>(nsName)), start_(start), count_(count), calcTotal_(calcTotal) {}

	Query() = default;
	virtual ~Query();

	Query(Query&& other) noexcept;
	Query& operator=(Query&& other) noexcept = default;
	Query(const Query& other);
	Query& operator=(const Query& other) = delete;

	/// Allows to compare 2 Query objects.
	[[nodiscard]] bool operator==(const Query&) const;

	/// Parses pure sql select query and initializes Query object data members as a result.
	/// @param q - sql query.
	[[nodiscard]] static Query FromSQL(std::string_view q);

	/// Logs query in 'Select field1, ... field N from namespace ...' format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	WrSerializer& GetSQL(WrSerializer& ser, bool stripArgs = false) const;

	/// Logs query in 'Select field1, ... field N from namespace ...' format.
	/// @param ser - serializer to store SQL string
	/// @param realType - replaces original query's type
	/// @param stripArgs - replace condition values with '?'
	WrSerializer& GetSQL(WrSerializer& ser, QueryType realType, bool stripArgs = false) const;

	/// Logs query in 'Select field1, ... field N from namespace ...' format.
	/// @param stripArgs - replace condition values with '?'
	/// @return Query in SQL format
	[[nodiscard]] std::string GetSQL(bool stripArgs = false) const;

	/// Logs query in 'Select field1, ... field N from namespace ...' format.
	/// @param realType - replaces original query's type
	/// @return Query in SQL format
	[[nodiscard]] std::string GetSQL(QueryType realType) const;

	/// Parses JSON dsl set. Throws Error-exception on errors
	/// @param dsl - dsl set.
	/// @return Result query
	static Query FromJSON(std::string_view dsl);

	/// returns structure of a query in JSON dsl format
	[[nodiscard]] std::string GetJSON() const;

	/// @class ValuesWrapper
	/// Allows to wrap input values.
	/// Helps to provide single interface in Query for VariantArrays, std-containers, single values and iterable sequnces.
	class [[nodiscard]] ValuesWrapper {
	public:
		ValuesWrapper(const ValuesWrapper&) = delete;
		ValuesWrapper(ValuesWrapper&&) = delete;
		ValuesWrapper& operator=(const ValuesWrapper&) = delete;
		ValuesWrapper& operator=(ValuesWrapper&&) = delete;

		ValuesWrapper() noexcept : dataPtr_{&storage_} {}
		template <concepts::ConvertibleToVariant T>
		ValuesWrapper(T&& arg) : storage_{Variant{std::forward<T>(arg)}}, dataPtr_{&storage_} {}
		template <concepts::PossibleMultiVariantContainer T>
		ValuesWrapper(T&& seq) : dataPtr_{&storage_} {
			if constexpr (concepts::HasSize<T>) {
				storage_.reserve(seq.size());
			}
			for (auto&& v : seq) {
				storage_.emplace_back(forward_like<T>(v));
			}
		}
		template <concepts::ConvertibleToVariant T>
		ValuesWrapper(std::initializer_list<T> seq) : dataPtr_{&storage_} {
			storage_.reserve(seq.size());
			for (auto& v : seq) {
				storage_.emplace_back(v);
			}
		}
		ValuesWrapper(VariantArray&& va) noexcept : dataPtr_{&va} {}
		ValuesWrapper(VariantArray& va) : storage_{va}, dataPtr_{&storage_} {}
		ValuesWrapper(const VariantArray& va) : storage_{va}, dataPtr_{&storage_} {}

		[[nodiscard]] VariantArray&& Extract() && noexcept { return std::move(*dataPtr_); }

	private:
		VariantArray storage_;
		VariantArray* dataPtr_;
	};

	/// Enable explain query
	/// @param on - signaling on/off
	/// @return Query object ready to be executed
	Query& Explain(bool on = true) & noexcept {
		walkNested(true, true, true, [on](Query& q) noexcept { q.explain_ = on; });
		return *this;
	}
	[[nodiscard]] Query&& Explain(bool on = true) && noexcept { return std::move(Explain(on)); }
	[[nodiscard]] bool NeedExplain() const noexcept { return explain_; }

	/// Mark query as 'local'. Local queries will always be executed on the current shard, ignoring sharding proxy logic
	Query& Local(bool on = true) & {
		local_ = on;
		return *this;
	}
	Query&& Local(bool on = true) && { return std::move(Local(on)); }

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param field - field used in condition clause.
	/// @param cond - type of condition.
	/// @param values - sequence of index values to be compared with.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& Where(Str&& field, CondType cond, ValuesWrapper values) & {
		std::ignore = entries_.Append<QueryEntry>(nextOp_, std::forward<Str>(field), cond, std::move(values).Extract());
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
		std::ignore = entries_.Append<QueryEntry>(nextOp_, std::forward<Str>(idx), cond, std::move(values));
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
		return std::move(WhereComposite(std::forward<Str>(idx), cond, std::move(l)));
	}

	/// Adds a condition to compare two fields of the same document.
	/// @param firstIdx - left index name.
	/// @param cond - type of condition.
	/// @param secondIdx - right index name.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	Query& WhereBetweenFields(Str1&& firstIdx, CondType cond, Str2&& secondIdx) & {
		std::ignore = entries_.Append<BetweenFieldsQueryEntry>(nextOp_, std::forward<Str1>(firstIdx), cond, std::forward<Str2>(secondIdx));
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
		std::ignore = entries_.Append<QueryEntry>(nextOp_, std::forward<Str>(field), CondDWithin, VariantArray::Create(p, distance));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& DWithin(Str&& field, Point p, double distance) && {
		return std::move(DWithin(std::forward<Str>(field), p, distance));
	}

	/// Adds nested query and applies geospatial condition to it's results.
	/// @param q - nested query, that has to return some sequence of points.
	/// @param p - point, that will be treat as the center of the boarding circle.
	/// @param distance - distance of the search (radius of the boarding circle).
	/// @return Query object ready to be executed.
	Query& DWithin(Query&& q, Point p, double distance) & { return Where(std::move(q), CondDWithin, VariantArray::Create(p, distance)); }
	[[nodiscard]] Query&& DWithin(Query&& q, Point p, double distance) && { return std::move(DWithin(std::move(q), p, distance)); }

	/// Adds nested query and applies condition with passed 'values' to it's results.
	/// @param q - nested query, that has to return some sequence of values (selection or aggregation result).
	/// @param cond - type of condition.
	/// @param values - sequence of values, that will be compared with nested query results.
	/// @return Query object ready to be executed.
	Query& Where(Query&& q, CondType cond, ValuesWrapper values) & {
		if (cond == CondEmpty || cond == CondAny) {
			q.checkSubQueryNoData();
			q.Limit(0);
		} else {
			q.checkSubQueryWithData();
			if (!q.selectFilter_.Fields().empty() && !q.HasLimit() && !q.HasOffset()) {
				// Converts main query condition to subquery condition
				q.sortingEntries_.clear();
				q.Where(std::move(q.selectFilter_.Fields()[0]), cond, std::move(values).Extract());
				q.selectFilter_.Clear();
				return Where(std::move(q), CondAny, VariantArray{});
			} else if (q.HasCalcTotal() || (!q.aggregations_.empty() &&
											(q.aggregations_[0].Type() == AggCount || q.aggregations_[0].Type() == AggCountCached))) {
				q.Limit(0);
			}
		}
		q.Strict(strictMode_).Debug(debugLevel_).Explain(explain_);
		std::ignore = entries_.Append<SubQueryEntry>(nextOp_, cond, subQueries_.size(), std::move(values).Extract());
		PopBackQEGuard guard{&entries_};
		adoptNested(q);
		subQueries_.emplace_back(std::move(q));
		guard.Reset();
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
		if (cond == CondDWithin) {
			throw Error(errLogic, "DWithin between field and subquery");
		}
		q.checkSubQueryWithData();
		adoptNested(q);
		if (q.HasCalcTotal() ||
			(!q.aggregations_.empty() && (q.aggregations_[0].Type() == AggCount || q.aggregations_[0].Type() == AggCountCached))) {
			q.Limit(0);
		}
		std::ignore = entries_.Append<SubQueryFieldEntry>(nextOp_, std::forward<Str>(field), cond, subQueries_.size());
		PopBackQEGuard guard{&entries_};
		adoptNested(q);
		subQueries_.emplace_back(std::move(q));
		guard.Reset();
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Where(Str&& field, CondType cond, Query&& q) && {
		return std::move(Where(std::forward<Str>(field), cond, std::move(q)));
	}

	/// Vectors search. Adds KNN-condition to get K nearest neighbors of the vector.
	/// @param field - vector index name.
	/// @param vec - target float vector.
	/// @param params - search params, depending on the specific vector index type.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str>
	Query& WhereKNN(Str&& field, ConstFloatVector vec, KnnSearchParams params) & {
		if (nextOp_ == OpNot) {
			throw Error(errLogic, "NOT operation is not allowed with knn condition");
		}
		params.Validate();
		std::ignore = entries_.Append<KnnQueryEntry>(nextOp_, std::forward<Str>(field), std::move(vec), std::move(params));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& WhereKNN(Str&& field, ConstFloatVector vec, KnnSearchParams params) && {
		return std::move(WhereKNN(std::forward<Str>(field), std::move(vec), std::move(params)));
	}
	template <concepts::ConvertibleToString Str>
	Query& WhereKNN(Str&& field, ConstFloatVectorView vec, KnnSearchParams params) & {
		return WhereKNN(std::forward<Str>(field), ConstFloatVector{vec.Span()}, std::move(params));
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& WhereKNN(Str&& field, ConstFloatVectorView vec, KnnSearchParams params) && {
		return std::move(WhereKNN(std::forward<Str>(field), ConstFloatVector{vec.Span()}, std::move(params)));
	}

	/// Vectors search with autoembedding. Adds KNN-condition to get K nearest neighbors of the 'data'.
	/// @param field - vector index name. This index has to have configured embedder.
	/// @param data - data to search. This will be sent to the 'query_embedder' to get corresponding vector and this vector will be used in
	/// KNN search.
	/// @param params - search params, depending on the specific vector index type.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	Query& WhereKNN(Str1&& field, Str2&& data, KnnSearchParams params) & {
		if (nextOp_ == OpNot) {
			throw Error(errLogic, "NOT operation is not allowed with knn condition");
		}
		params.Validate();
		std::ignore = entries_.Append<KnnQueryEntry>(nextOp_, std::forward<Str1>(field), std::forward<Str2>(data), std::move(params));
		nextOp_ = OpAnd;
		return *this;
	}
	template <concepts::ConvertibleToString Str1, concepts::ConvertibleToString Str2>
	[[nodiscard]] Query&& WhereKNN(Str1&& field, Str2&& data, KnnSearchParams params) && {
		return std::move(WhereKNN(std::forward<Str1>(field), std::forward<Str2>(data), std::move(params)));
	}

	/// Sets a new value for a field.
	/// @param field - field name.
	/// @param values - new value (or values).
	/// @param hasExpressions - true: value has expressions in it
	template <concepts::ConvertibleToString Str>
	Query& Set(Str&& field, ValuesWrapper values, bool hasExpressions = false) & {
		updateFields_.emplace_back(std::forward<Str>(field), std::move(values).Extract(), FieldModeSet, hasExpressions);
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Set(Str&& field, ValuesWrapper values, bool hasExpressions = false) && {
		return std::move(Set(std::forward<Str>(field), std::move(values).Extract(), hasExpressions));
	}

	/// Sets a value for a field as an object.
	/// @param field - field name.
	/// @param values - new value (or values).
	/// @param hasExpressions - true: value has expressions in it
	template <concepts::ConvertibleToString Str>
	Query& SetObject(Str&& field, ValuesWrapper values, bool hasExpressions = false) & {
		auto&& varArr = std::move(values).Extract();
		for (const auto& it : varArr) {
			checkSetObjectValue(it);
		}
		updateFields_.emplace_back(std::forward<Str>(field), std::move(varArr), FieldModeSetJson, hasExpressions);
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& SetObject(Str&& field, ValuesWrapper values, bool hasExpressions = false) && {
		return std::move(SetObject(std::forward<Str>(field), std::move(values).Extract(), hasExpressions));
	}

	/// Drops a value for a field.
	/// @param field - field name.
	template <concepts::ConvertibleToString Str>
	Query& Drop(Str&& field) & {
		updateFields_.emplace_back(std::forward<Str>(field), VariantArray(), FieldModeDrop);
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Drop(Str&& field) && {
		return std::move(Drop(std::forward<Str>(field)));
	}

	/// Add sql-function to query.
	/// @param function - function declaration.
	template <concepts::ConvertibleToString Str>
	void AddFunction(Str&& function) {
		selectFunctions_.emplace_back(std::forward<Str>(function));
	}

	/// Adds equal position fields to arrays queries.
	/// @param equalPosition - list of fields with equal array index position.
	Query& AddEqualPosition(std::span<std::string> equalPosition) & {
		auto* const bracket = entries_.LastOpenBracket();
		auto& eqPos = (bracket ? bracket->equalPositions : entries_.equalPositions);
		eqPos.emplace_back(std::make_move_iterator(equalPosition.begin()), std::make_move_iterator(equalPosition.end()));
		return *this;
	}
	Query& AddEqualPosition(h_vector<std::string> equalPosition) & {
		return AddEqualPosition(std::span<std::string>(equalPosition.begin(), equalPosition.end()));
	}
	[[nodiscard]] Query&& AddEqualPosition(h_vector<std::string> equalPosition) && {
		return std::move(AddEqualPosition(std::move(equalPosition)));
	}
	Query& AddEqualPosition(std::vector<std::string> equalPosition) & {
		return AddEqualPosition(std::span<std::string>(equalPosition.begin(), equalPosition.end()));
	}
	[[nodiscard]] Query&& AddEqualPosition(std::vector<std::string> equalPosition) && {
		return std::move(AddEqualPosition(std::move(equalPosition)));
	}
	Query& AddEqualPosition(std::initializer_list<std::string> l) & {
		auto* const bracket = entries_.LastOpenBracket();
		auto& eqPos = (bracket ? bracket->equalPositions : entries_.equalPositions);
		eqPos.emplace_back(l);
		return *this;
	}
	[[nodiscard]] Query&& AddEqualPosition(std::initializer_list<std::string> l) && { return std::move(AddEqualPosition(l)); }

	/// Joins namespace with another namespace. Analog to sql JOIN.
	/// @param joinType - type of Join (Inner, Left or OrInner).
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param rightField - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param op - operation type (and, or, not).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	Query& Join(JoinType joinType, std::string leftField, std::string rightField, CondType cond, OpType op, Query&& qr) &;
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& Join(JoinType joinType, StrL&& leftField, StrR&& rightField, CondType cond, OpType op, Query&& qr) && {
		return std::move(Join(joinType, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, op, std::move(qr)));
	}
	Query& Join(JoinType joinType, std::string leftField, std::string rightField, CondType cond, OpType op, const Query& qr) &;
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& Join(JoinType joinType, StrL&& leftField, StrR&& rightField, CondType cond, OpType op, const Query& qr) && {
		return std::move(Join(joinType, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, op, qr));
	}

	[[nodiscard]] auto Join(JoinType joinType, Query&& q) &;
	[[nodiscard]] auto Join(JoinType joinType, const Query& q) &;
	[[nodiscard]] auto Join(JoinType joinType, Query&& q) &&;
	[[nodiscard]] auto Join(JoinType joinType, const Query& q) &&;

	/// @public
	/// Inner Join of this namespace with another one.
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param rightField - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	Query& InnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, Query&& qr) & {  // -V1071
		return Join(JoinType::InnerJoin, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, OpAnd, std::move(qr));
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& InnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, Query&& qr) && {
		return std::move(InnerJoin(std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, std::move(qr)));
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	Query& InnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, const Query& qr) & {
		return Join(JoinType::InnerJoin, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, OpAnd, qr);
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& InnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, const Query& qr) && {
		return std::move(InnerJoin(std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, qr));
	}

	/// Left Join of this namespace with another one.
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param rightField - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	Query& LeftJoin(StrL&& leftField, StrR&& rightField, CondType cond, Query&& qr) & {
		return Join(JoinType::LeftJoin, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, OpAnd, std::move(qr));
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& LeftJoin(StrL&& leftField, StrR&& rightField, CondType cond, Query&& qr) && {
		return std::move(LeftJoin(std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, std::move(qr)));
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	Query& LeftJoin(StrL&& leftField, StrR&& rightField, CondType cond, const Query& qr) & {
		return Join(JoinType::LeftJoin, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, OpAnd, qr);
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& LeftJoin(StrL&& leftField, StrR&& rightField, CondType cond, const Query& qr) && {
		return std::move(LeftJoin(std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, qr));
	}

	/// OrInnerJoin of this namespace with another one.
	/// @param leftField - name of the field in the namespace of this Query object.
	/// @param rightField - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return a reference to a query object ready to be executed.
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	Query& OrInnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, Query&& qr) & {
		return Join(JoinType::OrInnerJoin, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, OpAnd, std::move(qr));
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& OrInnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, Query&& qr) && {
		return std::move(OrInnerJoin(std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, std::move(qr)));
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	Query& OrInnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, const Query& qr) & {
		return Join(JoinType::OrInnerJoin, std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, OpAnd, qr);
	}
	template <concepts::ConvertibleToString StrL, concepts::ConvertibleToString StrR>
	[[nodiscard]] Query&& OrInnerJoin(StrL&& leftField, StrR&& rightField, CondType cond, const Query& qr) && {
		return std::move(OrInnerJoin(std::forward<StrL>(leftField), std::forward<StrR>(rightField), cond, qr));
	}
	Query& Merge(const Query& q) &;
	[[nodiscard]] Query&& Merge(const Query& q) && { return std::move(Merge(q)); }
	Query& Merge(Query&& q) &;
	[[nodiscard]] Query&& Merge(Query&& q) && { return std::move(Merge(std::move(q))); }

	/// Changes debug level.
	/// @param level - debug level.
	/// @return Query object.
	Query& Debug(int level) & noexcept {
		walkNested(true, true, true, [level](Query& q) noexcept { q.debugLevel_ = level; });
		return *this;
	}
	[[nodiscard]] Query&& Debug(int level) && noexcept { return std::move(Debug(level)); }
	[[nodiscard]] int GetDebugLevel() const noexcept { return debugLevel_; }

	/// Changes strict mode.
	/// @param mode - strict mode.
	/// @return Query object.
	Query& Strict(StrictMode mode) & noexcept {
		walkNested(true, true, true, [mode](Query& q) noexcept { q.strictMode_ = mode; });
		return *this;
	}
	[[nodiscard]] Query&& Strict(StrictMode mode) && noexcept { return std::move(Strict(mode)); }
	[[nodiscard]] StrictMode GetStrictMode() const noexcept { return strictMode_; }

	/// Performs sorting by certain column. Same as sql 'ORDER BY'.
	/// @param sort - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	template <concepts::ConvertibleToString Str>
	Query& Sort(Str&& sort, bool desc) & {	// -V1071
		if (!strEmpty(sort)) {
			sortingEntries_.emplace_back(std::forward<Str>(sort), desc);
		}
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Sort(Str&& sort, bool desc) && {
		return std::move(Sort(std::forward<Str>(sort), desc));
	}

	/// Performs sorting by ST_Distance() expressions for geometry index. Sorting function will use distance between field and target point.
	/// @param field - field's name. This field must contain Point.
	/// @param p - target point.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	Query& SortStDistance(std::string_view field, reindexer::Point p, bool desc) &;
	[[nodiscard]] Query&& SortStDistance(std::string_view field, reindexer::Point p, bool desc) && {
		return std::move(SortStDistance(field, p, desc));
	}
	/// Performs sorting by ST_Distance() expressions for geometry index. Sorting function will use distance 2 fields.
	/// @param field1 - first field name. This field must contain Point.
	/// @param field2 - second field name.This field must contain Point.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	Query& SortStDistance(std::string_view field1, std::string_view field2, bool desc) &;
	[[nodiscard]] Query&& SortStDistance(std::string_view field1, std::string_view field2, bool desc) && {
		return std::move(SortStDistance(field1, field2, desc));
	}

	/// Performs sorting by certain column. Analog to sql ORDER BY.
	/// @param sort - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @param forcedSortOrder - list of values for forced sort order.
	/// @return Query object.
	template <concepts::ConvertibleToString Str>
	Query& Sort(Str&& sort, bool desc, ValuesWrapper forcedSortOrder) & {
		auto&& forcedSortOrderVarArr = std::move(forcedSortOrder).Extract();
		if (!sortingEntries_.empty() && !forcedSortOrderVarArr.empty()) [[unlikely]] {
			throw Error(errParams, "Forced sort order is allowed for the first sorting entry only");
		}
		SortingEntry entry{std::forward<Str>(sort), desc};
		if (!entry.expression.empty()) {  // Ignore empty sort expression
			if (std::ranges::any_of(forcedSortOrderVarArr, [](auto& v) noexcept { return v.IsNullValue(); })) [[unlikely]] {
				throw Error(errParams, "Null-values are not supported in forced sorting");
			}
			sortingEntries_.emplace_back(std::move(entry));
			forcedSortOrder_ = std::move(forcedSortOrderVarArr);
		}
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Sort(Str&& sort, bool desc, ValuesWrapper forcedSortOrder) && {
		return std::move(Sort(std::forward<Str>(sort), desc, std::move(forcedSortOrder).Extract()));
	}

	/// Performs 'distinct' for a indexes or fields.
	/// @param names - names of indexes or fields for distinct operation.
	template <concepts::ConvertibleToString... Str>
	Query& Distinct(Str&&... names) & {
		static_assert(sizeof...(names) > 0);
		if ((strEmpty(names) || ...)) {
			throw Error(errParams, "Distinct name empty");
		}
		h_vector<std::string, 1> v;
		v.reserve(sizeof...(names));
		(v.emplace_back(std::forward<Str>(names)), ...);
		aggregations_.emplace_back(AggDistinct, std::move(v));
		return *this;
	}

	template <concepts::ConvertibleToString... Str>
	[[nodiscard]] Query&& Distinct(Str&&... names) && {
		return std::move(Distinct(std::forward<Str>(names)...));
	}

	/// Sets list of columns in this namespace to be finally selected.
	/// The columns should be specified in the same case as the jsonpaths corresponding to them.
	/// Non-existent fields and fields in the wrong case are ignored.
	/// If there are no fields in this list that meet these conditions, then the filter works as "*".
	/// @param l - list of columns to be selected.
	template <concepts::ConvertibleToString Str>
	Query& Select(std::initializer_list<Str> l) & {
		return Select<std::initializer_list<Str>>(std::move(l));
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Select(std::initializer_list<Str> l) && {
		return std::move(Select<std::initializer_list<Str>>(std::move(l)));
	}

	template <typename StrCont>
	Query& Select(StrCont&& l) & {
		if (!CanAddSelectFilter()) {
			throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
		}
		selectFilter_.Add(l.begin(), l.end(), *this);
		return *this;
	}
	template <typename StrCont>
	[[nodiscard]] Query&& Select(StrCont&& l) && {
		return std::move(Select(std::forward<StrCont>(l)));
	}
	template <concepts::ConvertibleToString Str>
	Query& Select(Str&& f) & {
		if (!CanAddSelectFilter()) {
			throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
		}
		selectFilter_.Add(std::forward<Str>(f), *this);
		return *this;
	}
	template <concepts::ConvertibleToString Str>
	[[nodiscard]] Query&& Select(Str&& f) && {
		return std::move(Select(std::forward<Str>(f)));
	}
	/// Force to select all columns, including vector fields, that will not be selected by default
	Query& SelectAllFields() & {
		selectFilter_.SetAllRegularFields();
		selectFilter_.SetAllVectorFields();
		return *this;
	}
	[[nodiscard]] Query&& SelectAllFields() && { return std::move(SelectAllFields()); }

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
					 unsigned limit = QueryEntry::kDefaultLimit, unsigned offset = QueryEntry::kDefaultOffset) & {
		if (!CanAddAggregation(type)) {
			throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
		}
		SortingEntries sorting;
		sorting.reserve(sort.size());
		for (const auto& s : sort) {
			sorting.emplace_back(s.first, s.second);
		}
		aggregations_.emplace_back(type, std::move(fields), std::move(sorting), limit, offset);
		return *this;
	}
	[[nodiscard]] Query&& Aggregate(AggType type, h_vector<std::string, 1> fields,
									const std::vector<std::pair<std::string, bool>>& sort = {}, unsigned limit = QueryEntry::kDefaultLimit,
									unsigned offset = QueryEntry::kDefaultOffset) && {
		return std::move(Aggregate(type, std::move(fields), sort, limit, offset));
	}

	/// Sets next operation type to Or.
	/// @return Query object.
	Query& Or() & {
		if (nextOp_ == OpNot) [[unlikely]] {
			throw Error(errParams, kOrNotOpErrorMsg);
		}
		nextOp_ = OpOr;
		return *this;
	}
	[[nodiscard]] Query&& Or() && { return std::move(Or()); }

	/// Sets next operation type to Not.
	/// @return Query object.
	Query& Not() & {
		if (nextOp_ == OpOr) [[unlikely]] {
			throw Error(errParams, kOrNotOpErrorMsg);
		}
		nextOp_ = OpNot;
		return *this;
	}
	[[nodiscard]] Query&& Not() && { return std::move(Not()); }
	/// Sets next operation type to And.
	/// @return Query object.
	Query& And() & {
		nextOp_ = OpAnd;
		return *this;
	}
	[[nodiscard]] Query&& And() && { return std::move(And()); }
	Query& NextOp(OpType op) & {
		nextOp_ = op;
		return *this;
	}
	[[nodiscard]] Query&& NextOp(OpType op) && { return std::move(NextOp(op)); }
	[[nodiscard]] OpType NextOp() const noexcept { return nextOp_; }

	/// Insert open bracket to order logic operations.
	/// @return Query object.
	Query& OpenBracket() & {
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

	/// Sets the limit of selected rows.
	/// Analog to sql LIMIT rowsNumber.
	/// @param limit - number of rows to get from result set.
	/// @return Query object.
	Query& Limit(unsigned limit) & noexcept {
		count_ = limit;
		return *this;
	}
	[[nodiscard]] Query&& Limit(unsigned limit) && noexcept { return std::move(Limit(limit)); }

	/// Sets the number of the first selected row from result query.
	/// Analog to sql LIMIT OFFSET.
	/// @param offset - index of the first row to get from result set.
	/// @return Query object.
	Query& Offset(unsigned offset) & noexcept {
		start_ = offset;
		return *this;
	}
	[[nodiscard]] Query&& Offset(unsigned offset) && noexcept { return std::move(Offset(offset)); }

	/// Set the total count calculation mode to Accurate
	/// @return Query object
	Query& ReqTotal() & noexcept {
		calcTotal_ = ModeAccurateTotal;
		return *this;
	}
	[[nodiscard]] Query&& ReqTotal() && noexcept { return std::move(ReqTotal()); }

	/// Set the total count calculation mode to Cached.
	/// It will be use LRUCache for total count result
	/// @return Query object
	Query& CachedTotal() & noexcept {
		calcTotal_ = ModeCachedTotal;
		return *this;
	}
	[[nodiscard]] Query&& CachedTotal() && noexcept { return std::move(CachedTotal()); }

	/// Output fulltext rank
	/// Allowed only with fulltext query
	/// @return Query object
	Query& WithRank() & noexcept {
		withRank_ = true;
		return *this;
	}
	[[nodiscard]] Query&& WithRank() && noexcept { return std::move(WithRank()); }
	[[nodiscard]] bool IsWithRank() const noexcept { return withRank_; }

	/// Can we add aggregation functions
	/// or new select fields to a current query?
	[[nodiscard]] bool CanAddAggregation(AggType type) const noexcept { return type == AggDistinct || (selectFilter_.Fields().empty()); }
	[[nodiscard]] bool CanAddSelectFilter() const noexcept {
		return aggregations_.empty() || (aggregations_.size() == 1 && aggregations_.front().Type() == AggDistinct);
	}

	/// Serializes query data to stream.
	/// @param ser - serializer object for write.
	/// @param mode - serialization mode.
	void Serialize(WrSerializer& ser, uint8_t mode = Normal) const;

	/// Deserializes query data from stream.
	/// @param ser - serializer object.
	[[nodiscard]] static Query Deserialize(Serializer& ser);

	void WalkNested(bool withSelf, bool withMerged, bool withSubQueries, const std::function<void(const Query& q)>& visitor) const
		noexcept(noexcept(visitor(std::declval<Query>())));

	[[nodiscard]] bool HasLimit() const noexcept { return count_ != QueryEntry::kDefaultLimit; }
	[[nodiscard]] bool HasOffset() const noexcept { return start_ != QueryEntry::kDefaultOffset; }
	[[nodiscard]] bool IsWALQuery() const noexcept;
	[[nodiscard]] const std::vector<UpdateEntry>& UpdateFields() const noexcept { return updateFields_; }
	[[nodiscard]] QueryType Type() const noexcept { return type_; }
	[[nodiscard]] const std::string& NsName() const& noexcept { return namespace_; }
	[[nodiscard]] bool IsLocal() const noexcept { return local_; }
	template <concepts::ConvertibleToString T>
	void SetNsName(T&& nsName) & noexcept {
		namespace_ = std::forward<T>(nsName);
	}
	[[nodiscard]] unsigned Limit() const noexcept { return count_; }
	[[nodiscard]] unsigned Offset() const noexcept { return start_; }
	[[nodiscard]] CalcTotalMode CalcTotal() const noexcept { return calcTotal_; }
	[[nodiscard]] bool HasCalcTotal() const noexcept { return calcTotal_ != ModeNoTotal; }
	void CalcTotal(CalcTotalMode calcTotal) noexcept { calcTotal_ = calcTotal; }

	QueryType type_ = QuerySelect;				/// Query type
	std::vector<std::string> selectFunctions_;	/// List of sql functions

	std::vector<AggregateEntry> aggregations_;

	[[nodiscard]] auto NsName() const&& = delete;
	[[nodiscard]] const QueryEntries& Entries() const noexcept { return entries_; }
	/// Sets new entry value
	/// @param i - entry's offset
	/// @param args - construction arguments for the new entry
	/// @return actual count of the inserted entries
	template <typename T, typename... Args>
	[[nodiscard]] size_t SetEntry(size_t i, Args&&... args) {
		return entries_.SetValue(i, T{std::forward<Args>(args)...});
	}
	/// Tries to update values of the query entry without creation of the new entries
	/// @param i - entry's offset
	/// @param values - new values. Also return old values in case of success
	/// @return true - in case of success, false - in case if in-place update is not possible
	[[nodiscard]] bool TryUpdateQueryEntryInplace(size_t i, VariantArray& values) {
		return entries_.TryUpdateInplace<QueryEntry>(i, values);
	}
	void UpdateField(UpdateEntry&& ue) & { updateFields_.emplace_back(std::move(ue)); }

	Query& EqualPositions(EqualPosition_t&& ep) &;
	[[nodiscard]] Query&& EqualPositions(EqualPosition_t&& ep) && { return std::move(EqualPositions(std::move(ep))); }

	void Join(JoinedQuery&&) &;
	void ReserveQueryEntries(size_t s) & { entries_.Reserve(s); }
	template <typename T, typename... Args>
	Query& AppendQueryEntry(OpType op, Args&&... args) & {
		std::ignore = entries_.Append<T>(op, std::forward<Args>(args)...);
		return *this;
	}
	template <typename T, typename... Args>
	Query&& AppendQueryEntry(OpType op, Args&&... args) && {
		std::ignore = entries_.Append<T>(op, std::forward<Args>(args)...);
		return std::move(*this);
	}
	void SetLastOperation(OpType op) & { entries_.SetLastOperation(op); }
	[[nodiscard]] const Query& GetSubQuery(size_t i) const& noexcept { return subQueries_.at(i); }
	[[nodiscard]] const std::vector<Query>& GetSubQueries() const& noexcept { return subQueries_; }
	[[nodiscard]] const std::vector<JoinedQuery>& GetJoinQueries() const& noexcept { return joinQueries_; }
	[[nodiscard]] const std::vector<JoinedQuery>& GetMergeQueries() const& noexcept { return mergeQueries_; }
	[[nodiscard]] const FieldsNamesFilter& SelectFilters() const& noexcept { return selectFilter_; }
	void AddJoinQuery(JoinedQuery&&);
	void VerifyForUpdate() const;
	void VerifyForUpdateTransaction() const;
	template <InjectionDirection injectionDirection>
	size_t InjectConditionsFromOnConditions(size_t position, const h_vector<QueryJoinEntry, 1>& joinEntries,
											const QueryEntries& joinedQueryEntries, size_t joinedQueryNo,
											const std::vector<std::unique_ptr<Index>>* indexesFrom) {
		return entries_.InjectConditionsFromOnConditions<injectionDirection>(position, joinEntries, joinedQueryEntries, joinedQueryNo,
																			 indexesFrom);
	}

	void ReplaceSubQuery(size_t i, Query&& query);
	void ReplaceJoinQuery(size_t i, JoinedQuery&& query);
	void ReplaceMergeQuery(size_t i, JoinedQuery&& query);
	[[nodiscard]] const VariantArray& ForcedSortOrder() const& noexcept { return forcedSortOrder_; }
	[[nodiscard]] const SortingEntries& GetSortingEntries() const& noexcept { return sortingEntries_; }
	void ClearSorting() noexcept {
		sortingEntries_.clear();
		forcedSortOrder_.clear();
	}

	[[nodiscard]] auto GetSubQuery(size_t) const&& = delete;
	[[nodiscard]] auto GetSubQueries() const&& = delete;
	[[nodiscard]] auto GetJoinQueries() const&& = delete;
	[[nodiscard]] auto GetMergeQueries() const&& = delete;
	[[nodiscard]] auto SelectFilters() const&& = delete;
	[[nodiscard]] auto ForcedSortOrder() const&& = delete;
	[[nodiscard]] auto GetSortingEntries() const&& = delete;

private:
	class [[nodiscard]] PopBackQEGuard {
	public:
		explicit PopBackQEGuard(QueryEntries* e) noexcept : e_{e} {}
		~PopBackQEGuard() {
			if (e_) {
				e_->PopBack();
			}
		}
		void Reset() noexcept { e_ = nullptr; }

	private:
		QueryEntries* e_;
	};

	template <typename Q>
	class [[nodiscard]] OnHelperTempl;
	template <typename Q>
	class [[nodiscard]] OnHelperGroup {
	public:
		[[nodiscard]] OnHelperGroup&& Not() && noexcept {
			op_ = OpNot;
			return std::move(*this);
		}
		[[nodiscard]] OnHelperGroup&& Or() && noexcept {
			op_ = OpOr;
			return std::move(*this);
		}
		[[nodiscard]] OnHelperGroup&& On(std::string index, CondType cond, std::string joinIndex) &&;
		[[nodiscard]] Q CloseBracket() && noexcept { return std::forward<Q>(q_); }

	private:
		OnHelperGroup(Q q, JoinedQuery& jq) noexcept : q_{std::forward<Q>(q)}, jq_{jq} {}
		Q q_;
		JoinedQuery& jq_;
		OpType op_{OpAnd};
		friend class OnHelperTempl<Q>;
	};
	template <typename Q>
	class [[nodiscard]] OnHelperTempl {
	public:
		[[nodiscard]] OnHelperTempl&& Not() && noexcept {
			op_ = OpNot;
			return std::move(*this);
		}
		[[nodiscard]] Q On(std::string index, CondType cond, std::string joinIndex) &&;
		[[nodiscard]] OnHelperGroup<Q> OpenBracket() && noexcept { return {std::forward<Q>(q_), jq_}; }

	private:
		OnHelperTempl(Q q, JoinedQuery& jq) noexcept : q_{std::forward<Q>(q)}, jq_{jq} {}
		Q q_;
		JoinedQuery& jq_;
		OpType op_{OpAnd};
		friend class Query;
	};
	using OnHelper = OnHelperTempl<Query&>;
	using OnHelperR = OnHelperTempl<Query&&>;

	void checkSetObjectValue(const Variant& value) const;
	virtual void deserializeJoinOn(Serializer& ser);
	void deserialize(Serializer& ser, bool& hasJoinConditions);
	VariantArray deserializeValues(Serializer&, CondType) const;
	virtual void serializeJoinEntries(WrSerializer& ser) const;
	void checkSubQueryNoData() const;
	void checkSubQueryWithData() const;
	void checkSubQuery() const;
	void walkNested(bool withSelf, bool withMerged, bool withSubQueries,
					const std::function<void(Query& q)>& visitor) noexcept(noexcept(visitor(std::declval<Query&>())));
	void adoptNested(Query& nq) const noexcept { nq.Strict(GetStrictMode()).Explain(NeedExplain()).Debug(GetDebugLevel()); }

	SortingEntries sortingEntries_;				   /// Sorting data.
	VariantArray forcedSortOrder_;				   /// Keys that always go first - before any ordered values.
	std::string namespace_;						   /// Name of the namespace.
	unsigned start_ = QueryEntry::kDefaultOffset;  /// First row index from result set.
	unsigned count_ = QueryEntry::kDefaultLimit;   /// Number of rows from result set.
	CalcTotalMode calcTotal_ = ModeNoTotal;		   /// Calculation mode.
	QueryEntries entries_;						   /// List of 'where' entries
	std::vector<UpdateEntry> updateFields_;		   /// List of fields (and values) for update.
	std::vector<JoinedQuery> joinQueries_;		   /// List of queries for join.
	std::vector<JoinedQuery> mergeQueries_;		   /// List of merge queries.
	std::vector<Query> subQueries_;				   /// List of nested queries
	FieldsNamesFilter selectFilter_;			   /// List of columns in final result set.
	bool local_ = false;						   /// Local query if true
	bool withRank_ = false;						   /// Output fulltext/vectors rank in the results
	StrictMode strictMode_ = StrictModeNotSet;	   /// Strict mode.
	int debugLevel_ = 0;						   /// Debug level.
	bool explain_ = false;						   /// Explain query if true
	OpType nextOp_ = OpAnd;						   /// Next operation constant.
};

class [[nodiscard]] JoinedQuery final : public Query {
public:
	JoinedQuery(JoinType jt, const Query& q) : Query(q), joinType{jt} {}
	JoinedQuery(JoinType jt, Query&& q) : Query(std::move(q)), joinType{jt} {}
	using Query::Query;
	[[nodiscard]] bool operator==(const JoinedQuery& obj) const;
	[[nodiscard]] const std::string& RightNsName() const noexcept { return NsName(); }

	JoinType joinType{JoinType::LeftJoin};	   /// Default join type.
	h_vector<QueryJoinEntry, 1> joinEntries_;  /// Condition for join. Filled in each subqueries, empty in root query

private:
	void deserializeJoinOn(Serializer& ser) override;
	void serializeJoinEntries(WrSerializer& ser) const override;
};

template <typename Q>
[[nodiscard]] Q Query::OnHelperTempl<Q>::On(std::string index, CondType cond, std::string joinIndex) && {
	if (op_ == OpOr && jq_.joinEntries_.empty()) {
		throw Error{errLogic, "OR operator in first condition in ON"};
	}
	jq_.joinEntries_.emplace_back(op_, cond, std::move(index), std::move(joinIndex));
	return std::forward<Q>(q_);
}

template <typename Q>
[[nodiscard]] Query::OnHelperGroup<Q>&& Query::OnHelperGroup<Q>::On(std::string index, CondType cond, std::string joinIndex) && {
	if (op_ == OpOr && jq_.joinEntries_.empty()) {
		throw Error{errLogic, "OR operator in first condition in ON"};
	}
	jq_.joinEntries_.emplace_back(op_, cond, std::move(index), std::move(joinIndex));
	op_ = OpAnd;
	return std::move(*this);
}

[[nodiscard]] inline auto Query::Join(JoinType joinType, Query&& q) & {
	Join({joinType, std::move(q)});
	return OnHelper{*this, joinQueries_.back()};
}

[[nodiscard]] inline auto Query::Join(JoinType joinType, const Query& q) & {
	Join({joinType, q});
	return OnHelper{*this, joinQueries_.back()};
}

[[nodiscard]] inline auto Query::Join(JoinType joinType, Query&& q) && {
	Join({joinType, std::move(q)});
	return OnHelperR{std::move(*this), joinQueries_.back()};
}

[[nodiscard]] inline auto Query::Join(JoinType joinType, const Query& q) && {
	Join({joinType, q});
	return OnHelperR{std::move(*this), joinQueries_.back()};
}

}  // namespace reindexer
