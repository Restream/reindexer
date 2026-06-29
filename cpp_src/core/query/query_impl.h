#pragma once

#include <string>
#include "core/enums.h"
#include "core/function/function.h"
#include "core/keyvalue/variant.h"
#include "core/query/queryentry.h"
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "fields_names_filter.h"
#include "query_consts.h"

namespace reindexer {

constexpr std::string_view kAggregationWithSelectFieldsMsgError =
	"Not allowed to combine aggregation functions and fields' filter in a single query";

class WrSerializer;
class UpdateEntry;

namespace impl {

class JoinedQuery;

class [[nodiscard]] Query {
	template <typename QE>
	struct [[nodiscard]] QueryEntryValidater {
		static void Validate(const auto&...) noexcept {}
	};

public:
	template <concepts::ConvertibleToString Str>
	explicit Query(Str&& nsName, unsigned offset = kQueryMinOffset, unsigned limit = kQueryMaxLimit, CalcTotalMode calcTotal = ModeNoTotal)
		: namespace_(std::forward<Str>(nsName)), offset_(offset), limit_(limit), calcTotal_(calcTotal) {}
	virtual ~Query() = default;
	Query(const Query&);
	Query(Query&&) noexcept = default;
	Query& operator=(Query&&) noexcept = default;
	bool operator==(const Query&) const;

	static Query CreateEmpty() noexcept { return {}; }

	[[nodiscard]] const std::string& NsName() const& noexcept { return namespace_; }
	template <concepts::ConvertibleToString Str>
	void SetNsName(Str&& nsName) & noexcept {
		namespace_ = std::forward<Str>(nsName);
	}

	QueryType Type() const noexcept { return type_; }
	void Type(QueryType type) noexcept { type_ = type; }

	[[nodiscard]] static Query FromSQL(std::string_view q);

	void GetSQL(WrSerializer& ser, bool stripArgs = false, Pretty pretty = Pretty_False) const;
	void GetSQL(WrSerializer& ser, QueryType realType, bool stripArgs = false) const;
	[[nodiscard]] std::string GetSQL(bool stripArgs = false) const;
	[[nodiscard]] std::string GetSQL(QueryType realType, Pretty pretty = Pretty_False) const;

	static Query FromJSON(std::string_view dsl);
	[[nodiscard]] std::string GetJSON() const;

	void CalcTotal(CalcTotalMode total) noexcept { calcTotal_ = total; }
	CalcTotalMode CalcTotal() const noexcept { return calcTotal_; }
	bool HasCalcTotal() const noexcept { return calcTotal_ != ModeNoTotal; }

	void Limit(unsigned limit) noexcept { limit_ = limit; }
	unsigned Limit() const noexcept { return limit_; }
	bool HasLimit() const noexcept { return limit_ != kQueryMaxLimit; }

	void Offset(unsigned offset) noexcept { offset_ = offset; }
	unsigned Offset() const noexcept { return offset_; }
	bool HasOffset() const noexcept { return offset_ != kQueryMinOffset; }

	void Local(bool on = true) noexcept { local_ = on; }
	[[nodiscard]] bool IsLocal() const noexcept { return local_; }

	void WithRank() noexcept { withRank_ = true; }
	[[nodiscard]] bool IsWithRank() const noexcept { return withRank_; }

	void Strict(StrictMode mode) noexcept {
		walkNested(true, true, true, [mode](Query& q) noexcept { q.strictMode_ = mode; });
	}
	StrictMode GetStrictMode() const noexcept { return strictMode_; }

	void Explain(bool on = true) noexcept {
		walkNested(true, true, true, [on](Query& q) noexcept { q.explain_ = on; });
	}
	[[nodiscard]] bool NeedExplain() const noexcept { return explain_; }

	void DebugLevel(int level) noexcept {
		walkNested(true, true, true, [level](Query& q) noexcept { q.debugLevel_ = level; });
	}
	int DebugLevel() const noexcept { return debugLevel_; }

	void Select(std::string field);

	void SelectAllFields() {
		selectFilter_.SetAllRegularFields();
		selectFilter_.SetAllVectorFields();
	}

	[[nodiscard]] bool CanAddAggregation(AggType type) const noexcept { return type == AggDistinct || (selectFilter_.Fields().empty()); }
	[[nodiscard]] bool CanAddSelectFilter() const noexcept {
		return aggregations_.empty() || (aggregations_.size() == 1 && aggregations_.front().Type() == AggDistinct);
	}

	void AddSelectFunction(std::string function) { selectFunctions_.emplace_back(std::move(function)); }

	void Set(std::string field, VariantArray values, FieldModifyMode mode, HasExpression hasExpressions);
	void Set(UpdateEntry&& entry) { updateFields_.push_back(std::move(entry)); }

	void Sort(std::string sort, Desc desc, VariantArray forcedSortOrder);

	void SortStDistance(std::string field, reindexer::Point p, Desc desc);

	void SortStDistance(std::string field1, std::string field2, Desc desc);

	void ClearSorting() noexcept {
		sortingEntries_.clear();
		forcedSortOrder_.clear();
	}

	void Aggregate(AggType type, h_vector<std::string, 1> fields, const std::vector<std::pair<std::string, bool>>& sort = {},
				   unsigned limit = QueryEntry::kDefaultLimit, unsigned offset = QueryEntry::kDefaultOffset);
	void Aggregate(AggType type, h_vector<std::string, 1>&& fields, SortingEntries&&, unsigned limit, unsigned offset);
	void ClearAggregations() noexcept { aggregations_.clear(); }

	void WalkNested(bool withSelf, bool withMerged, bool withSubQueries, const std::function<void(const Query& q)>& visitor) const
		noexcept(noexcept(visitor(std::declval<const Query&>())));

	void ReserveQueryEntries(size_t s) & { entries_.Reserve(s); }
	template <typename T, typename... Args>
	void AppendQueryEntry(OpType op, Args&&... args) {
		QueryEntryValidater<T>::Validate(op, static_cast<const Args&>(args)...);
		std::ignore = entries_.Append<T>(op, std::forward<Args>(args)...);
	}
	void AppendSubQuery(OpType, Query&&, CondType, VariantArray);
	void AppendSubQuery(OpType, const Query&, CondType, VariantArray);
	void AppendSubQuery(OpType, std::string field, CondType, Query&&);
	void AppendSubQuery(OpType, std::string field, CondType, const Query&);

	template <concepts::Function Function>
	void AppendFunction(OpType op, Function&& function, CondType cond, VariantArray values) {
		checkFunctionForLeftExpression(function.Type());
		std::ignore = entries_.Append<QueryFunctionEntry>(op, std::forward<Function>(function), cond, std::move(values));
	}
	template <concepts::Function Function>
	void AppendFunction(OpType op, std::string field, CondType cond, Function&& function) {
		checkFunctionForRightExpression(function.Type());
		std::ignore = entries_.Append<QueryFunctionEntry>(op, std::move(field), cond, std::forward<Function>(function));
	}
	void AppendFunction(OpType op, std::string field, CondType cond, functions::FunctionVariant&& function) {
		std::visit([&](auto& fn) { AppendFunction(op, std::move(field), cond, std::move(fn)); }, function);
	}
	void AppendFunction(OpType op, functions::FunctionVariant&& function, CondType cond, VariantArray values) {
		std::visit([&](auto& fn) { AppendFunction(op, std::move(fn), cond, std::move(values)); }, function);
	}
	void Append(OpType, functions::FunctionVariant&&, CondType, Query&&);
	void Append(OpType, functions::FunctionVariant&&, CondType, const Query&);
	void Append(OpType, Query&&, CondType, functions::FunctionVariant&&);
	void Append(OpType, const Query&, CondType, functions::FunctionVariant&&);

	void Merge(const Query&);
	void Merge(Query&&);

	void Join(OpType, JoinedQuery&&);
	void AddJoinQuery(JoinedQuery&&);

	void OpenBracket(OpType op) { entries_.OpenBracket(op); }
	void CloseBracket() { entries_.CloseBracket(); }

	void EqualPositions(EqualPosition_t&&);
	bool IsWALQuery() const noexcept;

	void Serialize(WrSerializer&, uint8_t mode = Normal) const;
	static Query Deserialize(Serializer&);

	template <typename T, typename... Args>
	[[nodiscard]] size_t EmplaceQueryEntry(size_t i, Args&&... args) {
		return entries_.SetValue(i, T{std::forward<Args>(args)...});
	}
	[[nodiscard]] bool TryUpdateQueryEntryInplace(size_t i, VariantArray& values) {
		return entries_.TryUpdateInplace<QueryEntry>(i, values);
	}
	template <JoinConditionInsertionDirection insertionDirection>
	size_t InsertConditionsFromOnConditions(size_t position, const h_vector<QueryJoinEntry, 1>& joinEntries,
											const QueryEntries& joinedQueryEntries, size_t joinedQueryNo,
											const std::vector<std::unique_ptr<Index>>* indexesFrom) {
		return entries_.InsertConditionsFromOnConditions<insertionDirection>(position, joinEntries, joinedQueryEntries, joinedQueryNo,
																			 indexesFrom);
	}

	void VerifyForUpdate() const;
	void VerifyForUpdateTransaction() const;

	void ReplaceSubQuery(size_t, Query&&);
	void ReplaceJoinQuery(size_t, JoinedQuery&&);
	void ReplaceMergeQuery(size_t, JoinedQuery&&);

	[[nodiscard]] const std::vector<JoinedQuery>& JoinQueries() const& noexcept { return joinQueries_; }
	[[nodiscard]] const std::vector<JoinedQuery>& MergeQueries() const& noexcept { return mergeQueries_; }
	[[nodiscard]] const std::vector<Query>& SubQueries() const& noexcept { return subQueries_; }
	[[nodiscard]] const std::vector<AggregateEntry>& Aggregations() const& noexcept { return aggregations_; }
	[[nodiscard]] const QueryEntries& Entries() const& noexcept { return entries_; }
	[[nodiscard]] const SortingEntries& GetSortingEntries() const& noexcept { return sortingEntries_; }
	[[nodiscard]] const std::vector<std::string>& SelectFunctions() const& noexcept { return selectFunctions_; }
	[[nodiscard]] const std::vector<UpdateEntry>& UpdateFields() const& noexcept { return updateFields_; }
	[[nodiscard]] const FieldsNamesFilter& SelectFilters() const& noexcept { return selectFilter_; }
	[[nodiscard]] const VariantArray& ForcedSortOrder() const& noexcept { return forcedSortOrder_; }

	auto JoinQueries() const&& = delete;
	auto MergeQueries() const&& = delete;
	auto SubQueries() const&& = delete;
	auto Aggregations() const&& = delete;
	auto Entries() const&& = delete;
	auto GetSortingEntries() const&& = delete;
	auto UpdateFields() const&& = delete;
	auto SelectFilters() const&& = delete;
	auto ForcedSortOrder() const&& = delete;

protected:
	Query() noexcept;

private:
	template <typename QE, typename LHS, typename RHS>
	void appendSubQuery(OpType, LHS&&, CondType, RHS&&);
	template <typename Q>
	void appendSubQuery(OpType, Q&& subQuery, CondType, VariantArray&&);
	void appendSubQueryImpl(OpType, Query& subQuery, CondType, VariantArray&&);
	void checkSetObjectValue(const Variant& value) const;
	void walkNested(bool withSelf, bool withMerged, bool withSubQueries,
					const std::function<void(Query& q)>& visitor) noexcept(noexcept(visitor(std::declval<Query&>())));
	void adoptNested(Query& nested) const noexcept {
		nested.Strict(strictMode_);
		nested.Explain(explain_);
		nested.DebugLevel(debugLevel_);
	}
	void checkSubQueryNoData() const;
	void checkSubQueryWithData() const;
	void checkSubQuery() const;
	static void checkFunctionForLeftExpression(FunctionType);
	static void checkFunctionForRightExpression(FunctionType);

	virtual void serializeJoinEntries(WrSerializer&) const;
	void deserialize(Serializer&);
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
};

class [[nodiscard]] JoinedQuery final : public Query {
public:
	JoinedQuery(JoinType jt, const Query& q) : Query(q), joinType_{jt} {}
	JoinedQuery(JoinType jt, Query&& q) : Query(std::move(q)), joinType_{jt} {}
	JoinedQuery(const JoinedQuery&) = default;
	JoinedQuery(JoinedQuery&&) noexcept = default;
	JoinedQuery& operator=(JoinedQuery&&) noexcept = default;

	static JoinedQuery CreateEmpty() noexcept { return {}; }

	[[nodiscard]] const std::string& RightNsName() const noexcept { return NsName(); }
	JoinType GetJoinType() const noexcept { return joinType_; }
	void SetJoinType(JoinType type) noexcept { joinType_ = type; }

	void EmplaceOnEntry(OpType op, std::string leftField, CondType cond, std::string rightField,
						ReverseNsOrder reverseNsOrder = ReverseNsOrder_False) {
		joinEntries_.emplace_back(op, std::move(leftField), cond, std::move(rightField), reverseNsOrder);
	}
	const h_vector<QueryJoinEntry, 1>& JoinEntries() const& noexcept { return joinEntries_; }

private:
	JoinedQuery() noexcept = default;

	void deserializeJoinOn(Serializer&) override;
	void serializeJoinEntries(WrSerializer&) const override;

	JoinType joinType_{JoinType::LeftJoin};
	h_vector<QueryJoinEntry, 1> joinEntries_;  /// Condition for join. Filled in each subqueries, empty in root query
};

template <>
struct [[nodiscard]] Query::QueryEntryValidater<KnnQueryEntry> {
	static void Validate(OpType op, const auto&, const auto&, const KnnSearchParams& params) {
		if (op == OpNot) [[unlikely]] {
			throw Error(errLogic, "NOT operation is not allowed with knn condition");
		}
		params.Validate();
	}
};

}  // namespace impl
}  // namespace reindexer
