#pragma once

#include <type_traits>
#include <utility>
#include "query.h"

namespace reindexer {

class [[nodiscard]] ConstQueryImpl {
public:
	explicit ConstQueryImpl(const Query& q) noexcept : q_{&q} {}
	ConstQueryImpl(const Query&&) = delete;

	const Query& operator*() const noexcept { return *q_; }
	const std::string& NsName() const& noexcept { return q_->NsName(); }
	QueryType Type() const noexcept { return q_->type(); }
	void GetSQL(WrSerializer& ser, bool stripArgs = false, Pretty pretty = Pretty_False) const { q_->getSQL(ser, stripArgs, pretty); }
	void GetSQL(WrSerializer& ser, QueryType realType, bool stripArgs = false) const { q_->getSQL(ser, realType, stripArgs); }
	std::string GetSQL(QueryType realType, Pretty pretty = Pretty_False) const { return q_->getSQL(realType, pretty); }
	std::string GetSQL(bool stripArgs = false) const { return q_->GetSQL(stripArgs); }
	std::string GetJSON() const { return q_->GetJSON(); }
	CalcTotalMode CalcTotal() const noexcept { return q_->calcTotal(); }
	bool HasCalcTotal() const noexcept { return q_->hasCalcTotal(); }
	unsigned Limit() const noexcept { return q_->limit(); }
	bool HasLimit() const noexcept { return q_->hasLimit(); }
	unsigned Offset() const noexcept { return q_->offset(); }
	bool HasOffset() const noexcept { return q_->hasOffset(); }
	bool IsLocal() const noexcept { return q_->isLocal(); }
	bool IsWithRank() const noexcept { return q_->isWithRank(); }
	StrictMode GetStrictMode() const noexcept { return q_->getStrictMode(); }
	bool NeedExplain() const noexcept { return q_->needExplain(); }
	int DebugLevel() const noexcept { return q_->debugLevel(); }
	bool CanAddAggregation(AggType type) const noexcept { return q_->canAddAggregation(type); }
	bool CanAddSelectFilter() const noexcept { return q_->canAddSelectFilter(); }
	bool IsWALQuery() const noexcept { return q_->IsWALQuery(); }
	void Serialize(WrSerializer& ser, uint8_t mode, QueryFormat format) const { q_->serialize(ser, mode, format); }
	void VerifyForUpdate() const { q_->verifyForUpdate(); }
	void VerifyForUpdateTransaction() const { q_->verifyForUpdateTransaction(); }
	const std::vector<JoinedQuery>& JoinQueries() const& noexcept { return q_->joinQueries(); }
	const std::vector<JoinedQuery>& MergeQueries() const& noexcept { return q_->mergeQueries(); }
	const std::vector<Query>& SubQueries() const& noexcept { return q_->subQueries(); }
	const std::vector<AggregateEntry>& Aggregations() const& noexcept { return q_->aggregations(); }
	const QueryEntries& Entries() const& noexcept { return q_->entries(); }
	const SortingEntries& GetSortingEntries() const& noexcept { return q_->getSortingEntries(); }
	const std::vector<std::string>& SelectFunctions() const& noexcept { return q_->selectFunctions(); }
	const std::vector<UpdateEntry>& UpdateFields() const& noexcept { return q_->updateFields(); }
	const FieldsNamesFilter& SelectFilters() const& noexcept { return q_->selectFilters(); }
	const VariantArray& ForcedSortOrder() const& noexcept { return q_->forcedSortOrder(); }
	bool HasJoinQueries() const noexcept { return q_->hasJoinQueries(); }
	bool HasVolatileExpressions() const noexcept { return q_->hasVolatileExpressions(); }
	std::optional<int64_t> ExecutionNowNsec() const noexcept { return q_->executionNowNsec(); }

	void WalkNested(bool withSelf, bool withMerged, bool withSubQueries, const std::function<void(ConstQueryImpl)>& visitor) const;

private:
	const Query* q_;
};

class [[nodiscard]] QueryImpl {
public:
	explicit QueryImpl(Query& q) noexcept : q_{&q} {}
	QueryImpl(Query&&) = delete;

	Query& operator*() noexcept { return *q_; }
	const Query& operator*() const noexcept { return *q_; }
	Query* operator->() noexcept { return q_; }
	operator ConstQueryImpl() const noexcept { return ConstQueryImpl{*q_}; }

	const std::string& NsName() const& noexcept { return q_->NsName(); }
	template <concepts::ConvertibleToString Str>
	void SetNsName(Str&& nsName) {
		q_->setNsName(std::forward<Str>(nsName));
	}
	void NextOp(OpType op) noexcept { q_->nextOp(op); }
	QueryType Type() const noexcept { return q_->type(); }
	void Type(QueryType type) noexcept { q_->type(type); }
	void GetSQL(WrSerializer& ser, bool stripArgs = false, Pretty pretty = Pretty_False) const { q_->getSQL(ser, stripArgs, pretty); }
	void GetSQL(WrSerializer& ser, QueryType realType, bool stripArgs = false) const { q_->getSQL(ser, realType, stripArgs); }
	std::string GetSQL(QueryType realType, Pretty pretty = Pretty_False) const { return q_->getSQL(realType, pretty); }
	std::string GetSQL(bool stripArgs = false) const { return q_->GetSQL(stripArgs); }
	std::string GetJSON() const { return q_->GetJSON(); }
	void CalcTotal(CalcTotalMode total) noexcept { q_->calcTotal(total); }
	CalcTotalMode CalcTotal() const noexcept { return q_->calcTotal(); }
	bool HasCalcTotal() const noexcept { return q_->hasCalcTotal(); }
	unsigned Limit() const noexcept { return q_->limit(); }
	bool HasLimit() const noexcept { return q_->hasLimit(); }
	unsigned Offset() const noexcept { return q_->offset(); }
	bool HasOffset() const noexcept { return q_->hasOffset(); }
	bool IsLocal() const noexcept { return q_->isLocal(); }
	bool IsWithRank() const noexcept { return q_->isWithRank(); }
	StrictMode GetStrictMode() const noexcept { return q_->getStrictMode(); }
	bool NeedExplain() const noexcept { return q_->needExplain(); }
	int DebugLevel() const noexcept { return q_->debugLevel(); }
	bool CanAddAggregation(AggType type) const noexcept { return q_->canAddAggregation(type); }
	bool CanAddSelectFilter() const noexcept { return q_->canAddSelectFilter(); }
	void Set(UpdateEntry&& entry) { q_->set(std::move(entry)); }
	void ClearSorting() noexcept { q_->clearSorting(); }
	void ClearAggregations() noexcept { q_->clearAggregations(); }
	void ReserveQueryEntries(size_t s) { q_->reserveQueryEntries(s); }
	template <typename T, typename... Args>
	void AddCondition(OpType op, Args&&... args) {
		q_->addCondition<T>(op, std::forward<Args>(args)...);
	}
	void AddConditionSubQuery(OpType op, Query&& query, CondType cond, VariantArray values) {
		q_->addConditionSubQuery(op, std::move(query), cond, std::move(values));
	}
	void AddConditionSubQuery(OpType op, std::string field, CondType cond, Query&& query) {
		q_->addConditionSubQuery(op, std::move(field), cond, std::move(query));
	}
	template <concepts::Function Function>
	void AddConditionFunction(OpType op, Function&& function, CondType cond, VariantArray values) {
		q_->addConditionFunction(op, std::forward<Function>(function), cond, std::move(values));
	}
	template <concepts::ConvertibleToString Str, concepts::Function Function>
	void AddConditionFunction(OpType op, Str&& field, CondType cond, Function&& function) {
		q_->addConditionFunction(op, std::forward<Str>(field), cond, std::forward<Function>(function));
	}
	template <concepts::ConvertibleToString Str>
	void AddConditionFunction(OpType op, Str&& field, CondType cond, functions::FunctionVariant&& function) {
		q_->addConditionFunction(op, std::forward<Str>(field), cond, std::move(function));
	}
	void AddConditionFunction(OpType op, functions::FunctionVariant&& function, CondType cond, VariantArray values) {
		q_->addConditionFunction(op, std::move(function), cond, std::move(values));
	}
	void AddConditionFunctionSubQuery(OpType op, functions::FunctionVariant&& function, CondType cond, Query&& query) {
		q_->addConditionFunctionSubQuery(op, std::move(function), cond, std::move(query));
	}
	void AddConditionSubQueryFunction(OpType op, Query&& query, CondType cond, functions::FunctionVariant&& function) {
		q_->addConditionSubQueryFunction(op, std::move(query), cond, std::move(function));
	}
	void Join(OpType op, JoinedQuery&& query) { q_->join(op, std::move(query)); }
	bool IsWALQuery() const noexcept { return q_->IsWALQuery(); }
	void Serialize(WrSerializer& ser, uint8_t mode, QueryFormat format) const { q_->serialize(ser, mode, format); }
	template <typename T = Query>
	static T Deserialize(Serializer& ser, QueryFormat format) {
		return Query::deserialize<T>(ser, format);
	}
	template <typename T, typename... Args>
	size_t ReplaceQueryEntry(size_t i, Args&&... args) {
		return q_->replaceQueryEntry<T>(i, std::forward<Args>(args)...);
	}
	bool TryUpdateQueryEntryInplace(size_t i, VariantArray& values) { return q_->tryUpdateQueryEntryInplace(i, values); }
	template <JoinConditionInsertionDirection direction>
	size_t InsertConditionsFromOnConditions(size_t position, const h_vector<QueryJoinEntry, 1>& joinEntries,
											const QueryEntries& joinedQueryEntries, size_t joinedQueryNo,
											const std::vector<std::unique_ptr<Index>>* indexesFrom) {
		return q_->insertConditionsFromOnConditions<direction>(position, joinEntries, joinedQueryEntries, joinedQueryNo, indexesFrom);
	}
	void VerifyForUpdate() const { q_->verifyForUpdate(); }
	void VerifyForUpdateTransaction() const { q_->verifyForUpdateTransaction(); }
	void ReplaceSubQuery(size_t i, Query&& query) { q_->replaceSubQuery(i, std::move(query)); }
	void ReplaceJoinQuery(size_t i, JoinedQuery&& query) { q_->replaceJoinQuery(i, std::move(query)); }
	void ReplaceMergeQuery(size_t i, JoinedQuery&& query) { q_->replaceMergeQuery(i, std::move(query)); }
	const std::vector<JoinedQuery>& JoinQueries() const& noexcept { return q_->joinQueries(); }
	std::span<JoinedQuery> GetJoinQueriesSpan() noexcept { return q_->getJoinQueriesSpan(); }
	const std::vector<JoinedQuery>& MergeQueries() const& noexcept { return q_->mergeQueries(); }
	const std::vector<Query>& SubQueries() const& noexcept { return q_->subQueries(); }
	const std::vector<AggregateEntry>& Aggregations() const& noexcept { return q_->aggregations(); }
	const QueryEntries& Entries() const& noexcept { return q_->entries(); }
	const SortingEntries& GetSortingEntries() const& noexcept { return q_->getSortingEntries(); }
	const std::vector<std::string>& SelectFunctions() const& noexcept { return q_->selectFunctions(); }
	const std::vector<UpdateEntry>& UpdateFields() const& noexcept { return q_->updateFields(); }
	const FieldsNamesFilter& SelectFilters() const& noexcept { return q_->selectFilters(); }
	const VariantArray& ForcedSortOrder() const& noexcept { return q_->forcedSortOrder(); }
	bool HasJoinQueries() const noexcept { return q_->hasJoinQueries(); }
	bool HasVolatileExpressions() const noexcept { return q_->hasVolatileExpressions(); }
	std::optional<int64_t> ExecutionNowNsec() const noexcept { return q_->executionNowNsec(); }
	void ExecutionNowNsec(int64_t value) noexcept { q_->executionNowNsec(value); }

private:
	Query* q_;
};

inline QueryImpl Impl(Query& q) noexcept { return QueryImpl{q}; }
inline ConstQueryImpl Impl(const Query& q) noexcept { return ConstQueryImpl{q}; }
QueryImpl Impl(Query&&) = delete;
ConstQueryImpl Impl(const Query&&) = delete;

class [[nodiscard]] ConstJoinedQueryImpl {
public:
	explicit ConstJoinedQueryImpl(const JoinedQuery& q) noexcept : q_{&q} {}
	ConstJoinedQueryImpl(const JoinedQuery&&) = delete;

	const JoinedQuery& operator*() const noexcept { return *q_; }

	const std::string& RightNsName() const noexcept { return q_->rightNsName(); }
	JoinType GetJoinType() const noexcept { return q_->getJoinType(); }
	const h_vector<QueryJoinEntry, 1>& JoinEntries() const noexcept { return q_->joinEntries(); }
	bool HasVolatileExpressions() const noexcept { return ConstQueryImpl{*q_}.HasVolatileExpressions(); }

private:
	const JoinedQuery* q_;
};

class [[nodiscard]] JoinedQueryImpl {
public:
	explicit JoinedQueryImpl(JoinedQuery& q) noexcept : q_{&q} {}
	JoinedQueryImpl(JoinedQuery&&) = delete;

	JoinedQuery& operator*() noexcept { return *q_; }
	const JoinedQuery& operator*() const noexcept { return *q_; }
	operator ConstJoinedQueryImpl() const noexcept { return ConstJoinedQueryImpl{*q_}; }

	const std::string& RightNsName() const noexcept { return q_->rightNsName(); }
	JoinType GetJoinType() const noexcept { return q_->getJoinType(); }
	const h_vector<QueryJoinEntry, 1>& JoinEntries() const noexcept { return q_->joinEntries(); }

	void SetJoinType(JoinType type) noexcept { q_->setJoinType(type); }
	void EmplaceBackOnEntry(OpType op, std::string leftField, CondType cond, std::string rightField,
							ReverseNsOrder reverseNsOrder = ReverseNsOrder_False) {
		q_->emplaceBackOnEntry(op, std::move(leftField), cond, std::move(rightField), reverseNsOrder);
	}

private:
	JoinedQuery* q_;
};

inline JoinedQueryImpl JoinedImpl(JoinedQuery& q) noexcept { return JoinedQueryImpl{q}; }
inline ConstJoinedQueryImpl JoinedImpl(const JoinedQuery& q) noexcept { return ConstJoinedQueryImpl{q}; }
JoinedQueryImpl JoinedImpl(JoinedQuery&&) = delete;
ConstJoinedQueryImpl JoinedImpl(const JoinedQuery&&) = delete;

static_assert(sizeof(QueryImpl) == sizeof(Query*) && std::is_trivially_copyable_v<QueryImpl>);
static_assert(sizeof(ConstQueryImpl) == sizeof(Query*) && std::is_trivially_copyable_v<ConstQueryImpl>);
static_assert(sizeof(JoinedQueryImpl) == sizeof(JoinedQuery*) && std::is_trivially_copyable_v<JoinedQueryImpl>);
static_assert(sizeof(ConstJoinedQueryImpl) == sizeof(JoinedQuery*) && std::is_trivially_copyable_v<ConstJoinedQueryImpl>);
static_assert(!std::is_constructible_v<ConstQueryImpl, Query>);
static_assert(!std::is_constructible_v<QueryImpl, Query>);
static_assert(std::is_same_v<decltype(*std::declval<const QueryImpl&>()), const Query&>);
static_assert(std::is_same_v<decltype(*std::declval<const JoinedQueryImpl&>()), const JoinedQuery&>);

}  // namespace reindexer
