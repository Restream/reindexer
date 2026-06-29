#include "core/query/query.h"
#include <algorithm>
#include <string>
#include "core/enums.h"
#include "core/function/function.h"
#include "core/query/dsl/dslencoder.h"
#include "core/query/dsl/dslparser.h"
#include "core/query/query_impl.h"
#include "core/query/queryentry.h"
#include "core/query/sql/sql_formatters.h"
#include "core/query/sql/sqlparser.h"
#include "core/type_consts.h"

namespace {
constexpr std::string_view kOrNotOpErrorMsg =
	"'OR NOT' operation is not supported yet. Use version with brackets instead: 'OR ( NOT ... )'";
}  // namespace

#if 0	// TODO
namespace reindexer {

using namespace std::string_view_literals;

Query::Query(Query&& other) noexcept = default;
Query::Query(const Query& other) = default;
Query::~Query() = default;

bool Query::operator==(const Query& obj) const {
	if (entries_ != obj.entries_ || aggregations_ != obj.aggregations_ ||

		NsName() != obj.NsName() || sortingEntries_ != obj.sortingEntries_ || CalcTotal() != obj.CalcTotal() || Offset() != obj.Offset() ||
		Limit() != obj.Limit() || debugLevel_ != obj.debugLevel_ || strictMode_ != obj.strictMode_ || selectFilter_ != obj.selectFilter_ ||
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

bool JoinedQuery::operator==(const JoinedQuery& obj) const {
	if (joinEntries_ != obj.joinEntries_) {
		return false;
	}
	if (joinType != obj.joinType) {
		return false;
	}
	return Query::operator==(obj);
}

void Query::AddJoinQuery(JoinedQuery&& jq) {
	adoptNested(jq);
	joinQueries_.emplace_back(std::move(jq));
}

}  // namespace reindexer
#endif	// TODO

/// ============================================= TODO remove this

namespace reindexer {
#define CATCH_INTO(state)                                                \
	catch (Error & e) {                                                  \
		state = std::move(e);                                            \
	}                                                                    \
	catch (std::exception & e) {                                         \
		state = std::move(e);                                            \
	}                                                                    \
	catch (...) {                                                        \
		state = Error(errLogic, "Broken query for an unknown reason"sv); \
	}

#define CATCH_INTO_STATE CATCH_INTO(state_)

#define CATCH_AND_RETURN_QUERY                                                 \
	catch (Error & e) {                                                        \
		return Query{std::move(e)};                                            \
	}                                                                          \
	catch (std::exception & e) {                                               \
		return Query{std::move(e)};                                            \
	}                                                                          \
	catch (...) {                                                              \
		return Query{Error(errLogic, "Broken query for an unknown reason"sv)}; \
	}

#define CATCH_AND_RETURN_UNEXPECTED                                               \
	catch (Error & e) {                                                           \
		return Unexpected{std::move(e)};                                          \
	}                                                                             \
	catch (std::exception & e) {                                                  \
		return Unexpected{Error{errLogic, "Broken query: {}", e.what()}};         \
	}                                                                             \
	catch (...) {                                                                 \
		return Unexpected{Error{errLogic, "Broken query for an unknown reason"}}; \
	}

// clang-format off
#define TRY_IF_STATE_OK \
	if (state_.ok()) [[likely]]{  \
		try {

#define CATCH_INTO_STATE_AND_RETURN_THIS \
    } CATCH_INTO_STATE                     \
	}                                    \
	return *this;
// clang-format on

using namespace std::string_view_literals;

Query::Query(Query&& other) noexcept : state_{std::move(other.state_)} {
	if (state_.ok()) {
		impl_ = std::move(other.impl_);
	}
}

Query::Query(const Query& other) noexcept : state_{other.state_} {
	if (state_.ok() && other.impl_) {
		initImpl(*other.impl_);
	}
}

Query::Query(impl::Query&& other) noexcept { initImpl(std::move(other)); }
Query::Query(Error&& err) noexcept : state_{std::move(err)} {
	assertrx(!state_.ok());	 // TODO _dbg
}

template <typename... Args>
void Query::initImpl(Args&&... args) noexcept {
	try {
		impl_ = std::make_shared<impl::Query>(std::forward<Args>(args)...);
	} CATCH_INTO_STATE
}
template void Query::initImpl(std::string&&, unsigned&, unsigned&, CalcTotalMode&) noexcept;

Query Query::FromSQL(std::string_view sql) noexcept {
	try {
		return Query{impl::Query::FromSQL(sql)};
	} CATCH_AND_RETURN_QUERY
}

Query Query::FromJSON(std::string_view dsl) noexcept {
	try {
		return Query{impl::Query::FromJSON(dsl)};
	} CATCH_AND_RETURN_QUERY
}

Query& Query::ReqTotal() & noexcept {
	if (state_.ok()) [[likely]] {
		impl().CalcTotal(ModeAccurateTotal);
	}
	return *this;
}
Query& Query::CachedTotal() & noexcept {
	if (state_.ok()) [[likely]] {
		impl().CalcTotal(ModeCachedTotal);
	}
	return *this;
}

Query& Query::Update() & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Type(QueryUpdate);
	}
	return *this;
}

Query& Query::Delete() & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Type(QueryDelete);
	}
	return *this;
}

Expected<std::string> Query::GetSQL() const noexcept {
	if (!state_.ok()) {
		return Unexpected{state_};
	}
	try {
		return impl().GetSQL();
	} CATCH_AND_RETURN_UNEXPECTED
}

void Query::SetNsName(std::string nsName) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().SetNsName(std::move(nsName));
	}
}

Query& Query::WithRank() & noexcept {
	if (state_.ok()) [[likely]] {
		impl().WithRank();
	}
	return *this;
}

Query& Query::Limit(unsigned limit) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Limit(limit);
	}
	return *this;
}
Query& Query::Offset(unsigned offset) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Offset(offset);
	}
	return *this;
}

Query& Query::Local(bool on) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Local(on);
	}
	return *this;
}

Query& Query::Strict(StrictMode mode) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Strict(mode);
	}
	return *this;
}

Query& Query::Explain(bool on) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Explain(on);
	}
	return *this;
}

Query& Query::Debug(int level) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().DebugLevel(level);
	}
	return *this;
}

Query& Query::Select(std::string field) & noexcept {
	if (state_.ok()) [[likely]] {
		impl().Select(std::move(field));
	}
	return *this;
}

Query& Query::SelectAllFields() & noexcept {
	if (state_.ok()) [[likely]] {
		impl().SelectAllFields();
	}
	return *this;
}

void Query::AddFunction(std::string function) noexcept {
	if (state_.ok()) [[likely]] {
		impl().AddSelectFunction(std::move(function));
	}
}

Query& Query::Set(std::string field, ValuesWrapper values, bool hasExpressions) & noexcept {
	TRY_IF_STATE_OK
		impl().Set(std::move(field), std::move(values), FieldModeSet, HasExpression(hasExpressions));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::SetObject(std::string field, ValuesWrapper values, bool hasExpressions) & noexcept {
	TRY_IF_STATE_OK
		impl().Set(std::move(field), std::move(values), FieldModeSetJson, HasExpression(hasExpressions));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Drop(std::string field) & noexcept {
	TRY_IF_STATE_OK
		impl().Set(std::move(field), {}, FieldModeDrop, HasExpression_False);
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Sort(std::string field, bool desc) & noexcept {
	TRY_IF_STATE_OK
		impl().Sort(std::move(field), Desc(desc), {});
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Sort(std::string field, bool desc, ValuesWrapper forcedSortOrder) & noexcept {
	TRY_IF_STATE_OK
		impl().Sort(std::move(field), Desc(desc), std::move(forcedSortOrder).Extract());
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::SortStDistance(std::string field, Point p, bool desc) & noexcept {
	TRY_IF_STATE_OK
		impl().SortStDistance(std::move(field), p, Desc(desc));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::SortStDistance(std::string field1, std::string field2, bool desc) & noexcept {
	TRY_IF_STATE_OK
		impl().SortStDistance(std::move(field1), std::move(field2), Desc(desc));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Aggregate(AggType type, h_vector<std::string, 1> fields, const std::vector<std::pair<std::string, bool>>& sort,
						unsigned limit, unsigned offset) & noexcept {
	TRY_IF_STATE_OK
		impl().Aggregate(type, std::move(fields), sort, limit, offset);
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Distinct(h_vector<std::string, 1> fields) & noexcept {
	TRY_IF_STATE_OK
		impl().Aggregate(AggDistinct, std::move(fields));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Where(std::string field, CondType cond, ValuesWrapper values) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendQueryEntry<QueryEntry>(nextOp_, std::move(field), cond, std::move(values).Extract());
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::WhereComposite(std::string idx, CondType cond, std::span<const VariantArray> v) & noexcept {
	TRY_IF_STATE_OK
		VariantArray values;
		values.reserve(v.size());
		for (auto it = v.begin(); it != v.end(); it++) {
			values.emplace_back(*it);
		}
		impl().AppendQueryEntry<QueryEntry>(nextOp_, std::move(idx), cond, std::move(values));
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::WhereBetweenFields(std::string firstIdx, CondType cond, std::string secondIdx) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendQueryEntry<BetweenFieldsQueryEntry>(nextOp_, std::move(firstIdx), cond, std::move(secondIdx));
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::DWithin(std::string field, Point p, double distance) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendQueryEntry<QueryEntry>(nextOp_, std::move(field), CondDWithin, VariantArray::Create(p, distance));
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::DWithin(Query&& q, Point p, double distance) & noexcept {
	TRY_IF_STATE_OK
		return Where(std::move(q), CondDWithin, VariantArray::Create(p, distance));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::WhereKNN(std::string field, FloatVector vec, KnnSearchParams params) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendQueryEntry<KnnQueryEntry>(nextOp_, std::move(field), std::move(vec), std::move(params));
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::WhereKNN(std::string field, std::string data, KnnSearchParams params) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendQueryEntry<KnnQueryEntry>(nextOp_, std::move(field), std::move(data), std::move(params));
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::WhereKNN(std::string field, ConstFloatVectorView vec, KnnSearchParams params) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendQueryEntry<KnnQueryEntry>(nextOp_, std::move(field), FloatVector(vec), std::move(params));
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Where(Query&& subQuery, CondType cond, ValuesWrapper values) & noexcept {
	TRY_IF_STATE_OK
		if (subQuery.state_.ok()) [[likely]] {
			if (subQuery.impl_.use_count() == 1) {
				impl().AppendSubQuery(nextOp_, std::move(subQuery.impl()), cond, std::move(values).Extract());
			} else {
				impl().AppendSubQuery(nextOp_, subQuery.impl(), cond, std::move(values).Extract());
			}
			nextOp_ = OpAnd;
		} else {
			state_ = Error(errQueryExec, "Broken subquery '{}'", subQuery.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Where(std::string&& field, CondType cond, Query&& subQuery) & noexcept {
	TRY_IF_STATE_OK
		if (subQuery.state_.ok()) [[likely]] {
			if (subQuery.impl_.use_count() == 1) {
				impl().AppendSubQuery(nextOp_, std::move(field), cond, std::move(subQuery.impl()));
			} else {
				impl().AppendSubQuery(nextOp_, std::move(field), cond, subQuery.impl());
			}
			nextOp_ = OpAnd;
		} else {
			state_ = Error(errQueryExec, "Broken subquery '{}'", subQuery.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

template <concepts::Function Function>
Query& Query::Where(Function&& function, CondType cond, ValuesWrapper values) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendFunction(nextOp_, std::forward<Function>(function), cond, std::move(values).Extract());
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}
template Query& Query::Where(functions::Now&&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(functions::Now&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(const functions::Now&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(functions::Serial&&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(functions::Serial&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(const functions::Serial&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(functions::FlatArrayLen&&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(functions::FlatArrayLen&, CondType, ValuesWrapper) & noexcept;
template Query& Query::Where(const functions::FlatArrayLen&, CondType, ValuesWrapper) & noexcept;

template <concepts::Function Function>
Query& Query::Where(std::string field, CondType cond, Function&& function) & noexcept {
	TRY_IF_STATE_OK
		impl().AppendFunction(nextOp_, std::move(field), cond, std::forward<Function>(function));
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}
template Query& Query::Where(std::string, CondType, functions::Now&&) & noexcept;
template Query& Query::Where(std::string, CondType, functions::Now&) & noexcept;
template Query& Query::Where(std::string, CondType, const functions::Now&) & noexcept;
template Query& Query::Where(std::string, CondType, functions::Serial&&) & noexcept;
template Query& Query::Where(std::string, CondType, functions::Serial&) & noexcept;
template Query& Query::Where(std::string, CondType, const functions::Serial&) & noexcept;
template Query& Query::Where(std::string, CondType, functions::FlatArrayLen&&) & noexcept;
template Query& Query::Where(std::string, CondType, functions::FlatArrayLen&) & noexcept;
template Query& Query::Where(std::string, CondType, const functions::FlatArrayLen&) & noexcept;

Query& Query::Where(std::string field, CondType cond, functions::FunctionVariant&& function) & noexcept {
	TRY_IF_STATE_OK
		std::visit([&](auto& fn) { impl().AppendFunction(nextOp_, std::move(field), cond, std::move(fn)); }, function);
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Where(functions::FunctionVariant&& function, CondType cond, ValuesWrapper values) & noexcept {
	TRY_IF_STATE_OK
		std::visit([&](auto& fn) { impl().AppendFunction(nextOp_, std::move(fn), cond, std::move(values).Extract()); }, function);
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Where(functions::FunctionVariant&& function, CondType cond, Query&& subQuery) & noexcept {
	TRY_IF_STATE_OK
		if (subQuery.state_.ok()) [[likely]] {
			if (subQuery.impl_.use_count() == 1) {
				impl().Append(nextOp_, std::move(function), cond, std::move(subQuery.impl()));
			} else {
				impl().Append(nextOp_, std::move(function), cond, subQuery.impl());
			}
			nextOp_ = OpAnd;
		} else {
			state_ = Error(errQueryExec, "Broken subquery '{}'", subQuery.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Where(Query&& subQuery, CondType cond, functions::FunctionVariant&& function) & noexcept {
	TRY_IF_STATE_OK
		if (subQuery.state_.ok()) [[likely]] {
			if (subQuery.impl_.use_count() == 1) {
				impl().Append(nextOp_, std::move(subQuery.impl()), cond, std::move(function));
			} else {
				impl().Append(nextOp_, subQuery.impl(), cond, std::move(function));
			}
			nextOp_ = OpAnd;
		} else {
			state_ = Error(errQueryExec, "Broken subquery '{}'", subQuery.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::EqualPositions(EqualPosition_t&& equalPosition) & noexcept {
	TRY_IF_STATE_OK
		impl().EqualPositions(std::move(equalPosition));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::EqualPositions(std::span<std::string> equalPosition) & noexcept {
	TRY_IF_STATE_OK
		impl().EqualPositions(EqualPosition_t(equalPosition.begin(), equalPosition.end()));
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Or() & noexcept {
	if (state_.ok()) [[likely]] {
		if (nextOp_ == OpNot) [[unlikely]] {
			state_ = Error(errParams, kOrNotOpErrorMsg);
		} else {
			nextOp_ = OpOr;
		}
	}
	return *this;
}

Query& Query::Not() & noexcept {
	if (state_.ok()) [[likely]] {
		if (nextOp_ == OpOr) [[unlikely]] {
			state_ = Error(errParams, kOrNotOpErrorMsg);
		} else {
			nextOp_ = OpNot;
		}
	}
	return *this;
}

Query& Query::Merge(const Query& q) & noexcept {
	TRY_IF_STATE_OK
		if (q.state_.ok()) {
			impl().Merge(q.impl());
		} else {
			state_ = Error(errQueryExec, "Merge broken query '{}'", q.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Merge(Query&& q) & noexcept {
	TRY_IF_STATE_OK
		if (q.state_.ok()) {
			if (q.impl_.use_count() == 1) {
				impl().Merge(std::move(q.impl()));
			} else {
				impl().Merge(q.impl());
			}
		} else {
			state_ = Error(errQueryExec, "Merge broken query '{}'", q.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Join(JoinType joinType, Query&& q, OpType op, std::string leftField, CondType cond, std::string rightField) & noexcept {
	TRY_IF_STATE_OK
		if (q.state_.ok()) {
			auto jq = q.impl_.use_count() == 1 ? impl::JoinedQuery{joinType, std::move(q.impl())} : impl::JoinedQuery{joinType, q.impl()};
			jq.EmplaceOnEntry(op, std::move(leftField), cond, std::move(rightField));
			impl().Join(nextOp_, std::move(jq));
			nextOp_ = OpAnd;
		} else {
			state_ = Error(errQueryExec, "Join broken query '{}'", q.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::Join(JoinType joinType, const Query& q, OpType op, std::string leftField, CondType cond, std::string rightField) & noexcept {
	TRY_IF_STATE_OK
		if (q.state_.ok()) {
			impl::JoinedQuery jq{joinType, q.impl()};
			jq.EmplaceOnEntry(op, std::move(leftField), cond, std::move(rightField));
			impl().Join(nextOp_, std::move(jq));
			nextOp_ = OpAnd;
		} else {
			state_ = Error(errQueryExec, "Join broken query '{}'", q.state_.whatStr());
		}
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query::OnHelper Query::Join(JoinType joinType, Query&& q) & noexcept {
	if (!q.state_.ok()) {
		state_ = Error(errQueryExec, "Join broken query '{}'", q.state_.whatStr());
	} else if (state_.ok()) {
		try {
			if (q.impl_.use_count() == 1) {
				return {*this, std::exchange(nextOp_, OpAnd), std::make_unique<impl::JoinedQuery>(joinType, std::move(q.impl()))};
			} else {
				return {*this, std::exchange(nextOp_, OpAnd), std::make_unique<impl::JoinedQuery>(joinType, q.impl())};
			}
		} CATCH_INTO_STATE
	}
	return {*this, nextOp_, nullptr};
}

Query::OnHelper Query::Join(JoinType joinType, const Query& q) & noexcept {
	if (!q.state_.ok()) {
		state_ = Error(errQueryExec, "Join broken query '{}'", q.state_.whatStr());
	} else if (state_.ok()) {
		try {
			return {*this, std::exchange(nextOp_, OpAnd), std::make_unique<impl::JoinedQuery>(joinType, q.impl())};
		} CATCH_INTO_STATE
	}
	return {*this, nextOp_, nullptr};
}

Query::OnHelperR Query::Join(JoinType joinType, Query&& q) && noexcept {
	if (!q.state_.ok()) {
		state_ = Error(errQueryExec, "Join broken query '{}'", q.state_.whatStr());
	} else if (state_.ok()) {
		try {
			if (q.impl_.use_count() == 1) {
				return {std::move(*this), std::exchange(nextOp_, OpAnd),
						std::make_unique<impl::JoinedQuery>(joinType, std::move(q.impl()))};
			} else {
				return {std::move(*this), std::exchange(nextOp_, OpAnd), std::make_unique<impl::JoinedQuery>(joinType, q.impl())};
			}
		} CATCH_INTO_STATE
	}
	return {std::move(*this), nextOp_, nullptr};
}

Query::OnHelperR Query::Join(JoinType joinType, const Query& q) && noexcept {
	if (!q.state_.ok()) {
		state_ = Error(errQueryExec, "Join broken query '{}'", q.state_.whatStr());
	} else if (state_.ok()) {
		try {
			return {std::move(*this), std::exchange(nextOp_, OpAnd), std::make_unique<impl::JoinedQuery>(joinType, q.impl())};
		} CATCH_INTO_STATE
	}
	return {std::move(*this), nextOp_, nullptr};
}

Query& Query::OpenBracket() & noexcept {
	TRY_IF_STATE_OK
		impl().OpenBracket(nextOp_);
		nextOp_ = OpAnd;
	CATCH_INTO_STATE_AND_RETURN_THIS
}

Query& Query::CloseBracket() & noexcept {
	TRY_IF_STATE_OK
		impl().CloseBracket();
	CATCH_INTO_STATE_AND_RETURN_THIS
}

template <typename Q>
[[nodiscard]] Q Query::OnHelperTempl<Q>::On(std::string index, CondType cond, std::string joinIndex) && noexcept {
	if (!mainQuery_.state_.ok()) {
		return std::forward<Q>(mainQuery_);
	}
	try {
		assertrx(joiningQuery_);  // _dbg
		joiningQuery_->EmplaceOnEntry(nextOnOp_, std::move(index), cond, std::move(joinIndex));
		mainQuery_.impl().Join(op_, std::move(*joiningQuery_));
	} CATCH_INTO(mainQuery_.state_)
	return std::forward<Q>(mainQuery_);
}
template Query& Query::OnHelperTempl<Query&>::On(std::string, CondType, std::string) && noexcept;
template Query&& Query::OnHelperTempl<Query&&>::On(std::string, CondType, std::string) && noexcept;

template <typename Q>
[[nodiscard]] Query::OnHelperGroup<Q>&& Query::OnHelperGroup<Q>::On(std::string index, CondType cond, std::string joinIndex) && noexcept {
	if (!mainQuery_.state_.ok()) {
		return std::move(*this);
	}
	try {
		assertrx(joiningQuery_);  // _dbg
		joiningQuery_->EmplaceOnEntry(nextOnOp_, std::move(index), cond, std::move(joinIndex));
		nextOnOp_ = OpAnd;
	} CATCH_INTO(mainQuery_.state_)
	return std::move(*this);
}
template Query::OnHelperGroup<Query&>&& Query::OnHelperGroup<Query&>::On(std::string, CondType, std::string) && noexcept;
template Query::OnHelperGroup<Query&&>&& Query::OnHelperGroup<Query&&>::On(std::string, CondType, std::string) && noexcept;

template <typename Q>
[[nodiscard]] Q Query::OnHelperGroup<Q>::CloseBracket() && noexcept {
	if (!mainQuery_.state_.ok()) {
		return std::forward<Q>(mainQuery_);
	}
	try {
		assertrx(joiningQuery_);  // _dbg
		mainQuery_.impl().Join(op_, std::move(*joiningQuery_));
	} CATCH_INTO(mainQuery_.state_)
	return std::forward<Q>(mainQuery_);
}
template Query& Query::OnHelperGroup<Query&>::CloseBracket() && noexcept;
template Query&& Query::OnHelperGroup<Query&&>::CloseBracket() && noexcept;

}  // namespace reindexer
