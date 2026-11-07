#include "functions_optimizations.h"
#include "core/function/precomputed_values.h"
#include "core/query/query_impl.h"
#include "tools/assertrx.h"
namespace reindexer {
namespace {

template <concepts::OneOf<Query, JoinedQuery> Parent, concepts::OneOf<Query, JoinedQuery> Child, typename Replace>
void PropagateExecutionNow(const Parent& original, const std::vector<Child>& nested, std::optional<Parent>& queryCopy,
						   functions::PrecomputedValues& precomputedValues, Replace replace) {
	for (size_t i = 0, sz = nested.size(); i < sz; ++i) {
		std::optional<Child> childCopy;
		OptimizeFunctionEntries(nested[i], childCopy, precomputedValues);
		if (!childCopy.has_value()) {
			continue;
		}
		if (!queryCopy.has_value()) {
			queryCopy.emplace(original);
		}
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access) both optionals are engaged above
		replace(*queryCopy, i, std::move(*childCopy));
	}
}

// The same now() snapshot is applied to nested joins, merges and subqueries.
template <concepts::OneOf<Query, JoinedQuery> Q>
void PropagateExecutionNow(const Q& query, std::optional<Q>& queryCopy, functions::PrecomputedValues& precomputedValues) {
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access) checked with has_value()
	const ConstQueryImpl currentImpl = queryCopy.has_value() ? Impl(*queryCopy) : Impl(query);
	PropagateExecutionNow(query, currentImpl.SubQueries(), queryCopy, precomputedValues,
						  [](Q& parent, size_t i, Query&& child) { Impl(parent).ReplaceSubQuery(i, std::move(child)); });
	PropagateExecutionNow(query, currentImpl.JoinQueries(), queryCopy, precomputedValues,
						  [](Q& parent, size_t i, JoinedQuery&& child) { Impl(parent).ReplaceJoinQuery(i, std::move(child)); });
	PropagateExecutionNow(query, currentImpl.MergeQueries(), queryCopy, precomputedValues,
						  [](Q& parent, size_t i, JoinedQuery&& child) { Impl(parent).ReplaceMergeQuery(i, std::move(child)); });
}

}  // namespace

template <concepts::OneOf<Query, JoinedQuery> Q>
void OptimizeFunctionEntries(const Q& query, std::optional<Q>& queryCopy, functions::PrecomputedValues& precomputedValues) {
	bool nowCalledOnce = false;
	bool arithmeticUsesNow = false;
	h_vector<std::pair<size_t, std::variant<QueryEntry, SubQueryEntry>>, 1> optimizedEntries;

	auto computeNowOnce = [&](const auto& now) -> std::optional<Variant> {
		if (!nowCalledOnce) {
			precomputedValues.Put(functions::Now(TimeUnit::nsec));
			nowCalledOnce = true;
		}
		if (auto v = precomputedValues.Get(now); v.has_value()) {
			return v;
		}
		return std::optional<Variant>{};
	};

	ConstQueryImpl queryImpl = Impl(query);
	for (const auto& ue : queryImpl.UpdateFields()) {
		if (ue.IsExpression()) {
			if (!nowCalledOnce) {
				precomputedValues.Put(functions::Now(TimeUnit::nsec));
				nowCalledOnce = true;
			}
		}
	}

	for (size_t i = 0, size = queryImpl.Entries().Size(); i < size; ++i) {
		queryImpl.Entries().Visit(
			i,
			Skip<QueryEntriesBracket, QueryEntry, BetweenFieldsQueryEntry, JoinQueryEntry, AlwaysTrue, AlwaysFalse, SubQueryEntry,
				 SubQueryFieldEntry, MultiDistinctQueryEntry, KnnQueryEntry>{},
			[&](const QueryArithmeticEntry& qe) noexcept { arithmeticUsesNow |= qe.UsesNow(); },
			[&](const QueryFunctionEntry& qe) {
				std::visit(overloaded{[&](const functions::Now& now) {
										  if (auto v = computeNowOnce(now); v.has_value()) {
											  optimizedEntries.emplace_back(
												  i, QueryEntry{qe.ComparisonField().FieldName(), qe.Condition(), VariantArray{v.value()}});
										  }
									  },
									  [&](const auto&) {}},
						   qe.FunctionVariant());
			},
			[&](const SubQueryFunctionEntry& qe) {
				std::visit(overloaded{[&](const functions::Now& now) {
										  if (auto v = computeNowOnce(now); v.has_value()) {
											  optimizedEntries.emplace_back(
												  i, SubQueryEntry{qe.Condition(), qe.QueryIndex(), VariantArray{v.value()}});
										  }
									  },
									  [&](const auto&) {}},
						   qe.FunctionVariant());
			});
	}

	if (arithmeticUsesNow) {
		precomputedValues.Put(functions::Now(TimeUnit::nsec));
		if (!queryCopy.has_value()) {
			queryCopy.emplace(query);
		}
		const auto nowNsec = precomputedValues.GetNowNsec();
		assertrx_throw(nowNsec.has_value());
		Impl(*queryCopy).ExecutionNowNsec(*nowNsec);
	}

	if (!optimizedEntries.empty()) {
		if (!queryCopy.has_value()) {
			queryCopy.emplace(query);
		}
		QueryImpl queryCopyImpl = Impl(*queryCopy);
		for (auto& [i, entry] : optimizedEntries) {
			size_t inserted = std::visit(
				overloaded{[&](QueryEntry& qe) { return queryCopyImpl.template ReplaceQueryEntry<QueryEntry>(i, std::move(qe)); },
						   [&](SubQueryEntry& sqe) { return queryCopyImpl.template ReplaceQueryEntry<SubQueryEntry>(i, std::move(sqe)); }},
				entry);
			if (inserted != 1) {
				throw Error(errLogic, "Failed to optimize QueryFunctionEntry: wrong number of inserted entries {}", inserted);
			}
		}
	}

	PropagateExecutionNow(query, queryCopy, precomputedValues);
}

template void OptimizeFunctionEntries(const Query& query, std::optional<Query>& queryCopy, functions::PrecomputedValues&);

template void OptimizeFunctionEntries(const JoinedQuery& query, std::optional<JoinedQuery>& queryCopy, functions::PrecomputedValues&);

}  // namespace reindexer
