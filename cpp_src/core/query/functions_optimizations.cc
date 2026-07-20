#include "functions_optimizations.h"
#include "core/function/precomputed_values.h"
#include "core/query/query.h"

namespace reindexer {

template <concepts::OneOf<Query, JoinedQuery> Q>
void OptimizeFunctionEntries(const Q& query, std::optional<Q>& queryCopy, functions::PrecomputedValues& precomputedValues) {
	bool nowCalledOnce = false;
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

	for (const auto& ue : query.UpdateFields()) {
		if (ue.IsExpression()) {
			if (!nowCalledOnce) {
				precomputedValues.Put(functions::Now(TimeUnit::nsec));
				nowCalledOnce = true;
			}
		}
	}

	for (size_t i = 0, size = query.Entries().Size(); i < size; ++i) {
		query.Entries().Visit(
			i,
			Skip<QueryEntriesBracket, QueryEntry, BetweenFieldsQueryEntry, JoinQueryEntry, AlwaysTrue, AlwaysFalse, SubQueryEntry,
				 SubQueryFieldEntry, MultiDistinctQueryEntry, KnnQueryEntry>{},
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

	if (!optimizedEntries.empty()) {
		if (!queryCopy.has_value()) {
			queryCopy.emplace(query);
		}
		for (auto& [i, entry] : optimizedEntries) {
			size_t inserted =
				std::visit(overloaded{[&](QueryEntry& qe) { return queryCopy->template SetEntry<QueryEntry>(i, std::move(qe)); },
									  [&](SubQueryEntry& sqe) { return queryCopy->template SetEntry<SubQueryEntry>(i, std::move(sqe)); }},
						   entry);
			if (inserted != 1) {
				throw Error(errLogic, "Failed to optimize QueryFunctionEntry: wrong number of inserted entries {}", inserted);
			}
		}
	}
}

template void OptimizeFunctionEntries<reindexer::Query>(const reindexer::Query& query, std::optional<reindexer::Query>& queryCopy,
														functions::PrecomputedValues&);

template void OptimizeFunctionEntries<reindexer::JoinedQuery>(const reindexer::JoinedQuery& query,
															  std::optional<reindexer::JoinedQuery>& queryCopy,
															  functions::PrecomputedValues&);

}  // namespace reindexer
