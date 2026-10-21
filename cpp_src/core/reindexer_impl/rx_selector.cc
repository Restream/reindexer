#include "rx_selector.h"
#include "core/nsselecter/joins/results.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/query/query_impl.h"
#include "core/queryresults/context.h"
#include "core/queryresults/queryresults.h"
#include "tools/logger.h"

namespace reindexer {
namespace {
/// Recursively checks if the query or its joined queries contain subqueries.
/// Merged queries are intentionally ignored.
///
/// @param q - The query to inspect.
/// @returns true if at least one subquery is found in the main query or its join tree.
bool HasSubqueries(ConstQueryImpl q) noexcept {
	if (!q.SubQueries().empty()) {
		return true;
	}
	for (const JoinedQuery& jq : q.JoinQueries()) {
		if (HasSubqueries(Impl(jq))) {
			return true;
		}
	}
	return false;
}
}  // namespace

class [[nodiscard]] ItemRefLess {
public:
	bool operator()(const ItemRef& lhs, const ItemRef& rhs) const noexcept {
		if (lhs.Nsid() == rhs.Nsid()) {
			return lhs.Id() < rhs.Id();
		}
		return lhs.Nsid() < rhs.Nsid();
	}
};

template <RankOrdering rankOrdering>
class [[nodiscard]] ItemRefRankedLess : private ItemRefLess {
public:
	bool operator()(const ItemRefRanked& lhs, const ItemRefRanked& rhs) const noexcept {
		static_assert(rankOrdering != RankOrdering::Off);
		if (lhs.Rank() > rhs.Rank()) {
			return rankOrdering == RankOrdering::Desc;
		} else if (lhs.Rank() < rhs.Rank()) {
			return rankOrdering == RankOrdering::Asc;
		} else {
			return ItemRefLess::operator()(lhs.NotRanked(), rhs.NotRanked());
		}
	}
};

template <typename LockerType>
void RxSelector::preselectSubQueriesMain(ConstQueryImpl q, std::optional<Query>& queryCopy, LockerType& locks, FtFunctionsHolder& func,
										 std::vector<SubQueryExplain>& subQueryExplains, Explain::Duration& preselectTimeTotal,
										 std::vector<LocalQueryResults>& queryResultsHolder, LogLevel logLevel, const RdxContext& ctx) {
	if (HasSubqueries(q)) {
		if (q.DebugLevel() >= LogInfo || logLevel >= LogInfo) {
			logFmt(LogInfo, "Query before subqueries substitution: {}", q.GetSQL());
		}
		if (!queryCopy) {
			queryCopy.emplace(*q);
		}
		const auto preselectStartTime = Explain::Clock::now();
		if (!q.SubQueries().empty()) {
			preselectSubQueries(Impl(*queryCopy), queryResultsHolder, locks, func, subQueryExplains, ctx);
		}
		preselectSubQueriesInJoins(Impl(*queryCopy), queryResultsHolder, locks, func, subQueryExplains, ctx);
		preselectTimeTotal += Explain::Clock::now() - preselectStartTime;
	}
}

RankOrdering GetRankOrdering(QueryRankType type, const SortingEntries& sortingEntries) {
	switch (type) {
		case QueryRankType::No:
			return RankOrdering::Off;
		case QueryRankType::FullText:
		case QueryRankType::KnnIP:
		case QueryRankType::KnnCos:
			return RankOrdering::Desc;
		case QueryRankType::KnnL2:
			return RankOrdering::Asc;
		case QueryRankType::Hybrid:
			return (sortingEntries.empty() || sortingEntries[0].desc) ? RankOrdering::Desc : RankOrdering::Asc;
		case QueryRankType::NotSet:
			break;
	}
	throw_as_assert;
}

template <typename LockerType>
void RxSelector::DoSelect(ConstQueryImpl q, std::optional<Query>& queryCopy, LocalQueryResults& result, LockerType& locks,
						  FtFunctionsHolder& func, const RdxContext& ctx) {
	auto ns = locks.Get(q.NsName());
	std::vector<LocalQueryResults> queryResultsHolder;
	Explain::Duration preselectTimeTotal{0};
	std::vector<SubQueryExplain> subQueryExplains;
	preselectSubQueriesMain(q, queryCopy, locks, func, subQueryExplains, preselectTimeTotal, queryResultsHolder, ns->config_.logLevel, ctx);

	ConstQueryImpl query = queryCopy ? Impl(*queryCopy) : q;
	std::vector<QueryResultsContext> joinQueryResultsContexts;
	bool withJoins = !query.JoinQueries().empty();
	if (!withJoins) {
		for (const Query& mq : query.MergeQueries()) {
			if (!Impl(mq).JoinQueries().empty()) {
				withJoins = true;
				break;
			}
		}
	}

	joins::ItemsProcessors mainJoinItemsProcessors;
	if (withJoins) {
		result.Joined().SetJoinsTable(query);
		if (!query.JoinQueries().empty()) {
			const int nsid = 0;
			const auto preselectStartTime = Explain::Clock::now();
			mainJoinItemsProcessors =
				joins::ItemsProcessor::BuildForQuery(nsid, query, result, locks, func, joinQueryResultsContexts, IsModifyQuery_False, ctx);
			preselectTimeTotal += Explain::Clock::now() - preselectStartTime;
		}
	}
	QueryRankType commonQueryRankType{QueryRankType::NotSet};
	auto commonLimit = query.Limit();
	auto commonOffset = query.Offset();
	auto commonRankOrdering = RankOrdering::Off;
	Explain explain;
	{
		MainSelectCtx selCtx(query, std::nullopt, &result.GetFloatVectorsHolder());
		selCtx.joinItemsProcessors = mainJoinItemsProcessors;
		selCtx.joinPreSelectTimeTotal = preselectTimeTotal;
		selCtx.contextCollectingMode = true;
		selCtx.functions = &func;
		selCtx.explain = &explain;
		selCtx.nsid = 0;
		selCtx.subQueriesExplains = std::move(subQueryExplains);
		if (!query.MergeQueries().empty()) {
			selCtx.isMergeQuery = IsMergeQuery_True;
			for (const auto& a : query.Aggregations()) {
				switch (a.Type()) {
					case AggCount:
					case AggCountCached:
					case AggSum:
					case AggMin:
					case AggMax:
						continue;
					case AggAvg:
					case AggFacet:
					case AggDistinct:
					case AggUnknown:
						throw Error{errNotValid, "Aggregation '{}' in merge query is not implemented yet",
									AggTypeToStr(a.Type())};  // TODO #1506
				}
			}
			if (QueryEntry::kDefaultLimit - commonOffset > commonLimit) {
				commonLimit += commonOffset;
			} else {
				commonLimit = QueryEntry::kDefaultLimit;
			}
			commonOffset = QueryEntry::kDefaultOffset;
		}
		selCtx.requiresCrashTracking = true;
		selCtx.limit = commonLimit;
		selCtx.offset = commonOffset;
		ns->Select(result, selCtx, ctx);
		result.AddNamespace(ns, true);
		commonQueryRankType = selCtx.queryRankType;
	}
	// should be destroyed after results.lockResults()
	std::vector<joins::ItemsProcessors> mergeJoinItemsProcessors;
	if (!query.MergeQueries().empty()) {
		if (commonQueryRankType != QueryRankType::Hybrid && !query.GetSortingEntries().empty()) [[unlikely]] {
			throw Error{errNotValid, "Sorting in merge query is not implemented yet"};	// TODO #1449
		}
		commonRankOrdering = GetRankOrdering(commonQueryRankType, query.GetSortingEntries());
		mergeJoinItemsProcessors.reserve(query.MergeQueries().size());
		uint16_t counter = 0;

		auto hasUnsupportedAggregations = [](const std::vector<AggregateEntry>& aggVector, AggType& t) -> bool {
			for (const auto& a : aggVector) {
				if (a.Type() != AggCount || a.Type() != AggCountCached) {
					t = a.Type();
					return true;
				}
			}
			t = AggUnknown;
			return false;
		};
		AggType errType;
		if ((query.HasLimit() || query.HasOffset()) && hasUnsupportedAggregations(query.Aggregations(), errType)) [[unlikely]] {
			throw Error(errParams, "Limit and offset are not supported for aggregations '{}'", AggTypeToStr(errType));
		}
		for (const JoinedQuery& mq : query.MergeQueries()) {
			ConstQueryImpl mqImpl = Impl(mq);
			if (isSystemNamespaceNameFast(mqImpl.NsName())) [[unlikely]] {
				throw Error(errParams, "Queries to system namespaces ('{}') are not supported inside MERGE statement", mqImpl.NsName());
			}
			if (commonQueryRankType != QueryRankType::Hybrid && !mqImpl.GetSortingEntries().empty()) [[unlikely]] {
				throw Error(errParams, "Sorting in inner merge query is not allowed");
			}
			if (commonRankOrdering != GetRankOrdering(commonQueryRankType, mqImpl.GetSortingEntries())) [[unlikely]] {
				throw Error(errParams, "All merging queries should have the same ordering (ASC or DESC)");
			}
			if (!mqImpl.Aggregations().empty() || mqImpl.HasCalcTotal()) [[unlikely]] {
				throw Error(errParams, "Aggregations in inner merge query are not allowed");
			}
			if (mqImpl.HasLimit() || mqImpl.HasOffset()) [[unlikely]] {
				throw Error(errParams, "Limit and offset in inner merge query is not allowed");
			}
			if (!mqImpl.MergeQueries().empty()) [[unlikely]] {
				throw Error(errParams, "MERGEs nested into the MERGEs are not supported");
			}
			Explain::Duration mergePreselectTimeTotal{0};
			std::optional<JoinedQuery> mQueryCopy;
			std::vector<SubQueryExplain> mSubQueryExplains;
			const bool hasSubqueries{HasSubqueries(mqImpl)};
			if (hasSubqueries) {
				const auto preselectStartTime = Explain::Clock::now();
				mQueryCopy.emplace(mq);
				if (!mqImpl.SubQueries().empty()) {
					preselectSubQueries(Impl(*mQueryCopy), queryResultsHolder, locks, func, mSubQueryExplains, ctx);
				}
				preselectSubQueriesInJoins(Impl(*mQueryCopy), queryResultsHolder, locks, func, mSubQueryExplains, ctx);
				mergePreselectTimeTotal += Explain::Clock::now() - preselectStartTime;
			}
			const JoinedQuery& mQuery{mQueryCopy ? *mQueryCopy : mq};
			ConstQueryImpl mQueryImpl = Impl(mQuery);
			MainSelectCtx mctx(mQueryImpl, query, &result.GetFloatVectorsHolder());
			if (mQueryCopy.has_value()) {
				assertrx_throw(hasSubqueries);
				mctx.subQueriesExplains = std::move(mSubQueryExplains);
			}

			auto mns = locks.Get(mQueryImpl.NsName());
			assertrx_throw(mns);
			mctx.nsid = ++counter;
			if (counter >= std::numeric_limits<uint8_t>::max()) [[unlikely]] {
				throw Error(errForbidden, "Too many namespaces requested in query result: {}", counter);
			}
			mctx.isMergeQuery = IsMergeQuery_True;
			mctx.queryRankType = commonQueryRankType;
			mctx.functions = &func;
			mctx.contextCollectingMode = true;
			mctx.limit = commonLimit;
			mctx.offset = commonOffset;
			mctx.explain = &explain;
			if (withJoins && !mQueryImpl.JoinQueries().empty()) {
				const auto preselectStartTime = Explain::Clock::now();
				auto& mjs = mergeJoinItemsProcessors.emplace_back(joins::ItemsProcessor::BuildForQuery(
					mctx.nsid, mQueryImpl, result, locks, func, joinQueryResultsContexts, IsModifyQuery_False, ctx));
				mctx.joinItemsProcessors = mjs;
				mergePreselectTimeTotal += Explain::Clock::now() - preselectStartTime;
			}
			mctx.joinPreSelectTimeTotal = mergePreselectTimeTotal;
			mctx.requiresCrashTracking = true;
			mns->Select(result, mctx, ctx);
			result.AddNamespace(mns, true);
		}
		const auto mergedSortStart = q.NeedExplain() ? Explain::Clock::now() : Explain::Clock::time_point();
		ItemRefVector& itemRefVec = result.Items();
		if (query.Offset() >= itemRefVec.Size()) {
			result.Erase(itemRefVec.begin(), itemRefVec.end());
		} else {
			switch (commonRankOrdering) {
				case RankOrdering::Off:
					boost::sort::pdqsort(itemRefVec.begin().NotRanked(), itemRefVec.end().NotRanked(), ItemRefLess());
					break;
				case RankOrdering::Asc:
					boost::sort::pdqsort(itemRefVec.begin().Ranked(), itemRefVec.end().Ranked(), ItemRefRankedLess<RankOrdering::Asc>());
					break;
				case RankOrdering::Desc:
					boost::sort::pdqsort(itemRefVec.begin().Ranked(), itemRefVec.end().Ranked(), ItemRefRankedLess<RankOrdering::Desc>());
					break;
			}
			if (query.HasOffset()) {
				result.Erase(itemRefVec.begin(), itemRefVec.begin() + query.Offset());
			}
			if (itemRefVec.Size() > query.Limit()) {
				result.Erase(itemRefVec.begin() + query.Limit(), itemRefVec.end());
			}
			if (q.NeedExplain()) [[unlikely]] {
				explain.AddSortTime(Explain::Clock::now() - mergedSortStart);
			}
		}
	}
	// Adding context to QueryResults
	for (const auto& jctx : joinQueryResultsContexts) {
		result.addNSContext(jctx.type_, jctx.tagsMatcher_, jctx.fieldsFilter_, jctx.schema_, jctx.nsIncarnationTag_);
	}
	if (q.NeedExplain()) [[unlikely]] {
		result.explainResults = explain.GetJSON();
	}
}

void RxSelector::DoPreSelectForUpdateDelete(ConstQueryImpl q, std::optional<Query>& queryCopy, LocalQueryResults& result, NsLockerW& locks,
											FloatVectorsHolderMap* fvHolder, const RdxContext& rdxCtx) {
	FtFunctionsHolder func;
	auto ns = locks.Get(q.NsName());

	std::vector<LocalQueryResults> queryResultsHolder;
	Explain::Duration preselectTimeTotal{0};
	std::vector<SubQueryExplain> subQueryExplains;
	preselectSubQueriesMain(q, queryCopy, locks, func, subQueryExplains, preselectTimeTotal, queryResultsHolder, ns->config_.logLevel,
							rdxCtx);

	ConstQueryImpl query = queryCopy.has_value() ? Impl(*queryCopy) : q;
	joins::ItemsProcessors mainJoinItemsProcessors;
	if (!q.JoinQueries().empty()) {
		const int nsid = 0;
		const auto preselectStartTime = Explain::Clock::now();
		std::vector<QueryResultsContext> joinQueryResultsContexts;
		result.Joined().SetJoinsTable(query);
		mainJoinItemsProcessors =
			joins::ItemsProcessor::BuildForQuery(nsid, query, result, locks, func, joinQueryResultsContexts, IsModifyQuery_True, rdxCtx);
		preselectTimeTotal += Explain::Clock::now() - preselectStartTime;
	}

	Explain explain;
	MainSelectCtx selCtx(query, std::nullopt, fvHolder);
	selCtx.joinItemsProcessors = mainJoinItemsProcessors;
	selCtx.joinPreSelectTimeTotal = preselectTimeTotal;
	selCtx.contextCollectingMode = true;
	selCtx.functions = &func;
	selCtx.nsid = 0;
	selCtx.requiresCrashTracking = true;
	selCtx.explain = &explain;

	NsSelecter selecter(ns.get());
	selecter(result, selCtx, rdxCtx);
	result.AddNamespace(ns, true);

	if (q.NeedExplain()) [[unlikely]] {
		// TODO: Add update/delete explain some day. Issue #2399
		result.explainResults = explain.GetJSON();
	}
}

template <typename LockerT>
bool RxSelector::selectSubQuery(ConstQueryImpl subQuery, ConstQueryImpl mainQuery, LockerT& locks, FtFunctionsHolder& func,
								std::vector<SubQueryExplain>& explainsOut, const RdxContext& rdxCtx) {
	auto ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	Explain explain;
	LocalQueryResults result;
	MainSelectCtx sctx{subQuery, mainQuery, &result.GetFloatVectorsHolder()};
	sctx.nsid = 0;
	sctx.requiresCrashTracking = true;
	sctx.reqMatchedOnceFlag = true;
	sctx.contextCollectingMode = true;
	sctx.functions = &func;
	sctx.explain = &explain;

	ns->Select(result, sctx, rdxCtx);
	locks.Delete(ns);
	if (subQuery.NeedExplain()) {
		explainsOut.emplace_back(subQuery.NsName(), explain.GetJSON());
	}
	return sctx.matchedAtLeastOnce;
}

template <typename LockerT>
VariantArray RxSelector::selectSubQuery(ConstQueryImpl subQuery, ConstQueryImpl mainQuery, LockerT& locks, LocalQueryResults& qr,
										FtFunctionsHolder& func, std::variant<std::string, size_t> fieldOrKeys,
										std::vector<SubQueryExplain>& explainsOut, const RdxContext& rdxCtx) {
	NamespaceImpl::Ptr ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	Explain explain;
	MainSelectCtx sctx{subQuery, mainQuery, &qr.GetFloatVectorsHolder()};
	sctx.nsid = 0;
	sctx.requiresCrashTracking = true;
	sctx.contextCollectingMode = true;
	sctx.functions = &func;
	sctx.explain = &explain;

	ns->Select(qr, sctx, rdxCtx);
	VariantArray result, buf;
	if (qr.GetAggregationResults().empty()) {
		assertrx_throw(!subQuery.SelectFilters().Fields().empty());
		const std::string_view field = subQuery.SelectFilters().Fields()[0];
		result.reserve(qr.Count());
		if (int idxNo = -1; ns->tryGetIndexByNameOrJsonPath(field, idxNo) && !ns->indexes()[idxNo]->Opts().IsSparse()) {
			if (idxNo < ns->indexes().firstCompositePos()) {
				for (const auto& it : qr) {
					if (!it.Status().ok()) {
						throw it.Status();
					}
					ConstPayload{ns->payloadType(), ns->items_[it.GetItemRef().Id()]}.Get(idxNo, buf);
					for (Variant& v : buf) {
						result.emplace_back(std::move(v));
					}
				}
			} else {
				const auto fields = ns->indexes()[idxNo]->Fields();
				QueryField::CompositeTypesVecT fieldsTypes;
#ifndef NDEBUG
				const bool ftIdx = IsFullText(ns->indexes()[idxNo]->Type());
#endif
				for (const auto f : ns->indexes()[idxNo]->Fields()) {
					if (f == IndexValueType::SetByJsonPath) {
						// not indexed fields allowed only in ft composite indexes
						assertrx_throw(ftIdx);
						fieldsTypes.emplace_back(KeyValueType::String{});
					} else {
						assertrx_throw(f <= ns->indexes().firstCompositePos());
						fieldsTypes.emplace_back(ns->indexes()[f]->SelectKeyType());
					}
				}
				for (const auto& it : qr) {
					if (!it.Status().ok()) {
						throw it.Status();
					}
					result.emplace_back(
						ConstPayload{ns->payloadType(), ns->items_[it.GetItemRef().Id()]}.GetComposite(fields, fieldsTypes));
				}
			}
		} else {
			if (idxNo < 0) {
				switch (mainQuery.GetStrictMode()) {
					case StrictModeIndexes:
						throw Error(errStrictMode,
									"Current query strict mode allows aggregate index fields only. There are no indexes with name '{}' in "
									"namespace '{}'",
									field, subQuery.NsName());
					case StrictModeNames:
						if (ns->tagsMatcher().path2tag(field).empty()) {
							throw Error(errStrictMode,
										"Current query strict mode allows aggregate existing fields only. There are no fields with name "
										"'{}' in namespace '{}'",
										field, subQuery.NsName());
						}
						break;
					case StrictModeNone:
					case StrictModeNotSet:
						break;
				}
			}
			for (const auto& it : qr) {
				if (!it.Status().ok()) {
					throw it.Status();
				}
				ConstPayload{ns->payloadType(), ns->items_[it.GetItemRef().Id()]}.GetByJsonPath(field, ns->tagsMatcher(), buf,
																								KeyValueType::Undefined{});
				for (Variant& v : buf) {
					result.emplace_back(std::move(v));
				}
			}
		}
	} else {
		const auto v = qr.GetAggregationResults()[0].GetValue();
		if (v.has_value()) {
			result.emplace_back(*v);
		}
	}
	locks.Delete(ns);
	if (subQuery.NeedExplain()) {
		explainsOut.emplace_back(subQuery.NsName(), explain.GetJSON());
		explainsOut.back().SetFieldOrKeys(std::move(fieldOrKeys));
	}
	return result;
}

template <typename LockerT>
void RxSelector::preselectSubQueries(QueryImpl mainQuery, std::vector<LocalQueryResults>& queryResultsHolder, LockerT& locks,
									 FtFunctionsHolder& func, std::vector<SubQueryExplain>& explains, const RdxContext& ctx) {
	if (mainQuery.NeedExplain() || mainQuery.DebugLevel() >= LogInfo) {
		explains.reserve(explains.size() + mainQuery.SubQueries().size());
	}
	for (size_t i = 0; i < mainQuery.Entries().Size();) {
		[[maybe_unused]] const size_t cur = i;
		mainQuery.Entries().Visit(
			i, overloaded{[&i](const concepts::OneOf<QueryEntriesBracket, QueryEntry, BetweenFieldsQueryEntry, JoinQueryEntry, AlwaysTrue,
													 AlwaysFalse, KnnQueryEntry, MultiDistinctQueryEntry, QueryFunctionEntry,
													 QueryArithmeticEntry> auto&) noexcept { ++i; },
						  [&](const SubQueryEntry& sqe) {
							  try {
								  const CondType cond = sqe.Condition();
								  if (cond == CondAny || cond == CondEmpty) {
									  if (selectSubQuery(Impl(mainQuery.SubQueries()[sqe.QueryIndex()]), mainQuery, locks, func, explains,
														 ctx) == (cond == CondAny)) {
										  i += mainQuery.ReplaceQueryEntry<AlwaysTrue>(i);
										  return;
									  }
									  i += mainQuery.ReplaceQueryEntry<AlwaysFalse>(i);
									  return;
								  }
								  LocalQueryResults qr;
								  const auto values = selectSubQuery(Impl(mainQuery.SubQueries()[sqe.QueryIndex()]), mainQuery, locks, qr,
																	 func, sqe.Values().size(), explains, ctx);
								  if (QueryEntries::CheckIfSatisfyCondition(values, sqe.Condition(), sqe.Values())) {
									  i += mainQuery.ReplaceQueryEntry<AlwaysTrue>(i);
									  return;
								  }
								  i += mainQuery.ReplaceQueryEntry<AlwaysFalse>(i);
							  } catch (const Error& err) {
								  throw Error(err.code(), "Error during preprocessing of subquery '" +
															  mainQuery.SubQueries()[sqe.QueryIndex()].GetSQL() + "': " + err.what());
							  }
						  },
						  [&](const SubQueryFieldEntry& sqe) {
							  try {
								  queryResultsHolder.resize(queryResultsHolder.size() + 1);
								  i += mainQuery.ReplaceQueryEntry<QueryEntry>(
									  i, sqe.FieldName(), sqe.Condition(),
									  selectSubQuery(Impl(mainQuery.SubQueries()[sqe.QueryIndex()]), mainQuery, locks,
													 queryResultsHolder.back(), func, sqe.FieldName(), explains, ctx));
							  } catch (const Error& err) {
								  throw Error(err.code(), "Error during preprocessing of subquery '" +
															  mainQuery.SubQueries()[sqe.QueryIndex()].GetSQL() + "': " + err.what());
							  }
						  },
						  [&](const SubQueryFunctionEntry& sqe) {
							  try {
								  queryResultsHolder.resize(queryResultsHolder.size() + 1);
								  i += mainQuery.ReplaceQueryEntry<QueryFunctionEntry>(
									  i, sqe.FunctionVariant(), sqe.Condition(),
									  selectSubQuery(Impl(mainQuery.SubQueries()[sqe.QueryIndex()]), mainQuery, locks,
													 queryResultsHolder.back(), func, sqe.Function().ToString(), explains, ctx));
							  } catch (const Error& err) {
								  throw Error(err.code(), "Error during preprocessing of subquery '" +
															  mainQuery.SubQueries()[sqe.QueryIndex()].GetSQL() + "': " + err.what());
							  }
						  }});
		assertrx_dbg(i > cur);
	}
}

template <typename LockerType>
void RxSelector::preselectSubQueriesInJoins(QueryImpl q, std::vector<LocalQueryResults>& queryResultsHolder, LockerType& locks,
											FtFunctionsHolder& func, std::vector<SubQueryExplain>& subQueryExplains,
											const RdxContext& ctx) {
	for (JoinedQuery& jq : q.GetJoinQueriesSpan()) {
		auto jqImpl = Impl(jq);
		if (!jqImpl.SubQueries().empty()) {
			preselectSubQueries(jqImpl, queryResultsHolder, locks, func, subQueryExplains, ctx);
		}
		preselectSubQueriesInJoins(jqImpl, queryResultsHolder, locks, func, subQueryExplains, ctx);
	}
}

template void RxSelector::DoSelect<RxSelector::NsLocker<const RdxContext>>(ConstQueryImpl, std::optional<Query>&, LocalQueryResults&,
																		   NsLocker<const RdxContext>&, FtFunctionsHolder&,
																		   const RdxContext&);

}  // namespace reindexer
