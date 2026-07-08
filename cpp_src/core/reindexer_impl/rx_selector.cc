#include "rx_selector.h"
#include "core/nsselecter/joins/queryresults.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/queryresults/context.h"
#include "core/queryresults/queryresults.h"
#include "tools/logger.h"

namespace reindexer {

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
void RxSelector::preselectSubQuriesMain(const Query& q, std::optional<Query>& queryCopy, LockerType& locks, FtFunctionsHolder& func,
										std::vector<SubQueryExplain>& subQueryExplains, Explain::Duration& preselectTimeTotal,
										std::vector<LocalQueryResults>& queryResultsHolder, LogLevel logLevel, const RdxContext& ctx) {
	if (!q.GetSubQueries().empty()) {
		if (q.GetDebugLevel() >= LogInfo || logLevel >= LogInfo) {
			logFmt(LogInfo, "Query before subqueries substitution: {}", q.GetSQL());
		}
		if (!queryCopy) {
			queryCopy.emplace(q);
		}
		const auto preselectStartTime = Explain::Clock::now();
		subQueryExplains = preselectSubQueries(*queryCopy, queryResultsHolder, locks, func, ctx);
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
void RxSelector::DoSelect(const Query& q, std::optional<Query>& queryCopy, LocalQueryResults& result, LockerType& locks,
						  FtFunctionsHolder& func, const RdxContext& ctx) {
	auto ns = locks.Get(q.NsName());
	std::vector<LocalQueryResults> queryResultsHolder;
	Explain::Duration preselectTimeTotal{0};
	std::vector<SubQueryExplain> subQueryExplains;
	preselectSubQuriesMain(q, queryCopy, locks, func, subQueryExplains, preselectTimeTotal, queryResultsHolder, ns->config_.logLevel, ctx);

	const Query& query = queryCopy ? *queryCopy : q;
	std::vector<QueryResultsContext> joinQueryResultsContexts;
	bool thereAreJoins = !query.GetJoinQueries().empty();
	if (!thereAreJoins) {
		for (const Query& mq : query.GetMergeQueries()) {
			if (!mq.GetJoinQueries().empty()) {
				thereAreJoins = true;
				break;
			}
		}
	}

	joins::ItemsProcessors mainJoinItemsProcessors;
	if (thereAreJoins) {
		const auto preselectStartTime = Explain::Clock::now();
		mainJoinItemsProcessors =
			joins::ItemsProcessor::BuildForQuery(query, result, locks, func, &joinQueryResultsContexts, IsModifyQuery_False, ctx);
		result.joined_.resize(1 + query.GetMergeQueries().size());
		result.joined_[0].SetItemsProcessorsCount(mainJoinItemsProcessors.size());
		preselectTimeTotal += Explain::Clock::now() - preselectStartTime;
	}
	QueryRankType commonQueryRankType{QueryRankType::NotSet};
	auto commonLimit = query.Limit();
	auto commonOffset = query.Offset();
	auto commonRankOrdering = RankOrdering::Off;
	Explain explain;
	{
		MainSelectCtx selCtx(query, nullptr, &result.GetFloatVectorsHolder());
		selCtx.joinItemsProcessors = mainJoinItemsProcessors.size() ? &mainJoinItemsProcessors : nullptr;
		selCtx.joinPreSelectTimeTotal = preselectTimeTotal;
		selCtx.contextCollectingMode = true;
		selCtx.functions = &func;
		selCtx.explain = &explain;
		selCtx.nsid = 0;
		selCtx.subQueriesExplains = std::move(subQueryExplains);
		if (!query.GetMergeQueries().empty()) {
			selCtx.isMergeQuery = IsMergeQuery_True;
			for (const auto& a : query.aggregations_) {
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
	if (!query.GetMergeQueries().empty()) {
		if (commonQueryRankType != QueryRankType::Hybrid && !query.GetSortingEntries().empty()) [[unlikely]] {
			throw Error{errNotValid, "Sorting in merge query is not implemented yet"};	// TODO #1449
		}
		commonRankOrdering = GetRankOrdering(commonQueryRankType, query.GetSortingEntries());
		mergeJoinItemsProcessors.reserve(query.GetMergeQueries().size());
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
		if ((query.HasLimit() || query.HasOffset()) && hasUnsupportedAggregations(query.aggregations_, errType)) [[unlikely]] {
			throw Error(errParams, "Limit and offset are not supported for aggregations '{}'", AggTypeToStr(errType));
		}
		for (const JoinedQuery& mq : query.GetMergeQueries()) {
			if (isSystemNamespaceNameFast(mq.NsName())) [[unlikely]] {
				throw Error(errParams, "Queries to system namespaces ('{}') are not supported inside MERGE statement", mq.NsName());
			}
			if (commonQueryRankType != QueryRankType::Hybrid && !mq.GetSortingEntries().empty()) [[unlikely]] {
				throw Error(errParams, "Sorting in inner merge query is not allowed");
			}
			if (commonRankOrdering != GetRankOrdering(commonQueryRankType, mq.GetSortingEntries())) [[unlikely]] {
				throw Error(errParams, "All merging queries should have the same ordering (ASC or DESC)");
			}
			if (!mq.aggregations_.empty() || mq.HasCalcTotal()) [[unlikely]] {
				throw Error(errParams, "Aggregations in inner merge query are not allowed");
			}
			if (mq.HasLimit() || mq.HasOffset()) [[unlikely]] {
				throw Error(errParams, "Limit and offset in inner merge query is not allowed");
			}
			if (!mq.GetMergeQueries().empty()) [[unlikely]] {
				throw Error(errParams, "MERGEs nested into the MERGEs are not supported");
			}
			std::optional<JoinedQuery> mQueryCopy;
			if (!mq.GetSubQueries().empty()) {
				mQueryCopy.emplace(mq);
			}
			const JoinedQuery& mQuery = mQueryCopy ? *mQueryCopy : mq;
			MainSelectCtx mctx(mQuery, &query, &result.GetFloatVectorsHolder());
			if (mQueryCopy.has_value()) {
				assertrx_throw(!mq.GetSubQueries().empty());
				mctx.subQueriesExplains = preselectSubQueries(*mQueryCopy, queryResultsHolder, locks, func, ctx);
			}

			auto mns = locks.Get(mQuery.NsName());
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
			if (thereAreJoins) {
				auto& mjs = mergeJoinItemsProcessors.emplace_back(
					joins::ItemsProcessor::BuildForQuery(mQuery, result, locks, func, &joinQueryResultsContexts, IsModifyQuery_False, ctx));
				mctx.joinItemsProcessors = mjs.size() ? &mjs : nullptr;
				result.joined_[mctx.nsid].SetItemsProcessorsCount(mjs.size());
			}
			mctx.requiresCrashTracking = true;
			mns->Select(result, mctx, ctx);
			result.AddNamespace(mns, true);
		}
		const auto mergedSortStart = q.NeedExplain() ? Explain::Clock::now() : Explain::Clock::time_point();
		ItemRefVector& itemRefVec = result.Items();
		if (query.Offset() >= itemRefVec.Size()) {
			result.Erase(itemRefVec.begin(), itemRefVec.end());
			return;
		}
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
	// Adding context to QueryResults
	for (const auto& jctx : joinQueryResultsContexts) {
		result.addNSContext(jctx.type_, jctx.tagsMatcher_, jctx.fieldsFilter_, jctx.schema_, jctx.nsIncarnationTag_);
	}
	if (q.NeedExplain()) [[unlikely]] {
		result.explainResults = explain.GetJSON();
	}
}

void RxSelector::DoPreSelectForUpdateDelete(const Query& q, std::optional<Query>& queryCopy, LocalQueryResults& result, NsLockerW& locks,
											FloatVectorsHolderMap* fvHolder, const RdxContext& rdxCtx) {
	FtFunctionsHolder func;
	auto ns = locks.Get(q.NsName());

	std::vector<LocalQueryResults> queryResultsHolder;
	Explain::Duration preselectTimeTotal{0};
	std::vector<SubQueryExplain> subQueryExplains;
	preselectSubQuriesMain(q, queryCopy, locks, func, subQueryExplains, preselectTimeTotal, queryResultsHolder, ns->config_.logLevel,
						   rdxCtx);

	joins::ItemsProcessors mainJoinItemsProcessors;
	if (!q.GetJoinQueries().empty()) {
		const auto preselectStartTime = Explain::Clock::now();
		const Query& query = queryCopy.has_value() ? *queryCopy : q;
		// Do not add contexts into QueryResults: joins in update/delete queries do not send actual data and contexts do not required
		std::vector<QueryResultsContext>* joinQueryResultsContexts = nullptr;
		mainJoinItemsProcessors =
			joins::ItemsProcessor::BuildForQuery(query, result, locks, func, joinQueryResultsContexts, IsModifyQuery_True, rdxCtx);
		result.joined_.resize(1);
		result.joined_[0].SetItemsProcessorsCount(mainJoinItemsProcessors.size());
		preselectTimeTotal += Explain::Clock::now() - preselectStartTime;
	}

	Explain explain;
	const Query& query = queryCopy.has_value() ? *queryCopy : q;
	MainSelectCtx selCtx(query, nullptr, fvHolder);
	selCtx.joinItemsProcessors = mainJoinItemsProcessors.size() ? &mainJoinItemsProcessors : nullptr;
	selCtx.joinPreSelectTimeTotal = preselectTimeTotal;
	selCtx.contextCollectingMode = true;
	selCtx.functions = &func;
	selCtx.nsid = 0;
	selCtx.requiresCrashTracking = true;
	selCtx.selectBeforeUpdate = true;
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
bool RxSelector::selectSubQuery(const Query& subQuery, const Query& mainQuery, LockerT& locks, FtFunctionsHolder& func,
								std::vector<SubQueryExplain>& explainsOut, const RdxContext& rdxCtx) {
	auto ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	Explain explain;
	LocalQueryResults result;
	MainSelectCtx sctx{subQuery, &mainQuery, &result.GetFloatVectorsHolder()};
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
VariantArray RxSelector::selectSubQuery(const Query& subQuery, const Query& mainQuery, LockerT& locks, LocalQueryResults& qr,
										FtFunctionsHolder& func, std::variant<std::string, size_t> fieldOrKeys,
										std::vector<SubQueryExplain>& explainsOut, const RdxContext& rdxCtx) {
	NamespaceImpl::Ptr ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	Explain explain;
	MainSelectCtx sctx{subQuery, &mainQuery, &qr.GetFloatVectorsHolder()};
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
		if (int idxNo = -1; ns->tryGetIndexByNameOrJsonPath(field, idxNo) && !ns->indexes_[idxNo]->Opts().IsSparse()) {
			if (idxNo < ns->indexes_.firstCompositePos()) {
				for (const auto& it : qr) {
					if (!it.Status().ok()) {
						throw it.Status();
					}
					ConstPayload{ns->payloadType_, ns->items_[it.GetItemRef().Id()]}.Get(idxNo, buf);
					for (Variant& v : buf) {
						result.emplace_back(std::move(v));
					}
				}
			} else {
				const auto fields = ns->indexes_[idxNo]->Fields();
				QueryField::CompositeTypesVecT fieldsTypes;
#ifndef NDEBUG
				const bool ftIdx = IsFullText(ns->indexes_[idxNo]->Type());
#endif
				for (const auto f : ns->indexes_[idxNo]->Fields()) {
					if (f == IndexValueType::SetByJsonPath) {
						// not indexed fields allowed only in ft composite indexes
						assertrx_throw(ftIdx);
						fieldsTypes.emplace_back(KeyValueType::String{});
					} else {
						assertrx_throw(f <= ns->indexes_.firstCompositePos());
						fieldsTypes.emplace_back(ns->indexes_[f]->SelectKeyType());
					}
				}
				for (const auto& it : qr) {
					if (!it.Status().ok()) {
						throw it.Status();
					}
					result.emplace_back(ConstPayload{ns->payloadType_, ns->items_[it.GetItemRef().Id()]}.GetComposite(fields, fieldsTypes));
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
						if (ns->tagsMatcher_.path2tag(field).empty()) {
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
				ConstPayload{ns->payloadType_, ns->items_[it.GetItemRef().Id()]}.GetByJsonPath(field, ns->tagsMatcher_, buf,
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
std::vector<SubQueryExplain> RxSelector::preselectSubQueries(Query& mainQuery, std::vector<LocalQueryResults>& queryResultsHolder,
															 LockerT& locks, FtFunctionsHolder& func, const RdxContext& ctx) {
	std::vector<SubQueryExplain> explains;
	if (mainQuery.NeedExplain() || mainQuery.GetDebugLevel() >= LogInfo) {
		explains.reserve(mainQuery.GetSubQueries().size());
	}
	for (size_t i = 0; i < mainQuery.Entries().Size();) {
		[[maybe_unused]] const size_t cur = i;
		mainQuery.Entries().Visit(
			i,
			overloaded{[&i](const concepts::OneOf<QueryEntriesBracket, QueryEntry, BetweenFieldsQueryEntry, JoinQueryEntry, AlwaysTrue,
												  AlwaysFalse, KnnQueryEntry, MultiDistinctQueryEntry, QueryFunctionEntry> auto&) noexcept {
						   ++i;
					   },
					   [&](const SubQueryEntry& sqe) {
						   try {
							   const CondType cond = sqe.Condition();
							   if (cond == CondAny || cond == CondEmpty) {
								   if (selectSubQuery(mainQuery.GetSubQuery(sqe.QueryIndex()), mainQuery, locks, func, explains, ctx) ==
									   (cond == CondAny)) {
									   i += mainQuery.SetEntry<AlwaysTrue>(i);
									   return;
								   }
								   i += mainQuery.SetEntry<AlwaysFalse>(i);
								   return;
							   }
							   LocalQueryResults qr;
							   const auto values = selectSubQuery(mainQuery.GetSubQuery(sqe.QueryIndex()), mainQuery, locks, qr, func,
																  sqe.Values().size(), explains, ctx);
							   if (QueryEntries::CheckIfSatisfyCondition(values, sqe.Condition(), sqe.Values())) {
								   i += mainQuery.SetEntry<AlwaysTrue>(i);
								   return;
							   }
							   i += mainQuery.SetEntry<AlwaysFalse>(i);
						   } catch (const Error& err) {
							   throw Error(err.code(), "Error during preprocessing of subquery '" +
														   mainQuery.GetSubQuery(sqe.QueryIndex()).GetSQL() + "': " + err.what());
						   }
					   },
					   [&](const SubQueryFieldEntry& sqe) {
						   try {
							   queryResultsHolder.resize(queryResultsHolder.size() + 1);
							   i += mainQuery.SetEntry<QueryEntry>(
								   i, sqe.FieldName(), sqe.Condition(),
								   selectSubQuery(mainQuery.GetSubQuery(sqe.QueryIndex()), mainQuery, locks, queryResultsHolder.back(),
												  func, sqe.FieldName(), explains, ctx));
						   } catch (const Error& err) {
							   throw Error(err.code(), "Error during preprocessing of subquery '" +
														   mainQuery.GetSubQuery(sqe.QueryIndex()).GetSQL() + "': " + err.what());
						   }
					   },
					   [&](const SubQueryFunctionEntry& sqe) {
						   try {
							   queryResultsHolder.resize(queryResultsHolder.size() + 1);
							   i += mainQuery.SetEntry<QueryFunctionEntry>(
								   i, sqe.FunctionVariant(), sqe.Condition(),
								   selectSubQuery(mainQuery.GetSubQuery(sqe.QueryIndex()), mainQuery, locks, queryResultsHolder.back(),
												  func, sqe.Function().ToString(), explains, ctx));
						   } catch (const Error& err) {
							   throw Error(err.code(), "Error during preprocessing of subquery '" +
														   mainQuery.GetSubQuery(sqe.QueryIndex()).GetSQL() + "': " + err.what());
						   }
					   }});
		assertrx_dbg(i > cur);
	}
	return explains;
}

template void RxSelector::DoSelect<RxSelector::NsLocker<const RdxContext>>(const Query&, std::optional<Query>&, LocalQueryResults&,
																		   NsLocker<const RdxContext>&, FtFunctionsHolder&,
																		   const RdxContext&);

}  // namespace reindexer
