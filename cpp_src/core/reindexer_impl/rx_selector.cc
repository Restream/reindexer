#include "rx_selector.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/queryresults/joinresults.h"
#include "estl/charset.h"
#include "estl/restricted.h"
#include "tools/logger.h"

namespace reindexer {

struct ItemRefLess {
	bool operator()(const ItemRef& lhs, const ItemRef& rhs) const noexcept {
		if (lhs.Proc() == rhs.Proc()) {
			if (lhs.Nsid() == rhs.Nsid()) {
				return lhs.Id() < rhs.Id();
			}
			return lhs.Nsid() < rhs.Nsid();
		}
		return lhs.Proc() > rhs.Proc();
	}
};

struct RxSelector::QueryResultsContext {
	QueryResultsContext() = default;
	QueryResultsContext(PayloadType type, TagsMatcher tagsMatcher, const FieldsSet& fieldsFilter, std::shared_ptr<const Schema> schema)
		: type_(std::move(type)), tagsMatcher_(std::move(tagsMatcher)), fieldsFilter_(fieldsFilter), schema_(std::move(schema)) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsSet fieldsFilter_;
	std::shared_ptr<const Schema> schema_;
};

template <typename T, typename QueryType>
void RxSelector::DoSelect(const Query& q, QueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func, const RdxContext& ctx,
						  QueryStatCalculator<QueryType>& queryStatCalculator) {
	auto ns = locks.Get(q.NsName());
	if rx_unlikely (!ns) {
		throw Error(errParams, "Namespace '%s' does not exist", q.NsName());
	}
	std::vector<QueryResults> queryResultsHolder;
	std::optional<Query> queryCopy;
	ExplainCalc::Duration preselectTimeTotal{0};
	std::vector<SubQueryExplain> subQueryExplains;
	if (!q.GetSubQueries().empty()) {
		if (q.GetDebugLevel() >= LogInfo || ns->config_.logLevel >= LogInfo) {
			logPrintf(LogInfo, "Query before subqueries substitution: %s", q.GetSQL());
		}
		queryCopy.emplace(q);
		const auto preselectStartTime = ExplainCalc::Clock::now();
		subQueryExplains = preselectSubQueries(*queryCopy, queryResultsHolder, locks, func, ctx);
		preselectTimeTotal += ExplainCalc::Clock::now() - preselectStartTime;
	}
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

	JoinedSelectors mainJoinedSelectors;
	if (thereAreJoins) {
		const auto preselectStartTime = ExplainCalc::Clock::now();
		mainJoinedSelectors = prepareJoinedSelectors(query, result, locks, func, joinQueryResultsContexts, ctx);
		result.joined_.resize(1 + query.GetMergeQueries().size());
		result.joined_[0].SetJoinedSelectorsCount(mainJoinedSelectors.size());
		preselectTimeTotal += ExplainCalc::Clock::now() - preselectStartTime;
	}
	IsFTQuery isFtQuery{IsFTQuery::NotSet};
	{
		SelectCtxWithJoinPreSelect selCtx(query, nullptr);
		selCtx.joinedSelectors = mainJoinedSelectors.size() ? &mainJoinedSelectors : nullptr;
		selCtx.preResultTimeTotal = preselectTimeTotal;
		selCtx.contextCollectingMode = true;
		selCtx.functions = &func;
		selCtx.nsid = 0;
		selCtx.subQueriesExplains = std::move(subQueryExplains);
		if (!query.GetMergeQueries().empty()) {
			selCtx.isMergeQuery = IsMergeQuery::Yes;
			if rx_unlikely (!query.sortingEntries_.empty()) {
				throw Error{errNotValid, "Sorting in merge query is not implemented yet"};	// TODO #1449
			}
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
						throw Error{errNotValid, "Aggregation '%s' in merge query is not implemented yet",
									AggTypeToStr(a.Type())};  // TODO #1506
				}
			}
		}
		selCtx.requiresCrashTracking = true;
		ns->Select(result, selCtx, ctx);
		result.AddNamespace(ns, true);
		isFtQuery = selCtx.isFtQuery;
		if (selCtx.explain.IsEnabled()) {
			queryStatCalculator.AddExplain(selCtx.explain);
		}
	}
	// should be destroyed after results.lockResults()
	std::vector<JoinedSelectors> mergeJoinedSelectors;
	if (!query.GetMergeQueries().empty()) {
		mergeJoinedSelectors.reserve(query.GetMergeQueries().size());
		uint8_t counter = 0;

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
		if (rx_unlikely((query.HasLimit() || query.HasOffset()) && hasUnsupportedAggregations(query.aggregations_, errType))) {
			throw Error(errParams, "Limit and offset are not supported for aggregations '%s'", AggTypeToStr(errType));
		}
		for (const JoinedQuery& mq : query.GetMergeQueries()) {
			if rx_unlikely (isSystemNamespaceNameFast(mq.NsName())) {
				throw Error(errParams, "Queries to system namespaces ('%s') are not supported inside MERGE statement", mq.NsName());
			}
			if rx_unlikely (!mq.sortingEntries_.empty()) {
				throw Error(errParams, "Sorting in inner merge query is not allowed");
			}
			if rx_unlikely (!mq.aggregations_.empty() || mq.HasCalcTotal()) {
				throw Error(errParams, "Aggregations in inner merge query are not allowed");
			}
			if rx_unlikely (mq.HasLimit() || mq.HasOffset()) {
				throw Error(errParams, "Limit and offset in inner merge query is not allowed");
			}
			if rx_unlikely (!mq.GetMergeQueries().empty()) {
				throw Error(errParams, "MERGEs nested into the MERGEs are not supported");
			}
			std::optional<JoinedQuery> mQueryCopy;
			if (!mq.GetSubQueries().empty()) {
				mQueryCopy.emplace(mq);
			}
			const JoinedQuery& mQuery = mQueryCopy ? *mQueryCopy : mq;
			SelectCtxWithJoinPreSelect mctx(mQuery, &query);
			if (!mq.GetSubQueries().empty()) {
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				mctx.subQueriesExplains = preselectSubQueries(*mQueryCopy, queryResultsHolder, locks, func, ctx);
			}

			auto mns = locks.Get(mQuery.NsName());
			assertrx_throw(mns);
			mctx.nsid = ++counter;
			mctx.isMergeQuery = IsMergeQuery::Yes;
			mctx.isFtQuery = isFtQuery;
			mctx.functions = &func;
			mctx.contextCollectingMode = true;
			if (thereAreJoins) {
				auto& mjs =
					mergeJoinedSelectors.emplace_back(prepareJoinedSelectors(mQuery, result, locks, func, joinQueryResultsContexts, ctx));
				mctx.joinedSelectors = mjs.size() ? &mjs : nullptr;
				result.joined_[mctx.nsid].SetJoinedSelectorsCount(mjs.size());
			}
			mctx.requiresCrashTracking = true;
			mns->Select(result, mctx, ctx);
			result.AddNamespace(mns, true);
		}
		ItemRefVector& itemRefVec = result.Items();
		if (query.Offset() >= itemRefVec.size()) {
			result.Erase(itemRefVec.begin(), itemRefVec.end());
			return;
		}
		boost::sort::pdqsort(itemRefVec.begin(), itemRefVec.end(), ItemRefLess());
		if (query.HasOffset()) {
			result.Erase(itemRefVec.begin(), itemRefVec.begin() + query.Offset());
		}
		if (itemRefVec.size() > query.Limit()) {
			result.Erase(itemRefVec.begin() + query.Limit(), itemRefVec.end());
		}
	}
	// Adding context to QueryResults
	for (const auto& jctx : joinQueryResultsContexts) {
		result.addNSContext(jctx.type_, jctx.tagsMatcher_, jctx.fieldsFilter_, jctx.schema_);
	}
}

[[nodiscard]] static bool byJoinedField(std::string_view sortExpr, std::string_view joinedNs) {
	constexpr static estl::Charset kJoinedIndexNameSyms{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
														'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
														'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
														'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', '+'};
	std::string_view::size_type i = 0;
	const auto s = sortExpr.size();
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	bool inQuotes = false;
	if (i < s && sortExpr[i] == '"') {
		++i;
		inQuotes = true;
	}
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	std::string_view::size_type j = 0, s2 = joinedNs.size();
	for (; j < s2 && i < s; ++i, ++j) {
		if (sortExpr[i] != joinedNs[j]) {
			return false;
		}
	}
	if (i >= s || sortExpr[i] != '.') {
		return false;
	}
	for (++i; i < s; ++i) {
		if (!kJoinedIndexNameSyms.test(sortExpr[i])) {
			if (isspace(sortExpr[i])) {
				break;
			}
			if (inQuotes && sortExpr[i] == '"') {
				inQuotes = false;
				++i;
				break;
			}
			return false;
		}
	}
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	if (inQuotes && i < s && sortExpr[i] == '"') {
		++i;
	}
	while (i < s && isspace(sortExpr[i])) {
		++i;
	}
	return i == s;
}

StoredValuesOptimizationStatus RxSelector::isPreResultValuesModeOptimizationAvailable(const Query& jItemQ, const NamespaceImpl::Ptr& jns,
																					  const Query& mainQ) {
	auto status = StoredValuesOptimizationStatus::Enabled;
	jItemQ.Entries().VisitForEach([](const SubQueryEntry&) { assertrx_throw(0); }, [](const SubQueryFieldEntry&) { assertrx_throw(0); },
								  Skip<JoinQueryEntry, QueryEntriesBracket, AlwaysFalse, AlwaysTrue>{},
								  [&jns, &status](const QueryEntry& qe) {
									  if (qe.IsFieldIndexed()) {
										  assertrx_throw(jns->indexes_.size() > static_cast<size_t>(qe.IndexNo()));
										  const IndexType indexType = jns->indexes_[qe.IndexNo()]->Type();
										  if (IsComposite(indexType)) {
											  status = StoredValuesOptimizationStatus::DisabledByCompositeIndex;
										  } else if (IsFullText(indexType)) {
											  status = StoredValuesOptimizationStatus::DisabledByFullTextIndex;
										  }
									  }
								  },
								  [&jns, &status](const BetweenFieldsQueryEntry& qe) {
									  if (qe.IsLeftFieldIndexed()) {
										  assertrx_throw(jns->indexes_.size() > static_cast<size_t>(qe.LeftIdxNo()));
										  const IndexType indexType = jns->indexes_[qe.LeftIdxNo()]->Type();
										  if (IsComposite(indexType)) {
											  status = StoredValuesOptimizationStatus::DisabledByCompositeIndex;
										  } else if (IsFullText(indexType)) {
											  status = StoredValuesOptimizationStatus::DisabledByFullTextIndex;
										  }
									  }
									  if (qe.IsRightFieldIndexed()) {
										  assertrx_throw(jns->indexes_.size() > static_cast<size_t>(qe.RightIdxNo()));
										  if (IsComposite(jns->indexes_[qe.RightIdxNo()]->Type())) {
											  status = StoredValuesOptimizationStatus::DisabledByCompositeIndex;
										  }
									  }
								  });
	if (status == StoredValuesOptimizationStatus::Enabled) {
		for (const auto& se : mainQ.sortingEntries_) {
			if (byJoinedField(se.expression, jItemQ.NsName())) {
				return StoredValuesOptimizationStatus::DisabledByJoinedFieldSort;  // TODO maybe allow #1410
			}
		}
	}
	return status;
}

template <typename T>
bool RxSelector::selectSubQuery(const Query& subQuery, const Query& mainQuery, NsLocker<T>& locks, SelectFunctionsHolder& func,
								std::vector<SubQueryExplain>& explain, const RdxContext& rdxCtx) {
	auto ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	SelectCtxWithJoinPreSelect sctx{subQuery, &mainQuery};
	sctx.nsid = 0;
	sctx.requiresCrashTracking = true;
	sctx.reqMatchedOnceFlag = true;
	sctx.contextCollectingMode = true;
	sctx.functions = &func;

	QueryResults result;
	ns->Select(result, sctx, rdxCtx);
	locks.Delete(ns);
	if (!result.GetExplainResults().empty()) {
		explain.emplace_back(subQuery.NsName(), result.MoveExplainResults());
	}
	return sctx.matchedAtLeastOnce;
}

template <typename T>
VariantArray RxSelector::selectSubQuery(const Query& subQuery, const Query& mainQuery, NsLocker<T>& locks, QueryResults& qr,
										SelectFunctionsHolder& func, std::variant<std::string, size_t> fieldOrKeys,
										std::vector<SubQueryExplain>& explain, const RdxContext& rdxCtx) {
	NamespaceImpl::Ptr ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	SelectCtxWithJoinPreSelect sctx{subQuery, &mainQuery};
	sctx.nsid = 0;
	sctx.requiresCrashTracking = true;
	sctx.contextCollectingMode = true;
	sctx.functions = &func;

	ns->Select(qr, sctx, rdxCtx);
	VariantArray result, buf;
	if (qr.GetAggregationResults().empty()) {
		assertrx_throw(!subQuery.SelectFilters().empty());
		const std::string_view field = subQuery.SelectFilters()[0];
		result.reserve(qr.Count());
		if (int idxNo = -1; ns->getIndexByNameOrJsonPath(field, idxNo) && !ns->indexes_[idxNo]->Opts().IsSparse()) {
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
						throw Error(errParams,
									"Current query strict mode allows aggregate index fields only. There are no indexes with name '%s' in "
									"namespace '%s'",
									field, subQuery.NsName());
					case StrictModeNames:
						if (ns->tagsMatcher_.path2tag(field).empty()) {
							throw Error(errParams,
										"Current query strict mode allows aggregate existing fields only. There are no fields with name "
										"'%s' in namespace '%s'",
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
	if (!qr.GetExplainResults().empty()) {
		explain.emplace_back(subQuery.NsName(), std::move(qr.MoveExplainResults()));
		explain.back().SetFieldOrKeys(std::move(fieldOrKeys));
	}
	return result;
}

template <typename T>
JoinedSelectors RxSelector::prepareJoinedSelectors(const Query& q, QueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func,
												   std::vector<QueryResultsContext>& queryResultsContexts, const RdxContext& rdxCtx) {
	JoinedSelectors joinedSelectors;
	if (q.GetJoinQueries().empty()) {
		return joinedSelectors;
	}
	auto ns = locks.Get(q.NsName());
	assertrx_throw(ns);

	// For each joined queries
	for (size_t i = 0, jqCount = q.GetJoinQueries().size(); i < jqCount; ++i) {
		const auto& jq = q.GetJoinQueries()[i];
		if rx_unlikely (isSystemNamespaceNameFast(jq.NsName())) {
			throw Error(errParams, "Queries to system namespaces ('%s') are not supported inside JOIN statement", jq.NsName());
		}
		if rx_unlikely (!jq.GetJoinQueries().empty()) {
			throw Error(errParams, "JOINs nested into the other JOINs are not supported");
		}
		if rx_unlikely (!jq.GetMergeQueries().empty()) {
			throw Error(errParams, "MERGEs nested into the JOINs are not supported");
		}
		if rx_unlikely (!jq.GetSubQueries().empty()) {
			throw Error(errParams, "Subquery in the JOINs are not supported");
		}
		if rx_unlikely (!jq.aggregations_.empty()) {
			throw Error(errParams, "Aggregations are not allowed in joined subqueries");
		}
		if rx_unlikely (jq.HasCalcTotal()) {
			throw Error(errParams, "Count()/count_cached() are not allowed in joined subqueries");
		}

		// Get common results from joined namespaces_
		auto jns = locks.Get(jq.NsName());
		assertrx_throw(jns);

		// Do join for each item in main result
		Query jItemQ(jq.NsName());
		jItemQ.Explain(q.NeedExplain());
		jItemQ.Debug(jq.GetDebugLevel()).Limit(jq.Limit());
		jItemQ.Strict(q.GetStrictMode());
		for (const auto& jse : jq.sortingEntries_) {
			jItemQ.Sort(jse.expression, jse.desc);
		}

		jItemQ.ReserveQueryEntries(jq.joinEntries_.size());

		// Construct join conditions
		for (auto& je : jq.joinEntries_) {
			QueryPreprocessor::SetQueryField(const_cast<QueryJoinEntry&>(je).LeftFieldData(), *ns);
			QueryPreprocessor::SetQueryField(const_cast<QueryJoinEntry&>(je).RightFieldData(), *jns);
			jItemQ.AppendQueryEntry<QueryEntry>(je.Operation(), QueryField(je.RightFieldData()), InvertJoinCondition(je.Condition()),
												QueryEntry::IgnoreEmptyValues{});
		}

		Query jjq(static_cast<const Query&>(jq));
		uint32_t joinedFieldIdx = uint32_t(joinedSelectors.size());
		if (jq.joinType == InnerJoin || jq.joinType == OrInnerJoin) {
			jjq.InjectConditionsFromOnConditions<InjectionDirection::FromMain>(jjq.Entries().Size(), jq.joinEntries_, q.Entries(), i,
																			   &ns->indexes_);
		}
		jjq.Offset(QueryEntry::kDefaultOffset);
		jjq.Limit(QueryEntry::kDefaultLimit);
		JoinCacheRes joinRes;
		if (!jjq.NeedExplain()) {
			jns->getFromJoinCache(jjq, joinRes);
		}
		JoinPreResult::CPtr preResult;
		if (joinRes.haveData) {
			preResult = std::move(joinRes.it.val.preResult);
		} else {
			SelectCtxWithJoinPreSelect ctx(jjq, &q, JoinPreResultBuildCtx{std::make_shared<JoinPreResult>()});
			ctx.preSelect.Result().storedValuesOptStatus = isPreResultValuesModeOptimizationAvailable(jItemQ, jns, q);
			ctx.functions = &func;
			ctx.requiresCrashTracking = true;
			QueryResults jr;
			jns->Select(jr, ctx, rdxCtx);
			std::visit(overloaded{[&](JoinPreResult::Values& values) {
									  values.PreselectAllowed(static_cast<size_t>(jns->config().maxPreselectSize) >= values.size());
									  values.Lock();
								  },
								  Restricted<IdSet, SelectIteratorContainer>{}([](const auto&) {})},
					   ctx.preSelect.Result().payload);
			preResult = ctx.preSelect.ResultPtr();
			if (joinRes.needPut) {
				jns->putToJoinCache(joinRes, preResult);
			}
		}

		queryResultsContexts.emplace_back(jns->payloadType_, jns->tagsMatcher_, FieldsSet(jns->tagsMatcher_, jq.SelectFilters()),
										  jns->schema_);

		const auto nsUpdateTime = jns->lastUpdateTimeNano();
		result.AddNamespace(jns, true);
		std::visit(overloaded{[&](const JoinPreResult::Values&) {
								  locks.Delete(jns);
								  jns.reset();
							  },
							  Restricted<IdSet, SelectIteratorContainer>{}([](const auto&) {})},
				   preResult->payload);
		joinedSelectors.emplace_back(jq.joinType, ns, std::move(jns), std::move(joinRes), std::move(jItemQ), result, jq,
									 JoinPreResultExecuteCtx{preResult}, joinedFieldIdx, func, false, nsUpdateTime, rdxCtx);
		ThrowOnCancel(rdxCtx);
	}
	return joinedSelectors;
}

template <typename T>
std::vector<SubQueryExplain> RxSelector::preselectSubQueries(Query& mainQuery, std::vector<QueryResults>& queryResultsHolder,
															 NsLocker<T>& locks, SelectFunctionsHolder& func, const RdxContext& ctx) {
	std::vector<SubQueryExplain> explains;
	if (mainQuery.NeedExplain() || mainQuery.GetDebugLevel() >= LogInfo) {
		explains.reserve(mainQuery.GetSubQueries().size());
	}
	for (size_t i = 0, s = mainQuery.Entries().Size(); i < s; ++i) {
		mainQuery.Entries().Visit(
			i, Skip<QueryEntriesBracket, QueryEntry, BetweenFieldsQueryEntry, JoinQueryEntry, AlwaysTrue, AlwaysFalse>{},
			[&](const SubQueryEntry& sqe) {
				try {
					const CondType cond = sqe.Condition();
					if (cond == CondAny || cond == CondEmpty) {
						if (selectSubQuery(mainQuery.GetSubQuery(sqe.QueryIndex()), mainQuery, locks, func, explains, ctx) ==
							(cond == CondAny)) {
							mainQuery.SetEntry<AlwaysTrue>(i);
						} else {
							mainQuery.SetEntry<AlwaysFalse>(i);
						}
					} else {
						QueryResults qr;
						const auto values = selectSubQuery(mainQuery.GetSubQuery(sqe.QueryIndex()), mainQuery, locks, qr, func,
														   sqe.Values().size(), explains, ctx);
						if (QueryEntries::CheckIfSatisfyCondition(values, sqe.Condition(), sqe.Values())) {
							mainQuery.SetEntry<AlwaysTrue>(i);
						} else {
							mainQuery.SetEntry<AlwaysFalse>(i);
						}
					}
				} catch (const Error& err) {
					throw Error(err.code(), "Error during preprocessing of subquery '" + mainQuery.GetSubQuery(sqe.QueryIndex()).GetSQL() +
												"': " + err.what());
				}
			},
			[&](const SubQueryFieldEntry& sqe) {
				try {
					queryResultsHolder.resize(queryResultsHolder.size() + 1);
					mainQuery.SetEntry<QueryEntry>(i, std::move(mainQuery.GetUpdatableEntry<SubQueryFieldEntry>(i)).FieldName(),
												   sqe.Condition(),
												   selectSubQuery(mainQuery.GetSubQuery(sqe.QueryIndex()), mainQuery, locks,
																  queryResultsHolder.back(), func, sqe.FieldName(), explains, ctx));
				} catch (const Error& err) {
					throw Error(err.code(), "Error during preprocessing of subquery '" + mainQuery.GetSubQuery(sqe.QueryIndex()).GetSQL() +
												"': " + err.what());
				}
			});
	}
	return explains;
}

template void RxSelector::DoSelect<const RdxContext, long_actions::QueryEnum2Type<QueryType::QuerySelect>>(
	const Query&, QueryResults&, NsLocker<const RdxContext>&, SelectFunctionsHolder&, const RdxContext&,
	QueryStatCalculator<long_actions::QueryEnum2Type<QueryType::QuerySelect>, long_actions::Logger>&);

}  // namespace reindexer
