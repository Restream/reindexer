#include "rx_selector.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/queryresults/fields_filter.h"
#include "core/queryresults/joinresults.h"
#include "estl/charset.h"
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

struct [[nodiscard]] RxSelector::QueryResultsContext {
	QueryResultsContext() = default;
	QueryResultsContext(PayloadType type, TagsMatcher tagsMatcher, FieldsFilter fieldsFilter, std::shared_ptr<const Schema> schema,
						lsn_t nsIncarnationTag)
		: type_(std::move(type)),
		  tagsMatcher_(std::move(tagsMatcher)),
		  fieldsFilter_(std::move(fieldsFilter)),
		  schema_(std::move(schema)),
		  nsIncarnationTag_(std::move(nsIncarnationTag)) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsFilter fieldsFilter_;
	std::shared_ptr<const Schema> schema_;
	lsn_t nsIncarnationTag_;
};

template <typename LockerType>
void RxSelector::preselectSubQuriesMain(const Query& q, std::optional<Query>& queryCopy, LockerType& locks, FtFunctionsHolder& func,
										std::vector<SubQueryExplain>& subQueryExplains, ExplainCalc::Duration& preselectTimeTotal,
										std::vector<LocalQueryResults>& queryResultsHolder, LogLevel logLevel, const RdxContext& ctx) {
	if (!q.GetSubQueries().empty()) {
		if (q.GetDebugLevel() >= LogInfo || logLevel >= LogInfo) {
			logFmt(LogInfo, "Query before subqueries substitution: {}", q.GetSQL());
		}
		if (!queryCopy) {
			queryCopy.emplace(q);
		}
		const auto preselectStartTime = ExplainCalc::Clock::now();
		subQueryExplains = preselectSubQueries(*queryCopy, queryResultsHolder, locks, func, ctx);
		preselectTimeTotal += ExplainCalc::Clock::now() - preselectStartTime;
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

template <typename LockerType, typename QueryType>
void RxSelector::DoSelect(const Query& q, std::optional<Query>& queryCopy, LocalQueryResults& result, LockerType& locks,
						  FtFunctionsHolder& func, const RdxContext& ctx, QueryStatCalculator<QueryType>& queryStatCalculator) {
	auto ns = locks.Get(q.NsName());
	std::vector<LocalQueryResults> queryResultsHolder;
	ExplainCalc::Duration preselectTimeTotal{0};
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

	JoinedSelectors mainJoinedSelectors;
	if (thereAreJoins) {
		const auto preselectStartTime = ExplainCalc::Clock::now();
		mainJoinedSelectors =
			prepareJoinedSelectors(query, result, locks, func, joinQueryResultsContexts, SetLimit0ForChangeJoin_False, ctx);
		result.joined_.resize(1 + query.GetMergeQueries().size());
		result.joined_[0].SetJoinedSelectorsCount(mainJoinedSelectors.size());
		preselectTimeTotal += ExplainCalc::Clock::now() - preselectStartTime;
	}
	QueryRankType commonQueryRankType{QueryRankType::NotSet};
	auto commonLimit = query.Limit();
	auto commonOffset = query.Offset();
	auto commonRankOrdering = RankOrdering::Off;
	{
		SelectCtxWithJoinPreSelect selCtx(query, nullptr, &result.GetFloatVectorsHolder());
		selCtx.joinedSelectors = mainJoinedSelectors.size() ? &mainJoinedSelectors : nullptr;
		selCtx.preResultTimeTotal = preselectTimeTotal;
		selCtx.contextCollectingMode = true;
		selCtx.functions = &func;
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
		if (selCtx.explain.IsEnabled()) {
			queryStatCalculator.AddExplain(selCtx.explain);
		}
	}
	// should be destroyed after results.lockResults()
	std::vector<JoinedSelectors> mergeJoinedSelectors;
	if (!query.GetMergeQueries().empty()) {
		if (commonQueryRankType != QueryRankType::Hybrid && !query.GetSortingEntries().empty()) [[unlikely]] {
			throw Error{errNotValid, "Sorting in merge query is not implemented yet"};	// TODO #1449
		}
		commonRankOrdering = GetRankOrdering(commonQueryRankType, query.GetSortingEntries());
		mergeJoinedSelectors.reserve(query.GetMergeQueries().size());
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
			SelectCtxWithJoinPreSelect mctx(mQuery, &query, &result.GetFloatVectorsHolder());
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
			if (thereAreJoins) {
				auto& mjs = mergeJoinedSelectors.emplace_back(
					prepareJoinedSelectors(mQuery, result, locks, func, joinQueryResultsContexts, SetLimit0ForChangeJoin_False, ctx));
				mctx.joinedSelectors = mjs.size() ? &mjs : nullptr;
				result.joined_[mctx.nsid].SetJoinedSelectorsCount(mjs.size());
			}
			mctx.requiresCrashTracking = true;
			mns->Select(result, mctx, ctx);
			result.AddNamespace(mns, true);
		}
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
	}
	// Adding context to QueryResults
	for (const auto& jctx : joinQueryResultsContexts) {
		result.addNSContext(jctx.type_, jctx.tagsMatcher_, jctx.fieldsFilter_, jctx.schema_, jctx.nsIncarnationTag_);
	}
}

void RxSelector::DoPreSelectForUpdateDelete(const Query& q, std::optional<Query>& queryCopy, LocalQueryResults& result, NsLockerW& locks,
											FloatVectorsHolderMap* fvHolder, const RdxContext& rdxCtx) {
	FtFunctionsHolder func;
	auto ns = locks.Get(q.NsName());

	std::vector<LocalQueryResults> queryResultsHolder;
	ExplainCalc::Duration preselectTimeTotal{0};
	std::vector<SubQueryExplain> subQueryExplains;
	preselectSubQuriesMain(q, queryCopy, locks, func, subQueryExplains, preselectTimeTotal, queryResultsHolder, ns->config_.logLevel,
						   rdxCtx);

	std::vector<QueryResultsContext> joinQueryResultsContexts;

	JoinedSelectors mainJoinedSelectors;
	if (!q.GetJoinQueries().empty()) {
		const auto preselectStartTime = ExplainCalc::Clock::now();
		const Query& query = queryCopy.has_value() ? *queryCopy : q;
		mainJoinedSelectors =
			prepareJoinedSelectors(query, result, locks, func, joinQueryResultsContexts, SetLimit0ForChangeJoin_True, rdxCtx);
		result.joined_.resize(1);
		result.joined_[0].SetJoinedSelectorsCount(mainJoinedSelectors.size());
		preselectTimeTotal += ExplainCalc::Clock::now() - preselectStartTime;
	}

	const Query& query = queryCopy.has_value() ? *queryCopy : q;
	SelectCtxWithJoinPreSelect selCtx(query, nullptr, fvHolder);
	selCtx.joinedSelectors = mainJoinedSelectors.size() ? &mainJoinedSelectors : nullptr;
	selCtx.preResultTimeTotal = preselectTimeTotal;
	selCtx.contextCollectingMode = true;
	selCtx.functions = &func;
	selCtx.nsid = 0;
	selCtx.requiresCrashTracking = true;
	selCtx.selectBeforeUpdate = true;

	NsSelecter selecter(ns.get());
	selecter(result, selCtx, rdxCtx);
	result.AddNamespace(ns, true);
	// Adding context to QueryResults
	for (const auto& jctx : joinQueryResultsContexts) {
		result.addNSContext(jctx.type_, jctx.tagsMatcher_, jctx.fieldsFilter_, jctx.schema_, jctx.nsIncarnationTag_);
	}
}

static bool byJoinedField(std::string_view sortExpr, std::string_view joinedNs) {
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
		if (tolower(sortExpr[i]) != tolower(joinedNs[j])) {
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
	jItemQ.Entries().VisitForEach([](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) { assertrx_throw(0); },
								  Skip<JoinQueryEntry, QueryEntriesBracket, AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry>{},
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
								  },
								  [&status](const KnnQueryEntry&) { status = StoredValuesOptimizationStatus::DisabledByFloatVectorIndex; });
	if (status == StoredValuesOptimizationStatus::Enabled) {
		for (const auto& se : mainQ.GetSortingEntries()) {
			if (byJoinedField(se.expression, jItemQ.NsName())) {
				return StoredValuesOptimizationStatus::DisabledByJoinedFieldSort;  // TODO maybe allow #1410
			}
		}
	}
	return status;
}

template <typename LockerT>
bool RxSelector::selectSubQuery(const Query& subQuery, const Query& mainQuery, LockerT& locks, FtFunctionsHolder& func,
								std::vector<SubQueryExplain>& explain, const RdxContext& rdxCtx) {
	auto ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	LocalQueryResults result;
	SelectCtxWithJoinPreSelect sctx{subQuery, &mainQuery, &result.GetFloatVectorsHolder()};
	sctx.nsid = 0;
	sctx.requiresCrashTracking = true;
	sctx.reqMatchedOnceFlag = true;
	sctx.contextCollectingMode = true;
	sctx.functions = &func;

	ns->Select(result, sctx, rdxCtx);
	locks.Delete(ns);
	if (!result.GetExplainResults().empty()) {
		explain.emplace_back(subQuery.NsName(), result.MoveExplainResults());
	}
	return sctx.matchedAtLeastOnce;
}

template <typename LockerT>
VariantArray RxSelector::selectSubQuery(const Query& subQuery, const Query& mainQuery, LockerT& locks, LocalQueryResults& qr,
										FtFunctionsHolder& func, std::variant<std::string, size_t> fieldOrKeys,
										std::vector<SubQueryExplain>& explain, const RdxContext& rdxCtx) {
	NamespaceImpl::Ptr ns = locks.Get(subQuery.NsName());
	assertrx_throw(ns);

	SelectCtxWithJoinPreSelect sctx{subQuery, &mainQuery, &qr.GetFloatVectorsHolder()};
	sctx.nsid = 0;
	sctx.requiresCrashTracking = true;
	sctx.contextCollectingMode = true;
	sctx.functions = &func;

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
						throw Error(errParams,
									"Current query strict mode allows aggregate index fields only. There are no indexes with name '{}' in "
									"namespace '{}'",
									field, subQuery.NsName());
					case StrictModeNames:
						if (ns->tagsMatcher_.path2tag(field).empty()) {
							throw Error(errParams,
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
	if (!qr.GetExplainResults().empty()) {
		explain.emplace_back(subQuery.NsName(), std::move(qr.MoveExplainResults()));
		explain.back().SetFieldOrKeys(std::move(fieldOrKeys));
	}
	return result;
}

template <typename LockerType>
JoinedSelectors RxSelector::prepareJoinedSelectors(const Query& q, LocalQueryResults& result, LockerType& locks, FtFunctionsHolder& func,
												   std::vector<QueryResultsContext>& queryResultsContexts, SetLimit0ForChangeJoin limit0,
												   const RdxContext& rdxCtx) {
	JoinedSelectors joinedSelectors;
	if (q.GetJoinQueries().empty()) {
		return joinedSelectors;
	}
	auto ns = locks.Get(q.NsName());
	const StrictMode strictMode{(q.GetStrictMode() != StrictModeNotSet) ? q.GetStrictMode() : ns->config_.strictMode};

	// For each joined queries
	for (size_t i = 0, jqCount = q.GetJoinQueries().size(); i < jqCount; ++i) {
		const auto& jq = q.GetJoinQueries()[i];
		if (isSystemNamespaceNameFast(jq.NsName())) [[unlikely]] {
			throw Error(errParams, "Queries to system namespaces ('{}') are not supported inside JOIN statement", jq.NsName());
		}
		if (!jq.GetJoinQueries().empty()) [[unlikely]] {
			throw Error(errParams, "JOINs nested into the other JOINs are not supported");
		}
		if (!jq.GetMergeQueries().empty()) [[unlikely]] {
			throw Error(errParams, "MERGEs nested into the JOINs are not supported");
		}
		if (!jq.GetSubQueries().empty()) [[unlikely]] {
			throw Error(errParams, "Subquery in the JOINs are not supported");
		}
		if (!jq.aggregations_.empty()) [[unlikely]] {
			throw Error(errParams, "Aggregations are not allowed in joined subqueries");
		}
		if (jq.HasCalcTotal()) [[unlikely]] {
			throw Error(errParams, "Count()/count_cached() are not allowed in joined subqueries");
		}

		// Get common results from joined namespaces_
		auto jns = locks.Get(jq.NsName());
		assertrx_throw(jns);

		// Do join for each item in main result
		Query jItemQ(jq.NsName());
		jItemQ.Explain(q.NeedExplain());
		jItemQ.Debug(jq.GetDebugLevel());
		jItemQ.Limit(jq.Limit());
		jItemQ.Strict(q.GetStrictMode());
		for (const auto& jse : jq.GetSortingEntries()) {
			jItemQ.Sort(jse.expression, *jse.desc);
		}

		jItemQ.ReserveQueryEntries(jq.joinEntries_.size());

		// Construct join conditions
		for (auto& je : jq.joinEntries_) {
			QueryPreprocessor::SetQueryField(const_cast<QueryJoinEntry&>(je).LeftFieldData(), *ns);
			QueryPreprocessor::VerifyOnStatementField(je.LeftFieldData(), *ns, strictMode);
			QueryPreprocessor::SetQueryField(const_cast<QueryJoinEntry&>(je).RightFieldData(), *jns);
			QueryPreprocessor::VerifyOnStatementField(je.RightFieldData(), *jns, strictMode);
			jItemQ.AppendQueryEntry<QueryEntry>(je.Operation(), QueryField(je.RightFieldData()), InvertJoinCondition(je.Condition()),
												QueryEntry::IgnoreEmptyValues{});
		}

		Query jjq(static_cast<const Query&>(jq));
		const uint32_t joinedFieldIdx = uint32_t(joinedSelectors.size());
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
			SelectCtxWithJoinPreSelect ctx(jjq, &q, JoinPreResultBuildCtx{std::make_shared<JoinPreResult>()},
										   &result.GetFloatVectorsHolder());
			ctx.preSelect.Result().storedValuesOptStatus = isPreResultValuesModeOptimizationAvailable(jItemQ, jns, q);
			ctx.functions = &func;
			ctx.requiresCrashTracking = true;
			LocalQueryResults jr;
			jns->Select(jr, ctx, rdxCtx);
			std::visit(overloaded{[&](JoinPreResult::Values& values) {
									  values.PreselectAllowed(static_cast<size_t>(jns->config().maxPreselectSize) >= values.Size());
									  values.Lock();
								  },
								  []<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) {}},
					   ctx.preSelect.Result().payload);
			preResult = ctx.preSelect.ResultPtr();
			if (joinRes.needPut) {
				jns->putToJoinCache(joinRes, preResult);
			}
		}

		const auto nsUpdateTime = jns->lastUpdateTimeNano();
		result.AddNamespace(jns, true);
		queryResultsContexts.emplace_back(jns->payloadType_, jns->tagsMatcher_, FieldsFilter{jq.SelectFilters(), *jns}, jns->schema_,
										  jns->incarnationTag_);

		std::visit(overloaded{[&](const JoinPreResult::Values&) {
								  locks.Delete(jns);
								  jns.reset();
							  },
							  []<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) {}},
				   preResult->payload);
		joinedSelectors.emplace_back(jq.joinType, ns, std::move(jns), std::move(joinRes), std::move(jItemQ),
									 FieldsFilter{jq.SelectFilters(), *ns}, result, jq, JoinPreResultExecuteCtx{preResult}, joinedFieldIdx,
									 func, false, nsUpdateTime, limit0, rdxCtx);
		ThrowOnCancel(rdxCtx);
	}
	return joinedSelectors;
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
			i, overloaded{[&i](const concepts::OneOf<QueryEntriesBracket, QueryEntry, BetweenFieldsQueryEntry, JoinQueryEntry, AlwaysTrue,
													 AlwaysFalse, KnnQueryEntry, MultiDistinctQueryEntry> auto&) noexcept { ++i; },
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
						  }});
		assertrx_dbg(i > cur);
	}
	return explains;
}

template void RxSelector::DoSelect<RxSelector::NsLocker<const RdxContext>, long_actions::QueryEnum2Type<QueryType::QuerySelect>>(
	const Query&, std::optional<Query>&, LocalQueryResults&, NsLocker<const RdxContext>&, FtFunctionsHolder&, const RdxContext&,
	QueryStatCalculator<long_actions::QueryEnum2Type<QueryType::QuerySelect>, long_actions::Logger>&);

}  // namespace reindexer
