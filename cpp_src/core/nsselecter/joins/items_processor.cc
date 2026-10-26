#include "items_processor.h"
#include <tuple>

#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joins/results.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/queryresults/context.h"
#include "core/reindexer_impl/rx_selector.h"
#include "estl/algorithm.h"
#include "helpers.h"
#include "vendor/sparse-map/sparse_set.h"

using namespace reindexer;

namespace reindexer::joins {
namespace {

constexpr size_t kMaxNestedJoinDepth = 64;

struct [[nodiscard]] RightNsItemQuery {
	Query query;
	size_t joinEntriesStart = 0;
};

void removeNullValues(VariantArray& values) {
	values.erase(unstable_remove_if(values.begin(), values.end(), [](const Variant& v) noexcept { return v.IsNullValue(); }),
				 values.cend());
}

size_t updateQueryEntry(QueryImpl itemQuery, VariantArray&& values, CondType condition, size_t entryIndex) {
	size_t updatedEntries = 0;

	if (values.empty()) {
		updatedEntries = itemQuery.ReplaceQueryEntry<AlwaysFalse>(entryIndex);
	} else {
		const QueryEntry& qentry{itemQuery.Entries().Get<QueryEntry>(entryIndex)};
		updatedEntries = itemQuery.ReplaceQueryEntry<QueryEntry>(entryIndex, qentry, condition, std::move(values));
		values = {};
	}

	return updatedEntries;
}

size_t updateQueryEntryValues(Query& itemQuery, std::optional<Query>& itemQueryCopy, VariantArray& values, size_t qeIdx) {
	auto itemQueryImpl = Impl(itemQueryCopy.has_value() ? itemQueryCopy.value() : itemQuery);

#ifdef RX_WITH_STDLIB_DEBUG
	const auto initialCond = itemQueryImpl.Entries().Get<QueryEntry>(qeIdx).Condition();
	const auto initialSize = itemQueryImpl.Entries().Size();
#endif	// RX_WITH_STDLIB_DEBUG

	size_t updatedEntries{1};

	removeNullValues(values);
	if (values.empty() || !itemQueryImpl.TryUpdateQueryEntryInplace(qeIdx, values)) {
		if (!itemQueryCopy.has_value()) {
			itemQueryCopy.emplace(itemQuery);
			itemQueryImpl = Impl(itemQueryCopy.value());
		}
		const QueryEntry& qentry{itemQueryImpl.Entries().Get<QueryEntry>(qeIdx)};
		updatedEntries = updateQueryEntry(itemQueryImpl, std::move(values), qentry.Condition(), qeIdx);
	}
#ifdef RX_WITH_STDLIB_DEBUG
	else {
		assertrx_dbg(initialCond == itemQueryImpl.Entries().Get<QueryEntry>(qeIdx).Condition());
		assertrx_dbg(initialSize == itemQueryImpl.Entries().Size());
	}
#endif	// RX_WITH_STDLIB_DEBUG

	return updatedEntries;
}

/**
 * Building a per-item query for the Right NS.
 * If the source query with joins looks like this:
 *
 *     SELECT * FROM ns
 *     INNER JOIN (
 *         SELECT * FROM ns2
 *         INNER JOIN ns3 ON ns2.x = ns3.x OR ns2.y = ns3.y
 *         WHERE ns2.active = true
 *     ) ON ns.id = ns2.owner_id OR ns.alt_id = ns2.owner_id;
 *
 * Then the per-item Right NS query (with such ids: {"id": 10, "alt_id": 20}) should look like this:
 *
 *     SELECT * FROM ns2
 *     INNER JOIN ns3 ON ns2.x = ns3.x OR ns2.y = ns3.y
 *     WHERE ns2.active = true
 *       AND ns2.owner_id = 10 OR ns2.owner_id = 20;
 **/
RightNsItemQuery buildRightNsItemQuery(ConstQueryImpl mainQuery, ConstJoinedQueryImpl joinedQuery, const NamespaceImpl& leftNs,
									   const NamespaceImpl& rightNs, StrictMode strictMode) {
	const auto joinedQueryImpl = Impl(*joinedQuery);
	const bool withNestedJoins{!joinedQueryImpl.JoinQueries().empty()};

	Query itemQuery{withNestedJoins ? static_cast<const Query&>(*joinedQuery) : Query{joinedQueryImpl.NsName()}};
	auto itemQueryImpl = Impl(itemQuery);
	itemQuery.Explain(mainQuery.NeedExplain())
		.Debug(joinedQueryImpl.DebugLevel())
		.Limit(joinedQueryImpl.Limit())
		.Strict(mainQuery.GetStrictMode());
	if (!withNestedJoins) {
		for (const auto& jse : joinedQueryImpl.GetSortingEntries()) {
			itemQuery.Sort(jse.expression, jse.desc ? SortOrder::Desc : SortOrder::Asc);
		}
	}

	itemQuery.Offset(0);
	itemQueryImpl.ReserveQueryEntries(itemQueryImpl.Entries().Size() + joinedQuery.JoinEntries().size());

	const size_t joinEntriesStart{itemQueryImpl.Entries().Size()};
	for (auto& je : joinedQuery.JoinEntries()) {
		QueryPreprocessor::SetQueryField(const_cast<QueryJoinEntry&>(je).LeftFieldData(), leftNs);
		QueryPreprocessor::VerifyOnStatementField(je.LeftFieldData(), leftNs, strictMode);
		QueryPreprocessor::SetQueryField(const_cast<QueryJoinEntry&>(je).RightFieldData(), rightNs);
		QueryPreprocessor::VerifyOnStatementField(je.RightFieldData(), rightNs, strictMode);
		itemQueryImpl.AddCondition<QueryEntry>(je.Operation(), QueryField(je.RightFieldData()), InvertJoinCondition(je.Condition()),
											   QueryEntry::IgnoreEmptyValues{});
	}

	return {std::move(itemQuery), joinEntriesStart};
}

void setJoinedFieldsCount(Results& joined, int nsid, uint32_t joinedFields) {
	// A zero count only pads the vector with an empty NamespaceResults. Its fast_hash_map allocates on
	// construction, and Insert never uses that slot: rows are stored under the left namespace id.
	if (joinedFields == 0) {
		return;
	}
	if (size_t minSize = nsid + 1; minSize > joined.size()) {
		joined.resize(minSize);
	}
	if (joined[nsid].GetFieldsCount() < joinedFields) {
		joined[nsid].SetJoinedFieldsCount(joinedFields);
	}
}

}  // namespace

bool ItemsProcessor::Process(IdType rowId, int nsId, ConstPayload payload, FloatVectorsHolderMap* floatVectorsHolder,
							 bool withJoinedItems) {
	++called_;
	if (optimized_ && !withJoinedItems) {
		matched_++;
		return true;
	}

	const auto startTime = Explain::Clock::now();

	std::optional<Query> itemQueryCopy;
	if (Impl(itemQuery_).NeedExplain() && !explainOneSelect_.empty()) {
		itemQuery_.Explain(false);
	}
	size_t qeIdx = joinEntriesStart_;
	for (auto& je : joinQuery_.JoinEntries()) {
		payload.GetByFieldsSet(je.LeftFields(), tmpValues_, je.LeftFieldType(), je.LeftCompositeFieldsTypes());
		qeIdx += updateQueryEntryValues(itemQuery_, itemQueryCopy, tmpValues_, qeIdx);
	}
	Query& itemQuery{itemQueryCopy.has_value() ? itemQueryCopy.value() : itemQuery_};
	itemQuery.Limit((withJoinedItems && !limit0_) ? Impl(*joinQuery_).Limit() : 0);

	LocalQueryResults qr;
	bool found = false;
	bool matchedAtLeastOnce = false;
	std::visit(overloaded{[&](const PreSelect::Values&) { selectFromPreSelectValues(qr, Impl(itemQuery), found, matchedAtLeastOnce); },
						  [&]<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {
							  selectFromRightNs(qr, Impl(itemQuery), floatVectorsHolder, found, matchedAtLeastOnce);
						  }},
			   PreSelectResults().payload);

	if (withJoinedItems && found) {
		assertrx_throw(nsId < static_cast<int>(result_.Joined().size()));
		joins::NamespaceResults& nsJoinRes = result_.Joined()[nsId];
		assertrx_dbg(nsJoinRes.GetFieldsCount());
		if (floatVectorsHolder) {
			std::visit(overloaded{[&](const PreSelect::Values&) noexcept {},
								  [&]<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {
									  floatVectorsHolder->Add(*RightNs(), qr.begin(), qr.end(), fieldsFilter_);
								  }},
					   PreSelectResults().payload);
		}
		nsJoinRes.Insert(rowId, rightNsId_, joinedFieldIdx_, std::move(qr));
	}
	if (matchedAtLeastOnce) {
		++matched_;
	}

	selectTime_ += (Explain::Clock::now() - startTime);

	return matchedAtLeastOnce;
}

void ItemsProcessor::BuildSelectIteratorsOfIndexedFields(int* maxIterations, unsigned sortId, const FtFunction::Ptr& ftFunc,
														 const RdxContext& rdxCtx, SelectIteratorContainer& iterators) {
	static constexpr size_t kMaxIterationsScaleForInnerJoinOptimization = 100;

	assertrx_throw(!ftFunc || ftFunc->Empty());
	std::ignore = ftFunc;

	const auto& preselect = PreSelectResults();
	if (joinType_ != JoinType::InnerJoin || preSelectCtx_.Mode() != PreSelectMode::Execute ||
		std::visit(overloaded{[](const SelectIteratorContainer&) { return true; },
							  [maxIterations]<concepts::OneOf<IdSetPlain, PreSelect::Values> T>(const T& v) {
								  return v.Size() > *maxIterations * kMaxIterationsScaleForInnerJoinOptimization;
							  }},
				   preselect.payload)) {
		return;
	}

	unsigned optimized = 0;
	const auto joinedQueryImpl = joinQuery_;
	assertrx_throw(!std::holds_alternative<PreSelect::Values>(preselect.payload) ||
				   Impl(itemQuery_).Entries().Size() == joinedQueryImpl.JoinEntries().size());
	for (size_t i = 0; i < joinedQueryImpl.JoinEntries().size(); ++i) {
		const QueryJoinEntry& joinEntry = joinedQueryImpl.JoinEntries()[i];
		if (!joinEntry.IsLeftFieldIndexed() || joinEntry.Operation() != OpAnd ||
			(joinEntry.Condition() != CondEq && joinEntry.Condition() != CondSet) ||
			(i + 1 < joinedQueryImpl.JoinEntries().size() && joinedQueryImpl.JoinEntries()[i + 1].Operation() == OpOr)) {
			continue;
		}
		const auto& leftIndex = leftNs_->indexes()[joinEntry.LeftIdxNo()];
		if (IsFullText(leftIndex->Type()) || IsComposite(leftIndex->Type())) {
			continue;
		}

		// Avoid using GetByJsonPath() when extracting values.
		// TODO: This substitution can sometimes be effective with GetByJsonPath(),
		// so users should be allowed to hint at this optimization.
		bool hasSparseInRightField = false;
		for (int field : joinEntry.RightFields()) {
			if (field == SetByJsonPath) {
				hasSparseInRightField = true;
				break;
			}
		}
		if (hasSparseInRightField) {
			continue;
		}

		VariantArray values = std::visit(overloaded{[&](const IdSetPlain& preselected) {
														const std::vector<IdType>* sortOrders = nullptr;
														if (preselect.sortOrder.index) {
															sortOrders = &(preselect.sortOrder.index->SortOrders());
														}
														return readValuesOfRightNsFrom(
															preselected,
															[this, sortOrders](IdType rowId) noexcept {
																const auto properRowId =
																	sortOrders ? (*sortOrders)[rowId.ToNumber()] : rowId;
																return ConstPayload{rightNs_->payloadType(), rightNs_->items_[properRowId]};
															},
															joinEntry, rightNs_->payloadType());
													},
													[&](const PreSelect::Values&) { return readValuesFromPreSelect(joinEntry); },
													[](const SelectIteratorContainer&) -> VariantArray { throw_as_assert; }},
										 preselect.payload);

		if (leftIndex->Opts().GetCollateMode() == CollateUTF8) {
			for (auto& key : values) {
				key.EnsureUTF8();
			}
		}
		const auto selectType = leftIndex->SelectKeyType();
		const auto& pt = leftIndex->GetPayloadType();
		const auto& fields = leftIndex->Fields();
		for (auto& key : values) {
			std::ignore = key.convert(selectType, &pt, &fields);
		}

		Index::SelectContext selectContext;
		selectContext.opts.maxIterations = iterators.GetPlanningBudget();
		selectContext.opts.indexesNotOptimized = !leftNs_->SortOrdersBuilt();
		selectContext.opts.inTransaction = inTransaction_;

		auto selectResults = leftIndex->SelectKey(values, CondSet, sortId, selectContext, rdxCtx);
		auto* selectKeyResultsVector = std::get_if<SelectKeyResultsVector>(&selectResults);
		if (!selectKeyResultsVector || selectKeyResultsVector->empty()) {
			continue;
		}

		SelectIterator selectIterator{std::move(*selectKeyResultsVector->begin()), IsDistinct_False, std::string(joinEntry.LeftFieldName()),
									  joinEntry.LeftIdxNo()};
		for (auto it = selectKeyResultsVector->begin() + 1, end = selectKeyResultsVector->end(); it != end; ++it) {
			selectIterator.Append(std::move(*it));
		}
		const size_t fallback = *maxIterations > 0 ? size_t(*maxIterations) : 0;
		const size_t estimatedIterations = int(selectIterator.EstimateMaxIterations(fallback));
		const int curIterations = std::min(estimatedIterations, size_t(std::numeric_limits<int>::max()));
		if (curIterations && curIterations < *maxIterations) {
			*maxIterations = curIterations;
		}
		std::ignore = iterators.Append(OpAnd, std::move(selectIterator));
		++optimized;
	}
	optimized_ = (optimized == joinedQueryImpl.JoinEntries().size());
}

joins::PreSelect::CPtr ItemsProcessor::buildPreSelect(int nsid, ConstQueryImpl query, size_t joinedField,
													  std::span<ItemsProcessor> itemsProcessors,
													  PreSelect::ValuesOptimizationStatus storedValuesOptStatus,
													  const NamespaceImpl::Ptr& ns, const NamespaceImpl::Ptr& jns,
													  LocalQueryResults& result, FtFunctionsHolder& func, CacheRes& cacheRes,
													  const RdxContext& rdxCtx) {
	const auto joinedQuery = JoinedImpl(query.JoinQueries()[joinedField]);
	Query preSelectQuery{static_cast<const Query&>(*joinedQuery)};
	auto preSelectQueryImpl = Impl(preSelectQuery);
	if (joinedQuery.GetJoinType() == InnerJoin || joinedQuery.GetJoinType() == OrInnerJoin) {
		std::ignore = preSelectQueryImpl.InsertConditionsFromOnConditions<JoinConditionInsertionDirection::FromMain>(
			preSelectQueryImpl.Entries().Size(), joinedQuery.JoinEntries(), query.Entries(), joinedField, &ns->indexes());
	}
	preSelectQuery.Offset(QueryEntry::kDefaultOffset);
	preSelectQuery.Limit(QueryEntry::kDefaultLimit);
	if (!preSelectQueryImpl.NeedExplain() && !preSelectQueryImpl.HasVolatileExpressions()) {
		jns->getFromJoinCache(preSelectQueryImpl, cacheRes);
	}

	joins::PreSelect::CPtr preSelect;
	if (cacheRes.haveData) {
		preSelect = std::move(cacheRes.it.val.preSelect);
	} else {
		JoinPreSelectCtx ctx{preSelectQueryImpl, query, joins::PreSelectBuildCtx{std::make_shared<joins::PreSelect>()},
							 &result.GetFloatVectorsHolder()};
		assertrx_throw(result.Joined().GetJoinsTable().has_value());
		const int rightNsId{
			result.Joined().GetJoinsTable()->GetJoinedNsId(nsid, joinedField)};	 // NOLINT(bugprone-unchecked-optional-access)
		setJoinedFieldsCount(result.Joined(), rightNsId, itemsProcessors.size());
		ctx.nsid = rightNsId;
		ctx.joinItemsProcessors = itemsProcessors;
		ctx.preSelect.Result().storedValuesOptStatus = storedValuesOptStatus;
		ctx.functions = &func;
		ctx.requiresCrashTracking = true;
		ctx.explain = nullptr;	// No external explain for joins preselect
		LocalQueryResults jr;
		jns->Select(jr, ctx, rdxCtx);
		std::visit(overloaded{[&](joins::PreSelect::Values& values) {
								  values.PreselectAllowed(static_cast<size_t>(jns->config().maxPreselectSize) >= values.Size());
								  values.Lock();
							  },
							  []<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {}},
				   ctx.preSelect.Result().payload);
		preSelect = ctx.preSelect.ResultPtr();
		if (cacheRes.needPut) {
			jns->putToJoinCache(cacheRes, preSelect);
		}
	}

	return preSelect;
}

template <typename Locker>
ItemsProcessor ItemsProcessor::buildItemsProcessor(int nsid, ConstQueryImpl query, size_t joinedField, LocalQueryResults& result,
												   Locker& locks, FtFunctionsHolder& func,
												   std::vector<QueryResultsContext>& queryResultsContexts, IsModifyQuery isModifyQuery,
												   const RdxContext& rdxCtx, size_t depth) {
	if (depth > kMaxNestedJoinDepth) [[unlikely]] {
		throw Error(errParams, "Maximum nested join depth exceeded: {} > {}", depth, kMaxNestedJoinDepth);
	}
	const auto joinedQuery = JoinedImpl(query.JoinQueries()[joinedField]);
	const auto joinedQueryImpl = Impl(*joinedQuery);
	if (isSystemNamespaceNameFast(joinedQueryImpl.NsName())) [[unlikely]] {
		throw Error(errParams, "Queries to system namespaces ('{}') are not supported inside JOIN statement", joinedQueryImpl.NsName());
	}
	if (!joinedQueryImpl.MergeQueries().empty()) [[unlikely]] {
		throw Error(errParams, "MERGEs nested into the JOINs are not supported");
	}
	if (!joinedQueryImpl.Aggregations().empty()) [[unlikely]] {
		throw Error(errParams, "Aggregations are not allowed in joined subqueries");
	}
	if (joinedQueryImpl.HasCalcTotal()) [[unlikely]] {
		throw Error(errParams, "Count()/count_cached() are not allowed in joined subqueries");
	}
	if (joinedQuery.JoinEntries().empty()) [[unlikely]] {
		throw Error{errQueryExec, "Join without ON conditions"};
	}
	if (joinedQuery.JoinEntries().front().Operation() == OpOr) [[unlikely]] {
		throw Error{errQueryExec, "OR operator in first condition or after left join"};
	}

	auto ns{locks.Get(query.NsName())};
	auto jns{locks.Get(joinedQueryImpl.NsName())};
	assertrx_throw(ns);
	assertrx_throw(jns);
	if (!isModifyQuery && joinedQueryImpl.Limit() != 0) {
		result.AddNamespace(jns, true);
	}

	assertrx_throw(result.Joined().GetJoinsTable().has_value());
	const auto& joinsTable{result.Joined().GetJoinsTable()};
	assertrx_throw(joinsTable.has_value());

	const int rightNsId{joinsTable->GetJoinedNsId(nsid, joinedField)};	// NOLINT(bugprone-unchecked-optional-access)
	const size_t rightNsCtxIndex{queryResultsContexts.size()};
	FieldsFilter rightNsFilter{joinedQueryImpl.SelectFilters(), *jns};
	if (!isModifyQuery) {
		queryResultsContexts.emplace_back(jns->payloadType(), jns->tagsMatcher(),
										  FieldsFilter::Create(joinedQueryImpl.SelectFilters(), *jns), jns->schema_, jns->incarnationTag_);
	}

	ItemsProcessors childItemsProcessors;
	childItemsProcessors.reserve(joinedQueryImpl.JoinQueries().size());
	if (!joinedQueryImpl.JoinQueries().empty()) {
		for (size_t i = 0; i < joinedQueryImpl.JoinQueries().size(); ++i) {
			childItemsProcessors.emplace_back(buildItemsProcessor(rightNsId, joinedQueryImpl, i, result, locks, func, queryResultsContexts,
																  isModifyQuery, rdxCtx, depth + 1));
		}
		setJoinedFieldsCount(result.Joined(), rightNsId, childItemsProcessors.size());
	}

	const StrictMode strictMode{(query.GetStrictMode() != StrictModeNotSet) ? query.GetStrictMode() : ns->config_.strictMode};
	RightNsItemQuery rightNsItemQuery{buildRightNsItemQuery(query, joinedQuery, *ns, *jns, strictMode)};

	const auto valuesOptimizationStatus{ItemsProcessor::isValuesOptimizationEnabled(Impl(rightNsItemQuery.query), jns, query)};

	CacheRes joinRes;
	joins::PreSelect::CPtr preSelect{
		buildPreSelect(nsid, query, joinedField, childItemsProcessors, valuesOptimizationStatus, ns, jns, result, func, joinRes, rdxCtx)};

	const auto nsUpdateTime{jns->lastUpdateTimeNano()};

	std::visit(overloaded{[&](const joins::PreSelect::Values&) {
							  locks.Delete(jns);
							  jns.reset();
						  },
						  []<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {}},
			   preSelect->payload);

	ThrowOnCancel(rdxCtx);

	return ItemsProcessor{joinedQuery.GetJoinType(),
						  ns,
						  std::move(jns),
						  queryResultsContexts.empty() ? nullptr : &queryResultsContexts[rightNsCtxIndex],
						  nsid,
						  rightNsId,
						  std::move(joinRes),
						  std::move(rightNsItemQuery.query),
						  std::move(rightNsFilter),
						  result,
						  *joinedQuery,
						  rightNsItemQuery.joinEntriesStart,
						  joins::PreSelectExecuteCtx{preSelect},
						  static_cast<uint16_t>(joinedField),
						  std::move(childItemsProcessors),
						  func,
						  false,
						  nsUpdateTime,
						  isModifyQuery ? SetLimit0ForChangeJoin_True : SetLimit0ForChangeJoin_False,
						  rdxCtx};
}

template <typename LockerType>
ItemsProcessors ItemsProcessor::BuildForQuery(int nsid, ConstQueryImpl q, LocalQueryResults& result, LockerType& locks,
											  FtFunctionsHolder& func, std::vector<QueryResultsContext>& queryResultsContexts,
											  IsModifyQuery isModifyQuery, const RdxContext& rdxCtx) {
	ItemsProcessors joinItemsProcessors;
	if (q.JoinQueries().empty()) {
		return joinItemsProcessors;
	}
	const auto joinQueriesCount{result.Joined().GetJoinsTable()->GetJoinQueriesCount()};  // NOLINT(bugprone-unchecked-optional-access)
	ItemsProcessors itemsProcessors;
	itemsProcessors.reserve(joinQueriesCount);
	queryResultsContexts.reserve(joinQueriesCount);
	for (size_t i = 0; i < q.JoinQueries().size(); ++i) {
		itemsProcessors.emplace_back(buildItemsProcessor(nsid, q, i, result, locks, func, queryResultsContexts, isModifyQuery, rdxCtx, 1));
	}
	if (size_t size = 1 + q.MergeQueries().size(); size > result.Joined().size()) {
		result.Joined().resize(size);
	}
	setJoinedFieldsCount(result.Joined(), nsid, itemsProcessors.size());
	return itemsProcessors;
}

void ItemsProcessor::selectFromRightNs(LocalQueryResults& joinItemR, ConstQueryImpl query, FloatVectorsHolderMap* floatVectorsHolder,
									   bool& found, bool& matchedAtLeastOnce) {
	assertrx_dbg(rightNs_);

	CacheRes joinResLong;
	if (!skipJoinCache_ && query.JoinQueries().empty()) {
		rightNs_->getFromJoinCache(query, *joinQuery_, joinResLong);
		rightNs_->getInsideFromJoinCache(joinRes_);
		if (joinRes_.needPut) {
			rightNs_->putToJoinCache(joinRes_, preSelectCtx_.ResultPtr());
		}
	}
	if (joinResLong.haveData) {
		found = !joinResLong.it.val.ids->IsEmpty();
		matchedAtLeastOnce = joinResLong.it.val.matchedAtLeastOnce;
		rightNs_->FillResult(joinItemR, *joinResLong.it.val.ids);
	} else {
		Explain explain;
		JoinSelectCtx ctx(query, std::nullopt, preSelectCtx_, floatVectorsHolder);
		setJoinedFieldsCount(result_.Joined(), rightNsId_, childItemsProcessors_.size());
		ctx.nsid = rightNsId_;
		ctx.joinItemsProcessors = childItemsProcessors_;
		ctx.matchedAtLeastOnce = false;
		ctx.reqMatchedOnceFlag = true;
		ctx.skipIndexesLookup = true;
		ctx.functions = &selectFunctions_;
		ctx.explain = &explain;
		rightNs_->Select(joinItemR, ctx, rdxCtx_);
		if (query.NeedExplain()) {
			explainOneSelect_ = explain.GetJSON();
		}

		found = joinItemR.Count();
		matchedAtLeastOnce = ctx.matchedAtLeastOnce;
	}
	if (joinResLong.needPut) {
		CacheVal val;
		val.ids = make_intrusive<intrusive_atomic_rc_wrapper<IdSetPlain>>();
		val.matchedAtLeastOnce = matchedAtLeastOnce;
		for (const auto& it : joinItemR.Items()) {
			val.ids->AddUnordered(it.GetItemRef().Id());
		}
		rightNs_->putToJoinCache(joinResLong, std::move(val));
	}
}

void ItemsProcessor::selectFromPreSelectValues(LocalQueryResults& joinItemR, ConstQueryImpl query, bool& found,
											   bool& matchedAtLeastOnce) const {
	size_t matched = 0;
	const auto& entries = query.Entries();
	const PreSelect::Values& values = std::get<PreSelect::Values>(PreSelectResults().payload);
	const auto& pt = values.payloadType;
	if (values.IsRanked()) {
		for (auto it : values) {
			const ItemRefRanked& rankedItem = *it.Ranked();
			const ItemRef& item = rankedItem.NotRanked();
			const auto& v = item.Value();
			assertrx_throw(!v.IsFree());
			if (entries.CheckIfSatisfyConditions({pt, v})) {
				if (++matched > query.Limit()) {
					break;
				}
				found = true;
				joinItemR.AddItemRef(rankedItem.Rank(), item);
			}
		}
	} else {
		for (auto it : values) {
			const ItemRef& item = *it.NotRanked();
			const auto& v = item.Value();
			assertrx_throw(!v.IsFree());
			if (entries.CheckIfSatisfyConditions({pt, v})) {
				if (++matched > query.Limit()) {
					break;
				}
				found = true;
				joinItemR.AddItemRef(item);
			}
		}
	}
	matchedAtLeastOnce = matched;
}

template <typename Cont, typename Fn>
VariantArray ItemsProcessor::readValuesOfRightNsFrom(const Cont& data, const Fn& createPayload, const QueryJoinEntry& entry,
													 const PayloadType& pt) const {
	const auto rightFieldType = entry.RightFieldType();
	const auto leftFieldType = entry.LeftFieldType();
	VariantArray res;
	if (rightFieldType.Is<KeyValueType::Composite>()) {
		unordered_payload_ref_set set(data.Size(), hash_composite_ref(pt, entry.RightFields()),
									  equal_composite_ref(pt, entry.RightFields()));
		for (const auto& v : data) {
			const auto pl = createPayload(v);
			if (!pl.Value()->IsFree()) {
				set.insert(*pl.Value());
			}
		}
		res.reserve(set.size());
		for (auto& s : set) {
			res.emplace_back(std::move(s));
		}
	} else {
		tsl::sparse_set<Variant> set(data.Size());
		VariantArray values;
		for (const auto& val : data) {
			const auto pl = createPayload(val);
			if (pl.Value()->IsFree()) {
				continue;
			}
			pl.GetByFieldsSet(entry.RightFields(), values, entry.RightFieldType(), entry.RightCompositeFieldsTypes());
			if (!leftFieldType.Is<KeyValueType::Undefined>() && !leftFieldType.Is<KeyValueType::Composite>()) {
				for (Variant& v : values) {
					if (!v.IsNullValue()) {
						set.insert(std::move(v.convert(leftFieldType)));
					}
				}
			} else {
				for (Variant& v : values) {
					if (!v.IsNullValue()) {
						set.insert(std::move(v));
					}
				}
			}
		}
		res.reserve(set.size() + res.size());
		for (auto& s : set) {
			res.emplace_back(std::move(s));
		}
	}
	return res;
}

VariantArray ItemsProcessor::readValuesFromPreSelect(const QueryJoinEntry& entry) const {
	const PreSelect::Values& values = std::get<PreSelect::Values>(PreSelectResults().payload);
	return readValuesOfRightNsFrom(
		values, [&values](const auto& it) noexcept { return ConstPayload{values.payloadType, it.GetItemRef().Value()}; }, entry,
		values.payloadType);
}

PreSelect::ValuesOptimizationStatus ItemsProcessor::isValuesOptimizationEnabled(ConstQueryImpl jItemQ, const NamespaceImpl::Ptr& jns,
																				ConstQueryImpl mainQ) {
	auto status = PreSelect::ValuesOptimizationStatus::Enabled;
	if (!jItemQ.JoinQueries().empty()) {
		// PreSelect::Values cannot evaluate JoinQueryEntry, we need other paths to be able
		// to call SelectIteratorContainer::Process (which already handles JOIN logic).
		return PreSelect::ValuesOptimizationStatus::DisabledByNestedJoin;
	}
	jItemQ.Entries().VisitForEach(
		[](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, SubQueryFunctionEntry> auto&) { assertrx_throw(0); },
		Skip<JoinQueryEntry, QueryEntriesBracket, AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry, QueryFunctionEntry, KnnQueryEntry>{},
		[&status](const QueryArithmeticEntry&) noexcept { status = PreSelect::ValuesOptimizationStatus::DisabledByArithmeticExpression; },
		[&jns, &status](const QueryEntry& qe) {
			if (qe.IsFieldIndexed()) {
				assertrx_throw(jns->indexes().size() > static_cast<size_t>(qe.IndexNo()));
				const IndexType indexType = jns->indexes()[qe.IndexNo()]->Type();
				if (IsComposite(indexType)) {
					status = PreSelect::ValuesOptimizationStatus::DisabledByCompositeIndex;
				}
			}
		},
		[&jns, &status](const BetweenFieldsQueryEntry& qe) {
			if (qe.IsLeftFieldIndexed()) {
				assertrx_throw(jns->indexes().size() > static_cast<size_t>(qe.LeftIdxNo()));
				const IndexType indexType = jns->indexes()[qe.LeftIdxNo()]->Type();
				if (IsComposite(indexType)) {
					status = PreSelect::ValuesOptimizationStatus::DisabledByCompositeIndex;
				}
			}
			if (qe.IsRightFieldIndexed()) {
				assertrx_throw(jns->indexes().size() > static_cast<size_t>(qe.RightIdxNo()));
				if (IsComposite(jns->indexes()[qe.RightIdxNo()]->Type())) {
					status = PreSelect::ValuesOptimizationStatus::DisabledByCompositeIndex;
				}
			}
		});
	if (status == PreSelect::ValuesOptimizationStatus::Enabled) {
		for (const auto& se : mainQ.GetSortingEntries()) {
			if (IsSortedByJoinedField(se.expression, jItemQ.NsName())) {
				return PreSelect::ValuesOptimizationStatus::DisabledByJoinedFieldSort;	// TODO maybe allow #1410
			}
		}
	}
	return status;
}

template std::vector<ItemsProcessor> ItemsProcessor::BuildForQuery<RxSelector::NsLocker<const RdxContext>>(
	int, ConstQueryImpl, LocalQueryResults&, RxSelector::NsLocker<const RdxContext>&, FtFunctionsHolder&, std::vector<QueryResultsContext>&,
	IsModifyQuery isModifyQuery, const RdxContext&);

template std::vector<ItemsProcessor> ItemsProcessor::BuildForQuery<RxSelector::NsLockerW>(int, ConstQueryImpl, LocalQueryResults&,
																						  RxSelector::NsLockerW&, FtFunctionsHolder&,
																						  std::vector<QueryResultsContext>&,
																						  IsModifyQuery isModifyQuery, const RdxContext&);

}  // namespace reindexer::joins
