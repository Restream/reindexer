#include "items_processor.h"

#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/queryresults/context.h"
#include "core/queryresults/queryresults.h"
#include "core/reindexer_impl/rx_selector.h"
#include "estl/algorithm.h"
#include "estl/charset.h"
#include "helpers.h"
#include "queryresults.h"
#include "vendor/sparse-map/sparse_set.h"

using namespace reindexer;

namespace {
constexpr size_t kMaxIterationsScaleForInnerJoinOptimization = 100;

bool isSortedByJoinedField(std::string_view sortExpr, std::string_view joinedNs) {
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
}  // namespace

namespace reindexer::joins {

bool ItemsProcessor::Process(IdType rowId, int nsId, ConstPayload payload, FloatVectorsHolderMap* floatVectorsHolder,
							 bool withJoinedItems) {
	++called_;
	if (optimized_ && !withJoinedItems) {
		matched_++;
		return true;
	}

	const auto startTime = Explain::Clock::now();
	// Put values to join conditions
	size_t i = 0;
	if (itemQuery_.NeedExplain() && !explainOneSelect_.empty()) {
		itemQuery_.Explain(false);
	}
	std::unique_ptr<Query> itemQueryCopy;
	Query* itemQueryPtr = &itemQuery_;
	for (auto& je : joinQuery_.joinEntries_) {
		size_t changedCount = 1;

#ifdef RX_WITH_STDLIB_DEBUG
		const auto initialCond = itemQueryPtr->Entries().Get<QueryEntry>(i).Condition();
		const auto initialSize = itemQueryPtr->Entries().Size();
#endif	// RX_WITH_STDLIB_DEBUG

		payload.GetByFieldsSet(je.LeftFields(), tmpValues_, je.LeftFieldType(), je.LeftCompositeFieldsTypes());

		tmpValues_.erase(
			unstable_remove_if(tmpValues_.begin(), tmpValues_.end(), [](const Variant& v) noexcept { return v.IsNullValue(); }),
			tmpValues_.cend());
		if (tmpValues_.empty() || !itemQueryPtr->TryUpdateQueryEntryInplace(i, tmpValues_)) {
			if (itemQueryPtr == &itemQuery_) {
				itemQueryCopy = std::make_unique<Query>(itemQuery_);
				itemQueryPtr = itemQueryCopy.get();
			}
			if (tmpValues_.empty()) {
				changedCount = itemQueryPtr->SetEntry<AlwaysFalse>(i);
			} else {
				const QueryEntry& qentry = itemQueryPtr->Entries().Get<QueryEntry>(i);
				changedCount = itemQueryPtr->SetEntry<QueryEntry>(i, QueryEntry{qentry, qentry.Condition(), std::move(tmpValues_)});
				tmpValues_ = {};
			}
		}
#ifdef RX_WITH_STDLIB_DEBUG
		else {
			assertrx_dbg(initialCond == itemQueryPtr->Entries().Get<QueryEntry>(i).Condition());
			assertrx_dbg(initialSize == itemQueryPtr->Entries().Size());
		}
#endif	// RX_WITH_STDLIB_DEBUG

		i += changedCount;
	}
	itemQueryPtr->Limit((withJoinedItems && !limit0_) ? joinQuery_.Limit() : 0);

	bool found = false;
	bool matchedAtLeastOnce = false;
	LocalQueryResults joinItemR;
	std::visit(overloaded{[&](const PreSelect::Values&) { selectFromPreSelectValues(joinItemR, *itemQueryPtr, found, matchedAtLeastOnce); },
						  [&]<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {
							  selectFromRightNs(joinItemR, *itemQueryPtr, floatVectorsHolder, found, matchedAtLeastOnce);
						  }},
			   PreSelectResults().payload);
	if (withJoinedItems && found) {
		assertrx_throw(nsId < static_cast<int>(result_.joined_.size()));
		joins::NamespaceResults& nsJoinRes = result_.joined_[nsId];
		assertrx_dbg(nsJoinRes.GetJoinItemsProcessorsCount());
		if (floatVectorsHolder) {
			std::visit(overloaded{[&](const PreSelect::Values&) noexcept {},
								  [&]<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {
									  floatVectorsHolder->Add(*RightNs(), joinItemR.begin(), joinItemR.end(), fieldsFilter_);
								  }},
					   PreSelectResults().payload);
		}
		nsJoinRes.Insert(rowId, joinedFieldIdx_, std::move(joinItemR));
	}
	if (matchedAtLeastOnce) {
		++matched_;
	}
	selectTime_ += (Explain::Clock::now() - startTime);
	return matchedAtLeastOnce;
}

void ItemsProcessor::BuildSelectIteratorsOfIndexedFields(int* maxIterations, unsigned sortId, const FtFunction::Ptr& ftFunc,
														 const RdxContext& rdxCtx, SelectIteratorContainer& iterators) {
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
	assertrx_throw(!std::holds_alternative<PreSelect::Values>(preselect.payload) ||
				   itemQuery_.Entries().Size() == joinQuery_.joinEntries_.size());
	for (size_t i = 0; i < joinQuery_.joinEntries_.size(); ++i) {
		const QueryJoinEntry& joinEntry = joinQuery_.joinEntries_[i];
		if (!joinEntry.IsLeftFieldIndexed() || joinEntry.Operation() != OpAnd ||
			(joinEntry.Condition() != CondEq && joinEntry.Condition() != CondSet) ||
			(i + 1 < joinQuery_.joinEntries_.size() && joinQuery_.joinEntries_[i + 1].Operation() == OpOr)) {
			continue;
		}
		const auto& leftIndex = leftNs_->indexes_[joinEntry.LeftIdxNo()];
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

		VariantArray values =
			std::visit(overloaded{[&](const IdSetPlain& preselected) {
									  const std::vector<IdType>* sortOrders = nullptr;
									  if (preselect.sortOrder.index) {
										  sortOrders = &(preselect.sortOrder.index->SortOrders());
									  }
									  return readValuesOfRightNsFrom(
										  preselected,
										  [this, sortOrders](IdType rowId) noexcept {
											  const auto properRowId = sortOrders ? (*sortOrders)[rowId.ToNumber()] : rowId;
											  return ConstPayload{rightNs_->payloadType_, rightNs_->items_[properRowId]};
										  },
										  joinEntry, rightNs_->payloadType_);
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
		selectContext.opts.maxIterations = iterators.GetMaxIterations();
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
		const int curIterations = selectIterator.GetMaxIterations();
		if (curIterations && curIterations < *maxIterations) {
			*maxIterations = curIterations;
		}
		std::ignore = iterators.Append(OpAnd, std::move(selectIterator));
		++optimized;
	}
	optimized_ = (optimized == joinQuery_.joinEntries_.size());
}

template <typename LockerType>
std::vector<ItemsProcessor> ItemsProcessor::BuildForQuery(const Query& q, LocalQueryResults& result, LockerType& locks,
														  FtFunctionsHolder& func, std::vector<QueryResultsContext>* queryResultsContexts,
														  IsModifyQuery isModifyQuery, const RdxContext& rdxCtx) {
	ItemsProcessors joinItemsProcessors;
	if (q.GetJoinQueries().empty()) {
		return joinItemsProcessors;
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

		if (jq.joinEntries_.empty()) [[unlikely]] {
			throw Error{errQueryExec, "Join without ON conditions"};
		}

		if (jq.joinEntries_.front().Operation() == OpOr) [[unlikely]] {
			throw Error{errQueryExec, "OR operator in first condition or after left join"};
		}

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
		const uint32_t joinedFieldIdx = uint32_t(joinItemsProcessors.size());
		if (jq.joinType == InnerJoin || jq.joinType == OrInnerJoin) {
			jjq.InsertConditionsFromOnConditions<JoinConditionInsertionDirection::FromMain>(jjq.Entries().Size(), jq.joinEntries_,
																							q.Entries(), i, &ns->indexes_);
		}
		jjq.Offset(QueryEntry::kDefaultOffset);
		jjq.Limit(QueryEntry::kDefaultLimit);
		CacheRes joinRes;
		if (!jjq.NeedExplain()) {
			jns->getFromJoinCache(jjq, joinRes);
		}
		joins::PreSelect::CPtr preSelect;
		if (joinRes.haveData) {
			preSelect = std::move(joinRes.it.val.preSelect);
		} else {
			JoinPreSelectCtx ctx(jjq, &q, joins::PreSelectBuildCtx{std::make_shared<joins::PreSelect>()}, &result.GetFloatVectorsHolder());
			ctx.preSelect.Result().storedValuesOptStatus = ItemsProcessor::isPreSelectValuesOptimizationEnabled(jItemQ, jns, q);
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
			if (joinRes.needPut) {
				jns->putToJoinCache(joinRes, preSelect);
			}
		}

		const auto nsUpdateTime = jns->lastUpdateTimeNano();
		if (!isModifyQuery && jItemQ.Limit() != 0) {
			// Namespace data do not required if no documents will actually be joined
			result.AddNamespace(jns, true);
		}
		if (queryResultsContexts) {
			queryResultsContexts->emplace_back(jns->payloadType_, jns->tagsMatcher_, FieldsFilter{jq.SelectFilters(), *jns}, jns->schema_,
											   jns->incarnationTag_);
		}

		std::visit(overloaded{[&](const joins::PreSelect::Values&) {
								  locks.Delete(jns);
								  jns.reset();
							  },
							  []<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {}},
				   preSelect->payload);
		joinItemsProcessors.emplace_back(jq.joinType, ns, std::move(jns), std::move(joinRes), std::move(jItemQ),
										 FieldsFilter{jq.SelectFilters(), *ns}, result, jq, joins::PreSelectExecuteCtx{preSelect},
										 joinedFieldIdx, func, false, nsUpdateTime,
										 isModifyQuery ? SetLimit0ForChangeJoin_True : SetLimit0ForChangeJoin_False, rdxCtx);
		ThrowOnCancel(rdxCtx);
	}
	return joinItemsProcessors;
}

void ItemsProcessor::selectFromRightNs(LocalQueryResults& joinItemR, const Query& query, FloatVectorsHolderMap* floatVectorsHolder,
									   bool& found, bool& matchedAtLeastOnce) {
	assertrx_dbg(rightNs_);

	CacheRes joinResLong;
	rightNs_->getFromJoinCache(query, joinQuery_, joinResLong);

	rightNs_->getInsideFromJoinCache(joinRes_);
	if (joinRes_.needPut) {
		rightNs_->putToJoinCache(joinRes_, preSelectCtx_.ResultPtr());
	}
	if (joinResLong.haveData) {
		found = !joinResLong.it.val.ids->IsEmpty();
		matchedAtLeastOnce = joinResLong.it.val.matchedAtLeastOnce;
		rightNs_->FillResult(joinItemR, *joinResLong.it.val.ids);
	} else {
		Explain explain;
		JoinSelectCtx ctx(query, nullptr, preSelectCtx_, floatVectorsHolder);
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

void ItemsProcessor::selectFromPreSelectValues(LocalQueryResults& joinItemR, const Query& query, bool& found,
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

StoredValuesOptimizationStatus ItemsProcessor::isPreSelectValuesOptimizationEnabled(const Query& jItemQ, const NamespaceImpl::Ptr& jns,
																					const Query& mainQ) {
	auto status = StoredValuesOptimizationStatus::Enabled;
	jItemQ.Entries().VisitForEach(
		[](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, SubQueryFunctionEntry> auto&) { assertrx_throw(0); },
		Skip<JoinQueryEntry, QueryEntriesBracket, AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry, QueryFunctionEntry>{},
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
			if (isSortedByJoinedField(se.expression, jItemQ.NsName())) {
				return StoredValuesOptimizationStatus::DisabledByJoinedFieldSort;  // TODO maybe allow #1410
			}
		}
	}
	return status;
}

template std::vector<ItemsProcessor>
reindexer::joins::ItemsProcessor::BuildForQuery<reindexer::RxSelector::NsLocker<const reindexer::RdxContext>>(
	const reindexer::Query&, reindexer::LocalQueryResults&, reindexer::RxSelector::NsLocker<const reindexer::RdxContext>&,
	reindexer::FtFunctionsHolder&, std::vector<reindexer::QueryResultsContext>*, reindexer::IsModifyQuery, const reindexer::RdxContext&);

template std::vector<ItemsProcessor> reindexer::joins::ItemsProcessor::BuildForQuery<reindexer::RxSelector::NsLockerW>(
	const reindexer::Query&, reindexer::LocalQueryResults&, reindexer::RxSelector::NsLockerW&, reindexer::FtFunctionsHolder&,
	std::vector<reindexer::QueryResultsContext>*, reindexer::IsModifyQuery, const reindexer::RdxContext&);

}  // namespace reindexer::joins
