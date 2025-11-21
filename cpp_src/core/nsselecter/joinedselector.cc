#include "joinedselector.h"

#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "estl/algorithm.h"
#include "nsselecter.h"
#include "vendor/sparse-map/sparse_set.h"

constexpr size_t kMaxIterationsScaleForInnerJoinOptimization = 100;

namespace reindexer {

void JoinedSelector::selectFromRightNs(LocalQueryResults& joinItemR, const Query& query, FloatVectorsHolderMap* floatVectorsHolder,
									   bool& found, bool& matchedAtLeastOnce) {
	assertrx_dbg(rightNs_);

	JoinCacheRes joinResLong;
	rightNs_->getFromJoinCache(query, joinQuery_, joinResLong);

	rightNs_->getInsideFromJoinCache(joinRes_);
	if (joinRes_.needPut) {
		rightNs_->putToJoinCache(joinRes_, preSelectCtx_.ResultPtr());
	}
	if (joinResLong.haveData) {
		found = joinResLong.it.val.ids->size();
		matchedAtLeastOnce = joinResLong.it.val.matchedAtLeastOnce;
		rightNs_->FillResult(joinItemR, *joinResLong.it.val.ids);
	} else {
		SelectCtxWithJoinPreSelect<JoinPreResultExecuteCtx> ctx(query, nullptr, preSelectCtx_, floatVectorsHolder);
		ctx.matchedAtLeastOnce = false;
		ctx.reqMatchedOnceFlag = true;
		ctx.skipIndexesLookup = true;
		ctx.functions = &selectFunctions_;
		rightNs_->Select(joinItemR, ctx, rdxCtx_);
		if (query.NeedExplain()) {
			explainOneSelect_ = std::move(joinItemR.explainResults);
			joinItemR.explainResults = {};
		}

		found = joinItemR.Count();
		matchedAtLeastOnce = ctx.matchedAtLeastOnce;
	}
	if (joinResLong.needPut) {
		JoinCacheVal val;
		val.ids = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
		val.matchedAtLeastOnce = matchedAtLeastOnce;
		for (const auto& it : joinItemR.Items()) {
			std::ignore = val.ids->Add(it.GetItemRef().Id(), IdSet::Unordered, 0);
		}
		rightNs_->putToJoinCache(joinResLong, std::move(val));
	}
}

void JoinedSelector::selectFromPreResultValues(LocalQueryResults& joinItemR, const Query& query, bool& found,
											   bool& matchedAtLeastOnce) const {
	size_t matched = 0;
	const auto& entries = query.Entries();
	const JoinPreResult::Values& values = std::get<JoinPreResult::Values>(PreResult().payload);
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

bool JoinedSelector::Process(IdType rowId, int nsId, ConstPayload payload, FloatVectorsHolderMap* floatVectorsHolder,
							 bool withJoinedItems) {
	++called_;
	if (optimized_ && !withJoinedItems) {
		matched_++;
		return true;
	}

	const auto startTime = ExplainCalc::Clock::now();
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
	std::visit(
		overloaded{[&](const JoinPreResult::Values&) { selectFromPreResultValues(joinItemR, *itemQueryPtr, found, matchedAtLeastOnce); },
				   [&]<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) {
					   selectFromRightNs(joinItemR, *itemQueryPtr, floatVectorsHolder, found, matchedAtLeastOnce);
				   }},
		PreResult().payload);
	if (withJoinedItems && found) {
		assertrx_throw(nsId < static_cast<int>(result_.joined_.size()));
		joins::NamespaceResults& nsJoinRes = result_.joined_[nsId];
		assertrx_dbg(nsJoinRes.GetJoinedSelectorsCount());
		if (floatVectorsHolder) {
			std::visit(overloaded{[&](const JoinPreResult::Values&) noexcept {},
								  [&]<concepts::OneOf<IdSet, SelectIteratorContainer> T>(const T&) {
									  floatVectorsHolder->Add(*RightNs(), joinItemR.begin(), joinItemR.end(), fieldsFilter_);
								  }},
					   PreResult().payload);
		}
		nsJoinRes.Insert(rowId, joinedFieldIdx_, std::move(joinItemR));
	}
	if (matchedAtLeastOnce) {
		++matched_;
	}
	selectTime_ += (ExplainCalc::Clock::now() - startTime);
	return matchedAtLeastOnce;
}

template <typename Cont, typename Fn>
VariantArray JoinedSelector::readValuesOfRightNsFrom(const Cont& data, const Fn& createPayload, const QueryJoinEntry& entry,
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

VariantArray JoinedSelector::readValuesFromPreResult(const QueryJoinEntry& entry) const {
	const JoinPreResult::Values& values = std::get<JoinPreResult::Values>(PreResult().payload);
	return readValuesOfRightNsFrom(
		values, [&values](const auto& it) noexcept { return ConstPayload{values.payloadType, it.GetItemRef().Value()}; }, entry,
		values.payloadType);
}

void JoinedSelector::AppendSelectIteratorOfJoinIndexData(SelectIteratorContainer& iterators, int* maxIterations, unsigned sortId,
														 const FtFunction::Ptr& ftFunc, const RdxContext& rdxCtx) {
	assertrx_throw(!ftFunc || ftFunc->Empty());
	(void)ftFunc;

	const auto& preresult = PreResult();
	if (joinType_ != JoinType::InnerJoin || preSelectCtx_.Mode() != JoinPreSelectMode::Execute ||
		std::visit(overloaded{[](const SelectIteratorContainer&) { return true; },
							  [maxIterations]<concepts::OneOf<IdSet, JoinPreResult::Values> T>(const T& v) {
								  return v.Size() > *maxIterations * kMaxIterationsScaleForInnerJoinOptimization;
							  }},
				   preresult.payload)) {
		return;
	}

	unsigned optimized = 0;
	assertrx_throw(!std::holds_alternative<JoinPreResult::Values>(preresult.payload) ||
				   itemQuery_.Entries().Size() == joinQuery_.joinEntries_.size());
	for (size_t i = 0; i < joinQuery_.joinEntries_.size(); ++i) {
		const QueryJoinEntry& joinEntry = joinQuery_.joinEntries_[i];
		if (!joinEntry.IsLeftFieldIndexed() || joinEntry.Operation() != OpAnd ||
			(joinEntry.Condition() != CondEq && joinEntry.Condition() != CondSet) ||
			(i + 1 < joinQuery_.joinEntries_.size() && joinQuery_.joinEntries_[i + 1].Operation() == OpOr)) {
			continue;
		}
		const auto& leftIndex = leftNs_->indexes_[joinEntry.LeftIdxNo()];
		if (IsFullText(leftIndex->Type())) {
			continue;
		}

		// Avoiding to use 'GetByJsonPath' during values extraction
		// TODO: Sometimes this substitution may be effective even with 'GetByJsonPath', so we should allow user to hint this
		// optimization.
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
		const VariantArray values =
			std::visit(overloaded{[&](const IdSet& preselected) {
									  const std::vector<IdType>* sortOrders = nullptr;
									  if (preresult.sortOrder.index) {
										  sortOrders = &(preresult.sortOrder.index->SortOrders());
									  }
									  return readValuesOfRightNsFrom(
										  preselected,
										  [this, sortOrders](IdType rowId) noexcept {
											  const auto properRowId = sortOrders ? (*sortOrders)[rowId] : rowId;
											  return ConstPayload{rightNs_->payloadType_, rightNs_->items_[properRowId]};
										  },
										  joinEntry, rightNs_->payloadType_);
								  },
								  [&](const JoinPreResult::Values&) { return readValuesFromPreResult(joinEntry); },
								  [](const SelectIteratorContainer&) -> VariantArray { throw_as_assert; }},
					   preresult.payload);

		if (leftIndex->Opts().GetCollateMode() == CollateUTF8) {
			for (auto& key : values) {
				key.EnsureUTF8();
			}
		}
		Index::SelectContext selectContext;
		selectContext.opts.maxIterations = iterators.GetMaxIterations();
		selectContext.opts.indexesNotOptimized = !leftNs_->SortOrdersBuilt();
		selectContext.opts.inTransaction = inTransaction_;

		auto selectResults = leftIndex->SelectKey(values, CondSet, sortId, selectContext, rdxCtx);
		auto* selRes = std::get_if<SelectKeyResultsVector>(&selectResults);
		if (!selRes || selRes->empty()) {
			continue;
		}

		SelectIterator selIter{std::move(*selRes->begin()), IsDistinct_False, std::string(joinEntry.LeftFieldName()),
							   joinEntry.LeftIdxNo()};
		for (auto it = selRes->begin() + 1, end = selRes->end(); it != end; ++it) {
			selIter.Append(std::move(*it));
		}
		const int curIterations = selIter.GetMaxIterations();
		if (curIterations && curIterations < *maxIterations) {
			*maxIterations = curIterations;
		}
		std::ignore = iterators.Append(OpAnd, std::move(selIter));

		++optimized;
	}
	optimized_ = (optimized == joinQuery_.joinEntries_.size());
}

}  // namespace reindexer
