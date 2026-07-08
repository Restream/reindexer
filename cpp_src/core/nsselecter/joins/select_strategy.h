#pragma once

#include "core/nsselecter/joins/preselect.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/selectctx_traits.h"
#include "core/queryresults/localqueryresults.h"
#include "tools/logger.h"

namespace reindexer::joins {

template <typename SelectCtxT>
class [[nodiscard]] SelectStrategyContract {
public:
	explicit SelectStrategyContract(SelectCtxT& selectCtx, NamespaceImpl& ns) : selectCtx_{selectCtx}, ns_{ns} {}

	void InitializeSortOrders() {}
	void DisableBtreeIndexOptimization() {}
	void FetchPreSelectResults(SelectIteratorContainer&) {}
	void SetPreSelectValuesAfterSorting() {}
	bool BuildPreSelect(SelectIteratorContainer&, SortingEntries&, LogLevel) { return true; }
	bool ExecutePreSelect(int&) { return true; }
	void BuildSelectIteratorsOfIndexedFields(int&, IsRanked, SelectIteratorContainer&, const RdxContext&, const FtFunction::Ptr&) {}

protected:
	SelectCtxT& selectCtx_;
	NamespaceImpl& ns_;
};

template <>
class [[nodiscard]] SelectStrategy<JoinPreSelectCtx> : private SelectStrategyContract<JoinPreSelectCtx> {
	using Base = SelectStrategyContract<JoinPreSelectCtx>;

public:
	explicit SelectStrategy(JoinPreSelectCtx& selectCtx, NamespaceImpl& ns) : SelectStrategyContract(selectCtx, ns) {}

	using Base::FetchPreSelectResults;
	using Base::BuildSelectIteratorsOfIndexedFields;
	using Base::ExecutePreSelect;

	RX_ALWAYS_INLINE void InitializeSortOrders() {
		// All further queries to this Join MUST have the same enableSortOrders flag.
		selectCtx_.preSelect.Result().enableSortOrders = selectCtx_.sortingContext.enableSortOrders;
	}

	RX_ALWAYS_INLINE void DisableBtreeIndexOptimization() { selectCtx_.preSelect.Result().btreeIndexOptimizationEnabled = false; }

	bool BuildPreSelect(SelectIteratorContainer& iterators, SortingEntries& sortBy, LogLevel logLevel) {
		auto& preSelect = selectCtx_.preSelect.Result();
		if (auto sortFieldEntry = selectCtx_.sortingContext.sortFieldEntryIfOrdered(); sortFieldEntry) {
			preSelect.sortOrder = PreSelect::SortOrderContext{.index = sortFieldEntry->index, .sortingEntry = sortFieldEntry->data};
		}
		preSelect.properties.emplace(std::min(static_cast<int64_t>(iterators.GetMaxIterations()),
											  static_cast<int64_t>(NsSelecter::GetMaxScanIterations(ns_, selectCtx_.sortingContext))),
									 ns_.config().maxIterationsIdSetPreSelect);
		auto& preselectProps = preSelect.properties.value();
		assertrx_throw(preselectProps.maxIterationsIdSetPreSelect > PreSelect::MaxIterationsForValuesOptimization);
		if ((preSelect.storedValuesOptStatus == StoredValuesOptimizationStatus::Enabled) &&
			preselectProps.qresMaxIterations <= PreSelect::MaxIterationsForValuesOptimization) {
			preSelect.payload.template emplace<PreSelect::Values>(ns_.payloadType_, ns_.tagsMatcher_);
		} else {
			preselectProps.isLimitExceeded = (preselectProps.qresMaxIterations >= preselectProps.maxIterationsIdSetPreSelect);
			preselectProps.isUnorderedIndexSort =
				!preselectProps.isLimitExceeded && (selectCtx_.sortingContext.entries.size() && !selectCtx_.sortingContext.sortIndex());
			preselectProps.btreeIndexOptimizationEnabled = preSelect.btreeIndexOptimizationEnabled;
			// Return pre-result as QueryIterators if:
			if (preselectProps.isLimitExceeded ||		// 1. We have > QueryIterator which expects more than configured max iterations.
				preselectProps.isUnorderedIndexSort ||	// 2. We have sorted query, by unordered index
				preselectProps.btreeIndexOptimizationEnabled) {	 // 3. We have btree-index that is not committed yet
				preSelect.payload.template emplace<SelectIteratorContainer>();
				std::get<SelectIteratorContainer>(preSelect.payload).Append(iterators.cbegin(), iterators.cend());
				if (logLevel >= LogInfo) [[unlikely]] {
					logFmt(LogInfo, "Built preResult (expected {} iterations) with {} iterators, q='{}'", preselectProps.qresMaxIterations,
						   iterators.Size(), selectCtx_.query.GetSQL());
				}
				return false;
			} else {  // Build pre-result as single IdSet
				preSelect.payload.template emplace<IdSetPlain>();
				// For building join pre-result always use ASC sort orders
				for (SortingEntry& se : sortBy) {
					se.desc = Desc_False;
				}
			}
		}
		return true;
	}

	RX_ALWAYS_INLINE void SetPreSelectValuesAfterSorting() {
		std::visit(overloaded{[&](PreSelect::Values& values) {
								  for (const auto& it : values) {
									  ItemRef& iref = it.GetItemRef();
									  if (!iref.ValueInitialized()) {
										  iref.SetValue(ns_.items_[iref.Id()]);
									  }
								  }
							  },
							  []<concepts::OneOf<IdSetPlain, SelectIteratorContainer> T>(const T&) {}},
				   selectCtx_.preSelect.Result().payload);
	}
};

template <>
class [[nodiscard]] SelectStrategy<JoinSelectCtx> : private SelectStrategyContract<JoinSelectCtx> {
	using Base = SelectStrategyContract<JoinSelectCtx>;

public:
	explicit SelectStrategy(JoinSelectCtx& selectCtx, NamespaceImpl& ns) : SelectStrategyContract(selectCtx, ns) {}

	using Base::DisableBtreeIndexOptimization;
	using Base::SetPreSelectValuesAfterSorting;
	using Base::BuildSelectIteratorsOfIndexedFields;
	using Base::BuildPreSelect;

	RX_ALWAYS_INLINE void InitializeSortOrders() {
		// If sort orders are disabled in the current join query,
		// then the appropriate flag in preSelect query should also be disabled.
		// If this assertion fails, it's possible namespace is unlocked or
		// ns->sortOrdersFlag_ was reset under read lock!
		assertrx_throw(selectCtx_.sortingContext.enableSortOrders || !selectCtx_.preSelect.Result().enableSortOrders);
		selectCtx_.sortingContext.enableSortOrders = selectCtx_.preSelect.Result().enableSortOrders;
	}

	RX_ALWAYS_INLINE void FetchPreSelectResults(SelectIteratorContainer& dst) {
		assertrx_throw(selectCtx_.preSelect.Mode() == PreSelectMode::Execute || selectCtx_.preSelect.Mode() == PreSelectMode::ForInsertion);
		std::visit(overloaded{[&](const IdSetPlain& ids) {
								  SelectKeyResult res;
								  res.emplace_back(ids);
								  // Iterator Field Kind: Preselect IdSet -> None
								  std::ignore = dst.Append(
									  OpAnd, SelectIterator(std::move(res), IsDistinct_False, "-preresult", IndexValueType::NotSet));
							  },
							  [&](const SelectIteratorContainer& iterators) {
								  if (selectCtx_.preSelect.Mode() == PreSelectMode::Execute) {
									  dst.Append(iterators.begin(), iterators.end());
								  }
							  },
							  [](const PreSelect::Values&) { throw_as_assert; }},
				   selectCtx_.preSelect.Result().payload);
	}

	RX_ALWAYS_INLINE bool ExecutePreSelect(int& maxIterations) {
		// Main sorting index must be the same during join preselect build and execution
		assertrx_throw(selectCtx_.preSelect.Result().sortOrder.index == selectCtx_.sortingContext.sortIndexIfOrdered());
		if (selectCtx_.preSelect.Mode() == PreSelectMode::ForInsertion &&
			maxIterations > long(selectCtx_.preSelect.MainQueryMaxIterations()) * selectCtx_.preSelect.MainQueryMaxIterations()) {
			selectCtx_.preSelect.Reject();
			return false;
		}
		return true;
	}
};

template <>
class [[nodiscard]] SelectStrategy<MainSelectCtx> : private SelectStrategyContract<MainSelectCtx> {
	using Base = SelectStrategyContract<MainSelectCtx>;

public:
	explicit SelectStrategy(MainSelectCtx& selectCtx, NamespaceImpl& ns) : SelectStrategyContract(selectCtx, ns) {}

	using Base::InitializeSortOrders;
	using Base::DisableBtreeIndexOptimization;
	using Base::FetchPreSelectResults;
	using Base::SetPreSelectValuesAfterSorting;
	using Base::BuildPreSelect;
	using Base::ExecutePreSelect;

	RX_ALWAYS_INLINE void BuildSelectIteratorsOfIndexedFields(int& maxIterations, IsRanked isRanked, SelectIteratorContainer& iterators,
															  const RdxContext& rdxCtx, const FtFunction::Ptr& ftFunc) {
		if (!selectCtx_.sortingContext.isOptimizationEnabled() && !isRanked && maxIterations > kMinIterationsForInnerJoinOptimization) {
			for (size_t i = 0, size = iterators.Size(); i < size; i = iterators.Next(i)) {
				// for optimization use only isolated InnerJoin
				if (iterators.GetOperation(i) == OpAnd && iterators.IsJoinIterator(i) &&
					(iterators.Next(i) >= size || iterators.GetOperation(iterators.Next(i)) != OpOr)) {
					const JoinSelectIterator& it = iterators.Get<JoinSelectIterator>(i);
					assertrx_throw(selectCtx_.joinItemsProcessors && selectCtx_.joinItemsProcessors->size() > it.joinIndex);
					ItemsProcessor& js = (*selectCtx_.joinItemsProcessors)[it.joinIndex];
					js.BuildSelectIteratorsOfIndexedFields(&maxIterations, selectCtx_.sortingContext.sortId(), ftFunc, rdxCtx, iterators);
				}
			}
		}
	}

private:
	static constexpr int kMinIterationsForInnerJoinOptimization = 100;
};

}  // namespace reindexer::joins
