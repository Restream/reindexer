#pragma once

#include "core/nsselecter/explaincalc.h"
#include "core/nsselecter/joins/cache.h"
#include "core/nsselecter/joins/query_joins_table.h"
#include "core/queryresults/fields_filter.h"
#include "preselect.h"

namespace reindexer {

class SortExpression;
class NsSelecter;
class QueryPreprocessor;
struct QueryResultsContext;

namespace SortExprFuncs {
struct DistanceBetweenJoinedIndexesSameNs;
}  // namespace SortExprFuncs

namespace joins {

class ItemsProcessor;
using ItemsProcessors = std::vector<ItemsProcessor>;

class [[nodiscard]] ItemsProcessor {
	friend SortExpression;
	friend SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	friend NsSelecter;
	friend QueryPreprocessor;

public:
	ItemsProcessor(JoinType joinType, NamespaceImpl::Ptr leftNs, NamespaceImpl::Ptr rightNs, const QueryResultsContext* rightNsCtx,
				   int leftNsId, int rightNsId, CacheRes&& joinRes, Query&& itemQuery, FieldsFilter fieldsFilter, LocalQueryResults& result,
				   const JoinedQuery& joinQuery, size_t joinEntriesStart, PreSelectExecuteCtx&& preSelCtx, uint16_t joinedFieldIdx,
				   ItemsProcessors&& childItemsProcessors, FtFunctionsHolder& selectFunctions, bool inTransaction, int64_t lastUpdateTime,
				   SetLimit0ForChangeJoin limit0, const RdxContext& rdxCtx)
		: joinType_(joinType),
		  called_(0),
		  matched_(0),
		  leftNs_(std::move(leftNs)),
		  rightNs_(std::move(rightNs)),
		  rightNsCtx_(rightNsCtx),
		  leftNsId_(leftNsId),
		  rightNsId_(rightNsId),
		  joinRes_(std::move(joinRes)),
		  itemQuery_(std::move(itemQuery)),
		  fieldsFilter_(std::move(fieldsFilter)),
		  result_(result),
		  joinQuery_(joinQuery),
		  joinEntriesStart_(joinEntriesStart),
		  preSelectCtx_(std::move(preSelCtx)),
		  joinedFieldIdx_(joinedFieldIdx),
		  selectFunctions_(selectFunctions),
		  rdxCtx_(rdxCtx),
		  optimized_(false),
		  inTransaction_{inTransaction},
		  lastUpdateTime_{lastUpdateTime},
		  limit0_(limit0),
		  childItemsProcessors_{std::move(childItemsProcessors)} {
#ifndef NDEBUG
		for (const auto& jqe : joinQuery_.joinEntries_) {
			assertrx_throw(jqe.FieldsHaveBeenSet());
		}
#endif
	}

	template <typename Locker>
	static ItemsProcessors BuildForQuery(int nsid, const Query& q, LocalQueryResults&, Locker&, FtFunctionsHolder&,
										 std::vector<QueryResultsContext>&, IsModifyQuery isModifyQuery, const RdxContext&);

	ItemsProcessor(ItemsProcessor&&) = default;
	ItemsProcessor& operator=(ItemsProcessor&&) = delete;
	ItemsProcessor(const ItemsProcessor&) = delete;
	ItemsProcessor& operator=(const ItemsProcessor&) = delete;

	bool Process(IdType rowId, int nsId, ConstPayload pv, FloatVectorsHolderMap*, bool withJoinedItems);
	void BuildSelectIteratorsOfIndexedFields(int* maxIterations, unsigned sortId, const FtFunction::Ptr&, const RdxContext&,
											 SelectIteratorContainer& dst);

	JoinType Type() const noexcept { return joinType_; }
	void SetType(JoinType type) noexcept { joinType_ = type; }
	const std::string& RightNsName() const noexcept { return itemQuery_.NsName(); }
	int64_t LastUpdateTime() const noexcept { return lastUpdateTime_; }
	const JoinedQuery& JoinQuery() const noexcept { return joinQuery_; }
	int LeftNsId() const { return leftNsId_; }
	size_t JoinEntryIndex(size_t joinEntry) const noexcept { return joinEntriesStart_ + joinEntry; }
	int Called() const noexcept { return called_; }
	int Matched(bool invert) const noexcept {
		assertrx_dbg(called_ >= matched_);
		return invert ? (called_ - matched_) : matched_;
	}
	const PreSelect& PreSelectResults() const& noexcept { return preSelectCtx_.Result(); }
	const PreSelect::CPtr& PreSelectResultPtr() const& noexcept { return preSelectCtx_.ResultPtr(); }
	PreSelectMode PreSelectStrategy() const noexcept { return preSelectCtx_.Mode(); }
	const NamespaceImpl::Ptr& RightNs() const noexcept { return rightNs_; }
	Explain::Duration SelectTime() const noexcept { return selectTime_; }
	const std::string& ExplainOneSelect() const& noexcept { return explainOneSelect_; }
	std::span<const ItemsProcessor> ChildItemsProcessors() const noexcept {
		return std::span<const ItemsProcessor>{childItemsProcessors_.data(), childItemsProcessors_.size()};
	}

	auto ExplainOneSelect() const&& = delete;
	auto PreSelectResults() const&& = delete;
	auto PreSelectResultPtr() const&& = delete;

private:
	std::span<ItemsProcessor> childItemsProcessors() {
		return std::span<ItemsProcessor>{childItemsProcessors_.data(), childItemsProcessors_.size()};
	}

	VariantArray readValuesFromPreSelect(const QueryJoinEntry&) const;
	template <typename Cont, typename Fn>
	VariantArray readValuesOfRightNsFrom(const Cont& from, const Fn& createPayload, const QueryJoinEntry&, const PayloadType&) const;
	void selectFromRightNs(LocalQueryResults& joinItemR, const Query&, FloatVectorsHolderMap*, bool& found, bool& matchedAtLeastOnce);
	void selectFromPreSelectValues(LocalQueryResults& joinItemR, const Query&, bool& found, bool& matchedAtLeastOnce) const;

	static PreSelect::ValuesOptimizationStatus isValuesOptimizationEnabled(const Query& jItemQ, const NamespaceImpl::Ptr& jns,
																		   const Query& mainQ);

	static joins::PreSelect::CPtr buildPreSelect(int nsid, const Query& query, size_t joinedField,
												 std::span<ItemsProcessor> itemsProcessors, PreSelect::ValuesOptimizationStatus,
												 const NamespaceImpl::Ptr& ns, const NamespaceImpl::Ptr& jns, LocalQueryResults&,
												 FtFunctionsHolder&, CacheRes&, const RdxContext&);

	template <typename Locker>
	static ItemsProcessor buildItemsProcessor(int nsid, const Query& query, size_t joinedField, LocalQueryResults&, Locker& locks,
											  FtFunctionsHolder&, std::vector<QueryResultsContext>&, IsModifyQuery isModifyQuery,
											  const RdxContext&, size_t depth);

	JoinType joinType_;
	int called_, matched_;
	NamespaceImpl::Ptr leftNs_;
	NamespaceImpl::Ptr rightNs_;
	const QueryResultsContext* rightNsCtx_;
	int leftNsId_;
	int rightNsId_;
	CacheRes joinRes_;
	Query itemQuery_;
	FieldsFilter fieldsFilter_;
	LocalQueryResults& result_;
	const JoinedQuery& joinQuery_;
	size_t joinEntriesStart_ = 0;
	PreSelectExecuteCtx preSelectCtx_;
	std::string explainOneSelect_;
	uint16_t joinedFieldIdx_;
	FtFunctionsHolder& selectFunctions_;
	const RdxContext& rdxCtx_;
	bool optimized_ = false;
	bool inTransaction_ = false;
	int64_t lastUpdateTime_ = 0;
	Explain::Duration selectTime_ = Explain::Duration::zero();
	SetLimit0ForChangeJoin limit0_ = SetLimit0ForChangeJoin_False;
	VariantArray tmpValues_;

	// For nested join queries.
	ItemsProcessors childItemsProcessors_;
};

}  // namespace joins
}  // namespace reindexer
