#pragma once

#include "core/nsselecter/explaincalc.h"
#include "core/nsselecter/joins/cache.h"
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

class [[nodiscard]] ItemsProcessor {
	friend SortExpression;
	friend SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	friend NsSelecter;
	friend QueryPreprocessor;

public:
	ItemsProcessor(JoinType joinType, NamespaceImpl::Ptr leftNs, NamespaceImpl::Ptr rightNs, CacheRes&& joinRes, Query&& itemQuery,
				   FieldsFilter fieldsFilter, LocalQueryResults& result, const JoinedQuery& joinQuery, PreSelectExecuteCtx&& preSelCtx,
				   uint32_t joinedFieldIdx, FtFunctionsHolder& selectFunctions, bool inTransaction, int64_t lastUpdateTime,
				   SetLimit0ForChangeJoin limit0, const RdxContext& rdxCtx)
		: joinType_(joinType),
		  called_(0),
		  matched_(0),
		  leftNs_(std::move(leftNs)),
		  rightNs_(std::move(rightNs)),
		  joinRes_(std::move(joinRes)),
		  itemQuery_(std::move(itemQuery)),
		  fieldsFilter_(std::move(fieldsFilter)),
		  result_(result),
		  joinQuery_(joinQuery),
		  preSelectCtx_(std::move(preSelCtx)),
		  joinedFieldIdx_(joinedFieldIdx),
		  selectFunctions_(selectFunctions),
		  rdxCtx_(rdxCtx),
		  optimized_(false),
		  inTransaction_{inTransaction},
		  lastUpdateTime_{lastUpdateTime},
		  limit0_(limit0) {
#ifndef NDEBUG
		for (const auto& jqe : joinQuery_.joinEntries_) {
			assertrx_throw(jqe.FieldsHaveBeenSet());
		}
#endif
	}

	template <typename Locker>
	static std::vector<ItemsProcessor> BuildForQuery(const Query& q, LocalQueryResults& result, Locker& locks, FtFunctionsHolder& func,
													 std::vector<QueryResultsContext>*, IsModifyQuery isModifyQuery,
													 const RdxContext& ctx);

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

	auto ExplainOneSelect() const&& = delete;
	auto PreSelectResults() const&& = delete;
	auto PreSelectResultPtr() const&& = delete;

private:
	VariantArray readValuesFromPreSelect(const QueryJoinEntry&) const;
	template <typename Cont, typename Fn>
	VariantArray readValuesOfRightNsFrom(const Cont& from, const Fn& createPayload, const QueryJoinEntry&, const PayloadType&) const;
	void selectFromRightNs(LocalQueryResults& joinItemR, const Query&, FloatVectorsHolderMap*, bool& found, bool& matchedAtLeastOnce);
	void selectFromPreSelectValues(LocalQueryResults& joinItemR, const Query&, bool& found, bool& matchedAtLeastOnce) const;

	static StoredValuesOptimizationStatus isPreSelectValuesOptimizationEnabled(const Query& jItemQ, const NamespaceImpl::Ptr& jns,
																			   const Query& mainQ);

	JoinType joinType_;
	int called_, matched_;
	NamespaceImpl::Ptr leftNs_;
	NamespaceImpl::Ptr rightNs_;
	CacheRes joinRes_;
	Query itemQuery_;
	FieldsFilter fieldsFilter_;
	LocalQueryResults& result_;
	const JoinedQuery& joinQuery_;
	PreSelectExecuteCtx preSelectCtx_;
	std::string explainOneSelect_;
	uint32_t joinedFieldIdx_;
	FtFunctionsHolder& selectFunctions_;
	const RdxContext& rdxCtx_;
	bool optimized_ = false;
	bool inTransaction_ = false;
	int64_t lastUpdateTime_ = 0;
	Explain::Duration selectTime_ = Explain::Duration::zero();
	SetLimit0ForChangeJoin limit0_ = SetLimit0ForChangeJoin_False;
	VariantArray tmpValues_;
};
using ItemsProcessors = std::vector<ItemsProcessor>;

}  // namespace joins
}  // namespace reindexer
