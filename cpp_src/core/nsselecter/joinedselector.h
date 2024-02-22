#pragma once
#include "core/joincache.h"
#include "core/namespace/namespaceimpl.h"
#include "explaincalc.h"
#include "selectiteratorcontainer.h"

namespace reindexer {

struct JoinPreResult {
	class Values : public std::vector<ItemRef> {
	public:
		Values(Values &&other)
			: std::vector<ItemRef>(std::move(other)),
			  payloadType(std::move(other.payloadType)),
			  tagsMatcher(std::move(other.tagsMatcher)),
			  locked_(other.locked_) {
			other.locked_ = false;
		}
		Values() : locked_(false) {}
		Values(const Values &) = delete;
		Values &operator=(const Values &) = delete;
		Values &operator=(Values &&) = delete;
		~Values() {
			if (locked_) {
				for (size_t i = 0; i < size(); ++i) Payload{payloadType, (*this)[i].Value()}.ReleaseStrings();
			}
		}
		bool Locked() const { return locked_; }
		void Lock() {
			assertrx(!locked_);
			for (size_t i = 0; i < size(); ++i) Payload{payloadType, (*this)[i].Value()}.AddRefStrings();
			locked_ = true;
		}
		bool IsPreselectAllowed() const noexcept { return preselectAllowed_; }
		void PreselectAllowed(bool a) noexcept { preselectAllowed_ = a; }

		PayloadType payloadType;
		TagsMatcher tagsMatcher;

	private:
		bool locked_ = false;
		bool preselectAllowed_ = true;
	};

	typedef std::shared_ptr<JoinPreResult> Ptr;
	typedef std::shared_ptr<const JoinPreResult> CPtr;
	IdSet ids;
	SelectIteratorContainer iterators;
	Values values;
	enum { ModeEmpty, ModeBuild, ModeExecute } executionMode = ModeEmpty;
	enum { ModeIterators, ModeIdSet, ModeValues } dataMode = ModeIterators;
	bool enableSortOrders = false;
	bool btreeIndexOptimizationEnabled = true;
	bool enableStoredValues = false;
	std::string explainPreSelect, explainOneSelect;
	ExplainCalc::Duration selectTime = ExplainCalc::Duration::zero();
};

class SortExpression;
namespace SortExprFuncs {
struct DistanceBetweenJoinedIndexesSameNs;
}  // namespace SortExprFuncs
class NsSelecter;
class QueryPreprocessor;

class JoinedSelector {
	friend SortExpression;
	friend SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	friend NsSelecter;
	friend QueryPreprocessor;

public:
	JoinedSelector(JoinType joinType, NamespaceImpl::Ptr leftNs, NamespaceImpl::Ptr rightNs, JoinCacheRes &&joinRes, Query &&itemQuery,
				   LocalQueryResults &result, const JoinedQuery &joinQuery, JoinPreResult::Ptr preResult, uint32_t joinedFieldIdx,
				   SelectFunctionsHolder &selectFunctions, uint32_t joinedSelectorsCount, bool inTransaction, int64_t lastUpdateTime,
				   const RdxContext &rdxCtx)
		: joinType_(joinType),
		  called_(0),
		  matched_(0),
		  leftNs_(std::move(leftNs)),
		  rightNs_(std::move(rightNs)),
		  joinRes_(std::move(joinRes)),
		  itemQuery_(std::move(itemQuery)),
		  result_(result),
		  joinQuery_(joinQuery),
		  preResult_(std::move(preResult)),
		  joinedFieldIdx_(joinedFieldIdx),
		  selectFunctions_(selectFunctions),
		  joinedSelectorsCount_(joinedSelectorsCount),
		  rdxCtx_(rdxCtx),
		  optimized_(false),
		  inTransaction_{inTransaction},
		  lastUpdateTime_{lastUpdateTime} {
#ifndef NDEBUG
		for (const auto &jqe : joinQuery_.joinEntries_) {
			assertrx_throw(jqe.FieldsHaveBeenSet());
		}
#endif
	}

	JoinedSelector(JoinedSelector &&) = default;
	JoinedSelector &operator=(JoinedSelector &&) = delete;
	JoinedSelector(const JoinedSelector &) = delete;
	JoinedSelector &operator=(const JoinedSelector &) = delete;

	bool Process(IdType, int nsId, ConstPayload, bool match);
	JoinType Type() const noexcept { return joinType_; }
	void SetType(JoinType type) noexcept { joinType_ = type; }
	const std::string &RightNsName() const noexcept { return itemQuery_.NsName(); }
	int64_t LastUpdateTime() const noexcept { return lastUpdateTime_; }
	const JoinedQuery &JoinQuery() const noexcept { return joinQuery_; }
	int Called() const noexcept { return called_; }
	int Matched() const noexcept { return matched_; }
	void AppendSelectIteratorOfJoinIndexData(SelectIteratorContainer &, int *maxIterations, unsigned sortId, const SelectFunction::Ptr &,
											 const RdxContext &);
	static constexpr int MaxIterationsForPreResultStoreValuesOptimization() noexcept { return 200; }
	JoinPreResult::CPtr PreResult() const noexcept { return preResult_; }
	const NamespaceImpl::Ptr &RightNs() const noexcept { return rightNs_; }

private:
	[[nodiscard]] VariantArray readValuesFromRightNs(const QueryJoinEntry &) const;
	[[nodiscard]] VariantArray readValuesFromPreResult(const QueryJoinEntry &) const;
	template <typename Cont, typename Fn>
	[[nodiscard]] VariantArray readValuesOfRightNsFrom(const Cont &from, const Fn &createPayload, const QueryJoinEntry &,
													   const PayloadType &) const;
	void selectFromRightNs(LocalQueryResults &joinItemR, const Query &, bool &found, bool &matchedAtLeastOnce);
	void selectFromPreResultValues(LocalQueryResults &joinItemR, const Query &, bool &found, bool &matchedAtLeastOnce) const;

	JoinType joinType_;
	int called_, matched_;
	NamespaceImpl::Ptr leftNs_;
	NamespaceImpl::Ptr rightNs_;
	JoinCacheRes joinRes_;
	Query itemQuery_;
	LocalQueryResults &result_;
	const JoinedQuery &joinQuery_;
	JoinPreResult::Ptr preResult_;
	uint32_t joinedFieldIdx_;
	SelectFunctionsHolder &selectFunctions_;
	uint32_t joinedSelectorsCount_;
	const RdxContext &rdxCtx_;
	bool optimized_ = false;
	bool inTransaction_ = false;
	int64_t lastUpdateTime_ = 0;
};
using JoinedSelectors = std::vector<JoinedSelector>;

}  // namespace reindexer
