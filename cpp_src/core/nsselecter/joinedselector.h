#pragma once
#include <optional>
#include "core/joincache.h"
#include "core/namespace/namespaceimpl.h"
#include "explaincalc.h"
#include "selectiteratorcontainer.h"

namespace reindexer {

struct PreselectProperties {
	PreselectProperties(int64_t qresMaxIts, int64_t maxItersIdSetPreResult) noexcept
		: qresMaxIteratios{qresMaxIts}, maxIterationsIdSetPreResult{maxItersIdSetPreResult} {}

	bool isLimitExceeded = false;
	bool isUnorderedIndexSort = false;
	bool btreeIndexOptimizationEnabled = false;
	int64_t qresMaxIteratios;
	const int64_t maxIterationsIdSetPreResult;
};

struct JoinPreResult {
	class Values : public std::vector<ItemRef> {
	public:
		Values(const PayloadType &pt, const TagsMatcher &tm) noexcept : payloadType{pt}, tagsMatcher{tm} {}
		Values(Values &&other) noexcept
			: std::vector<ItemRef>(std::move(other)),
			  payloadType(std::move(other.payloadType)),
			  tagsMatcher(std::move(other.tagsMatcher)),
			  locked_(other.locked_) {
			other.locked_ = false;
		}
		Values() noexcept : locked_(false) {}
		Values(const Values &) = delete;
		Values &operator=(const Values &) = delete;
		Values &operator=(Values &&) = delete;
		~Values() {
			if (locked_) {
				for (size_t i = 0; i < size(); ++i) Payload{payloadType, (*this)[i].Value()}.ReleaseStrings();
			}
		}
		bool Locked() const noexcept { return locked_; }
		void Lock() {
			assertrx_throw(!locked_);
			for (size_t i = 0; i < size(); ++i) Payload{payloadType, (*this)[i].Value()}.AddRefStrings();
			locked_ = true;
		}
		bool IsPreselectAllowed() const noexcept { return preselectAllowed_; }
		void PreselectAllowed(bool a) noexcept { preselectAllowed_ = a; }

		PayloadType payloadType;
		TagsMatcher tagsMatcher;
		NamespaceName nsName;

	private:
		bool locked_ = false;
		bool preselectAllowed_ = true;
	};

	using PreselectT = std::variant<IdSet, SelectIteratorContainer, Values>;
	typedef std::shared_ptr<JoinPreResult> Ptr;
	typedef std::shared_ptr<const JoinPreResult> CPtr;
	PreselectT payload;
	bool enableSortOrders = false;
	bool btreeIndexOptimizationEnabled = true;
	StoredValuesOptimizationStatus storedValuesOptStatus = StoredValuesOptimizationStatus::Enabled;
	std::optional<PreselectProperties> properties;
	std::string explainPreSelect;
};

enum class JoinPreSelectMode { Empty, Build, Execute, ForInjection, InjectionRejected };

class JoinPreResultBuildCtx {
public:
	explicit JoinPreResultBuildCtx(JoinPreResult::Ptr r) noexcept : result_{std::move(r)} {}
	JoinPreResult &Result() & noexcept { return *result_; }
	JoinPreSelectMode Mode() const noexcept { return JoinPreSelectMode::Build; }
	const JoinPreResult::Ptr &ResultPtr() const & noexcept { return result_; }
	auto ResultPtr() const && = delete;

private:
	JoinPreResult::Ptr result_;
};

class JoinPreResultExecuteCtx {
public:
	explicit JoinPreResultExecuteCtx(JoinPreResult::CPtr r) noexcept : result_{std::move(r)}, mode_{JoinPreSelectMode::Execute} {}
	explicit JoinPreResultExecuteCtx(JoinPreResult::CPtr r, int maxIters) noexcept
		: result_{std::move(r)}, mode_{JoinPreSelectMode::ForInjection}, mainQueryMaxIterations_{maxIters} {}
	const JoinPreResult &Result() const & noexcept { return *result_; }
	JoinPreSelectMode Mode() const noexcept { return mode_; }
	int MainQueryMaxIterations() const {
		assertrx_dbg(mode_ == JoinPreSelectMode::ForInjection);
		return mainQueryMaxIterations_;
	}
	const JoinPreResult::CPtr &ResultPtr() const & noexcept { return result_; }
	void Reject() {
		assertrx_dbg(mode_ == JoinPreSelectMode::ForInjection);
		mode_ = JoinPreSelectMode::InjectionRejected;
	}

	auto Result() const && = delete;
	auto ResultPtr() const && = delete;

private:
	JoinPreResult::CPtr result_;
	JoinPreSelectMode mode_;
	int mainQueryMaxIterations_{0};
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
				   LocalQueryResults &result, const JoinedQuery &joinQuery, JoinPreResultExecuteCtx &&preSelCtx, uint32_t joinedFieldIdx,
				   SelectFunctionsHolder &selectFunctions, bool inTransaction, int64_t lastUpdateTime, const RdxContext &rdxCtx)
		: joinType_(joinType),
		  called_(0),
		  matched_(0),
		  leftNs_(std::move(leftNs)),
		  rightNs_(std::move(rightNs)),
		  joinRes_(std::move(joinRes)),
		  itemQuery_(std::move(itemQuery)),
		  result_(result),
		  joinQuery_(joinQuery),
		  preSelectCtx_(std::move(preSelCtx)),
		  joinedFieldIdx_(joinedFieldIdx),
		  selectFunctions_(selectFunctions),
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
	const JoinPreResult &PreResult() const & noexcept { return preSelectCtx_.Result(); }
	const JoinPreResult::CPtr &PreResultPtr() const & noexcept { return preSelectCtx_.ResultPtr(); }
	JoinPreSelectMode PreSelectMode() const noexcept { return preSelectCtx_.Mode(); }
	const NamespaceImpl::Ptr &RightNs() const noexcept { return rightNs_; }
	ExplainCalc::Duration SelectTime() const noexcept { return selectTime_; }
	const std::string &ExplainOneSelect() const & noexcept { return explainOneSelect_; }

	auto ExplainOneSelect() const && = delete;
	auto PreResult() const && = delete;
	auto PreResultPtr() const && = delete;

private:
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
	JoinPreResultExecuteCtx preSelectCtx_;
	std::string explainOneSelect_;
	uint32_t joinedFieldIdx_;
	SelectFunctionsHolder &selectFunctions_;
	const RdxContext &rdxCtx_;
	bool optimized_ = false;
	bool inTransaction_ = false;
	int64_t lastUpdateTime_ = 0;
	ExplainCalc::Duration selectTime_ = ExplainCalc::Duration::zero();
};
using JoinedSelectors = std::vector<JoinedSelector>;

}  // namespace reindexer
