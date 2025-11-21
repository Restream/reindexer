#pragma once

#include "aggregator.h"
#include "core/enums.h"
#include "core/index/ft_preselect.h"
#include "core/query/queryentry.h"
#include "estl/h_vector.h"
#include "joinedselector.h"

namespace reindexer {

class NamespaceImpl;
class QresExplainHolder;
template <typename>
struct SelectCtxWithJoinPreSelect;

class [[nodiscard]] QueryPreprocessor : private QueryEntries {
	class Merger;
	struct [[nodiscard]] Ranked {
		QueryRankType queryRankType;
		IndexValueType rankedIndexNo;
	};
	enum class [[nodiscard]] EvaluationStage : uint8_t {
		First,
		Second	// Currently we do not have queries with more than 2 stages
	};

public:
	enum class [[nodiscard]] ValuesType : bool { Scalar, Composite };

	QueryPreprocessor(QueryEntries&&, NamespaceImpl*, const SelectCtx&);
	const QueryEntries& GetQueryEntries() const& noexcept { return *this; }
	auto GetQueryEntries() const&& = delete;
	Changed LookupQueryIndexes() {
		const unsigned lookupEnd = container_.size();
		assertrx_throw(lookupEnd <= uint32_t(std::numeric_limits<uint16_t>::max() - 1));
		const auto [merged, changed] = lookupQueryIndexes(lookupEnd);
		if (merged != 0) {
			container_.resize(container_.size() - merged);
			return Changed_True;
		}
		return changed;
	}
	Ranked GetQueryRankType() const;
	bool ContainsForcedSortOrder() const noexcept {
		if (HasForcedSortOptimizationQueryEntry()) {
			return forcedStage();
		}
		return forcedSortOrder_;
	}
	Changed SubstituteCompositeIndexes() { return Changed{substituteCompositeIndexes(0, container_.size()) != 0}; }
	void InitIndexedQueries() { initIndexedQueries(0, Size()); }
	void AddDistinctEntries(const h_vector<Aggregator, 4>&);
	bool NeedNextEvaluation(unsigned start, unsigned count, bool& matchedAtLeastOnce, QresExplainHolder& qresHolder, bool needCalcTotal,
							const SelectCtx&);
	unsigned Start() const noexcept { return start_; }
	unsigned Count() const noexcept { return count_; }
	bool MoreThanOneEvaluation() const noexcept { return HasForcedSortOptimizationQueryEntry(); }
	bool AvailableSelectBySortIndex() const noexcept { return !HasForcedSortOptimizationQueryEntry() || !forcedStage(); }
	void InjectConditionsFromJoins(JoinedSelectors& js, OnConditionInjections& explainOnInjections, LogLevel, bool inTransaction,
								   bool enableSortOrders, const RdxContext& rdxCtx);
	void Reduce();
	using QueryEntries::Size;
	using QueryEntries::Dump;
	using QueryEntries::ToDsl;
	template <typename JoinPreResultCtx>
	SortingEntries GetSortingEntries(const SelectCtxWithJoinPreSelect<JoinPreResultCtx>&, QueryRankType queryRankType) const {
		if (ftEntry_) {
			return {};
		}
		// DO NOT use deducted sort order in the following cases:
		// - query contains explicit specified sort order
		// - query contains ranked query.
		const bool disableOptimizedSortOrder =
			!query_.GetSortingEntries().empty() || queryRankType != QueryRankType::No || !std::is_same_v<JoinPreResultCtx, void>;
		// Queries with ordered indexes may have different selection plan depending on filters' values.
		// This may lead to items reordering when SingleRange becomes main selection method.
		// By default, all the results are ordered by internal IDs, but with SingleRange results will be ordered by values first.
		// So we're trying to order results by values in any case, even if there are no SingleRange in selection plan.
		return disableOptimizedSortOrder ? query_.GetSortingEntries() : detectOptimalSortOrder();
	}

	bool IsFtExcluded() const noexcept { return ftEntry_.has_value(); }
	void ExcludeFtQuery(const RdxContext&);
	FtMergeStatuses& GetFtMergeStatuses() noexcept {
		assertrx_throw(ftPreselect_);
		return *ftPreselect_;
	}
	FtPreselectT&& MoveFtPreselect() noexcept {
		assertrx_throw(ftPreselect_);
		return std::move(*ftPreselect_);
	}
	bool IsFtPreselected() const noexcept { return ftPreselect_ && !ftEntry_; }
	static void SetQueryField(QueryField&, const NamespaceImpl&);
	static void VerifyOnStatementField(const QueryField&, const NamespaceImpl&, StrictMode strictMode);
	bool IsSecondForcedSortStage() const noexcept {
		return hasForcedSortOptimizationEntry_ && (evaluationStage_ == EvaluationStage::Second);
	}
	bool HasForcedSortOptimizationQueryEntry() const noexcept { return hasForcedSortOptimizationEntry_; }

private:
	enum class [[nodiscard]] NeedSwitch : bool { Yes = true, No = false };
	enum class [[nodiscard]] MergeOrdered : bool { Yes = true, No = false };
	struct [[nodiscard]] FoundIndexInfo {
		enum class [[nodiscard]] ConditionType { Incompatible = 0, Compatible = 1 };

		FoundIndexInfo() noexcept : index(nullptr), size(0), isFitForSortOptimization(0) {}
		FoundIndexInfo(const Index* i, ConditionType ct) noexcept : index(i), size(i->Size()), isFitForSortOptimization(unsigned(ct)) {}

		const Index* index;
		uint64_t size : 63;
		uint64_t isFitForSortOptimization : 1;
	};

	static void setQueryIndex(QueryField&, int idxNo, const NamespaceImpl&);
	SortingEntries detectOptimalSortOrder() const;
	bool forcedStage() const noexcept {
		return desc_ ? (evaluationStage_ == EvaluationStage::Second) : (evaluationStage_ == EvaluationStage::First);
	}
	std::pair<size_t, Changed> lookupQueryIndexes(uint16_t srcEnd);
	size_t substituteCompositeIndexes(size_t from, size_t to);
	template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
	MergeResult mergeQueryEntries(size_t lhs, size_t rhs, MergeOrdered, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesSetSet(QueryEntry& lqe, QueryEntry& rqe, IsDistinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesAllSetSet(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, IsDistinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesAllSetAllSet(QueryEntry& lqe, QueryEntry& rqe, IsDistinct, size_t position, const CmpArgs&...);
	MergeResult mergeQueryEntriesAny(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, IsDistinct, size_t position);
	template <ValuesType, typename F>
	MergeResult mergeQueryEntriesSetNotSet(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, F filter, IsDistinct, size_t position,
										   MergeOrdered);
	template <ValuesType, typename F, typename... CmpArgs>
	MergeResult mergeQueryEntriesAllSetNotSet(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, F filter, IsDistinct, size_t position,
											  const CmpArgs&...);
	MergeResult mergeQueryEntriesDWithin(QueryEntry& lqe, QueryEntry& rqe, IsDistinct, size_t position);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesLt(QueryEntry& lqe, QueryEntry& rqe, IsDistinct, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesGt(QueryEntry& lqe, QueryEntry& rqe, IsDistinct, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesLtGt(QueryEntry& lqe, QueryEntry& rqe, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesLeGe(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, IsDistinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeLt(NeedSwitch, QueryEntry& range, QueryEntry& ge, IsDistinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeLe(NeedSwitch, QueryEntry& range, QueryEntry& ge, IsDistinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeGt(NeedSwitch, QueryEntry& range, QueryEntry& ge, IsDistinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeGe(NeedSwitch, QueryEntry& range, QueryEntry& ge, IsDistinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRange(QueryEntry& range, QueryEntry& ge, IsDistinct, size_t position, const CmpArgs&...);
	const std::vector<int>* getCompositeIndex(int field) const noexcept;
	void initIndexedQueries(size_t begin, size_t end);
	const Index* findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const;
	void findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end,
					  h_vector<FoundIndexInfo, 32>& foundIndexes) const;
	/** @brief recurrently checks and injects Join ON conditions
	 *  @returns injected conditions and EntryBrackets count
	 */
	template <typename ExplainPolicy>
	size_t injectConditionsFromJoins(size_t from, size_t to, JoinedSelectors&, OnConditionInjections&, int embracedMaxIterations,
									 h_vector<int, 256>& maxIterations, bool inTransaction, bool enableSortOrders, const RdxContext&);
	std::pair<CondType, VariantArray> queryValuesFromOnCondition(std::string& outExplainStr, AggType&, NamespaceImpl& rightNs,
																 Query joinQuery, JoinPreResult::CPtr, const QueryJoinEntry&, CondType,
																 int mainQueryMaxIterations, const RdxContext&);
	std::pair<CondType, VariantArray> queryValuesFromOnCondition(CondType condition, const QueryJoinEntry&, const JoinedSelector&,
																 const CollateOpts&);
	void checkStrictMode(const QueryField&) const;
	void checkAllowedCondition(const QueryField&, CondType) const;
	int calculateMaxIterations(size_t from, size_t to, int maxMaxIters, std::span<int>& maxIterations, bool inTransaction,
							   bool enableSortOrders, const RdxContext&) const;
	Changed removeBrackets();
	size_t removeBrackets(size_t begin, size_t end);
	bool canRemoveBracket(size_t i) const;
	Changed removeAlwaysFalse();
	std::pair<size_t, Changed> removeAlwaysFalse(size_t begin, size_t end);
	Changed removeAlwaysTrue();
	std::pair<size_t, Changed> removeAlwaysTrue(size_t begin, size_t end);
	bool containsJoin(size_t) noexcept;

	template <typename JS>
	size_t briefDump(size_t from, size_t to, const std::vector<JS>& joinedSelectors, WrSerializer& ser) const;

	NamespaceImpl& ns_;
	const Query& query_;
	StrictMode strictMode_;
	Desc desc_ = Desc_False;
	bool forcedSortOrder_ = false;
	bool reqMatchedOnce_ = false;
	EvaluationStage evaluationStage_ = EvaluationStage::First;
	unsigned start_ = QueryEntry::kDefaultOffset;
	unsigned count_ = QueryEntry::kDefaultLimit;
	std::optional<QueryEntry> ftEntry_;
	std::optional<FtPreselectT> ftPreselect_;
	FloatVectorsHolderMap* floatVectorsHolder_ = nullptr;
	bool hasForcedSortOptimizationEntry_ = false;
};

}  // namespace reindexer
