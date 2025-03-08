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

class QueryPreprocessor : private QueryEntries {
public:
	enum class [[nodiscard]] ValuesType : bool { Scalar, Composite };

	QueryPreprocessor(QueryEntries&&, const h_vector<Aggregator, 4>&, NamespaceImpl*, const SelectCtx&);
	const QueryEntries& GetQueryEntries() const noexcept { return *this; }
	bool LookupQueryIndexes() {
		bool forcedSortOptimization = hasForcedSortOptimizationQueryEntry();
		const unsigned lookupEnd = container_.size() - size_t(forcedSortOptimization);
		assertrx_throw(lookupEnd <= uint32_t(std::numeric_limits<uint16_t>::max() - 1));
		const size_t merged = lookupQueryIndexes(0, 0, lookupEnd);
		if (forcedSortOptimization) {
			container_[container_.size() - merged - 1] = std::move(container_.back());
		}
		if (merged != 0) {
			container_.resize(container_.size() - merged);
			return true;
		}
		return false;
	}
	RankedTypeQuery GetRankedTypeQuery() const;
	bool ContainsForcedSortOrder() const noexcept {
		if (hasForcedSortOptimizationQueryEntry()) {
			return forcedStage();
		}
		return forcedSortOrder_;
	}
	bool SubstituteCompositeIndexes() {
		return substituteCompositeIndexes(0, container_.size() - hasForcedSortOptimizationQueryEntry()) != 0;
	}
	void InitIndexedQueries() { initIndexedQueries(0, Size()); }
	bool NeedNextEvaluation(unsigned start, unsigned count, bool& matchedAtLeastOnce, QresExplainHolder& qresHolder) noexcept;
	unsigned Start() const noexcept { return start_; }
	unsigned Count() const noexcept { return count_; }
	bool MoreThanOneEvaluation() const noexcept { return hasForcedSortOptimizationQueryEntry(); }
	bool AvailableSelectBySortIndex() const noexcept { return !hasForcedSortOptimizationQueryEntry() || !forcedStage(); }
	void InjectConditionsFromJoins(JoinedSelectors& js, OnConditionInjections& explainOnInjections, LogLevel, bool inTransaction,
								   bool enableSortOrders, const RdxContext& rdxCtx);
	void Reduce();
	using QueryEntries::Size;
	using QueryEntries::Dump;
	using QueryEntries::ToDsl;
	template <typename JoinPreResultCtx>
	[[nodiscard]] SortingEntries GetSortingEntries(const SelectCtxWithJoinPreSelect<JoinPreResultCtx>&) const {
		if (ftEntry_) {
			return {};
		}
		// DO NOT use deducted sort order in the following cases:
		// - query contains explicit specified sort order
		// - query contains ranked query.
		const bool disableOptimizedSortOrder =
			!query_.sortingEntries_.empty() || GetRankedTypeQuery() != RankedTypeQuery::No || !std::is_same_v<JoinPreResultCtx, void>;
		// Queries with ordered indexes may have different selection plan depending on filters' values.
		// This may lead to items reordering when SingleRange becomes main selection method.
		// By default, all the results are ordered by internal IDs, but with SingleRange results will be ordered by values first.
		// So we're trying to order results by values in any case, even if there are no SingleRange in selection plan.
		return disableOptimizedSortOrder ? query_.sortingEntries_ : detectOptimalSortOrder();
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
	static void VerifyOnStatementField(const QueryField&, const NamespaceImpl&);

private:
	enum class NeedSwitch : bool { Yes = true, No = false };
	enum class [[nodiscard]] MergeResult { NotMerged, Merged, Annihilated };
	enum class MergeOrdered : bool { Yes = true, No = false };
	struct FoundIndexInfo {
		enum class ConditionType { Incompatible = 0, Compatible = 1 };

		FoundIndexInfo() noexcept : index(nullptr), size(0), isFitForSortOptimization(0) {}
		FoundIndexInfo(const Index* i, ConditionType ct) noexcept : index(i), size(i->Size()), isFitForSortOptimization(unsigned(ct)) {}

		const Index* index;
		uint64_t size : 63;
		uint64_t isFitForSortOptimization : 1;
	};

	static void setQueryIndex(QueryField&, int idxNo, const NamespaceImpl&);
	[[nodiscard]] SortingEntries detectOptimalSortOrder() const;
	[[nodiscard]] bool forcedStage() const noexcept { return evaluationsCount_ == (desc_ ? 1 : 0); }
	[[nodiscard]] size_t lookupQueryIndexes(uint16_t dst, uint16_t srcBegin, uint16_t srcEnd);
	[[nodiscard]] size_t substituteCompositeIndexes(size_t from, size_t to);
	template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
	MergeResult mergeQueryEntries(size_t lhs, size_t rhs, MergeOrdered, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesSetSet(QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesAllSetSet(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesAllSetAllSet(QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position, const CmpArgs&...);
	MergeResult mergeQueryEntriesAny(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position);
	template <ValuesType, typename F>
	MergeResult mergeQueryEntriesSetNotSet(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, F filter, bool distinct, size_t position,
										   MergeOrdered);
	template <ValuesType, typename F, typename... CmpArgs>
	MergeResult mergeQueryEntriesAllSetNotSet(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, F filter, bool distinct, size_t position,
											  const CmpArgs&...);
	MergeResult mergeQueryEntriesDWithin(QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesLt(QueryEntry& lqe, QueryEntry& rqe, bool distinct, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesGt(QueryEntry& lqe, QueryEntry& rqe, bool distinct, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesLtGt(QueryEntry& lqe, QueryEntry& rqe, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesLeGe(NeedSwitch, QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeLt(NeedSwitch, QueryEntry& range, QueryEntry& ge, bool distinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeLe(NeedSwitch, QueryEntry& range, QueryEntry& ge, bool distinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeGt(NeedSwitch, QueryEntry& range, QueryEntry& ge, bool distinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRangeGe(NeedSwitch, QueryEntry& range, QueryEntry& ge, bool distinct, size_t position, const CmpArgs&...);
	template <ValuesType, typename... CmpArgs>
	MergeResult mergeQueryEntriesRange(QueryEntry& range, QueryEntry& ge, bool distinct, size_t position, const CmpArgs&...);
	[[nodiscard]] const std::vector<int>* getCompositeIndex(int field) const noexcept;
	void initIndexedQueries(size_t begin, size_t end);
	[[nodiscard]] const Index* findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const;
	void findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end,
					  h_vector<FoundIndexInfo, 32>& foundIndexes) const;
	/** @brief recurrently checks and injects Join ON conditions
	 *  @returns injected conditions and EntryBrackets count
	 */
	template <typename ExplainPolicy>
	size_t injectConditionsFromJoins(size_t from, size_t to, JoinedSelectors&, OnConditionInjections&, int embracedMaxIterations,
									 h_vector<int, 256>& maxIterations, bool inTransaction, bool enableSortOrders, const RdxContext&);
	[[nodiscard]] std::pair<CondType, VariantArray> queryValuesFromOnCondition(std::string& outExplainStr, AggType&, NamespaceImpl& rightNs,
																			   Query joinQuery, JoinPreResult::CPtr, const QueryJoinEntry&,
																			   CondType, int mainQueryMaxIterations, const RdxContext&);
	[[nodiscard]] std::pair<CondType, VariantArray> queryValuesFromOnCondition(CondType condition, const QueryJoinEntry&,
																			   const JoinedSelector&, const CollateOpts&);
	void checkStrictMode(const QueryField&) const;
	void checkAllowedCondition(const QueryField&, CondType) const;
	[[nodiscard]] int calculateMaxIterations(const size_t from, const size_t to, int maxMaxIters, std::span<int>& maxIterations,
											 bool inTransaction, bool enableSortOrders, const RdxContext&) const;
	[[nodiscard]] bool removeBrackets();
	[[nodiscard]] size_t removeBrackets(size_t begin, size_t end);
	[[nodiscard]] bool canRemoveBracket(size_t i) const;
	[[nodiscard]] bool removeAlwaysFalse();
	[[nodiscard]] std::pair<size_t, bool> removeAlwaysFalse(size_t begin, size_t end);
	[[nodiscard]] bool removeAlwaysTrue();
	[[nodiscard]] std::pair<size_t, bool> removeAlwaysTrue(size_t begin, size_t end);
	[[nodiscard]] bool containsJoin(size_t) noexcept;

	template <typename JS>
	void briefDump(size_t from, size_t to, const std::vector<JS>& joinedSelectors, WrSerializer& ser) const;

	bool hasForcedSortOptimizationQueryEntry() const noexcept { return Size() && container_.back().Is<ForcedSortOptimizationQueryEntry>(); }
	void addDistinctEntries(const h_vector<Aggregator, 4>&);

	NamespaceImpl& ns_;
	const Query& query_;
	StrictMode strictMode_;
	bool desc_ = false;
	bool forcedSortOrder_ = false;
	bool reqMatchedOnce_ = false;
	size_t evaluationsCount_ = 0;
	unsigned start_ = QueryEntry::kDefaultOffset;
	unsigned count_ = QueryEntry::kDefaultLimit;
	std::optional<QueryEntry> ftEntry_;
	std::optional<FtPreselectT> ftPreselect_;
	const bool isMergeQuery_ = false;
	FloatVectorsHolderMap* floatVectorsHolder_;
};

}  // namespace reindexer
