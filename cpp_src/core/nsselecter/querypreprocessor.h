#pragma once

#include "aggregator.h"
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
	QueryPreprocessor(QueryEntries &&, NamespaceImpl *, const SelectCtx &);
	const QueryEntries &GetQueryEntries() const noexcept { return *this; }
	bool LookupQueryIndexes() {
		const unsigned lookupEnd = queryEntryAddedByForcedSortOptimization_ ? container_.size() - 1 : container_.size();
		assertrx_throw(lookupEnd <= uint32_t(std::numeric_limits<uint16_t>::max() - 1));
		const size_t merged = lookupQueryIndexes(0, 0, lookupEnd);
		if (queryEntryAddedByForcedSortOptimization_) {
			container_[container_.size() - merged - 1] = std::move(container_.back());
		}
		if (merged != 0) {
			container_.resize(container_.size() - merged);
			return true;
		}
		return false;
	}
	bool ContainsFullTextIndexes() const;
	bool ContainsForcedSortOrder() const noexcept {
		if (queryEntryAddedByForcedSortOptimization_) {
			return forcedStage();
		}
		return forcedSortOrder_;
	}
	void CheckUniqueFtQuery() const;
	bool SubstituteCompositeIndexes() {
		return substituteCompositeIndexes(0, container_.size() - queryEntryAddedByForcedSortOptimization_) != 0;
	}
	void InitIndexedQueries() { initIndexedQueries(0, Size()); }
	void AddDistinctEntries(const h_vector<Aggregator, 4> &);
	bool NeedNextEvaluation(unsigned start, unsigned count, bool &matchedAtLeastOnce, QresExplainHolder &qresHolder) noexcept;
	unsigned Start() const noexcept { return start_; }
	unsigned Count() const noexcept { return count_; }
	bool MoreThanOneEvaluation() const noexcept { return queryEntryAddedByForcedSortOptimization_; }
	bool AvailableSelectBySortIndex() const noexcept { return !queryEntryAddedByForcedSortOptimization_ || !forcedStage(); }
	void InjectConditionsFromJoins(JoinedSelectors &js, OnConditionInjections &expalainOnInjections, LogLevel, bool inTransaction,
								   bool enableSortOrders, const RdxContext &rdxCtx);
	void Reduce(bool isFt);
	using QueryEntries::Size;
	using QueryEntries::Dump;
	using QueryEntries::ToDsl;
	template <typename JoinPreResultCtx>
	[[nodiscard]] SortingEntries GetSortingEntries(const SelectCtxWithJoinPreSelect<JoinPreResultCtx> &) const {
		if (ftEntry_) return {};
		// DO NOT use deducted sort order in the following cases:
		// - query contains explicity specified sort order
		// - query contains FullText query.
		const bool disableOptimizedSortOrder =
			!query_.sortingEntries_.empty() || ContainsFullTextIndexes() || !std::is_same_v<JoinPreResultCtx, void>;
		// Queries with ordered indexes may have different selection plan depending on filters' values.
		// This may lead to items reordering when SingleRange becomes main selection method.
		// By default all the results are ordereb by internal IDs, but with SingleRange results will be ordered by values first.
		// So we're trying to order results by values in any case, even if there are no SingleRange in selection plan.
		return disableOptimizedSortOrder ? query_.sortingEntries_ : detectOptimalSortOrder();
	}

	bool IsFtExcluded() const noexcept { return ftEntry_.has_value(); }
	void ExcludeFtQuery(const RdxContext &);
	FtMergeStatuses &GetFtMergeStatuses() noexcept {
		assertrx(ftPreselect_);
		return *ftPreselect_;
	}
	FtPreselectT &&MoveFtPreselect() noexcept {
		assertrx(ftPreselect_);
		return std::move(*ftPreselect_);
	}
	bool IsFtPreselected() const noexcept { return ftPreselect_ && !ftEntry_; }
	static void SetQueryField(QueryField &, const NamespaceImpl &);

private:
	enum class NeedSwitch : bool { Yes = true, No = false };
	enum class MergeResult { NotMerged, Merged, Annihilated };
	enum class MergeOrdered : bool { Yes = true, No = false };
	struct FoundIndexInfo {
		enum class ConditionType { Incompatible = 0, Compatible = 1 };

		FoundIndexInfo() noexcept : index(nullptr), size(0), isFitForSortOptimization(0) {}
		FoundIndexInfo(const Index *i, ConditionType ct) noexcept : index(i), size(i->Size()), isFitForSortOptimization(unsigned(ct)) {}

		const Index *index;
		uint64_t size : 63;
		uint64_t isFitForSortOptimization : 1;
	};

	static void setQueryIndex(QueryField &, int idxNo, const NamespaceImpl &);
	[[nodiscard]] SortingEntries detectOptimalSortOrder() const;
	[[nodiscard]] bool forcedStage() const noexcept { return evaluationsCount_ == (desc_ ? 1 : 0); }
	[[nodiscard]] size_t lookupQueryIndexes(uint16_t dst, uint16_t srcBegin, uint16_t srcEnd);
	[[nodiscard]] size_t substituteCompositeIndexes(size_t from, size_t to);
	[[nodiscard]] MergeResult mergeQueryEntries(size_t lhs, size_t rhs, MergeOrdered, const CollateOpts &);
	[[nodiscard]] MergeResult mergeQueryEntriesSetSet(QueryEntry &lqe, QueryEntry &rqe, bool distinct, size_t position,
													  const CollateOpts &);
	template <NeedSwitch>
	[[nodiscard]] MergeResult mergeQueryEntriesAllSetSet(QueryEntry &lqe, QueryEntry &rqe, bool distinct, size_t position,
														 const CollateOpts &);
	[[nodiscard]] MergeResult mergeQueryEntriesAllSetAllSet(QueryEntry &lqe, QueryEntry &rqe, bool distinct, size_t position,
															const CollateOpts &);
	template <NeedSwitch>
	[[nodiscard]] MergeResult mergeQueryEntriesAny(QueryEntry &lqe, QueryEntry &rqe, bool distinct, size_t position);
	template <NeedSwitch, typename F>
	[[nodiscard]] MergeResult mergeQueryEntriesSetNotSet(QueryEntry &lqe, QueryEntry &rqe, F filter, bool distinct, size_t position,
														 MergeOrdered);
	template <NeedSwitch, typename F>
	[[nodiscard]] MergeResult mergeQueryEntriesAllSetNotSet(QueryEntry &lqe, QueryEntry &rqe, F filter, bool distinct, size_t position,
															const CollateOpts &);
	[[nodiscard]] MergeResult mergeQueryEntriesDWithin(QueryEntry &lqe, QueryEntry &rqe, bool distinct, size_t position);
	[[nodiscard]] MergeResult mergeQueryEntriesLt(QueryEntry &lqe, QueryEntry &rqe, bool distinct, const CollateOpts &);
	[[nodiscard]] MergeResult mergeQueryEntriesGt(QueryEntry &lqe, QueryEntry &rqe, bool distinct, const CollateOpts &);
	[[nodiscard]] MergeResult mergeQueryEntriesLtGt(QueryEntry &lqe, QueryEntry &rqe, size_t position, const CollateOpts &);
	template <NeedSwitch>
	[[nodiscard]] MergeResult mergeQueryEntriesLeGe(QueryEntry &lqe, QueryEntry &rqe, bool distinct, size_t position, const CollateOpts &);
	template <NeedSwitch>
	[[nodiscard]] MergeResult mergeQueryEntriesRangeLt(QueryEntry &range, QueryEntry &ge, bool distinct, size_t position,
													   const CollateOpts &);
	template <NeedSwitch>
	[[nodiscard]] MergeResult mergeQueryEntriesRangeLe(QueryEntry &range, QueryEntry &ge, bool distinct, size_t position,
													   const CollateOpts &);
	template <NeedSwitch>
	[[nodiscard]] MergeResult mergeQueryEntriesRangeGt(QueryEntry &range, QueryEntry &ge, bool distinct, size_t position,
													   const CollateOpts &);
	template <NeedSwitch>
	[[nodiscard]] MergeResult mergeQueryEntriesRangeGe(QueryEntry &range, QueryEntry &ge, bool distinct, size_t position,
													   const CollateOpts &);
	[[nodiscard]] MergeResult mergeQueryEntriesRange(QueryEntry &range, QueryEntry &ge, bool distinct, size_t position,
													 const CollateOpts &);
	[[nodiscard]] const std::vector<int> *getCompositeIndex(int field) const noexcept;
	void initIndexedQueries(size_t begin, size_t end);
	[[nodiscard]] const Index *findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const;
	void findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end,
					  h_vector<FoundIndexInfo, 32> &foundIndexes) const;
	/** @brief recurrently checks and injects Join ON conditions
	 *  @returns injected conditions and EntryBrackets count
	 */
	template <typename ExplainPolicy>
	size_t injectConditionsFromJoins(size_t from, size_t to, JoinedSelectors &, OnConditionInjections &, int embracedMaxIterations,
									 h_vector<int, 256> &maxIterations, bool inTransaction, bool enableSortOrders, const RdxContext &);
	[[nodiscard]] std::pair<CondType, VariantArray> queryValuesFromOnCondition(std::string &outExplainStr, AggType &,
																			   NamespaceImpl &rightNs, Query joinQuery, JoinPreResult::CPtr,
																			   const QueryJoinEntry &, CondType, int mainQueryMaxIterations,
																			   const RdxContext &);
	[[nodiscard]] std::pair<CondType, VariantArray> queryValuesFromOnCondition(CondType condition, const QueryJoinEntry &,
																			   const JoinedSelector &, const CollateOpts &);
	void checkStrictMode(const QueryField &) const;
	[[nodiscard]] int calculateMaxIterations(const size_t from, const size_t to, int maxMaxIters, span<int> &maxIterations,
											 bool inTransaction, bool enableSortOrders, const RdxContext &) const;
	[[nodiscard]] bool removeBrackets();
	[[nodiscard]] size_t removeBrackets(size_t begin, size_t end);
	[[nodiscard]] bool canRemoveBracket(size_t i) const;
	[[nodiscard]] bool removeAlwaysFalse();
	[[nodiscard]] std::pair<size_t, bool> removeAlwaysFalse(size_t begin, size_t end);
	[[nodiscard]] bool removeAlwaysTrue();
	[[nodiscard]] std::pair<size_t, bool> removeAlwaysTrue(size_t begin, size_t end);
	[[nodiscard]] bool containsJoin(size_t) noexcept;

	template <typename JS>
	void briefDump(size_t from, size_t to, const std::vector<JS> &joinedSelectors, WrSerializer &ser) const;

	NamespaceImpl &ns_;
	const Query &query_;
	StrictMode strictMode_;
	size_t evaluationsCount_ = 0;
	unsigned start_ = QueryEntry::kDefaultOffset;
	unsigned count_ = QueryEntry::kDefaultLimit;
	bool queryEntryAddedByForcedSortOptimization_ = false;
	bool desc_ = false;
	bool forcedSortOrder_ = false;
	bool reqMatchedOnce_ = false;
	const bool isMergeQuery_ = false;
	std::optional<QueryEntry> ftEntry_;
	std::optional<FtPreselectT> ftPreselect_;
};

}  // namespace reindexer
