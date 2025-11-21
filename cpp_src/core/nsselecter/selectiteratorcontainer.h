#pragma once
#include <limits>
#include "core/expressiontree.h"
#include "core/ft/functions/ft_function.h"
#include "core/index/float_vector/knn_raw_result.h"
#include "core/index/index.h"
#include "core/nsselecter/comparator/comparator_indexed.h"
#include "core/nsselecter/comparator/comparator_not_indexed.h"
#include "core/nsselecter/comparator/comporator_distinct_multi.h"
#include "core/nsselecter/comparator/equalposition_comparator.h"
#include "core/nsselecter/comparator/fieldscomparator.h"
#include "core/nsselecter/selectiterator.h"
#include "core/query/queryentry.h"

namespace reindexer {

struct SelectCtx;
class RdxContext;
class JoinedSelector;
class QueryPreprocessor;
class NamespaceImpl;
class RanksHolder;
class Reranker;

struct [[nodiscard]] JoinSelectIterator {
	size_t joinIndex;
	double Cost() const noexcept { return std::numeric_limits<float>::max(); }
	void Dump(WrSerializer&, const std::vector<JoinedSelector>&) const;
};

struct [[nodiscard]] SelectIteratorsBracket : private Bracket {
	using Bracket::Bracket;
	using Bracket::Size;
	using Bracket::Append;
	using Bracket::Erase;
	void CopyPayloadFrom(const SelectIteratorsBracket& other) noexcept { haveJoins = other.haveJoins; }
	bool haveJoins = false;
};

class [[nodiscard]] KnnRawSelectResult {
public:
	KnnRawSelectResult(KnnRawResult&& raw, int idxNo) noexcept : rawResult_{std::move(raw)}, indexNo_{idxNo} {}
	int IndexNo() const noexcept { return indexNo_; }
	const KnnRawResult& RawResult() const& noexcept { return rawResult_; }
	KnnRawResult& RawResult() & noexcept { return rawResult_; }

	auto RawResult() const&& = delete;

private:
	KnnRawResult rawResult_;
	int indexNo_;
};

using ComparatorsPackT =
	TypesPack<FieldsComparator, EqualPositionComparator, GroupingEqualPositionComparator, ComparatorNotIndexed, ComparatorDistinctMulti,
			  ComparatorDistinctMultiArray, ComparatorDistinctMultiScalarBase<ComparatorDistinctMultiIndexedGetter>,
			  ComparatorDistinctMultiScalarBase<ComparatorDistinctMultiColumnGetter>,
			  ComparatorDistinctMultiScalarBase<ComparatorDistinctMultiScalarGetter>, ComparatorIndexed<bool>, ComparatorIndexed<int>,
			  ComparatorIndexed<int64_t>, ComparatorIndexed<double>, ComparatorIndexed<key_string>, ComparatorIndexed<PayloadValue>,
			  ComparatorIndexed<Point>, ComparatorIndexed<Uuid>, ComparatorIndexed<FloatVector>>;

template <typename... Ts>
using SelectIteratorContainerTreeBase =
	ExpressionTree<OpType, SelectIteratorsBracket, 2, SelectIterator, JoinSelectIterator, KnnRawSelectResult, AlwaysTrue, Ts...>;

using SelectIteratorContainerTree = WithTypesPack<SelectIteratorContainerTreeBase, ComparatorsPackT>::type;

class SelectIteratorContainer : public SelectIteratorContainerTree {
	using Base = SelectIteratorContainerTree;

public:
	enum class [[nodiscard]] MergeType : bool;

private:
	template <MergeType, bool desc, VectorMetric>
	class [[nodiscard]] MergerRankedImpl;
	class [[nodiscard]] MergerRanked;

public:
	SelectIteratorContainer(PayloadType pt = PayloadType(), SelectCtx* ctx = nullptr) noexcept
		: pt_(std::move(pt)), ctx_(ctx), maxIterations_(std::numeric_limits<int>::max()) {}

	void SortByCost(int expectedIterations);
	bool HasIdsets() const;
	// Check NOT or comparator must not be 1st
	void CheckFirstQuery();
	// Let iterators choose most effective algorithm
	void SetExpectMaxIterations(int expectedIterations);
	void PrepareIteratorsForSelectLoop(QueryPreprocessor&, unsigned sortId, QueryRankType, RankSortType, const NamespaceImpl&,
									   FtFunction::Ptr&, RanksHolder::Ptr&, const RdxContext&);
	template <bool reverse>
	RX_ALWAYS_INLINE bool Process(const PayloadValue& pv, bool* finish, IdType* rowId, IdType properRowId, bool withJoinedItems) {
		const auto rowIdV = *rowId;
		if (auto it = begin(); checkIfSatisfyAllConditions(++it, end(), pv, finish, rowIdV, properRowId, withJoinedItems)) {
			// Check distinct condition:
			// Exclude last sets of id from each query result, so duplicated keys will
			// be removed
			VisitForEach([](const KnnRawSelectResult&) { throw_as_assert; },
						 Skip<SelectIteratorsBracket, JoinSelectIterator, AlwaysFalse, AlwaysTrue>{},
						 [rowIdV](SelectIterator& sit) {
							 if (sit.IsDistinct()) {
								 sit.ExcludeLastSet(rowIdV);
							 }
						 },
						 [&pv, properRowId](concepts::OneOf<ComparatorsPackT> auto& comp) { comp.ExcludeDistinctValues(pv, properRowId); });

			if (preservedDistincts_.Empty()) [[likely]] {
				return true;
			}

			// Check distinct conditions, preserved from the previous execution stage
			if (checkIfSatisfyAllConditions(preservedDistincts_.begin(), preservedDistincts_.end(), pv, finish, rowIdV, properRowId,
											withJoinedItems)) {
				preservedDistincts_.VisitForEach(
					[]<concepts::OneOf<SelectIterator, KnnRawSelectResult> T>(const T&) { throw_as_assert; },
					Skip<SelectIteratorsBracket, JoinSelectIterator, AlwaysFalse, AlwaysTrue>{},
					[&pv, properRowId](concepts::OneOf<ComparatorsPackT> auto& comp) { comp.ExcludeDistinctValues(pv, properRowId); });
				return true;
			}
		}
		*rowId = getNextItemId<reverse>(cbegin(), cend(), rowIdV);
		return false;
	}

	bool IsSelectIterator(size_t i) const noexcept {
		assertrx_throw(i < Size());
		return container_[i].Is<SelectIterator>();
	}
	bool IsJoinIterator(size_t i) const noexcept {
		assertrx_throw(i < container_.size());
		return container_[i].Is<JoinSelectIterator>();
	}
	reindexer::IsDistinct IsDistinct(size_t i) const noexcept {
		return Visit(
			i,
			[] RX_PRE_LMBD_ALWAYS_INLINE(
				const concepts::OneOf<SelectIteratorsBracket, JoinSelectIterator, AlwaysFalse, AlwaysTrue, KnnRawSelectResult> auto&)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { return IsDistinct_False; },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SelectIterator, ComparatorsPackT> auto& comp)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { return comp.IsDistinct(); });
	}
	void ExplainJSON(int iters, JsonBuilder& builder, const std::vector<JoinedSelector>* js) const;

	void Clear(bool preserveDistincts);
	int GetMaxIterations() const noexcept { return maxIterations_; }
	std::string Dump() const;
	static bool IsExpectingOrderedResults(const QueryEntry& qe) noexcept {
		return IsOrderedCondition(qe.Condition()) || (qe.Condition() != CondAny && qe.Values().size() <= 1);
	}
	void MergeRanked(RanksHolder::Ptr&, const Reranker&, const NamespaceImpl&);

private:
	ContainRanked prepareIteratorsForSelectLoop(QueryPreprocessor&, size_t begin, size_t end, unsigned sortId, QueryRankType, RankSortType,
												const NamespaceImpl&, FtFunction::Ptr&, RanksHolder::Ptr&, const RdxContext&);
	void sortByCost(std::span<unsigned int> indexes, std::span<double> costs, unsigned from, unsigned to, int expectedIterations);
	double fullCost(std::span<unsigned> indexes, unsigned i, unsigned from, unsigned to, int expectedIterations) const noexcept;
	double cost(std::span<unsigned> indexes, unsigned cur, int expectedIterations) const noexcept;
	double cost(std::span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const noexcept;
	void moveJoinsToTheBeginningOfORs(std::span<unsigned> indexes, unsigned from, unsigned to);
	bool checkIfSatisfyAllConditions(iterator begin, iterator end, const PayloadValue&, bool* finish, IdType rowId, IdType properRowId,
									 bool match);
	static std::string explainJSON(const_iterator it, const_iterator to, int iters, JsonBuilder& builder,
								   const std::vector<JoinedSelector>*);
	template <bool reverse>
	static IdType getNextItemId(const_iterator begin, const_iterator end, IdType from);
	static bool isIdset(const_iterator it, const_iterator end);
	static bool markBracketsHavingJoins(iterator begin, iterator end) noexcept;
	bool haveJoins(size_t i) const noexcept;

	SelectKeyResults processQueryEntry(const QueryEntry& qe, const NamespaceImpl& ns, StrictMode strictMode);
	h_vector<SelectKeyResults, 2> processQueryEntry(const QueryEntry& qe, bool enableSortIndexOptimize, const NamespaceImpl& ns,
													unsigned sortId, QueryRankType, RankSortType, FtFunction::Ptr& selectFnc,
													RanksHolder::Ptr&, reindexer::IsRanked&, StrictMode, QueryPreprocessor& qPreproc,
													const RdxContext&);
	SelectKeyResult processKnnQueryEntry(const KnnQueryEntry&, const NamespaceImpl&, RanksHolder::Ptr&, const RdxContext&);
	KnnRawResult processKnnQueryEntryRaw(const KnnQueryEntry&, const NamespaceImpl&, const RdxContext&);
	template <bool left>
	void processField(FieldsComparator&, const QueryField&, const NamespaceImpl&) const;
	void processJoinEntry(const JoinQueryEntry&, OpType);
	void processQueryEntryResults(SelectKeyResults&&, OpType, const NamespaceImpl&, const QueryEntry&, reindexer::IsRanked,
								  std::optional<OpType> nextOp);

	using EqualPositions = h_vector<size_t, 4>;
	void processEqualPositions(const h_vector<EqualPositions, 2>& equalPositions, const NamespaceImpl& ns, const QueryEntries& queries,
							   const EqualPositions_t& eqPosStr);

	static h_vector<EqualPositions, 2> prepareEqualPositions(const NamespaceImpl& ns, const QueryEntries& queries, size_t begin,
															 size_t end);
	template <typename EqCompT>
	void bindFieldEqualPositions(const NamespaceImpl& ns, const h_vector<size_t, 4>& eqPos, const QueryEntries& queries,
								 const EqualPosition_t& eqPosStr);
	static std::pair<iterator, iterator> findFirstRanked(iterator begin, iterator end, const NamespaceImpl&);
	static bool isRanked(const_iterator, const NamespaceImpl&);
	static void throwIfNotFt(const QueryEntries&, size_t i, const NamespaceImpl&);
	template <bool desc>
	void mergeRanked(RanksHolder::Ptr&, const Reranker&, const NamespaceImpl&);
	bool hasDistinctComparatorsFromPreviousStage() const noexcept { return !preservedDistincts_.Empty(); }

	/// @return end() if empty or last opened bracket is empty
	iterator lastAppendedOrClosed();
	static void dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JoinedSelector>&, WrSerializer&);

	PayloadType pt_;
	SelectCtx* ctx_;
	int maxIterations_;
	struct : Base {
	} preservedDistincts_;
};

}  // namespace reindexer
