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
#include "estl/restricted.h"

namespace reindexer {

struct SelectCtx;
class RdxContext;
class JoinedSelector;
class QueryPreprocessor;
class NamespaceImpl;
class RanksHolder;
class Reranker;

struct JoinSelectIterator {
	size_t joinIndex;
	double Cost() const noexcept { return std::numeric_limits<float>::max(); }
	void Dump(WrSerializer&, const std::vector<JoinedSelector>&) const;
};

struct SelectIteratorsBracket : private Bracket {
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
	[[nodiscard]] int IndexNo() const noexcept { return indexNo_; }
	[[nodiscard]] const KnnRawResult& RawResult() const& noexcept { return rawResult_; }
	[[nodiscard]] KnnRawResult& RawResult() & noexcept { return rawResult_; }

	auto RawResult() const&& = delete;

private:
	KnnRawResult rawResult_;
	int indexNo_;
};

class SelectIteratorContainer
	: public ExpressionTree<OpType, SelectIteratorsBracket, 2, SelectIterator, JoinSelectIterator, FieldsComparator, AlwaysTrue,
							ComparatorIndexed<bool>, ComparatorIndexed<int>, ComparatorIndexed<int64_t>, ComparatorIndexed<double>,
							ComparatorIndexed<key_string>, ComparatorIndexed<PayloadValue>, ComparatorIndexed<Point>,
							ComparatorIndexed<Uuid>, ComparatorIndexed<FloatVector>, EqualPositionComparator, ComparatorNotIndexed,
							ComparatorDistinctMulti, KnnRawSelectResult> {
	using Base = ExpressionTree<OpType, SelectIteratorsBracket, 2, SelectIterator, JoinSelectIterator, FieldsComparator, AlwaysTrue,
								ComparatorIndexed<bool>, ComparatorIndexed<int>, ComparatorIndexed<int64_t>, ComparatorIndexed<double>,
								ComparatorIndexed<key_string>, ComparatorIndexed<PayloadValue>, ComparatorIndexed<Point>,
								ComparatorIndexed<Uuid>, ComparatorIndexed<FloatVector>, EqualPositionComparator, ComparatorNotIndexed,
								ComparatorDistinctMulti, KnnRawSelectResult>;

public:
	enum class [[nodiscard]] MergeType : bool;

private:
	template <MergeType, bool desc, VectorMetric>
	class [[nodiscard]] MergerRankedImpl;
	class [[nodiscard]] MergerRanked;

public:
	SelectIteratorContainer(PayloadType pt = PayloadType(), SelectCtx* ctx = nullptr)
		: pt_(std::move(pt)), ctx_(ctx), maxIterations_(std::numeric_limits<int>::max()), wasZeroIterations_(false) {}

	void SortByCost(int expectedIterations);
	bool HasIdsets() const;
	// Check NOT or comparator must not be 1st
	void CheckFirstQuery();
	// Let iterators choose most effective algorithm
	void SetExpectMaxIterations(int expectedIterations);
	void PrepareIteratorsForSelectLoop(QueryPreprocessor&, unsigned sortId, RankedTypeQuery, RankSortType, const NamespaceImpl&,
									   FtFunction::Ptr&, RanksHolder::Ptr&, const RdxContext&);
	template <bool reverse, bool hasComparators>
	bool Process(PayloadValue&, bool* finish, IdType* rowId, IdType, bool match);

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
			[] RX_PRE_LMBD_ALWAYS_INLINE(OneOf<SelectIteratorsBracket, JoinSelectIterator, FieldsComparator, AlwaysFalse, AlwaysTrue,
											   EqualPositionComparator, KnnRawSelectResult>)
				RX_POST_LMBD_ALWAYS_INLINE noexcept { return IsDistinct_False; },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const ComparatorDistinctMulti&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return IsDistinct_True; },
			Restricted<ComparatorNotIndexed, SelectIterator,
					   Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>>{}(
				[] RX_PRE_LMBD_ALWAYS_INLINE(const auto& comp) RX_POST_LMBD_ALWAYS_INLINE noexcept { return comp.IsDistinct(); }));
	}
	void ExplainJSON(int iters, JsonBuilder& builder, const std::vector<JoinedSelector>* js) const {
		explainJSON(cbegin(), cend(), iters, builder, js);
	}

	void Clear() {
		clear();
		maxIterations_ = std::numeric_limits<int>::max();
		wasZeroIterations_ = false;
	}
	int GetMaxIterations(bool withZero = false) const noexcept { return (withZero && wasZeroIterations_) ? 0 : maxIterations_; }
	std::string Dump() const;
	static bool IsExpectingOrderedResults(const QueryEntry& qe) noexcept {
		return IsOrderedCondition(qe.Condition()) || (qe.Condition() != CondAny && qe.Values().size() <= 1);
	}
	void MergeRanked(RanksHolder::Ptr&, const Reranker&, const NamespaceImpl&);

private:
	ContainRanked prepareIteratorsForSelectLoop(QueryPreprocessor&, size_t begin, size_t end, unsigned sortId, RankedTypeQuery,
												RankSortType, const NamespaceImpl&, FtFunction::Ptr&, RanksHolder::Ptr&, const RdxContext&);
	void sortByCost(std::span<unsigned int> indexes, std::span<double> costs, unsigned from, unsigned to, int expectedIterations);
	double fullCost(std::span<unsigned> indexes, unsigned i, unsigned from, unsigned to, int expectedIterations) const noexcept;
	double cost(std::span<unsigned> indexes, unsigned cur, int expectedIterations) const noexcept;
	double cost(std::span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const noexcept;
	void moveJoinsToTheBeginningOfORs(std::span<unsigned> indexes, unsigned from, unsigned to);
	// Check idset must be 1st
	template <bool reverse>
	bool checkIfSatisfyCondition(SelectIterator&, bool* finish, IdType rowId);
	bool checkIfSatisfyCondition(JoinSelectIterator&, PayloadValue&, IdType properRowId, bool match);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue&, bool* finish, IdType rowId, IdType properRowId,
									 bool match);
	static std::string explainJSON(const_iterator it, const_iterator to, int iters, JsonBuilder& builder,
								   const std::vector<JoinedSelector>*);
	template <bool reverse>
	static IdType getNextItemId(const_iterator begin, const_iterator end, IdType from);
	static bool isIdset(const_iterator it, const_iterator end);
	static bool markBracketsHavingJoins(iterator begin, iterator end) noexcept;
	bool haveJoins(size_t i) const noexcept;

	SelectKeyResults processQueryEntry(const QueryEntry& qe, const NamespaceImpl& ns, StrictMode strictMode);
	SelectKeyResults processQueryEntry(const QueryEntry& qe, bool enableSortIndexOptimize, const NamespaceImpl& ns, unsigned sortId,
									   RankedTypeQuery, RankSortType, FtFunction::Ptr& selectFnc, RanksHolder::Ptr&, reindexer::IsRanked&,
									   IsSparse&, QueryPreprocessor& qPreproc, const RdxContext&);
	SelectKeyResult processKnnQueryEntry(const KnnQueryEntry&, const NamespaceImpl&, RanksHolder::Ptr&, const RdxContext&);
	KnnRawResult processKnnQueryEntryRaw(const KnnQueryEntry&, const NamespaceImpl&, const RdxContext&);
	template <bool left>
	void processField(FieldsComparator&, const QueryField&, const NamespaceImpl&) const;
	void processJoinEntry(const JoinQueryEntry&, OpType);
	void processQueryEntryResults(SelectKeyResults&&, OpType, const NamespaceImpl&, const QueryEntry&, reindexer::IsRanked, IsSparse,
								  std::optional<OpType> nextOp);
	using EqualPositions = h_vector<size_t, 4>;
	void processEqualPositions(const std::vector<EqualPositions>& equalPositions, const NamespaceImpl& ns, const QueryEntries& queries);
	static std::vector<EqualPositions> prepareEqualPositions(const QueryEntries& queries, size_t begin, size_t end);
	[[nodiscard]] static std::pair<iterator, iterator> findFirstRanked(iterator begin, iterator end, const NamespaceImpl&);
	[[nodiscard]] static bool isRanked(const_iterator, const NamespaceImpl&);
	static void throwIfNotFt(const QueryEntries&, size_t i, const NamespaceImpl&);
	template <bool desc>
	void mergeRanked(RanksHolder::Ptr&, const Reranker&, const NamespaceImpl&);

	/// @return end() if empty or last opened bracket is empty
	iterator lastAppendedOrClosed() {
		typename Container::iterator it = this->container_.begin(), end = this->container_.end();
		if (!this->activeBrackets_.empty()) {
			it += (this->activeBrackets_.back() + 1);
		}
		if (it == end) {
			return this->end();
		}
		iterator i = it, i2 = it, e = end;
		while (++i2 != e) {
			i = i2;
		}
		return i;
	}
	static void dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JoinedSelector>&, WrSerializer&);

	PayloadType pt_;
	SelectCtx* ctx_;
	int maxIterations_;
	bool wasZeroIterations_;
};

}  // namespace reindexer
