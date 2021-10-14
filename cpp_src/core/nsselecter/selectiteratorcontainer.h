#pragma once
#include <limits>
#include "core/expressiontree.h"
#include "core/nsselecter/selectiterator.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfunc.h"

namespace reindexer {

struct SelectCtx;
class RdxContext;
class JoinedSelector;

struct JoinSelectIterator {
	size_t joinIndex;
	double Cost() const noexcept { return std::numeric_limits<float>::max(); }
	void Dump(WrSerializer &, const std::vector<JoinedSelector> &) const;
};

struct SelectIteratorsBracket : private Bracket {
	using Bracket::Bracket;
	using Bracket::Size;
	using Bracket::Append;
	void CopyPayloadFrom(const SelectIteratorsBracket &) const noexcept {}
	bool haveJoins = false;
};

class SelectIteratorContainer : public ExpressionTree<OpType, SelectIteratorsBracket, 2, SelectIterator, JoinSelectIterator> {
	using Base = ExpressionTree<OpType, SelectIteratorsBracket, 2, SelectIterator, JoinSelectIterator>;

public:
	SelectIteratorContainer(PayloadType pt = PayloadType(), SelectCtx *ctx = nullptr)
		: pt_(pt), ctx_(ctx), maxIterations_(std::numeric_limits<int>::max()), wasZeroIterations_(false) {}

	const SelectIterator &GetSelectIterator(size_t i) const {
		assert(i < container_.size());
		return container_[i].Value();
	}
	const JoinSelectIterator &GetJoinSelectIterator(size_t i) const {
		assert(i < container_.size());
		return container_[i].Value<JoinSelectIterator>();
	}

	void SortByCost(int expectedIterations);
	bool HasIdsets() const;
	// Check NOT or comparator must not be 1st
	void CheckFirstQuery();
	// Let iterators choose most effecive algorith
	void SetExpectMaxIterations(int expectedIterations);
	void PrepareIteratorsForSelectLoop(const QueryEntries &, size_t queriesBegin, size_t queriesEnd,
									   const std::multimap<unsigned, EqualPosition> &equalPositions, unsigned sortId, bool isFt,
									   const NamespaceImpl &, SelectFunction::Ptr selectFnc, FtCtx::Ptr &ftCtx, const RdxContext &);
	template <bool reverse, bool hasComparators>
	bool Process(PayloadValue &, bool *finish, IdType *rowId, IdType, bool match);

	bool IsSelectIterator(size_t i) const noexcept {
		assert(i < Size());
		return container_[i].HoldsOrReferTo<SelectIterator>();
	}
	bool IsJoinIterator(size_t i) const noexcept {
		assert(i < container_.size());
		return container_[i].HoldsOrReferTo<JoinSelectIterator>();
	}
	void ExplainJSON(int iters, JsonBuilder &builder, const vector<JoinedSelector> *js) const {
		explainJSON(cbegin(), cend(), iters, builder, js);
	}

	void Clear() {
		clear();
		maxIterations_ = std::numeric_limits<int>::max();
		wasZeroIterations_ = false;
	}
	int GetMaxIterations(bool withZero = false) { return (withZero && wasZeroIterations_) ? 0 : maxIterations_; }
	std::string Dump() const;

private:
	void sortByCost(span<unsigned> indexes, span<double> costs, unsigned from, unsigned to, int expectedIterations);
	double fullCost(span<unsigned> indexes, unsigned i, unsigned from, unsigned to, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned cur, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const;
	void moveJoinsToTheBeginingOfORs(span<unsigned> indexes, unsigned from, unsigned to);
	// Check idset must be 1st
	static void checkFirstQuery(Container &);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyCondition(SelectIterator &, PayloadValue &, bool *finish, IdType rowId, IdType properRowId);
	bool checkIfSatisfyCondition(JoinSelectIterator &, PayloadValue &, IdType properRowId, bool match);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue &, bool *finish, IdType rowId, IdType properRowId,
									 bool match);
	static std::string explainJSON(const_iterator it, const_iterator to, int iters, JsonBuilder &builder, const vector<JoinedSelector> *);
	template <bool reverse>
	static IdType next(const_iterator, IdType from);
	template <bool reverse>
	static IdType getNextItemId(const_iterator begin, const_iterator end, IdType from);
	static bool isIdset(const_iterator it, const_iterator end);
	static bool markBracketsHavingJoins(iterator begin, iterator end) noexcept;
	bool haveJoins(size_t i) const noexcept;

	SelectKeyResults processQueryEntry(const QueryEntry &qe, const NamespaceImpl &ns, StrictMode strictMode);
	SelectKeyResults processQueryEntry(const QueryEntry &qe, bool enableSortIndexOptimize, const NamespaceImpl &ns, unsigned sortId,
									   bool isQueryFt, SelectFunction::Ptr selectFnc, bool &isIndexFt, bool &isIndexSparse, FtCtx::Ptr &,
									   const RdxContext &);
	void processJoinEntry(const QueryEntry &qe, OpType op);
	void processQueryEntryResults(SelectKeyResults &selectResults, OpType, const NamespaceImpl &ns, const QueryEntry &qe, bool isIndexFt,
								  bool isIndexSparse, bool nonIndexField);
	void processEqualPositions(const std::multimap<unsigned, EqualPosition> &equalPositions, size_t begin, size_t end,
							   const NamespaceImpl &ns, const QueryEntries &queries);

	/// @return end() if empty or last opened bracket is empty
	iterator lastAppendedOrClosed() {
		typename Container::iterator it = this->container_.begin(), end = this->container_.end();
		if (!this->activeBrackets_.empty()) it += (this->activeBrackets_.back() + 1);
		if (it == end) return this->end();
		iterator i = it, i2 = it, e = end;
		while (++i2 != e) i = i2;
		return i;
	}
	static void dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JoinedSelector> &, WrSerializer &);

	PayloadType pt_;
	SelectCtx *ctx_;
	int maxIterations_;
	bool wasZeroIterations_;
};

}  // namespace reindexer
