#pragma once
#include "core/expressiontree.h"
#include "core/nsselecter/selectiterator.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfunc.h"

namespace reindexer {

struct SelectCtx;
class RdxContext;

template <>
bool ExpressionTree<SelectIterator, OpType, 2>::Leaf::IsEqual(const Node &) const;

class SelectIteratorContainer : public ExpressionTree<SelectIterator, OpType, 2> {
public:
	SelectIteratorContainer(PayloadType pt = PayloadType(), SelectCtx *ctx = nullptr) : pt_(pt), ctx_(ctx){};

	void ForEachIterator(const std::function<void(const SelectIterator &, OpType)> &func) const { ForEachValue(func); }
	void ForEachIterator(const std::function<void(SelectIterator &)> &func) { ForEachValue(func); }

	void SortByCost(int expectedIterations);
	bool HasIdsets() const;
	// Check NOT or comparator must not be 1st
	void CheckFirstQuery();
	// Let iterators choose most effecive algorith
	void SetExpectMaxIterations(int expectedIterations);
	void PrepareIteratorsForSelectLoop(const QueryEntries &, size_t queriesBegin, size_t queriesEnd,
									   const std::multimap<unsigned, EqualPosition> &equalPositions, unsigned sortId, bool isFt,
									   const Namespace &, SelectFunction::Ptr selectFnc, FtCtx::Ptr &ftCtx, const RdxContext &);
	template <bool reverse, bool hasComparators>
	bool Process(PayloadValue &, bool *finish, IdType *rowId, IdType, bool match);

	bool IsIterator(size_t i) const { return IsValue(i); }
	void ExplainJSON(int iters, JsonBuilder &builder) const { explainJSON(cbegin(), cend(), iters, builder); }

private:
	void sortByCost(span<unsigned> indexes, span<double> costs, unsigned from, unsigned to, int expectedIterations);
	double fullCost(span<unsigned> indexes, unsigned i, unsigned from, unsigned to, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned cur, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const;
	void moveJoinsToTheBeginingOfORs(span<unsigned> indexes, unsigned from, unsigned to);
	// Check idset must be 1st
	static void checkFirstQuery(Container &);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyCondition(SelectIterator &, PayloadValue &, bool *finish, IdType rowId, IdType properRowId, bool match);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue &, bool *finish, IdType rowId, IdType properRowId,
									 bool match);
	static void explainJSON(const_iterator it, const_iterator to, int iters, JsonBuilder &builder);
	template <bool reverse>
	static IdType next(const_iterator, IdType from);
	template <bool reverse>
	static IdType getNextItemId(const_iterator begin, const_iterator end, IdType from);
	static bool isIdset(const_iterator it, const_iterator end);

	SelectKeyResults processQueryEntry(const QueryEntry &qe, const Namespace &ns);
	SelectKeyResults processQueryEntry(const QueryEntry &qe, bool enableSortIndexOptimize, const Namespace &ns, unsigned sortId,
									   bool isQueryFt, SelectFunction::Ptr selectFnc, bool &isIndexFt, bool &isIndexSparse, FtCtx::Ptr &,
									   const RdxContext &);
	void processJoinEntry(const QueryEntry &qe, OpType op);
	void processQueryEntryResults(SelectKeyResults &selectResults, OpType, const Namespace &ns, const QueryEntry &qe, bool isIndexFt,
								  bool isIndexSparse, bool nonIndexField);
	void processEqualPositions(const std::multimap<unsigned, EqualPosition> &equalPositions, size_t begin, size_t end, const Namespace &ns,
							   const QueryEntries &queries);
	bool processJoins(SelectIterator &it, const ConstPayload &pl, IdType properRowId, bool match);

	/// @return end() if empty or last opened bracket is empty
	iterator lastAppendedOrClosed() {
		typename Container::iterator it = this->container_.begin(), end = this->container_.end();
		if (!this->activeBrackets_.empty()) it += (this->activeBrackets_.back() + 1);
		if (it == end) return this->end();
		iterator i = it, e = end;
		while (i + 1 != e) ++i;
		return i;
	}

	PayloadType pt_;
	SelectCtx *ctx_;
};

}  // namespace reindexer
